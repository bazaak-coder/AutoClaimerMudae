"""Mudae task machinery: reply matching and task handlers (milestone 7).

The handlers implement the owner-defined domain semantics:

- **roll**: gate on the learned rolls window; send the configured roll
  command; a claimable card is claimed by reacting exactly once
  (duplicate deliveries of the card are excluded from the confirmation
  wait); quota/window replies are anchored to absolute UTC and
  persisted; each recurring round reschedules itself at the learned
  deadline. A lost claim confirmation is an UNCERTAIN mutation outcome
  (the reaction may have applied) — never blindly repeated.
- **rt**: if the learned ``$rt`` window is in the future, reschedule at
  it; a cooldown reply to ``$rt`` itself teaches the deadline; when
  ``$rt`` was consumed, the owner's rule applies — CHECK ``$rtu``
  afterwards to learn the next reset deadline (``$rt`` is
  perk-dependent, 5-20h observed: never assumed, always looked up).
- **mu**: claim-cooldown LOOKUP (the reply rides the existing
  claim-timer rule; no dedicated parser entry exists, by design).

Everything honors agents.md section 17: Mudae replies are domain
outcomes; missing duration ≠ zero; deadlines are never fabricated;
missing (T9) or malformed (T8) replies take bounded recoverable paths;
duplicate logical triggers act once (T10).
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any

from .adapters import MessageCreatedEvent, PlatformAdapter, UncertainOutcomeError
from .mudae import ParseResult, anchor_deadline, claim_status, parse_mudae, roll_reset_status
from .mudae_parser import CooldownKind, MessageFamily
from .persistence import MudaeStateKind, TaskStateStore, utc_now
from .registry import TaskContext, TaskOutcome, TaskStatus

logger = logging.getLogger(__name__)

_MAX_SNOWFLAKE = 2**63 - 1
_MAX_TIMEOUT = 120.0
_MAX_INTERVAL = 86400.0

_COMMON_KEYS = {"channel", "author_id", "reply_timeout_seconds", "round_interval_seconds"}
_ROLL_KEYS = _COMMON_KEYS | {"command", "claim", "claim_emoji"}


@dataclass(frozen=True)
class MudaeReply:
    """One matched Mudae reply: the raw normalized message + its parse."""

    message: Any  # ChannelMessage (kept untyped: adapters may evolve)
    result: ParseResult


class _Waiter:
    """Subscribe-with-predicate core shared by both await variants."""

    def __init__(
        self,
        adapter: PlatformAdapter,
        *,
        channel_id: int,
        author_id: int | None,
        exclude_message_ids: Iterable[int] | None,
    ) -> None:
        self._adapter = adapter
        self._channel_id = channel_id
        self._author_id = author_id
        self._exclude = set(exclude_message_ids) if exclude_message_ids is not None else set()
        self._loop = asyncio.get_running_loop()
        self._got: asyncio.Future[MessageCreatedEvent] = self._loop.create_future()
        self._lost = asyncio.Event()

    def _on_event(self, event: Any) -> None:
        if not isinstance(event, MessageCreatedEvent) or self._got.done():
            return
        message = event.message
        if message.channel_id != self._channel_id or message.id in self._exclude:
            return
        if self._author_id is not None and message.author_id != self._author_id:
            return
        self._exclude.add(message.id)  # duplicates of a matched id never re-match
        self._got.set_result(event)

    def _on_loss(self, count: int) -> None:
        # The awaited reply may have been one of the lost events: fail
        # fast to the bounded re-query path (T9) instead of waiting out
        # the timeout.
        self._lost.set()

    async def _lost_wait_none(self) -> None:
        await self._lost.wait()

    async def wait(self, timeout: float) -> MudaeReply | None:
        unsubscribe = self._adapter.subscribe(self._on_event, on_events_lost=self._on_loss)
        loss_waiter: asyncio.Task[None] | None = None
        try:
            loss_waiter = asyncio.create_task(self._lost_wait_none())
            futures: set[asyncio.Future[object]] = {
                asyncio.ensure_future(self._got),
                asyncio.ensure_future(loss_waiter),
            }
            await asyncio.wait(
                futures,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=timeout,
            )
            if self._got.done() and not self._got.cancelled():
                event = self._got.result()
                return MudaeReply(message=event.message, result=parse_mudae(event.message))
            return None
        finally:
            if loss_waiter is not None:
                loss_waiter.cancel()
            unsubscribe()


async def await_mudae_reply(
    adapter: PlatformAdapter,
    *,
    channel_id: int,
    command: str,
    timeout: float,
    author_id: int | None = None,
    exclude_message_ids: Iterable[int] | None = None,
) -> MudaeReply | None:
    """Send a Mudae command and await its reply, event-driven.

    Subscribes BEFORE sending (no loss window) and sends through the
    rate-limited adapter (pacing included). Returns ``None`` on timeout
    or event-loss while waiting (T9: the caller re-queries under a
    bounded policy — a missing reply is never fabricated). Always
    unsubscribes. Duplicate deliveries of the same message id match
    once (T10).
    """
    waiter = _Waiter(
        adapter,
        channel_id=channel_id,
        author_id=author_id,
        exclude_message_ids=exclude_message_ids,
    )
    await adapter.send_message(channel_id, command)
    return await waiter.wait(timeout)


async def await_mudae_event(
    adapter: PlatformAdapter,
    *,
    channel_id: int,
    timeout: float,
    author_id: int | None = None,
    exclude_message_ids: Iterable[int] | None = None,
) -> MudaeReply | None:
    """Await one matching Mudae message WITHOUT sending anything.

    Used for spontaneous follow-ups (e.g. the claim confirmation after a
    claim reaction). Same matching, dedup, loss, and cleanup contract as
    :func:`await_mudae_reply`.
    """
    waiter = _Waiter(
        adapter,
        channel_id=channel_id,
        author_id=author_id,
        exclude_message_ids=exclude_message_ids,
    )
    return await waiter.wait(timeout)


# ----- configuration validation (agents.md section 17.2.1) -----


@dataclass(frozen=True)
class _TaskConfig:
    channel: int
    author_id: int | None
    reply_timeout: float
    round_interval: float
    command: str = "$wa"
    claim: bool = True
    claim_emoji: str = "❤️"


def _validated(config: Any, *, allow_roll_keys: bool) -> _TaskConfig:
    if not isinstance(config, dict):
        raise ValueError("task parameters must be an object")
    allowed = _ROLL_KEYS if allow_roll_keys else _COMMON_KEYS
    unknown = sorted(set(config) - allowed)
    if unknown:
        raise ValueError(f"unknown task parameter(s): {', '.join(unknown)}")

    channel = config.get("channel")
    if type(channel) is not int or not 0 < channel <= _MAX_SNOWFLAKE:
        raise ValueError("parameter 'channel' must be a positive Discord snowflake ID")

    author_id = config.get("author_id")
    if author_id is not None and (
        type(author_id) is not int or not 0 < author_id <= _MAX_SNOWFLAKE
    ):
        raise ValueError("parameter 'author_id' must be a positive snowflake ID or omitted")

    def bounded(name: str, default: float, maximum: float) -> float:
        value = config.get(name, default)
        if type(value) not in (int, float) or not 0 < float(value) <= maximum:
            raise ValueError(f"parameter '{name}' must be a number in (0, {maximum}]")
        return float(value)

    cfg = _TaskConfig(
        channel=channel,
        author_id=author_id,
        reply_timeout=bounded("reply_timeout_seconds", 10.0, _MAX_TIMEOUT),
        round_interval=bounded("round_interval_seconds", 2.0, _MAX_INTERVAL),
    )
    if allow_roll_keys:
        command = config.get("command", cfg.command)
        if not isinstance(command, str) or not command.strip():
            raise ValueError("parameter 'command' must be a non-empty string")
        claim = config.get("claim", True)
        if type(claim) is not bool:
            raise ValueError("parameter 'claim' must be a boolean")
        emoji = config.get("claim_emoji", cfg.claim_emoji)
        if not isinstance(emoji, str) or not emoji.strip():
            raise ValueError("parameter 'claim_emoji' must be a non-empty string")
        cfg = _TaskConfig(
            channel=cfg.channel,
            author_id=cfg.author_id,
            reply_timeout=cfg.reply_timeout,
            round_interval=cfg.round_interval,
            command=command,
            claim=claim,
            claim_emoji=emoji,
        )
    return cfg


def _recoverable(message: str) -> TaskOutcome:
    return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message=message)


def _round(message: str, retry_at: datetime) -> TaskOutcome:
    """A successful recurring round: recorded as success AND rescheduled."""
    return TaskOutcome(TaskStatus.SUCCEEDED, message=message, retry_at=retry_at)


# ----- task handlers -----


def _make_mu_handler(
    store: TaskStateStore, clock: Callable[[], datetime]
) -> tuple[Callable[[Any], None], Callable[[TaskContext], Any]]:
    def validate(config: Any) -> None:
        _validated(config, allow_roll_keys=False)

    async def handler(context: TaskContext) -> TaskOutcome:
        cfg = _validated(context.config, allow_roll_keys=False)
        now = clock()
        deadline = (await store.mudae_deadlines(context.account_id)).get(MudaeStateKind.CLAIM)
        if deadline is not None and deadline > now:
            return _round("claim cooldown known; waiting", deadline)

        reply = await await_mudae_reply(
            context.adapter,
            channel_id=cfg.channel,
            command="$mu",
            timeout=cfg.reply_timeout,
            author_id=cfg.author_id,
        )
        if reply is None:
            return _recoverable("$mu reply did not arrive")  # T9: bounded re-query
        status = claim_status(reply.result)
        if status is None:
            return _recoverable("$mu reply not recognized")  # T8: no fabricated state
        if status.available:
            await store.clear_mudae_deadline(context.account_id, MudaeStateKind.CLAIM)
            return _round("claim available", now + timedelta(seconds=cfg.round_interval))
        if status.reported is None:
            return _recoverable("claim cooldown reply without a duration")
        learned = anchor_deadline(status.reported, now=now)
        await store.set_mudae_deadline(context.account_id, MudaeStateKind.CLAIM, learned)
        return _round("claim cooldown learned", learned)

    return validate, handler


def _make_rt_handler(
    store: TaskStateStore, clock: Callable[[], datetime]
) -> tuple[Callable[[Any], None], Callable[[TaskContext], Any]]:
    def validate(config: Any) -> None:
        _validated(config, allow_roll_keys=False)

    async def handler(context: TaskContext) -> TaskOutcome:

        cfg = _validated(context.config, allow_roll_keys=False)
        now = clock()
        deadline = (await store.mudae_deadlines(context.account_id)).get(MudaeStateKind.RT)
        if deadline is not None and deadline > now:
            return _round("rt window known; waiting", deadline)

        reply = await await_mudae_reply(
            context.adapter,
            channel_id=cfg.channel,
            command="$rt",
            timeout=cfg.reply_timeout,
            author_id=cfg.author_id,
        )
        if reply is None:
            return _recoverable("$rt reply did not arrive")
        rt = roll_reset_status(reply.result)
        if rt is not None and not rt.available and rt.reported is not None:
            learned = anchor_deadline(rt.reported, now=now)
            await store.set_mudae_deadline(context.account_id, MudaeStateKind.RT, learned)
            return _round("rt cooldown learned", learned)
        if rt is not None and rt.available:
            await store.clear_mudae_deadline(context.account_id, MudaeStateKind.RT)
            return _round("rt available", now + timedelta(seconds=cfg.round_interval))

        # $rt was consumed: the owner's rule — check $rtu afterwards to
        # LEARN the next reset deadline (perk-dependent; never assumed).
        check = await await_mudae_reply(
            context.adapter,
            channel_id=cfg.channel,
            command="$rtu",
            timeout=cfg.reply_timeout,
            author_id=cfg.author_id,
        )
        if check is None:
            return _recoverable("$rtu check did not arrive after $rt use")
        rt_check = roll_reset_status(check.result)
        if rt_check is not None and not rt_check.available and rt_check.reported is not None:
            learned = anchor_deadline(rt_check.reported, now=now)
            await store.set_mudae_deadline(context.account_id, MudaeStateKind.RT, learned)
            return _round("rt reset deadline learned via $rtu", learned)
        if rt_check is not None and rt_check.available:
            await store.clear_mudae_deadline(context.account_id, MudaeStateKind.RT)
            return _round("rt available after use", now + timedelta(seconds=cfg.round_interval))
        return _recoverable("$rtu reply not recognized")

    return validate, handler


def _make_roll_handler(
    store: TaskStateStore, clock: Callable[[], datetime]
) -> tuple[Callable[[Any], None], Callable[[TaskContext], Any]]:
    def validate(config: Any) -> None:
        _validated(config, allow_roll_keys=True)

    async def handler(context: TaskContext) -> TaskOutcome:

        cfg = _validated(context.config, allow_roll_keys=True)
        now = clock()
        deadline = (await store.mudae_deadlines(context.account_id)).get(MudaeStateKind.RT)
        if deadline is not None and deadline > now:
            return _round("roll window known; waiting", deadline)

        reply = await await_mudae_reply(
            context.adapter,
            channel_id=cfg.channel,
            command=cfg.command,
            timeout=cfg.reply_timeout,
            author_id=cfg.author_id,
        )
        if reply is None:
            return _recoverable("roll reply did not arrive")  # T9

        for cooldown in reply.result.cooldowns:
            if cooldown.kind in (CooldownKind.ROULETTE, CooldownKind.RT):
                if cooldown.remaining is None:
                    return _recoverable("roll-window reply without a duration")
                learned = anchor_deadline(cooldown.remaining, now=now)
                await store.set_mudae_deadline(context.account_id, MudaeStateKind.RT, learned)
                return _round("roll window learned", learned)

        if reply.result.family is MessageFamily.CHARACTER:
            card = reply.result.character
            interval = now + timedelta(seconds=cfg.round_interval)
            if cfg.claim and card is not None and card.claimable:
                card_name = card.name
                await context.adapter.add_reaction(cfg.channel, reply.message.id, cfg.claim_emoji)
                # Owner semantics (2026-10-29): a claim confirmation is a
                # username/character pair — the CHARACTER must match our
                # card (another claimer's pair is not our confirmation);
                # a claim-cooldown reply appears only when the claim was
                # attempted before the reset — learn and stop.
                excluded = {reply.message.id}
                wait_deadline = asyncio.get_running_loop().time() + cfg.reply_timeout
                ours: MudaeReply | None = None
                early_cooldown = False
                while True:
                    remaining = wait_deadline - asyncio.get_running_loop().time()
                    if remaining <= 0:
                        break
                    candidate = await await_mudae_event(
                        context.adapter,
                        channel_id=cfg.channel,
                        timeout=remaining,
                        author_id=cfg.author_id,
                        exclude_message_ids=excluded,
                    )
                    if candidate is None:
                        break
                    excluded.add(candidate.message.id)
                    claim_cooldown = next(
                        (
                            cd
                            for cd in candidate.result.cooldowns
                            if cd.kind is CooldownKind.CLAIM and cd.remaining is not None
                        ),
                        None,
                    )
                    if claim_cooldown is not None and claim_cooldown.remaining is not None:
                        await store.set_mudae_deadline(
                            context.account_id,
                            MudaeStateKind.CLAIM,
                            anchor_deadline(claim_cooldown.remaining, now=now),
                        )
                        early_cooldown = True
                        break
                    if candidate.result.family is MessageFamily.CLAIM and _pair_matches(
                        candidate.result, card_name
                    ):
                        ours = candidate
                        break
                    # Someone else's pair or an unrelated message: keep
                    # waiting for OUR confirmation.
                if ours is not None:
                    return _round("claimed", interval)
                if early_cooldown:
                    return _round("claim attempted during cooldown; deadline learned", interval)
                # The reaction may have applied (a mutation): the outcome
                # is UNCERTAIN — never blindly repeated.
                raise UncertainOutcomeError(
                    "claim reaction sent; confirmation lost — outcome unknown"
                )
            return _round("rolled", interval)

        return _recoverable("roll reply not recognized")  # T8

    return validate, handler


def _pair_matches(result: Any, card_name: str | None) -> bool:
    """Does a CLAIM-family message confirm OUR card?

    When the pair carries the kakera suffix the parser captures the
    character name (``metadata['character']``) — strict equality with
    the card's name. Bare pairs (servers without the kakera suffix in
    their claim message) expose no character name: the distinctive pair
    shape is accepted (documented limitation).
    """
    from .mudae_parser import KakeraEventKind

    for event in result.kakera_events:
        if event.kind is KakeraEventKind.CLAIM_GAIN:
            character = event.metadata.get("character")
            if character and card_name and character != card_name:
                return False  # another claimer's character
            return True
    return True  # bare pair: no character captured


def register_mudae_tasks(
    registry: Any, *, store: TaskStateStore, clock: Callable[[], datetime] = utc_now
) -> None:
    """Register the Mudae task types ``roll``/``rt``/``mu``.

    Handlers are closures bound to *store* (and an injectable *clock* for
    deterministic tests) — the composition root owns wiring; task
    ``parameters`` stay plain JSON configuration. Task ids in
    ``accounts.json`` must reference these ids.
    """
    from .registry import TaskType

    for task_id, maker, description in (
        ("roll", _make_roll_handler, "Send the roll command; claim claimable cards."),
        ("rt", _make_rt_handler, "Use $rt and learn the reset window via $rtu."),
        ("mu", _make_mu_handler, "Look up the claim cooldown ($mu)."),
    ):
        validate, handler = maker(store, clock)
        registry.register(
            TaskType(
                id=task_id,
                description=description,
                validate_config=validate,
                handler=handler,
            )
        )
