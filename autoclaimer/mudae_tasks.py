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
import re
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

#: Lookup tasks (mu/rt) default to a conservative interval; the 2-second
#: default is a ROLL cadence and must never drive lookup loops (audit
#: fix: the shared default produced a ~0.5 msg/s command spam loop).
_LOOKUP_INTERVAL = 300.0

_COMMON_KEYS = {"channel", "author_id", "reply_timeout_seconds", "round_interval_seconds"}
#: mu/roll verify the reply's ADDRESSEE against the configured account
#: username (CI review finding 1: attribution must check OUR identity).
_ATTRIBUTED_KEYS = _COMMON_KEYS | {"username"}
_ROLL_KEYS = _ATTRIBUTED_KEYS | {"command", "claim", "claim_emoji"}


@dataclass(frozen=True)
class MudaeReply:
    """One matched Mudae reply: the raw normalized message + its parse."""

    message: Any  # ChannelMessage (kept untyped: adapters may evolve)
    result: ParseResult


#: CI review finding 3: bounded reply buffering. Overflow fails fast to
#: the bounded re-query path (never silent, never unbounded growth).
_MATCH_QUEUE_MAX = 64


class _Waiter:
    """Persistent subscribe-with-predicate matcher (audit-hardened).

    Fixes verified by the 20-agent audit wave and the CI review:

    - ``begin()`` subscribes BEFORE the triggering act (send / claim
      reaction) — a reply racing the operation's own await is matched,
      not silently dispatched to zero subscribers.
    - **Temporal correlation** (CI review finding 2): candidates whose
      ``received_at`` predates ``not_before`` (captured immediately
      before the send) are rejected — a slow-delivered leftover from a
      PREVIOUS command can never be mistaken for this command's reply.
      Residual ambiguity (a delayed previous reply arriving genuinely
      after our send) is unresolvable without message correlation ids;
      semantic validation at each call site bounds its effect.
    - Matches accumulate in a BOUNDED queue (64): a finished ``wait()``
      never swallows burst-delivered follow-ups; saturation fails fast
      to the loss path.
    - One subscription for a whole dialogue (no per-candidate churn);
      ``end()`` always unsubscribes.
    - Event-loss during a wait fails fast to the bounded re-query path.
    """

    def __init__(
        self,
        adapter: PlatformAdapter,
        *,
        channel_id: int,
        author_id: int | None,
        exclude_message_ids: Iterable[int] | None,
        not_before: datetime | None = None,
    ) -> None:
        self._adapter = adapter
        self._channel_id = channel_id
        self._author_id = author_id
        self._seen = set(exclude_message_ids) if exclude_message_ids is not None else set()
        self._not_before = not_before
        self._matches: asyncio.Queue[MessageCreatedEvent] = asyncio.Queue(_MATCH_QUEUE_MAX)
        self._lost = asyncio.Event()
        self._unsubscribe: Callable[[], None] | None = None

    def begin(self) -> None:
        """Subscribe. Call BEFORE the act whose reply is awaited."""
        if self._unsubscribe is None:
            self._unsubscribe = self._adapter.subscribe(
                self._on_event, on_events_lost=self._on_loss
            )

    def end(self) -> None:
        """Unsubscribe (idempotent)."""
        if self._unsubscribe is not None:
            self._unsubscribe()
            self._unsubscribe = None

    def _on_event(self, event: Any) -> None:
        if not isinstance(event, MessageCreatedEvent):
            return
        message = event.message
        if message.channel_id != self._channel_id or message.id in self._seen:
            return
        if self._author_id is not None and message.author_id != self._author_id:
            return
        if self._not_before is not None and event.received_at < self._not_before:
            return  # predates the operation: a previous command's leftover
        self._seen.add(message.id)  # duplicates of a matched id never re-match
        try:
            self._matches.put_nowait(event)
        except asyncio.QueueFull:
            # Saturated: fail fast to the bounded re-query path rather
            # than growing memory or silently dropping (CI finding 3).
            self._lost.set()

    def _on_loss(self, count: int) -> None:
        # The awaited reply may have been one of the lost events: fail
        # fast to the bounded re-query path (T9) instead of waiting out
        # the timeout.
        self._lost.set()

    async def _lost_wait_none(self) -> None:
        await self._lost.wait()

    async def wait(self, timeout: float) -> MudaeReply | None:
        """Return the NEXT buffered/live match, or None on timeout/loss."""
        loss_waiter: asyncio.Task[None] | None = None
        try:
            getter: asyncio.Future[MessageCreatedEvent] = asyncio.ensure_future(self._matches.get())
            loss_waiter = asyncio.create_task(self._lost_wait_none())
            futures: set[asyncio.Future[object]] = {
                asyncio.ensure_future(getter),
                asyncio.ensure_future(loss_waiter),
            }
            await asyncio.wait(
                futures,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=timeout,
            )
            if self._lost.is_set():
                # A loss was signaled (saturation or dispatcher loss):
                # the reconciliation path takes precedence over buffered
                # chatter (CI review finding 3).
                getter.cancel()
                return None
            if getter.done() and not getter.cancelled():
                event = getter.result()
                return MudaeReply(message=event.message, result=parse_mudae(event.message))
            getter.cancel()
            return None
        finally:
            if loss_waiter is not None:
                loss_waiter.cancel()


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

    Subscribes BEFORE sending (no loss window — audit fix: a reply
    racing the send's own await is matched, not lost) and sends through
    the rate-limited adapter (pacing included). Returns ``None`` on
    timeout or event-loss while waiting (T9: the caller re-queries
    under a bounded policy — a missing reply is never fabricated).
    Always unsubscribes, including when the send itself fails.
    Duplicate deliveries of the same message id match once (T10).
    """
    waiter = _Waiter(
        adapter,
        channel_id=channel_id,
        author_id=author_id,
        exclude_message_ids=exclude_message_ids,
        not_before=utc_now(),  # captured before the send: stale leftovers rejected
    )
    waiter.begin()
    try:
        await adapter.send_message(channel_id, command)
        return await waiter.wait(timeout)
    finally:
        waiter.end()


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
    waiter.begin()
    try:
        return await waiter.wait(timeout)
    finally:
        waiter.end()


# ----- configuration validation (agents.md section 17.2.1) -----


@dataclass(frozen=True)
class _TaskConfig:
    channel: int
    author_id: int
    reply_timeout: float
    round_interval: float
    username: str = ""  # account identity for reply attribution (mu/roll)
    command: str = "$wa"
    claim: bool = True
    claim_emoji: str = "❤️"


def _validated(
    config: Any,
    *,
    allow_roll_keys: bool,
    interval_default: float = 2.0,
    require_username: bool = False,
) -> _TaskConfig:
    if not isinstance(config, dict):
        raise ValueError("task parameters must be an object")
    allowed = (
        _ROLL_KEYS if allow_roll_keys else _ATTRIBUTED_KEYS if require_username else _COMMON_KEYS
    )
    unknown = sorted(set(config) - allowed)
    if unknown:
        raise ValueError(f"unknown task parameter(s): {', '.join(unknown)}")

    channel = config.get("channel")
    if type(channel) is not int or not 0 < channel <= _MAX_SNOWFLAKE:
        raise ValueError("parameter 'channel' must be a positive Discord snowflake ID")

    # Audit fix: the Mudae bot's author id is REQUIRED — with it absent,
    # any channel user's lookalike message is accepted as a reply (state
    # poisoning, wasted parses; see the adversarial audit).
    author_id = config.get("author_id")
    if type(author_id) is not int or not 0 < author_id <= _MAX_SNOWFLAKE:
        raise ValueError(
            "parameter 'author_id' must be the Mudae bot's snowflake ID "
            "(required: replies are matched by author)"
        )

    # CI review finding 1: mu/roll verify the reply's ADDRESSEE against
    # the configured account username — attribution against OUR identity,
    # not the reply's own content.
    username = config.get("username", "")
    if require_username:
        if not isinstance(username, str) or not username.strip():
            raise ValueError(
                "parameter 'username' must be this account's Discord username "
                "(required: replies are attributed by username)"
            )
        username = username.strip()

    def bounded(name: str, default: float, maximum: float) -> float:
        value = config.get(name, default)
        if type(value) not in (int, float) or not 0 < float(value) <= maximum:
            raise ValueError(f"parameter '{name}' must be a number in (0, {maximum}]")
        return float(value)

    cfg = _TaskConfig(
        channel=channel,
        author_id=author_id,
        reply_timeout=bounded("reply_timeout_seconds", 10.0, _MAX_TIMEOUT),
        round_interval=bounded("round_interval_seconds", interval_default, _MAX_INTERVAL),
        username=username,
    )
    if allow_roll_keys:
        command = config.get("command", cfg.command)
        if not isinstance(command, str) or not command.strip():
            raise ValueError("parameter 'command' must be a non-empty string")
        if len(command) > 2000:  # Discord message cap: fail loudly at validation
            raise ValueError("parameter 'command' must be at most 2000 characters")
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
            username=cfg.username,
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


#: Audit fix: sanity cap on LEARNED durations. The owner-observed ranges
#: (rt 5-20 h; claim resets a few hours) sit far below this; a reply
#: reporting more is untrusted (spoofed/malformed) — rejected as
#: unrecognized (recoverable), never persisted (never fabricated).
_MAX_LEARNED = timedelta(hours=48)


def _learn_deadline(reported: timedelta, *, now: datetime) -> datetime:
    """Anchor a reported duration, rejecting implausible values."""
    if reported > _MAX_LEARNED or reported < timedelta(0):
        raise _UntrustedDuration(reported)
    return anchor_deadline(reported, now=now)


class _UntrustedDuration(Exception):
    """Internal: a reported duration outside plausible bounds."""

    def __init__(self, reported: timedelta) -> None:
        super().__init__(f"reported duration outside plausible bounds: {reported}")


# Owner-confirmed claim-confirmation pair template: the character name
# is captured from the message text itself because the vendored rule
# only exposes it when the optional kakera-suffix group matches — and
# the owner-confirmed format carries a SPACE before the kakera emoji,
# which that group does not match (audit-verified). Wrapper-side
# extraction; the vendored parser stays byte-identical.
_PAIR_PATTERN = re.compile(
    r"「\*\*(?P<username>[^*」\n]+)\*\*」added「\*\*(?P<character>[^*」\n]+)\*\*」"
)

#: Claim-line addressee prefix ("**User**, you …") — extracts the
#: username a cooldown/claim reply is ADDRESSED to (attribution).
_ADDRESSEE_PATTERN = re.compile(r"^\*\*(?P<username>[^*\n]+)\*\*, you ")


def _pair_from_content(content: str) -> tuple[str, str] | None:
    """Extract (username, character) from a claim-pair message, or None."""
    match = _PAIR_PATTERN.search(content)
    if match is None:
        return None
    return match.group("username").strip(), match.group("character").strip()


# ----- task handlers -----


def _make_mu_handler(
    store: TaskStateStore, clock: Callable[[], datetime]
) -> tuple[Callable[[Any], None], Callable[[TaskContext], Any]]:
    def validate(config: Any) -> None:
        _validated(
            config,
            allow_roll_keys=False,
            interval_default=_LOOKUP_INTERVAL,
            require_username=True,
        )

    async def handler(context: TaskContext) -> TaskOutcome:
        cfg = _validated(
            context.config,
            allow_roll_keys=False,
            interval_default=_LOOKUP_INTERVAL,
            require_username=True,
        )
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
        # CI review finding 1: the claim line is ADDRESSED to a username —
        # it must be THIS account's, or the reply is someone else's state.
        if status.username != cfg.username:
            return _recoverable("$mu reply is not addressed to this account")
        if status.available:
            await store.clear_mudae_deadline(context.account_id, MudaeStateKind.CLAIM)
            return _round("claim available", clock() + timedelta(seconds=cfg.round_interval))
        if status.reported is None:
            return _recoverable("claim cooldown reply without a duration")
        try:
            learned = _learn_deadline(status.reported, now=clock())
        except _UntrustedDuration:
            return _recoverable("$mu reported an implausible duration")
        await store.set_mudae_deadline(context.account_id, MudaeStateKind.CLAIM, learned)
        return _round("claim cooldown learned", learned)

    return validate, handler


def _make_rt_handler(
    store: TaskStateStore, clock: Callable[[], datetime]
) -> tuple[Callable[[Any], None], Callable[[TaskContext], Any]]:
    def validate(config: Any) -> None:
        _validated(
            config, allow_roll_keys=False, interval_default=_LOOKUP_INTERVAL
        )  # rt: NO username key (the parser captures no addressee on $rt/$rtu)

    async def handler(context: TaskContext) -> TaskOutcome:
        cfg = _validated(context.config, allow_roll_keys=False, interval_default=_LOOKUP_INTERVAL)
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
            try:
                learned = _learn_deadline(rt.reported, now=clock())
            except _UntrustedDuration:
                return _recoverable("$rt reported an implausible duration")
            await store.set_mudae_deadline(context.account_id, MudaeStateKind.RT, learned)
            return _round("rt cooldown learned", learned)
        if rt is not None and rt.available:
            await store.clear_mudae_deadline(context.account_id, MudaeStateKind.RT)
            return _round("rt available", clock() + timedelta(seconds=cfg.round_interval))

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
            try:
                learned = _learn_deadline(rt_check.reported, now=clock())
            except _UntrustedDuration:
                return _recoverable("$rtu reported an implausible duration")
            await store.set_mudae_deadline(context.account_id, MudaeStateKind.RT, learned)
            return _round("rt reset deadline learned via $rtu", learned)
        if rt_check is not None and rt_check.available:
            # "$rt is available!" after a consumed $rt is not evidence
            # the perk cooldown is absent — schedule a conservative
            # re-check rather than an available-loop (audit fix).
            return _round(
                "rt reportedly available after use; re-checking later",
                clock() + timedelta(seconds=cfg.round_interval),
            )
        return _recoverable("$rtu reply not recognized")

    return validate, handler


def _make_roll_handler(
    store: TaskStateStore, clock: Callable[[], datetime]
) -> tuple[Callable[[Any], None], Callable[[TaskContext], Any]]:
    def validate(config: Any) -> None:
        _validated(
            config,
            allow_roll_keys=True,
            require_username=True,
        )

    async def handler(context: TaskContext) -> TaskOutcome:

        cfg = _validated(context.config, allow_roll_keys=True, require_username=True)
        deadlines = await store.mudae_deadlines(context.account_id)
        now = clock()
        window = deadlines.get(MudaeStateKind.ROLLS)
        if window is not None and window > now:
            return _round("roll window known; waiting", window)
        # Audit fix: never burn claim reactions while the claim cooldown
        # is known-future — rolling continues, claiming waits.
        claim_window = deadlines.get(MudaeStateKind.CLAIM)
        claim_gated = claim_window is not None and claim_window > now

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
            if cooldown.kind in (CooldownKind.ROULETTE, CooldownKind.ROLLS_RESET):
                if cooldown.remaining is None:
                    return _recoverable("roll-window reply without a duration")
                try:
                    learned = _learn_deadline(cooldown.remaining, now=clock())
                except _UntrustedDuration:
                    return _recoverable("roll reply reported an implausible duration")
                await store.set_mudae_deadline(context.account_id, MudaeStateKind.ROLLS, learned)
                return _round("roll window learned", learned)

        if reply.result.family is MessageFamily.CHARACTER:
            card = reply.result.character
            interval = clock() + timedelta(seconds=cfg.round_interval)
            may_claim = (
                cfg.claim
                and card is not None
                and card.claimable
                # Audit fix: the ownership footer ("Belongs to X") vetoes
                # the claimable marker — an owned card is never reacted to.
                and card.claimed_by is None
                and not claim_gated
            )
            if may_claim and card is not None:
                card_name = card.name
                # Audit fix: subscribe BEFORE the reaction — a
                # confirmation racing the reaction's await is matched,
                # not dispatched to zero subscribers. ONE subscription
                # drains the whole confirmation dialogue.
                confirmation = _Waiter(
                    context.adapter,
                    channel_id=cfg.channel,
                    author_id=cfg.author_id,
                    exclude_message_ids={reply.message.id},
                )
                confirmation.begin()
                try:
                    await context.adapter.add_reaction(
                        cfg.channel, reply.message.id, cfg.claim_emoji
                    )
                    # Owner semantics (2026-10-29): a claim confirmation is
                    # a username/character pair — the CHARACTER must match
                    # our card (another claimer's pair is not our
                    # confirmation); a claim-cooldown reply (addressee =
                    # us, NOT the "can claim right now" variant) appears
                    # only when the claim was attempted before the reset.
                    wait_deadline = asyncio.get_running_loop().time() + cfg.reply_timeout
                    ours: MudaeReply | None = None
                    early_cooldown = False
                    while True:
                        remaining = wait_deadline - asyncio.get_running_loop().time()
                        if remaining <= 0:
                            break
                        candidate = await confirmation.wait(remaining)
                        if candidate is None:
                            break
                        early = _early_claim_cooldown(candidate, cfg.username)
                        if early is not None:
                            try:
                                learned = _learn_deadline(early, now=clock())
                            except _UntrustedDuration:
                                return _recoverable(
                                    "claim cooldown reported an implausible duration"
                                )
                            await store.set_mudae_deadline(
                                context.account_id, MudaeStateKind.CLAIM, learned
                            )
                            early_cooldown = True
                            break
                        if candidate.result.family is MessageFamily.CLAIM and _pair_matches(
                            candidate.message.content, card_name, cfg.username
                        ):
                            ours = candidate
                            break
                        # Someone else's pair or an unrelated message: keep
                        # waiting for OUR confirmation.
                finally:
                    confirmation.end()
                if ours is not None:
                    return _round("claimed", interval)
                if early_cooldown:
                    return _round("claim attempted during cooldown; deadline learned", interval)
                # The reaction may have applied (a mutation): the outcome
                # is UNCERTAIN — never blindly repeated.
                raise UncertainOutcomeError(
                    "claim reaction sent; confirmation lost — outcome unknown"
                )
            if claim_gated and cfg.claim and card is not None and card.claimable:
                return _round("rolled (claim window pending)", interval)
            return _round("rolled", interval)

        return _recoverable("roll reply not recognized")  # T8

    return validate, handler


def _early_claim_cooldown(candidate: MudaeReply, account_username: str) -> timedelta | None:
    """The candidate's claim cooldown iff it is OUR early-claim reply.

    Attribution (CI review finding 1): the claim line's ADDRESSEE —
    extracted from the reply text — must equal the CONFIGURED account
    username (never the reply's own captured copy: that compared the
    reply against itself). The "__can__ claim right now" variant
    (``available=True``) is a RESET time, never a cooldown — rejected.
    """
    addressee = _pair_username(candidate)
    for cooldown in candidate.result.cooldowns:
        if cooldown.kind is not CooldownKind.CLAIM or cooldown.remaining is None:
            continue
        if cooldown.metadata.get("available") is True:
            continue  # reset-time variant, not an early-claim cooldown
        if addressee != account_username:
            continue  # addressed to someone else
        return cooldown.remaining
    return None


def _pair_username(candidate: MudaeReply) -> str | None:
    """The account-addressee username carried by the reply, if any."""
    content = candidate.message.content
    pair = _PAIR_PATTERN.search(content)
    if pair is not None:
        return pair.group("username").strip()
    addressee = _ADDRESSEE_PATTERN.search(content)
    if addressee is not None:
        return addressee.group("username").strip()
    return None


def _pair_matches(content: str, card_name: str | None, account_username: str) -> bool:
    """Does a claim-pair message confirm OUR account's claim of OUR card?

    CI review finding 1: BOTH halves of the pair are verified — the
    claiming username must be THIS account's, and the character must
    match our card. The pair shape is extracted from the owner-confirmed
    template (the vendored rule's capture misses the owner's format).
    """
    pair = _pair_from_content(content)
    if pair is None:
        return False
    username, character = pair
    if username != account_username:
        return False  # another account's claim — even of the same character
    if card_name and character != card_name:
        return False  # our username but a different card: not this claim
    return True


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
