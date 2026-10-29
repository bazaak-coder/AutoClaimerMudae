"""Mudae task machinery: reply matching and handlers (milestone 7)."""

import asyncio
import dataclasses
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from autoclaimer.adapters import (
    ChannelMessage,
    EmbedText,
    MessageCreatedEvent,
    UncertainOutcomeError,
)
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.mudae import anchor_deadline
from autoclaimer.mudae_tasks import _Waiter, await_mudae_reply, register_mudae_tasks
from autoclaimer.persistence import MudaeStateKind, TaskStateStore
from autoclaimer.registry import TaskContext, TaskRegistry

ACCOUNT = 271485504361996288
CHANNEL = 999999000001111222
MUDAE = 4322549498252411


def reply_event(
    message_id: int,
    content: str,
    *,
    author_id: int = MUDAE,
    embeds: tuple[EmbedText, ...] = (),
) -> MessageCreatedEvent:
    return MessageCreatedEvent(
        account_id=ACCOUNT,
        received_at=datetime.now(UTC),
        message=ChannelMessage(
            id=message_id,
            channel_id=CHANNEL,
            author_id=author_id,
            content=content,
            created_at=None,
            embeds=embeds,
        ),
    )


async def emit_after_send(adapter: MockAdapter, event: Any) -> None:
    """Deterministic orchestration: emit the reply once the command send
    has been recorded by the mock adapter (re-stamped at emit time: the
    waiter's temporal correlation rejects pre-send timestamps)."""
    import dataclasses as _dc

    while not adapter.sent_messages:
        await asyncio.sleep(0.005)
    await adapter.emit(_dc.replace(event, received_at=datetime.now(UTC)))


def reaction_event(
    message_id: int, emoji: str = "\u2705", *, user_id: int = MUDAE, received: Any = None
) -> Any:
    from autoclaimer.adapters import RawReactionAddedEvent

    return RawReactionAddedEvent(
        account_id=ACCOUNT,
        received_at=received or datetime.now(UTC),
        channel_id=CHANNEL,
        message_id=message_id,
        user_id=user_id,
        emoji=emoji,
    )


async def check_mark_after_send(adapter: MockAdapter) -> None:
    """The bot confirms OUR $rt message with a heavy check mark, stamped
    at delivery time (after the send resolves and the target binds)."""
    import dataclasses as _dc

    while not adapter.sent_messages:
        await asyncio.sleep(0.005)
    sent_id = adapter.sent_messages[0].message_id
    await asyncio.sleep(0.02)  # let the send resolve + target bind first
    await adapter.emit(_dc.replace(reaction_event(sent_id), received_at=datetime.now(UTC)))


async def test_await_reply_parses_the_matched_message() -> None:
    adapter = MockAdapter()
    emitter = asyncio.create_task(
        emit_after_send(adapter, reply_event(7, "**u**, you can't claim for another **42** min."))
    )
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=2.0, author_id=MUDAE
    )
    await emitter
    assert reply is not None
    assert reply.message.id == 7
    assert reply.result.family is not None  # parsed


async def test_await_reply_times_out_returning_none() -> None:
    """T9: no reply within the window — the caller must re-query on a
    bounded policy; the helper never fabricates a reply."""
    adapter = MockAdapter()
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=0.05, author_id=MUDAE
    )
    assert reply is None
    assert adapter.sent_messages  # the command WAS sent


async def test_await_reply_ignores_other_authors_and_channels() -> None:
    adapter = MockAdapter()

    async def noise() -> None:
        import dataclasses as _dc

        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        now = datetime.now(UTC)
        await adapter.emit(
            _dc.replace(reply_event(1, "hi", author_id=42), received_at=now)
        )  # wrong author
        await adapter.emit(
            MessageCreatedEvent(
                account_id=ACCOUNT,
                received_at=datetime.now(UTC),
                message=ChannelMessage(
                    id=2, channel_id=111, author_id=MUDAE, content="other channel"
                ),
            )
        )
        await adapter.emit(reply_event(3, "**u**, you can't claim for another **42** min."))

    noise_task = asyncio.create_task(noise())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=2.0, author_id=MUDAE
    )
    await noise_task
    assert reply is not None
    assert reply.message.id == 3  # only the matching message wins


async def test_await_reply_deduplicates_by_message_id() -> None:
    """T10: the same logical reply delivered twice (duplicate trigger)
    matches once — the duplicate is never mistaken for a second reply."""
    adapter = MockAdapter()
    duplicate = reply_event(7, "**u**, you can't claim for another **42** min.")

    async def twice() -> None:
        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        import dataclasses as _dc

        now = datetime.now(UTC)
        fresh = _dc.replace(duplicate, received_at=now)
        await adapter.emit(fresh)
        await adapter.emit(fresh)  # duplicate logical trigger

    task = asyncio.create_task(twice())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=2.0, author_id=MUDAE
    )
    await task
    assert reply is not None
    assert reply.message.id == 7


async def test_await_reply_excludes_seen_message_ids() -> None:
    """The roll card (already consumed) must not be mistaken for the
    claim confirmation arriving in the same channel."""
    adapter = MockAdapter()

    async def confirmation() -> None:
        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await adapter.emit(reply_event(7, "roll card content"))  # the seen card
        await adapter.emit(reply_event(8, "「**u**」added「**c**」into the collection"))

    task = asyncio.create_task(confirmation())
    reply = await await_mudae_reply(
        adapter,
        channel_id=CHANNEL,
        command="$wa",
        timeout=2.0,
        author_id=MUDAE,
        exclude_message_ids={7},
    )
    await task
    assert reply is not None
    assert reply.message.id == 8


async def test_await_reply_always_unsubscribes() -> None:
    adapter = MockAdapter()
    await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=0.05, author_id=MUDAE
    )
    assert adapter._events.stats().subscriber_count == 0  # noqa: SLF001


# ----- task handlers (milestone 7) -----


NOW = datetime(2026, 10, 29, 12, 0, 0, tzinfo=UTC)

CLAIM_COOLDOWN_REPLY = "**ExampleUser**, you can't claim for another **1h 42** min."
RT_COOLDOWN_REPLY = "The cooldown of $rt is not over. Time left: **2h 59** min."
ROLL_QUOTA_REPLY = "**ExampleUser**, the roulette is limited to 2 uses per hour. **59** min left."
# The owner-confirmed card shape: embed author = character name,
# description = stats + kakera value + the claimable marker (the marker
# rule targets embed DESCRIPTION).
CARD_EMBED = EmbedText(
    author_name="Ena Shinonome",
    description=(
        "Project SEKAI: Colorful Stage! | Claims: #892 | Likes: #689 | **1,043**"
        "<:kakera:469835869059153940> React with any emoji to claim!"
    ),
)
# The owner-confirmed confirmation: username/character pair (with the
# kakera suffix that lets the parser capture the character name).
CLAIM_CONFIRMATION = (
    "「**ExampleUser**」added「**Ena Shinonome**」into the collection. "
    "**+250** <:kakera:469835869059153940>"
)
SOMEONE_ELSES_CLAIM = (
    "「**Rival**」added「**Someone Else**」into the collection. "
    "**+111** <:kakera:469835869059153940>"
)


def make_registry(store: TaskStateStore) -> TaskRegistry:
    registry = TaskRegistry()
    register_mudae_tasks(registry, store=store, clock=lambda: NOW)
    return registry


def context_for(
    registry_task: str, adapter: MockAdapter, config: dict[str, Any] | None = None
) -> TaskContext:
    if config is None:
        config = {
            "channel": CHANNEL,
            "author_id": MUDAE,
            "reply_timeout_seconds": 1.0,
        }
        if registry_task != "rt":  # rt has no username key (no attribution there)
            config["username"] = "ExampleUser"
    return TaskContext(
        account_id=ACCOUNT,
        task_id=registry_task,
        config=config,
        adapter=adapter,
        credentials=None,
    )


class Emitter:
    """Deterministically emits scripted replies after each send.

    Events are (re)stamped with a fresh ``received_at`` AT EMIT TIME —
    fixtures construct events eagerly, and the waiter's temporal
    correlation rejects messages whose received_at predates the send
    (a deliberately-stale fixture keeps its old stamp for negative
    tests via ``stamp_received_at=False``)."""

    def __init__(self, adapter: MockAdapter) -> None:
        self.adapter = adapter
        self.script: list[list[Any]] = []
        self.task: asyncio.Task[None] | None = None

    def on_send(self, *events_per_send: Any, stamp_received_at: bool = True) -> "Emitter":
        self.script.append(list(events_per_send))
        self.stamp = stamp_received_at
        if self.task is None:
            self.task = asyncio.create_task(self._run())
        return self

    async def _run(self) -> None:
        import dataclasses as _dc

        index = 0
        while index < len(self.script):
            while len(self.adapter.sent_messages) <= index:
                await asyncio.sleep(0.005)
            for event in self.script[index]:
                if getattr(self, "stamp", True):
                    event = _dc.replace(event, received_at=datetime.now(UTC))
                await self.adapter.emit(event)
            index += 1


async def run_mu(adapter: MockAdapter, config: dict[str, Any] | None = None) -> Any:
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("mu", adapter, config))
    return store, outcome


async def test_mu_learns_claim_deadline_and_reschedules() -> None:
    adapter = MockAdapter()
    emitter = Emitter(adapter)
    emitter.on_send(reply_event(11, CLAIM_COOLDOWN_REPLY))
    store, outcome = await run_mu(adapter)
    deadlines = await store.mudae_deadlines(ACCOUNT)
    learned = anchor_deadline(timedelta(hours=1, minutes=42), now=NOW)
    assert deadlines[MudaeStateKind.CLAIM] == learned
    assert outcome.status.value == "succeeded"
    assert outcome.retry_at == learned  # round rescheduled AT the deadline
    assert adapter.sent_messages[-1].content == "$mu"
    await store.close()


async def test_mu_gated_by_known_deadline_sends_nothing() -> None:
    store = TaskStateStore(":memory:")
    await store.open()
    await store.set_mudae_deadline(ACCOUNT, MudaeStateKind.CLAIM, NOW + timedelta(hours=1))
    registry = make_registry(store)
    adapter = MockAdapter()
    outcome = await registry.execute(context_for("mu", adapter))
    assert adapter.sent_messages == []  # gated: no command sent
    assert outcome.retry_at == NOW + timedelta(hours=1)
    await store.close()


async def test_mu_available_clears_deadline() -> None:
    store = TaskStateStore(":memory:")
    await store.open()
    # A STALE (expired) deadline does not gate: the lookup runs and the
    # available reply clears it (ready state is current truth).
    await store.set_mudae_deadline(ACCOUNT, MudaeStateKind.CLAIM, NOW - timedelta(hours=2))
    registry = make_registry(store)
    adapter = MockAdapter()
    Emitter(adapter).on_send(
        reply_event(
            12,
            (
                "**ExampleUser**, you __can__ claim right now! "
                "The next claim reset is in **2h 5** min."
            ),
        )
    )
    outcome = await registry.execute(context_for("mu", adapter))
    assert outcome.message == "claim available"
    assert MudaeStateKind.CLAIM not in await store.mudae_deadlines(ACCOUNT)
    await store.close()


async def test_mu_timeout_is_bounded_requery_no_fabrication() -> None:
    """T9: no reply → recoverable, bounded; NOTHING persisted."""
    adapter = MockAdapter()  # no emitter: no reply ever
    store, outcome = await run_mu(adapter)
    assert outcome.status.value == "failed_recoverable"
    assert "did not arrive" in outcome.message
    assert await store.mudae_deadlines(ACCOUNT) == {}  # never fabricated
    await store.close()


async def test_mu_unrecognized_reply_is_bounded_no_fabrication() -> None:
    """T8: a malformed/unrelated reply → recoverable; no state written."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(reply_event(13, "total gibberish"))
    store, outcome = await run_mu(adapter)
    assert outcome.status.value == "failed_recoverable"
    assert "not recognized" in outcome.message
    assert await store.mudae_deadlines(ACCOUNT) == {}
    await store.close()


async def test_rt_cooldown_reply_teaches_deadline() -> None:
    adapter = MockAdapter()
    Emitter(adapter).on_send(reply_event(21, RT_COOLDOWN_REPLY))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 1.0}
        )
    )
    learned = anchor_deadline(timedelta(hours=2, minutes=59), now=NOW)
    assert (await store.mudae_deadlines(ACCOUNT))[MudaeStateKind.RT] == learned
    assert outcome.retry_at == learned
    assert [m.content for m in adapter.sent_messages] == ["$rt"]  # no $rtu needed
    await store.close()


async def test_rt_use_is_followed_by_rtu_check() -> None:
    """The owner's rule: after $rt is USED (positively confirmed by the
    bot's heavy check mark on OUR message), check $rtu to learn the next
    reset deadline (perk-dependent 5-20h — never assumed)."""
    adapter = MockAdapter()
    confirmed = asyncio.create_task(check_mark_after_send(adapter))
    Emitter(adapter).on_send(  # send 1 ($rt): neutral text (not a cooldown)
        reply_event(31, "rolled something unrelated", author_id=MUDAE)
    ).on_send(  # send 2 ($rtu): the cooldown reply
        reply_event(32, RT_COOLDOWN_REPLY)
    )
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 1.0}
        )
    )
    await confirmed
    assert [m.content for m in adapter.sent_messages] == ["$rt", "$rtu"]
    learned = anchor_deadline(timedelta(hours=2, minutes=59), now=NOW)
    assert (await store.mudae_deadlines(ACCOUNT))[MudaeStateKind.RT] == learned
    assert outcome.retry_at == learned
    await store.close()


async def test_roll_quota_reply_teaches_window() -> None:
    adapter = MockAdapter()
    Emitter(adapter).on_send(reply_event(41, ROLL_QUOTA_REPLY))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    learned = anchor_deadline(timedelta(minutes=59), now=NOW)
    assert (await store.mudae_deadlines(ACCOUNT))[MudaeStateKind.ROLLS] == learned
    assert outcome.retry_at == learned  # next round at the learned window
    await store.close()


async def emit_after_reaction(adapter: MockAdapter, *events: Any) -> None:
    """Deterministic orchestration: emit follow-ups after the claim
    reaction has been recorded (the confirmation arrives in response to
    the reaction, not to the roll)."""
    import dataclasses as _dc

    while not adapter.reactions:
        await asyncio.sleep(0.005)
    for event in events:
        await adapter.emit(_dc.replace(event, received_at=datetime.now(UTC)))


async def test_roll_claimable_card_reacts_once_and_parses_confirmation() -> None:
    adapter = MockAdapter()
    card = reply_event(51, "", embeds=(CARD_EMBED,))
    confirmation = reply_event(52, CLAIM_CONFIRMATION)
    Emitter(adapter).on_send(card)  # the card follows the roll send
    follow = asyncio.create_task(emit_after_reaction(adapter, confirmation))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    await follow
    assert outcome.message == "claimed"
    assert len(adapter.reactions) == 1  # exactly ONE claim action (T10)
    assert adapter.reactions[0][1] == 51  # reacted to the card
    await store.close()


async def test_roll_ignores_other_claimers_and_waits_for_our_pair() -> None:
    """Owner semantics: the confirmation pairs a username with OUR card's
    character; a rival's pair for a different character is not ours —
    the handler keeps waiting and still claims when our pair arrives."""
    adapter = MockAdapter()
    card = reply_event(55, "", embeds=(CARD_EMBED,))
    rival = reply_event(56, SOMEONE_ELSES_CLAIM)
    ours = reply_event(57, CLAIM_CONFIRMATION)
    Emitter(adapter).on_send(card)
    follow = asyncio.create_task(emit_after_reaction(adapter, rival, ours))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    await follow
    assert outcome.message == "claimed"
    assert len(adapter.reactions) == 1  # our single claim action stood
    await store.close()


async def test_roll_early_claim_learns_claim_deadline() -> None:
    """Owner semantics: the claim cooldown appears only when the claim
    is attempted before the reset — learn the deadline, round ends."""
    adapter = MockAdapter()
    card = reply_event(58, "", embeds=(CARD_EMBED,))
    cooldown = reply_event(59, CLAIM_COOLDOWN_REPLY)
    Emitter(adapter).on_send(card)
    follow = asyncio.create_task(emit_after_reaction(adapter, cooldown))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    await follow
    assert "cooldown" in outcome.message
    learned = anchor_deadline(timedelta(hours=1, minutes=42), now=NOW)
    assert (await store.mudae_deadlines(ACCOUNT))[MudaeStateKind.CLAIM] == learned
    await store.close()


async def test_roll_confirmation_lost_is_uncertain_never_repeated() -> None:
    """The claim reaction may have applied — a lost confirmation is an
    UNCERTAIN mutation outcome, never a blind retry."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(reply_event(61, "", embeds=(CARD_EMBED,)))  # no confirmation
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    with pytest.raises(UncertainOutcomeError, match="confirmation lost"):
        await registry.execute(context_for("roll", adapter))
    assert len(adapter.reactions) == 1  # reacted once, never repeated
    await store.close()


async def test_roll_timeout_is_bounded() -> None:
    adapter = MockAdapter()
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    assert outcome.status.value == "failed_recoverable"
    assert await store.mudae_deadlines(ACCOUNT) == {}
    await store.close()


async def test_roll_unrecognized_reply_is_bounded() -> None:
    adapter = MockAdapter()
    Emitter(adapter).on_send(reply_event(71, "gibberish"))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    assert outcome.status.value == "failed_recoverable"
    assert await store.mudae_deadlines(ACCOUNT) == {}
    await store.close()


def test_roll_config_rejects_unknown_keys_and_bad_values() -> None:
    store = TaskStateStore(":memory:")
    registry = TaskRegistry()
    register_mudae_tasks(registry, store=store, clock=lambda: NOW)
    task_type = registry.get("roll")
    with pytest.raises(ValueError, match="unknown task parameter"):
        task_type.validate_config(
            {"channel": CHANNEL, "author_id": MUDAE, "username": "ExampleUser", "surprise": 1}
        )
    with pytest.raises(ValueError, match="channel"):
        task_type.validate_config({"channel": -5, "author_id": MUDAE, "username": "ExampleUser"})
    with pytest.raises(ValueError, match="author_id.*required|author_id"):
        task_type.validate_config(
            {"channel": CHANNEL, "username": "ExampleUser"}
        )  # author_id required
    with pytest.raises(ValueError, match="reply_timeout"):
        task_type.validate_config(
            {
                "channel": CHANNEL,
                "author_id": MUDAE,
                "username": "ExampleUser",
                "reply_timeout_seconds": 0,
            }
        )
    with pytest.raises(ValueError, match="username"):
        task_type.validate_config({"channel": CHANNEL, "author_id": MUDAE})  # required
    with pytest.raises(ValueError, match="unknown task parameter"):
        registry.get("rt").validate_config(
            {"channel": CHANNEL, "author_id": MUDAE, "username": "ExampleUser"}
        )  # rt has no username key (no attribution there)


async def test_roll_owned_card_is_never_reacted_to() -> None:
    """Owner semantics: the footer 'Belongs to X' marks an
    already-claimed card — no claim action, the round simply completes
    (the parser surfaces ownership as claimed_by/claimable=None)."""
    adapter = MockAdapter()
    owned_card = EmbedText(
        author_name="Ena Shinonome",
        description=(
            "Project SEKAI: Colorful Stage! | Claims: #892 | Likes: #689 | **1,043**"
            "<:kakera:469835869059153940>"
        ),
        footer="Belongs to Rival",
    )
    Emitter(adapter).on_send(reply_event(71, "", embeds=(owned_card,)))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    assert outcome.message == "rolled"
    assert adapter.reactions == []  # never react to an owned card
    await store.close()


# ----- 20-agent audit hardening regressions -----


class LatencyAdapter(MockAdapter):
    """Send takes `delay` seconds — replies can arrive DURING the send
    (the subscribe-before-send race reproducer)."""

    def __init__(self, delay: float = 0.05) -> None:
        super().__init__()
        self._delay = delay

    async def send_message(self, channel_id: int, content: str) -> Any:
        await asyncio.sleep(self._delay)
        return await super().send_message(channel_id, content)


async def test_reply_racing_the_send_is_matched_not_lost() -> None:
    """Audit F1 (reproducer-based): a reply arriving DURING the send's
    own await (gateway push racing the HTTP ack) must be matched — the
    old code subscribed after the send completed and lost it."""
    adapter = LatencyAdapter(delay=0.05)

    async def reply_mid_send() -> None:
        await asyncio.sleep(0.02)  # inside the send window
        await adapter.emit(reply_event(99, CLAIM_COOLDOWN_REPLY))

    task = asyncio.create_task(reply_mid_send())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=2.0, author_id=MUDAE
    )
    await task
    assert reply is not None  # matched despite racing the send
    assert reply.message.id == 99


async def test_waiter_unsubscribes_when_the_send_fails() -> None:
    """Audit F1 blast-radius: subscribe-before-send must not leak the
    subscription when the send itself raises."""
    from autoclaimer.adapters import PermanentAdapterError

    adapter = MockAdapter()
    adapter.script("send_message", PermanentAdapterError("send rejected"))
    with pytest.raises(PermanentAdapterError):
        await await_mudae_reply(
            adapter, channel_id=CHANNEL, command="$mu", timeout=0.5, author_id=MUDAE
        )
    assert adapter._events.stats().subscriber_count == 0  # noqa: SLF001


async def test_learned_deadline_cap_rejects_implausible_durations() -> None:
    """Audit cap: a reply reporting >48h is untrusted — recoverable, and
    NOTHING persisted (never fabricated, never parked for weeks)."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(
        reply_event(15, "**ExampleUser**, you can't claim for another **999h 59** min.")
    )
    store, outcome = await run_mu(adapter)
    assert outcome.status.value == "failed_recoverable"
    assert "implausible" in outcome.message
    assert await store.mudae_deadlines(ACCOUNT) == {}
    await store.close()


async def test_lookup_tasks_default_to_conservative_interval() -> None:
    """Audit F2: mu/rt must NOT inherit roll's 2s cadence — the default
    for lookup tasks is 300s (no command spam loops)."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(
        reply_event(
            16,
            (
                "**ExampleUser**, you __can__ claim right now! "
                "The next claim reset is in **2h 5** min."
            ),
        )
    )
    store, outcome = await run_mu(adapter)
    assert outcome.status.value == "succeeded"
    assert outcome.retry_at == NOW + timedelta(seconds=300.0)
    await store.close()


async def test_rt_available_after_use_schedules_conservative_recheck() -> None:
    """Audit rt-available branch: "$rt is available!" after a consumed
    $rt is not evidence the perk cooldown is absent — conservative
    re-check at the lookup interval, never an available-loop."""
    adapter = MockAdapter()
    confirmed = asyncio.create_task(check_mark_after_send(adapter))
    Emitter(adapter).on_send(
        reply_event(31, "rolled something unrelated", author_id=MUDAE)
    ).on_send(reply_event(32, "$rt is available!"))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 1.0}
        )
    )
    await confirmed
    assert outcome.message == "rt reportedly available after use; re-checking later"
    assert outcome.retry_at == NOW + timedelta(seconds=300.0)  # NOT 2s
    await store.close()


async def test_roll_skips_claiming_while_claim_cooldown_runs() -> None:
    """Audit F10: roll never burns claim reactions while the CLAIM
    deadline is known-future — the round rolls, the claim waits."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(reply_event(51, "", embeds=(CARD_EMBED,)))
    store = TaskStateStore(":memory:")
    await store.open()
    await store.set_mudae_deadline(ACCOUNT, MudaeStateKind.CLAIM, NOW + timedelta(hours=1))
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    assert outcome.message == "rolled (claim window pending)"
    assert adapter.reactions == []  # no claim action during the cooldown
    assert len(adapter.sent_messages) == 1  # the roll still happened
    await store.close()


async def test_owned_card_with_claimable_marker_is_never_reacted_to() -> None:
    """Audit F9 (marker-over-footer precedence): a card carrying BOTH the
    claimable marker and a "Belongs to" footer is OWNED — the footer
    vetoes the marker."""
    adapter = MockAdapter()
    contested = EmbedText(
        author_name="Ena Shinonome",
        description=(
            "Project SEKAI | **1,043**<:kakera:469835869059153940> React with any emoji to claim!"
        ),
        footer="Belongs to Rival",
    )
    Emitter(adapter).on_send(reply_event(52, "", embeds=(contested,)))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    assert outcome.message == "rolled"
    assert adapter.reactions == []
    await store.close()


async def test_rival_pair_with_captured_character_is_not_our_confirmation() -> None:
    """Audit (attribution): the pair's CHARACTER is extracted from the
    owner-confirmed template even without the kakera suffix — a rival's
    pair for a different character is skipped and OUR pair confirms.
    Also pins the matched confirmation id (audit-11 F1 fix)."""
    adapter = MockAdapter()
    card = reply_event(60, "", embeds=(CARD_EMBED,))
    rival_bare = reply_event(61, "「**Rival**」added「**Someone Else**」into the collection")
    ours_bare = reply_event(62, "「**ExampleUser**」added「**Ena Shinonome**」into the collection")
    Emitter(adapter).on_send(card)
    follow = asyncio.create_task(emit_after_reaction(adapter, rival_bare, ours_bare))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    await follow
    assert outcome.message == "claimed"
    assert len(adapter.reactions) == 1
    # audit-11 F1: prove WHICH confirmation ended the wait — the rival's
    # bare pair (61) was skipped; ours (62) matched.
    assert rival_bare.message.id == 61 and ours_bare.message.id == 62


# CI review finding 1: attribution against OUR account identity


async def test_rival_claiming_the_same_character_is_not_our_confirmation() -> None:
    """The reviewer's exact case: another user claims the SAME character
    first — the pair matches our card but the USERNAME is the rival's:
    never accepted as our claim (previously only the character was
    checked)."""
    adapter = MockAdapter()
    card = reply_event(80, "", embeds=(CARD_EMBED,))
    rival_same_character = reply_event(
        81, "「**Rival**」added「**Ena Shinonome**」into the collection"
    )
    Emitter(adapter).on_send(card)
    follow = asyncio.create_task(emit_after_reaction(adapter, rival_same_character))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    with pytest.raises(UncertainOutcomeError, match="confirmation lost"):
        await registry.execute(context_for("roll", adapter))
    await follow
    assert len(adapter.reactions) == 1  # we reacted once; the rival's pair
    # was NOT our confirmation -> uncertain (the reaction may have applied)


async def test_claim_cooldown_addressed_to_another_user_is_rejected() -> None:
    """A claim-line addressed to a DIFFERENT username (e.g. another
    account's $mu reply in the same channel) never teaches OUR claim
    deadline."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(
        reply_event(82, "**Rival**, you can't claim for another **1h 42** min.")
    )
    store, outcome = await run_mu(adapter)
    assert outcome.status.value == "failed_recoverable"
    assert "not addressed to this account" in outcome.message
    assert await store.mudae_deadlines(ACCOUNT) == {}  # nothing learned
    await store.close()


async def test_roll_early_cooldown_addressed_to_rival_is_not_learned() -> None:
    """Same attribution rule on the roll confirmation path: a rival's
    early-claim cooldown arriving in our window is skipped (keep
    waiting) — our reaction's outcome stays uncertain rather than
    learning the rival's deadline."""
    adapter = MockAdapter()
    card = reply_event(83, "", embeds=(CARD_EMBED,))
    rival_cooldown = reply_event(84, "**Rival**, you can't claim for another **3** min.")
    Emitter(adapter).on_send(card)
    follow = asyncio.create_task(emit_after_reaction(adapter, rival_cooldown))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    with pytest.raises(UncertainOutcomeError, match="confirmation lost"):
        await registry.execute(context_for("roll", adapter))
    await follow
    assert await store.mudae_deadlines(ACCOUNT) == {}  # rival's deadline NOT learned
    await store.close()


# CI review finding 2: temporal correlation to the current command


async def test_stale_reply_from_a_previous_command_is_ignored() -> None:
    """A message whose received_at predates the current command's send
    (a slow-delivered leftover of a PREVIOUS operation — e.g. delivered
    during our rate-limit wait) is rejected; the fresh reply matches."""
    adapter = LatencyAdapter(delay=0.05)
    stale = dataclasses.replace(
        reply_event(90, RT_COOLDOWN_REPLY), received_at=datetime.now(UTC) - timedelta(seconds=30)
    )
    fresh_reply = reply_event(91, CLAIM_COOLDOWN_REPLY)

    async def emitter() -> None:
        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await adapter.emit(stale)  # stale delivered DURING our send window
        # The fresh reply is stamped AT DELIVERY (real replies are).
        await adapter.emit(dataclasses.replace(fresh_reply, received_at=datetime.now(UTC)))

    task = asyncio.create_task(emitter())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=2.0, author_id=MUDAE
    )
    await task
    assert reply is not None
    assert reply.message.id == 91  # the stale 90 was temporally rejected


async def test_stale_reply_alone_times_out_to_bounded_requery() -> None:
    """Only-stale traffic: the wait yields None (bounded re-query), never
    a wrong learn."""
    adapter = LatencyAdapter(delay=0.05)
    stale = dataclasses.replace(
        reply_event(92, RT_COOLDOWN_REPLY),
        received_at=datetime.now(UTC) - timedelta(seconds=30),
    )

    async def emitter() -> None:
        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await adapter.emit(stale)

    task = asyncio.create_task(emitter())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$rtu", timeout=0.2, author_id=MUDAE
    )
    await task
    assert reply is None


# CI review finding 3: bounded reply buffering


async def test_waiter_queue_saturation_fails_fast_to_loss() -> None:
    """Overflowing the match queue triggers the loss path (fast None)
    instead of unbounded growth."""
    adapter = MockAdapter()

    async def flood() -> None:
        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        for i in range(200):  # far beyond the 64-slot bound
            await adapter.emit(reply_event(1000 + i, f"chatter {i}"))

    task = asyncio.create_task(flood())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=2.0, author_id=MUDAE
    )
    await task
    assert reply is None  # saturated -> loss path (bounded re-query upstream)


# CI review finding 5: $rtu only after POSITIVE $rt confirmation


async def test_check_mark_confirms_then_rtu_follows() -> None:
    """The bot's ✅ on OUR $rt message → confirmed success → $rtu sent."""
    adapter = MockAdapter()
    confirmed = asyncio.create_task(check_mark_after_send(adapter))
    Emitter(adapter).on_send(reply_event(31, "some unrelated text", author_id=MUDAE)).on_send(
        reply_event(32, RT_COOLDOWN_REPLY)
    )
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 0.5}
        )
    )
    await confirmed
    assert [m.content for m in adapter.sent_messages] == ["$rt", "$rtu"]
    assert outcome.retry_at is not None
    await store.close()


async def test_no_check_mark_means_no_rtu() -> None:
    """An unrelated response WITHOUT the ✅ → the use is unconfirmed →
    $rtu is never sent; conservative recoverable."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(reply_event(31, "some unrelated text", author_id=MUDAE))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 0.3}
        )
    )
    assert [m.content for m in adapter.sent_messages] == ["$rt"]  # no $rtu
    assert outcome.status.value == "failed_recoverable"
    assert "unconfirmed" in outcome.message
    await store.close()


async def test_check_mark_on_older_message_does_not_confirm() -> None:
    """A ✅ reacting to an OLDER message (not our $rt) never confirms the
    current use."""

    async def old_mark(adapter: MockAdapter) -> None:
        import dataclasses as _dc

        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)
        await adapter.emit(
            _dc.replace(reaction_event(1), received_at=datetime.now(UTC))
        )  # message_id=1: an older message, not our $rt

    adapter = MockAdapter()
    stale = asyncio.create_task(old_mark(adapter))
    Emitter(adapter).on_send(reply_event(31, "some unrelated text", author_id=MUDAE))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 0.3}
        )
    )
    await stale
    assert [m.content for m in adapter.sent_messages] == ["$rt"]
    assert "unconfirmed" in outcome.message
    await store.close()


async def test_lost_check_mark_recovers_bounded() -> None:
    """The reaction dispatch is lost entirely → bounded wait → safe
    recoverable (never an assumed success)."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(reply_event(31, "some unrelated text", author_id=MUDAE))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 0.2}
        )
    )
    assert outcome.status.value == "failed_recoverable"
    assert MudaeStateKind.RT not in await store.mudae_deadlines(ACCOUNT)
    await store.close()


async def test_check_mark_after_timeout_does_not_confirm_later() -> None:
    """A ✅ arriving AFTER the wait timed out lands on nobody (the
    subscription ended) — it cannot retroactively confirm anything, and
    a SUBSEQUENT round re-checks from scratch."""

    async def late_mark(adapter: MockAdapter) -> None:
        import dataclasses as _dc

        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.5)  # well past the 0.2s wait
        sent_id = adapter.sent_messages[0].message_id
        await adapter.emit(_dc.replace(reaction_event(sent_id), received_at=datetime.now(UTC)))

    adapter = MockAdapter()
    late = asyncio.create_task(late_mark(adapter))
    Emitter(adapter).on_send(reply_event(31, "some unrelated text", author_id=MUDAE))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 0.2}
        )
    )
    assert outcome.status.value == "failed_recoverable"
    await late
    await asyncio.sleep(0.05)
    assert [m.content for m in adapter.sent_messages] == ["$rt"]  # no phantom $rtu
    await store.close()


async def test_cancelled_reaction_wait_leaks_no_background_task() -> None:
    """CI review finding 4: cancelling the caller mid-wait must tear
    down BOTH background tasks — an orphaned queue getter would consume
    a later message after cancellation."""
    adapter = MockAdapter()
    waiter = _Waiter(
        adapter,
        channel_id=CHANNEL,
        author_id=MUDAE,
        exclude_message_ids=None,
        reaction_target=(77, MUDAE, "✅"),
    )
    waiter.begin()
    try:
        waiting = asyncio.create_task(waiter.wait(5.0))
        await asyncio.sleep(0.02)
        waiting.cancel()
        with pytest.raises(asyncio.CancelledError):
            await waiting
        # A later matching event must remain UNCONSUMED (no orphan).
        await adapter.emit(reaction_event(77))
        await asyncio.sleep(0.05)
        assert waiter._matches.qsize() == 1  # noqa: SLF001
    finally:
        waiter.end()


# CI review finding 6: zero-duration deadlines never hot-loop


async def test_zero_duration_cooldown_settles_at_interval() -> None:
    """A recognized cooldown with ZERO remaining = ready now — clear the
    deadline and recheck at the interval; the retry_at is strictly in
    the FUTURE (no immediate re-execution)."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(
        reply_event(17, "**ExampleUser**, you can't claim for another **0** min.")
    )
    store, outcome = await run_mu(adapter)
    assert outcome.status.value == "succeeded"
    assert outcome.message == "claim ready now"
    assert outcome.retry_at is not None and outcome.retry_at > NOW
    assert outcome.retry_at == NOW + timedelta(seconds=300.0)  # lookup interval
    assert MudaeStateKind.CLAIM not in await store.mudae_deadlines(ACCOUNT)
    await store.close()


async def test_repeated_zero_duration_does_not_hot_loop() -> None:
    """Repeated zero-duration replies each end in ONE command per round
    with an interval-spaced reschedule — never an immediate loop."""
    adapter = MockAdapter()
    retry_ats: list[Any] = []
    sent_total = 0
    for i in range(3):
        adapter = MockAdapter()  # fresh adapter per round (fresh Emitter
        # indices) — the reply always follows THIS round's send
        emitter = Emitter(adapter)
        emitter.on_send(
            reply_event(100 + i, "**ExampleUser**, you can't claim for another **0** min.")
        )
        store, outcome = await run_mu(adapter)
        assert outcome.message == "claim ready now"
        assert outcome.retry_at is not None
        retry_ats.append(outcome.retry_at)
        sent_total += len(adapter.sent_messages)
        await store.close()
    # Every reschedule is interval-spaced from the frozen NOW — no
    # deadline ever equals 'now' (which would be immediately due).
    assert all(r == NOW + timedelta(seconds=300.0) for r in retry_ats)
    assert sent_total == 3  # one $mu per round, no extras
