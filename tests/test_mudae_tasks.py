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
from autoclaimer.mudae_tasks import (
    _MATCH_QUEUE_MAX,
    _Waiter,
    await_mudae_reply,
    register_mudae_tasks,
)
from autoclaimer.persistence import MudaeStateKind, TaskStateStore, utc_now
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
    (the subscribe-before-send race reproducer). The delay simulates
    POST-dispatch latency (the wire round trip), so on_dispatch fires
    before the delay: fast replies racing the send are matched."""

    def __init__(self, delay: float = 0.05) -> None:
        super().__init__()
        self._delay = delay

    async def send_message(self, channel_id: int, content: str, *, on_dispatch: Any = None) -> Any:
        if on_dispatch is not None:
            on_dispatch()  # dispatched: pacing done, on the wire
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
    # reply_timeout is the ONLY scheduler slack for the scripted events
    # (emitted near-instantly) AND the check mark (delivered ~20ms after
    # the send): a loaded CI runner (observed: py3.11 matrix leg) can
    # stall coroutine processing past 1.0s and the round then takes its
    # designed unconfirmed branch. Generous margin; the property under
    # test (available-after-use → conservative 300s recheck) is
    # independent of the reply timeout.
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 5.0}
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
    """The REAL send-boundary race (review fix): a stale-looking reply
    with a CURRENT timestamp delivered while the send is blocked BEFORE
    dispatch (outbound pacing) is rejected by the on_dispatch boundary;
    only the post-wire fresh reply matches. Explicit synchronization:
    the send blocks pre-dispatch on the gate → stale emitted → gate
    released (on_dispatch fires) → fresh emitted → only fresh accepted."""
    adapter = GatedSendAdapter()

    async def race_script() -> None:
        import dataclasses as _dc

        # Wait until the send is recorded AND gated (pre-wire pacing).
        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)  # the send is now blocked on the gate
        # Stale traffic during the pacing window — CURRENT timestamp.
        await adapter.emit(
            _dc.replace(reply_event(90, RT_COOLDOWN_REPLY), received_at=datetime.now(UTC))
        )
        adapter.gate.set()  # wire moment: on_dispatch fires inside send
        await asyncio.sleep(0.01)
        await adapter.emit(
            _dc.replace(reply_event(91, CLAIM_COOLDOWN_REPLY), received_at=datetime.now(UTC))
        )

    task = asyncio.create_task(race_script())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=2.0, author_id=MUDAE
    )
    await task
    assert reply is not None
    assert reply.message.id == 91  # the current-timestamp stale 90 was rejected


async def test_backdated_reply_is_rejected_as_defense_in_depth() -> None:
    """Defense in depth: even a slow-delivered event whose received_at
    predates the dispatch boundary is rejected regardless of the gate."""
    adapter = LatencyAdapter(delay=0.05)
    stale = dataclasses.replace(
        reply_event(93, RT_COOLDOWN_REPLY), received_at=datetime.now(UTC) - timedelta(seconds=30)
    )
    fresh_reply = reply_event(94, CLAIM_COOLDOWN_REPLY)

    async def emitter() -> None:
        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await adapter.emit(stale)
        await adapter.emit(dataclasses.replace(fresh_reply, received_at=datetime.now(UTC)))

    task = asyncio.create_task(emitter())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=2.0, author_id=MUDAE
    )
    await task
    assert reply is not None
    assert reply.message.id == 94


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


# $rt reaction-confirmation protocol (spec round 4): deterministic races


class GatedSendAdapter(MockAdapter):
    """Send records the message, then BLOCKS on a gate simulating
    PRE-WIRE pacing (readiness / rate-limiter wait); on_dispatch fires
    only after the gate — the wire moment. Events emitted while gated
    are PRE-dispatch (stale by construction); after the gate they are
    genuine replies. MockAdapter ids are sequential (_FIRST_ID+n)."""

    def __init__(self) -> None:
        super().__init__()
        self.gate = asyncio.Event()

    async def send_message(self, channel_id: int, content: str, *, on_dispatch: Any = None) -> Any:
        result = await super().send_message(channel_id, content)  # records the id
        await self.gate.wait()  # outbound pacing delay
        if on_dispatch is not None:
            on_dispatch()  # the wire moment
        return result


async def test_reaction_during_blocked_send_confirms_after_binding() -> None:
    """Spec §7.2 (the early-reaction race): Mudae's ✅ arrives while
    send_message is still blocked — BEFORE the id exists for the caller.
    The pre-registered subscription captures it; after the send returns
    and binds the ACTUAL id, the buffered reaction confirms."""
    adapter = GatedSendAdapter()
    waiter = _Waiter(
        adapter,
        channel_id=CHANNEL,
        author_id=MUDAE,
        exclude_message_ids=None,
        not_before=utc_now(),
        reaction_target=(0, MUDAE, "\u2705"),
    )
    waiter.begin()
    sending = asyncio.create_task(waiter_send(waiter, adapter))
    # While the send is blocked (recorded, id not yet returned), the bot
    # reacts — with the id the mock will return (sequential ids).
    from autoclaimer.mock_adapter import _FIRST_ID

    rt_message_id = _FIRST_ID + 1
    await asyncio.sleep(0.05)  # ensure the send recorded + waiter queued
    await adapter.emit(reaction_event(rt_message_id))
    adapter.gate.set()  # release the send; the id binds
    reply = await sending
    waiter.end()
    assert reply is not None
    assert reply.message.message_id == rt_message_id  # exact id matched


async def waiter_send(waiter: _Waiter, adapter: GatedSendAdapter) -> Any:
    sent = await adapter.send_message(CHANNEL, "$rt")
    waiter.bind_target_message(sent.message_id)
    return await waiter.wait(2.0)


async def test_buffered_reaction_for_other_message_is_rejected() -> None:
    """Spec §7.3: a reaction captured during the blocked send targets a
    DIFFERENT message — never confirms; the qualifying one (after
    binding) does."""
    adapter = GatedSendAdapter()
    waiter = _Waiter(
        adapter,
        channel_id=CHANNEL,
        author_id=MUDAE,
        exclude_message_ids=None,
        not_before=utc_now(),
        reaction_target=(0, MUDAE, "\u2705"),
    )
    waiter.begin()
    sending = asyncio.create_task(waiter_send(waiter, adapter))
    from autoclaimer.mock_adapter import _FIRST_ID

    await asyncio.sleep(0.05)
    await adapter.emit(reaction_event(_FIRST_ID - 100))  # buffered: wrong id
    adapter.gate.set()
    await asyncio.sleep(0.02)
    await adapter.emit(reaction_event(_FIRST_ID + 1))  # the real one
    reply = await sending
    waiter.end()
    assert reply is not None
    assert reply.message.message_id == _FIRST_ID + 1  # exact id, not the buffered other


async def test_check_mark_from_non_bot_user_is_ignored() -> None:
    """Spec §7.4: a ✅ on the right message from ANOTHER USER never
    confirms (identity check)."""
    adapter = MockAdapter()
    waiter = _Waiter(
        adapter,
        channel_id=CHANNEL,
        author_id=MUDAE,
        exclude_message_ids=None,
        not_before=utc_now(),
        reaction_target=(55, MUDAE, "\u2705"),
    )
    waiter.begin()
    waiter.bind_target_message(55)
    await adapter.emit(reaction_event(55, user_id=987654))  # not the bot
    reply = await waiter.wait(0.2)
    waiter.end()
    assert reply is None  # ignored: wrong user identity


async def test_reaction_in_other_channel_is_ignored() -> None:
    """Spec §7.5: the right reaction on the right message in ANOTHER
    channel never confirms."""
    from autoclaimer.adapters import RawReactionAddedEvent

    adapter = MockAdapter()
    waiter = _Waiter(
        adapter,
        channel_id=CHANNEL,
        author_id=MUDAE,
        exclude_message_ids=None,
        not_before=utc_now(),
        reaction_target=(56, MUDAE, "\u2705"),
    )
    waiter.begin()
    waiter.bind_target_message(56)
    await adapter.emit(
        RawReactionAddedEvent(
            account_id=ACCOUNT,
            received_at=datetime.now(UTC),
            channel_id=111222,  # a different channel
            message_id=56,
            user_id=MUDAE,
            emoji="\u2705",
        )
    )
    reply = await waiter.wait(0.2)
    waiter.end()
    assert reply is None


async def test_reaction_buffer_overflow_recovers_conservatively() -> None:
    """Spec §7.9a: overflowing the bounded reaction buffer marks the
    result unreliable → None (conservative), never a wrong confirm."""
    adapter = MockAdapter()
    waiter = _Waiter(
        adapter,
        channel_id=CHANNEL,
        author_id=MUDAE,
        exclude_message_ids=None,
        not_before=utc_now(),
        reaction_target=(0, MUDAE, "\u2705"),
    )
    waiter.begin()
    for i in range(100):  # far beyond the 64-slot bound
        await adapter.emit(reaction_event(5000 + i))  # bot ✅s, wrong ids
    waiter.bind_target_message(5100)
    reply = await waiter.wait(0.3)
    waiter.end()
    assert reply is None  # saturated → unreliable → conservative None


async def test_event_delivery_continues_while_outbound_send_is_delayed() -> None:
    """Spec §7.13: inbound dispatch is NOT blocked by the outbound send
    (the rate-limiter/pacing separation): while $rt's send is gated,
    an unrelated subscriber still receives a message event."""
    adapter = GatedSendAdapter()
    received: list[Any] = []

    async def subscriber(event: Any) -> None:
        received.append(event)

    unsubscribe = adapter.subscribe(subscriber)
    confirmation = _Waiter(
        adapter,
        channel_id=CHANNEL,
        author_id=MUDAE,
        exclude_message_ids=None,
        not_before=utc_now(),
        reaction_target=(0, MUDAE, "\u2705"),
    )
    confirmation.begin()

    async def send_and_confirm() -> None:
        sent = await adapter.send_message(CHANNEL, "$rt")  # blocks on gate
        confirmation.bind_target_message(sent.message_id)

    task = asyncio.create_task(send_and_confirm())
    await asyncio.sleep(0.05)  # send recorded, blocked
    chatter = reply_event(70, "unrelated chatter", author_id=MUDAE)
    import dataclasses as _dc

    await adapter.emit(_dc.replace(chatter, received_at=datetime.now(UTC)))
    await asyncio.sleep(0.05)
    assert len(received) == 1  # the dispatcher delivered during the delay
    adapter.gate.set()
    await task
    confirmation.end()
    unsubscribe()


async def test_subscription_active_before_send_begins() -> None:
    """Spec §7.1: the reaction subscription is registered BEFORE the
    $rt send starts (the waiter's subscriber exists while the send is
    still gated)."""
    adapter = GatedSendAdapter()
    before = adapter._events.stats().subscriber_count  # noqa: SLF001
    confirmation = _Waiter(
        adapter,
        channel_id=CHANNEL,
        author_id=MUDAE,
        exclude_message_ids=None,
        not_before=utc_now(),
        reaction_target=(0, MUDAE, "\u2705"),
    )
    confirmation.begin()
    during_send = asyncio.get_running_loop().create_future()

    async def send() -> None:
        during_send.set_result(
            adapter._events.stats().subscriber_count  # noqa: SLF001
        )
        await adapter.send_message(CHANNEL, "$rt")  # blocks

    task = asyncio.create_task(send())
    count_inside_send = await during_send
    adapter.gate.set()
    await task
    confirmation.end()
    assert before == 0
    assert count_inside_send >= 1  # subscribed before/inside the send


# Review round 5: dispatch-boundary races, seen-state bounds, handler-level gating


async def test_stale_reply_with_current_timestamp_during_pacing_is_rejected() -> None:
    """Review finding 1 (the REAL race): a stale Mudae message delivered
    during the outbound pacing wait has a CURRENT timestamp — the old
    pre-send not_before accepted it. With dispatch-time anchoring it is
    PRE-wire: rejected; the genuine post-wire reply matches."""
    adapter = GatedSendAdapter()

    async def stale_then_fresh() -> None:
        import dataclasses as _dc

        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        # While the send is gated (pre-wire), a leftover from a PREVIOUS
        # command arrives — fresh timestamp, current content shape.
        await adapter.emit(
            _dc.replace(reply_event(200, CLAIM_COOLDOWN_REPLY), received_at=datetime.now(UTC))
        )
        adapter.gate.set()  # wire moment
        await asyncio.sleep(0.01)
        await adapter.emit(
            _dc.replace(
                reply_event(201, "**ExampleUser**, you can't claim for another **1h 42** min."),
                received_at=datetime.now(UTC),
            )
        )

    task = asyncio.create_task(stale_then_fresh())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$mu", timeout=2.0, author_id=MUDAE
    )
    await task
    assert reply is not None
    assert reply.message.id == 201  # the stale 200 (current timestamp!) rejected


async def test_stale_current_timestamp_reply_alone_yields_none() -> None:
    """Only stale-during-pacing traffic → None (bounded re-query), even
    though every timestamp is current."""
    adapter = GatedSendAdapter()

    async def stale_only() -> None:
        import dataclasses as _dc

        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await adapter.emit(
            _dc.replace(reply_event(210, RT_COOLDOWN_REPLY), received_at=datetime.now(UTC))
        )
        adapter.gate.set()

    task = asyncio.create_task(stale_only())
    reply = await await_mudae_reply(
        adapter, channel_id=CHANNEL, command="$rtu", timeout=0.3, author_id=MUDAE
    )
    await task
    assert reply is None


async def test_seen_state_stops_growing_after_loss() -> None:
    """Review finding 2: after the queue saturates (loss), the dedup set
    must stop accumulating ids — a sustained flood leaves BOTH the
    queue and _seen bounded."""
    waiter = _Waiter(
        MockAdapter(),
        channel_id=CHANNEL,
        author_id=MUDAE,
        exclude_message_ids=None,
    )
    waiter._on_event(  # noqa: SLF001 — drive the subscriber directly
        dataclasses.replace(reply_event(300, "first"), received_at=datetime.now(UTC))
    )
    assert len(waiter._seen) == 1  # noqa: SLF001
    # Saturate the queue (64): loss fires.
    for i in range(64):
        waiter._on_event(  # noqa: SLF001
            dataclasses.replace(reply_event(400 + i, f"flood {i}"), received_at=datetime.now(UTC))
        )
    assert waiter._lost.is_set()  # noqa: SLF001
    seen_at_loss = len(waiter._seen)  # noqa: SLF001
    bound = _MATCH_QUEUE_MAX + 2  # pre-loss margin
    assert seen_at_loss <= bound
    # A sustained post-loss stream: neither queue nor seen grows.
    for i in range(500):
        waiter._on_event(  # noqa: SLF001
            dataclasses.replace(
                reply_event(1000 + i, f"post-loss {i}"), received_at=datetime.now(UTC)
            )
        )
    assert len(waiter._seen) == seen_at_loss  # noqa: SLF001 — frozen at loss
    assert waiter._matches.qsize() <= _MATCH_QUEUE_MAX  # noqa: SLF001


async def test_handler_level_gated_reaction_race_confirms_exact_id() -> None:
    """Review finding 3: the HANDLER-level early-reaction race — Mudae's
    ✅ arrives while the $rt send is still gated (id known only to the
    mock); after release the send returns, binds, and the buffered mark
    confirms the EXACT id; $rtu follows. A wrong-id mark in the same
    window never confirms."""
    adapter = GatedSendAdapter()
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)

    from autoclaimer.mock_adapter import _FIRST_ID

    rt_id = _FIRST_ID + 1  # the mock's first send id (deterministic)

    async def script() -> None:
        import dataclasses as _dc

        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        # While gated (pre-wire): a WRONG-id mark and the RIGHT-id mark
        # both arrive — the right one is post-gate in real life, but the
        # waiter only anchors at dispatch, so emit both AFTER the wire.
        adapter.gate.set()
        await asyncio.sleep(0.01)
        now = datetime.now(UTC)
        await adapter.emit(_dc.replace(reaction_event(rt_id - 50), received_at=now))  # wrong id
        await adapter.emit(_dc.replace(reaction_event(rt_id), received_at=now))  # ours

    emitter = Emitter(adapter)
    emitter.on_send(reply_event(31, "unrelated text", author_id=MUDAE)).on_send(
        reply_event(32, RT_COOLDOWN_REPLY)
    )
    race = asyncio.create_task(script())
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 1.0}
        )
    )
    await race
    assert [m.content for m in adapter.sent_messages] == ["$rt", "$rtu"]
    learned = anchor_deadline(timedelta(hours=2, minutes=59), now=NOW)
    assert outcome.retry_at == learned
    assert (await store.mudae_deadlines(ACCOUNT))[MudaeStateKind.RT] == learned
    await store.close()


# Review round 6: early reaction completes promptly (concurrent wait)


async def test_immediate_reaction_does_not_wait_out_the_text_timeout() -> None:
    """Review finding 2: a valid ✅ arriving immediately (with NO textual
    reply at all) completes the confirmation promptly — the handler waits
    text and reaction CONCURRENTLY, so success is recognized without
    burning the full reply_timeout."""
    adapter = MockAdapter()
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)

    from autoclaimer.mock_adapter import _FIRST_ID

    rt_id = _FIRST_ID + 1

    async def immediate_mark() -> None:
        import dataclasses as _dc

        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)  # bind settles
        await adapter.emit(_dc.replace(reaction_event(rt_id), received_at=datetime.now(UTC)))
        # NO textual reply is ever emitted for $rt.

    emitter = Emitter(adapter)
    emitter.on_send(reply_event(31, "placeholder", author_id=MUDAE)).on_send(
        reply_event(32, RT_COOLDOWN_REPLY)  # the $rtu reply (send 2)
    )
    mark = asyncio.create_task(immediate_mark())
    loop = asyncio.get_running_loop()
    started = loop.time()
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 2.0}
        )
    )
    elapsed = loop.time() - started
    await mark
    assert [m.content for m in adapter.sent_messages] == ["$rt", "$rtu"]  # confirmed → $rtu
    # Prompt completion: well under the 2.0s text timeout (the old
    # sequential code would have waited it out before checking the mark).
    assert elapsed < 1.2, f"confirmation took {elapsed:.2f}s — sequential wait suspected"
    assert outcome.retry_at is not None
    await store.close()


# Review round 7: awaited concurrent-waiter cleanup at the loop boundary


async def test_unrelated_text_then_reaction_no_overlap_no_survivors() -> None:
    """Review fix: unrelated text arrives at the loop boundary (the pair
    is cancelled); a qualifying ✅ follows. The reaction is handled ONCE,
    no overlapping queue consumers remain on either waiter, and no child
    tasks survive the round."""
    adapter = MockAdapter()
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)

    from autoclaimer.mock_adapter import _FIRST_ID

    rt_id = _FIRST_ID + 1

    async def unrelated_text_then_mark() -> None:
        import dataclasses as _dc

        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)  # bind settles; the concurrent loop is live
        now = datetime.now(UTC)
        # Unrelated Mudae chatter FIRST: triggers a loop iteration whose
        # wait pair must be fully torn down before the next pair starts.
        await adapter.emit(_dc.replace(reply_event(60, "chatter one"), received_at=now))
        await asyncio.sleep(0.05)
        await adapter.emit(_dc.replace(reply_event(61, "chatter two"), received_at=now))
        await asyncio.sleep(0.05)
        # The qualifying reaction, after the loop boundary.
        await adapter.emit(_dc.replace(reaction_event(rt_id), received_at=now))

    emitter = Emitter(adapter)
    emitter.on_send(reply_event(31, "text placeholder", author_id=MUDAE)).on_send(
        reply_event(32, RT_COOLDOWN_REPLY)
    )
    script = asyncio.create_task(unrelated_text_then_mark())
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 2.0}
        )
    )
    await script
    # Confirmed exactly once: $rtu followed and the deadline was learned.
    assert [m.content for m in adapter.sent_messages] == ["$rt", "$rtu"]
    learned = anchor_deadline(timedelta(hours=2, minutes=59), now=NOW)
    assert outcome.retry_at == learned
    assert (await store.mudae_deadlines(ACCOUNT))[MudaeStateKind.RT] == learned
    # No overlapping queue consumers: both waiters ended with empty
    # queues and no in-flight wait() getters.
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task() and not t.done()]
    # Only the harness emitter/pump tasks may remain — no stray waiters.
    assert all("wait" not in t.get_name() and "waiter" not in t.get_name() for t in pending)
    await store.close()


# Review round 8: simultaneous cooldown text + qualifying reaction


async def test_simultaneous_cooldown_text_wins_over_reaction() -> None:
    """Review fix: when the cooldown reply and the qualifying ✅ complete
    in the same wait batch, the RECOGNIZED COOLDOWN WINS (the documented
    contract): the deadline is learned, the confirmation path (and its
    $rtu) never runs — the reaction alone cannot override a cooldown."""
    adapter = MockAdapter()
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)

    from autoclaimer.mock_adapter import _FIRST_ID

    rt_id = _FIRST_ID + 1

    async def both_at_once() -> None:
        import dataclasses as _dc

        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await asyncio.sleep(0.02)  # bind settles; the concurrent loop is live
        now = datetime.now(UTC)
        # Delivered back-to-back: the cooldown text and the qualifying
        # mark land in the same wait window.
        await adapter.emit(_dc.replace(reply_event(70, RT_COOLDOWN_REPLY), received_at=now))
        await adapter.emit(_dc.replace(reaction_event(rt_id), received_at=now))

    emitter = Emitter(adapter)
    emitter.on_send(reply_event(31, "placeholder", author_id=MUDAE))
    script = asyncio.create_task(both_at_once())
    outcome = await registry.execute(
        context_for(
            "rt", adapter, {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 1.0}
        )
    )
    await script
    # The cooldown path won: deadline learned, NO $rtu (confirmation
    # path skipped), round rescheduled AT the learned deadline.
    learned = anchor_deadline(timedelta(hours=2, minutes=59), now=NOW)
    assert outcome.message == "rt cooldown learned"
    assert outcome.retry_at == learned
    assert (await store.mudae_deadlines(ACCOUNT))[MudaeStateKind.RT] == learned
    assert [m.content for m in adapter.sent_messages] == ["$rt"]  # no $rtu
    await store.close()


# External audit (BUG-001): zero-duration roll cooldowns never fail the task


async def test_roll_zero_quota_duration_reschedules_instead_of_failing() -> None:
    """BUG-001: a recognized roll-quota reply with **0** min left means
    the window is READY — clear ROLLS, reschedule at the interval, task
    alive (the exception previously escaped and the worker recorded a
    PERMANENT failure)."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(
        reply_event(
            42, "**ExampleUser**, the roulette is limited to 2 uses per hour. **0** min left."
        )
    )
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    assert outcome.status.value == "succeeded"
    assert outcome.message == "roll window ready now"
    assert outcome.retry_at == NOW + timedelta(seconds=2.0)  # roll interval
    assert MudaeStateKind.ROLLS not in await store.mudae_deadlines(ACCOUNT)  # never learned
    await store.close()


async def test_roll_zero_early_claim_duration_reschedules_instead_of_failing() -> None:
    """BUG-001 (early-claim path): an attributed claim cooldown reporting
    zero remaining clears the CLAIM deadline and ends the round at the
    interval — never an escaped exception."""
    adapter = MockAdapter()
    card = reply_event(85, "", embeds=(CARD_EMBED,))
    zero_cooldown = reply_event(86, "**ExampleUser**, you can't claim for another **0** min.")
    Emitter(adapter).on_send(card)
    follow = asyncio.create_task(emit_after_reaction(adapter, zero_cooldown))
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    await follow
    assert outcome.status.value == "succeeded"
    assert "cooldown" in outcome.message
    assert MudaeStateKind.CLAIM not in await store.mudae_deadlines(ACCOUNT)  # never learned
    assert outcome.retry_at is not None and outcome.retry_at > NOW  # interval, not now
    await store.close()


# BUG-005 (external audit batch 4): the claim confirmation must not accept
# traffic that predates the reaction's DISPATCH boundary.


async def test_claim_ignores_confirmation_arriving_before_reaction_dispatch() -> None:
    """A delayed duplicate of a PREVIOUS round's claim pair arrives while
    the reaction operation is still pacing (not yet on the wire). It must
    never satisfy THIS round's confirmation."""
    adapter = MockAdapter()
    card = reply_event(71, "", embeds=(CARD_EMBED,))
    stale = reply_event(72, CLAIM_CONFIRMATION)  # delayed duplicate
    Emitter(adapter).on_send(card)
    real_add_reaction = adapter.add_reaction

    async def add_reaction_with_pacing_traffic(
        channel_id: int,
        message_id: int,
        emoji: str,
        *,
        on_dispatch: object = None,
    ) -> None:
        # Pre-wire traffic: the stale pair lands during the pacing wait,
        # i.e. after the waiter subscribed but BEFORE the reaction request.
        await adapter.emit(dataclasses.replace(stale, received_at=datetime.now(UTC)))
        await asyncio.sleep(0.01)  # let the waiter's callback run
        import inspect
        from collections.abc import Callable
        from typing import cast

        if "on_dispatch" in inspect.signature(real_add_reaction).parameters:
            await real_add_reaction(
                channel_id,
                message_id,
                emoji,
                on_dispatch=cast("Callable[[], None] | None", on_dispatch),
            )
        else:  # pre-fix adapter contract (fail-first compatibility)
            await real_add_reaction(channel_id, message_id, emoji)

    adapter.add_reaction = add_reaction_with_pacing_traffic  # type: ignore[method-assign]
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    with pytest.raises(UncertainOutcomeError, match="confirmation lost"):
        await registry.execute(
            context_for(
                "roll",
                adapter,
                {
                    "channel": CHANNEL,
                    "author_id": MUDAE,
                    "username": "ExampleUser",
                    "reply_timeout_seconds": 0.1,
                },
            )
        )
    assert len(adapter.reactions) == 1  # the reaction itself still fired
    await store.close()


async def test_claim_accepts_confirmation_after_reaction_dispatch() -> None:
    """Control: the SAME pair arriving after the reaction reaches the wire
    is a valid confirmation (the gate must not over-reject)."""
    adapter = MockAdapter()
    card = reply_event(73, "", embeds=(CARD_EMBED,))
    stale = reply_event(74, CLAIM_CONFIRMATION)  # pre-dispatch: ignored
    fresh = reply_event(75, CLAIM_CONFIRMATION)  # post-dispatch: ours
    Emitter(adapter).on_send(card)
    real_add_reaction = adapter.add_reaction

    async def add_reaction_with_pacing_traffic(
        channel_id: int,
        message_id: int,
        emoji: str,
        *,
        on_dispatch: object = None,
    ) -> None:
        import inspect
        from collections.abc import Callable
        from typing import cast

        await adapter.emit(dataclasses.replace(stale, received_at=datetime.now(UTC)))
        await asyncio.sleep(0.01)
        if "on_dispatch" in inspect.signature(real_add_reaction).parameters:
            await real_add_reaction(
                channel_id,
                message_id,
                emoji,
                on_dispatch=cast("Callable[[], None] | None", on_dispatch),
            )
        else:  # pre-fix adapter contract (fail-first compatibility)
            await real_add_reaction(channel_id, message_id, emoji)
        # Post-wire: the genuine confirmation for THIS reaction.
        await adapter.emit(dataclasses.replace(fresh, received_at=datetime.now(UTC)))

    adapter.add_reaction = add_reaction_with_pacing_traffic  # type: ignore[method-assign]
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("roll", adapter))
    assert outcome.message == "claimed"
    await store.close()
