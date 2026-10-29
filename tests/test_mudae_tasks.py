"""Mudae task machinery: reply matching and handlers (milestone 7)."""

import asyncio
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
from autoclaimer.mudae_tasks import await_mudae_reply, register_mudae_tasks
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
    has been recorded by the mock adapter."""
    while not adapter.sent_messages:
        await asyncio.sleep(0.005)
    await adapter.emit(event)


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
        while not adapter.sent_messages:
            await asyncio.sleep(0.005)
        await adapter.emit(reply_event(1, "hi", author_id=42))  # wrong author
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
        await adapter.emit(duplicate)
        await adapter.emit(duplicate)  # duplicate logical trigger

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
    return TaskContext(
        account_id=ACCOUNT,
        task_id=registry_task,
        config=config or {"channel": CHANNEL, "author_id": MUDAE, "reply_timeout_seconds": 1.0},
        adapter=adapter,
        credentials=None,
    )


class Emitter:
    """Deterministically emits scripted replies after each send."""

    def __init__(self, adapter: MockAdapter) -> None:
        self.adapter = adapter
        self.script: list[list[Any]] = []
        self.task: asyncio.Task[None] | None = None

    def on_send(self, *events_per_send: Any) -> "Emitter":
        self.script.append(list(events_per_send))
        if self.task is None:
            self.task = asyncio.create_task(self._run())
        return self

    async def _run(self) -> None:
        index = 0
        while index < len(self.script):
            while len(self.adapter.sent_messages) <= index:
                await asyncio.sleep(0.005)
            for event in self.script[index]:
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
            "**u**, you __can__ claim right now! The next claim reset is in **2h 5** min.",
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
    outcome = await registry.execute(context_for("rt", adapter))
    learned = anchor_deadline(timedelta(hours=2, minutes=59), now=NOW)
    assert (await store.mudae_deadlines(ACCOUNT))[MudaeStateKind.RT] == learned
    assert outcome.retry_at == learned
    assert [m.content for m in adapter.sent_messages] == ["$rt"]  # no $rtu needed
    await store.close()


async def test_rt_use_is_followed_by_rtu_check() -> None:
    """The owner's rule: after $rt is USED, check $rtu to learn the next
    reset deadline (perk-dependent 5-20h — never assumed)."""
    adapter = MockAdapter()
    Emitter(adapter).on_send(  # send 1 ($rt): neutral reply -> consumed
        reply_event(31, "rolled something unrelated", author_id=MUDAE)
    ).on_send(  # send 2 ($rtu): the cooldown reply
        reply_event(32, RT_COOLDOWN_REPLY)
    )
    store = TaskStateStore(":memory:")
    await store.open()
    registry = make_registry(store)
    outcome = await registry.execute(context_for("rt", adapter))
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
    assert (await store.mudae_deadlines(ACCOUNT))[MudaeStateKind.RT] == learned
    assert outcome.retry_at == learned  # next round at the learned window
    await store.close()


async def emit_after_reaction(adapter: MockAdapter, *events: Any) -> None:
    """Deterministic orchestration: emit follow-ups after the claim
    reaction has been recorded (the confirmation arrives in response to
    the reaction, not to the roll)."""
    while not adapter.reactions:
        await asyncio.sleep(0.005)
    for event in events:
        await adapter.emit(event)


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
        task_type.validate_config({"channel": CHANNEL, "surprise": 1})
    with pytest.raises(ValueError, match="channel"):
        task_type.validate_config({"channel": -5})
    with pytest.raises(ValueError, match="reply_timeout"):
        task_type.validate_config({"channel": CHANNEL, "reply_timeout_seconds": 0})


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
