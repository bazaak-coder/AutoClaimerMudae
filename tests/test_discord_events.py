"""Event-driven adapter correction: dispatch, isolation, backpressure, lifecycle."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from typing import Any

from autoclaimer.adapters import (
    ChannelMessage,
    MessageCreatedEvent,
    MessageEditedEvent,
    RawReactionAddedEvent,
    ReactionAddedEvent,
)
from autoclaimer.discord_adapter import DiscordAdapter

ACCOUNT = 271485504361996288
OTHER = 380028190142042112
CHANNEL = 999999000001111222
TOKEN = "fake-token"  # never a real credential


class FakeCredentials:
    def get_token(self, account_id: int) -> str:
        return TOKEN

    def close(self) -> None:
        return None


class EventFakeClient:
    """Client surface for event tests: event(), login, connect, close, dispatch."""

    def __init__(self) -> None:
        self.registered: list[str] = []
        self.closed = False
        self.connect_runs = 0
        self._stop = asyncio.Event()
        self.login_ok = True

    def event(self, coro: Any) -> Any:
        setattr(self, coro.__name__, coro)
        self.registered.append(coro.__name__)
        return coro

    async def login(self, token: str) -> None:
        return None

    async def connect(self, *, reconnect: bool = True) -> None:
        self.connect_runs += 1
        await self._stop.wait()

    async def wait_until_ready(self) -> None:
        return None

    async def close(self) -> None:
        self.closed = True
        self._stop.set()

    def get_channel(self, channel_id: int) -> None:
        return None


def make_adapter(client: EventFakeClient | None = None, *, queue_size: int = 64) -> DiscordAdapter:
    return DiscordAdapter(
        account_id=ACCOUNT,
        credentials=FakeCredentials(),
        client=client if client is not None else EventFakeClient(),  # type: ignore[arg-type]
        jitter=lambda low, high: 0.0,
        sleep=_no_sleep,
        event_queue_size=queue_size,
    )


async def _no_sleep(seconds: float) -> None:
    return None


def lib_message(
    message_id: int = 1,
    content: str = "$ha",
    *,
    author_id: int = 4242,
    components: list[Any] | None = None,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=message_id,
        content=content,
        author=SimpleNamespace(id=author_id),
        channel=SimpleNamespace(id=CHANNEL),
        components=components or [],
        created_at=datetime.now(UTC),
    )


def lib_reaction(
    message: SimpleNamespace, emoji: Any, user_id: int = 777
) -> tuple[SimpleNamespace, SimpleNamespace]:
    reaction = SimpleNamespace(message=message, emoji=emoji, count=1)
    user = SimpleNamespace(id=user_id)
    return reaction, user


async def drain(rounds: int = 5) -> None:
    for _ in range(rounds):
        await asyncio.sleep(0)


# 1. message dispatch


async def test_message_dispatch() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    await client.on_message(lib_message(9, "$mu", components=[SimpleNamespace()]))  # type: ignore[attr-defined]
    await drain()
    assert len(box) == 1
    event = box[0]
    assert isinstance(event, MessageCreatedEvent)
    assert event.account_id == ACCOUNT
    assert event.message.id == 9
    assert event.message.content == "$mu"
    assert event.message.channel_id == CHANNEL
    assert event.message.author_id == 4242
    assert event.message.has_components is True
    assert event.message.created_at is not None
    await adapter.close()


# 2. reaction dispatch (cached context, emoji normalization)


async def test_reaction_dispatch_with_cached_message() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    message = lib_message(5, "$wa")
    reaction, user = lib_reaction(message, "🎲")
    await client.on_reaction_add(reaction, user)  # type: ignore[attr-defined]
    await drain()
    event = box[0]
    assert isinstance(event, ReactionAddedEvent)
    assert event.message_id == 5
    assert event.channel_id == CHANNEL
    assert event.user_id == 777
    assert event.emoji == "🎲"
    assert event.message is not None
    assert event.message.content == "$wa"
    await adapter.close()


async def test_reaction_emoji_object_is_normalized() -> None:
    class FakePartialEmoji:
        def __str__(self) -> str:
            return ":Claim:123"

    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    reaction, user = lib_reaction(lib_message(5), FakePartialEmoji())
    await client.on_reaction_add(reaction, user)  # type: ignore[attr-defined]
    await drain()
    assert box[0].emoji == ":Claim:123"
    await adapter.close()


# 3. message-edit dispatch via the raw bridge (cached and uncached)


def raw_edit_payload(*, cached: SimpleNamespace | None, after: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(
        message=after, cached_message=cached, message_id=after.id, channel_id=CHANNEL
    )


async def test_message_edit_dispatch_before_after() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    cached_before = lib_message(7, "before")
    after = lib_message(7, "after")
    await client.on_raw_message_edit(  # type: ignore[attr-defined]
        raw_edit_payload(cached=cached_before, after=after)
    )
    await drain()
    event = box[0]
    assert isinstance(event, MessageEditedEvent)
    assert event.before is not None and event.before.content == "before"
    assert event.after.content == "after"
    assert event.before.id == event.after.id == 7
    await adapter.close()


async def test_uncached_message_edit_delivers_without_before() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    after = lib_message(8, "only-after")
    await client.on_raw_message_edit(  # type: ignore[attr-defined]
        raw_edit_payload(cached=None, after=after)
    )
    await drain()
    event = box[0]
    assert isinstance(event, MessageEditedEvent)
    assert event.before is None  # uncached edit: no pre-edit snapshot
    assert event.after.content == "only-after"
    await adapter.close()


# 3b. raw reaction dispatch (no cache requirement)


async def test_raw_reaction_dispatch_covers_uncached_messages() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    payload = SimpleNamespace(
        channel_id=CHANNEL,
        message_id=42,
        user_id=777,
        emoji="🎯",
        message_author_id=4242,
    )
    await client.on_raw_reaction_add(payload)  # type: ignore[attr-defined]
    await drain()
    event = box[0]
    assert isinstance(event, RawReactionAddedEvent)
    assert event.message_id == 42
    assert event.user_id == 777
    assert event.emoji == "🎯"
    assert event.message_author_id == 4242
    assert event.channel_id == CHANNEL
    await adapter.close()


async def test_cached_reaction_delivers_raw_then_rich() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    raw = SimpleNamespace(
        channel_id=CHANNEL,
        message_id=5,
        user_id=777,
        emoji="🎲",
        message_author_id=None,
    )
    message = lib_message(5, "$wa")
    reaction, user = lib_reaction(message, "🎲")
    # The fork dispatches raw first, then the cached rich event.
    await client.on_raw_reaction_add(raw)  # type: ignore[attr-defined]
    await client.on_reaction_add(reaction, user)  # type: ignore[attr-defined]
    await drain()
    assert [type(e).__name__ for e in box] == [
        "RawReactionAddedEvent",
        "ReactionAddedEvent",
    ]
    await adapter.close()


# 4. account isolation


async def test_account_isolation() -> None:
    client_a = EventFakeClient()
    client_b = EventFakeClient()
    adapter_a = DiscordAdapter(
        account_id=ACCOUNT,
        credentials=FakeCredentials(),
        client=client_a,  # type: ignore[arg-type]
        jitter=lambda low, high: 0.0,
        sleep=_no_sleep,
    )
    adapter_b = DiscordAdapter(
        account_id=OTHER,
        credentials=FakeCredentials(),
        client=client_b,  # type: ignore[arg-type]
        jitter=lambda low, high: 0.0,
        sleep=_no_sleep,
    )
    box_a: list[Any] = []
    box_b: list[Any] = []
    adapter_a.subscribe(box_a.append)
    adapter_b.subscribe(box_b.append)
    await adapter_a.start()
    await adapter_b.start()
    await client_a.on_message(lib_message(1))  # type: ignore[attr-defined]
    await client_b.on_message(lib_message(2))  # type: ignore[attr-defined]
    await drain()
    assert [e.message.id for e in box_a] == [1]
    assert [e.message.id for e in box_b] == [2]
    assert all(e.account_id == ACCOUNT for e in box_a)
    assert all(e.account_id == OTHER for e in box_b)
    await adapter_a.close()
    await adapter_b.close()


# 5. multiple subscribers + 6. consumer exception isolation


async def test_multiple_subscribers_and_exception_isolation() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    good_a: list[Any] = []
    good_b: list[Any] = []

    async def failing(event: Any) -> None:
        raise RuntimeError("consumer bug")

    adapter.subscribe(failing)
    adapter.subscribe(good_a.append)
    adapter.subscribe(good_b.append)
    await adapter.start()
    for i in range(3):
        await client.on_message(lib_message(i))  # type: ignore[attr-defined]
    await drain(10)
    assert len(good_a) == 3  # failing consumer did not block others
    assert len(good_b) == 3
    await adapter.close()


# 7. backpressure: drop-oldest with a slow consumer


async def test_backpressure_drops_oldest_for_slow_consumer() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client, queue_size=2)
    slow: list[Any] = []
    release = asyncio.Event()

    async def slow_consumer(event: Any) -> None:
        await release.wait()
        slow.append(event)

    fast: list[Any] = []
    adapter.subscribe(slow_consumer)
    adapter.subscribe(fast.append)
    await adapter.start()
    for i in range(4):
        await client.on_message(lib_message(i))  # type: ignore[attr-defined]
        await drain(2)  # let the fast pump keep pace between events
    await drain(10)
    assert len(fast) == 4  # fast subscriber unaffected
    release.set()
    await drain(10)
    # The slow pump consumed event 0 before blocking; its bounded queue
    # (size 2) then kept the newest pair, dropping event 1 (drop-oldest).
    assert [e.message.id for e in slow] == [0, 2, 3]
    assert adapter._events.dropped == 1  # noqa: SLF001
    await adapter.close()


# 8. cancellation and shutdown stop delivery


async def test_close_stops_event_delivery_and_releases_tasks() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    await client.on_message(lib_message(1))  # type: ignore[attr-defined]
    await drain()
    assert len(box) == 1
    await adapter.close()
    assert client.closed is True
    pumps = [t for t in asyncio.all_tasks() if "discord-" in t.get_name()]
    assert pumps == []
    # A straggler library callback after close finds no subscribers: the
    # dispatcher was stopped, so nothing more is delivered.
    await client.on_message(lib_message(2))  # type: ignore[attr-defined]
    await drain()
    assert len(box) == 1


async def test_unsubscribe_stops_one_consumer_only() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box_a: list[Any] = []
    box_b: list[Any] = []
    unsubscribe = adapter.subscribe(box_a.append)
    adapter.subscribe(box_b.append)
    await adapter.start()
    await client.on_message(lib_message(1))  # type: ignore[attr-defined]
    await drain()
    assert len(box_a) == 1 and len(box_b) == 1
    unsubscribe()
    await client.on_message(lib_message(2))  # type: ignore[attr-defined]
    await drain()
    assert len(box_a) == 1 and len(box_b) == 2
    await adapter.close()


# 9. reconnection does not duplicate handlers or dispatch


async def test_reconnect_does_not_duplicate() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    await adapter.start()  # idempotent: must not re-register bridges
    assert sorted(client.registered) == [
        "on_message",
        "on_raw_message_edit",
        "on_raw_reaction_add",
        "on_reaction_add",
    ]
    # Simulate the library reconnecting internally: same handlers still attached.
    await client.on_message(lib_message(1))  # type: ignore[attr-defined]
    await drain()
    assert len(box) == 1
    await adapter.close()


# 10. registration before start (no startup loss window)


async def test_subscribers_registered_before_start_receive_events() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    box: list[Any] = []
    adapter.subscribe(box.append)  # before start()
    await adapter.start()
    assert sorted(client.registered) == [
        "on_message",
        "on_raw_message_edit",
        "on_raw_reaction_add",
        "on_reaction_add",
    ]
    await client.on_message(lib_message(1))  # type: ignore[attr-defined]
    await drain()
    assert len(box) == 1
    await adapter.close()


# 11. mock adapter compatibility with the framework contract


def test_mock_adapter_satisfies_platform_adapter_contract() -> None:
    from autoclaimer.adapters import PlatformAdapter
    from autoclaimer.mock_adapter import MockAdapter

    assert isinstance(MockAdapter(), PlatformAdapter)
    assert isinstance(make_adapter(), PlatformAdapter)


async def test_rate_limited_adapter_passes_subscriptions_through() -> None:
    from autoclaimer.mock_adapter import MockAdapter
    from autoclaimer.ratelimit import RateLimiter
    from autoclaimer.workers import RateLimitedAdapter

    inner = MockAdapter()
    wrapped = RateLimitedAdapter(inner, RateLimiter(), ACCOUNT)
    box: list[Any] = []
    wrapped.subscribe(box.append)
    await inner.emit(
        MessageCreatedEvent(
            account_id=ACCOUNT,
            received_at=datetime.now(UTC),
            message=ChannelMessage(
                id=1, channel_id=2, author_id=3, content="x", has_components=False
            ),
        )
    )
    await drain()
    assert len(box) == 1
    await inner.close()


# 12. regression: the runtime never calls history()


async def test_runtime_workflow_never_calls_history() -> None:
    client = EventFakeClient()
    adapter = make_adapter(client)
    assert not hasattr(adapter, "fetch_recent_messages")
    assert not hasattr(DiscordAdapter, "fetch_recent_messages")
    box: list[Any] = []
    adapter.subscribe(box.append)
    await adapter.start()
    await client.on_message(lib_message(1))  # type: ignore[attr-defined]
    reaction, user = lib_reaction(lib_message(2), "🎲")
    await client.on_reaction_add(reaction, user)  # type: ignore[attr-defined]
    await client.on_raw_message_edit(  # type: ignore[attr-defined]
        raw_edit_payload(cached=lib_message(3, "a"), after=lib_message(3, "b"))
    )
    await drain()
    assert len(box) == 3
    await adapter.close()
    # The whole event path ran without any history access; the fake client
    # has no history attribute at all, so any call would raise AttributeError.
