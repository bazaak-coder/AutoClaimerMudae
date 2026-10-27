"""Batch 5 regression tests (dispatcher/limiter hardening)."""

import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import pytest

from autoclaimer.adapters import EventDispatcher

ACCOUNT = 271485504361996288
OTHER = 380028190142042112


def make_event(account_id: int = ACCOUNT) -> Any:
    from autoclaimer.adapters import ChannelMessage, MessageCreatedEvent

    return MessageCreatedEvent(
        account_id=account_id,
        received_at=datetime.now(UTC),
        message=ChannelMessage(id=1, channel_id=2, author_id=3, content="x", has_components=False),
    )


# E1: shutdown cannot hang on a cancellation-suppressing consumer


async def test_stop_is_bounded_against_cancellation_suppressing_consumer() -> None:
    dispatcher = EventDispatcher(queue_size=4, name="hard")
    release = asyncio.Event()

    async def stubborn(event: Any) -> None:
        try:
            await release.wait()
        except asyncio.CancelledError:
            await asyncio.sleep(0)  # legally suppresses the first cancel
            await release.wait()

    dispatcher.subscribe(stubborn)
    await dispatcher.dispatch(make_event())
    await asyncio.sleep(0.01)  # pump is inside the consumer
    try:
        await asyncio.wait_for(dispatcher.stop(pump_timeout=0.1), timeout=2.0)
    finally:
        release.set()
    # stop() returned within bounds; the abandoned pump ends after release.


async def test_stop_from_inside_consumer_is_safe() -> None:
    mock_like = EventDispatcher(queue_size=4, name="selfstop")
    completed: list[Any] = []

    async def suicidal(event: Any) -> None:
        # Closing the dispatcher from within a consumer (e.g. an adapter
        # close during shutdown) must not self-deadlock or RuntimeError.
        await mock_like.stop(pump_timeout=0.1)
        completed.append(event)

    async def other(event: Any) -> None:
        completed.append(event)

    mock_like.subscribe(suicidal)
    mock_like.subscribe(other)
    await mock_like.dispatch(make_event())
    await asyncio.sleep(0.05)
    # The calling consumer completed (no self-await RuntimeError); the
    # other subscriber was shut down by design (stop == shutdown all).
    assert len(completed) == 1
    assert not mock_like._subscribers  # noqa: SLF001 - fully stopped


# E2: drop attribution per subscriber


async def test_drop_log_attributes_the_slow_subscriber(caplog: Any) -> None:
    import logging

    logging.getLogger("autoclaimer").propagate = True
    dispatcher = EventDispatcher(queue_size=1, name="attr")

    async def slow(event: Any) -> None:
        await asyncio.Event().wait()  # never processes

    async def fast(event: Any) -> None:
        return None

    dispatcher.subscribe(slow)
    dispatcher.subscribe(fast)
    with caplog.at_level("WARNING", logger="autoclaimer.adapters"):
        for _ in range(3):
            await dispatcher.dispatch(make_event())
        await asyncio.sleep(0.02)
    warnings = [r for r in caplog.records if "dropped oldest" in r.message]
    assert warnings, "drop was not logged"
    assert any(getattr(r, "scope", None) == "slow" for r in warnings)
    await dispatcher.stop(pump_timeout=0.2)


# F1: naive `until` rejected before poisoning state


async def test_naive_until_rejected_and_limiter_still_usable() -> None:
    from autoclaimer.ratelimit import RateLimiter, account_scope

    limiter = RateLimiter()
    naive = datetime(2026, 10, 27, 12, 0, 0)  # no tzinfo
    with pytest.raises(ValueError, match="timezone-aware"):
        await limiter.record_cooldown(account_scope(ACCOUNT), until=naive, server_directed=True)
    assert limiter.status().cooldowns == ()  # state not poisoned
    # A valid cooldown still works afterwards.
    await limiter.record_cooldown(
        account_scope(ACCOUNT),
        until=datetime.now(UTC) + timedelta(seconds=5),
        server_directed=True,
    )
    assert len(limiter.status().cooldowns) == 1


# F2: expired in-memory cooldown entries are pruned


async def test_expired_cooldowns_are_pruned() -> None:
    from autoclaimer.ratelimit import RateLimiter, account_scope

    class FakeWall:
        def __init__(self) -> None:
            self.now = datetime(2026, 10, 27, 12, 0, 0, tzinfo=UTC)

        def __call__(self) -> datetime:
            return self.now

    wall = FakeWall()
    limiter = RateLimiter(wall=wall)
    await limiter.record_cooldown(
        account_scope(ACCOUNT),
        until=wall() + timedelta(seconds=10),
        server_directed=True,
    )
    assert len(limiter._cooldowns) == 1  # noqa: SLF001
    wall.now += timedelta(seconds=11)
    limiter._active_deadline(account_scope(ACCOUNT))  # noqa: SLF001
    assert account_scope(ACCOUNT) not in limiter._cooldowns  # noqa: SLF001


# F3: cooldown recorded during permit wait still gates the operation


async def test_cooldown_recorded_during_permit_wait_gates_operation() -> None:
    from autoclaimer.ratelimit import EffectivePolicy, RateLimiter, account_scope

    limiter = RateLimiter(default_policy=EffectivePolicy(max_concurrency=1))

    async def first_op() -> None:
        async with limiter.operation(account_id=ACCOUNT):
            await asyncio.Event().wait()  # hold the only permit

    async def second_op() -> None:
        async with limiter.operation(account_id=ACCOUNT):
            return "ran"

    first = asyncio.create_task(first_op())
    await asyncio.sleep(0.02)  # first holds the permit
    second = asyncio.create_task(second_op())
    await asyncio.sleep(0.02)  # second is queued on the permit
    # A server cooldown lands while the second op waits for the permit.
    await limiter.record_cooldown(
        account_scope(ACCOUNT),
        until=datetime.now(UTC) + timedelta(seconds=0.15),
        server_directed=True,
    )
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    started = asyncio.get_running_loop().time()
    result = await asyncio.wait_for(second, timeout=3.0)
    elapsed = asyncio.get_running_loop().time() - started
    assert result == "ran"
    assert elapsed >= 0.1  # gated until the cooldown expired (0.15s deadline)
