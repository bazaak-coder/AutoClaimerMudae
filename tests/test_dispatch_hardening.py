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

    async def first_op() -> str:
        async with limiter.operation(account_id=ACCOUNT):
            await asyncio.Event().wait()  # hold the only permit
            return "held"

    async def second_op() -> str:
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


# Milestone 5: event-loss recovery — loss signaling and queue pressure


async def test_on_events_lost_fires_on_overflow_with_cumulative_count() -> None:
    """A subscriber that must not miss actionable events registers a
    loss callback: drop-oldest overflow invokes it with the CUMULATIVE
    dropped count (delta-safe for consumers that diff)."""
    dispatcher = EventDispatcher(queue_size=1, name="loss")
    lost: list[int] = []

    async def slow(event: Any) -> None:
        await asyncio.Event().wait()  # never drains

    dispatcher.subscribe(slow, on_events_lost=lost.append)
    for _ in range(3):
        await dispatcher.dispatch(make_event())
    await asyncio.sleep(0.02)
    assert lost == [1, 2]  # two oldest dropped while queue holds 1
    await dispatcher.stop(pump_timeout=0.2)


async def test_loss_callback_exceptions_are_isolated() -> None:
    """A broken loss callback must not break dispatch (isolation
    boundary per agents.md section 17.4) and must not hide the loss."""
    dispatcher = EventDispatcher(queue_size=1, name="boomloss")

    async def slow(event: Any) -> None:
        await asyncio.Event().wait()

    def broken(count: int) -> None:
        raise RuntimeError("callback bug")

    dispatcher.subscribe(slow, on_events_lost=broken)
    for _ in range(2):
        await dispatcher.dispatch(make_event())  # must not raise
    await asyncio.sleep(0.02)
    assert dispatcher.dropped == 1
    await dispatcher.stop(pump_timeout=0.2)


async def test_stats_expose_queue_pressure_and_drops() -> None:
    dispatcher = EventDispatcher(queue_size=2, name="stats")

    async def slow(event: Any) -> None:
        await asyncio.Event().wait()

    async def idle(event: Any) -> None:
        return None

    dispatcher.subscribe(slow)
    dispatcher.subscribe(idle)
    for _ in range(4):
        await dispatcher.dispatch(make_event())
        await asyncio.sleep(0)  # let the draining subscriber keep up
    stats = dispatcher.stats()
    assert stats.subscriber_count == 2
    # Arithmetic: of 4 events, the slow subscriber consumed 1 (then hung
    # inside that consumer call), 2 sit queued, and 1 was dropped.
    assert stats.total_dropped == 1
    assert sum(stats.subscriber_dropped.values()) == 1
    assert max(stats.queue_depths.values()) == 2  # slow subscriber's backlog
    assert min(stats.queue_depths.values()) == 0  # idle subscriber drained
    await dispatcher.stop(pump_timeout=0.2)


async def test_stop_reports_undelivered_events_as_lost() -> None:
    """N4 observability: events still queued at stop() (e.g. adapter
    eviction) are reported to the loss callback instead of vanishing
    silently. No yields between dispatch and stop, so both events are
    still queued (the pump never ran)."""
    dispatcher = EventDispatcher(queue_size=8, name="stoploss")
    lost: list[int] = []

    async def parked(event: Any) -> None:
        await asyncio.Event().wait()  # would never drain

    dispatcher.subscribe(parked, on_events_lost=lost.append)
    await dispatcher.dispatch(make_event())
    await dispatcher.dispatch(make_event())
    assert lost == []  # queued, not dropped
    await dispatcher.stop(pump_timeout=0.2)
    assert lost == [2]  # both undelivered events reported at shutdown
    assert dispatcher.dropped == 2  # honest global accounting


# PR #3 review fix: interrupted in-flight delivery is a loss


async def test_stop_reports_in_flight_event_interrupted_delivery() -> None:
    """Review finding 1: an event already dequeued by the pump and
    blocked inside its consumer is invisible to queue.qsize() — stop()
    must still count and report it (no silent hole in the loss
    contract), without double-counting (in-flight and queued are
    disjoint sets)."""
    dispatcher = EventDispatcher(queue_size=4, name="inflight")
    lost: list[int] = []
    started = asyncio.Event()

    async def stuck_mid_delivery(event: Any) -> None:
        started.set()
        await asyncio.Event().wait()  # blocks INSIDE the consumer

    dispatcher.subscribe(stuck_mid_delivery, on_events_lost=lost.append)
    await dispatcher.dispatch(make_event())
    await started.wait()  # the pump now holds the event in the consumer
    assert sum(dispatcher.stats().queue_depths.values()) == 0  # queue empty
    await dispatcher.stop(pump_timeout=0.2)
    assert lost == [1]  # the interrupted delivery IS a loss
    assert dispatcher.dropped == 1


# PR #3 follow-up review: self-stop must not report its own in-flight event


async def test_self_stop_does_not_report_own_in_flight_as_lost() -> None:
    """stop() is documented safe to call from inside a consumer; that
    pump is deliberately NOT cancelled — it resumes and finishes the
    delivery. The completed event must not be counted or reported as a
    loss (false loss would trigger needless reconciliation)."""
    dispatcher = EventDispatcher(queue_size=4, name="selfloss")
    lost: list[int] = []
    delivered: list[Any] = []

    async def self_stopping(event: Any) -> None:
        await dispatcher.stop(pump_timeout=0.2)
        delivered.append(event)  # completes AFTER stop() returned

    dispatcher.subscribe(self_stopping, on_events_lost=lost.append)
    await dispatcher.dispatch(make_event())
    await asyncio.sleep(0.05)  # pump entered the consumer; stop() ran inside it

    assert len(delivered) == 1  # the event WAS delivered
    assert lost == []  # therefore not a loss
    assert dispatcher.dropped == 0


# PR #3 third-round review: self-stop must not keep draining the queue


async def test_self_stop_finishes_current_but_never_drains_queue() -> None:
    """The surviving (self-stopping) pump may finish its CURRENT
    consumer call — and nothing more: events queued behind it are
    reported lost exactly once and are never delivered after stop(); the
    pump exits instead of resuming its loop."""
    dispatcher = EventDispatcher(queue_size=4, name="selfdrain")
    lost: list[int] = []
    delivered: list[Any] = []

    async def self_stopping(event: Any) -> None:
        await dispatcher.stop(pump_timeout=0.2)
        delivered.append(event)  # finishes AFTER stop() returned

    dispatcher.subscribe(self_stopping, on_events_lost=lost.append)
    pumps = [s.pump for s in dispatcher._subscribers.values()]  # noqa: SLF001
    await dispatcher.dispatch(make_event())  # becomes the in-flight event
    await dispatcher.dispatch(make_event())  # queued behind
    await dispatcher.dispatch(make_event())  # queued behind
    await asyncio.sleep(0.05)  # pump entered the consumer; stop() ran inside

    assert len(delivered) == 1  # current event finished, not reported lost
    await asyncio.sleep(0.05)  # the pump loop resumes: it must EXIT
    assert len(delivered) == 1  # queued events were NOT delivered after stop
    assert lost == [2]  # both queued events reported lost exactly once
    assert dispatcher.dropped == 2
    assert pumps[0] is not None and pumps[0].done()  # pump exited


# Consolidated review (#1 variant, #2, #3): loss lifecycle completeness


async def test_self_stop_with_blocked_consumer_release_after_stop() -> None:
    """Reviewer's exact sequence: the self-stopping consumer blocks on
    the first event with another queued behind it, stop() runs, the
    consumer is released afterwards."""
    dispatcher = EventDispatcher(queue_size=4, name="blockedrelease")
    release = asyncio.Event()
    lost: list[int] = []
    delivered: list[Any] = []

    async def blocked(event: Any) -> None:
        await dispatcher.stop(pump_timeout=0.2)
        await release.wait()  # blocks AFTER stop() returned
        delivered.append(event)

    dispatcher.subscribe(blocked, on_events_lost=lost.append)
    pumps = [s.pump for s in dispatcher._subscribers.values()]  # noqa: SLF001
    await dispatcher.dispatch(make_event())  # in-flight
    await dispatcher.dispatch(make_event())  # queued behind
    await asyncio.sleep(0.05)  # consumer entered; stop() ran inside it
    assert lost == [1]  # queued event: lost exactly once
    release.set()
    await asyncio.sleep(0.05)
    assert len(delivered) == 1  # first event finished after release; not a loss
    assert lost == [1]  # queued event never delivered
    assert pumps[0] is not None and pumps[0].done()


async def test_consumer_exception_counts_loss_and_continues() -> None:
    """Review #2: a consumer invocation that raises has NOT processed
    its event — that is a loss for reconciliation purposes (counted +
    reported), and the pump must continue with later events."""
    dispatcher = EventDispatcher(queue_size=4, name="boomcount")
    lost: list[int] = []
    processed: list[Any] = []

    async def flaky(event: Any) -> None:
        processed.append(event)
        if len(processed) == 1:
            raise RuntimeError("consumer bug")

    dispatcher.subscribe(flaky, on_events_lost=lost.append)
    await dispatcher.dispatch(make_event())
    await asyncio.sleep(0.02)
    assert lost == [1]  # the failed invocation's event: reported lost
    await dispatcher.dispatch(make_event())
    await asyncio.sleep(0.02)
    assert len(processed) == 2  # the pump survived and delivered the next
    assert lost == [1]  # no extra losses
    assert dispatcher.dropped == 1
    await dispatcher.stop(pump_timeout=0.2)


async def test_unsubscribe_accounts_queued_and_inflight_losses() -> None:
    """Review #3 (external unsubscribe): queued + in-flight events are
    abandoned — counted and reported exactly once, pump cancelled."""
    dispatcher = EventDispatcher(queue_size=4, name="unsub")
    lost: list[int] = []
    started = asyncio.Event()

    async def stuck(event: Any) -> None:
        started.set()
        await asyncio.Event().wait()

    unsubscribe = dispatcher.subscribe(stuck, on_events_lost=lost.append)
    pumps = [s.pump for s in dispatcher._subscribers.values()]  # noqa: SLF001
    await dispatcher.dispatch(make_event())  # becomes in-flight
    await started.wait()
    await dispatcher.dispatch(make_event())  # queued behind
    unsubscribe()
    assert lost == [2]  # in-flight (interrupted) + queued: both lost
    assert dispatcher.dropped == 2
    await asyncio.sleep(0.02)
    assert pumps[0] is not None and pumps[0].done()  # cancelled, ended


async def test_self_unsubscribe_finishes_current_accounts_queue_only() -> None:
    """Review #3 (self-unsubscribe): no self-cancellation; the current
    callback finishes (not a loss), queued events are accounted, and the
    pump exits instead of draining."""
    dispatcher = EventDispatcher(queue_size=4, name="selfunsub")
    lost: list[int] = []
    delivered: list[Any] = []
    unsubscribe_holder: list[Any] = []

    async def one_shot(event: Any) -> None:
        unsubscribe_holder[0]()  # its own unsubscribe, mid-callback
        delivered.append(event)

    unsubscribe_holder.append(dispatcher.subscribe(one_shot, on_events_lost=lost.append))
    pumps = [s.pump for s in dispatcher._subscribers.values()]  # noqa: SLF001
    await dispatcher.dispatch(make_event())  # in-flight
    await dispatcher.dispatch(make_event())  # queued at unsubscribe time
    await asyncio.sleep(0.05)
    assert len(delivered) == 1  # current callback finished
    assert lost == [1]  # ONLY the queued event accounted as lost
    assert dispatcher.dropped == 1
    await dispatcher.dispatch(make_event())  # gone subscription: no crash
    await asyncio.sleep(0.02)
    assert len(delivered) == 1  # nothing further delivered
    assert pumps[0] is not None and pumps[0].done()  # pump exited


# Review follow-up: subscribing after stop must be explicit


async def test_subscribe_after_stop_is_rejected() -> None:
    """stop() is terminal: a subscription accepted afterwards would have
    an already-exited pump and silently drop every future dispatch.
    The API rejects it loudly instead."""
    dispatcher = EventDispatcher(queue_size=2, name="poststop")

    async def consumer(event: Any) -> None:
        return None

    await dispatcher.stop()
    with pytest.raises(RuntimeError, match="stopped"):
        dispatcher.subscribe(consumer)
