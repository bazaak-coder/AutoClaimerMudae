"""Deterministic failure simulations (milestone 6; audit test matrix).

Rows implemented here (pre-handler-implementable):

- T1: crash window A — process dies after the durable claim, before the
  handler ran. Restart recovers the row (running→pending, attempts
  preserved) and executes exactly one platform send.
- T2: crash window B — the mutation was sent, the outcome was never
  recorded. Restart re-executes (at-least-once): the duplicate side
  effect is PINNED as the documented contract; the future Mudae layer
  reconciles from the duplicate reply.
- T4: expired persisted cooldown — restart loads nothing active; the
  first operation runs without waiting.
- T7: dispatcher events queued at account-fatal eviction are reported
  lost through the subscriber's loss callback (milestone 5 wiring).
- T11: back-dated schedules — a past deadline means "due now", never a
  negative wait.

Already covered elsewhere: T3 (F1 fix test), T5 (F3 tests), T6
(milestone 5 tests). T8/T9/T10 require the Mudae task layer
(milestone 7) and remain documented there.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.adapters import AccountFatalError, MessageCreatedEvent
from autoclaimer.adapters import ChannelMessage as _ChannelMessage
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter, account_scope
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager, WorkerState

ACCOUNT = 271485504361996288
BASE_TIME = datetime(2026, 10, 29, 12, 0, 0, tzinfo=UTC)

POLL_INTERVAL = 0.01
WAIT_TIMEOUT = 5.0


def event(index: int) -> MessageCreatedEvent:
    return MessageCreatedEvent(
        account_id=ACCOUNT,
        received_at=BASE_TIME,
        message=_ChannelMessage(
            id=index, channel_id=7, author_id=2, content=f"msg {index}", created_at=None
        ),
    )


async def build_manager(
    tmp_path: Path,
    db: Path,
    *,
    adapter: MockAdapter,
    handler: Any,
) -> WorkerManager:
    app = parse_app_config(
        {
            "version": 1,
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 5},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "tasks": {"probe": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["probe"]}]}, app
    )
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="probe",
            description="Failure-simulation probe.",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    store = TaskStateStore(db)
    await store.open()
    limiter = RateLimiter(store=store)
    return WorkerManager(
        registry=registry,
        store=store,
        limiter=limiter,
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: adapter,
        idle_interval_seconds=0.02,
    )


async def wait_until(check: Any, description: str) -> None:
    deadline = asyncio.get_running_loop().time() + WAIT_TIMEOUT
    while asyncio.get_running_loop().time() < deadline:
        if await check():
            return
        await asyncio.sleep(POLL_INTERVAL)
    pytest.fail(f"condition never met: {description}")


async def _status(store: TaskStateStore, task_id: str) -> TaskInstanceState:
    return (await store.get_instance_by_ids(ACCOUNT, task_id)).status


# --- T1: crash window A (after claim, before handler) ---


async def test_t1_crash_after_claim_before_handler(tmp_path: Path) -> None:
    adapter = MockAdapter()

    async def sender(context: TaskContext) -> TaskOutcome:
        await context.adapter.send_message(7, "$wa")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    db = tmp_path / "state.db"
    store = TaskStateStore(db)
    await store.open()
    record = await store.create_instance(ACCOUNT, "probe")
    await store.claim(record.id)  # durable claim committed; process "dies" now
    await store.close()

    manager = await build_manager(tmp_path, db, adapter=adapter, handler=sender)

    async def succeeded() -> bool:
        return (
            await _status(manager._store, "probe") is TaskInstanceState.SUCCEEDED  # noqa: SLF001
        )

    async with manager:
        await wait_until(succeeded, "recovered instance succeeds after restart")

    final = await manager._store.get_instance_by_ids(ACCOUNT, "probe")  # noqa: SLF001
    assert final.attempt_count == 2  # the crashed claim's attempt survived
    assert len(adapter.sent_messages) == 1  # exactly one platform send total
    await manager._store.close()  # noqa: SLF001


# --- T2: crash window B (mutation sent, outcome unrecorded) ---


async def test_t2_crash_after_send_before_record(tmp_path: Path) -> None:
    """The at-least-once contract, pinned: the pre-crash mutation stands
    and the restart re-executes it — a duplicate side effect the future
    Mudae layer reconciles from the duplicate reply."""
    adapter = MockAdapter()

    async def sender(context: TaskContext) -> TaskOutcome:
        await context.adapter.send_message(7, "$wa")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    db = tmp_path / "state.db"
    store = TaskStateStore(db)
    await store.open()
    record = await store.create_instance(ACCOUNT, "probe")
    await store.claim(record.id)
    await adapter.send_message(7, "$wa")  # the handler's platform mutation…
    await store.close()  # …then the process dies before recording anything

    manager = await build_manager(tmp_path, db, adapter=adapter, handler=sender)

    async def succeeded() -> bool:
        return (
            await _status(manager._store, "probe") is TaskInstanceState.SUCCEEDED  # noqa: SLF001
        )

    async with manager:
        await wait_until(succeeded, "uncertain instance re-executes and succeeds")

    final = await manager._store.get_instance_by_ids(ACCOUNT, "probe")  # noqa: SLF001
    assert final.attempt_count == 2
    assert len(adapter.sent_messages) == 2  # duplicate send: documented at-least-once
    await manager._store.close()  # noqa: SLF001


# --- T4: expired persisted cooldown does not block after restart ---


async def test_t4_expired_cooldown_no_wait_after_restart(tmp_path: Path) -> None:
    clock = {"now": BASE_TIME}
    db = tmp_path / "state.db"
    store = TaskStateStore(db, clock=lambda: clock["now"])
    await store.open()
    limiter = RateLimiter(store=store, wall=lambda: clock["now"])
    await limiter.record_cooldown(
        account_scope(ACCOUNT), until=BASE_TIME + timedelta(minutes=10), server_directed=True
    )
    await store.close()

    # Restart AFTER the deadline passed: nothing loads, nothing waits.
    clock["now"] = BASE_TIME + timedelta(minutes=11)
    store2 = TaskStateStore(db, clock=lambda: clock["now"])
    await store2.open()
    sleeps: list[float] = []

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)

    limiter2 = RateLimiter(store=store2, wall=lambda: clock["now"], sleep=fake_sleep)
    await limiter2.load_persisted_cooldowns()
    assert limiter2.status().cooldowns == ()  # expired -> not adopted
    await limiter2.wait_scopes(ACCOUNT)
    assert sleeps == []  # no residual wait for a deadline already past
    await store2.close()


# --- T7: queued events are reported lost at account-fatal eviction ---


async def test_t7_account_fatal_reports_queued_events_lost(tmp_path: Path) -> None:
    adapter = MockAdapter()
    lost: list[int] = []

    async def parked(consumer_event: Any) -> None:
        await asyncio.Event().wait()  # subscriber never drains

    adapter.subscribe(parked, on_events_lost=lost.append)
    await adapter.emit(event(1))
    await adapter.emit(event(2))
    assert lost == []  # queued, not lost

    async def fatal(context: TaskContext) -> TaskOutcome:
        raise AccountFatalError("authentication failed: simulated dead token")

    manager = await build_manager(tmp_path, tmp_path / "state.db", adapter=adapter, handler=fatal)

    async def account_stopped() -> bool:
        return await _account_stopped(manager)

    async with manager:
        await wait_until(account_stopped, "account worker stopped after fatal error")
    # Arithmetic (updated by the PR #3 review fix): the pump holds
    # event(1) in-flight inside the stuck consumer and event(2) sits
    # queued — BOTH are undelivered and both are counted exactly once
    # at eviction (in-flight + queued are disjoint sets).
    assert lost == [2]
    await manager._store.close()  # noqa: SLF001


async def _account_stopped(manager: WorkerManager) -> bool:
    status = await manager.status()
    return bool(status.workers) and status.workers[0].state is WorkerState.STOPPED


# --- T11: back-dated schedules are due immediately ---


class FakeClockFixed:
    def __init__(self, now: datetime) -> None:
        self.now = now

    def __call__(self) -> datetime:
        return self.now


async def test_t11_backdated_outcome_deadline_is_due_now(tmp_path: Path) -> None:
    """T11 (re-homed after schedule_attempt removal): a back-dated
    LEARNED deadline means due-now — the worker clamps expired retry_at
    values to the wall clock instead of computing a negative wait."""

    clock = FakeClockFixed(BASE_TIME)
    store = TaskStateStore(tmp_path / "state.db", clock=clock)
    await store.open()
    try:
        record = await store.create_instance(ACCOUNT, "probe")
        claimed = await store.claim(record.id)
        from autoclaimer.registry import TaskOutcome, TaskStatus

        backdated = BASE_TIME - timedelta(hours=1)
        await store.record_failure(
            claimed.id,
            TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="learned", retry_at=backdated),
            retry_at=backdated,
        )
        due = await store.due_instances()
        assert claimed.id in [r.id for r in due]  # past deadline = due now
    finally:
        await store.close()
