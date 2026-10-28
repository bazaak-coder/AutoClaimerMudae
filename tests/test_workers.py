"""Tests for the worker manager: concurrency, ordering, controls, recovery."""

import asyncio
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.adapters import CooldownError, RecoverableAdapterError
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import (
    TaskInstanceRecord,
    TaskInstanceState,
    TaskStateStore,
)
from autoclaimer.ratelimit import EffectivePolicy, RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import RateLimitedAdapter, WorkerManager, WorkerState

ACCOUNT = 271485504361996288
OTHER = 380028190142042112

POLL_INTERVAL = 0.01
WAIT_TIMEOUT = 3.0


@dataclass
class Harness:
    manager: WorkerManager
    store: TaskStateStore
    limiter: RateLimiter
    adapters: dict[int, MockAdapter] = field(default_factory=dict)
    calls: list[tuple[int, str]] = field(default_factory=list)
    in_flight: list[int] = field(default_factory=lambda: [0])
    max_in_flight: list[int] = field(default_factory=lambda: [0])
    block_events: dict[tuple[int, str], asyncio.Event] = field(default_factory=dict)
    counters: dict[tuple[int, str], int] = field(default_factory=dict)


async def build_harness(
    tmp_path: Path,
    *,
    tasks_raw: dict[str, Any],
    accounts_raw: list[dict[str, Any]],
    max_concurrent: int = 4,
    limiter_policy: EffectivePolicy | None = None,
    shutdown_timeout: int = 15,
) -> Harness:
    app = parse_app_config(
        {
            "version": 1,
            "settings": {
                "max_concurrent_accounts": max_concurrent,
                "shutdown_timeout_seconds": shutdown_timeout,
            },
            "ratelimit": {"backoff_base_seconds": 0.05, "backoff_cap_seconds": 0.05},
            "tasks": tasks_raw,
        }
    )
    accounts = parse_accounts_config({"version": 1, "accounts": accounts_raw}, app)

    calls: list[tuple[int, str]] = []
    in_flight = [0]
    max_in_flight = [0]
    counters: dict[tuple[int, str], int] = {}
    block_events: dict[tuple[int, str], asyncio.Event] = {}
    adapters = {profile.id: MockAdapter() for profile in accounts.accounts}

    async def run_behavior(context: TaskContext) -> TaskOutcome:
        behavior = context.config.get("behavior", "noop")
        key = (context.account_id, context.task_id)
        if behavior == "slow":
            await asyncio.sleep(0.05)
            return TaskOutcome(TaskStatus.SUCCEEDED)
        if behavior == "block":
            event = block_events.setdefault(key, asyncio.Event())
            await event.wait()
            return TaskOutcome(TaskStatus.SUCCEEDED)
        if behavior == "recoverable_once":
            count = counters.get(key, 0) + 1
            counters[key] = count
            if count == 1:
                return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="transient failure")
            return TaskOutcome(TaskStatus.SUCCEEDED)
        if behavior == "crash":
            raise RuntimeError("handler bug")
        if behavior == "cooldown_once":
            await context.adapter.send_message(1, "claim")
            return TaskOutcome(TaskStatus.SUCCEEDED)
        if behavior == "recoverable_always":
            await context.adapter.add_reaction(1, 2, "x")
            return TaskOutcome(TaskStatus.SUCCEEDED)
        return TaskOutcome(TaskStatus.SUCCEEDED)

    async def handler(context: TaskContext) -> TaskOutcome:
        calls.append((context.account_id, context.task_id))
        in_flight[0] += 1
        max_in_flight[0] = max(max_in_flight[0], in_flight[0])
        try:
            return await run_behavior(context)
        finally:
            in_flight[0] -= 1

    registry = TaskRegistry()
    for task_id in tasks_raw:
        registry.register(
            TaskType(
                id=task_id,
                description=f"Test task {task_id}.",
                validate_config=lambda config: None,
                handler=handler,
            )
        )

    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    limiter = RateLimiter(default_policy=limiter_policy, store=store)
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=limiter,
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: adapters[account_id],
        idle_interval_seconds=0.02,
    )
    return Harness(
        manager=manager,
        store=store,
        limiter=limiter,
        adapters=adapters,
        calls=calls,
        in_flight=in_flight,
        max_in_flight=max_in_flight,
        block_events=block_events,
        counters=counters,
    )


async def wait_for_status(
    store: TaskStateStore, account_id: int, task_id: str, *statuses: TaskInstanceState
) -> TaskInstanceRecord:
    deadline = asyncio.get_running_loop().time() + WAIT_TIMEOUT
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get_instance_by_ids(account_id, task_id)
        if record.status in statuses:
            return record
        await asyncio.sleep(POLL_INTERVAL)
    pytest.fail(f"instance {account_id}/{task_id} never reached {statuses}")


def block_event(h: Harness, account: int, task_id: str) -> asyncio.Event:
    """Pre-create the handler's block event (claim can precede handler start)."""
    return h.block_events.setdefault((account, task_id), asyncio.Event())


def task(behavior: str = "noop", *, priority: int | None = None) -> dict[str, Any]:
    raw: dict[str, Any] = {"parameters": {"behavior": behavior}}
    if priority is not None:
        raw["priority"] = priority
    return raw


# --- concurrency and sequencing ---


async def test_tasks_are_sequential_within_one_account(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"first": task("slow"), "second": task("slow")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["first", "second"]}],
    )
    async with h.manager:
        await wait_for_status(h.store, ACCOUNT, "first", TaskInstanceState.SUCCEEDED)
        await wait_for_status(h.store, ACCOUNT, "second", TaskInstanceState.SUCCEEDED)
    assert h.max_in_flight[0] == 1  # never two ops for one account


async def test_accounts_run_concurrently(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("slow")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}, {"id": OTHER, "tasks": ["job"]}],
    )
    async with h.manager:
        await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
        await wait_for_status(h.store, OTHER, "job", TaskInstanceState.SUCCEEDED)
    assert h.max_in_flight[0] == 2


async def test_global_concurrency_is_bounded(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("slow")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}, {"id": OTHER, "tasks": ["job"]}],
        max_concurrent=1,
    )
    async with h.manager:
        await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
        await wait_for_status(h.store, OTHER, "job", TaskInstanceState.SUCCEEDED)
    assert h.max_in_flight[0] == 1


# --- ordering ---


async def test_priority_ordering_within_account(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={
            "low": task("slow", priority=0),
            "high": task("slow", priority=10),
            "mid": task("slow", priority=5),
        },
        accounts_raw=[{"id": ACCOUNT, "tasks": ["low", "high", "mid"]}],
    )
    async with h.manager:
        for name in ("high", "mid", "low"):
            await wait_for_status(h.store, ACCOUNT, name, TaskInstanceState.SUCCEEDED)
    order = [task_id for _, task_id in h.calls]
    assert order == ["high", "mid", "low"]


async def test_queue_reports_scheduling_order(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={
            "blocker": task("block", priority=10),
            "next": task("noop", priority=5),
            "last": task("noop", priority=1),
        },
        accounts_raw=[{"id": ACCOUNT, "tasks": ["blocker", "next", "last"]}],
    )
    await h.manager.start()
    await wait_for_status(h.store, ACCOUNT, "blocker", TaskInstanceState.RUNNING)
    queued = await h.manager.queue(ACCOUNT)
    assert [r.task_id for r in queued] == ["next", "last"]
    block_event(h, ACCOUNT, "blocker").set()
    await h.manager.stop()
    await wait_for_status(h.store, ACCOUNT, "blocker", TaskInstanceState.SUCCEEDED)


# --- duplicate prevention ---


async def test_wake_storm_does_not_duplicate_execution(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("block")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    await h.manager.start()
    await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.RUNNING)
    for _ in range(5):
        h.manager.wake(ACCOUNT)
        await asyncio.sleep(0)
    block_event(h, ACCOUNT, "job").set()
    await h.manager.stop()
    await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    assert len([1 for c in h.calls if c[1] == "job"]) == 1


# --- account controls ---


async def test_disabling_account_stops_new_work_only(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"first": task("block", priority=10), "second": task("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["first", "second"]}],
    )
    await h.manager.start()
    await wait_for_status(h.store, ACCOUNT, "first", TaskInstanceState.RUNNING)
    h.manager.set_account_enabled(ACCOUNT, False)
    block_event(h, ACCOUNT, "first").set()
    await wait_for_status(h.store, ACCOUNT, "first", TaskInstanceState.SUCCEEDED)
    await asyncio.sleep(0.1)
    assert (await h.store.get_instance_by_ids(ACCOUNT, "second")).status is (
        TaskInstanceState.PENDING
    )  # in-flight finished; new work never started
    await h.manager.stop()


async def test_pause_and_resume_account(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    h.manager.pause_account(ACCOUNT)  # paused before start for determinism
    await h.manager.start()
    await asyncio.sleep(0.1)
    assert (await h.store.get_instance_by_ids(ACCOUNT, "job")).status is (TaskInstanceState.PENDING)
    h.manager.resume_account(ACCOUNT)
    await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    await h.manager.stop()


async def test_global_pause_and_resume(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}, {"id": OTHER, "tasks": ["job"]}],
    )
    h.manager.pause_all()  # paused before start: workers must gate immediately
    await h.manager.start()
    await asyncio.sleep(0.1)
    for account in (ACCOUNT, OTHER):
        assert (await h.store.get_instance_by_ids(account, "job")).status is (
            TaskInstanceState.PENDING
        )
    assert (await h.manager.status()).globally_paused is True
    h.manager.resume_all()
    for account in (ACCOUNT, OTHER):
        await wait_for_status(h.store, account, "job", TaskInstanceState.SUCCEEDED)
    await h.manager.stop()


async def test_status_reports_worker_states(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    await h.manager.start()
    h.manager.pause_account(ACCOUNT)
    status = await h.manager.status()
    assert status.workers[0].state is WorkerState.PAUSED
    assert status.workers[0].account_id == ACCOUNT
    h.manager.resume_account(ACCOUNT)
    await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    assert (await h.manager.status()).workers[0].state is WorkerState.RUNNING
    await h.manager.stop()
    assert (await h.manager.status()).workers[0].state is WorkerState.STOPPED


# --- retries, failure isolation, rate limiting ---


async def test_recoverable_outcome_is_retried(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("recoverable_once")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    async with h.manager:
        await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    record = await h.store.get_instance_by_ids(ACCOUNT, "job")
    assert record.attempt_count == 2


async def test_handler_crash_is_isolated(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"broken": task("crash", priority=10), "healthy": task("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["broken", "healthy"]}],
    )
    async with h.manager:
        await wait_for_status(h.store, ACCOUNT, "broken", TaskInstanceState.FAILED)
        await wait_for_status(h.store, ACCOUNT, "healthy", TaskInstanceState.SUCCEEDED)
    broken = await h.store.get_instance_by_ids(ACCOUNT, "broken")
    assert "RuntimeError" in (broken.last_outcome.message if broken.last_outcome else "")


async def test_platform_operations_go_through_limiter(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("cooldown_once")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
        limiter_policy=EffectivePolicy(retry_limit=1),
    )
    h.adapters[ACCOUNT].script(
        "send_message", CooldownError(0.1)
    )  # first op hits a short server cooldown
    async with h.manager:
        await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    # Proof the operation went through the limiter: it recorded the server
    # cooldown on the operation's route and account scopes (persisted).
    rows = await h.store._run(h.store._fetchall_sync, "SELECT scope FROM scope_cooldowns", ())
    scopes = {row["scope"] for row in rows}
    assert "route:send_message" in scopes
    assert f"account:{ACCOUNT}" in scopes
    assert h.adapters[ACCOUNT].sent_messages  # op retried and succeeded


async def test_retry_limit_exhaustion_fails_permanently(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("recoverable_always")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
        limiter_policy=EffectivePolicy(retry_limit=1, backoff_base_seconds=0.01),
    )
    adapter = h.adapters[ACCOUNT]
    for _ in range(5):  # both attempts (retry_limit=1) hit recoverable errors
        adapter.script("add_reaction", RecoverableAdapterError("down"))
    async with h.manager:
        await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.FAILED)
    record = await h.store.get_instance_by_ids(ACCOUNT, "job")
    assert record.last_outcome is not None
    assert "Retry limit" in record.last_outcome.message


# --- shutdown and restart recovery ---


async def test_graceful_shutdown_while_idle(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    await h.manager.start()
    await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    await h.manager.stop()
    await h.manager.stop()  # idempotent
    assert (await h.manager.status()).workers[0].state is WorkerState.STOPPED


async def test_shutdown_during_uncertain_operation_leaves_running(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("block")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
        shutdown_timeout=1,
    )
    await h.manager.start()
    await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.RUNNING)
    await h.manager.stop(timeout=0.05)  # hard-cancels mid-operation
    record = await h.store.get_instance_by_ids(ACCOUNT, "job")
    assert record.status is TaskInstanceState.RUNNING  # outcome uncertain
    assert record.attempt_count == 1
    await h.store.close()

    # Restart: recovery resets the orphan and (with a healthy task) completes.
    h2 = await build_harness(
        tmp_path,
        tasks_raw={"job": task("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    async with h2.manager:
        await wait_for_status(h2.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    record = await h2.store.get_instance_by_ids(ACCOUNT, "job")
    assert record.attempt_count == 2  # recovered attempt preserved + re-executed


async def test_orphaned_running_rows_recovered_on_start(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("block")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    await h.manager.start()
    await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.RUNNING)
    await h.manager.stop(timeout=0.05)
    orphan = await h.store.get_instance_by_ids(ACCOUNT, "job")
    assert orphan.status is TaskInstanceState.RUNNING
    await h.store.close()

    h2 = await build_harness(
        tmp_path,
        tasks_raw={"job": task("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    async with h2.manager:
        await wait_for_status(h2.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    await h2.store.close()


async def test_manager_is_restartable(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("block")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    event = block_event(h, ACCOUNT, "job")
    await h.manager.start()
    await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.RUNNING)
    await h.manager.stop(timeout=0.05)  # hard-cancel mid-operation
    assert (await h.store.get_instance_by_ids(ACCOUNT, "job")).status is (TaskInstanceState.RUNNING)
    event.set()  # the restarted attempt may proceed immediately
    await h.manager.start()  # same manager instance starts again
    await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    await h.manager.stop()


async def test_enable_replaces_dead_worker_and_resumes_pending_work(
    tmp_path: Path,
) -> None:
    """PR #1 finding 6: re-enabling must replace a registered dead worker."""
    h = await build_harness(
        tmp_path,
        tasks_raw={
            "stuck": task("block", priority=10),  # in-flight when worker dies
            "queued": task("noop"),
        },
        accounts_raw=[{"id": ACCOUNT, "tasks": ["stuck", "queued"]}],
    )
    await h.manager.start()
    await wait_for_status(h.store, ACCOUNT, "stuck", TaskInstanceState.RUNNING)
    old_task = h.manager._workers[ACCOUNT].task  # noqa: SLF001
    assert old_task is not None
    old_task.cancel()  # simulate an unrecoverable worker death
    await asyncio.sleep(0)
    assert old_task.done()
    status = await h.manager.status()
    assert status.workers[0].state is WorkerState.STOPPED  # observable, no auto-restart

    h.manager.set_account_enabled(ACCOUNT, False)
    h.manager.set_account_enabled(ACCOUNT, True)  # revive
    # Poll (no fixed sleep): the detached enable must spawn a replacement.
    deadline = asyncio.get_running_loop().time() + 3.0
    new_task = old_task
    while new_task is old_task and asyncio.get_running_loop().time() < deadline:
        candidate = h.manager._workers[ACCOUNT].task  # noqa: SLF001
        if candidate is not None and candidate is not old_task:
            new_task = candidate
            break
        await asyncio.sleep(0.01)
    assert new_task is not old_task
    assert not new_task.done()
    await wait_for_status(h.store, ACCOUNT, "queued", TaskInstanceState.SUCCEEDED)
    stuck = await h.store.get_instance_by_ids(ACCOUNT, "stuck")
    assert stuck.status is TaskInstanceState.RUNNING  # uncertain outcome preserved
    block_event(h, ACCOUNT, "stuck").set()
    await h.manager.stop()


async def test_store_closes_cleanly_after_manager_stop(tmp_path: Path) -> None:
    h = await build_harness(
        tmp_path,
        tasks_raw={"job": task("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}],
    )
    async with h.manager:
        await wait_for_status(h.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    # Adapters were closed by the manager; the store still answers queries.
    assert h.adapters[ACCOUNT].closed is True
    assert (await h.store.get_instance_by_ids(ACCOUNT, "job")).status is (
        TaskInstanceState.SUCCEEDED
    )
    await h.store.close()


# --- cache reads are not rate-limited operations ---


async def test_cache_reads_are_not_rate_limited() -> None:
    """get_channel/validate_channel are gateway-cache lookups: they must
    pass straight through to the inner adapter without touching the
    central rate limiter (no permit, no pacing slot, no route)."""
    from autoclaimer.adapters import ChannelInfo
    from autoclaimer.ratelimit import EffectivePolicy, RateLimiter

    class NoRunLimiter(RateLimiter):
        async def run(
            self,
            operation_fn: Any,
            *,
            account_id: int,
            **kwargs: Any,
        ) -> Any:
            raise AssertionError("cache reads must not be rate-limited")

    inner = MockAdapter()
    inner.add_channel(ChannelInfo(channel_id=7, name="spam", channel_type="text"))
    wrapped = RateLimitedAdapter(inner, NoRunLimiter(default_policy=EffectivePolicy()), ACCOUNT)
    assert await wrapped.get_channel(7) == ChannelInfo(7, "spam", "text")
    assert await wrapped.validate_channel(7) is True
    assert [call.operation for call in inner.calls] == ["get_channel", "validate_channel"]


# --- restart recovery: persisted cooldowns (audit F1) ---


async def test_start_loads_persisted_server_cooldowns(tmp_path: Path) -> None:
    """Audit F1 regression: a restarting manager must adopt still-active
    persisted scope cooldowns. A server-directed 429 deadline recorded in
    a previous process gates the new process's first operation instead of
    being forgotten."""
    from autoclaimer.ratelimit import RateLimiter, account_scope

    app = parse_app_config(
        {
            "version": 1,
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 5},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "tasks": {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )

    async def noop(context: Any) -> Any:
        from autoclaimer.registry import TaskOutcome, TaskStatus

        return TaskOutcome(TaskStatus.SUCCEEDED)

    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="job",
            description="Restart cooldown probe.",
            validate_config=lambda config: None,
            handler=noop,
        )
    )
    db = tmp_path / "state.db"

    # "Previous run": a server-directed cooldown lands in the store.
    store1 = TaskStateStore(db)
    await store1.open()
    limiter1 = RateLimiter(store=store1)
    await limiter1.record_cooldown(
        account_scope(ACCOUNT), retry_after_seconds=600, server_directed=True
    )
    await store1.close()

    # Restart: fresh store handle, fresh limiter (empty in-memory state),
    # manager over the same database. The limiter's clock advances with
    # every fake sleep so cooldown re-check loops converge.
    store2 = TaskStateStore(db)
    await store2.open()
    sleeps: list[float] = []
    clock = {"now": datetime.now(UTC)}

    def fake_wall() -> datetime:
        return clock["now"]

    async def fake_sleep(seconds: float) -> None:
        sleeps.append(seconds)
        clock["now"] = clock["now"] + timedelta(seconds=seconds)

    limiter2 = RateLimiter(store=store2, wall=fake_wall, sleep=fake_sleep)
    manager = WorkerManager(
        registry=registry,
        store=store2,
        limiter=limiter2,
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
    )
    await manager.start()

    loaded = {info.scope: info for info in limiter2.status().cooldowns}
    assert account_scope(ACCOUNT) in loaded, "persisted cooldown not adopted on start"
    assert loaded[account_scope(ACCOUNT)].server_directed is True

    # Behavioral: the account is parked until the persisted deadline.
    await limiter2.wait_scopes(ACCOUNT)
    assert sleeps and 500 < sleeps[-1] <= 600

    await manager.stop()
    await store2.close()


# --- domain-scheduled retries via outcome deadlines (milestone 4) ---


async def test_outcome_retry_at_schedules_from_learned_deadline(tmp_path: Path) -> None:
    """A handler that LEARNED a Mudae deadline returns it on the
    recoverable outcome; the worker schedules the next attempt at that
    absolute deadline instead of computing transport backoff."""
    from datetime import UTC, datetime

    learned = datetime(2026, 10, 29, 13, 0, 0, tzinfo=UTC)

    async def learns_deadline(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(
            TaskStatus.FAILED_RECOVERABLE,
            message="$rt cooldown active",
            retry_at=learned,
        )

    app = parse_app_config(
        {
            "version": 1,
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 5},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "tasks": {"roll": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["roll"]}]}, app
    )
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="roll",
            description="Learns a deadline.",
            validate_config=lambda config: None,
            handler=learns_deadline,
        )
    )
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    limiter = RateLimiter(store=store)
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=limiter,
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
        wall=lambda: datetime(2026, 10, 29, 12, 0, 0, tzinfo=UTC),
    )
    async with manager:
        await wait_for_status(store, ACCOUNT, "roll", TaskInstanceState.WAITING)
    record = await store.get_instance_by_ids(ACCOUNT, "roll")
    assert record.next_attempt_at == learned  # the learned deadline, not backoff
    await store.close()


# PR #3 review fix: naive outcome retry_at never leaves the row running


async def test_naive_outcome_retry_at_fails_terminally_not_stuck(tmp_path: Path) -> None:
    """Review finding 2: a handler returning a timezone-naive retry_at
    would crash the worker's max() comparison AFTER the durable claim —
    leaving the instance RUNNING forever. The worker boundary validates
    the deadline: the attempt fails terminally with a clear message and
    supervision continues."""
    from datetime import datetime as _dt

    async def bad_deadline(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(
            TaskStatus.FAILED_RECOVERABLE,
            message="learned deadline",
            retry_at=_dt(2026, 10, 29, 13, 0, 0),  # naive: contract violation
        )

    app = parse_app_config(
        {
            "version": 1,
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 5},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "tasks": {"roll": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["roll"]}]}, app
    )
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="roll",
            description="Returns a naive deadline.",
            validate_config=lambda config: None,
            handler=bad_deadline,
        )
    )
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=RateLimiter(store=store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
    )
    async with manager:
        await wait_for_status(store, ACCOUNT, "roll", TaskInstanceState.FAILED)
    record = await store.get_instance_by_ids(ACCOUNT, "roll")
    assert record.status is TaskInstanceState.FAILED  # NOT stuck running
    assert record.last_outcome is not None
    assert "timezone-naive" in record.last_outcome.message
    await store.close()


# Consolidated review #4: loss callback through the production contract


async def test_rate_limited_adapter_forwards_on_events_lost() -> None:

    inner = MockAdapter()
    limiter = RateLimiter()
    wrapped = RateLimitedAdapter(inner, limiter, ACCOUNT)
    lost: list[int] = []
    wrapped.subscribe(lambda event: None, on_events_lost=lost.append)
    stats = inner._events.stats()  # noqa: SLF001
    assert stats.subscriber_count == 1  # forwarded to the real dispatcher
    await inner.close()
    assert lost == []  # nothing lost on a clean stop


# Consolidated review #6: naive retry_at on ANY outcome never strands the row


async def test_naive_retry_at_on_success_fails_terminally(tmp_path: Path) -> None:
    from datetime import datetime as _dt

    async def bad_success(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED, retry_at=_dt(2026, 10, 29, 13, 0, 0))

    app = parse_app_config(
        {
            "version": 1,
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 5},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "tasks": {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="job",
            description="Naive success.",
            validate_config=lambda config: None,
            handler=bad_success,
        )
    )
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=RateLimiter(store=store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
    )
    async with manager:
        await wait_for_status(store, ACCOUNT, "job", TaskInstanceState.FAILED)
    record = await store.get_instance_by_ids(ACCOUNT, "job")
    assert record.status is TaskInstanceState.FAILED  # not stranded running
    assert record.last_outcome is not None
    assert "timezone-naive" in record.last_outcome.message
    await store.close()


async def test_naive_retry_at_on_permanent_failure_keeps_terminal(tmp_path: Path) -> None:
    from datetime import datetime as _dt

    async def bad_permanent(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(
            TaskStatus.FAILED_PERMANENT,
            message="boom",
            retry_at=_dt(2026, 10, 29, 13, 0, 0),
        )

    app = parse_app_config(
        {
            "version": 1,
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 5},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "tasks": {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="job",
            description="Naive permanent.",
            validate_config=lambda config: None,
            handler=bad_permanent,
        )
    )
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=RateLimiter(store=store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
    )
    async with manager:
        await wait_for_status(store, ACCOUNT, "job", TaskInstanceState.FAILED)
    record = await store.get_instance_by_ids(ACCOUNT, "job")
    assert record.status is TaskInstanceState.FAILED  # terminal, not running
    assert record.last_outcome is not None
    assert "timezone-naive" in record.last_outcome.message
    await store.close()
