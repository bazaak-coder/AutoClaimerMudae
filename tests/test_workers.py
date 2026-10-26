"""Tests for the worker manager: concurrency, ordering, controls, recovery."""

import asyncio
from dataclasses import dataclass, field
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
from autoclaimer.workers import WorkerManager, WorkerState

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
