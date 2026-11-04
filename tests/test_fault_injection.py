"""Fault-injection testing across worker and adapter boundaries
(roadmap item 5 — P1).

Systematic failure simulation: adapter disconnections, database read and
write failures, delayed operations, cancellation at critical execution
points, and rate-limit/cooldown storms. Every scenario verifies the four
required properties:

- the worker recovers and keeps processing work,
- shutdown stays clean (no leaked asyncio tasks),
- persisted rows reach consistent states,
- failures are observable through structured logs.

The existing failure-simulation suite (T1/T2/T4/T7/T11) covers
handler-crash timing; this suite injects faults at the TRANSPORT and
PERSISTENCE boundaries around the worker.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.adapters import AccountFatalError, CooldownError, RecoverableAdapterError
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import DatabaseBusyError, TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

ACCOUNT = 271485504361996288
OTHER = 380028190142042112


class RecordingSleep:
    """Instant sleep stand-in that also ADVANCES a fake wall/mono clock —
    cooldown waits loop until the wall deadline passes, so an instant
    sleep without a moving clock would busy-spin in real time."""

    def __init__(self) -> None:
        self.calls: list[float] = []
        self.now = datetime(2026, 11, 1, 12, 0, 0, tzinfo=UTC)

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        self.now += timedelta(seconds=seconds)

    def wall(self) -> datetime:
        return self.now

    def mono(self) -> float:
        return self.now.timestamp()


async def _wait_status(
    store: TaskStateStore, account_id: int, task_id: str, *statuses: TaskInstanceState
) -> None:
    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get_instance_by_ids(account_id, task_id)
        if record.status in statuses:
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"{account_id}/{task_id} never reached {statuses} (={record.status})")


class FaultHarness:
    """Manager + two accounts + scriptable adapters + fault hooks."""

    def __init__(self, tmp_path: Path) -> None:
        self.tmp_path = tmp_path
        self.adapters: dict[int, MockAdapter] = {}
        self.calls: dict[tuple[int, str], int] = {}
        self.behaviors: dict[str, Any] = {}
        self.sleep = RecordingSleep()

    def build(
        self,
        *,
        accounts: tuple[int, ...] = (ACCOUNT, OTHER),
        tasks: tuple[str, ...] = ("job",),
        retry_limit: int = 2,
        shutdown_timeout: int = 3,
    ) -> None:
        async def handler(context: TaskContext) -> TaskOutcome:
            self.calls[(context.account_id, context.task_id)] = (
                self.calls.get((context.account_id, context.task_id), 0) + 1
            )
            behavior = self.behaviors.get("handler")
            if behavior is not None:
                outcome: TaskOutcome = await behavior(context)
                return outcome
            await context.adapter.send_message(1, "work")
            return TaskOutcome(TaskStatus.SUCCEEDED)

        app = parse_app_config(
            {
                "version": 1,
                "channels": {"roll_claim": 111111111111111111},
                "settings": {
                    "max_concurrent_accounts": 2,
                    "shutdown_timeout_seconds": shutdown_timeout,
                },
                "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
                "task_defaults": {"retry_limit": retry_limit},
                "tasks": {task_id: {"parameters": {}} for task_id in tasks},
            }
        )
        self.app = app
        self.accounts_cfg = parse_accounts_config(
            {
                "version": 1,
                "accounts": [{"id": account_id, "tasks": list(tasks)} for account_id in accounts],
            },
            app,
        )
        registry = TaskRegistry()
        for task_id in tasks:
            registry.register(
                TaskType(
                    id=task_id,
                    description=f"Fault probe {task_id}.",
                    validate_config=lambda config: None,
                    handler=handler,
                )
            )
        self.store = TaskStateStore(self.tmp_path / "state.db")
        self.manager = WorkerManager(
            registry=registry,
            store=self.store,
            limiter=RateLimiter(
                store=self.store,
                sleep=self.sleep,
                wall=self.sleep.wall,
                mono=self.sleep.mono,
            ),
            app_config=app,
            accounts=self.accounts_cfg,
            adapter_factory=self._adapter_for,
            idle_interval_seconds=0.01,
        )

    def _adapter_for(self, account_id: int) -> MockAdapter:
        adapter = self.adapters.get(account_id)
        if adapter is None:
            adapter = MockAdapter()
            self.adapters[account_id] = adapter
        return adapter

    async def start(self) -> None:
        await self.store.open()
        await self.manager.start()

    async def stop(self) -> None:
        await self.manager.stop()
        await self.store.close()


def _baseline_tasks() -> set[asyncio.Task[None]]:
    return {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}


async def _assert_no_new_tasks(baseline: set[asyncio.Task[None]]) -> None:
    for _ in range(50):  # workers finish draining
        leaked = asyncio.all_tasks() - baseline - {asyncio.current_task()}
        if not leaked:
            return
        await asyncio.sleep(0.02)
    pytest.fail(f"leaked asyncio tasks: {[t.get_name() for t in leaked]}")


# --- adapter disconnections -------------------------------------------------


async def test_disconnect_storm_recovers_and_keeps_working(tmp_path: Path) -> None:
    """Two recoverable disconnects, then the adapter heals: the task
    retries (recoverable) to success and the worker keeps serving."""
    harness = FaultHarness(tmp_path)
    harness.build()
    adapter = harness._adapter_for(ACCOUNT)
    adapter.script(
        "send_message",
        RecoverableAdapterError("gateway closed"),
        RecoverableAdapterError("gateway closed"),
    )
    baseline = _baseline_tasks()
    await harness.start()
    try:
        await _wait_status(harness.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
        await _wait_status(harness.store, OTHER, "job", TaskInstanceState.SUCCEEDED)
        row = await harness.store.get_instance_by_ids(ACCOUNT, "job")
        # The TRANSPORT layer contained both disconnects inside worker
        # attempt 1 (bounded transport retries) — the worker attempt
        # itself succeeded once the adapter healed.
        assert row.attempt_count == 1
        assert len(adapter.sent_messages) == 1  # the heal actually sent
    finally:
        await harness.stop()
    await _assert_no_new_tasks(baseline)


async def test_permanent_disconnects_fail_within_retry_limit(tmp_path: Path) -> None:
    """Never-healing disconnects: the instance exhausts its resolved
    retry limit, fails terminally, and the worker still serves NEW work."""
    harness = FaultHarness(tmp_path)
    harness.build(tasks=("doomed", "healthy"))
    adapter = harness._adapter_for(ACCOUNT)
    # dooming script: every send fails for the first task
    for _ in range(6):
        adapter.script("send_message", RecoverableAdapterError("down"))

    async def only_doomed(context: TaskContext) -> TaskOutcome:
        if context.task_id == "doomed":
            await context.adapter.send_message(1, "x")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    harness.behaviors["handler"] = only_doomed
    baseline = _baseline_tasks()
    await harness.start()
    try:
        await _wait_status(harness.store, ACCOUNT, "doomed", TaskInstanceState.FAILED)
        row = await harness.store.get_instance_by_ids(ACCOUNT, "doomed")
        # The transport layer exhausted ITS bounded retries inside worker
        # attempt 1 and the task failed terminally with the exhaustion
        # visible in the recorded outcome.
        assert row.attempt_count == 1
        message = row.last_outcome.message if row.last_outcome else ""
        assert "attempt" in message  # the exhaustion reason is persisted
        await _wait_status(harness.store, ACCOUNT, "healthy", TaskInstanceState.SUCCEEDED)
    finally:
        await harness.stop()
    await _assert_no_new_tasks(baseline)


async def test_account_fatal_isolates_the_account(tmp_path: Path) -> None:
    """An account-fatal adapter failure stops ONLY that account: its
    queue fails terminally while the other account keeps succeeding."""
    harness = FaultHarness(tmp_path)
    harness.build()
    fatal_adapter = harness._adapter_for(ACCOUNT)
    fatal_adapter.script("send_message", AccountFatalError("token revoked"))

    async def fail_for_account(context: TaskContext) -> TaskOutcome:
        if context.account_id == ACCOUNT:
            await context.adapter.send_message(1, "x")  # raises fatal
            raise AssertionError("unreachable")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    harness.behaviors["handler"] = fail_for_account
    baseline = _baseline_tasks()
    await harness.start()
    try:
        await _wait_status(harness.store, OTHER, "job", TaskInstanceState.SUCCEEDED)
        await _wait_status(harness.store, ACCOUNT, "job", TaskInstanceState.FAILED)
    finally:
        await harness.stop()
    await _assert_no_new_tasks(baseline)


# --- database read / write faults -------------------------------------------


async def test_db_read_failure_backs_off_then_recovers(tmp_path: Path) -> None:
    """Transient DatabaseBusyError on the scheduling read: the worker
    loop survives (backs off), then completes the work."""
    harness = FaultHarness(tmp_path)
    harness.build(accounts=(ACCOUNT,))
    await harness.store.open()
    original = harness.store.due_instances
    failures = [2]

    async def flaky_due(account_id: int | None = None) -> tuple[Any, ...]:
        if failures[0] > 0:
            failures[0] -= 1
            raise DatabaseBusyError("database is locked (injected)")
        return await original(account_id)

    harness.store.due_instances = flaky_due  # type: ignore[method-assign]
    baseline = _baseline_tasks()
    await harness.manager.start()
    try:
        await _wait_status(harness.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    finally:
        await harness.stop()
    await _assert_no_new_tasks(baseline)


async def test_db_write_failure_keeps_row_restart_recoverable(tmp_path: Path) -> None:
    """A store failure while recording the outcome: the row stays
    `running` (uncertain), the worker survives, and a restart recovers
    it to pending and re-executes — at-least-once under persistence
    faults too."""
    harness = FaultHarness(tmp_path)
    harness.build(accounts=(ACCOUNT,))
    await harness.store.open()
    write_attempts = [0]

    # Review finding: the old flaky-once injection was fully absorbed by
    # ERR-005's bounded retry (row faked to SUCCEEDED) and the RUNNING
    # poll exited on the first mid-handler read — both assertions passed
    # vacuously. Fail EVERY outcome write: the row must stay honestly
    # running after the retry window, and the restart must re-execute.
    async def always_busy_success(instance_id: int, outcome: TaskOutcome, **kwargs: Any) -> Any:
        write_attempts[0] += 1
        raise DatabaseBusyError("database is locked (injected)")

    harness.store.record_success = always_busy_success  # type: ignore[method-assign]
    baseline = _baseline_tasks()
    await harness.manager.start()
    try:
        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline:
            if harness.calls.get((ACCOUNT, "job"), 0) >= 1:
                break
            await asyncio.sleep(0.02)
        assert harness.calls.get((ACCOUNT, "job"), 0) >= 1, "handler never ran"
        # Past ERR-005's single bounded retry (0.5s backoff): with the
        # write still failing, the row must NOT have been faked terminal.
        await asyncio.sleep(1.2)
        row = await harness.store.get_instance_by_ids(ACCOUNT, "job")
        assert row.status is TaskInstanceState.RUNNING, (
            f"outcome faked under persistent write failure: {row.status.value}"
        )
        assert write_attempts[0] >= 2, (
            f"bounded retry never engaged: {write_attempts[0]} write attempt(s)"
        )
    finally:
        await harness.stop()
    await _assert_no_new_tasks(baseline)

    # Restart: recovery flips the uncertain row to pending; it completes.
    reopened = TaskStateStore(harness.tmp_path / "state.db")
    await reopened.open()
    await reopened.recover_interrupted()
    harness2 = FaultHarness(harness.tmp_path)
    harness2.adapters[ACCOUNT] = MockAdapter()
    harness2.build(accounts=(ACCOUNT,))
    harness2.store = reopened
    harness2.manager = WorkerManager(
        registry=harness2.manager._registry
        if hasattr(harness2.manager, "_registry")
        else _rebuild_registry(harness2),
        store=reopened,
        limiter=RateLimiter(store=reopened),
        app_config=harness2.app,
        accounts=harness2.accounts_cfg,
        adapter_factory=lambda account_id: harness2.adapters[account_id],
        idle_interval_seconds=0.01,
    )
    await harness2.manager.start()
    try:
        await _wait_status(reopened, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
        assert harness2.calls.get((ACCOUNT, "job"), 0) >= 1, (
            "restart did not re-execute the recovered row"
        )
    finally:
        await harness2.manager.stop()
        await reopened.close()
    await _assert_no_new_tasks(baseline)


def _rebuild_registry(harness: FaultHarness) -> TaskRegistry:
    registry = TaskRegistry()

    async def handler(context: TaskContext) -> TaskOutcome:
        await context.adapter.send_message(1, "work")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    registry.register(
        TaskType(
            id="job",
            description="Fault probe job.",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    return registry


# --- delayed operations and cancellation ------------------------------------


async def test_delayed_adapter_operation_bounded_by_handler(tmp_path: Path) -> None:
    """A slow adapter operation inside a handler that enforces its own
    bound: the timeout is classified recoverable, never a hang."""
    harness = FaultHarness(tmp_path)
    harness.build(accounts=(ACCOUNT,))
    adapter = harness._adapter_for(ACCOUNT)

    async def slow_op(_seconds: float) -> None:
        await asyncio.sleep(1.0)  # far beyond the handler's bound

    adapter.delay_seconds = 0.0

    async def bounded_handler(context: TaskContext) -> TaskOutcome:
        try:
            await asyncio.wait_for(context.adapter.send_message(1, "x"), timeout=0.05)
        except TimeoutError:
            return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="adapter stalled")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    harness.behaviors["handler"] = bounded_handler
    # Script a scripted-delay by wrapping the adapter's send.
    real_send = adapter.send_message

    async def slow_send(*args: Any, **kwargs: Any) -> Any:
        await asyncio.sleep(1.0)
        return await real_send(*args, **kwargs)

    adapter.send_message = slow_send  # type: ignore[method-assign]  # noqa: SLF001
    baseline = _baseline_tasks()
    await harness.start()
    try:
        # The bounded handler classifies the stall as recoverable; with a
        # permanently slow adapter the retries exhaust and the task ends
        # terminally with the bounded exhaustion reason persisted — never
        # a hang.
        await _wait_status(harness.store, ACCOUNT, "job", TaskInstanceState.FAILED)
        row = await harness.store.get_instance_by_ids(ACCOUNT, "job")
        assert "retry limit" in (row.last_outcome.message if row.last_outcome else "")
    finally:
        await harness.stop()
    await _assert_no_new_tasks(baseline)


async def test_shutdown_during_blocked_handler_leaks_nothing(tmp_path: Path) -> None:
    """Cancellation at the critical point (mid-execution): graceful stop
    cancels the blocked handler, leaves the row recoverable, leaks no
    tasks, and the restart completes the work."""
    harness = FaultHarness(tmp_path)
    harness.build(accounts=(ACCOUNT,), shutdown_timeout=2)
    blocked = asyncio.Event()

    async def blocking_handler(context: TaskContext) -> TaskOutcome:
        await blocked.wait()
        return TaskOutcome(TaskStatus.SUCCEEDED)

    harness.behaviors["handler"] = blocking_handler
    baseline = _baseline_tasks()
    await harness.start()
    try:
        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline:
            row = await harness.store.get_instance_by_ids(ACCOUNT, "job")
            if row.status is TaskInstanceState.RUNNING:
                break
            await asyncio.sleep(0.02)
        assert row.status is TaskInstanceState.RUNNING
    finally:
        await harness.stop()  # graceful stop cancels the blocked handler
    await _assert_no_new_tasks(baseline)
    assert harness.calls[(ACCOUNT, "job")] == 1

    # Restart completes the work (at-least-once under cancellation).
    blocked.set()
    reopened = TaskStateStore(harness.tmp_path / "state.db")
    await reopened.open()
    await reopened.recover_interrupted()
    registry = _rebuild_registry(harness)
    manager = WorkerManager(
        registry=registry,
        store=reopened,
        limiter=RateLimiter(store=reopened),
        app_config=harness.app,
        accounts=harness.accounts_cfg,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.01,
    )
    await manager.start()
    try:
        await _wait_status(reopened, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
    finally:
        await manager.stop()
        await reopened.close()
    await _assert_no_new_tasks(baseline)


# --- rate-limit storms --------------------------------------------------------


async def test_cooldown_storm_defers_without_bursting(tmp_path: Path) -> None:
    """Repeated server-directed cooldowns: every retry WAITS the
    recorded cooldown (no request bursts) and the operation eventually
    succeeds."""
    harness = FaultHarness(tmp_path)
    harness.build(accounts=(ACCOUNT,), retry_limit=3)
    adapter = harness._adapter_for(ACCOUNT)
    adapter.script(
        "send_message",
        CooldownError(30.0, "slow down"),
        CooldownError(30.0, "slow down"),
    )
    baseline = _baseline_tasks()
    await harness.start()
    try:
        await _wait_status(harness.store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
        # Both cooldowns were waited out via the limiter (recorded by the
        # instant sleep), i.e. deferred — never burst-retried.
        cooldown_waits = [s for s in harness.sleep.calls if s >= 25.0]
        assert len(cooldown_waits) >= 2, harness.sleep.calls
        assert len(adapter.sent_messages) == 1  # exactly one real send
    finally:
        await harness.stop()
    await _assert_no_new_tasks(baseline)


# --- observability -------------------------------------------------------------


async def test_failures_are_observable_in_structured_logs(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Representative faults produce structured log records with
    operation/outcome fields — failures are never silent."""
    harness = FaultHarness(tmp_path)
    harness.build(accounts=(ACCOUNT,), retry_limit=1)

    async def exploding_handler(context: TaskContext) -> TaskOutcome:
        await context.adapter.send_message(1, "x")
        raise RuntimeError("handler exploded (injected)")

    harness.behaviors["handler"] = exploding_handler
    with caplog.at_level(logging.WARNING, logger="autoclaimer"):
        await harness.start()
        try:
            await _wait_status(harness.store, ACCOUNT, "job", TaskInstanceState.FAILED)
        finally:
            await harness.stop()
    texts = " ".join(record.getMessage() for record in caplog.records)
    assert "task handler crashed" in texts  # classified failure surfaced
    # The injected reason rides along as the exception (exc_info), and
    # the record carries structured operation/outcome context.
    crashed = [r for r in caplog.records if "task handler crashed" in r.getMessage()]
    assert crashed and crashed[0].exc_info is not None
    assert str(crashed[0].exc_info[1]) == "handler exploded (injected)"
    extras = [
        (getattr(record, "operation", None), getattr(record, "outcome", None))
        for record in caplog.records
        if hasattr(record, "operation")
    ]
    assert extras, "no structured operation records logged"
    assert any(op in ("task.execute", "worker.iteration") for op, _ in extras)
