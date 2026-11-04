"""Task execution timeouts (roadmap item 7 — P1).

Semantics under test (defined before implementation):
- configurable: settings default + per-task override; None disables;
- timeout cancellation PROPAGATES (cleanup runs, nothing runs on);
- timeout outcomes are persisted, recoverable, and ride the normal
  retry bound — a distinct message, never confused with handler
  exceptions (including a handler raising TimeoutError itself);
- a timeout during an adapter operation cancels the operation;
- shutdown during the timeout window leaks no handler tasks.
"""

import asyncio
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.config import parse_accounts_config, parse_app_config, resolve_task_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.models import (
    AccountProfile,  # noqa: F401
    ConfigValidationError,
)
from autoclaimer.persistence import TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

ACCOUNT = 271485504361996288


# --- configuration -------------------------------------------------------------


def test_timeout_config_validation_and_precedence() -> None:
    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 1},
            "settings": {"task_timeout_seconds": 5},
            "tasks": {
                "override": {"timeout_seconds": 2, "parameters": {}},
                "inherit": {"parameters": {}},
                "null_inherits": {"timeout_seconds": None, "parameters": {}},
            },
        }
    )
    profile = AccountProfile(id=ACCOUNT, tasks=("override", "inherit", "null_inherits"))
    assert resolve_task_config(app, profile, "override").timeout_seconds == 2.0
    assert resolve_task_config(app, profile, "inherit").timeout_seconds == 5.0
    # Explicit null = "no override" — inherits the settings default, like
    # every other optional field in the model.
    assert resolve_task_config(app, profile, "null_inherits").timeout_seconds == 5.0

    no_default = parse_app_config(
        {"version": 1, "channels": {"roll_claim": 1}, "tasks": {"job": {"parameters": {}}}}
    )
    assert no_default.settings.task_timeout_seconds is None
    assert (
        resolve_task_config(
            no_default, AccountProfile(id=ACCOUNT, tasks=("job",)), "job"
        ).timeout_seconds
        is None
    )


@pytest.mark.parametrize("bad", [0, -1, True, "5", 1e309, 10**400])
def test_invalid_timeout_values_rejected(bad: Any) -> None:
    with pytest.raises(ConfigValidationError, match="timeout"):
        parse_app_config(
            {
                "version": 1,
                "channels": {"roll_claim": 1},
                "settings": {"task_timeout_seconds": bad},
                "tasks": {"job": {"parameters": {}}},
            }
        )
    with pytest.raises(ConfigValidationError, match="timeout"):
        parse_app_config(
            {
                "version": 1,
                "channels": {"roll_claim": 1},
                "tasks": {"job": {"timeout_seconds": bad, "parameters": {}}},
            }
        )


# --- runtime --------------------------------------------------------------------


async def _wait_status(store: TaskStateStore, task_id: str, *statuses: TaskInstanceState) -> Any:
    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get_instance_by_ids(ACCOUNT, task_id)
        if record.status in statuses:
            return record
        await asyncio.sleep(0.02)
    pytest.fail(f"{ACCOUNT}/{task_id} never reached {statuses} (={record.status})")


def _build(
    tmp_path: Path,
    *,
    handler: Any,
    settings: dict[str, Any] | None = None,
    tasks: Any = None,
    retry_limit: int = 1,
    db_name: str = "state.db",
) -> tuple[WorkerManager, TaskStateStore]:
    tasks = tasks or {"job": {"parameters": {}}}
    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 1},
            "settings": {
                "max_concurrent_accounts": 1,
                "shutdown_timeout_seconds": 2,
                **(settings or {}),
            },
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "task_defaults": {"retry_limit": retry_limit},
            "tasks": tasks,
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": list(tasks)}]}, app
    )
    registry = TaskRegistry()
    for task_id in tasks:
        registry.register(
            TaskType(
                id=task_id,
                description=f"Timeout probe {task_id}.",
                validate_config=lambda config: None,
                handler=handler,
            )
        )
    store = TaskStateStore(tmp_path / db_name)
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=RateLimiter(store=store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.01,
    )
    return manager, store


async def test_timeout_fires_persists_and_retries_to_terminal(tmp_path: Path) -> None:
    """A stuck handler: the timeout outcome is recoverable with a distinct
    persisted message (single attempt, retry_limit 0), and with retries
    enabled it burns attempts and ends terminal by the bound."""

    async def stuck(context: TaskContext) -> TaskOutcome:
        await asyncio.sleep(30.0)
        raise AssertionError("should never finish")

    # Single attempt: the terminal row itself carries the timeout message.
    manager, store = _build(
        tmp_path, handler=stuck, settings={"task_timeout_seconds": 0.05}, retry_limit=0
    )
    await store.open()
    try:
        async with manager:
            record = await _wait_status(store, "job", TaskInstanceState.FAILED)
        assert record.attempt_count == 1
        message = record.last_outcome.message if record.last_outcome else ""
        assert "timed out after 0.05s" in message
    finally:
        await store.close()

    # With retries: two timeout attempts, then the bound's terminal
    # message (intermediate timeout outcomes are overwritten by the
    # exhaustion message — same as every recoverable failure path).
    manager2, store2 = _build(
        tmp_path,
        handler=stuck,
        settings={"task_timeout_seconds": 0.05},
        retry_limit=1,
        db_name="second.db",
    )
    await store2.open()
    try:
        async with manager2:
            record2 = await _wait_status(store2, "job", TaskInstanceState.FAILED)
        assert record2.attempt_count == 2
        message2 = record2.last_outcome.message if record2.last_outcome else ""
        assert "retry limit" in message2
    finally:
        await store2.close()


async def test_handler_timeout_exception_is_not_a_framework_timeout(
    tmp_path: Path,
) -> None:
    """A handler raising TimeoutError itself keeps its exception identity:
    handler-crash path (permanent), NOT the timeout message."""

    async def raises_timeout(context: TaskContext) -> TaskOutcome:
        raise TimeoutError("inner handler timeout")

    manager, store = _build(
        tmp_path, handler=raises_timeout, settings={"task_timeout_seconds": 5.0}
    )
    await store.open()
    try:
        async with manager:
            record = await _wait_status(store, "job", TaskInstanceState.FAILED)
        message = record.last_outcome.message if record.last_outcome else ""
        assert "TimeoutError: inner handler timeout" in message
        assert "timed out after" not in message
    finally:
        await store.close()


async def test_timeout_cancellation_runs_cleanup_and_stops_the_handler(
    tmp_path: Path,
) -> None:
    """Cleanup executes after the timeout cancellation, and the handler
    does not continue in the background."""
    cleanup_ran = asyncio.Event()
    post_timeout_side_effect: list[str] = []

    async def cleanup_tracking(context: TaskContext) -> TaskOutcome:
        try:
            await asyncio.sleep(30.0)
            post_timeout_side_effect.append("finished")  # must NOT happen
        except asyncio.CancelledError:
            cleanup_ran.set()
            raise
        raise AssertionError("unreachable")

    manager, store = _build(
        tmp_path,
        handler=cleanup_tracking,
        settings={"task_timeout_seconds": 0.05},
        retry_limit=0,
    )
    await store.open()
    try:
        async with manager:
            record = await _wait_status(store, "job", TaskInstanceState.FAILED)
            assert "timed out after" in (record.last_outcome.message if record.last_outcome else "")
        # Cleanup ran (cancellation propagated into the handler)…
        assert cleanup_ran.is_set()
        # …and the handler is NOT running in the background: it never
        # reached its finish line, and no stray task lingers.
        assert post_timeout_side_effect == []
        assert not any(
            "cleanup_tracking" in (task.get_coro() and repr(task.get_coro()) or "")
            for task in asyncio.all_tasks()
        )
    finally:
        await store.close()


async def test_timeout_during_adapter_operation_cancels_it(tmp_path: Path) -> None:
    """The timeout fires while the handler awaits a slow adapter
    operation: the operation is cancelled (nothing sent) and the timeout
    outcome is recorded."""
    adapters: dict[int, MockAdapter] = {}

    def factory(account_id: int) -> MockAdapter:
        adapter = adapters.setdefault(account_id, MockAdapter())
        return adapter

    async def slow_op_handler(context: TaskContext) -> TaskOutcome:
        await context.adapter.send_message(1, "slow")  # blocks past the timeout
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 1},
            "settings": {
                "max_concurrent_accounts": 1,
                "shutdown_timeout_seconds": 2,
                "task_timeout_seconds": 0.05,
            },
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "task_defaults": {"retry_limit": 0},
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
            description="Slow adapter probe.",
            validate_config=lambda config: None,
            handler=slow_op_handler,
        )
    )
    store = TaskStateStore(tmp_path / "state.db")
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=RateLimiter(store=store),
        app_config=app,
        accounts=accounts,
        adapter_factory=factory,
        idle_interval_seconds=0.01,
    )
    adapter = factory(ACCOUNT)
    adapter.delay_seconds = 5.0  # the platform op stalls past the timeout
    await store.open()
    try:
        async with manager:
            record = await _wait_status(store, "job", TaskInstanceState.FAILED)
        assert "timed out after" in (record.last_outcome.message if record.last_outcome else "")
        assert adapter.sent_messages == []  # the op was cancelled, not sent
    finally:
        await store.close()


async def test_no_timeout_configured_runs_unbounded(tmp_path: Path) -> None:
    """Control: without a timeout, a handler that outlives any would-be
    deadline completes normally (backward compatibility)."""

    async def slow_but_finite(context: TaskContext) -> TaskOutcome:
        await asyncio.sleep(0.3)  # longer than every timeout used above
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store = _build(tmp_path, handler=slow_but_finite)
    await store.open()
    try:
        async with manager:
            await _wait_status(store, "job", TaskInstanceState.SUCCEEDED)
    finally:
        await store.close()


async def test_shutdown_during_timeout_window_leaks_nothing(tmp_path: Path) -> None:
    """Stopping the manager while the handler sits inside its timeout
    window: the child handler is cancelled with the worker — no leaks."""

    started = asyncio.Event()

    async def parked(context: TaskContext) -> TaskOutcome:
        started.set()
        await asyncio.sleep(30.0)
        raise AssertionError("unreachable")

    manager, store = _build(tmp_path, handler=parked, settings={"task_timeout_seconds": 10.0})
    await store.open()
    baseline = {task for task in asyncio.all_tasks() if task is not asyncio.current_task()}
    try:
        await manager.start()
        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline and not started.is_set():
            await asyncio.sleep(0.02)
        assert started.is_set()  # inside the timeout window now
    finally:
        await manager.stop()
        await store.close()
    leaked = asyncio.all_tasks() - baseline - {asyncio.current_task()}
    assert not leaked, [t.get_name() for t in leaked]
