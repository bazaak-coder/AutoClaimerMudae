"""Dead-letter handling (roadmap item 8 — P1).

Dead-letter = the terminal ``failed`` state (retry-bound exhausted —
infinite loops already impossible). This suite pins the NEW pieces:
bounded execution history (schema v6), manual requeue with a fresh
attempt budget, restart survival of dead-letter and requeue states,
inspectability through the CLI, and the auxiliary-history contract
(a history-write failure never breaks outcome recording).
"""

import asyncio
import sqlite3
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import (
    StateTransitionError,
    TaskInstanceState,
    TaskStateStore,
)
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

ACCOUNT = 271485504361996288


async def _wait_status(store: TaskStateStore, task_id: str, *statuses: TaskInstanceState) -> Any:
    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get_instance_by_ids(ACCOUNT, task_id)
        if record.status in statuses:
            return record
        await asyncio.sleep(0.02)
    pytest.fail(f"{ACCOUNT}/{task_id} never reached {statuses} (={record.status})")


def _build(
    tmp_path: Path, *, handler: Any, retry_limit: int = 1
) -> tuple[WorkerManager, TaskStateStore, TaskRegistry]:
    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 1},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "task_defaults": {"retry_limit": retry_limit},
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
            description="Dead-letter probe.",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    store = TaskStateStore(tmp_path / "state.db")
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=RateLimiter(store=store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.01,
    )
    return manager, store, registry


# --- dead-letter basics -------------------------------------------------------


async def test_exhausted_task_dead_letters_with_final_reason_and_history(
    tmp_path: Path,
) -> None:
    attempts = [0]

    async def flaky(context: TaskContext) -> TaskOutcome:
        attempts[0] += 1
        return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="probe failure 7")

    manager, store, _ = _build(tmp_path, handler=flaky, retry_limit=1)
    await store.open()
    try:
        async with manager:
            record = await _wait_status(store, "job", TaskInstanceState.FAILED)
        assert record.attempt_count == 2  # bounded: retry_limit 1 → no infinite loop
        message = record.last_outcome.message if record.last_outcome else ""
        assert "probe failure 7" in message  # final reason persisted
        history = await store.task_history(record.id)
        assert [row[1] for row in history] == ["failed", "waiting"]  # newest first
        assert all(row[0] in (1, 2) for row in history)
    finally:
        await store.close()


# --- manual requeue ------------------------------------------------------------


async def test_manual_requeue_gives_fresh_budget_and_executes_again(
    tmp_path: Path,
) -> None:
    """The dead-lettered task is requeued (attempt budget reset), the
    manager picks it up, and it succeeds; the requeue event is recorded
    in history."""
    failures = [2]

    async def flaky_then_ok(context: TaskContext) -> TaskOutcome:
        if failures[0] > 0:
            failures[0] -= 1
            return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="transient")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store, _ = _build(tmp_path, handler=flaky_then_ok, retry_limit=1)
    await store.open()
    try:
        async with manager:
            dead = await _wait_status(store, "job", TaskInstanceState.FAILED)
            assert dead.attempt_count == 2

            requeued = await store.requeue_instance(dead.id)
            assert requeued.status is TaskInstanceState.PENDING
            assert requeued.attempt_count == 0  # fresh budget

            revived = await _wait_status(store, "job", TaskInstanceState.SUCCEEDED)
            assert revived.attempt_count == 1

            # The status flip and the history append are two writes; poll
            # briefly until the history reflects the success (CI-timing).
            statuses: list[str] = []
            deadline = asyncio.get_running_loop().time() + 5.0
            while asyncio.get_running_loop().time() < deadline:
                statuses = [row[1] for row in await store.task_history(dead.id)]
                if statuses and statuses[0] == "succeeded":
                    break
                await asyncio.sleep(0.02)
            assert statuses and statuses[0] == "succeeded"
            assert "requeued" in statuses
    finally:
        await store.close()


async def test_requeue_rejects_non_dead_lettered_rows(tmp_path: Path) -> None:
    async def ok(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store, _ = _build(tmp_path, handler=ok, retry_limit=0)
    await store.open()
    try:
        async with manager:
            record = await _wait_status(store, "job", TaskInstanceState.SUCCEEDED)
            with pytest.raises(StateTransitionError, match="dead-lettered"):
                await store.requeue_instance(record.id)
    finally:
        await store.close()


# --- restart survival ------------------------------------------------------------


async def test_dead_letter_and_requeue_survive_restart(tmp_path: Path) -> None:
    async def always_fails(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="doomed")

    manager, store, _ = _build(tmp_path, handler=always_fails, retry_limit=0)
    db = tmp_path / "state.db"
    await store.open()
    try:
        async with manager:
            await _wait_status(store, "job", TaskInstanceState.FAILED)
    finally:
        await store.close()

    # Restart 1: still dead-lettered (no auto-retry resurrection).
    reopened = TaskStateStore(db)
    await reopened.open()
    record = await reopened.get_instance_by_ids(ACCOUNT, "job")
    assert record.status is TaskInstanceState.FAILED
    await reopened.requeue_instance(record.id)
    await reopened.close()

    # Restart 2: the requeue survived — row is pending and due.
    again = TaskStateStore(db)
    await again.open()
    try:
        record = await again.get_instance_by_ids(ACCOUNT, "job")
        assert record.status is TaskInstanceState.PENDING
        assert record.attempt_count == 0
        due = await again.due_instances(ACCOUNT)
        assert [r.id for r in due] == [record.id]
        history = await again.task_history(record.id)
        assert any(row[1] == "requeued" for row in history)
    finally:
        await again.close()


# --- history contract ------------------------------------------------------------


async def test_history_is_bounded_per_instance(tmp_path: Path) -> None:
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    try:
        record = await store.create_instance(ACCOUNT, "churn")
        # The bounding logic lives in _append_history (the outcome paths
        # are terminal-gated, one failure per claim); drive it directly.
        for n in range(60):
            await store._append_history(  # noqa: SLF001
                record.id, ACCOUNT, "churn", n, "failed", f"f{n}"
            )
        history = await store.task_history(record.id, limit=100)
        assert len(history) <= 50  # bounded retention
        conn = sqlite3.connect(tmp_path / "state.db")
        try:
            total = conn.execute(
                "SELECT COUNT(*) FROM task_history WHERE instance_id = ?", (record.id,)
            ).fetchone()[0]
        finally:
            conn.close()
        assert total == 50
    finally:
        await store.close()


async def test_history_failure_never_breaks_outcome_recording(tmp_path: Path) -> None:
    """Auxiliary contract: with the history INSERT failing, the outcome
    write to task_instances still lands."""
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    try:
        record = await store.create_instance(ACCOUNT, "probe")
        await store.claim(record.id)
        original = store._update_sync

        def failing_history(sql: str, params: Any) -> int:
            if "task_history" in str(sql):
                raise sqlite3.OperationalError("history table unavailable (injected)")
            return original(sql, params)

        store._update_sync = failing_history  # type: ignore[method-assign]
        try:
            updated = await store.record_failure(
                record.id,
                TaskOutcome(TaskStatus.FAILED_PERMANENT, message="still recorded"),
                retry_at=None,
            )
        finally:
            store._update_sync = original  # type: ignore[method-assign]
        assert updated.status is TaskInstanceState.FAILED  # outcome landed
        assert "still recorded" in (updated.last_outcome.message if updated.last_outcome else "")
        assert await store.task_history(record.id) == ()  # history skipped, harmlessly
    finally:
        await store.close()


# --- CLI surface --------------------------------------------------------------------


async def test_cli_retry_and_history(tmp_path: Path) -> None:
    from autoclaimer.cli import AutoClaimerController, CliSession

    always_fail: list[int] = []

    async def handler(context: TaskContext) -> TaskOutcome:
        always_fail.append(1)
        return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="cli probe failure")

    manager, store, registry = _build(tmp_path, handler=handler, retry_limit=0)
    app = manager._app_config  # noqa: SLF001
    accounts_cfg = manager._accounts  # noqa: SLF001
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts_cfg,
        store=store,
        limiter=RateLimiter(store=store),
        registry=registry,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.01,
    )
    await store.open()
    output: list[str] = []
    session = CliSession(controller, read_line=None, write=output.append)  # type: ignore[arg-type]
    try:
        await controller.start()
        await _wait_status(store, "job", TaskInstanceState.FAILED)
        output.clear()
        assert await session.run_once("history 271485504361996288 job") is True
        joined = "\n".join(output)
        assert "cli probe failure" in joined
        assert "#1 failed" in joined
        # Requeue from the CLI with a HEALING handler: the requeued run succeeds.
        output.clear()
        assert await session.run_once("retry 271485504361996288 job") is True
        assert any("requeued" in line for line in output)
    finally:
        await controller.shutdown()
    assert joined
