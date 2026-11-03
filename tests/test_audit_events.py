"""Structured audit events (roadmap item 11 — P2).

Separate from debug logging: persisted (schema v7), restart-durable,
bounded, CLI-queryable. Every scenario also proves the two invariants:
emission never breaks the audited operation, and the trail never
contains secrets (redaction at the emit boundary).
"""

import asyncio
import sqlite3
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import pytest

from autoclaimer.audit import AuditLog
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

ACCOUNT = 271485504361996288
OTHER = 380028190142042112


async def _wait_status(store: TaskStateStore, task_id: str, *statuses: TaskInstanceState) -> Any:
    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get_instance_by_ids(ACCOUNT, task_id)
        if record.status in statuses:
            return record
        await asyncio.sleep(0.02)
    pytest.fail(f"never reached {statuses} (={record.status})")


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
            description="Audit probe.",
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


def _actions(store_rows: Any) -> list[str]:
    return [f"{row[1]}.{row[2]}" for row in store_rows]


# --- task lifecycle events -------------------------------------------------------


async def test_task_lifecycle_is_audited(tmp_path: Path) -> None:
    calls = [0]

    async def flaky_then_ok(context: TaskContext) -> TaskOutcome:
        calls[0] += 1
        if calls[0] == 1:
            return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="one retry")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store, _ = _build(tmp_path, handler=flaky_then_ok, retry_limit=2)
    await store.open()
    try:
        async with manager:
            await _wait_status(store, "job", TaskInstanceState.SUCCEEDED)
        events = await store.audit_events(limit=20)
        actions = _actions(events)
        assert "task.started" in actions
        assert "task.retried" in actions
        assert "task.completed" in actions
        # Timestamps and identifiers are structured fields, not prose.
        for timestamp, _category, _action, account_id, task_id, _detail in events:
            assert timestamp  # ISO timestamps
            assert account_id == ACCOUNT or account_id is None
            assert task_id in ("job", None)
    finally:
        await store.close()


async def test_terminal_failure_is_audited_with_reason(tmp_path: Path) -> None:
    async def doomed(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.FAILED_PERMANENT, message="config rejected")

    manager, store, _ = _build(tmp_path, handler=doomed, retry_limit=0)
    await store.open()
    try:
        async with manager:
            await _wait_status(store, "job", TaskInstanceState.FAILED)
        events = await store.audit_events(limit=10)
        failures = [row for row in events if row[2] == "failed"]
        assert failures and "config rejected" in failures[0][5]
    finally:
        await store.close()


# --- control-plane events ----------------------------------------------------------


async def test_account_and_runtime_controls_are_audited(tmp_path: Path) -> None:
    async def ok(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store, _ = _build(tmp_path, handler=ok)
    await store.open()
    try:
        async with manager:
            manager.pause_account(ACCOUNT)
            manager.resume_account(ACCOUNT)
            manager.pause_all()
            manager.resume_all()
            manager.set_account_enabled(ACCOUNT, False)
            manager.set_account_enabled(ACCOUNT, True)
            await asyncio.sleep(0.1)  # fire-and-forget emits drain
        actions = _actions(await store.audit_events(limit=20))
        for expected in (
            "account.paused",
            "account.resumed",
            "runtime.paused",
            "runtime.resumed",
            "account.disabled",
            "account.enabled",
        ):
            assert expected in actions, (expected, actions)
    finally:
        await store.close()


# --- restart durability + retention + redaction --------------------------------------


async def test_events_survive_restart(tmp_path: Path) -> None:
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    audit = AuditLog(store)
    await audit.emit(category="runtime", action="paused", detail="before restart")
    await store.close()

    reopened = TaskStateStore(tmp_path / "state.db")
    await reopened.open()
    try:
        events = await reopened.audit_events()
        assert events and events[0][2] == "paused"
    finally:
        await reopened.close()


async def test_retention_is_bounded(tmp_path: Path) -> None:
    from autoclaimer.persistence import _AUDIT_KEEP

    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    try:
        for n in range(_AUDIT_KEEP + 150):
            await store.record_audit_event(category="runtime", action="tick", detail=str(n))
        conn = sqlite3.connect(tmp_path / "state.db")
        try:
            total = conn.execute("SELECT COUNT(*) FROM audit_events").fetchone()[0]
        finally:
            conn.close()
        assert total == _AUDIT_KEEP
        newest = (await store.audit_events(limit=1))[0]
        assert newest[5] == str(_AUDIT_KEEP + 149)  # newest kept
    finally:
        await store.close()


async def test_detail_is_redacted_at_the_emit_boundary(tmp_path: Path) -> None:
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    try:
        audit = AuditLog(store)
        token = "gho_AuditSecretToken998877665544"
        await audit.emit(
            category="credential", action="set", account_id=ACCOUNT, detail=f"token: {token}"
        )
        events = await store.audit_events()
        assert token not in events[0][5]
    finally:
        await store.close()


async def test_emit_failure_never_breaks_the_operation(tmp_path: Path) -> None:
    """Auxiliary contract: with the audit INSERT failing, the audited
    operation (an outcome write) still lands."""
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    try:
        record = await store.create_instance(ACCOUNT, "probe")
        await store.claim(record.id)
        original = store._update_sync  # noqa: SLF001

        def failing_audit(sql: str, params: Any) -> int:
            if "audit_events" in str(sql):
                raise sqlite3.OperationalError("audit unavailable (injected)")
            return original(sql, params)

        store._update_sync = failing_audit  # type: ignore[method-assign]
        audit = AuditLog(store)
        try:
            await audit.emit(category="runtime", action="paused")  # swallowed
            updated = await store.record_failure(  # the OPERATION still lands
                record.id,
                TaskOutcome(TaskStatus.FAILED_PERMANENT, message="landed"),
                retry_at=None,
            )
        finally:
            store._update_sync = original  # type: ignore[method-assign]
        assert updated.status is TaskInstanceState.FAILED
    finally:
        await store.close()


# --- CLI surface -----------------------------------------------------------------------


async def test_cli_audit_command(tmp_path: Path) -> None:
    from autoclaimer.cli import AutoClaimerController, CliSession

    async def ok(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store, registry = _build(tmp_path, handler=ok)
    controller = AutoClaimerController(
        app_config=manager._app_config,  # noqa: SLF001
        accounts=manager._accounts,  # noqa: SLF001
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
        await _wait_status(store, "job", TaskInstanceState.SUCCEEDED)
        await asyncio.sleep(0.1)
        output.clear()
        assert await session.run_once("audit 10") is True
        joined = "\n".join(output)
        assert "task.started" in joined
        assert "task.completed" in joined
        assert f"account={ACCOUNT}" in joined
    finally:
        await controller.shutdown()


# --- LOW-033: control-plane audit emissions must be strongly referenced
# (no GC mid-flight) and drained on stop ---


async def test_controlplane_audit_tasks_are_referenced_and_drained(
    tmp_path: Path,
) -> None:
    """LOW-033: ``_audit_emit`` is fire-and-forget — the event loop only
    keeps WEAK references to tasks, so an unreferenced emission can be
    garbage-collected mid-flight (lost event). The manager must hold a
    strong reference while the emission is pending and give outstanding
    emissions a bounded chance to land during stop()."""

    async def handler(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store, _registry = _build(tmp_path, handler=handler)
    await store.open()
    try:
        await manager.start()
        manager.pause_all()  # sync control-plane emission (fire-and-forget)
        assert manager._audit_tasks, (  # noqa: SLF001 — the contract under test
            "pending audit emission is not strongly referenced (GC can drop it)"
        )
        await manager.stop()
        await asyncio.sleep(0)  # settle
        assert not manager._audit_tasks  # drained, references released
        rows = await store.audit_events(limit=10)
        assert any(r[1] == "runtime" and r[2] == "paused" for r in rows), rows
    finally:
        await store.close()


# --- review F1: a cancellation during the audit drain must not break
# stop()'s teardown contract (adapters closed, lifecycle reset) ---


async def test_stop_cancelled_during_audit_drain_still_tears_down(
    tmp_path: Path,
) -> None:
    """Review F1 (LOW-033 follow-up): stop() promises that cancelling it
    completes the teardown and re-raises (audit B2/B3). The audit drain
    must sit INSIDE the try whose finally closes adapters and resets the
    lifecycle flags — a cancellation while it waits would otherwise skip
    the finally entirely and permanently wedge the manager."""

    class _SlowAudit:
        def __init__(self) -> None:
            self.called = asyncio.Event()

        async def emit(self, **event: Any) -> None:
            self.called.set()
            await asyncio.sleep(0.5)

    async def handler(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store, _registry = _build(tmp_path, handler=handler)
    await store.open()
    try:
        await manager.start()
        slow = _SlowAudit()
        manager._audit = cast(Any, slow)  # noqa: SLF001 — the window under test
        await manager._adapter_for(ACCOUNT)  # noqa: SLF001 — a live adapter to close
        assert manager._adapters, "setup: adapter registered"

        manager.pause_all()  # fire-and-forget emission now pending (slow)
        await asyncio.wait_for(slow.called.wait(), timeout=2.0)
        stop_task = asyncio.create_task(manager.stop(timeout=30.0))
        await asyncio.sleep(0.1)  # stopped inside the audit drain
        stop_task.cancel()
        with suppress(asyncio.CancelledError):
            await stop_task
        assert stop_task.cancelled(), "cancellation of stop() must propagate"

        # The finally completed despite the cancellation...
        assert not manager._adapters, "adapters were not closed (teardown skipped)"
        assert manager._stopped is False, "manager left half-stopped (_stopped stuck)"
        assert manager._started is False, "manager left half-stopped (_started stuck)"
        # ...and workers drained out on the shutdown flag (their loop
        # exits at the next iteration boundary after the drain cancel —
        # an in-flight execution first finishes its own audit emission,
        # hence the >0.5s budget).
        await asyncio.sleep(1.0)
        assert all(
            w.task is None or w.task.done()
            for w in manager._workers.values()  # noqa: SLF001 — contract under test
        ), "workers not cleaned up after cancelled stop"
    finally:
        await store.close()
