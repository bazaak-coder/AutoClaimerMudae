"""Reliability regression tests from the Phase 8 audit."""

import asyncio
import logging
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import DatabaseBusyError, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

# test_logging configures the "autoclaimer" logger with propagate=False;
# re-enable propagation so caplog can observe worker logging here.
logging.getLogger("autoclaimer").propagate = True

ACCOUNT = 271485504361996288
OTHER = 380028190142042112


class FlakyStoreWrapper:
    """Delegates to a real store; fails due_instances N times."""

    def __init__(self, inner: TaskStateStore, failures: int) -> None:
        self._inner = inner
        self._failures = failures
        self.due_calls = 0
        self.raised = 0

    async def due_instances(self) -> Any:
        self.due_calls += 1
        if self._failures > 0:
            self._failures -= 1
            self.raised += 1
            raise DatabaseBusyError("simulated transient lock")
        return await self._inner.due_instances()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def build_manager(
    tmp_path: Path, *, store: Any = None
) -> tuple[WorkerManager, TaskStateStore]:
    app = parse_app_config(
        {
            "version": 1,
            "settings": {"max_concurrent_accounts": 2, "shutdown_timeout_seconds": 2},
            "tasks": {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )

    async def handler(context: Any) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="job",
            description="reliability probe",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    real_store = TaskStateStore(tmp_path / "state.db")
    await real_store.open()
    limiter = RateLimiter(store=real_store)
    manager = WorkerManager(
        registry=registry,
        store=store if store is not None else real_store,
        limiter=limiter,
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
    )
    return manager, real_store


async def wait_succeeded(store: TaskStateStore, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        rows = await store.list_instances()
        if rows and all(r.status.value == "succeeded" for r in rows):
            return
        await asyncio.sleep(0.01)
    pytest.fail("instance never succeeded")


async def test_worker_survives_transient_store_failure(tmp_path: Path) -> None:
    """Audit defect: a store blip must not silently kill the worker task."""
    real_store = await _open(tmp_path)
    flaky = FlakyStoreWrapper(real_store, failures=3)
    manager, store = await build_manager(tmp_path, store=flaky)
    await manager.start()
    await wait_succeeded(store)
    status = await manager.status()
    assert status.workers[0].state.value == "running"  # worker never died
    assert flaky.raised == 3  # the failures really happened
    await manager.stop()
    await store.close()


async def _open(tmp_path: Path) -> TaskStateStore:
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    return store


async def test_worker_logs_and_survives_persistent_store_failure(
    tmp_path: Path, caplog: Any
) -> None:
    real_store = await _open(tmp_path)
    flaky = FlakyStoreWrapper(real_store, failures=10_000)
    manager, store = await build_manager(tmp_path, store=flaky)
    with caplog.at_level("ERROR", logger="autoclaimer.workers"):
        await manager.start()
        await asyncio.sleep(0.15)  # several failed iterations elapse
        status = await manager.status()
        assert status.workers[0].state.value == "running"  # still alive
        await manager.stop()
    assert any("worker loop iteration failed" in r.message for r in caplog.records)
    await store.close()


def test_no_blocking_io_in_package() -> None:
    """Audit check: no blocking sleeps or sync event-loop driving anywhere."""
    offenders: list[str] = []
    for path in Path("autoclaimer").glob("*.py"):
        for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
            if "time.sleep" in line or "run_until_complete" in line:
                offenders.append(f"{path}:{number}: {line.strip()}")
    assert offenders == [], f"blocking I/O found:\n{offenders}"
