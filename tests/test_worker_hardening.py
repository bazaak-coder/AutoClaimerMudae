"""Batch 2 regression tests (Phase 9 worker-correctness fixes)."""

import asyncio
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import DatabaseBusyError, TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

ACCOUNT = 271485504361996288
OTHER = 380028190142042112


class ClaimFlakyStore:
    """Real store delegation; claim always fails (audit B4 hot-spin repro)."""

    def __init__(self, inner: TaskStateStore) -> None:
        self._inner = inner
        self.due_calls = 0
        self.claim_calls = 0

    async def due_instances(self, account_id: int | None = None) -> Any:
        self.due_calls += 1
        return await self._inner.due_instances(account_id)

    async def claim(self, instance_id: int) -> Any:
        self.claim_calls += 1
        raise DatabaseBusyError("persistently locked")

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def build_manager(
    tmp_path: Path, *, store: Any = None, tasks: dict[str, Any] | None = None
) -> tuple[WorkerManager, TaskStateStore, TaskRegistry]:
    app = parse_app_config(
        {
            "version": 1,
            "settings": {"max_concurrent_accounts": 2, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
            "tasks": tasks if tasks is not None else {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )
    registry = TaskRegistry()
    block_events: dict[int, asyncio.Event] = {ACCOUNT: asyncio.Event()}

    async def handler(context: Any) -> TaskOutcome:
        if context.config.get("behavior") == "block":
            await block_events[context.account_id].wait()
        return TaskOutcome(TaskStatus.SUCCEEDED)

    registry.register(
        TaskType(
            id="job",
            description="probe",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    real_store = TaskStateStore(tmp_path / "state.db")
    await real_store.open()
    created: list[MockAdapter] = []

    def factory(account_id: int) -> MockAdapter:
        adapter = MockAdapter()
        created.append(adapter)
        return adapter

    manager = WorkerManager(
        registry=registry,
        store=store if store is not None else real_store,
        limiter=RateLimiter(store=real_store),
        app_config=app,
        accounts=accounts,
        adapter_factory=factory,
        idle_interval_seconds=0.05,
    )
    manager.test_block_events = block_events  # type: ignore[attr-defined]
    manager.test_adapters = created  # type: ignore[attr-defined]
    return manager, real_store, registry


async def wait_terminal(store: TaskStateStore, instance_id: int, timeout: float = 3.0) -> Any:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get_instance(instance_id)
        if record.status in (TaskInstanceState.FAILED, TaskInstanceState.SUCCEEDED):
            return record
        await asyncio.sleep(0.01)
    pytest.fail("instance never reached a terminal state")


# B1: stale/misconfigured instance must fail terminally, not wedge as running


async def test_stale_task_row_fails_terminally_without_wedging(tmp_path: Path) -> None:
    manager, store, _ = await build_manager(tmp_path)
    # A row for a task that is NOT in the account's current assignments.
    stale = await store.create_instance(ACCOUNT, "ghost_task")
    healthy = await store.create_instance(ACCOUNT, "job")
    await manager.start()
    stale_row = await wait_terminal(store, stale.id)
    assert stale_row.status is TaskInstanceState.FAILED
    assert "not assigned" in (stale_row.last_outcome.message or "")
    healthy_row = await wait_terminal(store, healthy.id)
    assert healthy_row.status is TaskInstanceState.SUCCEEDED  # unaffected
    await manager.stop()
    await store.close()


async def test_invalid_task_config_fails_terminally(tmp_path: Path) -> None:
    # Task assigned but its parameters fail the handler's validator.
    app = parse_app_config(
        {
            "version": 1,
            "settings": {"shutdown_timeout_seconds": 2},
            "tasks": {"job": {"parameters": {"behavior": "noop"}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )
    registry = TaskRegistry()

    def require_channel(config: Any) -> None:
        if "channel" not in config:
            from autoclaimer.registry import TaskConfigError

            raise TaskConfigError("job requires 'channel'")

    async def handler(context: Any) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    registry.register(
        TaskType(
            id="job",
            description="validator probe",
            validate_config=require_channel,
            handler=handler,
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
    await manager.start()
    row = await store.get_instance_by_ids(ACCOUNT, "job")
    terminal = await wait_terminal(store, row.id)
    assert terminal.status is TaskInstanceState.FAILED
    assert "channel" in (terminal.last_outcome.message or "")
    await manager.stop()
    await store.close()


# B2/B3: cancelled stop() must not wedge the manager


async def test_cancelled_stop_still_completes_teardown_and_allows_restart(
    tmp_path: Path,
) -> None:
    manager, store, _ = await build_manager(
        tmp_path, tasks={"job": {"parameters": {"behavior": "block"}}}
    )
    await manager.start()
    # Keep a worker mid-flight so stop() is genuinely inside its drain.
    deadline = asyncio.get_running_loop().time() + 3.0
    while (await store.get_instance_by_ids(ACCOUNT, "job")).status is not TaskInstanceState.RUNNING:
        if asyncio.get_running_loop().time() > deadline:
            pytest.fail("instance never started running")
        await asyncio.sleep(0.01)
    stop_task = asyncio.create_task(manager.stop(timeout=30.0))
    await asyncio.sleep(0.02)  # stop() is now awaiting the drain
    stop_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await stop_task
    # N1: the finally still closed every adapter despite cancellation.
    created: list[MockAdapter] = manager.test_adapters  # type: ignore[attr-defined]
    assert created, "no adapter was ever built"
    assert all(a.closed for a in created)
    assert manager._adapters == {}  # noqa: SLF001
    # The finally-block completed teardown: a second stop returns cleanly
    # and a restart works.
    await manager.stop()
    await manager.start()
    await manager.stop()
    await store.close()


# B4: persistent claim failures must back off, not hot-spin


async def test_persistent_claim_failure_backs_off(tmp_path: Path) -> None:
    real_store = TaskStateStore(tmp_path / "state.db")
    await real_store.open()
    flaky = ClaimFlakyStore(real_store)
    manager, store, _ = await build_manager(tmp_path, store=flaky)
    await manager.start()
    await asyncio.sleep(0.5)  # several idle intervals elapse
    await manager.stop()
    # idle 0.05s -> at most ~10 claim attempts in 0.5s (hot-spin: hundreds).
    assert flaky.claim_calls >= 1, "claim path never exercised (vacuous test)"
    assert flaky.claim_calls <= 12, f"hot-spin: {flaky.claim_calls} claims in 0.5s"
    await store.close()


# B5: unknown account ids must be rejected


async def test_enable_unknown_account_is_rejected(tmp_path: Path) -> None:
    manager, store, _ = await build_manager(tmp_path)
    await manager.start()
    with pytest.raises(ValueError, match="unknown account"):
        manager.set_account_enabled(999999, True)
    status = await manager.status()
    assert all(w.account_id == ACCOUNT for w in status.workers)  # no zombie
    await manager.stop()
    await store.close()


# B6: cooldown wait happens outside the execution slot


async def test_wait_scopes_holds_no_permits(tmp_path: Path) -> None:
    from datetime import UTC, datetime, timedelta

    from autoclaimer.ratelimit import EffectivePolicy

    del tmp_path

    class TM:
        def __init__(self) -> None:
            self.mono = 0.0
            self.wall = datetime(2026, 10, 27, tzinfo=UTC)

        def now_mono(self) -> float:
            return self.mono

        def now_wall(self) -> datetime:
            return self.wall

        async def sleep(self, seconds: float) -> None:
            self.mono += seconds
            self.wall += timedelta(seconds=seconds)

    tm = TM()
    limiter = RateLimiter(
        default_policy=EffectivePolicy(max_concurrency=1),
        mono=tm.now_mono,
        wall=tm.now_wall,
        sleep=tm.sleep,
    )
    await limiter.record_cooldown(
        f"account:{ACCOUNT}", retry_after_seconds=10.0, server_directed=True
    )
    await limiter.wait_scopes(ACCOUNT)  # waits out the cooldown (tm advances)
    assert tm.mono == pytest.approx(10.0)
    assert limiter.status().permits == ()  # never created a permit/slot


# B8: worker-level retry bound + finite backoff at extreme attempts


async def test_recoverable_forever_eventually_fails_permanently(tmp_path: Path) -> None:
    app = parse_app_config(
        {
            "version": 1,
            "settings": {"shutdown_timeout_seconds": 2},
            "ratelimit": {
                "retry_limit": 1,
                "backoff_base_seconds": 0.01,
                "backoff_cap_seconds": 0.01,
            },
            "tasks": {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )
    registry = TaskRegistry()

    async def always_recoverable(context: Any) -> TaskOutcome:
        return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="flaky")

    registry.register(
        TaskType(
            id="job",
            description="flaky probe",
            validate_config=lambda config: None,
            handler=always_recoverable,
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
    await manager.start()
    row = await store.get_instance_by_ids(ACCOUNT, "job")
    terminal = await wait_terminal(store, row.id, timeout=5.0)
    assert terminal.status is TaskInstanceState.FAILED
    assert "worker retry limit" in (terminal.last_outcome.message or "")
    assert terminal.attempt_count == 2  # retry_limit 1 -> 2 attempts total
    await manager.stop()
    await store.close()


async def test_retry_delay_is_finite_at_extreme_attempts(tmp_path: Path) -> None:
    manager, store, _ = await build_manager(tmp_path)
    for attempts in (1, 100, 1025, 2000, 10**6):
        delay = manager._retry_delay(attempts, ACCOUNT, "job")  # noqa: SLF001
        assert 0.0 <= delay <= 0.02  # capped by the config's backoff cap
    await store.close()
