"""Batch 2 regression tests (Phase 9 worker-correctness fixes)."""

import asyncio
import logging
from contextlib import suppress
from pathlib import Path
from typing import Any, cast

import pytest

from autoclaimer.audit import AuditLog
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import (
    DatabaseBusyError,
    PersistenceError,
    TaskInstanceState,
    TaskStateStore,
)
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager, WorkerState

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
            "channels": {"roll_claim": 111111111111111111},
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
            "channels": {"roll_claim": 111111111111111111},
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
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"shutdown_timeout_seconds": 2},
            "ratelimit": {
                "backoff_base_seconds": 0.01,
                "backoff_cap_seconds": 0.01,
            },
            # Repo-sweep fix: the WORKER-level retry bound comes from the
            # resolved task config (task_defaults → task → override),
            # not the ratelimit layer.
            "task_defaults": {"retry_limit": 1},
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
        delay = manager._retry_delay(  # noqa: SLF001
            attempts,
            ACCOUNT,
            "job",
            app=manager._app_config,  # noqa: SLF001
            accounts=manager._accounts,  # noqa: SLF001
        )
        assert 0.0 <= delay <= 0.02  # capped by the config's backoff cap
    await store.close()


# BUG-033: startup must be all-or-nothing — no worker may survive a failed
# start (a retry would run recover_interrupted against a live worker's
# claimed row and re-execute it: duplicate side effects)


async def test_failed_startup_leaves_no_workers_and_retry_runs_once(
    tmp_path: Path,
) -> None:
    """BUG-033: materialization failure for a LATER account must not
    leave the EARLIER account's worker alive with ``_started`` False —
    retrying start() would reset that worker's in-flight row and execute
    the task twice. Startup materializes every account BEFORE the first
    worker spawns."""
    executions: list[int] = []

    async def handler(context: Any) -> TaskOutcome:
        executions.append(context.account_id)
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 2, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
            "tasks": {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {
            "version": 1,
            "accounts": [
                {"id": ACCOUNT, "tasks": ["job"]},
                {"id": OTHER, "tasks": ["job"]},
            ],
        },
        app,
    )
    registry = TaskRegistry()
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

    class FaultySecondAccountStore:
        """Delegating store: create_instance fails for OTHER until cleared."""

        def __init__(self, inner: TaskStateStore) -> None:
            self._inner = inner
            self.fault_active = True

        async def create_instance(self, account_id: int, task_id: str) -> Any:
            if self.fault_active and account_id == OTHER:
                raise DatabaseBusyError("fault injected: second account unavailable")
            return await self._inner.create_instance(account_id, task_id)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    faulty = FaultySecondAccountStore(real_store)
    manager = WorkerManager(
        registry=registry,
        store=cast(Any, faulty),
        limiter=RateLimiter(store=real_store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.05,
    )
    try:
        with pytest.raises(PersistenceError, match="second account unavailable"):
            await manager.start()
        live = [
            w
            for w in manager._workers.values()  # noqa: SLF001 — the contract under test
            if w.task is not None and not w.task.done()
        ]
        assert not live, f"{len(live)} worker(s) survived the failed startup"

        faulty.fault_active = False
        await manager.start()  # retry: full sequence again
        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline:
            record = await real_store.get_instance_by_ids(ACCOUNT, "job")
            if record.status is TaskInstanceState.SUCCEEDED:
                break
            await asyncio.sleep(0.01)
        else:
            pytest.fail("retry startup never executed the task")
        assert executions.count(ACCOUNT) == 1, executions  # exactly once
        assert record.attempt_count == 1
    finally:
        await manager.stop()
        await real_store.close()


# BUG-037: stop() must not return while cancelled workers are still unwinding


async def test_cancelled_stop_awaits_worker_unwind_before_returning(
    tmp_path: Path,
) -> None:
    """BUG-037: the cleanup path cancels straggler workers but must also
    AWAIT them before closing adapters and resetting lifecycle state —
    otherwise stop() returns while a worker is still unwinding, and an
    immediate restart runs recover_interrupted() against a still-live
    worker's claimed row (duplicate-execution race).

    The window is a cancellation during the audit DRAIN (pre-worker-gather):
    the gather's own cancellation semantics already await unwinding, but
    workers cancelled in the finally were never joined. The handler's
    finally-block models realistic unwind work (0.3s) so the race is
    deterministic."""
    from contextlib import suppress
    from typing import Any, cast

    calls: list[int] = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(context: Any) -> TaskOutcome:
        calls.append(context.account_id)
        entered.set()
        try:
            if len(calls) == 1:
                await release.wait()  # first execution blocks until cancelled
        finally:
            await asyncio.sleep(0.3)  # unwind work that survives cancellation

        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
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
            description="probe",
            validate_config=lambda config: None,
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
        idle_interval_seconds=0.05,
    )

    class _SlowAudit:
        """One pending slow emission keeps stop() inside its audit drain."""

        def __init__(self) -> None:
            self.called = asyncio.Event()

        async def emit(self, **event: Any) -> None:
            self.called.set()
            await asyncio.sleep(0.5)

    try:
        await manager.start()
        await asyncio.wait_for(entered.wait(), timeout=3.0)  # worker mid-handler

        slow_audit = _SlowAudit()
        manager._audit = cast(  # noqa: SLF001 — the drain window under test
            Any, slow_audit
        )
        manager.pause_all()  # pending slow emission -> stop() parks in the drain
        await asyncio.wait_for(slow_audit.called.wait(), timeout=2.0)

        stop_task = asyncio.create_task(manager.stop(timeout=30.0))
        await asyncio.sleep(0.1)  # stop() is inside the audit drain
        stop_task.cancel()
        with suppress(asyncio.CancelledError):
            await stop_task  # stop() has now returned

        # BUG-037: every worker task is DONE by the time stop() returns.
        live = [
            w
            for w in manager._workers.values()  # noqa: SLF001 — the contract
            if w.task is not None and not w.task.done()
        ]
        assert not live, f"stop() returned with {len(live)} worker(s) still unwinding"

        # Restart: the cancelled attempt's row is recovered and executed
        # exactly once more — no duplicate execution. (resume_all: the
        # drain setup globally paused the runtime; a restart does not
        # clear runtime pause state by design.)
        await manager.start()
        manager.resume_all()
        deadline = asyncio.get_running_loop().time() + 5.0
        while (
            await store.get_instance_by_ids(ACCOUNT, "job")
        ).status is not TaskInstanceState.SUCCEEDED:
            if asyncio.get_running_loop().time() > deadline:
                pytest.fail("recovered instance never succeeded")
            await asyncio.sleep(0.01)
        assert len(calls) == 2, calls  # one cancelled attempt + exactly one rerun
        record = await store.get_instance_by_ids(ACCOUNT, "job")
        assert record.attempt_count == 2
    finally:
        await manager.stop()
        await store.close()


# CLOSURE-001: a handler-raised PersistenceError is recoverable, not terminal


async def test_handler_persistence_error_is_retried_not_dead_lettered(
    tmp_path: Path,
) -> None:
    """CLOSURE-001: a PersistenceError raised INSIDE a handler (every
    Mudae handler is a store closure; deadline writes raise
    DatabaseBusyError under contention) is a transient infrastructure
    fault — the branch's own contract says "bounded recoverable retry,
    never a terminal kill of a recurring task". The row was terminal-
    failed on attempt 1, consuming none of its retry budget, with a
    FAILED_RECOVERABLE outcome persisted on a terminal row."""
    calls: list[str] = []
    flaky = {"fail": True}

    async def handler(context: Any) -> TaskOutcome:
        calls.append(context.task_id)
        if context.task_id == "flaky" and flaky["fail"]:
            flaky["fail"] = False
            raise DatabaseBusyError("injected: deadline write locked")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 2, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
            "task_defaults": {"retry_limit": 2},
            "tasks": {"flaky": {"parameters": {}}, "healthy": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["flaky", "healthy"]}]},
        app,
    )
    registry = TaskRegistry()
    for task_id in ("flaky", "healthy"):
        registry.register(
            TaskType(
                id=task_id,
                description="probe",
                validate_config=lambda config: None,
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
        idle_interval_seconds=0.05,
    )
    try:
        await manager.start()
        deadline = asyncio.get_running_loop().time() + 5.0
        flaky_record = await store.get_instance_by_ids(ACCOUNT, "flaky")
        while asyncio.get_running_loop().time() < deadline:
            flaky_record = await store.get_instance_by_ids(ACCOUNT, "flaky")
            if flaky_record.status is TaskInstanceState.SUCCEEDED:
                break
            await asyncio.sleep(0.02)
        assert flaky_record.status is TaskInstanceState.SUCCEEDED, (
            f"transient persistence fault dead-lettered the row: "
            f"{flaky_record.status.value} "
            f"({flaky_record.last_outcome.message if flaky_record.last_outcome else ''})"
        )
        assert flaky_record.attempt_count == 2  # one fault + one retry
        assert calls.count("flaky") == 2
        healthy = await store.get_instance_by_ids(ACCOUNT, "healthy")
        assert healthy.status is TaskInstanceState.SUCCEEDED  # worker survived
    finally:
        await manager.stop()
        await store.close()


# ERR-003: account-fatal containment must survive an outcome-record failure


async def test_fatal_containment_survives_record_failure(tmp_path: Path) -> None:
    """ERR-003: a PersistenceError from the fatal-branch outcome write
    escaped the containment — _handle_account_fatal never ran: the
    account stayed ENABLED (owner policy: never retry, never continue),
    the adapter stayed open, and the row stranded running."""
    from autoclaimer.adapters import AccountFatalError

    class _BusyRecordStore:
        """Real-store delegation; record_failure always fails."""

        def __init__(self, inner: TaskStateStore) -> None:
            self._inner = inner

        async def record_failure(self, instance_id: int, outcome: Any, **kw: Any) -> Any:
            raise DatabaseBusyError("injected: outcome write locked")

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    async def handler(context: Any) -> TaskOutcome:
        raise AccountFatalError("token revoked")

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
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
            description="probe",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    real_store = TaskStateStore(tmp_path / "state.db")
    await real_store.open()
    manager = WorkerManager(
        registry=registry,
        store=cast(Any, _BusyRecordStore(real_store)),
        limiter=RateLimiter(store=real_store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.05,
    )
    try:
        await manager.start()
        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline:
            if not manager.account_enabled(ACCOUNT):
                break
            await asyncio.sleep(0.02)
        # Containment ran despite the outcome-record failure:
        assert manager.account_enabled(ACCOUNT) is False, "account stayed enabled"
        assert ACCOUNT in manager._fatal_accounts  # noqa: SLF001 — the contract
        assert not manager._adapters, "fatal account's adapter was not closed"  # noqa: SLF001
        row = await real_store.get_instance_by_ids(ACCOUNT, "job")
        assert row.status in (
            TaskInstanceState.RUNNING,
            TaskInstanceState.FAILED,
        )  # running = documented restart-recovery bucket
    finally:
        await manager.stop()
        await real_store.close()


# CLOSURE-002: a handler-authored CancelledError must not kill the worker


async def test_handler_authored_cancel_does_not_kill_worker(tmp_path: Path) -> None:
    """CLOSURE-002: a handler raising CancelledError ITSELF (not manager
    shutdown — e.g. it awaited a child task it cancelled via its own
    timeout logic) re-raised as if it were shutdown: the account worker
    died silently (_worker_finished ignores cancelled) and the account's
    OTHER rows stranded pending with no executor while the manager kept
    running. Shutdown-authored cancellation (flag set) still propagates."""
    order: list[str] = []

    async def handler(context: Any) -> TaskOutcome:
        order.append(context.task_id)
        if context.task_id == "victim":
            raise asyncio.CancelledError  # handler-authored, NOT shutdown
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
            "tasks": {"victim": {"parameters": {}}, "queued": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["victim", "queued"]}]},
        app,
    )
    registry = TaskRegistry()
    for task_id in ("victim", "queued"):
        registry.register(
            TaskType(
                id=task_id,
                description="probe",
                validate_config=lambda config: None,
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
        idle_interval_seconds=0.05,
    )
    try:
        await manager.start()
        deadline = asyncio.get_running_loop().time() + 5.0
        while asyncio.get_running_loop().time() < deadline:
            queued = await store.get_instance_by_ids(ACCOUNT, "queued")
            if queued.status is TaskInstanceState.SUCCEEDED:
                break
            await asyncio.sleep(0.02)
        assert queued.status is TaskInstanceState.SUCCEEDED, (
            f"account worker died on a handler-authored cancellation; "
            f"sibling row stuck {queued.status.value}"
        )
        victim = await store.get_instance_by_ids(ACCOUNT, "victim")
        assert victim.status in (
            TaskInstanceState.RUNNING,  # uncertain outcome bucket
            TaskInstanceState.WAITING,
            TaskInstanceState.FAILED,
        )
        assert not manager._shutdown.is_set()  # this was never shutdown
    finally:
        await manager.stop()
        await store.close()


# RACE-002: a disable landing between the iteration gate and the claim


class _ParkingScopesLimiter(RateLimiter):
    """wait_scopes parks on an event once — the gate->claim window."""

    def __init__(self, store: TaskStateStore) -> None:
        super().__init__(store=store)
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def wait_scopes(self, account_id: int) -> None:  # type: ignore[override]
        if not self.release.is_set():
            self.entered.set()
            await self.release.wait()
        await super().wait_scopes(account_id)


async def test_disable_during_wait_scopes_stops_the_claim(tmp_path: Path) -> None:
    """RACE-002: the account gate is checked ONCE at the iteration top;
    a disable/pause landing while the worker is inside wait_scopes (an
    active account cooldown can park it for minutes) was never re-checked
    — the row was claimed and executed AFTER the operator disabled the
    account. The in-slot re-check (previously shutdown-only) must cover
    the full gate predicate."""
    executions: list[int] = []

    async def handler(context: Any) -> TaskOutcome:
        executions.append(context.account_id)
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
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
            description="probe",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    limiter = _ParkingScopesLimiter(store)
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=limiter,
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.05,
    )
    try:
        await manager.start()
        await asyncio.wait_for(limiter.entered.wait(), timeout=3.0)  # in wait_scopes

        manager.set_account_enabled(ACCOUNT, False)  # operator disables NOW
        limiter.release.set()
        await asyncio.sleep(0.5)

        assert executions == [], f"disabled account's row was claimed/executed: {executions}"
        row = await store.get_instance_by_ids(ACCOUNT, "job")
        assert row.status is TaskInstanceState.PENDING, (
            f"row claimed despite disable: {row.status.value} (attempts={row.attempt_count})"
        )
    finally:
        await manager.stop()
        await store.close()


# ERR-004: health observation must never break the error boundary


class _UnprintableError(Exception):
    """An exception whose __str__ raises (plugin wrapping lazy state)."""

    def __str__(self) -> str:  # pragma: no cover - the defect path
        raise RuntimeError("str() is broken")


async def test_unprintable_handler_exception_still_records_outcome(
    tmp_path: Path,
) -> None:
    """ERR-004: health.record_failure(str(exc)) was called UNGUARDED on
    the exception object inside except-blocks — an exception whose
    __str__ raises made the health update raise INSIDE the except block,
    escaping the error boundary and stranding the claimed row running
    (violates health.py's own 'callers wrap defensively' contract)."""

    async def handler(context: Any) -> TaskOutcome:
        raise _UnprintableError("boom")

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
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
            description="probe",
            validate_config=lambda config: None,
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
        idle_interval_seconds=0.05,
    )
    try:
        await manager.start()
        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline:
            row = await store.get_instance_by_ids(ACCOUNT, "job")
            if row.status is TaskInstanceState.FAILED:
                break
            await asyncio.sleep(0.02)
        assert row.status is TaskInstanceState.FAILED, (
            f"unprintable exception escaped the error boundary: {row.status.value}"
        )
        assert row.last_outcome is not None
        assert "_UnprintableError" in row.last_outcome.message  # bounded fallback
    finally:
        await manager.stop()
        await store.close()


# ERR-005: a transiently-busy outcome write gets one bounded retry


async def test_outcome_record_survives_one_busy_failure(tmp_path: Path) -> None:
    """ERR-005: _record had no bounded retry (its sibling _reschedule
    has one) — a single DatabaseBusyError on the post-claim outcome
    write propagated to the loop guard and stranded the claimed row
    running until manager restart."""

    class _OnceBusyRecordStore:
        def __init__(self, inner: TaskStateStore) -> None:
            self._inner = inner
            self.failed_once = False

        async def record_success(self, instance_id: int, outcome: Any) -> Any:
            if not self.failed_once:
                self.failed_once = True
                raise DatabaseBusyError("injected: outcome write locked")
            return await self._inner.record_success(instance_id, outcome)

        def __getattr__(self, name: str) -> Any:
            return getattr(self._inner, name)

    async def handler(context: Any) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
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
            description="probe",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    real_store = TaskStateStore(tmp_path / "state.db")
    await real_store.open()
    manager = WorkerManager(
        registry=registry,
        store=cast(Any, _OnceBusyRecordStore(real_store)),
        limiter=RateLimiter(store=real_store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.05,
    )
    try:
        await manager.start()
        deadline = asyncio.get_running_loop().time() + 5.0
        while asyncio.get_running_loop().time() < deadline:
            row = await real_store.get_instance_by_ids(ACCOUNT, "job")
            if row.status is TaskInstanceState.SUCCEEDED:
                break
            await asyncio.sleep(0.02)
        assert row.status is TaskInstanceState.SUCCEEDED, (
            f"one busy outcome-write stranded the row: {row.status.value}"
        )
    finally:
        await manager.stop()
        await real_store.close()


# RACE-005: a nudge arriving during the due-read must survive the wait


class _StaleDueStore:
    """due_instances parks (once, when armed) and returns the (stale)
    empty result — the window where a nudge for a freshly-due row lands
    mid-read."""

    def __init__(self, inner: TaskStateStore) -> None:
        self._inner = inner
        self.arm = False
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def due_instances(self, account_id: int | None = None) -> Any:
        if self.arm and not self.release.is_set():
            stale_result = await self._inner.due_instances(account_id)  # empty
            self.entered.set()
            await self.release.wait()
            return stale_result  # STALE: predates the newly-due row
        return await self._inner.due_instances(account_id)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def test_nudge_during_due_read_is_not_lost(tmp_path: Path) -> None:
    """RACE-005: wake.clear() ran AFTER the (awaited) due-read — a nudge
    landing while the worker was inside _next_due was wiped, and
    _soon_delay only scans WAITING rows, so a freshly requeued PENDING
    row waited a full idle interval despite the explicit wake (the
    CLI `retry` path requeue+wake is the production trigger). Clearing
    BEFORE the read lets the nudge survive into the wait."""
    executed_at: dict[str, float] = {}

    async def handler(context: Any) -> TaskOutcome:
        executed_at[context.task_id] = asyncio.get_running_loop().time()
        return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="dead-letter me")

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
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
            description="probe",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    real_store = TaskStateStore(tmp_path / "state.db")
    await real_store.open()
    stale = _StaleDueStore(real_store)
    manager = WorkerManager(
        registry=registry,
        store=cast(Any, stale),
        limiter=RateLimiter(store=real_store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=1.0,  # a lost nudge costs a full second
    )
    try:
        await manager.start()
        deadline = asyncio.get_running_loop().time() + 5.0
        while asyncio.get_running_loop().time() < deadline:
            row = await real_store.get_instance_by_ids(ACCOUNT, "job")
            if row.status is TaskInstanceState.FAILED:
                break
            await asyncio.sleep(0.02)
        assert row.status is TaskInstanceState.FAILED  # dead-lettered
        stale.arm = True  # arm the stale window for the NEXT due-read
        await asyncio.wait_for(stale.entered.wait(), timeout=5.0)  # mid-read

        # The production requeue path: row -> pending + explicit wake,
        # delivered while the worker is inside the (stale) read.
        await real_store.requeue_instance(row.id)
        manager.wake(ACCOUNT)
        nudge_at = asyncio.get_running_loop().time()
        stale.release.set()  # stale read returns: nothing due

        # The requeued row must be RE-CLAIMED promptly (fresh attempt),
        # not after the full idle interval.
        deadline = nudge_at + 0.5  # well under the 1.0s idle interval
        reclaimed = False
        while asyncio.get_running_loop().time() < deadline:
            current = await real_store.get_instance_by_ids(ACCOUNT, "job")
            if current.attempt_count >= 1 or current.status is TaskInstanceState.RUNNING:
                reclaimed = True
                break
            await asyncio.sleep(0.01)
        assert reclaimed, (
            "nudge delivered during the due-read was lost; the requeued row waited "
            f"the full idle interval ({asyncio.get_running_loop().time() - nudge_at:.2f}s, "
            f"status={current.status.value}, attempts={current.attempt_count})"
        )
    finally:
        await manager.stop()
        await real_store.close()


# RACE-006: a concurrent second stop() must not return mid-teardown


async def test_concurrent_second_stop_waits_for_first(tmp_path: Path) -> None:
    """RACE-006: stop()'s re-entrancy guard was check-then-act with no
    awaitable — a second concurrent stop() returned in ~0.1ms while the
    first was still draining an in-flight handler (adapters still open),
    and the caller then proceeded as if teardown were complete."""
    entered = asyncio.Event()
    release = asyncio.Event()

    async def handler(context: Any) -> TaskOutcome:
        entered.set()
        await release.wait()
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 5},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
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
            description="probe",
            validate_config=lambda config: None,
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
        idle_interval_seconds=0.05,
    )
    try:
        await manager.start()
        await asyncio.wait_for(entered.wait(), timeout=3.0)  # mid-handler

        stop1 = asyncio.create_task(manager.stop(timeout=30.0))
        await asyncio.sleep(0.1)  # stop1 is inside its worker drain
        assert not stop1.done()
        stop2 = asyncio.create_task(manager.stop(timeout=30.0))
        await asyncio.sleep(0.2)
        assert not stop2.done(), "second stop() returned while the first was still draining"

        release.set()
        await asyncio.wait_for(stop1, timeout=10.0)
        await asyncio.wait_for(stop2, timeout=10.0)
        assert not manager._adapters  # noqa: SLF001 — teardown completed
        assert manager._stopped is False  # noqa: SLF001
    finally:
        await manager.stop()
        await store.close()


# Shutdown review round: audit emissions and cancellation-resistant handlers


class _BlockedAuditSink:
    """record_audit_event parks forever and HONORS cancellation."""

    def __init__(self) -> None:
        self.calls = 0
        self.cancelled = 0

    async def record_audit_event(self, **_: Any) -> None:
        self.calls += 1
        try:
            await asyncio.sleep(3600)
        except asyncio.CancelledError:
            self.cancelled += 1
            raise


class _SwallowingAuditSink:
    """record_audit_event parks forever and suppresses cancellation."""

    def __init__(self) -> None:
        self.calls = 0
        self.cancel_deliveries = 0
        self.finished = asyncio.Event()

    async def record_audit_event(self, **_: Any) -> None:
        self.calls += 1
        while not self.finished.is_set():
            try:
                await self.finished.wait()
            except asyncio.CancelledError:
                self.cancel_deliveries += 1


async def _one_account_manager(
    tmp_path: Path,
    handler: Any,
    *,
    settings: dict[str, Any] | None = None,
    task_settings: dict[str, Any] | None = None,
) -> tuple[WorkerManager, TaskStateStore]:
    """Standard single-account manager fixture for the shutdown tests."""
    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2}
            | (settings or {}),
            "task_defaults": {"retry_limit": 0} | (task_settings or {}),
            "tasks": {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )
    registry = TaskRegistry()
    registry.register(
        TaskType(id="job", description="probe", validate_config=lambda c: None, handler=handler)
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
        idle_interval_seconds=0.05,
    )
    return manager, store


async def test_stop_cancels_blocked_audit_emissions(tmp_path: Path) -> None:
    """Shutdown finding (audit drain): stop()'s bounded audit wait
    ignored the still-pending tasks — a blocked sink let the emission
    outlive the shutdown boundary (adapters closed, lifecycle reset,
    emission still running). Pending emissions must be cancelled and
    awaited; none may outlive stop()."""
    sink = _BlockedAuditSink()

    async def handler(context: Any) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store = await _one_account_manager(tmp_path, handler)
    manager._audit = AuditLog(sink)  # noqa: SLF001 — the sink under test
    try:
        await manager.start()
        manager.pause_account(ACCOUNT)  # sync control plane: fire-and-forget emit
        await asyncio.sleep(0.1)
        assert sink.calls == 1  # the emission is parked in the sink

        await asyncio.wait_for(manager.stop(), timeout=8.0)

        assert sink.cancelled >= 1, (
            "stop() returned while a blocked audit emission was still running — "
            "the emission outlived the shutdown boundary"
        )
    finally:
        await manager.stop()
        await store.close()


async def test_stop_reports_uncancellable_audit_sink(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Even a sink that suppresses cancellation must not wedge stop():
    shutdown stays bounded and REPORTS the abandoned emission instead of
    silently proceeding."""
    sink = _SwallowingAuditSink()

    async def handler(context: Any) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store = await _one_account_manager(tmp_path, handler)
    manager._audit = AuditLog(sink)  # noqa: SLF001 — the sink under test
    try:
        with caplog.at_level(logging.WARNING, logger="autoclaimer.workers"):
            await manager.start()
            manager.pause_account(ACCOUNT)
            await asyncio.sleep(0.1)
            assert sink.calls == 1

            await asyncio.wait_for(manager.stop(), timeout=8.0)

        assert sink.cancel_deliveries >= 1, (
            "stop() never even attempted to cancel the blocked emission"
        )
        assert any(
            "audit emission" in r.getMessage() and "abandoned" in r.getMessage()
            for r in caplog.records
        ), "stop() did not report the emission that even cancellation could not unwind"
    finally:
        sink.finished.set()
        await asyncio.sleep(0.05)  # let the abandoned emission finish
        await manager.stop()
        await store.close()


async def test_stop_bounded_with_cancellation_resistant_handler(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Shutdown finding (resistant handler): the post-cancel
    gather() had no second deadline — a handler suppressing
    CancelledError blocked stop() forever. stop() must stay bounded,
    report the surviving worker, and not claim a clean teardown."""
    release = asyncio.Event()
    in_handler = asyncio.Event()

    async def handler(context: Any) -> TaskOutcome:
        in_handler.set()
        while not release.is_set():  # suppresses EVERY cancellation
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store = await _one_account_manager(tmp_path, handler)
    try:
        with caplog.at_level(logging.WARNING, logger="autoclaimer.workers"):
            await manager.start()
            await asyncio.wait_for(in_handler.wait(), timeout=3.0)

            # NOT wait_for: on the bug, stop() parks on an un-cancellable
            # gather and wait_for's own reaping await would hang with it.
            stop_task = asyncio.create_task(manager.stop(timeout=0.3))
            _done, still = await asyncio.wait({stop_task}, timeout=3.0)
            assert not still, "stop() blocked indefinitely on a cancellation-resistant handler"
            exc = stop_task.exception()  # pragma: no cover - defensive
            assert exc is None, f"stop() failed: {exc!r}"

        assert any("still running" in r.getMessage() for r in caplog.records), (
            "stop() claimed teardown without reporting the un-cancellable worker"
        )
        status = await manager.status()
        assert status.workers[0].state is WorkerState.RUNNING, (
            "a worker that survived shutdown must not be reported as stopped"
        )
    finally:
        release.set()  # the resistant handler finishes; the worker unwinds
        stop_wait, _ = await asyncio.wait({stop_task}, timeout=3.0)
        if not stop_task.done():  # pragma: no cover - defensive
            stop_task.cancel()
            with suppress(asyncio.CancelledError):
                await stop_task
        with suppress(Exception):
            await manager.stop()  # cooperative now: drains the released worker
        task = manager._workers[ACCOUNT].task  # noqa: SLF001 — test cleanup
        if task is not None and not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
        await store.close()


async def test_uncancellable_timed_out_handler_does_not_wedge_worker(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """The timeout path cancelled the handler child and awaited it
    UNBOUNDED — a handler suppressing CancelledError wedged the whole
    account worker (row stranded running). The worker must proceed with
    a failed outcome, keep living, and report the abandoned handler."""
    release = asyncio.Event()

    async def handler(context: Any) -> TaskOutcome:
        while not release.is_set():  # suppresses EVERY cancellation
            try:
                await release.wait()
            except asyncio.CancelledError:
                continue
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store = await _one_account_manager(
        tmp_path,
        handler,
        settings={"task_timeout_seconds": 0.15},
    )
    try:
        with caplog.at_level(logging.WARNING, logger="autoclaimer.workers"):
            await manager.start()
            deadline = asyncio.get_running_loop().time() + 5.0
            row = await store.get_instance_by_ids(ACCOUNT, "job")
            while asyncio.get_running_loop().time() < deadline:
                row = await store.get_instance_by_ids(ACCOUNT, "job")
                if row.status is TaskInstanceState.FAILED:
                    break
                await asyncio.sleep(0.02)
            assert row.status is TaskInstanceState.FAILED, (
                f"cancellation-resistant timed-out handler wedged the worker: {row.status.value}"
            )

        assert any("ignored cancellation" in r.getMessage() for r in caplog.records), (
            "the abandoned handler was never reported"
        )
        worker_task = manager._workers[ACCOUNT].task  # noqa: SLF001
        assert worker_task is not None and not worker_task.done(), (
            "the account worker should be alive (abandoned handler), not wedged or dead"
        )
    finally:
        release.set()
        await asyncio.sleep(0.05)  # the abandoned child completes
        await manager.stop()
        await store.close()
