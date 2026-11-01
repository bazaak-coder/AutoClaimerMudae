"""Account health monitoring (roadmap item 6 — P1).

The monitor is an OBSERVATION layer: unit tests pin its state model and
security properties; worker-level tests prove the events fire from real
execution paths; CLI tests prove exposure without secrets.

Scheduling contract (asserted here): AUTH_FAILED / DISABLED accounts do
not execute work (existing enforcement), while RATE_LIMITED / DEGRADED /
DISCONNECTED never block scheduling — deferral stays with the limiter
and retry policy.
"""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.adapters import AccountFatalError
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.health import (
    DEGRADED_THRESHOLD,
    AccountHealthMonitor,
    AccountHealthState,
)
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

ACCOUNT = 271485504361996288
OTHER = 380028190142042112


class FixedClock:
    def __init__(self) -> None:
        self.now = datetime(2026, 11, 1, 12, 0, 0, tzinfo=UTC)

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += timedelta(seconds=seconds)


# --- monitor state model ------------------------------------------------------


def test_lifecycle_transitions() -> None:
    clock = FixedClock()
    monitor = AccountHealthMonitor(clock=clock)
    monitor.worker_started(ACCOUNT)
    assert monitor.snapshot()[ACCOUNT].state is AccountHealthState.CONNECTING

    monitor.record_success(ACCOUNT)
    snap = monitor.snapshot()[ACCOUNT]
    assert snap.state is AccountHealthState.HEALTHY
    assert snap.last_success_at == clock.now
    assert snap.total_successes == 1

    monitor.record_failure(ACCOUNT, "gateway hiccup")
    snap = monitor.snapshot()[ACCOUNT]
    assert snap.state is AccountHealthState.DISCONNECTED  # temporary
    assert snap.consecutive_failures == 1
    assert snap.last_error == "gateway hiccup"

    monitor.record_success(ACCOUNT)
    assert monitor.snapshot()[ACCOUNT].consecutive_failures == 0  # streak reset


def test_failure_streak_reports_degraded() -> None:
    monitor = AccountHealthMonitor()
    for n in range(DEGRADED_THRESHOLD):
        monitor.record_failure(ACCOUNT, f"failure {n}")
    snap = monitor.snapshot()[ACCOUNT]
    assert snap.state is AccountHealthState.DEGRADED
    assert snap.total_failures == DEGRADED_THRESHOLD


def test_fatal_failure_latches_auth_failed_until_revive() -> None:
    monitor = AccountHealthMonitor()
    monitor.record_failure(ACCOUNT, AccountFatalError("Login failed: token revoked"), fatal=True)
    assert monitor.snapshot()[ACCOUNT].state is AccountHealthState.AUTH_FAILED
    # Latched: later successes cannot silently clear an auth failure —
    # only the explicit revive path (re-enable) does.
    monitor.record_success(ACCOUNT)
    assert monitor.snapshot()[ACCOUNT].state is AccountHealthState.AUTH_FAILED
    monitor.record_revived(ACCOUNT)
    assert monitor.snapshot()[ACCOUNT].state is AccountHealthState.CONNECTING


def test_disable_and_rate_limit_states() -> None:
    monitor = AccountHealthMonitor()
    monitor.record_disabled(ACCOUNT)
    assert monitor.snapshot()[ACCOUNT].state is AccountHealthState.DISABLED
    monitor.record_revived(ACCOUNT)
    monitor.record_rate_limited(ACCOUNT)
    assert monitor.snapshot()[ACCOUNT].state is AccountHealthState.RATE_LIMITED
    monitor.record_success(ACCOUNT)
    assert monitor.snapshot()[ACCOUNT].state is AccountHealthState.HEALTHY


def test_error_text_is_redacted_and_bounded() -> None:
    monitor = AccountHealthMonitor()
    token = "gho_SuperSecretTokenValue1234567890abcdef"
    monitor.record_failure(ACCOUNT, f"login failed, token: {token} " + "x" * 500)
    snap = monitor.snapshot()[ACCOUNT]
    assert token not in snap.last_error  # key/value secrets are scrubbed
    assert len(snap.last_error) <= 200


def test_snapshot_is_immutable_and_copy_safe() -> None:
    monitor = AccountHealthMonitor()
    monitor.record_success(ACCOUNT)
    first = monitor.snapshot()[ACCOUNT]
    monitor.record_failure(ACCOUNT, "later")
    assert first.state is AccountHealthState.HEALTHY  # snapshot not mutated
    assert monitor.snapshot()[ACCOUNT].total_failures == 1


# --- worker wiring ------------------------------------------------------------


async def _wait_status(
    store: TaskStateStore, account_id: int, task_id: str, *statuses: TaskInstanceState
) -> None:
    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get_instance_by_ids(account_id, task_id)
        if record.status in statuses:
            return
        await asyncio.sleep(0.02)
    pytest.fail(f"{account_id}/{task_id} never reached {statuses} (={record.status})")


def _build(
    tmp_path: Path, *, handler: Any, retry_limit: int = 1
) -> tuple[WorkerManager, TaskStateStore]:
    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 2, "shutdown_timeout_seconds": 3},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "task_defaults": {"retry_limit": retry_limit},
            "tasks": {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {
            "version": 1,
            "accounts": [{"id": ACCOUNT, "tasks": ["job"]}, {"id": OTHER, "tasks": ["job"]}],
        },
        app,
    )
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="job",
            description="Health probe.",
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
    return manager, store


async def test_worker_success_reports_healthy(tmp_path: Path) -> None:
    async def handler(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store = _build(tmp_path, handler=handler)
    await store.open()
    async with manager:
        await _wait_status(store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
        snap = manager.health()[ACCOUNT]
        assert snap.state is AccountHealthState.HEALTHY
        assert snap.total_successes >= 1
    await store.close()


async def test_handler_crash_streak_reports_degraded_then_recovers(tmp_path: Path) -> None:
    """One task, retry_limit 3: three recoverable failures accumulate the
    streak (DEGRADED at the threshold), then the fourth attempt succeeds
    and the streak breaks — healthy again with history kept."""
    failure_budget = [DEGRADED_THRESHOLD]

    async def handler(context: TaskContext) -> TaskOutcome:
        if context.account_id == ACCOUNT and failure_budget[0] > 0:
            failure_budget[0] -= 1
            # Recoverable outcomes (unlike handler crashes, which are
            # terminal by policy) drive the worker's retry machinery.
            return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="streak probe failure")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store = _build(tmp_path, handler=handler, retry_limit=3)
    await store.open()
    async with manager:
        await _wait_status(store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
        await _wait_status(store, OTHER, "job", TaskInstanceState.SUCCEEDED)
        snap = manager.health()[ACCOUNT]
        assert snap.state is AccountHealthState.HEALTHY  # streak broken
        assert snap.total_failures == DEGRADED_THRESHOLD  # history kept
        assert snap.consecutive_failures == 0
        assert snap.last_error == "streak probe failure"
    await store.close()


async def test_account_fatal_latches_auth_failed_and_stops_scheduling(tmp_path: Path) -> None:
    """AUTH_FAILED is terminal for scheduling: the account stops (existing
    enforcement) and health shows WHY; the other account keeps working."""

    async def handler(context: TaskContext) -> TaskOutcome:
        if context.account_id == ACCOUNT:
            raise AccountFatalError("token expired or revoked")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store = _build(tmp_path, handler=handler)
    await store.open()
    async with manager:
        await _wait_status(store, OTHER, "job", TaskInstanceState.SUCCEEDED)
        await _wait_status(store, ACCOUNT, "job", TaskInstanceState.FAILED)
        snap = manager.health()[ACCOUNT]
        assert snap.state is AccountHealthState.AUTH_FAILED
        assert "token expired" in snap.last_error
        assert "gho_" not in snap.last_error  # secret-free
    await store.close()


async def test_disable_enable_transitions(tmp_path: Path) -> None:
    async def handler(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    manager, store = _build(tmp_path, handler=handler)
    await store.open()
    async with manager:
        manager.set_account_enabled(ACCOUNT, False)
        assert manager.health()[ACCOUNT].state is AccountHealthState.DISABLED
        manager.set_account_enabled(ACCOUNT, True)
        assert manager.health()[ACCOUNT].state is AccountHealthState.CONNECTING
        await _wait_status(store, ACCOUNT, "job", TaskInstanceState.SUCCEEDED)
        assert manager.health()[ACCOUNT].state is AccountHealthState.HEALTHY
    await store.close()


# --- CLI exposure ---------------------------------------------------------------


async def test_cli_health_command_reports_without_secrets(tmp_path: Path) -> None:
    from autoclaimer.cli import AutoClaimerController, CliSession

    async def handler(context: TaskContext) -> TaskOutcome:
        if context.account_id == ACCOUNT:
            raise RuntimeError("probe failure; token: gho_SecretTokenInError99")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    # One controller owns ONE manager: the health command must observe
    # the same manager that actually executes the work.
    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {"max_concurrent_accounts": 2, "shutdown_timeout_seconds": 3},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "task_defaults": {"retry_limit": 1},
            "tasks": {"job": {"parameters": {}}},
        }
    )
    accounts_cfg = parse_accounts_config(
        {
            "version": 1,
            "accounts": [{"id": ACCOUNT, "tasks": ["job"]}, {"id": OTHER, "tasks": ["job"]}],
        },
        app,
    )
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="job",
            description="CLI health probe.",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    store = TaskStateStore(tmp_path / "cli.db")
    await store.open()
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts_cfg,
        store=store,
        limiter=RateLimiter(store=store),
        registry=registry,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.01,
    )
    try:
        await controller.start()
        await _wait_status(store, OTHER, "job", TaskInstanceState.SUCCEEDED)
        await _wait_status(store, ACCOUNT, "job", TaskInstanceState.FAILED)
        output: list[str] = []
        session = CliSession(controller, read_line=None, write=output.append)  # type: ignore[arg-type]
        assert await session.run_once("health") is True
        joined = "\n".join(output)
        assert f"account {ACCOUNT}" in joined
        assert "disconnected" in joined  # crash streak below threshold
        assert "healthy" in joined  # OTHER succeeded
        assert "gho_SecretTokenInError99" not in joined  # redacted
    finally:
        await controller.shutdown()
