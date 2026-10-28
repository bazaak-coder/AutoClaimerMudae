"""Account-fatal policy: fatal adapter errors stop the account, not one task.

Owner directive (2026-10-28): an authentication failure (LoginFailure /
AuthFailure), a fatal gateway close (session invalidated, e.g. 4004), or a
CAPTCHA block must clear ALL tasks for the account and shut its worker
down instead of retrying. These tests pin the manager-level policy; the
adapter-side latch (no re-login, no client rebuild) is pinned in
``test_discord_adapter.py``.
"""

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.adapters import AccountFatalError
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager, WorkerState

ACCOUNT = 271485504361996288
OTHER = 380028190142042112

POLL_INTERVAL = 0.01
WAIT_TIMEOUT = 5.0


async def build_manager(
    tmp_path: Path,
    *,
    tasks_raw: dict[str, Any],
    accounts_raw: list[dict[str, Any]],
) -> tuple[WorkerManager, TaskStateStore, list[tuple[int, str]], list[int]]:
    """Manager harness whose handlers raise AccountFatalError on demand.

    Behaviors (task ``parameters.behavior``):

    - ``fatal``: raise AccountFatalError (simulates the adapter error a
      dead token / invalidated session produces).
    - anything else: succeed.
    """
    app = parse_app_config(
        {
            "version": 1,
            "settings": {"max_concurrent_accounts": 4, "shutdown_timeout_seconds": 10},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "tasks": tasks_raw,
        }
    )
    accounts = parse_accounts_config({"version": 1, "accounts": accounts_raw}, app)

    calls: list[tuple[int, str]] = []
    factory_calls: list[int] = []

    async def handler(context: TaskContext) -> TaskOutcome:
        calls.append((context.account_id, context.task_id))
        if context.config.get("behavior") == "fatal":
            raise AccountFatalError("authentication failed: improper token")
        return TaskOutcome(TaskStatus.SUCCEEDED)

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
    limiter = RateLimiter(store=store)

    def adapter_factory(account_id: int) -> MockAdapter:
        factory_calls.append(account_id)
        return MockAdapter()

    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=limiter,
        app_config=app,
        accounts=accounts,
        adapter_factory=adapter_factory,
        idle_interval_seconds=0.02,
    )
    return manager, store, calls, factory_calls


async def wait_until(check: Callable[[], Awaitable[bool]], description: str) -> None:
    deadline = asyncio.get_running_loop().time() + WAIT_TIMEOUT
    while asyncio.get_running_loop().time() < deadline:
        if await check():
            return
        await asyncio.sleep(POLL_INTERVAL)
    pytest.fail(f"condition never met: {description}")


def task_raw(behavior: str, *, priority: int | None = None) -> dict[str, Any]:
    raw: dict[str, Any] = {"parameters": {"behavior": behavior}}
    if priority is not None:
        raw["priority"] = priority
    return raw


async def instance_state(store: TaskStateStore, account_id: int, task_id: str) -> TaskInstanceState:
    return (await store.get_instance_by_ids(account_id, task_id)).status


async def all_failed(store: TaskStateStore, account_id: int, *task_ids: str) -> bool:
    for task_id in task_ids:
        if (await instance_state(store, account_id, task_id)) is not TaskInstanceState.FAILED:
            return False
    return True


async def test_account_fatal_error_fails_queue_disables_account_stops_worker(
    tmp_path: Path,
) -> None:
    """The exact owner scenario: bad credentials must not be retried.

    The fatal task runs first (highest priority); the account's two
    remaining tasks must fail terminally with an account-fatal message,
    the account must be disabled, its worker STOPPED, and the adapter
    factory must never be asked for a rebuild. The other account is
    untouched.
    """
    manager, store, calls, factory_calls = await build_manager(
        tmp_path,
        tasks_raw={
            "boom": task_raw("fatal", priority=10),
            "later_a": task_raw("noop"),
            "later_b": task_raw("noop"),
            "other_account_job": task_raw("noop"),
        },
        accounts_raw=[
            {"id": ACCOUNT, "tasks": ["boom", "later_a", "later_b"]},
            {"id": OTHER, "tasks": ["other_account_job"]},
        ],
    )
    async with manager:
        await wait_until(
            lambda: all_failed(store, ACCOUNT, "boom", "later_a", "later_b"),
            "all ACCOUNT instances failed",
        )

        async def other_succeeded() -> bool:
            return (
                await instance_state(store, OTHER, "other_account_job")
                is TaskInstanceState.SUCCEEDED
            )

        await wait_until(other_succeeded, "OTHER instance succeeded")

        status = await manager.status()
        by_id = {info.account_id: info for info in status.workers}
        assert by_id[ACCOUNT].state is WorkerState.STOPPED
        assert by_id[ACCOUNT].enabled is False
        assert by_id[OTHER].state is WorkerState.RUNNING

    for task_id in ("boom", "later_a", "later_b"):
        record = await store.get_instance_by_ids(ACCOUNT, task_id)
        assert record.status is TaskInstanceState.FAILED, task_id
        assert record.last_outcome is not None
        assert "account-fatal" in record.last_outcome.message, task_id
    assert await instance_state(store, OTHER, "other_account_job") is TaskInstanceState.SUCCEEDED

    assert manager.account_enabled(ACCOUNT) is False
    assert manager.account_enabled(OTHER) is True

    # Exactly one execution and one adapter build for the fatal account:
    # no retry, no rebuild, no second login attempt.
    assert calls.count((ACCOUNT, "boom")) == 1
    assert (ACCOUNT, "later_a") not in calls
    assert (ACCOUNT, "later_b") not in calls
    assert factory_calls.count(ACCOUNT) == 1


async def test_reenabling_after_fatal_is_the_explicit_revive_path(
    tmp_path: Path,
) -> None:
    """After the owner fixes the credential, enable revives the account.

    Terminal rows stay terminal (the store's identity is unique and
    ``create_instance`` does nothing on conflict), so the revived account
    has an empty queue: it runs again, executes nothing, and — crucially
    — does NOT loop on the dead credentials.
    """
    manager, store, calls, _ = await build_manager(
        tmp_path,
        tasks_raw={"boom": task_raw("fatal"), "later": task_raw("noop")},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["boom", "later"]}],
    )
    async with manager:
        await wait_until(
            lambda: all_failed(store, ACCOUNT, "boom", "later"),
            "both instances failed",
        )
        assert manager.account_enabled(ACCOUNT) is False
        assert calls.count((ACCOUNT, "boom")) == 1

        # Owner fixes the token and revives explicitly.
        manager.set_account_enabled(ACCOUNT, True)
        assert manager.account_enabled(ACCOUNT) is True

        async def account_running() -> bool:
            return await _worker_state(manager, ACCOUNT) is WorkerState.RUNNING

        await wait_until(account_running, "ACCOUNT worker revived")
        # Empty queue: nothing re-executes, nothing re-fails.
        await asyncio.sleep(0.1)
        assert calls.count((ACCOUNT, "boom")) == 1
        assert (ACCOUNT, "later") not in calls


async def _worker_state(manager: WorkerManager, account_id: int) -> WorkerState:
    status = await manager.status()
    for info in status.workers:
        if info.account_id == account_id:
            return info.state
    pytest.fail(f"no worker for account {account_id}")
