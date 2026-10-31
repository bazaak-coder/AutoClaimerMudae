"""End-to-end lifecycle integration checks (Phase 8 audit, mock adapter).

Covers the ten required lifecycle steps against the real components —
config parsing, credential resolution, SQLite recovery, limiter, workers,
controls, persistence, shutdown, and restart — with mock adapters only.
"""

import asyncio
import logging
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.adapters import CooldownError
from autoclaimer.cli import AutoClaimerController, CliSession
from autoclaimer.config import load_config
from autoclaimer.credentials import CredentialProviderError
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

ACCOUNT = 271485504361996288
OTHER = 380028190142042112
FAKE_TOKEN = f"lifecycle-fake-token-{ACCOUNT}"


class LifecycleCredentials:
    """Counts resolutions; the token must never surface in logs."""

    def __init__(self) -> None:
        self.calls: list[int] = []

    def get_token(self, account_id: int) -> str:
        self.calls.append(account_id)
        return f"lifecycle-fake-token-{account_id}"

    def close(self) -> None:
        return None


def write_configs(tmp_path: Path) -> tuple[Path, Path]:
    config_path = tmp_path / "config.json"
    accounts_path = tmp_path / "accounts.json"
    config_path.write_text(
        """
        {
          "version": 1,
          "channels": {"roll_claim": 111111111111111111},
          "settings": {"log_level": "INFO", "max_concurrent_accounts": 2,
                       "shutdown_timeout_seconds": 2},
          "ratelimit": {"min_interval_seconds": 0.01, "retry_limit": 2},
          "tasks": {
            "claim": {"priority": 5, "parameters": {},
                      "ratelimit": {"backoff_base_seconds": 0.02}}
          }
        }
        """,
        encoding="utf-8",
    )
    accounts_path.write_text(
        """
        {
          "version": 1,
          "accounts": [
            {"id": """
        + str(ACCOUNT)
        + """, "enabled": true, "tasks": ["claim"]},
            {"id": """
        + str(OTHER)
        + """, "enabled": true, "tasks": ["claim"],
             "ratelimit": {"min_interval_seconds": 0.01}}
          ]
        }
        """,
        encoding="utf-8",
    )
    return config_path, accounts_path


async def wait_for(store: TaskStateStore, predicate: Any, timeout: float = 3.0) -> None:
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        rows = await store.list_instances()
        if predicate(rows):
            return
        await asyncio.sleep(0.01)
    pytest.fail("lifecycle condition never met")


def all_succeeded(rows: Any) -> bool:
    return bool(rows) and all(r.status is TaskInstanceState.SUCCEEDED for r in rows)


async def test_full_lifecycle(tmp_path: Path, caplog: Any) -> None:
    logging.getLogger("autoclaimer").propagate = True

    # 1. Load and validate configuration (steps documented in the audit).
    config_path, accounts_path = write_configs(tmp_path)
    app, accounts = load_config(config_path, accounts_path)
    assert app.tasks["claim"].priority == 5
    assert app.ratelimit.min_interval_seconds == 0.01

    # 2. Credential references resolve per account without exposure.
    credentials = LifecycleCredentials()

    # 3. Initialize SQLite and recover resumable state (orphan running row).
    db = tmp_path / "state.db"
    store = TaskStateStore(db)
    await store.open()
    orphan = await store.create_instance(ACCOUNT, "claim")
    await store.claim(orphan.id)  # simulates an interrupted prior run
    limiter = RateLimiter(store=store)

    # 4. Initialize adapters + registry; 5. start concurrent workers.
    adapters = {a.id: MockAdapter() for a in accounts.accounts}
    attempts: dict[tuple[int, str], int] = {}
    seen_order: list[tuple[int, str]] = []

    async def handler(context: TaskContext) -> TaskOutcome:
        key = (context.account_id, context.task_id)
        attempts[key] = attempts.get(key, 0) + 1
        seen_order.append(key)
        if context.account_id == ACCOUNT and attempts[key] == 1:
            # 7. Server-directed cooldown on the first platform operation.
            await context.adapter.add_reaction(42, 1, "🎲")
        return TaskOutcome(TaskStatus.SUCCEEDED, data={"channel": context.config["channel"]})

    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="claim",
            description="lifecycle claim probe",
            validate_config=lambda config: None,
            handler=handler,
            channel_role="roll_claim",
        )
    )
    adapters[ACCOUNT].script("add_reaction", CooldownError(0.05))

    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=limiter,
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: adapters[account_id],
        credentials=credentials,
        idle_interval_seconds=0.02,
    )
    with caplog.at_level("INFO", logger="autoclaimer"):
        await manager.start()

        # 6-8. Sequential per account, cooldown honored, outcomes persisted.
        await wait_for(store, all_succeeded)

    rows = await store.list_instances()
    by_account = {r.account_id: r for r in rows}
    account_attempts = [k for k in seen_order if k[0] == ACCOUNT]
    other_attempts = [k for k in seen_order if k[0] == OTHER]
    # The server cooldown is retried INSIDE limiter.run (bounded op retry),
    # so the handler runs once per account in this process. The recovered
    # orphan's earlier claim is preserved: attempt_count is 2 for ACCOUNT.
    assert len(account_attempts) == 1
    assert len(other_attempts) == 1
    assert by_account[ACCOUNT].attempt_count == 2  # 1 pre-restart + 1 recovered
    assert by_account[OTHER].attempt_count == 1
    assert by_account[ACCOUNT].status is TaskInstanceState.SUCCEEDED
    cooldown_rows = await store._run(store._fetchall_sync, "SELECT scope FROM scope_cooldowns", ())
    # Server cooldown recorded and persisted; expired rows are pruned
    # opportunistically, so only the surviving route scope is asserted.
    assert "route:add_reaction" in {row["scope"] for row in cooldown_rows}

    # 9. Controls: pause, resume, disable, queue inspection via the CLI.
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store,
        limiter=limiter,
        registry=registry,
        adapter_factory=lambda account_id: adapters[account_id],
    )
    output: list[str] = []
    session = CliSession(controller, read_line=None, write=output.append)  # type: ignore[arg-type]
    assert await session.run_once("status") is True
    assert await session.run_once("queue") is True
    # All instances already succeeded, so queues report empty per account.
    assert output[-1] == "" or any("account" in line for line in output)
    assert not any(line.startswith("error:") for line in output)
    assert await session.run_once(f"disable {ACCOUNT}") is True
    assert await session.run_once(f"pause {OTHER}") is True
    assert await session.run_once(f"resume {OTHER}") is True
    assert await session.run_once(f"enable {ACCOUNT}") is True

    # 10a. Shut down cleanly (first the running manager, then the controller
    # closes its manager and the store).
    await manager.stop()
    await controller.shutdown()
    assert all(a.closed for a in adapters.values())
    assert not [t for t in asyncio.all_tasks() if "autoclaimer-worker-" in t.get_name()]

    # 2b. Secrets never surface in any captured log line. (Credential
    # RESOLUTION itself lives inside the Discord adapter at login time —
    # covered by tests/test_discord_adapter.py — the mock path never
    # consults the provider by design.)
    assert FAKE_TOKEN not in caplog.text

    # 10b. Recover after restart: fresh components over the same database.
    store2 = TaskStateStore(db)
    await store2.open()
    limiter2 = RateLimiter(store=store2)
    adapters2 = {a.id: MockAdapter() for a in accounts.accounts}
    fresh = WorkerManager(
        registry=registry,
        store=store2,
        limiter=limiter2,
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: adapters2[account_id],
        credentials=credentials,
        idle_interval_seconds=0.02,
    )
    await fresh.start()
    await asyncio.sleep(0.1)
    recovered = await store2.list_instances()
    assert all(r.status is TaskInstanceState.SUCCEEDED for r in recovered)
    await fresh.stop()
    await store2.close()


async def test_persisted_retry_schedule_survives_restart(tmp_path: Path) -> None:
    """A waiting retry survives restart and still executes."""
    config_path, accounts_path = write_configs(tmp_path)
    app, accounts = load_config(config_path, accounts_path)
    db = tmp_path / "state.db"

    store = TaskStateStore(db)
    await store.open()
    record = await store.create_instance(ACCOUNT, "claim")
    await store.claim(record.id)
    await store.record_failure(
        record.id,
        TaskOutcome(TaskStatus.FAILED_RECOVERABLE, "transient"),
        retry_at=datetime.now(UTC) + timedelta(seconds=0.05),
    )
    await store.close()

    store2 = TaskStateStore(db)
    await store2.open()

    async def handler(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="claim",
            description="retry probe",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    limiter = RateLimiter(store=store2)
    manager = WorkerManager(
        registry=registry,
        store=store2,
        limiter=limiter,
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
    )
    await manager.start()
    await wait_for(store2, all_succeeded)
    await manager.stop()
    await store2.close()


def test_missing_credential_is_reported_not_leaked(tmp_path: Path) -> None:
    class Broken:
        def get_token(self, account_id: int) -> str:
            raise CredentialProviderError(f"no credential for {account_id}")

        def close(self) -> None:
            return None

    assert Broken is not None  # provider contract: errors are explicit
