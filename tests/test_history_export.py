"""Execution-history export (ROADMAP item 22): filters, JSON Lines
structure, atomic no-overwrite writes, redaction, and worker safety."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from autoclaimer.cli import AutoClaimerController, CliSession
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import PersistenceError, TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskOutcome, TaskRegistry, TaskStatus, TaskType

ACCOUNT = 271485504361996288
OTHER = 380028190142042112
CHANNEL = 111111111111111111
TOKEN_SHAPED = "MTIzNDU2Nzg5MDEyMzQ1Njc4.GaBcDe.FgHiJkLmNoPqRsTuVwXyZ0123456789abcd"


def _config_doc() -> dict[str, object]:
    return {
        "version": 1,
        "channels": {"roll_claim": CHANNEL},
        "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
        "task_defaults": {"retry_limit": 0},
        "tasks": {"job": {"parameters": {}}},
    }


async def _seeded_controller(
    tmp_path: Path,
) -> tuple[AutoClaimerController, TaskStateStore]:
    """A controller whose job task failed once with a token-shaped message
    (proving redaction-at-write flows into the export)."""
    app = parse_app_config(_config_doc())
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["job"]}]}, app
    )
    registry = TaskRegistry()

    async def handler(context: object) -> TaskOutcome:
        return TaskOutcome(TaskStatus.FAILED_PERMANENT, message=f"login failed: {TOKEN_SHAPED}")

    registry.register(
        TaskType(id="job", description="probe", validate_config=lambda c: None, handler=handler)
    )
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store,
        limiter=RateLimiter(store=store),
        registry=registry,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
        config_path=str(tmp_path / "config.json"),
        accounts_path=str(tmp_path / "accounts.json"),
    )
    await controller.start()
    deadline = asyncio.get_running_loop().time() + 5.0
    row = await store.get_instance_by_ids(ACCOUNT, "job")
    while asyncio.get_running_loop().time() < deadline:
        row = await store.get_instance_by_ids(ACCOUNT, "job")
        if row.status is TaskInstanceState.FAILED:
            break
        await asyncio.sleep(0.02)
    assert row.status is TaskInstanceState.FAILED
    return controller, store


async def test_export_writes_filtered_jsonl(tmp_path: Path) -> None:
    controller, store = await _seeded_controller(tmp_path)
    session = CliSession(controller, read_line=None, write=lambda line: None)  # type: ignore[arg-type]
    try:
        target = tmp_path / "export.jsonl"
        result = await session._cmd_export([str(target)])
        assert "1 history row" in result
        lines = target.read_text(encoding="utf-8").splitlines()
        assert len(lines) == 1
        doc = json.loads(lines[0])
        assert doc["account_id"] == ACCOUNT
        assert doc["task_id"] == "job"
        assert doc["status"] == "failed"
        assert doc["attempt"] == 1
        assert isinstance(doc["recorded_at"], str) and doc["recorded_at"]
        # Redaction-at-write flows through: the token-shaped message is
        # already scrubbed in the DATABASE, so the export is clean.
        assert TOKEN_SHAPED not in lines[0]
        assert "[REDACTED-TOKEN]" in doc["message"]

        # Account filter excludes everything else.
        other = tmp_path / "other.jsonl"
        await session._cmd_export([str(other), "account", str(OTHER)])
        assert other.read_text(encoding="utf-8") == ""

        # Date filter: since in the future excludes the row; since now
        # (UTC-aware) includes it.
        future = tmp_path / "future.jsonl"
        await session._cmd_export(
            [str(future), "since", (datetime.now(UTC) + timedelta(days=1)).strftime("%Y-%m-%d")]
        )
        assert future.read_text(encoding="utf-8") == ""
        past = tmp_path / "past.jsonl"
        await session._cmd_export([str(past), "since", "2020-01-01", "until", "2030-01-01"])
        assert len(past.read_text(encoding="utf-8").splitlines()) == 1
    finally:
        await controller.shutdown()
        await store.close()


async def test_export_refuses_overwrite_and_bad_filters(tmp_path: Path) -> None:
    controller, store = await _seeded_controller(tmp_path)
    session = CliSession(controller, read_line=None, write=lambda line: None)  # type: ignore[arg-type]
    try:
        target = tmp_path / "exists.jsonl"
        target.write_text("precious", encoding="utf-8")
        with pytest.raises(ValueError, match="refusing to overwrite"):
            await session._cmd_export([str(target)])
        assert target.read_text(encoding="utf-8") == "precious"
        with pytest.raises(ValueError, match="key/value"):
            await session._cmd_export([str(tmp_path / "x.jsonl"), "account"])
        with pytest.raises(ValueError, match="unknown export filter"):
            await session._cmd_export([str(tmp_path / "x.jsonl"), "bogus", "1"])
        with pytest.raises(ValueError, match="invalid date"):
            await session._cmd_export([str(tmp_path / "x.jsonl"), "since", "not-a-date"])
        assert not (tmp_path / "x.jsonl").exists()  # nothing written on bad args
    finally:
        await controller.shutdown()
        await store.close()


async def test_export_atomic_no_partial_file_on_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A write failure must leave NO file behind (atomic replace only)."""
    controller, store = await _seeded_controller(tmp_path)
    try:
        import autoclaimer.persistence as persistence_module

        original = persistence_module.TaskStateStore.export_history

        async def _exploding_export(self: object, **kwargs: object) -> list[dict[str, object]]:
            raise PersistenceError("database is locked (injected)")

        monkeypatch.setattr(persistence_module.TaskStateStore, "export_history", _exploding_export)
        target = tmp_path / "atomic.jsonl"
        with pytest.raises(PersistenceError):
            await controller.export_history(str(target), account_id=None, since=None, until=None)
        assert not target.exists()
        assert list(tmp_path.glob(".atomic.jsonl.*.tmp")) == []  # no temp litter
        monkeypatch.setattr(persistence_module.TaskStateStore, "export_history", original)
    finally:
        await controller.shutdown()
        await store.close()


async def test_export_while_workers_active(tmp_path: Path) -> None:
    """The export is a read-only snapshot: recurring work keeps running
    through it, unaffected (the serialized store queues the read briefly)."""
    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": CHANNEL},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "task_defaults": {"retry_limit": 5},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.02},
            "tasks": {"loop": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["loop"]}]}, app
    )
    registry = TaskRegistry()
    executions = 0

    async def handler(context: object) -> TaskOutcome:
        nonlocal executions
        executions += 1
        return TaskOutcome(
            TaskStatus.FAILED_RECOVERABLE,
            retry_at=datetime.now(UTC) + timedelta(seconds=0.1),
            message="round",
        )

    registry.register(
        TaskType(id="loop", description="p", validate_config=lambda c: None, handler=handler)
    )
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store,
        limiter=RateLimiter(store=store),
        registry=registry,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
    )
    await controller.start()
    try:
        await asyncio.sleep(0.3)  # rounds accumulate
        before = executions
        target = tmp_path / "live.jsonl"
        await controller.export_history(str(target), account_id=ACCOUNT, since=None, until=None)
        await asyncio.sleep(0.3)
        assert executions > before, "workers stopped or were interrupted by the export"
        assert target.exists()
    finally:
        await controller.shutdown()
        await store.close()
