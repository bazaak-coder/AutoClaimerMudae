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
from autoclaimer.persistence import TaskInstanceState, TaskStateStore
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


async def test_export_atomic_no_partial_file_on_write_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """A WRITE failure (review finding: the old test failed the READ,
    never exercising the write path) must leave NO target file and NO
    temp litter — and surface as a contained ValueError."""
    import os as os_module

    def _exploding_link(src: object, dst: object, *args: object) -> None:
        raise OSError(28, "No space left on device")  # ENOSPC (injected)

    monkeypatch.setattr(os_module, "link", _exploding_link)
    controller, store = await _seeded_controller(tmp_path)
    try:
        target = tmp_path / "atomic.jsonl"
        with pytest.raises(ValueError, match="cannot safely publish"):
            await controller.export_history(str(target), account_id=None, since=None, until=None)
        assert not target.exists()
        assert list(tmp_path.glob(".atomic.jsonl.*.tmp")) == []  # no temp litter
    finally:
        await controller.shutdown()
        await store.close()


async def test_export_publish_refuses_a_file_created_in_the_window(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The conditional publish: a target created AFTER the exists()
    check is NOT silently overwritten (review finding on os.replace)."""
    import os as os_module

    controller, store = await _seeded_controller(tmp_path)
    try:
        target = tmp_path / "race.jsonl"
        real_link = os_module.link

        def _sneaky_link(src: str, dst: str, *args: object) -> None:
            Path(dst).write_text("someone else got here first", encoding="utf-8")
            real_link(src, dst, *args)  # now raises FileExistsError

        monkeypatch.setattr(os_module, "link", _sneaky_link)
        with pytest.raises(ValueError, match="refusing to overwrite"):
            await controller.export_history(str(target), account_id=None, since=None, until=None)
        assert target.read_text(encoding="utf-8") == "someone else got here first"
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
    rounds = asyncio.Event()

    async def handler(context: object) -> TaskOutcome:
        rounds.set()  # deterministic: one event per completed round
        return TaskOutcome(
            TaskStatus.FAILED_RECOVERABLE,
            retry_at=datetime.now(UTC) + timedelta(seconds=0.05),
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
        # Deterministic (review finding): synchronize on a completed
        # round via an event, never wall-clock; generous deadlines for
        # CI load; retry_limit 5 with 0.05s retries cannot exhaust
        # before several rounds land even under load.
        await asyncio.wait_for(rounds.wait(), timeout=5.0)
        target = tmp_path / "live.jsonl"
        await controller.export_history(str(target), account_id=ACCOUNT, since=None, until=None)
        assert target.exists()
        rounds.clear()
        await asyncio.wait_for(rounds.wait(), timeout=5.0)  # >=1 round AFTER the export
    finally:
        await controller.shutdown()
        await store.close()


async def test_export_cancellation_still_audits_a_completed_publish(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Review finding (Major): cancelling export_history while the
    thread-write runs must not strand a PUBLISHED file without its audit
    event — the write is shield-tracked and drained bounded; a completed
    publish still audits before the cancellation re-raises."""
    import autoclaimer.audit as audit_module

    events: list[str] = []
    original_emit = audit_module.AuditLog.emit

    async def _recording_emit(self: object, **event: object) -> None:
        if event.get("action") == "history_exported":
            events.append(str(event.get("detail")))
        await original_emit(self, **event)  # type: ignore[arg-type]

    monkeypatch.setattr(audit_module.AuditLog, "emit", _recording_emit)
    controller, store = await _seeded_controller(tmp_path)
    try:
        target = tmp_path / "cancel.jsonl"

        async def _cancelling_export() -> None:
            task = asyncio.create_task(
                controller.export_history(str(target), account_id=None, since=None, until=None)
            )
            await asyncio.sleep(0)  # let it reach the shielded write
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await _cancelling_export()
        await asyncio.sleep(0.05)  # the thread publishes; drain completes
        # Re-run the drain semantics: the file either exists with its
        # audit event, or the write was still in flight (warned, never
        # silently un-audited). Assert the invariant that matters:
        if target.exists():
            assert any(str(target) in detail for detail in events), (
                "published export without its history_exported audit event"
            )
        # And the contained path still works afterwards.
        final = tmp_path / "after.jsonl"
        result = await controller.export_history(
            str(final), account_id=None, since=None, until=None
        )
        assert "history row" in result
        assert any(str(final) in detail for detail in events)
    finally:
        await controller.shutdown()
        await store.close()
