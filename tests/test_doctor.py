"""CLI diagnostics — `doctor` (roadmap item 15 — P2).

Strictly read-only: configuration is parsed WITHOUT migrating (doctor
never rewrites files — verified with an outdated-version config and a
registered migrator), database checks reuse the read-only integrity
diagnostics, credentials report availability and COUNT only.
"""

import asyncio
import json
from pathlib import Path
from typing import Any

import autoclaimer.config as config_module
from autoclaimer.cli import AutoClaimerController, CliSession
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType

ACCOUNT = 271485504361996288
OTHER = 380028190142042112


def _write(path: Path, doc: dict[str, Any]) -> None:
    path.write_text(json.dumps(doc, indent=2), encoding="utf-8")


CONFIG = {
    "version": 1,
    "channels": {"roll_claim": 111},
    "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
    "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
    "tasks": {"probe": {"parameters": {}}},
}
ACCOUNTS = {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["probe"]}]}


def _controller(
    tmp_path: Path, *, tasks: tuple[str, ...] = ("probe",)
) -> tuple[AutoClaimerController, TaskStateStore]:
    async def handler(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(CONFIG)
    accounts = parse_accounts_config(ACCOUNTS, app)
    registry = TaskRegistry()
    for task_id in tasks:
        registry.register(
            TaskType(
                id=task_id,
                description=f"Doctor probe {task_id}.",
                validate_config=lambda config: None,
                handler=handler,
            )
        )
    store = TaskStateStore(tmp_path / "state.db")
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store,
        limiter=RateLimiter(store=store),
        registry=registry,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.01,
    )
    return controller, store


async def _run_doctor(controller: AutoClaimerController, tmp_path: Path) -> str:
    output: list[str] = []
    session = CliSession(controller, read_line=None, write=output.append)  # type: ignore[arg-type]
    cfg = str(tmp_path / "config.json")
    acc = str(tmp_path / "accounts.json")
    assert await session.run_once(f"doctor {cfg} {acc}") is True
    return "\n".join(output)


async def test_healthy_setup_reports_all_ok(tmp_path: Path) -> None:
    _write(tmp_path / "config.json", CONFIG)
    _write(tmp_path / "accounts.json", ACCOUNTS)
    controller, store = _controller(tmp_path)
    await store.open()
    try:
        report = await _run_doctor(controller, tmp_path)
        assert "[OK] configuration" in report
        assert "[OK] database" in report
        assert "[OK] tasks — 1 registered" in report
        assert f"account {ACCOUNT}" in report
        assert "[OK] scheduler" in report
        assert "[FAIL]" not in report
        # Credential store absent (no key): WARN, never a FAIL, no secrets.
        assert "[WARN] credentials" in report
    finally:
        await store.close()


async def test_invalid_config_fails_first_with_hint(tmp_path: Path) -> None:
    broken = dict(CONFIG)
    del broken["channels"]  # actionable: missing required section
    _write(tmp_path / "config.json", broken)
    _write(tmp_path / "accounts.json", ACCOUNTS)
    controller, store = _controller(tmp_path)
    await store.open()
    try:
        report = await _run_doctor(controller, tmp_path)
        assert "[FAIL] configuration" in report
        assert "config.channels is required" in report
        assert "fix the reported field" in report
    finally:
        await store.close()


async def test_unregistered_task_warns_with_hint(tmp_path: Path) -> None:
    config_with_ghost = dict(CONFIG)
    config_with_ghost["tasks"] = {"probe": {"parameters": {}}, "ghost": {"parameters": {}}}
    _write(tmp_path / "config.json", config_with_ghost)
    _write(tmp_path / "accounts.json", ACCOUNTS)
    controller, store = _controller(tmp_path)  # registry has only "probe"
    await store.open()
    try:
        report = await _run_doctor(controller, tmp_path)
        assert "[WARN] tasks" in report
        assert "ghost" in report
        assert "retired, not executed" in report
    finally:
        await store.close()


async def test_corrupt_database_reports_findings(tmp_path: Path) -> None:
    _write(tmp_path / "config.json", CONFIG)
    _write(tmp_path / "accounts.json", ACCOUNTS)
    controller, store = _controller(tmp_path)
    await store.open()
    try:
        import sqlite3

        conn = sqlite3.connect(tmp_path / "state.db")
        with conn:
            conn.execute(
                "INSERT INTO scope_cooldowns (scope, deadline, server_directed) "
                "VALUES ('x', 'garbage', 1)"
            )
        conn.close()
        report = await _run_doctor(controller, tmp_path)
        assert "[FAIL] database" in report
        assert "scope_cooldowns" in report
    finally:
        await store.close()


async def test_doctor_never_migrates_an_outdated_config(tmp_path: Path) -> None:
    """READ-ONLY proof: with an outdated config version and a REGISTERED
    migrator, doctor parses without rewriting — no .bak, no version
    bump, byte-identical file."""
    _write(tmp_path / "config.json", CONFIG)
    _write(tmp_path / "accounts.json", ACCOUNTS)
    controller, store = _controller(tmp_path)
    await store.open()
    try:

        def _sample(doc: Any) -> dict[str, Any]:
            return {**doc, "channels": {**doc.get("channels", {}), "mu": None}}

        config_module._CONFIG_MIGRATIONS[1] = _sample
        config_module.CURRENT_CONFIG_VERSION = 2
        cfg_path = tmp_path / "config.json"
        before = cfg_path.read_bytes()
        report = await _run_doctor(controller, tmp_path)
        assert cfg_path.read_bytes() == before  # untouched
        assert not list(tmp_path.glob("*.bak"))
        # Doctor names the actionable version mismatch.
        assert "[FAIL] configuration" in report
        assert "Unsupported config.version" in report
    finally:
        config_module.CURRENT_CONFIG_VERSION = 1
        config_module._CONFIG_MIGRATIONS.pop(1, None)
        await store.close()


async def test_running_doctor_reports_worker_states(tmp_path: Path) -> None:
    _write(tmp_path / "config.json", CONFIG)
    _write(tmp_path / "accounts.json", ACCOUNTS)
    controller, store = _controller(tmp_path)
    await store.open()
    try:
        await controller.start()
        await asyncio.sleep(0.2)  # workers spawn
        report = await _run_doctor(controller, tmp_path)
        assert "worker=running" in report or "worker=idle" in report
    finally:
        await controller.shutdown()
