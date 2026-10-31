"""Tests for the interactive CLI: parsing, controls, listings, shutdown."""

import asyncio
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.cli import AutoClaimerController, CliSession
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import PersistenceError, TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskOutcome as RegistryOutcome
from autoclaimer.registry import TaskRegistry, TaskStatus, TaskType

ACCOUNT = 271485504361996288
OTHER = 380028190142042112


class CliHarness:
    controller: AutoClaimerController
    session: CliSession
    output: list[str]
    store: TaskStateStore
    limiter: RateLimiter
    adapters: dict[int, MockAdapter]
    block_events: dict[tuple[int, str], asyncio.Event]


async def build_cli(
    tmp_path: Path,
    *,
    tasks_raw: dict[str, Any] | None = None,
    accounts_raw: list[dict[str, Any]] | None = None,
    shutdown_timeout: int = 15,
) -> CliHarness:
    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "settings": {
                "max_concurrent_accounts": 2,
                "shutdown_timeout_seconds": shutdown_timeout,
            },
            "tasks": tasks_raw if tasks_raw is not None else {"job": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": accounts_raw or [{"id": ACCOUNT, "tasks": ["job"]}]},
        app,
    )
    block_events: dict[tuple[int, str], asyncio.Event] = {}

    async def handler(context: Any) -> RegistryOutcome:
        if context.config.get("behavior") == "block":
            event = block_events.setdefault((context.account_id, context.task_id), asyncio.Event())
            await event.wait()
        return RegistryOutcome(TaskStatus.SUCCEEDED)

    registry = TaskRegistry()
    for task_id in app.tasks:
        registry.register(
            TaskType(
                id=task_id,
                description=f"cli test task {task_id}",
                validate_config=lambda config: None,
                handler=handler,
            )
        )
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    limiter = RateLimiter(store=store)
    adapters = {profile.id: MockAdapter() for profile in accounts.accounts}
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store,
        limiter=limiter,
        registry=registry,
        adapter_factory=lambda account_id: adapters[account_id],
        idle_interval_seconds=0.02,
    )
    harness = CliHarness()
    harness.controller = controller
    harness.output = []
    harness.store = store
    harness.limiter = limiter
    harness.adapters = adapters
    harness.block_events = block_events
    harness.session = CliSession(controller, read_line=None, write=harness.output.append)  # type: ignore[arg-type]
    return harness


def scripted(lines: list[str]) -> Any:
    iterator = iter(lines)

    async def read(prompt: str) -> str:
        try:
            return next(iterator)
        except StopIteration:
            raise EOFError from None

    return read


async def settled(harness: CliHarness) -> None:
    await asyncio.sleep(0.05)


# --- parsing and validation ---


async def test_help_and_unknown_and_empty_commands(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    assert await h.session.run_once("help") is True
    assert any("commands:" in line for line in h.output)
    assert await h.session.run_once("frobnicate") is True
    assert any("unknown command" in line for line in h.output)
    assert await h.session.run_once("") is True
    assert await h.session.run_once("   ") is True
    assert await h.session.run_once('pause "unbalanced') is True
    assert any("unbalanced quoting" in line for line in h.output)


async def test_account_argument_validation(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    for bad in ("pause abc", "pause 123", "enable", "disable 1 2"):
        output_before = len(h.output)
        assert await h.session.run_once(bad) is True
        assert any(line.startswith("error:") for line in h.output[output_before:])


async def test_tasks_state_argument_validation(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    assert await h.session.run_once("tasks bogus") is True
    assert any("unknown task state" in line for line in h.output)


# --- listings ---


async def test_accounts_listing_shows_assignments(tmp_path: Path) -> None:
    h = await build_cli(
        tmp_path,
        accounts_raw=[
            {"id": ACCOUNT, "tasks": ["job"]},
            {"id": OTHER, "tasks": []},
        ],
    )
    assert await h.session.run_once("accounts") is True
    text = "\n".join(h.output)
    assert str(ACCOUNT) in text and "job" in text
    assert str(OTHER) in text and "(no tasks)" in text


async def test_tasks_listing_filters_by_state(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    record = await h.store.create_instance(ACCOUNT, "job")
    await h.store.claim(record.id)
    await h.store.record_success(record.id, RegistryOutcome(TaskStatus.SUCCEEDED))
    assert await h.session.run_once("tasks") is True
    assert await h.session.run_once("tasks succeeded") is True
    text = "\n".join(h.output)
    assert "succeeded" in text


async def test_queue_listing_shows_priority_order(tmp_path: Path) -> None:
    h = await build_cli(
        tmp_path,
        tasks_raw={"low": {"priority": 1}, "high": {"priority": 9}},
        accounts_raw=[{"id": ACCOUNT, "tasks": ["low", "high"]}],
    )
    await h.controller.manager.start()
    await h.controller.manager.stop()
    assert await h.session.run_once(f"queue {ACCOUNT}") is True
    text = "\n".join(h.output)
    assert "priority=9" in text and "priority=1" in text


async def test_cooldowns_listing(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    from datetime import UTC, datetime, timedelta

    await h.limiter.record_cooldown(
        f"account:{ACCOUNT}",
        until=datetime.now(UTC) + timedelta(seconds=30),
        server_directed=True,
    )
    assert await h.session.run_once("cooldowns") is True
    text = "\n".join(h.output)
    assert f"account:{ACCOUNT}" in text and "(server)" in text


async def test_errors_listing(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    record = await h.store.create_instance(ACCOUNT, "job")
    await h.store.claim(record.id)
    await h.store.record_failure(
        record.id,
        RegistryOutcome(TaskStatus.FAILED_PERMANENT, message="adapter exploded"),
        retry_at=None,
    )
    assert await h.session.run_once("errors") is True
    text = "\n".join(h.output)
    assert "adapter exploded" in text and "1 failed" in text


# --- account controls ---


async def test_account_controls_round_trip(tmp_path: Path) -> None:
    h = await build_cli(
        tmp_path, accounts_raw=[{"id": ACCOUNT, "tasks": ["job"]}, {"id": OTHER, "tasks": []}]
    )
    for command in (
        "pause",
        "resume",
        f"pause {ACCOUNT}",
        f"resume {ACCOUNT}",
        f"disable {ACCOUNT}",
        f"enable {ACCOUNT}",
    ):
        output_before = len(h.output)
        assert await h.session.run_once(command) is True
        assert h.output[output_before], f"{command} produced no output"
    assert await h.session.run_once("accounts") is True
    text = "\n".join(h.output)
    assert "enabled" in text


async def test_disable_then_start_gates_new_work(tmp_path: Path) -> None:
    h = await build_cli(tmp_path, tasks_raw={"job": {"parameters": {"behavior": "block"}}})
    await h.store.create_instance(ACCOUNT, "job")  # pre-existing pending work
    assert await h.session.run_once(f"disable {ACCOUNT}") is True
    assert await h.session.run_once("start") is True
    await settled(h)
    record = await h.store.get_instance_by_ids(ACCOUNT, "job")
    assert record.status is TaskInstanceState.PENDING  # disabled: never claimed
    await h.session.run_once("stop")


# --- lifecycle ---


async def test_start_status_stop(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    assert await h.session.run_once("start") is True
    assert await h.session.run_once("status") is True
    assert any("running" in line for line in h.output)
    assert await h.session.run_once("stop") is True
    assert any("framework stopped" in line for line in h.output)


async def test_quit_requests_graceful_shutdown(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    assert await h.session.run_once("shutdown") is False


async def test_eof_shuts_down_gracefully(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    h.session._read_line = scripted(["accounts"])  # noqa: SLF001
    await h.session.run()
    assert any("EOF" in line for line in h.output)
    assert any("shutdown complete" in line for line in h.output)
    with pytest.raises(PersistenceError, match="not open"):
        await h.store.list_instances()


async def test_keyboard_interrupt_shuts_down_gracefully(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)

    async def interrupt(prompt: str) -> str:
        raise KeyboardInterrupt

    h.session._read_line = interrupt  # noqa: SLF001
    await h.session.run()
    assert any("interrupted" in line for line in h.output)
    assert any("shutdown complete" in line for line in h.output)
    with pytest.raises(PersistenceError, match="not open"):
        await h.store.list_instances()


async def test_shutdown_while_workers_active(tmp_path: Path) -> None:
    h = await build_cli(
        tmp_path, tasks_raw={"job": {"parameters": {"behavior": "block"}}}, shutdown_timeout=1
    )
    delivered_start = False

    async def read(prompt: str) -> str:
        nonlocal delivered_start
        if not delivered_start:
            delivered_start = True
            return "start"
        while (
            await h.store.get_instance_by_ids(ACCOUNT, "job")
        ).status is not TaskInstanceState.RUNNING:
            await asyncio.sleep(0.01)
        return "shutdown"

    h.session._read_line = read  # noqa: SLF001
    await h.session.run()
    assert any("shutdown complete" in line for line in h.output)
    assert h.controller.closed is True
    # No worker tasks may outlive the shutdown (all gathered or cancelled).
    pending = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    assert pending == []


async def test_output_redacts_tokens(tmp_path: Path) -> None:
    h = await build_cli(tmp_path)
    leaked = "MTIzNDU2Nzg5MDEyMzQ1Njc4.GaBcDe.FgHiJkLmNoPqRsTuVwXyZ0123456789abcd"
    record = await h.store.create_instance(ACCOUNT, "job")
    await h.store.claim(record.id)
    await h.store.record_failure(
        record.id,
        RegistryOutcome(TaskStatus.FAILED_PERMANENT, message=f"token {leaked} rejected"),
        retry_at=None,
    )
    assert await h.session.run_once("errors") is True
    text = "\n".join(h.output)
    assert leaked not in text
    assert "[REDACTED-TOKEN]" in text
