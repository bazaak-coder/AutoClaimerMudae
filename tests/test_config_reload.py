"""Safe runtime configuration reload (roadmap item 13 — P2).

Contract under test (defined before implementation):
- the new configuration is FULLY validated first — an invalid reload is
  rejected and the running configuration is untouched;
- the reloadable surface applies atomically (task definitions, channels,
  policy layers, enabled/timeout/retry, logging) and takes effect from
  the next task execution;
- restart-required settings (max_concurrent_accounts, send_jitter) are
  REPORTED, never partially applied;
- in-flight executions keep the snapshot they were claimed with;
- the change is recorded as a `configuration.changed` audit event.
"""

import asyncio
import json
from datetime import UTC
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.cli import AutoClaimerController, CliSession
from autoclaimer.config import load_config, parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import TaskInstanceState, TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType

ACCOUNT = 271485504361996288
OTHER = 380028190142042112
CHANNEL_A = 111111111111111111
CHANNEL_B = 222222222222222222


def _write(path: Path, document: dict[str, Any]) -> None:
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")


def _config_doc(
    *, tasks: dict[str, Any] | None = None, settings: dict[str, Any] | None = None
) -> dict[str, Any]:
    return {
        "version": 1,
        "channels": {"roll_claim": CHANNEL_A},
        "settings": {
            "max_concurrent_accounts": 1,
            "shutdown_timeout_seconds": 3,
            **(settings or {}),
        },
        "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
        "tasks": tasks if tasks is not None else {"probe": {"parameters": {}}},
    }


def _accounts_doc(
    *, accounts: tuple[int, ...] = (ACCOUNT,), tasks: tuple[str, ...] = ("probe",)
) -> dict[str, Any]:
    return {
        "version": 1,
        "accounts": [{"id": account, "tasks": list(tasks)} for account in accounts],
    }


async def _wait_status(
    store: TaskStateStore, account: int, task: str, *statuses: TaskInstanceState
) -> Any:
    deadline = asyncio.get_running_loop().time() + 5.0
    while asyncio.get_running_loop().time() < deadline:
        record = await store.get_instance_by_ids(account, task)
        if record.status in statuses:
            return record
        await asyncio.sleep(0.02)
    pytest.fail(f"{account}/{task} never reached {statuses} (={record.status})")


def _controller(
    tmp_path: Path,
) -> tuple[AutoClaimerController, TaskStateStore, dict[int, list[int]]]:
    seen_channels: dict[int, list[int]] = {}

    async def handler(context: TaskContext) -> TaskOutcome:
        seen_channels.setdefault(context.account_id, []).append(context.config["channel"])
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(_config_doc())
    accounts = parse_accounts_config(_accounts_doc(), app)
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="probe",
            description="Reload probe.",
            validate_config=lambda config: None,
            handler=handler,
            channel_role="roll_claim",
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
    return controller, store, seen_channels


# --- reloadable surface -----------------------------------------------------------


async def test_reload_channel_change_reaches_new_task(tmp_path: Path) -> None:
    """The reloadable surface includes GLOBAL CHANNELS: after a reload
    that changes roll_claim, a newly materialized task executes on the
    new channel."""
    controller, store, seen = _controller(tmp_path)
    await store.open()
    try:
        await controller.start()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.SUCCEEDED)

        doc = _config_doc(tasks={"later": {"parameters": {}}})
        doc["channels"] = {"roll_claim": CHANNEL_B}

        async def handler(context: TaskContext) -> TaskOutcome:
            seen.setdefault(context.account_id, []).append(context.config["channel"])
            return TaskOutcome(TaskStatus.SUCCEEDED)

        controller._manager._registry.register(  # noqa: SLF001
            TaskType(
                id="later",
                description="Later probe.",
                validate_config=lambda config: None,
                handler=handler,
                channel_role="roll_claim",
            )
        )
        _write(tmp_path / "config.json", doc)
        _write(tmp_path / "accounts.json", _accounts_doc(tasks=("later",)))
        cfg_path = str(tmp_path / "config.json")
        acc_path = str(tmp_path / "accounts.json")
        await controller.reload_config(cfg_path, acc_path)
        await _wait_status(store, ACCOUNT, "later", TaskInstanceState.SUCCEEDED)
        assert seen[ACCOUNT][-1] == CHANNEL_B  # new channel took effect
    finally:
        await controller.shutdown()


# --- validation gate + restart-required ----------------------------------------------


async def test_invalid_reload_is_rejected_running_config_untouched(tmp_path: Path) -> None:
    controller, store, _seen = _controller(tmp_path)
    await store.open()
    try:
        await controller.start()
        # New config file is INVALID (missing channels).
        _write(tmp_path / "config.json", {"version": 1, "tasks": {}})
        _write(tmp_path / "accounts.json", _accounts_doc())
        with pytest.raises(Exception, match="channels"):
            await controller.reload_config(
                str(tmp_path / "config.json"), str(tmp_path / "accounts.json")
            )
        # Running configuration unchanged: the original task still runs.
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.SUCCEEDED)
    finally:
        await controller.shutdown()


async def test_restart_required_settings_are_reported_not_applied(tmp_path: Path) -> None:
    controller, store, _seen = _controller(tmp_path)
    await store.open()
    output: list[str] = []
    session = CliSession(controller, read_line=None, write=output.append)  # type: ignore[arg-type]
    try:
        await controller.start()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.SUCCEEDED)
        _write(
            tmp_path / "config.json",
            _config_doc(settings={"max_concurrent_accounts": 8, "send_jitter_seconds": [0.1, 0.2]}),
        )
        _write(tmp_path / "accounts.json", _accounts_doc())
        cmd = f"reload {tmp_path / 'config.json'} {tmp_path / 'accounts.json'}"
        assert await session.run_once(cmd) is True
        joined = "\n".join(output)
        assert "restart required" in joined
        assert "max_concurrent_accounts 1 -> 8" in joined
        assert "send_jitter_seconds" in joined
        assert "applied atomically" in joined
        # Running value kept until restart.
        assert controller._app_config.settings.max_concurrent_accounts == 8  # noqa: SLF001
        # (the app_config reflects the new file; the EXECUTION slots keep
        # the old bound until restart — that is the documented semantics)
    finally:
        await controller.shutdown()


# --- audit ---------------------------------------------------------------------------


async def test_reload_is_audited(tmp_path: Path) -> None:
    controller, store, _seen = _controller(tmp_path)
    await store.open()
    try:
        await controller.start()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.SUCCEEDED)
        _write(tmp_path / "config.json", _config_doc())
        _write(tmp_path / "accounts.json", _accounts_doc())
        cfg_path = str(tmp_path / "config.json")
        acc_path = str(tmp_path / "accounts.json")
        await controller.reload_config(cfg_path, acc_path)
        events = await store.audit_events(limit=5)
        assert any(row[1] == "configuration" and row[2] == "changed" for row in events)
    finally:
        await controller.shutdown()


# --- snapshot semantics -----------------------------------------------------------------


async def test_inflight_execution_keeps_its_snapshot(tmp_path: Path) -> None:
    """A task claimed before the reload completes on the configuration
    it was resolved with (channel A), even though the reload switched to
    channel B while it ran."""
    started = asyncio.Event()
    release = asyncio.Event()
    observed: list[int] = []

    controller, store, _seen = _controller(tmp_path)

    async def blocking_handler(context: TaskContext) -> TaskOutcome:
        started.set()
        await release.wait()
        observed.append(context.config["channel"])
        return TaskOutcome(TaskStatus.SUCCEEDED)

    controller._manager._registry._types["probe"] = TaskType(  # noqa: SLF001
        id="probe",
        description="Blocking probe.",
        validate_config=lambda config: None,
        handler=blocking_handler,
        channel_role="roll_claim",
    )
    await store.open()
    try:
        await controller.start()
        deadline = asyncio.get_running_loop().time() + 3.0
        while asyncio.get_running_loop().time() < deadline and not started.is_set():
            await asyncio.sleep(0.02)
        assert started.is_set()  # claimed and running on the OLD config

        doc = _config_doc()
        doc["channels"] = {"roll_claim": CHANNEL_B}
        _write(tmp_path / "config.json", doc)
        _write(tmp_path / "accounts.json", _accounts_doc())
        cfg_path = str(tmp_path / "config.json")
        acc_path = str(tmp_path / "accounts.json")
        await controller.reload_config(cfg_path, acc_path)
        release.set()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.SUCCEEDED)
        assert observed == [CHANNEL_A]  # snapshot, not the reloaded value
    finally:
        await controller.shutdown()


# --- BUG-029: `enabled: false` must gate EXECUTION, not just materialization ---


def _recurring_controller(
    tmp_path: Path,
    executions: list[float],
) -> tuple[AutoClaimerController, TaskStateStore]:
    """A controller whose task succeeds and reschedules 0.4s out — the
    recurring shape that always leaves a live DUE waiting row."""
    from datetime import datetime, timedelta

    async def handler(context: TaskContext) -> TaskOutcome:
        executions.append(asyncio.get_running_loop().time())
        return TaskOutcome(
            TaskStatus.SUCCEEDED,
            retry_at=datetime.now(UTC) + timedelta(seconds=2.0),
        )

    app = parse_app_config(_config_doc())
    accounts = parse_accounts_config(_accounts_doc(), app)
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="probe",
            description="Recurring probe.",
            validate_config=lambda config: None,
            handler=handler,
            channel_role="roll_claim",
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
        idle_interval_seconds=0.02,
    )
    return controller, store


async def test_reload_disabled_task_stops_executing(tmp_path: Path) -> None:
    """BUG-029: disabling a task via reload must stop a recurring task
    with a live due waiting row — the materialization gate alone let it
    keep executing (indefinitely: every round reschedules)."""
    executions: list[float] = []
    controller, store = _recurring_controller(tmp_path, executions)
    await store.open()
    try:
        await controller.start()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.WAITING)
        assert executions, "setup: at least one execution before the reload"

        doc = _config_doc(tasks={"probe": {"enabled": False, "parameters": {}}})
        _write(tmp_path / "config.json", doc)
        _write(tmp_path / "accounts.json", _accounts_doc())
        await controller.reload_config(
            str(tmp_path / "config.json"), str(tmp_path / "accounts.json")
        )

        await asyncio.sleep(2.6)  # several due windows pass while disabled
        record = await store.get_instance_by_ids(ACCOUNT, "probe")
        assert len(executions) == 1, (
            f"disabled task executed {len(executions) - 1} more time(s) after reload; "
            f"row={record.status.value}"
        )
        assert record.status is TaskInstanceState.WAITING  # paused, not retired
    finally:
        await controller.shutdown()


async def test_restart_with_disabled_task_does_not_execute(tmp_path: Path) -> None:
    """BUG-029 (restart leg): a full restart with `enabled: false` must
    not resume a waiting row materialized earlier — the row survives in
    state.db and was claimed anyway."""
    executions: list[float] = []
    controller, store = _recurring_controller(tmp_path, executions)
    await store.open()
    try:
        await controller.start()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.WAITING)
        assert executions, "setup: at least one execution before the stop"
        await controller.stop()

        doc = _config_doc(tasks={"probe": {"enabled": False, "parameters": {}}})
        _write(tmp_path / "config.json", doc)
        _write(tmp_path / "accounts.json", _accounts_doc())
        app, accounts = load_config(str(tmp_path / "config.json"), str(tmp_path / "accounts.json"))
        registry = controller.manager._registry  # noqa: SLF001 — same registry
        controller2 = AutoClaimerController(
            app_config=app,
            accounts=accounts,
            store=store,
            limiter=RateLimiter(store=store),
            registry=registry,
            adapter_factory=lambda account_id: MockAdapter(),
            idle_interval_seconds=0.02,
        )
        try:
            await controller2.start()
            await asyncio.sleep(2.6)  # the waiting row is due throughout
            record = await store.get_instance_by_ids(ACCOUNT, "probe")
            assert len(executions) == 1, (
                f"disabled task executed {len(executions) - 1} more time(s) "
                f"after restart; row={record.status.value}"
            )
            assert record.status is TaskInstanceState.WAITING
        finally:
            await controller2.shutdown()
    finally:
        if not controller.manager._stopped:  # noqa: SLF001 — stop() is restartable
            await controller.shutdown()


# --- BUG-030: reload must apply account `enabled` changes (runtime
# overrides preserved and reported) -----------------------------------------


async def test_reload_account_disable_stops_execution(tmp_path: Path) -> None:
    """BUG-030: `enabled: false` for a running account must apply on
    reload — the runtime overlay previously won forever and the reload
    reported success while the account kept executing."""
    executions: list[float] = []
    controller, store = _recurring_controller(tmp_path, executions)
    await store.open()
    try:
        await controller.start()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.WAITING)
        assert executions, "setup: at least one execution before the reload"

        _write(tmp_path / "config.json", _config_doc(tasks={"probe": {"parameters": {}}}))
        _write(
            tmp_path / "accounts.json",
            {
                "version": 1,
                "accounts": [{"id": ACCOUNT, "enabled": False, "tasks": ["probe"]}],
            },
        )
        await controller.reload_config(
            str(tmp_path / "config.json"), str(tmp_path / "accounts.json")
        )

        assert controller.manager.account_enabled(ACCOUNT) is False
        await asyncio.sleep(2.6)  # several due windows pass
        record = await store.get_instance_by_ids(ACCOUNT, "probe")
        assert len(executions) == 1, (
            f"disabled account executed {len(executions) - 1} more time(s); "
            f"row={record.status.value}"
        )
        assert record.status is TaskInstanceState.WAITING  # parked, not retired
    finally:
        await controller.shutdown()


async def test_reload_keeps_and_reports_runtime_enabled_override(tmp_path: Path) -> None:
    """BUG-030 companion: an EXPLICIT runtime control (enable/disable
    command, account-fatal stop) outranks the reloaded config — and the
    reload output must say so instead of silently shadowing it."""
    executions: list[float] = []
    controller, store = _recurring_controller(tmp_path, executions)
    await store.open()
    try:
        await controller.start()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.WAITING)
        controller.manager.set_account_enabled(ACCOUNT, True)  # explicit runtime control

        _write(tmp_path / "config.json", _config_doc(tasks={"probe": {"parameters": {}}}))
        _write(
            tmp_path / "accounts.json",
            {
                "version": 1,
                "accounts": [{"id": ACCOUNT, "enabled": False, "tasks": ["probe"]}],
            },
        )
        lines = await controller.reload_config(
            str(tmp_path / "config.json"), str(tmp_path / "accounts.json")
        )

        assert controller.manager.account_enabled(ACCOUNT) is True  # runtime wins
        assert any("271485504361996288" in line and "runtime" in line for line in lines), (
            f"override not reported: {lines}"
        )
    finally:
        await controller.shutdown()


# --- BUG-032: an account REMOVED by reload parks its worker, rows kept ---


async def test_reload_removed_account_parks_worker_and_keeps_rows(tmp_path: Path) -> None:
    """BUG-032: removing an account from accounts.json must park its
    worker (no further execution) WITHOUT retiring its rows — the
    previous behavior terminally failed every still-due row through the
    leftover worker ("task not assigned"), an irreversible change for a
    possibly temporary removal."""
    executions: list[float] = []
    controller, store = _recurring_controller(tmp_path, executions)
    await store.open()
    try:
        await controller.start()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.WAITING)
        assert executions, "setup: at least one execution before the reload"

        _write(tmp_path / "config.json", _config_doc(tasks={"probe": {"parameters": {}}}))
        _write(tmp_path / "accounts.json", {"version": 1, "accounts": []})
        await controller.reload_config(
            str(tmp_path / "config.json"), str(tmp_path / "accounts.json")
        )

        assert controller.manager.account_enabled(ACCOUNT) is False
        await asyncio.sleep(2.6)  # the waiting row is due throughout
        record = await store.get_instance_by_ids(ACCOUNT, "probe")
        assert len(executions) == 1, f"removed account executed {len(executions) - 1} more time(s)"
        assert record.status is TaskInstanceState.WAITING, (
            f"removed account's row was retired: {record.status.value} "
            f"({record.last_outcome.message if record.last_outcome else ''})"
        )
    finally:
        await controller.shutdown()


# --- review F2: a parked (removed/disabled) account must never retire rows
# from an iteration that was already past the enabled gate ---


async def test_execute_on_parked_account_leaves_row_untouched(tmp_path: Path) -> None:
    """Review F2 (BUG-032 follow-up): a worker iteration that passed the
    enabled gate BEFORE a reload removed/parked the account re-reads the
    profile inside _execute and would terminally retire the row ("task
    not assigned") — violating the rows-NOT-retired invariant. _execute
    must re-check the account gate and park instead."""
    executions: list[float] = []
    controller, store = _recurring_controller(tmp_path, executions)
    await store.open()
    try:
        await controller.start()
        await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.WAITING)
        # Remove the account (parks its worker) — then simulate the race:
        # the already-running iteration calls _execute with the due row.
        _write(tmp_path / "config.json", _config_doc(tasks={"probe": {"parameters": {}}}))
        _write(tmp_path / "accounts.json", {"version": 1, "accounts": []})
        await controller.reload_config(
            str(tmp_path / "config.json"), str(tmp_path / "accounts.json")
        )
        assert controller.manager.account_enabled(ACCOUNT) is False

        worker = controller.manager._workers[ACCOUNT]  # noqa: SLF001
        parked_record = await store.get_instance_by_ids(ACCOUNT, "probe")
        await controller.manager._execute(worker, parked_record)  # noqa: SLF001

        after = await store.get_instance_by_ids(ACCOUNT, "probe")
        assert after.status is TaskInstanceState.WAITING, (
            f"parked account's row was retired: {after.status.value} "
            f"({after.last_outcome.message if after.last_outcome else ''})"
        )
        assert after.id == parked_record.id
        assert not executions[1:]  # no execution while parked
    finally:
        await controller.shutdown()


# --- BUG-038: a pre-claim retirement must land in history end to end ---


async def test_removed_task_assignment_lands_in_history(tmp_path: Path) -> None:
    """BUG-038 (integration): removing a task from an account's
    assignment retires its due row through _terminal_without_claim →
    record_abandoned — the failure must be visible in task_history, not
    only in the instance listing."""
    executions: list[float] = []
    controller, store = _recurring_controller(tmp_path, executions)
    await store.open()
    try:
        await controller.start()
        record = await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.WAITING)
        assert executions, "setup: at least one execution before the reload"

        _write(tmp_path / "config.json", _config_doc(tasks={"probe": {"parameters": {}}}))
        _write(tmp_path / "accounts.json", _accounts_doc(tasks=()))
        await controller.reload_config(
            str(tmp_path / "config.json"), str(tmp_path / "accounts.json")
        )

        deadline = asyncio.get_running_loop().time() + 5.0
        while asyncio.get_running_loop().time() < deadline:
            record = await store.get_instance_by_ids(ACCOUNT, "probe")
            if record.status is TaskInstanceState.FAILED:
                break
            await asyncio.sleep(0.02)
        else:
            pytest.fail(f"retired row never failed: {record.status.value}")
        # The terminal state lands BEFORE its history row (the auxiliary
        # append is a separate store call — bounded-behind by contract),
        # so AWAIT the row instead of asserting it immediately: under
        # parallel-worker load the gap can exceed one poll interval.
        deadline = asyncio.get_running_loop().time() + 5.0
        rows: tuple[tuple[int, str, str, str], ...] = ()
        while asyncio.get_running_loop().time() < deadline:
            rows = await store.task_history(record.id)
            if any(
                status == "abandoned" and "not assigned" in message
                for _attempt, status, message, _ts in rows
            ):
                break
            await asyncio.sleep(0.02)
        assert any(
            status == "abandoned" and "not assigned" in message
            for _attempt, status, message, _ts in rows
        ), f"pre-claim retirement missing from history: {rows}"
    finally:
        await controller.shutdown()


# --- Reload generation mixing: one execution, ONE configuration snapshot ---


class _ParkingClaimStore:
    """Real-store delegation; claim() parks on an event the first time —
    the deterministic window where a reload swaps the configuration while
    an execution is between its config reads and its claim."""

    def __init__(self, inner: TaskStateStore, *, park_on: int = 1) -> None:
        self._inner = inner
        self.parked = asyncio.Event()
        self.release = asyncio.Event()
        self._park_on = park_on
        self._claims = 0

    async def claim(self, instance_id: int) -> Any:
        self._claims += 1
        if self._claims == self._park_on:
            self.parked.set()
            await self.release.wait()
        return await self._inner.claim(instance_id)

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)


async def _generation_controller(
    tmp_path: Path,
    handler: Any,
    store_wrapper: Any,
) -> tuple[AutoClaimerController, Any]:
    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": CHANNEL_A},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 3},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
            "task_defaults": {"retry_limit": 1},
            "tasks": {"probe": {"parameters": {"marker": "A"}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["probe"]}]}, app
    )
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="probe",
            description="Generation probe.",
            validate_config=lambda config: None,
            handler=handler,
            channel_role="roll_claim",
        )
    )
    real_store = store_wrapper._inner
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store_wrapper,
        limiter=RateLimiter(store=real_store),
        registry=registry,
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=0.02,
    )
    return controller, real_store


def _generation_b_docs() -> tuple[dict[str, Any], dict[str, Any]]:
    app_doc = {
        "version": 1,
        "channels": {"roll_claim": CHANNEL_B},
        "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 3},
        "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
        "task_defaults": {"retry_limit": 5},
        "tasks": {"probe": {"parameters": {"marker": "B"}}},
    }
    accounts_doc = {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["probe"]}]}
    return app_doc, accounts_doc


async def test_inflight_execution_never_mixes_channel_generation(
    tmp_path: Path,
) -> None:
    """High-priority race: ``_execute`` resolved parameters from
    generation A, then awaited (wait_scopes / slot / claim), and
    ``_execution_parameters`` read ``self._app_config.channels`` AFTER
    those awaits — a reload landing in the window produced ONE execution
    with old parameters on the NEW channel. Every configuration read for
    one execution must come from a single snapshot captured before the
    first await."""
    observed: dict[str, Any] = {}

    async def handler(context: TaskContext) -> TaskOutcome:
        observed["channel"] = context.config["channel"]
        observed["marker"] = context.config["marker"]
        return TaskOutcome(TaskStatus.SUCCEEDED)

    real = TaskStateStore(tmp_path / "state.db")
    await real.open()
    wrapper = _ParkingClaimStore(real)
    controller, store = await _generation_controller(tmp_path, handler, wrapper)
    try:
        await controller.start()
        await asyncio.wait_for(wrapper.parked.wait(), timeout=3.0)  # mid-claim

        app_doc, accounts_doc = _generation_b_docs()  # channel A -> B, marker A -> B
        app_b = parse_app_config(app_doc)
        controller.manager.update_configuration(  # noqa: SLF001 — the swap under test
            app_b, parse_accounts_config(accounts_doc, app_b)
        )
        wrapper.release.set()

        record = await _wait_status(store, ACCOUNT, "probe", TaskInstanceState.SUCCEEDED)
        assert observed["marker"] == "A"  # resolved before the first await
        assert observed["channel"] == CHANNEL_A, (
            f"mixed generations: parameters from A ({observed['marker']!r}) "
            f"but channel from B ({observed['channel']}) — expected {CHANNEL_A}"
        )
        assert record.status is TaskInstanceState.SUCCEEDED
    finally:
        await controller.shutdown()


async def test_inflight_execution_keeps_its_retry_limit_generation(
    tmp_path: Path,
) -> None:
    """Same race, retry-bound leg: the recoverable-failure branch
    re-resolved ``retry_limit`` from the CURRENT configuration after the
    awaits — an execution started under limit 1 could be retried under a
    swapped limit 5. The bound must come from the execution's own
    generation."""
    calls: list[int] = []

    async def handler(context: TaskContext) -> TaskOutcome:
        calls.append(context.account_id)
        return TaskOutcome(TaskStatus.FAILED_RECOVERABLE, message="transient")

    real = TaskStateStore(tmp_path / "state.db")
    await real.open()
    wrapper = _ParkingClaimStore(real, park_on=2)  # the bound-deciding attempt
    controller, store = await _generation_controller(tmp_path, handler, wrapper)
    try:
        await controller.start()
        await asyncio.wait_for(wrapper.parked.wait(), timeout=5.0)  # mid-claim #2

        app_doc, accounts_doc = _generation_b_docs()  # retry_limit 1 -> 5
        app_b = parse_app_config(app_doc)
        controller.manager.update_configuration(  # noqa: SLF001 — the swap under test
            app_b, parse_accounts_config(accounts_doc, app_b)
        )
        wrapper.release.set()

        # Generation A bounds the retries: attempt 1 >= limit 1 -> terminal.
        deadline = asyncio.get_running_loop().time() + 5.0
        record = await store.get_instance_by_ids(ACCOUNT, "probe")
        while asyncio.get_running_loop().time() < deadline:
            record = await store.get_instance_by_ids(ACCOUNT, "probe")
            if record.status is TaskInstanceState.FAILED:
                break
            await asyncio.sleep(0.02)
        assert record.status is TaskInstanceState.FAILED, (
            f"execution escaped its own generation's retry bound "
            f"(status={record.status.value}, attempts={record.attempt_count})"
        )
        assert record.last_outcome is not None
        assert "worker retry limit (1) exhausted" in record.last_outcome.message
        assert len(calls) == 2, calls  # limit 1 = initial attempt + one retry
    finally:
        await controller.shutdown()
