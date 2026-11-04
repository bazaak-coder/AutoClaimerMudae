"""Graceful shutdown and restart hardening (roadmap item 10 — P2).

REAL signals against REAL child processes running the full application
composition (config → store → limiter → manager → controller):

- SIGTERM and SIGINT trigger the GRACEFUL path: workers drained, stores
  closed, exit code 0 — and a blocked task is left honestly `running`.
- Forced termination (SIGKILL) is tested SEPARATELY: no graceful
  guarantees, but the database stays consistent and restart recovers.
- After either termination, a restart RESUMES the eligible work.
- A second signal is a guaranteed force-exit escape hatch.
- Adapters close and worker tasks do not leak (in-process assertions
  complement the process-level exit-code evidence: a leaked-task child
  would hang and fail the timeout).
"""

import asyncio
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from autoclaimer.persistence import TaskInstanceState, TaskStateStore

ACCOUNT = 271485504361996288

#: Full application composition in a child process. Modes:
#:   block — the task handler blocks forever (termination mid-execution)
#:   ok    — the task handler succeeds quickly (restart completion)
#: A "hang" variant (graceful trigger that never finishes) exercises the
#: second-signal escape hatch.
CHILD = r"""
import asyncio, os, sys

sys.path.insert(0, {repo!r})
os.chdir({workdir!r})

from autoclaimer.cli import AutoClaimerController, install_termination_handlers
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType

MODE = sys.argv[1]
ACCOUNT = {account}
STOP = asyncio.Event()


async def handler(context: TaskContext) -> TaskOutcome:
    if MODE == "block":
        await asyncio.sleep(3600)
    return TaskOutcome(TaskStatus.SUCCEEDED)


async def main() -> int:
    app = parse_app_config({{
        "version": 1,
        "channels": {{"roll_claim": 111}},
        "settings": {{"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 3}},
        "ratelimit": {{"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01}},
        "tasks": {{"job": {{"parameters": {{}}}}}},
    }})
    accounts = parse_accounts_config(
        {{"version": 1, "accounts": [{{"id": ACCOUNT, "tasks": ["job"]}}]}}, app
    )
    registry = TaskRegistry()
    registry.register(TaskType(id="job", description="signal probe.",
                                validate_config=lambda c: None, handler=handler))
    adapters = {{}}


    def factory(account_id):
        return adapters.setdefault(account_id, MockAdapter())


    store = TaskStateStore("state.db")
    await store.open()
    controller = AutoClaimerController(
        app_config=app, accounts=accounts, store=store,
        limiter=RateLimiter(store=store), registry=registry,
        adapter_factory=factory, idle_interval_seconds=0.01,
    )

    async def graceful() -> None:
        try:
            await controller.shutdown()
        finally:
            print("GRACEFUL", flush=True)
            STOP.set()

    def terminate() -> None:
        if MODE == "hang":
            return  # simulate a stuck graceful path: never progresses
        asyncio.ensure_future(graceful())

    uninstall = install_termination_handlers(terminate)
    await controller.start()
    print("READY", flush=True)
    await STOP.wait()
    return 0


sys.exit(asyncio.run(main()))
"""


def _child_script(workdir: Path, mode: str) -> str:
    return CHILD.format(
        repo=str(Path(__file__).resolve().parent.parent),
        workdir=str(workdir),
        account=ACCOUNT,
        mode=mode,
    )


def _spawn(workdir: Path, mode: str) -> subprocess.Popen[str]:
    os.makedirs(workdir, exist_ok=True)
    return subprocess.Popen(
        [sys.executable, "-c", _child_script(workdir, mode), mode],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        cwd=str(workdir),
    )


def _await_ready(proc: subprocess.Popen[str], timeout: float = 30.0) -> str:
    """Bounded readiness wait (review finding): a stalled or crashed
    child must FAIL the test with its output — including any traceback —
    not hang the run on a blocking readline."""
    assert proc.stdout is not None
    stdout = proc.stdout
    import threading

    got: dict[str, str] = {}

    def _read() -> None:
        got["line"] = stdout.readline()

    reader = threading.Thread(target=_read, daemon=True)
    reader.start()
    reader.join(timeout)
    if reader.is_alive():  # stalled child: kill, drain, fail loudly
        proc.kill()
        out, _ = proc.communicate(timeout=10)
        raise AssertionError(f"child never became READY within {timeout}s; output so far:\n{out}")
    line = got.get("line", "")
    if line.strip() != "READY":
        out, _ = proc.communicate(timeout=10)
        raise AssertionError(f"child not ready: {line!r}; remaining output:\n{out}")
    return line


def _wait_row(db: Path, task_id: str, *statuses: TaskInstanceState) -> None:
    import sqlite3
    import time

    wanted = {status.value for status in statuses}
    row = None
    for _ in range(100):
        conn = sqlite3.connect(db)
        try:
            row = conn.execute(
                "SELECT status FROM task_instances WHERE task_id = ?", (task_id,)
            ).fetchone()
        finally:
            conn.close()
        if row and row[0] in wanted:
            return
        time.sleep(0.1)
    pytest.fail(f"row {task_id!r} never reached {wanted} (={row[0] if row else None})")


# --- graceful: SIGTERM and SIGINT ---------------------------------------------


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT], ids=["SIGTERM", "SIGINT"])
def test_graceful_signal_mid_task_leaves_honest_state(tmp_path: Path, sig: signal.Signals) -> None:
    """Termination DURING an active task: graceful drain (exit 0,
    GRACEFUL marker), the blocked row honestly `running`, and a restart
    with a working handler completes it."""
    work = tmp_path / "run"
    proc = _spawn(work, "block")
    try:
        _await_ready(proc)
        _wait_row(work / "state.db", "job", TaskInstanceState.RUNNING)
        proc.send_signal(sig)
        out, _ = proc.communicate(timeout=20)
    finally:
        if proc.poll() is None:  # pragma: no cover - safety
            proc.kill()
    assert proc.returncode == 0, out
    assert "GRACEFUL" in out

    # Restart with a working handler: the interrupted row resumes.
    proc2 = _spawn(work, "ok")
    try:
        _await_ready(proc2)
        _wait_row(work / "state.db", "job", TaskInstanceState.SUCCEEDED)
    finally:
        proc2.send_signal(signal.SIGTERM)
        proc2.communicate(timeout=20)
    assert proc2.returncode == 0


# --- forced termination: separately ---------------------------------------------


def test_forced_kill_keeps_database_consistent_and_resumable(tmp_path: Path) -> None:
    """SIGKILL guarantees NOTHING graceful (no marker, non-zero exit) —
    but the database stays consistent and the restart still recovers and
    completes the work."""
    work = tmp_path / "run"
    proc = _spawn(work, "block")
    try:
        _await_ready(proc)
        _wait_row(work / "state.db", "job", TaskInstanceState.RUNNING)
        proc.kill()  # SIGKILL: no handlers run at all
        out, _ = proc.communicate(timeout=20)
    finally:
        if proc.poll() is None:  # pragma: no cover - safety
            proc.kill()
    assert proc.returncode == -signal.SIGKILL
    assert "GRACEFUL" not in out

    proc2 = _spawn(work, "ok")
    try:
        _await_ready(proc2)
        _wait_row(work / "state.db", "job", TaskInstanceState.SUCCEEDED)
    finally:
        proc2.send_signal(signal.SIGTERM)
        proc2.communicate(timeout=20)
    assert proc2.returncode == 0


# --- second-signal escape hatch -----------------------------------------------------


@pytest.mark.parametrize("sig", [signal.SIGTERM, signal.SIGINT], ids=["sigterm", "sigint"])
def test_second_signal_forces_exit_from_stuck_graceful(tmp_path: Path, sig: signal.Signals) -> None:
    """A graceful shutdown that never progresses ("hang" mode): the
    SECOND signal restores default handling and kills the process — the
    operator always retains a force-exit. Review finding: parametrized
    over BOTH signals (each sent on both attempts), and the return code
    must match termination BY that signal — a nonzero exception-based
    exit must not pass as a forced exit."""
    work = tmp_path / "run"
    proc = _spawn(work, "hang")
    try:
        _await_ready(proc)
        proc.send_signal(sig)
        # The stuck graceful path never prints GRACEFUL; give it a beat,
        # then force.
        import time

        time.sleep(0.5)
        assert proc.poll() is None  # still alive: first signal swallowed
        proc.send_signal(sig)
        out, _ = proc.communicate(timeout=20)
    finally:
        if proc.poll() is None:  # pragma: no cover - safety
            proc.kill()
    assert proc.returncode == -int(sig), (  # killed BY the signal on POSIX
        f"child exit {proc.returncode} is not forced termination by {sig.name}"
    )
    assert "GRACEFUL" not in out


# --- adapters closed / no worker leaks (in-process complements) ----------------------


async def test_stop_closes_adapters_and_leaks_no_worker_tasks(tmp_path: Path) -> None:
    """In-process complement to the exit-code evidence above: the
    graceful path closes every adapter and leaves no worker tasks."""
    from autoclaimer.cli import AutoClaimerController
    from autoclaimer.config import parse_accounts_config, parse_app_config
    from autoclaimer.mock_adapter import MockAdapter
    from autoclaimer.ratelimit import RateLimiter
    from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType

    async def handler(context: TaskContext) -> TaskOutcome:
        return TaskOutcome(TaskStatus.SUCCEEDED)

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111},
            "settings": {"max_concurrent_accounts": 1, "shutdown_timeout_seconds": 2},
            "ratelimit": {"backoff_base_seconds": 0.01, "backoff_cap_seconds": 0.01},
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
            description="close probe.",
            validate_config=lambda c: None,
            handler=handler,
        )
    )
    adapters: dict[int, MockAdapter] = {}
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store,
        limiter=RateLimiter(store=store),
        registry=registry,
        adapter_factory=lambda account_id: adapters.setdefault(account_id, MockAdapter()),
        idle_interval_seconds=0.01,
    )
    baseline = {t for t in asyncio.all_tasks() if t is not asyncio.current_task()}
    try:
        await controller.start()
        await asyncio.sleep(0.3)  # workers spawn and run
    finally:
        await controller.shutdown()
    leaked = asyncio.all_tasks() - baseline - {asyncio.current_task()}
    assert not leaked, [t.get_name() for t in leaked]
    for account_id, adapter in adapters.items():
        assert adapter.closed, f"adapter for {account_id} not closed"
