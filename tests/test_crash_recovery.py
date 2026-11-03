"""SQLite crash recovery — abrupt process termination (roadmap item 1).

Every scenario spawns a REAL child process that holds the database in a
specific mid-operation state, then SIGKILLs it (no cleanup of any kind
runs) and verifies recovery on the next open:

- termination during an active transaction  -> hot-journal rollback
- termination during state updates           -> committed changes durable
- termination during task claiming           -> recoverable, not stranded
- termination during completion              -> at-least-once re-execution
- no permanently stranded states across every persisted status
- eligible tasks recovered exactly once (no duplicate concurrent claim)

The store deliberately runs the ROLLBACK journal (not WAL: its shared
memory mapping is unreliable under the target environment — see
``persistence._connect``); "journal recovery" here means hot-journal
rollback at next open, which the deployment mode guarantees.
"""

import subprocess
import sys
from pathlib import Path

import pytest

from autoclaimer.persistence import (
    StateTransitionError,
    TaskInstanceState,
    TaskStateStore,
)
from autoclaimer.registry import TaskOutcome, TaskStatus

ACCOUNT = 271485504361996288

#: Child template: `child` runs after a generic open; it signals READY only
#: once the intended on-disk state is reached (journal fsynced / commit
#: durable), then parks so the parent can SIGKILL mid-"operation".
_CHILD_TAIL = """
import sqlite3, sys, time
db = sys.argv[1]
conn = sqlite3.connect(db, isolation_level=None)
{child}
print("READY", flush=True)
time.sleep(300)
"""

_MID_TRANSACTION = _CHILD_TAIL.format(
    child="""
conn.execute("BEGIN IMMEDIATE")
conn.execute(
    "UPDATE task_instances SET status = 'running', "
    "attempt_count = attempt_count + 1 WHERE id = 1"
)
"""
)

_COMMITTED_WAITING = _CHILD_TAIL.format(
    child="""
conn.execute("BEGIN IMMEDIATE")
conn.execute(
    "UPDATE task_instances SET status = 'waiting' WHERE id = 1"
)
conn.execute("COMMIT")
"""
)

_CLAIMED_PARKED = _CHILD_TAIL.format(
    child="""
conn.execute("BEGIN IMMEDIATE")
conn.execute(
    "UPDATE task_instances SET status = 'running', "
    "attempt_count = attempt_count + 1 WHERE id = 1"
)
conn.execute("COMMIT")
"""
)

_MID_COMPLETION = _CHILD_TAIL.format(
    child="""
conn.execute("BEGIN IMMEDIATE")
conn.execute(
    "UPDATE task_instances SET status = 'running', "
    "attempt_count = attempt_count + 1 WHERE id = 1"
)
conn.execute("COMMIT")
conn.execute("BEGIN IMMEDIATE")
conn.execute(
    "UPDATE task_instances SET status = 'succeeded', "
    "last_outcome = '{\\"status\\": \\"succeeded\\", \\"message\\": \\"\\"}' WHERE id = 1"
)
"""
)


async def _seed_and_kill(tmp_path: Path, child_code: str) -> TaskStateStore:
    """Create the database with one pending row, close it cleanly, then
    SIGKILL a child process holding *child_code*'s on-disk state."""
    db = tmp_path / "state.db"
    store = TaskStateStore(db)
    await store.open()
    record = await store.create_instance(ACCOUNT, "crash_probe")
    assert record.id == 1
    await store.close()

    proc = subprocess.Popen(
        [sys.executable, "-c", child_code, str(db)],
        stdout=subprocess.PIPE,
        text=True,
    )
    try:
        ready = proc.stdout.readline() if proc.stdout else ""
        assert ready.strip() == "READY", f"child did not reach its state: {ready!r}"
        proc.kill()  # SIGKILL: no connection cleanup, no journal removal
    finally:
        proc.wait(timeout=10)

    reopened = TaskStateStore(db)
    await reopened.open()
    return reopened


async def _reopen_raw(tmp_path: Path) -> TaskStateStore:
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    return store


# --- termination during an active transaction ---------------------------


async def test_kill_during_active_transaction_rolls_back(tmp_path: Path) -> None:
    """SIGKILL while a write transaction is open (hot journal present):
    the next open rolls it back — the row is EXACTLY as before, and the
    store remains fully usable."""
    store = await _seed_and_kill(tmp_path, _MID_TRANSACTION)
    try:
        row = await store.get_instance(1)
        assert row.status is TaskInstanceState.PENDING
        assert row.attempt_count == 0  # the uncommitted UPDATE is gone
        # Usability: the recovered store claims and completes normally.
        claimed = await store.claim(1)
        assert claimed.status is TaskInstanceState.RUNNING
        assert claimed.attempt_count == 1
        await store.record_success(1, TaskOutcome(TaskStatus.SUCCEEDED, message="after crash"))
        assert (await store.get_instance(1)).status is TaskInstanceState.SUCCEEDED
    finally:
        await store.close()


# --- termination during state updates ------------------------------------


async def test_kill_after_committed_update_is_durable(tmp_path: Path) -> None:
    """SIGKILL after a COMMIT: the committed change survives the abrupt
    death — crash recovery never loses acknowledged writes."""
    store = await _seed_and_kill(tmp_path, _COMMITTED_WAITING)
    try:
        row = await store.get_instance(1)
        assert row.status is TaskInstanceState.WAITING  # durable
        assert row.attempt_count == 0
    finally:
        await store.close()


# --- termination during task claiming -------------------------------------


async def test_kill_while_claimed_recovers_without_stranding(tmp_path: Path) -> None:
    """SIGKILL after the claim committed but before any outcome: the row
    is stranded `running` on disk; startup recovery flips it to `pending`
    (attempt preserved) and it is due exactly once."""
    store = await _seed_and_kill(tmp_path, _CLAIMED_PARKED)
    try:
        stranded = await store.get_instance(1)
        assert stranded.status is TaskInstanceState.RUNNING
        assert stranded.attempt_count == 1

        recovered = await store.recover_interrupted()
        assert recovered == 1
        row = await store.get_instance(1)
        assert row.status is TaskInstanceState.PENDING
        assert row.attempt_count == 1  # the burned attempt is preserved

        due = await store.due_instances(ACCOUNT)
        assert [r.id for r in due] == [1]  # eligible again, exactly once
    finally:
        await store.close()


# --- termination during completion ----------------------------------------


async def test_kill_during_completion_rolls_back_and_reschedules(tmp_path: Path) -> None:
    """SIGKILL while the SUCCESS write is mid-transaction: the completion
    rolls back, recovery reschedules the task — documented at-least-once
    semantics (the handler may run again), never a phantom success."""
    store = await _seed_and_kill(tmp_path, _MID_COMPLETION)
    try:
        row = await store.get_instance(1)
        assert row.status is TaskInstanceState.RUNNING  # claim durable,
        assert row.attempt_count == 1  # completion rolled back
        await store.recover_interrupted()
        row = await store.get_instance(1)
        assert row.status is TaskInstanceState.PENDING
        # Re-execution proceeds normally after recovery.
        await store.claim(1)
        await store.record_success(1, TaskOutcome(TaskStatus.SUCCEEDED, message="redo"))
        assert (await store.get_instance(1)).status is TaskInstanceState.SUCCEEDED
    finally:
        await store.close()


# --- no permanently stranded states ---------------------------------------


async def test_recovery_leaves_terminal_states_untouched(tmp_path: Path) -> None:
    """Matrix across every persisted status: ONLY `running` is recovered;
    pending/waiting/terminal rows are exactly as they were — nothing is
    permanently stranded and nothing completed is resurrected."""
    db = tmp_path / "state.db"
    store = TaskStateStore(db)
    await store.open()
    for task_id in ("t_pending", "t_waiting", "t_running", "t_done", "t_failed"):
        await store.create_instance(ACCOUNT, task_id)
    # Direct SQL lets us place rows in arbitrary persisted states.
    import sqlite3

    conn = sqlite3.connect(db)
    with conn:
        conn.execute("UPDATE task_instances SET status='waiting' WHERE task_id='t_waiting'")
        conn.execute("UPDATE task_instances SET status='running' WHERE task_id='t_running'")
        conn.execute("UPDATE task_instances SET status='succeeded' WHERE task_id='t_done'")
        conn.execute("UPDATE task_instances SET status='failed' WHERE task_id='t_failed'")
    conn.close()
    await store.close()

    reopened = await _reopen_raw(tmp_path)
    try:
        recovered = await reopened.recover_interrupted()
        assert recovered == 1  # only the running row
        by_task = {r.task_id: r.status for r in await reopened.list_instances()}
        assert by_task["t_pending"] is TaskInstanceState.PENDING
        assert by_task["t_waiting"] is TaskInstanceState.WAITING
        assert by_task["t_running"] is TaskInstanceState.PENDING  # recovered
        assert by_task["t_done"] is TaskInstanceState.SUCCEEDED  # untouched
        assert by_task["t_failed"] is TaskInstanceState.FAILED  # untouched
    finally:
        await reopened.close()


# --- recovered tasks execute without duplication --------------------------


async def test_recovered_task_cannot_be_double_claimed(tmp_path: Path) -> None:
    """After crash-recovery the task is claimable again — but exactly one
    claimer wins at a time (no concurrent duplicate execution) and a
    completed row is never re-executed."""
    store = await _seed_and_kill(tmp_path, _CLAIMED_PARKED)
    try:
        await store.recover_interrupted()
        claimed = await store.claim(1)
        assert claimed.status is TaskInstanceState.RUNNING
        # A second claim while running: rejected (single-owner execution).
        with pytest.raises(StateTransitionError):
            await store.claim(1)
        await store.record_success(1, TaskOutcome(TaskStatus.SUCCEEDED))
        # Terminal rows are never claimable again — no duplicate execution.
        with pytest.raises(StateTransitionError):
            await store.claim(1)
        assert (await store.due_instances(ACCOUNT)) == ()
    finally:
        await store.close()


# --- deployment journal mode is pinned (documentation guarantee) ----------


async def test_rollback_journal_mode_is_the_deployment_mode(tmp_path: Path) -> None:
    """Crash semantics above are guaranteed by the ROLLBACK journal (the
    store intentionally avoids WAL). Pin the mode so a silent switch to a
    different recovery model cannot go unnoticed."""
    import sqlite3

    db = tmp_path / "state.db"
    store = TaskStateStore(db)
    await store.open()
    await store.close()
    conn = sqlite3.connect(db)
    try:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    finally:
        conn.close()
    assert str(mode).lower() == "delete"  # rollback journal, no WAL
    # No stray journal artifacts remain after a clean close.
    assert not db.with_suffix(".db-journal").exists()
