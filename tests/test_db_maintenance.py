"""SQLite maintenance (roadmap item 14 — P2).

- backups are CONSISTENT snapshots (VACUUM INTO on the guarded
  connection — serialized with active operations, never torn);
- restoration is tested by opening the backup as a fresh store and
  comparing content;
- growth stats report sizes/pages/rows;
- the deployment journal mode (rollback journal, deliberately not WAL)
  is surfaced by the stats so a silent mode switch cannot go unnoticed.
"""

import sqlite3
from pathlib import Path

import pytest

from autoclaimer.persistence import PersistenceError, TaskStateStore
from autoclaimer.registry import TaskOutcome, TaskStatus

ACCOUNT = 271485504361996288


async def _seeded(tmp_path: Path) -> TaskStateStore:
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    record = await store.create_instance(ACCOUNT, "probe")
    await store.claim(record.id)
    await store.record_success(record.id, TaskOutcome(TaskStatus.SUCCEEDED, message="done"))
    await store.record_audit_event(category="runtime", action="paused")
    return store


async def test_backup_while_idle_is_a_consistent_restorable_copy(tmp_path: Path) -> None:
    store = await _seeded(tmp_path)
    try:
        backup_path = tmp_path / "backup.db"
        size = await store.backup_to(backup_path)
        assert size > 0

        # Restoration: the backup opens as a fresh store with identical
        # visible content.
        restored = TaskStateStore(backup_path)
        await restored.open()
        try:
            record = await restored.get_instance_by_ids(ACCOUNT, "probe")
            assert record.status.value == "succeeded"
            assert record.last_outcome is not None
            assert record.last_outcome.message == "done"
            assert len(await restored.audit_events()) == 1
        finally:
            await restored.close()
    finally:
        await store.close()


async def test_backup_refuses_to_overwrite(tmp_path: Path) -> None:
    store = await _seeded(tmp_path)
    try:
        target = tmp_path / "existing.db"
        target.write_bytes(b"sentinel")
        with pytest.raises(PersistenceError, match="refusing to overwrite"):
            await store.backup_to(target)
        assert target.read_bytes() == b"sentinel"  # untouched
    finally:
        await store.close()


async def test_backup_serializes_with_an_active_transaction(tmp_path: Path) -> None:
    """A backup requested while another connection holds an open write
    transaction never produces a torn copy: the backup either waits for
    serialization or fails typed — and the main database is unaffected."""
    store = await _seeded(tmp_path)
    try:
        other = sqlite3.connect(tmp_path / "state.db", isolation_level=None)
        other.execute("BEGIN IMMEDIATE")
        other.execute(
            "UPDATE task_instances SET status = 'pending' WHERE account_id = ?", (ACCOUNT,)
        )
        try:
            await store.backup_to(tmp_path / "live.db")
        finally:
            other.execute("ROLLBACK")
            other.close()
        # Whatever the interleaving, the live database still works and
        # the (possibly written) backup is a valid SQLite file if present.
        record = await store.get_instance_by_ids(ACCOUNT, "probe")
        assert record.status.value == "succeeded"  # rollback held
        if (tmp_path / "live.db").exists():
            probe = sqlite3.connect(tmp_path / "live.db")
            try:
                probe.execute("PRAGMA integrity_check").fetchone()
            finally:
                probe.close()
    finally:
        await store.close()


async def test_database_stats_snapshot(tmp_path: Path) -> None:
    store = await _seeded(tmp_path)
    try:
        stats = await store.database_stats()
        assert stats["size_bytes"] > 0
        assert stats["page_count"] >= 1
        assert stats["journal_mode"].lower() == "delete"  # rollback journal
        assert stats["rows"]["task_instances"] == 1
        assert stats["rows"]["audit_events"] == 1
        assert stats["rows"]["account_credentials"] == 0
    finally:
        await store.close()


async def test_retention_keeps_growth_bounded(tmp_path: Path) -> None:
    """Growth monitoring relies on bounded retention: audit and history
    tables never grow unboundedly (pruning verified end to end)."""
    from autoclaimer.persistence import _AUDIT_KEEP

    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    try:
        for n in range(_AUDIT_KEEP + 10):
            await store.record_audit_event(category="runtime", action="tick", detail=str(n))
        stats = await store.database_stats()
        assert stats["rows"]["audit_events"] == _AUDIT_KEEP
    finally:
        await store.close()


# --- BUG-039: live backup must serialize at TRANSACTION level, not just
# per-call guard level ---


async def test_backup_waits_for_open_transaction_on_own_connection(
    tmp_path: Path,
) -> None:
    """BUG-039: the store's OWN transaction (BEGIN held between guarded
    calls) leaves the connection inside an open transaction — SQLite
    rejects VACUUM INTO there ("cannot VACUUM from within a
    transaction"). The backup must therefore serialize on the store's
    async lock (like every other operation) and run AFTER the commit,
    not race the window between the transaction's guarded calls."""
    import asyncio

    store = await _seeded(tmp_path)
    try:
        backup_path = tmp_path / "live.bak"
        async with store.transaction():
            await store.create_instance(ACCOUNT, "txn_body_row")  # txn genuinely open
            backup = asyncio.create_task(store.backup_to(backup_path))
            await asyncio.sleep(0.3)  # the backup attempt runs while txn is open
            assert not backup.done(), (
                "backup did not wait for the open transaction: "
                f"{backup.exception() if backup.done() else 'pending'}"
            )
        # transaction committed -> the waiting backup completes
        size = await asyncio.wait_for(backup, timeout=5.0)
        assert backup_path.exists() and size > 0
        probe = sqlite3.connect(backup_path)
        try:
            assert probe.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        finally:
            probe.close()
    finally:
        await store.close()
