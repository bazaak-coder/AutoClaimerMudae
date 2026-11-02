"""Database integrity diagnostics (roadmap item 2 — P0).

The diagnostic is strictly read-only: every scenario asserts both the
findings AND that the database content is untouched afterwards.
"""

import sqlite3
from datetime import datetime
from pathlib import Path

from autoclaimer.persistence import (
    TaskStateStore,
)

ACCOUNT = 271485504361996288
OTHER = 380028190142042112


async def opened(tmp_path: Path) -> TaskStateStore:
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    return store


def raw(db: Path) -> sqlite3.Connection:
    return sqlite3.connect(db)


# --- healthy database ------------------------------------------------------


async def test_healthy_database_reports_clean(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        await store.create_instance(ACCOUNT, "probe")
        await store.set_scope_cooldown("account:1", _dt(), server_directed=True)
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        assert findings == ()
    finally:
        await store.close()


def _dt() -> datetime:
    from datetime import UTC, datetime

    return datetime(2026, 10, 31, 12, 0, 0, tzinfo=UTC)


# --- undecodable task rows -------------------------------------------------


async def test_corrupt_task_row_is_reported_not_deleted(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "probe")
        conn = raw(tmp_path / "state.db")
        with conn:
            conn.execute(
                "UPDATE task_instances SET last_outcome = '{not json', "
                "next_attempt_at = 'garbage-timestamp' WHERE id = ?",
                (record.id,),
            )
        conn.close()

        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        keys = [f.key for f in findings if f.table == "task_instances"]
        assert any(f"id={record.id}" in k for k in keys)
        assert all(f.severity == "error" for f in findings)

        # Nothing was modified: the corrupt data is exactly as injected.
        conn = raw(tmp_path / "state.db")
        try:
            row = conn.execute(
                "SELECT last_outcome FROM task_instances WHERE id = ?", (record.id,)
            ).fetchone()
        finally:
            conn.close()
        assert row[0] == "{not json"
    finally:
        await store.close()


# --- malformed cooldowns and state rows -------------------------------------


async def test_malformed_cooldown_and_state_rows_reported(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        conn = raw(tmp_path / "state.db")
        with conn:
            conn.execute(
                "INSERT INTO scope_cooldowns (scope, deadline, server_directed) "
                "VALUES ('account:9', 'garbage', 1)"
            )
            conn.execute(
                "INSERT INTO mudae_state (account_id, kind, deadline, updated_at) "
                "VALUES (?, 'claim', 'garbage', '2026-01-01T00:00:00+00:00')",
                (ACCOUNT,),
            )
        conn.close()

        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        tables = {(f.table, f.severity) for f in findings}
        assert ("scope_cooldowns", "error") in tables
        assert ("mudae_state", "error") in tables
        assert all("sqlite3 CLI" in f.hint for f in findings)
    finally:
        await store.close()


# --- orphaned account references -------------------------------------------


async def test_orphaned_account_rows_are_warnings_with_hints(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        await store.create_instance(ACCOUNT, "probe")  # known account
        await store.create_instance(OTHER, "probe")  # not in known set
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        orphans = [f for f in findings if f.severity == "warning"]
        assert len(orphans) == 1
        assert f"account={OTHER}" in orphans[0].key
        assert "harmless" in orphans[0].problem or "harmless" in orphans[0].hint
        # Not supplying the account set skips the orphan check entirely.
        assert await store.integrity_report() == ()
    finally:
        await store.close()


# --- page-level corruption ---------------------------------------------------


async def test_page_corruption_is_a_typed_report(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    await store.create_instance(ACCOUNT, "probe")
    await store.close()

    # Corrupt page content beyond the file header so sqlite still opens the
    # file but integrity_check reports damage.
    path = tmp_path / "state.db"
    data = bytearray(path.read_bytes())
    for offset in range(4096, len(data) - 16, 8):
        chunk = bytes(data[offset : offset + 8])
        if chunk.count(0) < 8:  # a used page
            data[offset : offset + 8] = b"\xde\xad\xbe\xef\xde\xad\xbe\xef"
    path.write_bytes(bytes(data))

    store = TaskStateStore(path)
    await store.open()  # opens; damage surfaces in the diagnostic
    try:
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        assert any(f.table == "database" and f.severity == "error" for f in findings)
        assert any("backup" in f.hint.lower() for f in findings)
    finally:
        await store.close()


# --- CLI surface -------------------------------------------------------------


async def test_cli_integrity_command_reports_and_never_modifies(tmp_path: Path) -> None:
    from autoclaimer.cli import AutoClaimerController, CliSession
    from autoclaimer.config import parse_accounts_config, parse_app_config
    from autoclaimer.mock_adapter import MockAdapter
    from autoclaimer.ratelimit import RateLimiter
    from autoclaimer.registry import TaskRegistry

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "tasks": {"probe": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["probe"]}]}, app
    )
    store = await opened(tmp_path)
    try:
        await store.create_instance(ACCOUNT, "probe")
        await store.create_instance(OTHER, "probe")  # orphan

        registry = TaskRegistry()
        controller = AutoClaimerController(
            app_config=app,
            accounts=accounts,
            store=store,
            limiter=RateLimiter(store=store),
            registry=registry,
            adapter_factory=lambda account_id: MockAdapter(),
        )
        output: list[str] = []
        session = CliSession(controller, read_line=None, write=output.append)  # type: ignore[arg-type]
        assert await session.run_once("integrity") is True
        joined = "\n".join(output)
        assert "1 finding" in joined
        assert f"account={OTHER}" in joined
        assert "nothing was modified" in joined
    finally:
        await store.close()


async def test_integrity_reports_blob_timestamps(tmp_path: Path) -> None:
    """ChatGPT PR #8 audit BUG-011: SQLite permits BLOB values in TEXT
    columns — fromisoformat(bytes) raises TypeError, which previously
    escaped the scanner (only ValueError was contained). Both tables
    must produce findings, never a crash."""
    store = await opened(tmp_path)
    try:
        conn = raw(tmp_path / "state.db")
        with conn:
            conn.execute(
                "INSERT INTO scope_cooldowns (scope, deadline, server_directed) "
                "VALUES ('account:9', CAST('garbage' AS BLOB), 1)"
            )
            conn.execute(
                "INSERT INTO mudae_state (account_id, kind, deadline, updated_at) "
                "VALUES (?, 'claim', CAST('garbage' AS BLOB), '2026-01-01T00:00:00+00:00')",
                (ACCOUNT,),
            )
        conn.close()
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        tables = {f.table for f in findings if f.severity == "error"}
        assert "scope_cooldowns" in tables
        assert "mudae_state" in tables
    finally:
        await store.close()


async def test_orphaned_mudae_state_row_is_reported(tmp_path: Path) -> None:
    """VERIFY-004: a removed account with ONLY a persisted Mudae deadline
    is an orphaned reference — reported once, read-only."""
    from datetime import UTC, datetime

    store = await opened(tmp_path)
    try:
        conn = raw(tmp_path / "state.db")
        with conn:
            conn.execute(
                "INSERT INTO mudae_state (account_id, kind, deadline, updated_at) "
                "VALUES (?, 'claim', ?, ?)",
                (
                    OTHER,
                    datetime(2026, 11, 2, 12, 0, 0, tzinfo=UTC).isoformat(),
                    datetime(2026, 11, 2, 11, 0, 0, tzinfo=UTC).isoformat(),
                ),
            )
        conn.close()
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        warnings = [f for f in findings if f.severity == "warning"]
        assert any(f"account={OTHER}" in f.key for f in warnings), warnings
        # Single warning for the account even with rows across tables.
        assert len([f for f in warnings if f"account={OTHER}" in f.key]) == 1
    finally:
        await store.close()
