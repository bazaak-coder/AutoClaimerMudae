"""Database integrity diagnostics (roadmap item 2 — P0).

The diagnostic is strictly read-only: every scenario asserts both the
findings AND that the database content is untouched afterwards.
"""

import asyncio
import contextlib
import sqlite3
from datetime import datetime
from pathlib import Path

import pytest

from autoclaimer.persistence import PersistenceError, TaskStateStore

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
        conn = raw(tmp_path / "state.db")
        try:
            kept = conn.execute(
                "SELECT deadline FROM scope_cooldowns WHERE scope = 'account:9'"
            ).fetchone()
        finally:
            conn.close()
        assert kept[0] == "garbage"  # read-only: row untouched
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
        # Read-only: both rows survive the scan.
        assert len(await store.list_instances()) == 2
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
        conn = raw(tmp_path / "state.db")
        try:
            blob = conn.execute(
                "SELECT deadline FROM scope_cooldowns WHERE scope = 'account:9'"
            ).fetchone()
        finally:
            conn.close()
        assert isinstance(blob[0], bytes)  # read-only: BLOB survives
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


async def test_integrity_reports_unsupported_mudae_kind(tmp_path: Path) -> None:
    """BUG-017: a persisted kind outside the enum (claim/rt/rolls) with a
    VALID deadline is an error finding naming the account and the kind —
    previously silently ignored (false-healthy database). The row is
    never modified."""
    from datetime import UTC, datetime

    store = await opened(tmp_path)
    try:
        conn = raw(tmp_path / "state.db")
        conn.execute("PRAGMA ignore_check_constraints = ON")  # hand-edit path
        with conn:
            conn.execute(
                "INSERT INTO mudae_state (account_id, kind, deadline, updated_at) "
                "VALUES (?, 'unknown', ?, ?)",
                (
                    ACCOUNT,
                    datetime(2026, 11, 2, 12, 0, 0, tzinfo=UTC).isoformat(),
                    datetime(2026, 11, 2, 11, 0, 0, tzinfo=UTC).isoformat(),
                ),
            )
        conn.close()
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        matches = [
            f
            for f in findings
            if f.table == "mudae_state"
            and f.severity == "error"
            and f"account={ACCOUNT}" in f.key
            and "unknown" in f.problem
        ]
        assert matches, findings
        assert "kind" in matches[0].problem.lower()
        # Read-only guarantee: the row is exactly as injected.
        conn = raw(tmp_path / "state.db")
        try:
            row = conn.execute(
                "SELECT kind, deadline FROM mudae_state WHERE account_id = ?",
                (ACCOUNT,),
            ).fetchone()
        finally:
            conn.close()
        assert row[0] == "unknown"
    finally:
        await store.close()


async def test_integrity_reports_kind_and_deadline_independently(tmp_path: Path) -> None:
    """BUG-017 requirement 6: one invalid field must not mask another —
    a row with BOTH an unsupported kind and a malformed deadline yields
    BOTH findings."""
    store = await opened(tmp_path)
    try:
        conn = raw(tmp_path / "state.db")
        conn.execute("PRAGMA ignore_check_constraints = ON")
        with conn:
            conn.execute(
                "INSERT INTO mudae_state (account_id, kind, deadline, updated_at) "
                "VALUES (?, 'bogus', 'garbage-timestamp', '2026-01-01T00:00:00+00:00')",
                (ACCOUNT,),
            )
        conn.close()
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        problems = [
            f.problem
            for f in findings
            if f.table == "mudae_state" and f"account={ACCOUNT}" in f.key
        ]
        assert any("bogus" in p for p in problems), problems  # kind finding
        assert any("deadline" in p.lower() for p in problems), problems  # deadline finding
    finally:
        await store.close()


# PR #8 final review: crash-class corruption (agent-2 F1, agent-7 #1)


async def test_outcome_non_object_json_is_contained_everywhere(tmp_path: Path) -> None:
    """`last_outcome` = valid JSON but NOT an object ('null', '5',
    '"x"', '[1]'): the row decodes to a typed corrupt finding, the
    scanner reports it, and due_queries skip it — never an AttributeError
    cascade into the worker loop or the REPL."""
    store = await opened(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "probe")
        conn = raw(tmp_path / "state.db")
        with conn:
            conn.execute(
                "UPDATE task_instances SET last_outcome = 'null' WHERE id = ?",
                (record.id,),
            )
        conn.close()

        # Scanner: a finding, not a crash.
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        assert any(f.table == "task_instances" and f.severity == "error" for f in findings), (
            findings
        )

        # Query path: skipped as corrupt, never an AttributeError.
        assert await store.list_instances() == ()

        # Direct row read: typed PersistenceError.
        from autoclaimer.persistence import PersistenceError as _PE

        with pytest.raises(_PE, match="corrupt"):
            await store.get_instance(record.id)
    finally:
        await store.close()


async def test_outcome_deeply_nested_json_is_contained(tmp_path: Path) -> None:
    """~100k-deep nested JSON in last_outcome raises RecursionError from
    json.loads — must land in the typed corrupt-row containment, never
    escape into the scanner or the REPL."""
    store = await opened(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "probe")
        deep = "[" * 100_000 + "]" * 100_000
        conn = raw(tmp_path / "state.db")
        with conn:
            conn.execute(
                "UPDATE task_instances SET last_outcome = ? WHERE id = ?",
                (deep, record.id),
            )
        conn.close()
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        assert any(f.table == "task_instances" and f.severity == "error" for f in findings), (
            findings
        )
        assert await store.list_instances() == ()
    finally:
        await store.close()


async def test_scan_contains_injected_unexpected_exceptions(tmp_path: Path) -> None:
    """Defense in depth (injection-based, per independent review): even
    an exception NO per-row handler anticipates becomes a database
    finding — the scanner is the tool you reach for when rows are
    corrupt, so it must never be the crash."""
    import autoclaimer.persistence as persistence_module

    store = await opened(tmp_path)
    try:
        await store.create_instance(ACCOUNT, "probe")

        original = persistence_module._row_to_record

        def exploding(row: sqlite3.Row) -> object:
            raise RuntimeError("injected scan-code failure")

        persistence_module._row_to_record = exploding  # type: ignore[assignment]
        try:
            findings = await store.integrity_report(known_account_ids={ACCOUNT})
        finally:
            persistence_module._row_to_record = original
        assert any(
            f.table == "database" and f.severity == "error" and "RuntimeError" in f.problem
            for f in findings
        ), findings
    finally:
        await store.close()


async def test_noncanonical_timezone_offset_is_a_finding(tmp_path: Path) -> None:
    """PR #8 final review (agent-2 F2): an aware-but-offset timestamp
    parses yet breaks the module's UTC-string-sortability contract (an
    overdue task hidden ~11 h). The scanner must flag it."""
    store = await opened(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "probe")
        conn = raw(tmp_path / "state.db")
        with conn:
            conn.execute(
                "UPDATE task_instances SET next_attempt_at = ? WHERE id = ?",
                ("2026-11-02T23:00:00+14:00", record.id),
            )
            conn.execute(
                "UPDATE scope_cooldowns SET deadline = ? WHERE scope = ?",
                ("2026-11-02T23:00:00+14:00", "x"),
            )
            conn.execute(
                "INSERT INTO scope_cooldowns (scope, deadline, server_directed) VALUES ('y', ?, 1)",
                ("2026-11-02T23:00:00+14:00",),
            )
        conn.close()
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        problems = [f.problem for f in findings if f.severity == "error"]
        assert any("non-canonical" in p or "timezone" in p.lower() for p in problems), problems
    finally:
        await store.close()


async def test_empty_string_timestamp_is_a_finding_not_due_now(tmp_path: Path) -> None:
    """PR #8 final review (agent-2 F3): '' in a timestamp column is
    corrupt data (no write path produces it) — reported, and never
    silently treated as absent (which made the row due immediately)."""
    store = await opened(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "probe")
        conn = raw(tmp_path / "state.db")
        with conn:
            conn.execute(
                "UPDATE task_instances SET next_attempt_at = ? WHERE id = ?",
                ("", record.id),
            )
        conn.close()
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        assert any(f.table == "task_instances" and f.severity == "error" for f in findings), (
            findings
        )
    finally:
        await store.close()


async def test_oversized_ids_and_account_values_are_clipped(tmp_path: Path) -> None:
    """BUG-019 completion: oversized TEXT values in id/account columns
    (SQLite stores them despite INTEGER affinity) produce BOUNDED
    finding keys — 100 KB junk never floods the diagnostic output."""
    store = await opened(tmp_path)
    try:
        big = "A" * 100_000
        conn = raw(tmp_path / "state.db")
        conn.execute("PRAGMA ignore_check_constraints = ON")
        with conn:
            # task_instances: oversized account_id + already-clipped task_id
            conn.execute(
                "INSERT INTO task_instances "
                "(account_id, task_id, status, attempt_count, created_at, updated_at) "
                "VALUES (?, ?, 'pending', 0, 'garbage', 'garbage')",
                (big, "t"),
            )
            # mudae_state: oversized account_id with a valid kind + deadline
            conn.execute(
                "INSERT INTO mudae_state (account_id, kind, deadline, updated_at) "
                "VALUES (?, 'claim', 'garbage-ts', 'garbage')",
                (big,),
            )
        conn.close()
        findings = await store.integrity_report(known_account_ids={ACCOUNT})
        assert findings, "expected findings for the corrupt rows"
        for finding in findings:
            combined = f"{finding.key} {finding.problem}"
            assert len(combined) < 500, (
                f"unclipped output ({len(combined)} chars) in {finding.table}: {combined[:100]}..."
            )
        # The oversized account_id appears (clipped) — the findings are
        # still actionable, just bounded.
        assert any("A" in f.key for f in findings if "account=" in f.key)
    finally:
        await store.close()


async def test_integrity_report_waits_for_inflight_transactions(tmp_path: Path) -> None:
    """BUG-003 (PR #8 audit): integrity_report called _call_threaded
    directly, bypassing the store lock/gate — the scan read UNCOMMITTED
    rows and emitted findings for data that was then rolled back (false
    diagnostics an operator would act on). The scan must serialize with
    every other store reader."""
    import sys

    sys.path.insert(0, ".")
    from autoclaimer.persistence import TaskStateStore

    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    try:
        entered = asyncio.Event()
        release = asyncio.Event()

        async def txn_with_uncommitted_orphan() -> None:
            async with store.transaction():
                # An uncommitted row for an account not in any config.
                await store._call_threaded(lambda: _raw_insert_orphan(store))
                entered.set()
                await release.wait()

        task = asyncio.create_task(txn_with_uncommitted_orphan())
        try:
            await asyncio.wait_for(entered.wait(), timeout=5.0)

            findings_task = asyncio.create_task(store.integrity_report(known_account_ids=set()))
            await asyncio.sleep(0.2)
            assert not findings_task.done(), (
                "the integrity scan ran while a transaction was uncommitted "
                "(it bypassed the store's serialization)"
            )
            task.cancel()  # the txn rolls back; the scan must NOT have
            # read its uncommitted orphan (it waits on the store lock).
            with contextlib.suppress(asyncio.CancelledError):
                await task
            findings = await findings_task
            orphans = [f for f in findings if "999999999999999999" in str(f)]
            assert not orphans, f"findings over uncommitted/rolled-back data: {orphans}"
        finally:
            release.set()  # never deadlock close() on the parked txn
            if not task.done():
                with contextlib.suppress(Exception):
                    await asyncio.wait_for(task, timeout=5.0)
    finally:
        await store.close()


def _raw_insert_orphan(store: "TaskStateStore") -> None:
    conn = store._connection()  # noqa: SLF001 — inside the txn, guarded
    assert conn is not None
    conn.execute(
        "INSERT INTO task_instances (account_id, task_id, status, attempt_count,"
        " created_at, updated_at) VALUES (999999999999999999, 'ghost', 'pending',"
        " 0, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00')"
    )


async def test_semantically_invalid_integer_columns_are_findings(
    tmp_path: Path,
) -> None:
    """BUG-005 (PR #8 audit): a TEXT attempt_count ('seven') decoded
    "successfully" into records (str leaked; SQLite arithmetic coerced
    it to 0 — silently resetting the retry budget) and the scanner
    reported the row healthy. Decode-time validation now contains it as
    a corrupt-row finding."""
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    try:
        record = await store.create_instance(271485504361996288, "probe")
        conn = sqlite3.connect(tmp_path / "state.db")
        with conn:
            conn.execute(
                "UPDATE task_instances SET attempt_count = 'seven' WHERE id = ?",
                (record.id,),
            )
        conn.close()

        findings = await store.integrity_report()
        task_findings = [f for f in findings if "corrupt" in str(f).lower()]
        assert task_findings, f"scanner green-lit a semantically invalid row: {findings}"
        with pytest.raises(PersistenceError, match="corrupt task_instances row"):
            await store.get_instance(record.id)
    finally:
        await store.close()
