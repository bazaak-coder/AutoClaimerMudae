"""Tests for the SQLite task-instance state store."""

import asyncio
import sqlite3
from collections.abc import AsyncIterator
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

from autoclaimer.persistence import (
    RESUMABLE_STATES,
    DatabaseBusyError,
    PersistenceError,
    StateTransitionError,
    TaskInstanceRecord,
    TaskInstanceState,
    TaskStateStore,
    UnknownInstanceError,
    UnsupportedSchemaVersionError,
    _map_sqlite_error,
)
from autoclaimer.registry import TaskOutcome, TaskStatus

ACCOUNT = 271485504361996288
BASE_TIME = datetime(2026, 10, 26, 12, 0, 0, tzinfo=UTC)


class FakeClock:
    """Deterministic, manually advanced UTC clock."""

    def __init__(self, start: datetime = BASE_TIME) -> None:
        self.now = start

    def __call__(self) -> datetime:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now = self.now + timedelta(seconds=seconds)


async def make_store(
    path: str | Path = ":memory:", *, clock: FakeClock | None = None
) -> TaskStateStore:
    store = TaskStateStore(path, clock=clock or FakeClock())
    await store.open()
    return store


@pytest.fixture
async def store() -> AsyncIterator[TaskStateStore]:
    s = await make_store()
    yield s
    await s.close()


async def seed(
    store: TaskStateStore, task_id: str = "claim_test", account: int = ACCOUNT
) -> TaskInstanceRecord:
    return await store.create_instance(account, task_id)


# --- schema creation and migrations ---


async def test_fresh_open_creates_schema_and_version(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    s = await make_store(path)
    try:
        version_row = await s._run(s._fetchone_sync, "PRAGMA user_version", ())
        assert version_row is not None
        assert version_row[0] == s.schema_version == 3  # v3: mudae_state added
    finally:
        await s.close()
    reopened = await make_store(path)
    try:
        rows = await reopened._run(
            reopened._fetchall_sync,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'task_instances'",
            (),
        )
        assert len(rows) == 1  # re-open did not duplicate schema objects
    finally:
        await reopened.close()


async def test_future_schema_version_is_rejected(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    s = await make_store(path)
    await s.close()
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version = 99")
    conn.close()
    future = TaskStateStore(path)
    with pytest.raises(UnsupportedSchemaVersionError, match="99"):
        await future.open()


# --- scope cooldowns (migration v2, rate-limiter persistence) ---


async def test_v1_database_is_migrated_to_current(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    s = await make_store(path)
    await s.close()
    conn = sqlite3.connect(path)
    conn.execute("DROP TABLE scope_cooldowns")
    conn.execute("PRAGMA user_version = 1")
    conn.commit()
    conn.close()
    migrated = await make_store(path)
    try:
        rows = await migrated._run(
            migrated._fetchall_sync,
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'scope_cooldowns'",
            (),
        )
        assert len(rows) == 1
        version_row = await migrated._run(migrated._fetchone_sync, "PRAGMA user_version", ())
        assert version_row is not None
        assert version_row[0] == migrated.schema_version == 3  # v3: mudae_state added
    finally:
        await migrated.close()


async def test_scope_cooldown_upsert_only_extends(tmp_path: Path) -> None:
    clock = FakeClock()
    s = await make_store(tmp_path / "state.db", clock=clock)
    try:
        await s.set_scope_cooldown(
            "route:claim", clock() + timedelta(seconds=30), server_directed=True
        )
        await s.set_scope_cooldown(
            "route:claim", clock() + timedelta(seconds=5), server_directed=False
        )
        active = await s.active_scope_cooldowns()
        deadline, server_directed = active["route:claim"]
        assert deadline == clock() + timedelta(seconds=30)
        assert server_directed is True  # sticky
        await s.set_scope_cooldown(
            "route:claim", clock() + timedelta(seconds=60), server_directed=False
        )
        deadline, server_directed = (await s.active_scope_cooldowns())["route:claim"]
        assert deadline == clock() + timedelta(seconds=60)
        assert server_directed is True
    finally:
        await s.close()


async def test_scope_cooldown_expired_entries_are_not_returned(tmp_path: Path) -> None:
    clock = FakeClock()
    s = await make_store(tmp_path / "state.db", clock=clock)
    try:
        await s.set_scope_cooldown(
            "route:claim", clock() + timedelta(seconds=10), server_directed=True
        )
        clock.advance(11)
        assert await s.active_scope_cooldowns() == {}
    finally:
        await s.close()


# --- creation, identity, idempotency ---


async def test_create_instance_is_idempotent(store: TaskStateStore) -> None:
    first = await store.create_instance(ACCOUNT, "claim_test")
    second = await store.create_instance(ACCOUNT, "claim_test")
    assert first == second
    assert len(await store.list_instances()) == 1
    assert first.status is TaskInstanceState.PENDING
    assert first.attempt_count == 0
    assert first.last_outcome is None


async def test_unknown_instance_is_reported(store: TaskStateStore) -> None:
    with pytest.raises(UnknownInstanceError, match="id 404"):
        await store.get_instance(404)
    with pytest.raises(UnknownInstanceError):
        await store.get_instance_by_ids(ACCOUNT, "missing_task")


# --- state transitions ---


async def test_claim_counts_attempts_and_clears_retry() -> None:
    clock = FakeClock()
    s = await make_store(clock=clock)
    try:
        record = await s.create_instance(ACCOUNT, "claim_test")
        claimed = await s.claim(record.id)
        assert claimed.status is TaskInstanceState.RUNNING
        assert claimed.attempt_count == 1
        await s.record_failure(
            record.id,
            TaskOutcome(TaskStatus.FAILED_RECOVERABLE, "retry"),
            retry_at=clock() + timedelta(seconds=30),
        )
        reclaimed = await s.claim(record.id)  # waiting is claimable again
        assert reclaimed.attempt_count == 2
        assert reclaimed.next_attempt_at is None
    finally:
        await s.close()


async def test_claim_from_running_is_rejected(store: TaskStateStore) -> None:
    record = await seed(store)
    await store.claim(record.id)
    with pytest.raises(StateTransitionError, match="running"):
        await store.claim(record.id)


async def test_record_success_persists_outcome(store: TaskStateStore) -> None:
    record = await seed(store)
    await store.claim(record.id)
    done = await store.record_success(
        record.id, TaskOutcome(TaskStatus.SUCCEEDED, message="claimed character")
    )
    assert done.status is TaskInstanceState.SUCCEEDED
    assert done.last_outcome is not None
    assert done.last_outcome.status is TaskStatus.SUCCEEDED
    assert done.last_outcome.message == "claimed character"
    assert done.next_attempt_at is None


async def test_record_success_is_idempotent(store: TaskStateStore) -> None:
    record = await seed(store)
    await store.claim(record.id)
    outcome = TaskOutcome(TaskStatus.SUCCEEDED, message="done")
    first = await store.record_success(record.id, outcome)
    second = await store.record_success(record.id, outcome)
    assert second == first


async def test_record_failure_with_retry_schedules_waiting(store: TaskStateStore) -> None:
    clock = FakeClock()
    s = await make_store(clock=clock)
    try:
        record = await s.create_instance(ACCOUNT, "claim_test")
        await s.claim(record.id)
        clock.advance(10)
        retry_at = clock() + timedelta(seconds=30)
        failed = await s.record_failure(
            record.id,
            TaskOutcome(TaskStatus.FAILED_RECOVERABLE, "cooldown hit"),
            retry_at=retry_at,
        )
        assert failed.status is TaskInstanceState.WAITING
        assert failed.next_attempt_at == retry_at
        assert failed.attempt_count == 1
    finally:
        await s.close()


async def test_record_failure_without_retry_is_terminal(store: TaskStateStore) -> None:
    record = await seed(store)
    await store.claim(record.id)
    failed = await store.record_failure(
        record.id, TaskOutcome(TaskStatus.FAILED_PERMANENT, "broken"), retry_at=None
    )
    assert failed.status is TaskInstanceState.FAILED
    with pytest.raises(StateTransitionError):
        await store.claim(failed.id)


async def test_cancel_from_active_states_only(store: TaskStateStore) -> None:
    record = await seed(store)
    cancelled = await store.cancel(record.id)
    assert cancelled.status is TaskInstanceState.CANCELLED
    with pytest.raises(StateTransitionError, match="cancel"):
        await store.cancel(record.id)  # terminal stays terminal


# --- timestamps ---


async def test_timestamps_are_utc_and_advance(store: TaskStateStore) -> None:
    clock = FakeClock()
    s = await make_store(clock=clock)
    try:
        record = await s.create_instance(ACCOUNT, "claim_test")
        clock.advance(5)
        claimed = await s.claim(record.id)
        assert record.created_at == BASE_TIME
        assert claimed.updated_at == BASE_TIME + timedelta(seconds=5)
        assert claimed.updated_at.tzinfo is not None
    finally:
        await s.close()


# --- retry scheduling queries ---


async def test_due_instances_respect_retry_times(store: TaskStateStore) -> None:
    clock = FakeClock()
    s = await make_store(clock=clock)
    try:
        await s.create_instance(ACCOUNT, "ready_task")
        snoozed = await s.create_instance(ACCOUNT, "snoozed_task")
        done = await s.create_instance(ACCOUNT, "done_task")
        await s.claim(snoozed.id)
        await s.record_failure(
            snoozed.id,
            TaskOutcome(TaskStatus.FAILED_RECOVERABLE, "later"),
            retry_at=clock() + timedelta(hours=1),
        )
        await s.claim(done.id)
        await s.record_success(done.id, TaskOutcome(TaskStatus.SUCCEEDED))
        due = await s.due_instances()
        assert [r.task_id for r in due] == ["ready_task"]
        clock.advance(3601)
        due_later = await s.due_instances()
        assert {r.task_id for r in due_later} == {"ready_task", "snoozed_task"}
    finally:
        await s.close()


# --- cooldowns (wall-clock across restarts) ---


async def test_cooldown_remaining_uses_wall_clock(tmp_path: Path) -> None:
    clock = FakeClock()
    path = tmp_path / "state.db"
    s = await make_store(path, clock=clock)
    try:
        record = await s.create_instance(ACCOUNT, "claim_test")
        await s.set_cooldown(record.id, clock() + timedelta(seconds=30), scope="account")
        assert await s.cooldown_remaining(record.id) == timedelta(seconds=30)
    finally:
        await s.close()

    # Restart the process: wall-clock deadlines keep working.
    restarted_clock = FakeClock(start=BASE_TIME + timedelta(seconds=20))
    resumed = await make_store(path, clock=restarted_clock)
    try:
        assert await resumed.cooldown_remaining(record.id) == timedelta(seconds=10)
        restarted_clock.advance(11)
        assert await resumed.cooldown_remaining(record.id) is None
    finally:
        await resumed.close()


# --- restart recovery ---


async def test_running_rows_are_recovered_on_restart(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    s = await make_store(path)
    orphan = await s.create_instance(ACCOUNT, "orphan_task")
    terminal = await s.create_instance(ACCOUNT, "done_task")
    waiting = await s.create_instance(ACCOUNT, "waiting_task")
    await s.claim(orphan.id)
    await s.claim(terminal.id)
    await s.record_success(terminal.id, TaskOutcome(TaskStatus.SUCCEEDED))
    await s.claim(waiting.id)
    await s.record_failure(
        waiting.id,
        TaskOutcome(TaskStatus.FAILED_RECOVERABLE, "backoff"),
        retry_at=BASE_TIME + timedelta(minutes=5),
    )
    await s.close()

    resumed = await make_store(path)
    try:
        recovered = await resumed.recover_interrupted()
        assert recovered == 1
        orphan_row = await resumed.get_instance_by_ids(ACCOUNT, "orphan_task")
        assert orphan_row.status is TaskInstanceState.PENDING
        assert orphan_row.attempt_count == 1  # attempt history preserved
        done_row = await resumed.get_instance_by_ids(ACCOUNT, "done_task")
        assert done_row.status is TaskInstanceState.SUCCEEDED  # terminal untouched
        waiting_row = await resumed.get_instance_by_ids(ACCOUNT, "waiting_task")
        assert waiting_row.status is TaskInstanceState.WAITING  # resumable as-is
    finally:
        await resumed.close()


def test_resumable_states_are_pending_and_waiting() -> None:
    assert RESUMABLE_STATES == {
        TaskInstanceState.PENDING,
        TaskInstanceState.WAITING,
    }


# --- transactions ---


async def test_transaction_commits_and_rolls_back_atomically(store: TaskStateStore) -> None:
    record_a = await store.create_instance(ACCOUNT, "task_a")
    record_b = await store.create_instance(ACCOUNT, "task_b")
    async with store.transaction():
        await store.claim(record_a.id)
        await store.claim(record_b.id)
    assert (await store.get_instance(record_a.id)).status is TaskInstanceState.RUNNING
    assert (await store.get_instance(record_b.id)).status is TaskInstanceState.RUNNING

    with pytest.raises(RuntimeError, match="boom"):
        async with store.transaction():
            await store.cancel(record_a.id)
            raise RuntimeError("boom")
    assert (await store.get_instance(record_a.id)).status is TaskInstanceState.RUNNING


# --- concurrency ---


async def test_concurrent_creates_and_claims_stay_consistent(tmp_path: Path) -> None:
    store = await make_store(tmp_path / "state.db")
    try:
        await asyncio.gather(*(store.create_instance(ACCOUNT, f"task_{i}") for i in range(10)))
        assert len(await store.list_instances()) == 10

        records = await store.list_instances()
        claimed = await asyncio.gather(
            *(store.claim(record.id) for record in records), return_exceptions=True
        )
        assert all(not isinstance(result, Exception) for result in claimed)
        running = await store.list_instances(TaskInstanceState.RUNNING)
        assert len(running) == 10
        assert {r.attempt_count for r in running} == {1}
    finally:
        await store.close()


async def test_two_stores_share_a_file_safely(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    first = await make_store(path)
    second = await make_store(path)
    try:
        await asyncio.gather(
            *(first.create_instance(ACCOUNT, f"a_{i}") for i in range(5)),
            *(second.create_instance(ACCOUNT, f"b_{i}") for i in range(5)),
        )
        merged = await second.list_instances()
        assert len(merged) == 10  # second store sees first store's committed rows
    finally:
        await first.close()
        await second.close()


# --- error mapping ---


def test_locked_database_maps_to_busy_error() -> None:
    mapped = _map_sqlite_error(sqlite3.OperationalError("database is locked"))
    assert isinstance(mapped, DatabaseBusyError)


def test_other_sqlite_errors_map_to_persistence_error() -> None:
    mapped = _map_sqlite_error(sqlite3.OperationalError("no such table"))
    assert type(mapped) is PersistenceError


async def test_operations_before_open_are_refused() -> None:
    store = TaskStateStore(":memory:")
    with pytest.raises(PersistenceError, match="not open"):
        await store.list_instances()


# --- corrupt persisted state (audit F3/F4) ---


def _future(seconds: int) -> datetime:
    return BASE_TIME + timedelta(seconds=seconds)


async def _inject_raw_rows(store: TaskStateStore, *rows: tuple[str, str, int]) -> None:
    conn = store._connection()  # noqa: SLF001  # test: simulate hand-edited/corrupt rows
    for scope, deadline, server_directed in rows:
        conn.execute(
            "INSERT OR REPLACE INTO scope_cooldowns (scope, deadline, server_directed) "
            "VALUES (?, ?, ?)",
            (scope, deadline, server_directed),
        )


async def test_active_scope_cooldowns_skips_corrupt_deadline_rows(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """F3: a malformed deadline must not crash the scan with a raw
    ValueError. The corrupt row is skipped with an error log naming the
    scope; valid rows still load (audit T5: startup continues)."""
    import logging

    clock = FakeClock()
    store = TaskStateStore(tmp_path / "state.db", clock=clock)
    await store.open()
    try:
        await store.set_scope_cooldown("account:1", _future(300), server_directed=True)
        await _inject_raw_rows(
            store,
            ("account:2", "not-a-timestamp", 1),
            # naive-but-future-sorting: passes the SQL string filter, then
            # must be rejected by parsing (F4 via the F3 path)
            ("account:3", "2027-01-01T00:00:00", 0),
        )
        with caplog.at_level(logging.ERROR, logger="autoclaimer.persistence"):
            active = await store.active_scope_cooldowns()
        assert set(active) == {"account:1"}  # only the valid row survives
        assert "account:2" in caplog.text and "account:3" in caplog.text
        assert "corrupt" in caplog.text.lower()
    finally:
        await store.close()


async def test_load_persisted_cooldowns_survives_corrupt_rows(tmp_path: Path) -> None:
    """F3 end-to-end: manager startup cooldown loading must not raise on
    a corrupt row (typed containment, not a crash)."""
    from autoclaimer.ratelimit import RateLimiter

    clock = FakeClock()
    store = TaskStateStore(tmp_path / "state.db", clock=clock)
    await store.open()
    try:
        await store.set_scope_cooldown("account:1", _future(300), server_directed=True)
        await _inject_raw_rows(store, ("account:2", "garbage", 1))
        limiter = RateLimiter(store=store, wall=clock)
        await limiter.load_persisted_cooldowns()  # must not raise
        loaded = {info.scope for info in limiter.status().cooldowns}
        assert "account:1" in loaded
        assert "account:2" not in loaded
    finally:
        await store.close()


async def test_parse_ts_rejects_naive_timestamps() -> None:
    """F4: naive persisted timestamps are invalid data, not silently
    reinterpreted as UTC (agents.md section 17.6)."""
    from autoclaimer.persistence import _parse_ts

    with pytest.raises(ValueError, match="timezone-naive"):
        _parse_ts("2026-01-01T00:00:00")
    aware = _parse_ts("2026-01-01T00:00:00+02:00")
    offset = aware.utcoffset()
    assert offset is not None
    assert offset.total_seconds() == 0  # normalized to UTC


async def test_naive_timestamp_row_is_treated_as_corrupt(tmp_path: Path) -> None:
    """F4 end-to-end: a hand-edited naive timestamp in task_instances is
    contained as a corrupt row (skipped in scans, typed error on direct
    fetch) instead of being silently reinterpreted."""
    from autoclaimer.persistence import PersistenceError, TaskInstanceState

    store = await make_store(tmp_path / "state.db")
    try:
        await store.create_instance(1, "ok_task")
        conn = store._connection()  # noqa: SLF001
        conn.execute(
            "INSERT INTO task_instances "
            "(account_id, task_id, status, created_at, updated_at) "
            "VALUES (1, 'naive_task', 'pending', '2026-01-01T00:00:00', '2026-01-01T00:00:00')"
        )
        listed = await store.list_instances_by_account(1)
        assert {r.task_id for r in listed} == {"ok_task"}  # corrupt row skipped

        naive_row_id = conn.execute(
            "SELECT id FROM task_instances WHERE task_id = 'naive_task'"
        ).fetchone()[0]
        with pytest.raises(PersistenceError, match="corrupt"):
            await store.get_instance(naive_row_id)
        assert (await store.get_instance_by_ids(1, "ok_task")).status is TaskInstanceState.PENDING
    finally:
        await store.close()


# --- mudae_state: durable per-account Mudae deadlines (F2 decision) ---


async def test_mudae_deadline_roundtrip_and_fresh_lookup_replaces(tmp_path: Path) -> None:
    """A $rtu lookup REPLACES the stored deadline: the reply is the
    current truth (perk-dependent 5-20h — never assumed, always learned)."""
    from autoclaimer.persistence import MudaeStateKind

    store = await make_store(tmp_path / "state.db")
    try:
        await store.set_mudae_deadline(1, MudaeStateKind.RT, _future(19 * 3600))
        await store.set_mudae_deadline(1, MudaeStateKind.CLAIM, _future(3600))
        deadlines = await store.mudae_deadlines(1)
        assert deadlines[MudaeStateKind.RT] == _future(19 * 3600)
        assert deadlines[MudaeStateKind.CLAIM] == _future(3600)

        # Fresher lookup wins, even when it shows LESS time than stored
        # (stored value was stale/wrong — replace and diagnose via log).
        await store.set_mudae_deadline(1, MudaeStateKind.RT, _future(2 * 3600))
        deadlines = await store.mudae_deadlines(1)
        assert deadlines[MudaeStateKind.RT] == _future(2 * 3600)

        # Accounts are isolated.
        assert await store.mudae_deadlines(2) == {}
    finally:
        await store.close()


async def test_mudae_deadline_rejects_naive_datetime(tmp_path: Path) -> None:
    from autoclaimer.persistence import MudaeStateKind, PersistenceError

    store = await make_store(tmp_path / "state.db")
    try:
        with pytest.raises(PersistenceError, match="timezone-naive"):
            await store.set_mudae_deadline(
                1, MudaeStateKind.CLAIM, datetime(2026, 10, 29, 12, 0, 0)
            )
    finally:
        await store.close()


async def test_mudae_deadlines_contain_corrupt_rows(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """Hand-edited garbage in mudae_state must not crash the read; the
    corrupt row is skipped with a diagnosable error (F3/F4 policy)."""
    import logging

    from autoclaimer.persistence import MudaeStateKind

    store = await make_store(tmp_path / "state.db")
    try:
        await store.set_mudae_deadline(1, MudaeStateKind.RT, _future(3600))
        conn = store._connection()  # noqa: SLF001
        conn.execute(
            "INSERT OR REPLACE INTO mudae_state (account_id, kind, deadline, updated_at) "
            "VALUES (1, 'claim', 'garbage', '2026-01-01T00:00:00+00:00')"
        )
        with caplog.at_level(logging.ERROR, logger="autoclaimer.persistence"):
            deadlines = await store.mudae_deadlines(1)
        assert set(deadlines) == {MudaeStateKind.RT}
        assert "corrupt" in caplog.text.lower()
    finally:
        await store.close()


# --- domain-scheduled retries (milestone 4: retry ownership) ---


async def test_schedule_attempt_sets_waiting_until_absolute_deadline(tmp_path: Path) -> None:
    """The Mudae task layer schedules retries from LEARNED deadlines
    (parsed replies anchored to UTC) — not transport backoff. The
    instance stays waiting and not due until the deadline passes."""
    store = await make_store(tmp_path / "state.db")
    clock = store._clock
    assert isinstance(clock, FakeClock)
    try:
        record = await store.create_instance(1, "roll")
        await store.schedule_attempt(record.id, _future(3600))
        waiting = await store.get_instance(record.id)
        assert waiting.status is TaskInstanceState.WAITING
        assert waiting.next_attempt_at == _future(3600)

        due = await store.due_instances()
        assert record.id not in [r.id for r in due]  # not due yet

        clock.advance(3601)  # deadline passes -> claimable again
        due = await store.due_instances()
        assert record.id in [r.id for r in due]
    finally:
        await store.close()


async def test_schedule_attempt_replaces_previous_schedule(tmp_path: Path) -> None:
    """Fresher domain information replaces an earlier deadline (an $rtu
    re-check supersedes the previous assumption)."""
    store = await make_store(tmp_path / "state.db")
    try:
        record = await store.create_instance(1, "roll")
        await store.schedule_attempt(record.id, _future(7200))
        await store.schedule_attempt(record.id, _future(1800))  # re-learned
        waiting = await store.get_instance(record.id)
        assert waiting.next_attempt_at == _future(1800)
    finally:
        await store.close()


async def test_schedule_attempt_rejects_naive_and_terminal(tmp_path: Path) -> None:
    store = await make_store(tmp_path / "state.db")
    try:
        record = await store.create_instance(1, "roll")
        with pytest.raises(PersistenceError, match="timezone-naive"):
            await store.schedule_attempt(record.id, datetime(2026, 10, 29, 12, 0, 0))

        succeeded = await store.claim(record.id)
        from autoclaimer.registry import TaskOutcome, TaskStatus

        await store.record_success(succeeded.id, TaskOutcome(TaskStatus.SUCCEEDED, message="done"))
        with pytest.raises(StateTransitionError):
            await store.schedule_attempt(succeeded.id, _future(60))
    finally:
        await store.close()
