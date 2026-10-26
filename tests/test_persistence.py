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
        assert version_row[0] == s.schema_version == 2
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


async def test_v1_database_is_migrated_to_v2(tmp_path: Path) -> None:
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
        assert version_row[0] == 2
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
