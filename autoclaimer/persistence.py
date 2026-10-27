"""Asynchronous SQLite persistence for task-instance runtime state.

Dependency decision: this module uses the standard library ``sqlite3``
dispatched through ``asyncio.to_thread`` instead of adding ``aiosqlite``.
aiosqlite is itself a thread wrapper over ``sqlite3``; staying stdlib-only
honors the project's zero-runtime-dependency policy while keeping database
work off the event loop. All statements run on worker threads serialized by
an asyncio lock; task-scoped transactions (ContextVar holding the owning
task) let nested store
calls share one critical section without letting concurrent coroutines
bypass the lock.

Clock strategy: every persisted timestamp is a timezone-aware UTC
``datetime.isoformat()`` string (lexicographically sortable). Cooldown
deadlines and retry times are wall-clock UTC, never ``time.monotonic()``,
so remaining durations survive process restarts: remaining = deadline - now(UTC).

Resumability after restart:

- ``pending`` and ``waiting`` are resumable as-is (retry times persist).
- ``running`` is NOT resumable: a process that stopped mid-attempt cannot
  know the outcome, so :meth:`TaskStateStore.recover_interrupted` resets
  running rows to ``pending`` (attempt counts preserved) at startup.
- ``succeeded``, ``failed``, and ``cancelled`` are terminal and untouched.

No credentials, tokens, or authentication material are ever stored here;
the ``last_outcome`` column persists only the outcome status and message.

Concurrency model: one connection guarded by a re-entrant asyncio lock; the
default rollback journal plus a busy timeout handles contention from other
processes. WAL journaling is deliberately not used (its shared-memory
mapping is unreliable under PRoot emulation); a single serialized writer
loses nothing by that choice.
"""

from __future__ import annotations

import asyncio
import json
import logging
import sqlite3
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar, cast

from .registry import TaskOutcome, TaskStatus

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_SCHEMA_VERSION = 2
_BUSY_TIMEOUT_MS = 5000

_MIGRATIONS: dict[int, str] = {
    1: """
    CREATE TABLE IF NOT EXISTS task_instances (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        account_id INTEGER NOT NULL,
        task_id TEXT NOT NULL,
        status TEXT NOT NULL CHECK (
            status IN ('pending', 'running', 'waiting', 'succeeded', 'failed', 'cancelled')
        ),
        attempt_count INTEGER NOT NULL DEFAULT 0,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        last_outcome TEXT,
        next_attempt_at TEXT,
        cooldown_until TEXT,
        cooldown_scope TEXT,
        UNIQUE (account_id, task_id)
    );
    """,
    2: """
    CREATE TABLE IF NOT EXISTS scope_cooldowns (
        scope TEXT PRIMARY KEY,
        deadline TEXT NOT NULL,
        server_directed INTEGER NOT NULL
    );
    """,
}


class PersistenceError(Exception):
    """Base class for persistence-layer failures."""


class DatabaseBusyError(PersistenceError):
    """The database stayed locked longer than the busy timeout."""


class UnsupportedSchemaVersionError(PersistenceError):
    """The database schema version is newer than this code supports."""


class UnknownInstanceError(PersistenceError):
    """No task instance exists for the given identifier."""


class StateTransitionError(PersistenceError):
    """A state change was requested from a state that does not allow it."""


class TaskInstanceState(Enum):
    """Lifecycle states of a task instance."""

    PENDING = "pending"
    RUNNING = "running"
    WAITING = "waiting"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELLED = "cancelled"


RESUMABLE_STATES = frozenset({TaskInstanceState.PENDING, TaskInstanceState.WAITING})
TERMINAL_STATES = frozenset(
    {TaskInstanceState.SUCCEEDED, TaskInstanceState.FAILED, TaskInstanceState.CANCELLED}
)

_CLAIMABLE = (TaskInstanceState.PENDING, TaskInstanceState.WAITING)
_CANCELLABLE = (TaskInstanceState.PENDING, TaskInstanceState.RUNNING, TaskInstanceState.WAITING)


@dataclass(frozen=True)
class TaskInstanceRecord:
    """One persisted task instance row."""

    id: int
    account_id: int
    task_id: str
    status: TaskInstanceState
    attempt_count: int
    created_at: datetime
    updated_at: datetime
    last_outcome: TaskOutcome | None
    next_attempt_at: datetime | None
    cooldown_until: datetime | None
    cooldown_scope: str | None


def utc_now() -> datetime:
    """Current UTC time (injectable clock supplied to the store for tests)."""
    return datetime.now(UTC)


def _format_ts(value: datetime) -> str:
    if value.tzinfo is None:
        raise PersistenceError("Refusing to persist a timezone-naive datetime; use UTC.")
    return value.astimezone(UTC).isoformat()


def _parse_ts(value: str) -> datetime:
    parsed = datetime.fromisoformat(value)
    return parsed if parsed.tzinfo is not None else parsed.replace(tzinfo=UTC)


def _map_sqlite_error(exc: sqlite3.Error) -> PersistenceError:
    """Build the mapped error; callers raise it with ``from exc`` to keep the cause."""
    if isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower():
        return DatabaseBusyError(f"Database remained locked: {exc}")
    return PersistenceError(f"Unexpected database error: {exc}")


class TaskStateStore:
    """Async, transactional SQLite store for task-instance runtime state."""

    def __init__(
        self,
        path: str | Path,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._path = Path(path)
        self._clock = clock
        self._conn: sqlite3.Connection | None = None
        self._lock = asyncio.Lock()
        # Task-scoped transaction flag: re-entrancy must never let a second
        # coroutine bypass the lock while another holds it.
        self._txn_owner: ContextVar[object | None] = ContextVar(
            "autoclaimer_txn_owner", default=None
        )

    # ----- lifecycle -----

    async def open(self) -> None:
        """Open the connection, apply pragmas, and migrate the schema."""

        def _connect() -> sqlite3.Connection:
            conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
            conn.row_factory = sqlite3.Row
            conn.execute(f"PRAGMA busy_timeout = {_BUSY_TIMEOUT_MS}")
            # Deliberately no WAL journaling: its shared-memory mapping is
            # unreliable under PRoot thread emulation; this store is a single
            # serialized writer, which the default rollback journal serves
            # correctly on every filesystem.
            return conn

        async with self._lock:
            try:
                self._conn = await asyncio.to_thread(_connect)
                await asyncio.to_thread(self._migrate_sync, self._conn)
            except (sqlite3.Error, PersistenceError) as exc:
                if self._conn is not None:
                    await asyncio.to_thread(self._conn.close)
                    self._conn = None
                if isinstance(exc, sqlite3.Error):
                    raise _map_sqlite_error(exc) from exc
                raise

    async def close(self) -> None:
        """Close the connection; safe to call more than once."""
        async with self._lock:
            if self._conn is not None:
                await asyncio.to_thread(self._conn.close)
                self._conn = None

    @property
    def schema_version(self) -> int:
        return _SCHEMA_VERSION

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None:
            raise PersistenceError("Store is not open; call open() first.")
        return self._conn

    def _migrate_sync(self, conn: sqlite3.Connection) -> None:
        version = conn.execute("PRAGMA user_version").fetchone()[0]
        if version > _SCHEMA_VERSION:
            raise UnsupportedSchemaVersionError(
                f"Database schema version {version} is newer than supported "
                f"version {_SCHEMA_VERSION}; upgrade AutoClaimer first."
            )
        for target in sorted(_MIGRATIONS):
            if version >= target:
                continue
            # executescript() would implicitly COMMIT; execute statements
            # individually so the migration stays inside one transaction.
            statements = [s.strip() for s in _MIGRATIONS[target].split(";") if s.strip()]
            conn.execute("BEGIN IMMEDIATE")
            try:
                for statement in statements:
                    conn.execute(statement)
                conn.execute(f"PRAGMA user_version = {target}")
                conn.execute("COMMIT")
            except sqlite3.Error:
                conn.execute("ROLLBACK")
                raise
            version = target

    # ----- execution helpers -----

    async def _call_threaded(self, fn: Callable[..., _T], *args: Any) -> _T:
        try:
            return await asyncio.to_thread(fn, *args)
        except sqlite3.Error as exc:
            raise _map_sqlite_error(exc) from exc

    async def _run(self, fn: Callable[..., _T], *args: Any) -> _T:
        """Run one database call; inside a transaction the caller's lock covers us."""
        if self._txn_owner.get() is asyncio.current_task():
            return await self._call_threaded(fn, *args)
        async with self._lock:
            return await self._call_threaded(fn, *args)

    def _fetchone_sync(self, sql: str, params: Sequence[Any]) -> sqlite3.Row | None:
        row = self._connection().execute(sql, params).fetchone()
        return cast("sqlite3.Row | None", row)

    def _fetchall_sync(self, sql: str, params: Sequence[Any]) -> list[sqlite3.Row]:
        rows = self._connection().execute(sql, params).fetchall()
        return cast("list[sqlite3.Row]", rows)

    def _update_sync(self, sql: str, params: Sequence[Any]) -> int:
        cursor = self._connection().execute(sql, params)
        return cursor.rowcount

    async def _record(self, sql: str, params: Sequence[Any] = ()) -> TaskInstanceRecord:
        row = await self._run(self._fetchone_sync, sql, params)
        if row is None:
            raise PersistenceError(f"Row vanished after write: {sql}")
        return _row_to_record(row)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Group several store calls into one transaction.

        Re-entrant within the same task (nested ``transaction()`` blocks join
        the outermost one). Other tasks block on the store lock until the
        transaction finishes, keeping the single connection serialized.
        Child asyncio tasks created inside the body do NOT join: they
        block on the store lock like any other task, so do not await
        store-using child tasks inside a transaction body (self-deadlock).
        """
        if self._txn_owner.get() is not None:
            yield
            return
        async with self._lock:
            token = self._txn_owner.set(asyncio.current_task())
            try:
                await self._call_threaded(self._connection().execute, "BEGIN IMMEDIATE")
            except BaseException:
                self._txn_owner.reset(token)
                raise
            try:
                yield
            except BaseException:
                self._txn_owner.reset(token)
                try:
                    await self._call_threaded(self._connection().execute, "ROLLBACK")
                except sqlite3.Error:
                    logger.exception("rollback after transaction failure also failed")
                raise
            self._txn_owner.reset(token)
            try:
                await self._call_threaded(self._connection().execute, "COMMIT")
            except BaseException:
                try:
                    await self._call_threaded(self._connection().execute, "ROLLBACK")
                except sqlite3.Error:
                    logger.exception("rollback after failed commit also failed")
                raise

    # ----- queries -----

    async def get_instance(self, instance_id: int) -> TaskInstanceRecord:
        row = await self._run(
            self._fetchone_sync, "SELECT * FROM task_instances WHERE id = ?", (instance_id,)
        )
        if row is None:
            raise UnknownInstanceError(f"No task instance with id {instance_id}.")
        return _row_to_record(row)

    async def get_instance_by_ids(self, account_id: int, task_id: str) -> TaskInstanceRecord:
        row = await self._run(
            self._fetchone_sync,
            "SELECT * FROM task_instances WHERE account_id = ? AND task_id = ?",
            (account_id, task_id),
        )
        if row is None:
            raise UnknownInstanceError(
                f"No task instance for account {account_id} and task '{task_id}'."
            )
        return _row_to_record(row)

    async def list_instances(
        self, status: TaskInstanceState | None = None
    ) -> tuple[TaskInstanceRecord, ...]:
        if status is None:
            rows = await self._run(self._fetchall_sync, "SELECT * FROM task_instances", ())
        else:
            rows = await self._run(
                self._fetchall_sync,
                "SELECT * FROM task_instances WHERE status = ?",
                (status.value,),
            )
        return _safe_records(rows)

    async def list_instances_by_account(
        self, account_id: int, status: TaskInstanceState | None = None
    ) -> tuple[TaskInstanceRecord, ...]:
        """Instances for one account, optionally filtered by status."""
        if status is None:
            rows = await self._run(
                self._fetchall_sync,
                "SELECT * FROM task_instances WHERE account_id = ?",
                (account_id,),
            )
        else:
            rows = await self._run(
                self._fetchall_sync,
                "SELECT * FROM task_instances WHERE account_id = ? AND status = ?",
                (account_id, status.value),
            )
        return _safe_records(rows)

    async def due_instances(self) -> tuple[TaskInstanceRecord, ...]:
        """Claimable instances whose retry time (if any) has passed."""
        now = _format_ts(self._clock())
        rows = await self._run(
            self._fetchall_sync,
            "SELECT * FROM task_instances WHERE status IN ('pending', 'waiting') "
            "AND (next_attempt_at IS NULL OR next_attempt_at <= ?)",
            (now,),
        )
        return _safe_records(rows)

    # ----- state changes -----

    async def create_instance(self, account_id: int, task_id: str) -> TaskInstanceRecord:
        """Idempotently create a task instance; existing rows are returned unchanged."""
        now = _format_ts(self._clock())
        await self._run(
            self._update_sync,
            "INSERT INTO task_instances "
            "(account_id, task_id, status, created_at, updated_at) "
            "VALUES (?, ?, 'pending', ?, ?) "
            "ON CONFLICT (account_id, task_id) DO NOTHING",
            (account_id, task_id, now, now),
        )
        return await self.get_instance_by_ids(account_id, task_id)

    async def claim(self, instance_id: int) -> TaskInstanceRecord:
        """pending|waiting -> running, counting one new attempt."""
        now = _format_ts(self._clock())
        changed = await self._run(
            self._update_sync,
            "UPDATE task_instances SET status = 'running', "
            "attempt_count = attempt_count + 1, updated_at = ?, next_attempt_at = NULL "
            "WHERE id = ? AND status IN ('pending', 'waiting')",
            (now, instance_id),
        )
        if changed == 0:
            await self._require_claimable(instance_id)
        return await self.get_instance(instance_id)

    async def record_success(self, instance_id: int, outcome: TaskOutcome) -> TaskInstanceRecord:
        """running -> succeeded; idempotent when already succeeded identically."""
        return await self._record_terminal(
            instance_id, "succeeded", outcome, from_states=("running",)
        )

    async def record_failure(
        self, instance_id: int, outcome: TaskOutcome, *, retry_at: datetime | None
    ) -> TaskInstanceRecord:
        """running -> waiting (with retry time) or failed (when retry_at is None)."""
        target = "waiting" if retry_at is not None else "failed"
        return await self._record_terminal(
            instance_id, target, outcome, from_states=("running",), retry_at=retry_at
        )

    async def cancel(self, instance_id: int) -> TaskInstanceRecord:
        """pending|running|waiting -> cancelled (terminal states are refused)."""
        now = _format_ts(self._clock())
        changed = await self._run(
            self._update_sync,
            "UPDATE task_instances SET status = 'cancelled', updated_at = ?, "
            "next_attempt_at = NULL WHERE id = ? AND status IN ('pending', 'running', 'waiting')",
            (now, instance_id),
        )
        if changed == 0:
            current = await self.get_instance(instance_id)
            raise StateTransitionError(
                f"Cannot cancel task instance {instance_id} from state '{current.status.value}'."
            )
        return await self.get_instance(instance_id)

    async def set_cooldown(
        self, instance_id: int, until: datetime, scope: str
    ) -> TaskInstanceRecord:
        """Persist a wall-clock UTC cooldown deadline and its scope."""
        changed = await self._run(
            self._update_sync,
            "UPDATE task_instances SET cooldown_until = ?, cooldown_scope = ?, updated_at = ? "
            "WHERE id = ?",
            (_format_ts(until), scope, _format_ts(self._clock()), instance_id),
        )
        if changed == 0:
            raise UnknownInstanceError(f"No task instance with id {instance_id}.")
        return await self.get_instance(instance_id)

    async def cooldown_remaining(self, instance_id: int) -> timedelta | None:
        """Remaining cooldown from the persisted UTC deadline; None when expired/absent."""
        record = await self.get_instance(instance_id)
        if record.cooldown_until is None:
            return None
        remaining = record.cooldown_until - self._clock()
        return remaining if remaining > timedelta(0) else None

    async def recover_interrupted(self) -> int:
        """Startup recovery: running -> pending for rows orphaned by a shutdown."""
        changed = await self._run(
            self._update_sync,
            "UPDATE task_instances SET status = 'pending', updated_at = ? WHERE status = 'running'",
            (_format_ts(self._clock()),),
        )
        return changed

    async def record_abandoned(self, instance_id: int, message: str) -> TaskInstanceRecord:
        """pending|waiting -> failed without ever claiming (invalid config etc.)."""
        now = _format_ts(self._clock())
        changed = await self._run(
            self._update_sync,
            "UPDATE task_instances SET status = 'failed', last_outcome = ?, "
            "next_attempt_at = NULL, updated_at = ? "
            "WHERE id = ? AND status IN ('pending', 'waiting')",
            (_outcome_json(TaskOutcome(TaskStatus.FAILED_PERMANENT, message)), now, instance_id),
        )
        if changed == 0:
            current = await self.get_instance(instance_id)
            if current.status.value != "failed":
                raise StateTransitionError(
                    f"Cannot abandon task instance {instance_id} from state "
                    f"'{current.status.value}'."
                )
        return await self.get_instance(instance_id)

    # ----- scope cooldowns (rate limiter persistence) -----

    async def set_scope_cooldown(
        self, scope: str, deadline: datetime, *, server_directed: bool
    ) -> None:
        """Upsert a scope cooldown; only ever extends (max) and keeps the
        server-directed flag sticky once set."""
        await self._run(
            self._update_sync,
            "INSERT INTO scope_cooldowns (scope, deadline, server_directed) "
            "VALUES (?, ?, ?) "
            "ON CONFLICT (scope) DO UPDATE SET "
            "deadline = MAX(scope_cooldowns.deadline, excluded.deadline), "
            "server_directed = MAX(scope_cooldowns.server_directed, excluded.server_directed)",
            (scope, _format_ts(deadline), int(server_directed)),
        )

    async def active_scope_cooldowns(self) -> dict[str, tuple[datetime, bool]]:
        """Return unexpired scope cooldowns as {scope: (deadline, server_directed)}."""
        now = _format_ts(self._clock())
        rows = await self._run(
            self._fetchall_sync,
            "SELECT scope, deadline, server_directed FROM scope_cooldowns WHERE deadline > ?",
            (now,),
        )
        return {
            row["scope"]: (_parse_ts(row["deadline"]), bool(row["server_directed"])) for row in rows
        }

    # ----- internals -----

    async def _require_claimable(self, instance_id: int) -> None:
        current = await self.get_instance(instance_id)
        raise StateTransitionError(
            f"Cannot claim task instance {instance_id} from state '{current.status.value}'."
        )

    async def _record_terminal(
        self,
        instance_id: int,
        target: str,
        outcome: TaskOutcome,
        *,
        from_states: Sequence[str],
        retry_at: datetime | None = None,
    ) -> TaskInstanceRecord:
        now = _format_ts(self._clock())
        next_attempt = _format_ts(retry_at) if retry_at is not None else None
        changed = await self._run(
            self._update_sync,
            "UPDATE task_instances SET status = ?, last_outcome = ?, next_attempt_at = ?, "
            "updated_at = ? WHERE id = ? AND status IN "
            f"({', '.join('?' for _ in from_states)})",
            (target, _outcome_json(outcome), next_attempt, now, instance_id, *from_states),
        )
        if changed == 0:
            current = await self.get_instance(instance_id)
            if (
                current.status.value == target
                and current.last_outcome is not None
                and current.last_outcome.status == outcome.status
                and current.last_outcome.message == outcome.message
            ):
                return current  # idempotent re-record of the same outcome
            raise StateTransitionError(
                f"Cannot move task instance {instance_id} from state "
                f"'{current.status.value}' to '{target}'."
            )
        return await self.get_instance(instance_id)


def _outcome_json(outcome: TaskOutcome) -> str:
    return json.dumps({"status": outcome.status.value, "message": outcome.message})


def _safe_records(rows: Sequence[sqlite3.Row]) -> tuple[TaskInstanceRecord, ...]:
    """Decode rows, skipping (with an error log) corrupt ones.

    One bad row must not break every account's scheduling scan (audit C4).
    """
    decoded: list[TaskInstanceRecord] = []
    for row in rows:
        try:
            decoded.append(_row_to_record(row))
        except PersistenceError:
            logger.exception("skipping corrupt task_instances row")
    return tuple(decoded)


def _row_to_record(row: sqlite3.Row) -> TaskInstanceRecord:
    try:
        return _decode_row(row)
    except (ValueError, KeyError, TypeError) as exc:
        # Corrupt row (bad JSON / timestamp / enum): a typed error carrying
        # the row id so callers can quarantine instead of crashing raw.
        raise PersistenceError(
            f"corrupt task_instances row id={row['id']}: {type(exc).__name__}: {exc}"
        ) from exc


def _decode_row(row: sqlite3.Row) -> TaskInstanceRecord:
    outcome_raw = row["last_outcome"]
    outcome: TaskOutcome | None = None
    if outcome_raw is not None:
        payload = json.loads(outcome_raw)
        outcome = TaskOutcome(
            status=TaskStatus(payload["status"]), message=payload.get("message", "")
        )
    return TaskInstanceRecord(
        id=row["id"],
        account_id=row["account_id"],
        task_id=row["task_id"],
        status=TaskInstanceState(row["status"]),
        attempt_count=row["attempt_count"],
        created_at=_parse_ts(row["created_at"]),
        updated_at=_parse_ts(row["updated_at"]),
        last_outcome=outcome,
        next_attempt_at=_parse_ts(row["next_attempt_at"]) if row["next_attempt_at"] else None,
        cooldown_until=_parse_ts(row["cooldown_until"]) if row["cooldown_until"] else None,
        cooldown_scope=row["cooldown_scope"],
    )
