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
import threading
from collections.abc import AsyncIterator, Callable, Sequence
from contextlib import asynccontextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, TypeVar, cast

from .registry import TaskOutcome, TaskStatus

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_SCHEMA_VERSION = 7
_BUSY_TIMEOUT_MS = 5000

#: v5 — Fernet-encrypted account tokens. One row per account; the
#: Fernet key NEVER lives in this database (external key management:
#: see autoclaimer.credential_store). Owned by the shared migration
#: chain so a single state.db carries task state AND credentials.
_CREDENTIALS_DDL = """
    CREATE TABLE IF NOT EXISTS account_credentials (
        account_id INTEGER PRIMARY KEY,
        encrypted_token BLOB NOT NULL,
        key_version INTEGER NOT NULL DEFAULT 1,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL
    );
    """

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
    3: """
    CREATE TABLE IF NOT EXISTS mudae_state (
        account_id INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('claim', 'rt')),
        deadline TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (account_id, kind)
    );
    """,
    4: """
    -- Audit fix: split the hourly roll-quota window from the $rt perk
    -- window. SQLite CHECK constraints are immutable: rebuild the table
    -- with the widened kind set and copy existing rows.
    CREATE TABLE mudae_state_v4 (
        account_id INTEGER NOT NULL,
        kind TEXT NOT NULL CHECK (kind IN ('claim', 'rt', 'rolls')),
        deadline TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        PRIMARY KEY (account_id, kind)
    );
    INSERT INTO mudae_state_v4 SELECT account_id, kind, deadline, updated_at FROM mudae_state;
    DROP TABLE mudae_state;
    ALTER TABLE mudae_state_v4 RENAME TO mudae_state;
    """,
    5: _CREDENTIALS_DDL,
    6: """
    -- Roadmap item 8: bounded per-attempt execution history. Auxiliary to
    -- task_instances (the source of truth): a history-write failure must
    -- never break outcome recording. Pruned per instance (latest N).
    CREATE TABLE IF NOT EXISTS task_history (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        instance_id INTEGER NOT NULL,
        account_id INTEGER NOT NULL,
        task_id TEXT NOT NULL,
        attempt INTEGER NOT NULL,
        status TEXT NOT NULL,
        message TEXT NOT NULL DEFAULT '',
        recorded_at TEXT NOT NULL
    );
    CREATE INDEX IF NOT EXISTS task_history_instance
        ON task_history (instance_id, id);
    """,
    7: """
    -- Roadmap item 11: structured audit events. Separate from debug
    -- logging: queryable, restart-durable, bounded retention, and the
    -- detail column is ALWAYS secret-free (redacted at the emit site).
    CREATE TABLE IF NOT EXISTS audit_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        timestamp TEXT NOT NULL,
        category TEXT NOT NULL,
        action TEXT NOT NULL,
        account_id INTEGER,
        task_id TEXT,
        detail TEXT NOT NULL DEFAULT ''
    );
    """,
}


class PersistenceError(Exception):
    """Base class for persistence-layer failures."""


class DatabaseBusyError(PersistenceError):
    """The database stayed locked longer than the busy timeout."""


class UnsupportedSchemaVersionError(PersistenceError):
    """The database schema version is newer than this code supports."""


@dataclass(frozen=True)
class IntegrityFinding:
    """One problem found by the read-only integrity diagnostic.

    Severity ``error``: the database or a row is unusable/undecodable.
    Severity ``warning``: suspicious but harmless to operation (e.g.
    leftovers from a removed account). Hints are actionable and NEVER
    involve automatic deletion — potentially recoverable user data is
    only ever touched by an explicit operator action.
    """

    severity: str  # "error" | "warning"
    table: str
    key: str
    problem: str
    hint: str


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


class MudaeStateKind(Enum):
    """Kinds of durable per-account Mudae deadlines (``mudae_state``).

    ``CLAIM``: the claim cooldown deadline (learned from ``$mu`` /
    claim-line replies via the claim-timer rule).
    ``RT``: the ``$rt`` perk-reset deadline (learned from ``$rtu``
    replies; perk-dependent 5h-20h — never assumed, always looked up).
    ``ROLLS``: the hourly roll-quota window (roulette-limit replies;
    distinct from ``RT`` so a 5-20 h perk window can never silence the
    hourly rolls and vice versa — audit fix for the shared-slot
    conflation).
    """

    CLAIM = "claim"
    RT = "rt"
    ROLLS = "rolls"


RESUMABLE_STATES = frozenset({TaskInstanceState.PENDING, TaskInstanceState.WAITING})


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


def _clip(value: object, limit: int = 60) -> str:
    """Bounded repr for finding text (final review): unbounded echoes
    dumped up to 100 KB of hostile row content into CLI output/logs."""
    text = repr(value)
    if len(text) > limit:
        return text[:limit] + f"...({len(text)} chars)"
    return text


def _parse_ts(value: str) -> datetime:
    # PR #8 audit BUG-011: SQLite permits BLOB values in TEXT columns —
    # fromisoformat(bytes) raises TypeError, which previously escaped
    # containment. Normalize to ValueError so every existing handler
    # reports malformed persisted timestamps as findings.
    if not isinstance(value, str):
        raise ValueError(f"persisted timestamp must be text, got {type(value).__name__}")
    if value == "":
        raise ValueError("persisted timestamp is empty")
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        # Naive timestamps are invalid persisted data: every write path
        # produces timezone-aware UTC strings, so naive means corrupt or
        # hand-edited state. Refuse to reinterpret it (agents.md
        # section 17.6; audit F4) — callers treat this as a corrupt row.
        raise ValueError(f"timezone-naive persisted timestamp {value[:60]!r}")
    # Final-review fix (agent-2 F2): every write path stores UTC
    # isoformat() so SQL string-comparison orders timestamps
    # chronologically; an aware-but-OFFSET value parses yet silently
    # breaks that contract (a +14:00 deadline hid an overdue task for
    # ~11 h). Only the exact canonical form is valid persisted data.
    if parsed.astimezone(UTC).isoformat() != value:
        raise ValueError(
            f"non-canonical persisted timestamp {value[:60]!r} (expected UTC isoformat)"
        )
    return parsed.astimezone(UTC)


def _map_sqlite_error(exc: sqlite3.Error) -> PersistenceError:
    """Build the mapped error; callers raise it with ``from exc`` to keep the cause."""
    if isinstance(exc, sqlite3.OperationalError) and "locked" in str(exc).lower():
        return DatabaseBusyError(f"Database remained locked: {exc}")
    return PersistenceError(f"Unexpected database error: {exc}")


#: Bounded wait for abandoned database calls to settle during a
#: transaction-cancellation cleanup (VERIFY-001 fix).
_DRAIN_TIMEOUT_SECONDS = 5.0

#: Per-instance execution-history retention (roadmap item 8): bounded so
#: history can never grow the database unboundedly.
_HISTORY_KEEP_PER_INSTANCE = 50

#: Audit-event retention (roadmap item 11): bounded, newest kept.
_AUDIT_KEEP = 1000


def _silence_task_exception(task: asyncio.Task[Any]) -> None:
    """Retrieve (and drop) an abandoned task's exception so Python does
    not warn about it — the cancellation path intentionally moves on."""
    if not task.cancelled():
        task.exception()


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
        # Thread-level connection guard (repo-sweep cancellation fix):
        # serializes every connection call across worker threads so an
        # abandoned (cancelled-await) threaded op can never interleave
        # with a later boundary or write. See ``_call_threaded``.
        self._conn_guard = threading.RLock()
        # In-flight guarded calls (VERIFY-001 fix): cancellation abandons
        # the await but the tracked task keeps running; the transaction
        # cleanup drains these before rolling back.
        self._in_flight: set[asyncio.Task[Any]] = set()

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
                # Final-review fix: migration/verification run through
                # the GUARDED threaded call (a raw to_thread bypassed the
                # connection guard), and ANY failure — including
                # cancellation — closes and nulls the half-open
                # connection instead of leaking it.
                await self._call_threaded(self._migrate_sync, self._conn)
                # Repo-sweep fix: CREATE TABLE IF NOT EXISTS no-ops on a
                # tampered/foreign schema and user_version still claims
                # current — verify the actual column shapes post-migrate
                # so a mismatched database fails FAST with a typed,
                # named error instead of forever-recoverable write errors.
                await self._call_threaded(self._verify_shape_sync, self._conn)
            except BaseException as exc:
                if self._conn is not None:
                    # Guarded close (repo-sweep): waits for any in-flight
                    # call to drain instead of closing underneath it.
                    try:
                        await self._call_threaded(self._conn.close)
                    except PersistenceError:
                        logger.exception("connection close after failed open also failed")
                    self._conn = None
                if isinstance(exc, sqlite3.Error):
                    raise _map_sqlite_error(exc) from exc
                raise

    async def close(self) -> None:
        """Close the connection; safe to call more than once.

        Final-review fix (agent-3 #3): ``_conn`` is nulled in a
        ``finally`` — a cancellation of the close await previously left
        the store holding a reference to the (thread-completed) closed
        connection, producing misleading typed errors afterwards."""
        async with self._lock:
            if self._conn is not None:
                conn = self._conn
                try:
                    # Guarded (repo-sweep): an abandoned in-flight call
                    # must finish before the connection closes under it.
                    await self._call_threaded(conn.close)
                finally:
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

    def _verify_shape_sync(self, conn: sqlite3.Connection) -> None:
        """Post-migration schema-shape verification (repo-sweep fix)."""
        expected = {
            "task_instances": {
                "id",
                "account_id",
                "task_id",
                "status",
                "attempt_count",
                "created_at",
                "updated_at",
                "last_outcome",
                "next_attempt_at",
                "cooldown_until",
                "cooldown_scope",
            },
            "scope_cooldowns": {"scope", "deadline", "server_directed"},
            "mudae_state": {"account_id", "kind", "deadline", "updated_at"},
            "account_credentials": {
                "account_id",
                "encrypted_token",
                "key_version",
                "created_at",
                "updated_at",
            },
            "task_history": {
                "id",
                "instance_id",
                "account_id",
                "task_id",
                "attempt",
                "status",
                "message",
                "recorded_at",
            },
            "audit_events": {
                "id",
                "timestamp",
                "category",
                "action",
                "account_id",
                "task_id",
                "detail",
            },
        }
        for table, columns in expected.items():
            found = {row[1] for row in conn.execute(f"PRAGMA table_info({table})")}
            missing = columns - found
            if missing:
                raise PersistenceError(
                    f"Database schema shape mismatch for table '{table}': "
                    f"missing column(s) {sorted(missing)}; the database "
                    "appears tampered or foreign. Restore from a backup or "
                    "delete the file to reinitialize."
                )

    # ----- execution helpers -----

    async def _call_threaded(self, fn: Callable[..., _T], *args: Any) -> _T:
        """Run *fn* on a worker thread, serialized by a CONNECTION GUARD.

        Repo-sweep fix (ChatGPT unranked risk, promoted by deterministic
        repro): cancelling the awaiting coroutine does NOT stop the
        underlying ``to_thread`` operation — the asyncio lock releases
        (or a transaction's ROLLBACK runs) while the abandoned call is
        still in flight. That let a cancelled transaction's write land
        afterwards in AUTOCOMMIT mode (orphaned write) and let the next
        BEGIN race an abandoned ROLLBACK ("cannot start a transaction
        within a transaction"). Acquiring a thread-level guard around
        EVERY connection call establishes happens-before ordering:
        boundaries never start until prior calls finish, and later
        callers wait for abandoned calls to drain — cancellation can
        abandon an await, never the connection's serialization.
        """

        def guarded() -> _T:
            with self._conn_guard:
                return fn(*args)

        # VERIFY-001 fix: run as a tracked task behind a shield. A
        # cancellation abandons the AWAIT, never the work — and the
        # abandoned task stays registered in ``_in_flight`` so a
        # transaction's cleanup can WAIT for it to settle before rolling
        # back (no reliance on threading.RLock acquisition order).
        task: asyncio.Task[_T] = asyncio.create_task(asyncio.to_thread(guarded))
        self._in_flight.add(task)
        task.add_done_callback(self._in_flight.discard)
        task.add_done_callback(_silence_task_exception)
        try:
            return await asyncio.shield(task)
        except sqlite3.Error as exc:
            raise _map_sqlite_error(exc) from exc

    async def _drain_and_rollback(self, where: str) -> None:
        """Transaction-cancellation cleanup: settle, THEN roll back.

        Waits (bounded) for every in-flight guarded call submitted before
        this point — including the abandoned BEGIN or body write — before
        the ROLLBACK reaches the connection. ``threading.RLock`` gives no
        FIFO guarantee, so without this drain the rollback could acquire
        the guard first, no-op, and let the abandoned BEGIN land after it
        (deterministically reproduced by the guard-reorder regression
        test). Shielded by callers so a second cancellation cannot skip
        the rollback; a drain timeout proceeds best-effort (logged).
        """
        pending = [t for t in self._in_flight if not t.done()]
        if pending:
            _done, stuck = await asyncio.wait(pending, timeout=_DRAIN_TIMEOUT_SECONDS)
            if stuck:
                logger.warning(
                    "cancelled database call(s) did not settle in time; rolling back anyway",
                    extra={"operation": "store.drain", "outcome": "drain_timeout"},
                )
        try:
            await self._call_threaded(self._connection().execute, "ROLLBACK")
        except sqlite3.Error:
            logger.exception(f"rollback after {where} also failed")

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
        if self._txn_owner.get() is asyncio.current_task():
            yield
            return
        async with self._lock:
            token = self._txn_owner.set(asyncio.current_task())
            try:
                await self._call_threaded(self._connection().execute, "BEGIN IMMEDIATE")
            except BaseException:
                self._txn_owner.reset(token)
                # PR #6 review + VERIFY-001: cancellation never stops the
                # abandoned ``to_thread`` call — the BEGIN may still
                # EXECUTE after this coroutine unwinds, leaking an open
                # transaction. The DRAIN-then-rollback settles the
                # abandoned call FIRST (no RLock-FIFO assumption), then
                # rolls back whatever it opened.
                await asyncio.shield(self._drain_and_rollback("cancelled BEGIN"))
                raise
            try:
                yield
            except BaseException:
                self._txn_owner.reset(token)
                await asyncio.shield(self._drain_and_rollback("transaction failure"))
                raise
            self._txn_owner.reset(token)
            try:
                await self._call_threaded(self._connection().execute, "COMMIT")
            except BaseException:
                await asyncio.shield(self._drain_and_rollback("failed commit"))
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

    async def due_instances(self, account_id: int | None = None) -> tuple[TaskInstanceRecord, ...]:
        """Claimable instances whose retry time (if any) has passed.

        ``account_id`` scopes the query in SQL so per-account worker
        loops stop scanning every account's rows (audit perf batch 6).
        """
        now = _format_ts(self._clock())
        if account_id is None:
            rows = await self._run(
                self._fetchall_sync,
                "SELECT * FROM task_instances WHERE status IN ('pending', 'waiting') "
                "AND (next_attempt_at IS NULL OR next_attempt_at <= ?)",
                (now,),
            )
        else:
            rows = await self._run(
                self._fetchall_sync,
                "SELECT * FROM task_instances WHERE account_id = ? "
                "AND status IN ('pending', 'waiting') "
                "AND (next_attempt_at IS NULL OR next_attempt_at <= ?)",
                (account_id, now),
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
        """pending|waiting -> running, counting one new attempt.

        Known limitation (final review, agent-3 #2 / agent-5 F1): the
        CAS UPDATE and the read-back SELECT are two lock acquisitions —
        a writer landing between them returns the LATER writer's state.
        Wrapping them in one transaction changes the lock profile and
        breaks the verified cancellation contracts (the transaction
        fast-path bypasses the store lock), so the tear is accepted and
        documented instead: the only running->pending actor is
        recover_interrupted (startup-only, behind the _started guard);
        the plausible interleaving is stop()-cancels-a-claim-in-flight
        followed by restart — bounded by at-least-once semantics and
        the attempt-count CAS itself."""
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
        record = await self._record_terminal(
            instance_id, "succeeded", outcome, from_states=("running",)
        )
        if record is None:
            # LOW-034: idempotent re-record — this attempt's history row
            # already exists; appending again would duplicate it.
            return await self.get_instance(instance_id)
        await self._append_history(
            record.id,
            record.account_id,
            record.task_id,
            record.attempt_count,
            "succeeded",
            outcome.message,
        )
        return record

    async def record_failure(
        self, instance_id: int, outcome: TaskOutcome, *, retry_at: datetime | None
    ) -> TaskInstanceRecord:
        """running -> waiting (with retry time) or failed (when retry_at is None)."""
        target = "waiting" if retry_at is not None else "failed"
        record = await self._record_terminal(
            instance_id, target, outcome, from_states=("running",), retry_at=retry_at
        )
        if record is None:
            # LOW-034: idempotent re-record — see record_success.
            return await self.get_instance(instance_id)
        await self._append_history(
            record.id,
            record.account_id,
            record.task_id,
            record.attempt_count,
            target,
            outcome.message,
        )
        return record

    async def _append_history(
        self,
        instance_id: int,
        account_id: int,
        task_id: str,
        attempt: int,
        status: str,
        message: str,
    ) -> None:
        """Append one bounded history row (roadmap item 8).

        AUXILIARY by contract: any failure here is logged and swallowed —
        the task_instances outcome write is the source of truth and must
        never break because history could not be recorded.
        """
        now = _format_ts(self._clock())
        try:
            await self._run(
                self._update_sync,
                "INSERT INTO task_history "
                "(instance_id, account_id, task_id, attempt, status, message, recorded_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (instance_id, account_id, task_id, attempt, status, message[:500], now),
            )
            await self._run(
                self._update_sync,
                "DELETE FROM task_history WHERE instance_id = ? AND id NOT IN ("
                " SELECT id FROM task_history WHERE instance_id = ? "
                f" ORDER BY id DESC LIMIT {_HISTORY_KEEP_PER_INSTANCE})",
                (instance_id, instance_id),
            )
        except PersistenceError:
            logger.exception(
                "execution history append failed (outcome unaffected)",
                extra={"operation": "history.append", "outcome": "history_failed"},
            )

    async def task_history(
        self, instance_id: int, *, limit: int = _HISTORY_KEEP_PER_INSTANCE
    ) -> tuple[tuple[int, str, str, str], ...]:
        """(attempt, status, message, recorded_at) rows, newest first."""
        rows = await self._run(
            self._fetchall_sync,
            "SELECT attempt, status, message, recorded_at FROM task_history "
            "WHERE instance_id = ? ORDER BY id DESC LIMIT ?",
            (instance_id, limit),
        )
        return tuple((int(r[0]), str(r[1]), str(r[2]), str(r[3])) for r in rows)

    async def record_audit_event(
        self,
        *,
        category: str,
        action: str,
        account_id: int | None = None,
        task_id: str | None = None,
        detail: str = "",
    ) -> None:
        """Persist one structured audit event (roadmap item 11).

        AUXILIARY by contract (like task history): an audit failure is
        logged and swallowed — the audited operation must never break.
        *detail* must already be secret-free (redacted at the emit site).
        Retention is bounded (latest ``_AUDIT_KEEP``).
        """
        try:
            await self._run(
                self._update_sync,
                "INSERT INTO audit_events "
                "(timestamp, category, action, account_id, task_id, detail) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    _format_ts(self._clock()),
                    category,
                    action,
                    account_id,
                    task_id,
                    detail[:500],
                ),
            )
            await self._run(
                self._update_sync,
                f"DELETE FROM audit_events WHERE id NOT IN ("
                f" SELECT id FROM audit_events ORDER BY id DESC LIMIT {_AUDIT_KEEP})",
                (),
            )
        except PersistenceError:
            logger.exception(
                "audit event could not be recorded (operation unaffected)",
                extra={"operation": "audit.append", "outcome": "audit_failed"},
            )

    async def backup_to(self, path: str | Path) -> int:
        """Consistent database backup via ``VACUUM INTO`` (roadmap 14).

        BUG-039: runs through :meth:`_run` — the store's ASYNC lock (the
        same transaction-level serialization every other operation uses),
        not just the per-call connection guard. A ``transaction()``
        block releases the guard between its individual statements while
        the connection still holds an open transaction, and SQLite
        rejects ``VACUUM INTO`` there ("cannot VACUUM from within a
        transaction") — the advertised live-backup guarantee was
        therefore unreliable. Under the lock, a concurrent transaction
        COMMITS first and the backup sees a consistent database (never
        torn); calling from inside one's own transaction fails loudly,
        which is the caller's contradiction. The result is a compacted,
        self-contained copy. Returns the backup size in bytes.
        """
        target = Path(path)
        if target.exists():
            raise PersistenceError(f"backup target {target} already exists; refusing to overwrite")
        target.parent.mkdir(parents=True, exist_ok=True)

        def _vacuum_into() -> None:
            conn = self._connection()
            with self._conn_guard:
                conn.execute("VACUUM INTO ?", (str(target),))

        try:
            await asyncio.to_thread(_vacuum_into)
        except sqlite3.Error as exc:
            raise _map_sqlite_error(exc) from exc
        return target.stat().st_size

    async def database_stats(self) -> dict[str, Any]:
        """Growth-monitoring snapshot (roadmap item 14): file size, page
        counts, and per-table row counts. Read-only."""

        def _stats() -> dict[str, Any]:
            conn = self._connection()
            with self._conn_guard:
                page_count = conn.execute("PRAGMA page_count").fetchone()[0]
                freelist = conn.execute("PRAGMA freelist_count").fetchone()[0]
                page_size = conn.execute("PRAGMA page_size").fetchone()[0]
                journal = conn.execute("PRAGMA journal_mode").fetchone()[0]
                rows = {
                    table: conn.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0]
                    for table in (
                        "task_instances",
                        "scope_cooldowns",
                        "mudae_state",
                        "account_credentials",
                        "task_history",
                        "audit_events",
                    )
                }
            return {
                "path": str(self._path),
                "size_bytes": self._path.stat().st_size if self._path.exists() else 0,
                "page_count": int(page_count),
                "freelist_pages": int(freelist),
                "page_size": int(page_size),
                "journal_mode": str(journal),
                "rows": rows,
            }

        return await asyncio.to_thread(_stats)

    async def audit_events(self, *, limit: int = 50) -> tuple[tuple[Any, ...], ...]:
        """Recent audit events, newest first:
        (timestamp, category, action, account_id, task_id, detail)."""
        rows = await self._run(
            self._fetchall_sync,
            "SELECT timestamp, category, action, account_id, task_id, detail "
            "FROM audit_events ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return tuple(tuple(row) for row in rows)

    async def requeue_instance(self, instance_id: int) -> TaskInstanceRecord:
        """Dead-letter manual retry (roadmap item 8): ``failed -> pending``
        with a FRESH attempt budget; the requeue event is recorded in the
        execution history. Only terminal-failed rows are requeueable —
        live/healthy rows are a configuration error, not a retry."""
        now = _format_ts(self._clock())
        changed = await self._run(
            self._update_sync,
            "UPDATE task_instances SET status = 'pending', attempt_count = 0, "
            "next_attempt_at = NULL, updated_at = ? "
            "WHERE id = ? AND status = 'failed'",
            (now, instance_id),
        )
        if changed == 0:
            current = await self.get_instance(instance_id)
            raise StateTransitionError(
                "only dead-lettered (failed) tasks can be requeued — "
                f"instance {instance_id} is '{current.status.value}'"
            )
        record = await self.get_instance(instance_id)
        await self._append_history(
            record.id,
            record.account_id,
            record.task_id,
            0,
            "requeued",
            "manual requeue from dead-letter",
        )
        return record

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
            return current  # idempotent: already failed; history already has it
        record = await self.get_instance(instance_id)
        # BUG-038: pre-claim terminal failures (invalid config, removed
        # assignment, unregistered type) are part of the execution trail —
        # same auxiliary-history contract as record_success /
        # record_failure / requeue_instance: a history-write failure is
        # logged and swallowed inside _append_history and never undoes
        # the authoritative terminal transition above.
        await self._append_history(
            record.id,
            record.account_id,
            record.task_id,
            record.attempt_count,
            "abandoned",
            message,
        )
        return record

    async def integrity_report(
        self, known_account_ids: set[int] | None = None
    ) -> tuple[IntegrityFinding, ...]:
        """Read-only database diagnostic (roadmap item 2 — P0).

        Never writes, never deletes, never repairs: every problem is
        reported as an actionable finding for the operator. Covers:

        - ``PRAGMA integrity_check`` (page-level corruption),
        - undecodable task rows (corrupt JSON outcomes, malformed
          timestamps, unsupported status values),
        - malformed ``scope_cooldowns`` rows,
        - malformed ``mudae_state`` rows (unsupported kind enums AND
          malformed deadlines — independently detected per row, BUG-017),
        - orphaned references: task/credential/Mudae-state rows for
          accounts absent from *known_account_ids* (when supplied by the
          caller, e.g. from the parsed accounts.json); one warning per
          account regardless of how many tables hold its rows
          (VERIFY-004).
        """
        findings: list[IntegrityFinding] = []

        def _scan_rows(conn: sqlite3.Connection, out: list[IntegrityFinding]) -> None:  # noqa: C901
            # 1. SQLite-level integrity.
            rows = conn.execute("PRAGMA integrity_check").fetchall()
            problems = [_clip(str(r[0]), 80) for r in rows if str(r[0]).lower() != "ok"]
            if problems:
                out.append(
                    IntegrityFinding(
                        severity="error",
                        table="database",
                        key="-",
                        problem="; ".join(problems[:5])
                        + (f" (+{len(problems) - 5} more)" if len(problems) > 5 else ""),
                        hint="Restore state.db from a backup (see docs/running.md: "
                        "Database diagnostics and recovery).",
                    )
                )
            # 2. Task rows the application cannot decode.
            for row in conn.execute(
                "SELECT id, account_id, task_id FROM task_instances ORDER BY id"
            ):
                try:
                    full = conn.execute(
                        "SELECT * FROM task_instances WHERE id = ?", (row[0],)
                    ).fetchone()
                    _row_to_record(full)
                except PersistenceError as exc:
                    out.append(
                        IntegrityFinding(
                            severity="error",
                            table="task_instances",
                            key=f"id={_clip(row[0])} account={_clip(row[1])} task={_clip(row[2])}",
                            problem=_clip(str(exc), 200),
                            hint="Quarantine the row (sqlite3 CLI) or restore the "
                            "backup; runtime queries already skip it.",
                        )
                    )
            # 3. Malformed cooldown rows.
            for row in conn.execute("SELECT scope, deadline, server_directed FROM scope_cooldowns"):
                try:
                    _parse_ts(row[1])
                except ValueError:
                    out.append(
                        IntegrityFinding(
                            severity="error",
                            table="scope_cooldowns",
                            key=f"scope={_clip(row[0])}",
                            problem=f"malformed deadline {_clip(row[1])}",
                            hint="Delete the row with the sqlite3 CLI; the limiter "
                            "re-learns cooldowns from the server.",
                        )
                    )
                if row[2] not in (0, 1):
                    out.append(
                        IntegrityFinding(
                            severity="error",
                            table="scope_cooldowns",
                            key=f"scope={_clip(row[0])}",
                            problem=(f"server_directed flag is {_clip(row[2])} (expected 0/1)"),
                            hint="Delete the row with the sqlite3 CLI.",
                        )
                    )
            # 4. Malformed mudae_state rows.
            for row in conn.execute("SELECT account_id, kind, deadline FROM mudae_state"):
                # BUG-017: the kind must be a supported enum value — an
                # unsupported kind is silently skipped by the runtime's
                # deadline loader, so without this check the diagnostic
                # could report a healthy database over dead rows. Validated
                # INDEPENDENTLY of the deadline so one invalid field never
                # masks the other. Read-only: the row is never touched.
                try:
                    MudaeStateKind(str(row[1]))
                except ValueError:
                    out.append(
                        IntegrityFinding(
                            severity="error",
                            table="mudae_state",
                            key=f"account={_clip(row[0])} kind={_clip(row[1])}",
                            problem=(
                                f"unsupported kind {_clip(row[1])} "
                                f"(expected one of: {', '.join(k.value for k in MudaeStateKind)})"
                            ),
                            hint="Delete the row with the sqlite3 CLI; the task "
                            "re-learns the window on its next round.",
                        )
                    )
                try:
                    _parse_ts(row[2])
                except ValueError:
                    out.append(
                        IntegrityFinding(
                            severity="error",
                            table="mudae_state",
                            key=f"account={_clip(row[0])} kind={_clip(row[1])}",
                            problem=f"malformed deadline {_clip(row[2])}",
                            hint="Delete the row with the sqlite3 CLI; the task "
                            "re-learns the window on its next round.",
                        )
                    )
            # 5. Orphaned account references (needs the caller's account set).
            if known_account_ids is not None:
                for row in conn.execute(
                    "SELECT DISTINCT account_id FROM task_instances "
                    "UNION SELECT account_id FROM account_credentials "
                    "UNION SELECT account_id FROM mudae_state"
                ):
                    if row[0] not in known_account_ids:
                        out.append(
                            IntegrityFinding(
                                severity="warning",
                                table="task_instances/account_credentials/mudae_state",
                                key=f"account={_clip(row[0])}",
                                problem="account is not present in accounts.json",
                                hint="Leftovers from a removed account. Rows are "
                                "harmless; remove them explicitly (sqlite3 CLI / "
                                "'credential delete') if the account is gone for good.",
                            )
                        )

        def _scan_sync() -> None:
            conn = self._connection()
            try:
                _scan_rows(conn, findings)
            except Exception as exc:  # noqa: BLE001
                # Final-review widening: heavy corruption can make even
                # the diagnostic's own queries fail — sqlite errors AND
                # any hostile-row surprise (the scanner is the tool you
                # reach for when rows are corrupt; it must never be the
                # crash). That failure IS the finding. The traceback is
                # logged so scan-code BUGS stay distinguishable from
                # data corruption.
                logger.exception("integrity scan hit an unexpected error")
                findings.append(
                    IntegrityFinding(
                        severity="error",
                        table="database",
                        key="-",
                        problem=(
                            f"scan could not complete: {type(exc).__name__}: {_clip(str(exc), 120)}"
                        ),
                        hint="Restore state.db from a backup (see docs/running.md: "
                        "Database diagnostics and recovery).",
                    )
                )

        await self._call_threaded(_scan_sync)
        return tuple(findings)

    async def fail_account_queue(self, account_id: int, message: str) -> int:
        """Bulk ``pending|waiting -> failed`` for one account's instances.

        Account-fatal policy (owner directive 2026-10-28): when an account's
        credentials or gateway session die, its queued work must fail
        terminally instead of retrying. Returns the number of instances
        failed. Terminal rows are untouched (idempotent); ``running`` rows
        are left to their own outcome path — their in-flight execution
        records its own failure.
        """
        now = _format_ts(self._clock())
        changed = await self._run(
            self._update_sync,
            "UPDATE task_instances SET status = 'failed', last_outcome = ?, "
            "next_attempt_at = NULL, updated_at = ? "
            "WHERE account_id = ? AND status IN ('pending', 'waiting')",
            (_outcome_json(TaskOutcome(TaskStatus.FAILED_PERMANENT, message)), now, account_id),
        )
        return changed

    # ----- scope cooldowns (rate limiter persistence) -----

    async def set_scope_cooldown(
        self, scope: str, deadline: datetime, *, server_directed: bool
    ) -> None:
        """Upsert a scope cooldown; only ever extends (max) and keeps the
        server-directed flag sticky once set."""
        now = _format_ts(self._clock())
        # Opportunistic retention: expired rows never accumulate.
        await self._run(
            self._update_sync,
            "DELETE FROM scope_cooldowns WHERE deadline <= ?",
            (now,),
        )
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
        """Return unexpired scope cooldowns as {scope: (deadline, server_directed)}.

        Corrupt rows (malformed or timezone-naive deadline, non-integer
        flag) are contained: skipped with an ERROR log naming the scope,
        raising a typed ``PersistenceError`` only as the logged diagnosis
        (audit F3). One bad row must never crash manager startup — a
        skipped cooldown self-corrects when the next violation re-records
        it, and the log makes the corruption diagnosable.
        """
        now = _format_ts(self._clock())
        rows = await self._run(
            self._fetchall_sync,
            "SELECT scope, deadline, server_directed FROM scope_cooldowns WHERE deadline > ?",
            (now,),
        )
        cooldowns: dict[str, tuple[datetime, bool]] = {}
        for row in rows:
            scope = row["scope"]
            try:
                deadline = _parse_ts(row["deadline"])
                raw_flag = row["server_directed"]
                # Repo-sweep fix: float values (0.9, 1.7) previously
                # truncated silently — require an exact 0/1 integer.
                if not isinstance(raw_flag, int) or raw_flag not in (0, 1):
                    raise ValueError(f"server_directed flag is {_clip(raw_flag)}")
                server_directed = raw_flag
            except (ValueError, TypeError) as exc:
                error = PersistenceError(
                    f"corrupt scope_cooldowns row scope={_clip(scope)}: "
                    f"{type(exc).__name__}: {_clip(str(exc), 120)}"
                )
                logger.error("skipping %s", error)
                continue
            cooldowns[scope] = (deadline, bool(server_directed))
        return cooldowns

    # ----- Mudae deadlines (per-account durable domain state) -----

    async def set_mudae_deadline(
        self, account_id: int, kind: MudaeStateKind, deadline: datetime
    ) -> None:
        """Store one learned Mudae deadline (absolute UTC).

        A fresh lookup REPLACES the stored value: the reply is the
        current truth (an ``$rtu`` check after ``$rt`` use supersedes any
        previously assumed deadline). Naive datetimes are rejected at
        this boundary (``_format_ts``). Expired rows for other kinds are
        pruned opportunistically (audit hygiene, mirrors
        ``set_scope_cooldown``).
        """
        now = _format_ts(self._clock())
        await self._run(
            self._update_sync,
            "DELETE FROM mudae_state WHERE deadline <= ? AND account_id = ?",
            (now, account_id),
        )
        await self._run(
            self._update_sync,
            "INSERT INTO mudae_state (account_id, kind, deadline, updated_at) "
            "VALUES (?, ?, ?, ?) "
            "ON CONFLICT (account_id, kind) DO UPDATE SET "
            "deadline = excluded.deadline, updated_at = excluded.updated_at",
            (account_id, kind.value, _format_ts(deadline), now),
        )

    async def clear_mudae_deadline(self, account_id: int, kind: MudaeStateKind) -> None:
        """Remove one learned Mudae deadline (no-op if absent).

        Consolidated review #5: an ``$rtu`` lookup reporting AVAILABLE
        ("$rt is available!") supersedes any previously stored deadline —
        the ready state is current truth, so the stale future deadline is
        cleared rather than left to mislead later reads. "Missing
        duration is not zero" semantics are untouched: absence after a
        clear means "known available", never a fabricated deadline.
        """
        await self._run(
            self._update_sync,
            "DELETE FROM mudae_state WHERE account_id = ? AND kind = ?",
            (account_id, kind.value),
        )

    async def reschedule_success(
        self, instance_id: int, outcome: TaskOutcome, retry_at: datetime
    ) -> TaskInstanceRecord:
        """Record a SUCCESSFUL round of a RECURRING task and schedule
        the next one (milestone 7).

        ``running -> waiting`` with ``next_attempt_at = retry_at`` and
        the attempt count RESET: each recurring round is a fresh window,
        never bounded by the transient-retry limit. Only the executing
        (claimed/running) row may be rescheduled.
        """
        now = _format_ts(self._clock())
        changed = await self._run(
            self._update_sync,
            "UPDATE task_instances SET status = 'waiting', last_outcome = ?, "
            "next_attempt_at = ?, attempt_count = 0, updated_at = ? "
            "WHERE id = ? AND status = 'running'",
            (_outcome_json(outcome), _format_ts(retry_at), now, instance_id),
        )
        if changed == 0:
            current = await self.get_instance(instance_id)
            if (
                current.status is TaskInstanceState.WAITING
                and current.last_outcome is not None
                and current.last_outcome.status == outcome.status
                and current.last_outcome.message == outcome.message
                and current.next_attempt_at == retry_at
                and current.attempt_count == 0
            ):
                return current  # idempotent re-record of the same round (audit fix)
            raise StateTransitionError(
                f"Cannot reschedule task instance {instance_id} from state "
                f"'{current.status.value}' (recurring success requires 'running')."
            )
        return await self.get_instance(instance_id)

    async def mudae_deadlines(self, account_id: int) -> dict[MudaeStateKind, datetime]:
        """Learned deadlines for one account ({kind: deadline}).

        Corrupt rows (malformed/naive deadline, unknown kind) are
        contained per the F3/F4 policy: skipped with an ERROR log naming
        account and kind; the read never crashes.
        """
        rows = await self._run(
            self._fetchall_sync,
            "SELECT kind, deadline FROM mudae_state WHERE account_id = ?",
            (account_id,),
        )
        deadlines: dict[MudaeStateKind, datetime] = {}
        for row in rows:
            kind_raw = row["kind"]
            try:
                kind = MudaeStateKind(kind_raw)
                deadline = _parse_ts(row["deadline"])
            except (ValueError, TypeError) as exc:
                error = PersistenceError(
                    f"corrupt mudae_state row account={account_id} kind={_clip(kind_raw)}: "
                    f"{type(exc).__name__}: {exc}"
                )
                logger.error("skipping %s", error)
                continue
            deadlines[kind] = deadline
        return deadlines

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
    ) -> TaskInstanceRecord | None:
        """``None`` when the row already held this exact outcome (the
        idempotent re-record path) — callers use that to avoid appending
        a duplicate history row for the same attempt (LOW-034)."""
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
                return None  # idempotent re-record of the same outcome
            raise StateTransitionError(
                f"Cannot move task instance {instance_id} from state "
                f"'{current.status.value}' to '{target}'."
            )
        return await self.get_instance(instance_id)


def _outcome_json(outcome: TaskOutcome) -> str:
    payload: dict[str, Any] = {"status": outcome.status.value, "message": outcome.message}
    if outcome.retry_at is not None:
        payload["retry_at"] = _format_ts(outcome.retry_at)
    return json.dumps(payload)


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
    except (ValueError, KeyError, TypeError, IndexError, RecursionError) as exc:
        # Corrupt row (bad JSON / timestamp / enum / MISSING COLUMN —
        # repo-sweep: IndexError from a hand-made schema previously
        # skipped containment entirely): a typed error carrying the row
        # id so callers can quarantine instead of crashing raw.
        raise PersistenceError(
            f"corrupt task_instances row id={row['id']}: {type(exc).__name__}: "
            f"{_clip(str(exc), 120)}"
        ) from exc


def _decode_row(row: sqlite3.Row) -> TaskInstanceRecord:
    outcome_raw = row["last_outcome"]
    outcome: TaskOutcome | None = None
    if outcome_raw is not None:
        payload = json.loads(outcome_raw)
        # Final-review fix: valid JSON that is NOT an object ('null',
        # numbers, strings, arrays) previously reached .get() and raised
        # an AttributeError OUTSIDE every containment — crashing the
        # integrity scanner, the worker loop, and the REPL.
        if not isinstance(payload, dict):
            raise ValueError(f"outcome JSON is not an object: {type(payload).__name__}")
        retry_at_raw = payload.get("retry_at")
        if retry_at_raw == "":
            raise ValueError("outcome retry_at is an empty string")
        outcome = TaskOutcome(
            status=TaskStatus(payload["status"]),
            message=payload.get("message", ""),
            retry_at=_parse_ts(retry_at_raw) if retry_at_raw is not None else None,
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
        next_attempt_at=(
            _parse_ts(row["next_attempt_at"]) if row["next_attempt_at"] is not None else None
        ),
        cooldown_until=(
            _parse_ts(row["cooldown_until"]) if row["cooldown_until"] is not None else None
        ),
        cooldown_scope=row["cooldown_scope"],
    )
