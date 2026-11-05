"""Batch 3 regression tests (Phase 9 persistence hardening)."""

import asyncio
import contextlib
import itertools
import sqlite3
import threading
from pathlib import Path

import pytest

from autoclaimer.persistence import (
    PersistenceError,
    TaskInstanceState,
    TaskStateStore,
    UnsupportedSchemaVersionError,
    _map_sqlite_error,
)

ACCOUNT = 271485504361996288


async def open_store(tmp_path: Path) -> TaskStateStore:
    store = TaskStateStore(tmp_path / "state.db")
    await store.open()
    return store


def inject_sql_failure(store: TaskStateStore, keyword: str) -> None:
    """Make statements starting with *keyword* fail inside the worker thread."""
    from collections.abc import Callable
    from typing import Any, cast

    original = store._call_threaded  # noqa: SLF001

    def bomb(*args: object) -> object:
        raise sqlite3.OperationalError("injected failure")

    async def injected(fn: object, *args: object) -> object:
        if args and str(args[0]).startswith(keyword):
            return await original(bomb, *args)
        callable_fn = cast("Callable[..., Any]", fn)
        return await original(callable_fn, *args)

    store._call_threaded = injected  # type: ignore[assignment,method-assign]


# C1: failed BEGIN must not leak the transaction-owner flag


async def test_failed_begin_does_not_leak_transaction_owner(tmp_path: Path) -> None:
    store = await open_store(tmp_path)
    inject_sql_failure(store, "BEGIN")
    with pytest.raises(PersistenceError):
        async with store.transaction():
            pass
    store._call_threaded = TaskStateStore._call_threaded.__get__(store)  # type: ignore[method-assign]
    # Flag not leaked: a subsequent store call acquires the lock normally
    # (with the flag leaked, the old code would skip locking entirely).
    await store.create_instance(ACCOUNT, "job")
    assert len(await store.list_instances()) == 1
    await store.close()


# C1b: failed COMMIT rolls back and leaves no dangling transaction


async def test_failed_commit_rolls_back(tmp_path: Path) -> None:
    store = await open_store(tmp_path)
    record = await store.create_instance(ACCOUNT, "job")
    inject_sql_failure(store, "COMMIT")
    with pytest.raises(PersistenceError):
        async with store.transaction():
            await store.claim(record.id)
    store._call_threaded = TaskStateStore._call_threaded.__get__(store)  # type: ignore[method-assign]
    # The claim was rolled back; a new transaction works (no dangling txn).
    row = await store.get_instance_by_ids(ACCOUNT, "job")
    assert row.status is TaskInstanceState.PENDING
    async with store.transaction():
        await store.claim(record.id)
    row = await store.get_instance_by_ids(ACCOUNT, "job")
    assert row.status is TaskInstanceState.RUNNING
    await store.close()


# C2: child tasks inside a transaction must NOT bypass the store lock


async def test_child_task_inside_transaction_awaits_lock(tmp_path: Path) -> None:
    store = await open_store(tmp_path)
    record = await store.create_instance(ACCOUNT, "job")
    child_started = asyncio.Event()

    async def child() -> None:
        child_started.set()
        # Same connection, different task: must serialize via the lock
        # (previously the inherited ContextVar let it skip the lock).
        await store.create_instance(ACCOUNT, "other")

    async with store.transaction():
        await store.claim(record.id)
        task = asyncio.create_task(child())
        await child_started.wait()
        await asyncio.sleep(0.02)  # child parks on the lock, not interleaving
        assert not task.done()
    await task  # lock released at commit: the child proceeds now
    rows = await store.list_instances()
    assert {r.task_id for r in rows} == {"job", "other"}
    await store.close()


# C3: open() must not leak the connection on unsupported schema versions


async def test_open_cleans_connection_on_unsupported_version(tmp_path: Path) -> None:
    path = tmp_path / "state.db"
    store = await open_store(tmp_path)
    await store.close()
    conn = sqlite3.connect(path)
    conn.execute("PRAGMA user_version = 99")
    conn.close()
    doomed = TaskStateStore(path)
    with pytest.raises(UnsupportedSchemaVersionError):
        await doomed.open()
    assert doomed._conn is None  # noqa: SLF001 - connection closed, not leaked
    with pytest.raises(PersistenceError, match="not open"):
        await doomed.list_instances()


# C4: corrupt rows are contained


async def test_corrupt_row_is_skipped_in_scans_and_typed_when_fetched(
    tmp_path: Path,
) -> None:
    store = await open_store(tmp_path)
    good = await store.create_instance(ACCOUNT, "good")
    bad = await store.create_instance(ACCOUNT, "bad")
    await store.close()
    conn = sqlite3.connect(tmp_path / "state.db")
    conn.execute("UPDATE task_instances SET last_outcome = 'not-json' WHERE id = ?", (bad.id,))
    conn.commit()
    conn.close()

    store = await open_store(tmp_path)
    # List/due scans skip the corrupt row and still return the healthy one.
    rows = await store.list_instances()
    assert [r.task_id for r in rows] == ["good"]
    due = await store.due_instances()
    assert [r.task_id for r in due] == ["good"]
    # Direct fetch of the corrupt row raises a typed PersistenceError.
    with pytest.raises(PersistenceError, match=f"row id={bad.id}"):
        await store.get_instance(bad.id)
    del good
    await store.close()


def test_error_mapping_unchanged() -> None:
    assert isinstance(
        _map_sqlite_error(sqlite3.OperationalError("database is locked")),
        PersistenceError,
    )


# Repo-sweep: cancellation vs threaded SQLite ops (ChatGPT unranked risk)


async def test_cancellation_mid_transaction_never_orphans_writes(tmp_path: Path) -> None:
    """Cancel a transaction while its threaded write is mid-op.

    The abandoned thread call must never outlive the rollback: with the
    original code the ROLLBACK completed first and the late write landed
    in AUTOCOMMIT mode — a cancelled transaction's write persisted."""
    store = await open_store(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "orphan_probe")
        started = threading.Event()
        release = threading.Event()
        done = threading.Event()
        original = store._call_threaded  # noqa: SLF001

        async def slow_claim(fn: object, *args: object) -> object:
            if args and str(args[0]).startswith("UPDATE task_instances SET status = 'running'"):

                def blocked() -> object:
                    from collections.abc import Callable
                    from typing import Any, cast

                    started.set()
                    assert release.wait(5.0)
                    result = cast("Callable[..., Any]", fn)(*args)
                    done.set()
                    return result

                return await original(blocked)
            from collections.abc import Callable
            from typing import Any, cast

            return await original(cast("Callable[..., Any]", fn), *args)

        store._call_threaded = slow_claim  # type: ignore[assignment,method-assign]

        async def txn_body() -> None:
            async with store.transaction():
                await store.claim(record.id)

        task = asyncio.create_task(txn_body())
        assert await asyncio.to_thread(started.wait, 5.0)
        task.cancel()
        # Let the cancellation reach the rollback boundary.
        await asyncio.sleep(0.1)
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await asyncio.to_thread(done.wait, 5.0)  # abandoned op fully applied
        row = await store.get_instance(record.id)
        assert row.status is TaskInstanceState.PENDING, "orphaned write persisted"
        assert row.attempt_count == 0
    finally:
        await store.close()


async def test_transaction_boundary_survives_repeated_cancellation(tmp_path: Path) -> None:
    """A cancel while ROLLBACK is mid-flight must not let the NEXT
    transaction race it.

    Deterministic interleave: transaction 2 issues BEGIN IMMEDIATE while
    transaction 1's abandoned ROLLBACK is still mid-op. Without
    serialization the BEGIN hits ``cannot start a transaction within a
    transaction``; with it, the BEGIN waits for the boundary and both
    transactions behave (and a repeated cancel cannot strand anything)."""
    store = await open_store(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "boundary_probe")
        rollback_started = threading.Event()
        release = threading.Event()
        rollback_done = threading.Event()
        begin2_seen = threading.Event()
        go2 = threading.Event()
        original = store._call_threaded  # noqa: SLF001
        begin_calls = itertools.count()

        async def orchestrated(fn: object, *args: object) -> object:
            from collections.abc import Callable
            from typing import Any, cast

            head = str(args[0]) if args else ""
            if head.startswith("ROLLBACK"):

                def blocked_rollback() -> object:
                    rollback_started.set()
                    assert release.wait(5.0)
                    result = cast("Callable[..., Any]", fn)(*args)
                    rollback_done.set()
                    return result

                return await original(blocked_rollback)
            if head.startswith("BEGIN IMMEDIATE") and next(begin_calls) > 0:
                # Issued while the abandoned rollback is still mid-op;
                # the gate below lets the test pin that interleaving.
                begin2_seen.set()
                assert await asyncio.to_thread(go2.wait, 5.0)
                return await original(cast("Callable[..., Any]", fn), *args)
            return await original(cast("Callable[..., Any]", fn), *args)

        store._call_threaded = orchestrated  # type: ignore[assignment,method-assign]

        async def txn_body() -> None:
            async with store.transaction():
                await store.claim(record.id)
                raise RuntimeError("boom")

        task = asyncio.create_task(txn_body())
        assert await asyncio.to_thread(rollback_started.wait, 5.0)
        task.cancel()  # cancel while ROLLBACK is mid-flight
        task.cancel()  # ...and again: boundaries must complete anyway
        with pytest.raises((RuntimeError, asyncio.CancelledError)):
            await task

        async def txn2_body() -> None:
            async with store.transaction():
                await store.claim(record.id)

        task2 = asyncio.create_task(txn2_body())
        assert await asyncio.to_thread(begin2_seen.wait, 5.0)
        assert not rollback_done.is_set()  # BEGIN 2 raced the rollback
        go2.set()
        release.set()
        assert await asyncio.to_thread(rollback_done.wait, 5.0)
        await task2  # BEGIN 2 must succeed despite the race
        assert (await store.get_instance(record.id)).status is TaskInstanceState.RUNNING
    finally:
        await store.close()


# BUG-006 (external consolidated audit): cancellation must never break the
# single-connection serialization — matrix completion for the plain-write
# and close paths (transaction-body and BEGIN already covered above).


async def test_cancelled_plain_write_serializes_against_next_op(tmp_path: Path) -> None:
    """Cancel an ordinary (non-transactional) write mid-op; the NEXT
    operation must not enter the connection until the abandoned write
    finishes (the asyncio lock releasing early is not a license to
    overlap the still-running thread call)."""
    store = await open_store(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "overlap_probe")
        entered = threading.Event()
        release = threading.Event()
        first_done = threading.Event()
        second_entered = threading.Event()
        overlapped = threading.Event()
        original = store._call_threaded  # noqa: SLF001
        real_update = store._update_sync  # noqa: SLF001
        real_fetch = store._fetchone_sync  # noqa: SLF001

        def slow_update(sql: str, params: object) -> object:
            if str(sql).startswith("UPDATE task_instances SET status = 'running'"):
                entered.set()
                assert release.wait(5.0)
                result = real_update(sql, params)  # type: ignore[arg-type]
                first_done.set()
                return result
            return real_update(sql, params)  # type: ignore[arg-type]

        def probing_fetch(sql: str, params: object) -> object:
            if str(sql).startswith("SELECT"):
                second_entered.set()
                if not first_done.is_set():
                    overlapped.set()  # entered while the abandoned op ran
            return real_fetch(sql, params)  # type: ignore[arg-type]

        async def routed(fn: object, *args: object) -> object:
            from collections.abc import Callable
            from typing import Any, cast

            if args and str(args[0]).startswith("UPDATE"):
                return await original(slow_update, *args)
            if args and str(args[0]).startswith("SELECT"):
                return await original(probing_fetch, *args)
            return await original(cast("Callable[..., Any]", fn), *args)

        store._call_threaded = routed  # type: ignore[assignment,method-assign]
        store._update_sync = slow_update  # type: ignore[assignment,method-assign]
        store._fetchone_sync = probing_fetch  # type: ignore[assignment,method-assign]

        cancelled_write = asyncio.create_task(store.claim(record.id))
        assert await asyncio.to_thread(entered.wait, 5.0)
        cancelled_write.cancel()
        with pytest.raises(asyncio.CancelledError):
            await cancelled_write  # asyncio lock released; thread still busy

        reader = asyncio.create_task(store.get_instance(record.id))
        await asyncio.sleep(0.1)  # ample time to (wrongly) enter early
        assert not second_entered.is_set(), "next op overlapped the abandoned write"
        release.set()
        record_after = await reader
        assert await asyncio.to_thread(first_done.wait, 5.0)
        assert not overlapped.is_set()
        # The abandoned claim completed in the background (autocommit).
        assert record_after.status is TaskInstanceState.RUNNING
        assert record_after.attempt_count == 1
    finally:
        await store.close()


async def test_close_waits_for_in_flight_operation(tmp_path: Path) -> None:
    """close() must not close the connection underneath an in-flight
    call: it waits for the abandoned op to drain, which then completes
    normally instead of dying on a closed connection."""
    store = await open_store(tmp_path)
    record = await store.create_instance(ACCOUNT, "close_probe")
    entered = threading.Event()
    release = threading.Event()
    op_done = threading.Event()
    original = store._call_threaded  # noqa: SLF001

    async def routed(fn: object, *args: object) -> object:
        from collections.abc import Callable
        from typing import Any, cast

        if args and str(args[0]).startswith("UPDATE task_instances SET status = 'running'"):

            def slow() -> object:
                entered.set()
                assert release.wait(5.0)
                result = cast("Callable[..., Any]", fn)(*args)
                op_done.set()
                return result

            return await original(slow)
        return await original(cast("Callable[..., Any]", fn), *args)

    store._call_threaded = routed  # type: ignore[assignment,method-assign]

    cancelled_write = asyncio.create_task(store.claim(record.id))
    assert await asyncio.to_thread(entered.wait, 5.0)
    cancelled_write.cancel()
    with pytest.raises(asyncio.CancelledError):
        await cancelled_write

    closer = asyncio.create_task(store.close())
    await asyncio.sleep(0.1)
    assert not closer.done(), "close() completed while an op was still in flight"
    release.set()
    await closer
    # The abandoned op finished cleanly (no closed-underneath failure).
    assert await asyncio.to_thread(op_done.wait, 5.0)
    await store.close()  # idempotent


# ChatGPT PR #6 review: cancellation during the BEGIN await itself must
# not leak an open transaction (the abandoned BEGIN still executes).


async def test_cancellation_during_begin_leaves_no_open_transaction(tmp_path: Path) -> None:
    """Cancel the task while its BEGIN IMMEDIATE is mid-op.

    The abandoned BEGIN still executes in the worker thread. Without a
    drain-and-rollback in the cancellation path, the connection is left
    inside an open transaction and the NEXT transaction's BEGIN fails
    with "cannot start a transaction within a transaction"."""
    store = await open_store(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "begin_probe")
        began = threading.Event()
        release = threading.Event()
        begin_done = threading.Event()
        original = store._call_threaded  # noqa: SLF001

        async def orchestrated(fn: object, *args: object) -> object:
            from collections.abc import Callable
            from typing import Any, cast

            if args and str(args[0]).startswith("BEGIN IMMEDIATE"):

                def blocked_begin() -> object:
                    began.set()
                    assert release.wait(5.0)
                    result = cast("Callable[..., Any]", fn)(*args)
                    begin_done.set()  # the abandoned BEGIN HAS executed
                    return result

                return await original(blocked_begin)
            return await original(cast("Callable[..., Any]", fn), *args)

        store._call_threaded = orchestrated  # type: ignore[assignment,method-assign]

        async def txn_body() -> None:
            async with store.transaction():
                await store.claim(record.id)

        task = asyncio.create_task(txn_body())
        assert await asyncio.to_thread(began.wait, 5.0)
        task.cancel()  # cancellation lands on the BEGIN await
        release.set()  # the abandoned BEGIN now executes for real
        with pytest.raises(asyncio.CancelledError):
            await task
        assert await asyncio.to_thread(begin_done.wait, 5.0)

        # No leaked transaction: a FRESH transaction must BEGIN cleanly
        # (and its write must actually commit — not join a zombie txn).
        async with store.transaction():
            await store.claim(record.id)
        row = await store.get_instance(record.id)
        assert row.status is TaskInstanceState.RUNNING
        assert row.attempt_count == 1
    finally:
        await store.close()


# ChatGPT PR #5 re-review VERIFY-001: the cleanup ROLLBACK can acquire the
# connection guard BEFORE the abandoned BEGIN thread (threading.RLock
# gives no FIFO guarantee) — the rollback then no-ops, the BEGIN lands
# afterwards, and an open transaction leaks DESPITE the cleanup.


class _ReorderableGuard:
    """Drop-in RLock stand-in that parks the FIRST acquirer until allowed,
    forcing the exact non-FIFO ordering VERIFY-001 describes."""

    def __init__(self) -> None:
        self._inner = threading.RLock()
        self._mu = threading.Lock()
        self.acquires = 0
        self.first_parked = threading.Event()
        self.allow_first = threading.Event()
        self.second_done = threading.Event()

    def acquire(self, blocking: bool = True, timeout: float = -1) -> bool:
        with self._mu:
            self.acquires += 1
            n = self.acquires
        if n == 1:
            self.first_parked.set()
            assert self.allow_first.wait(5.0)  # BEGIN thread parked pre-guard
        return self._inner.acquire()

    def release(self) -> None:
        self._inner.release()
        if self.acquires >= 2:
            self.second_done.set()  # the LATE acquirer finished its section

    def __enter__(self) -> "_ReorderableGuard":
        self.acquire()
        return self

    def __exit__(self, *_exc: object) -> None:
        self.release()


async def test_guard_reorder_rollback_before_begin_still_cleans_up(tmp_path: Path) -> None:
    """Deterministic VERIFY-001 interleaving: the cleanup ROLLBACK reaches
    the connection BEFORE the abandoned BEGIN does. The store must still
    end with no open transaction."""
    store = await open_store(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "reorder_probe")
        guard = _ReorderableGuard()
        store._conn_guard = guard  # type: ignore[assignment]

        async def txn_body() -> None:
            async with store.transaction():
                await store.claim(record.id)

        task = asyncio.create_task(txn_body())
        assert await asyncio.to_thread(guard.first_parked.wait, 5.0)
        task.cancel()  # lands on the BEGIN await; BEGIN thread parked
        # Buggy path: the cleanup ROLLBACK acquires the guard immediately
        # (second acquirer) and finishes while the BEGIN is still parked.
        # Fixed path: the cleanup drains the BEGIN first, so no second
        # acquirer can complete before allow_first.
        await asyncio.to_thread(guard.second_done.wait, 0.5)
        guard.allow_first.set()  # the abandoned BEGIN executes NOW
        with pytest.raises(asyncio.CancelledError):
            await task

        # No leaked transaction despite the adversarial guard order.
        async with store.transaction():
            await store.claim(record.id)
        row = await store.get_instance(record.id)
        assert row.status is TaskInstanceState.RUNNING
        assert row.attempt_count == 1
    finally:
        await store.close()


# --- PR #8 audit BUG-001: repeated cancellation vs the shielded drain ---


async def test_double_cancel_preserves_innocent_acknowledged_write(
    tmp_path: Path,
) -> None:
    """BUG-001 (PR #8 audit): a SECOND cancellation of a transaction-
    owning task breaks the shielded drain — the store lock is released
    while the failed transaction is still open, an innocent concurrent
    write's statements silently JOIN the zombie transaction, and the
    delayed ROLLBACK destroys the already-acknowledged write. Cleanup
    must survive repeated cancellation."""
    store = await open_store(tmp_path)
    try:
        record = await store.create_instance(ACCOUNT, "probe")

        # A tracked in-flight call that stalls the drain deterministically
        # (as any long guarded call would: integrity_check, VACUUM INTO).
        release = asyncio.Event()

        async def staller() -> None:
            await release.wait()

        fake = asyncio.create_task(staller())
        store._in_flight.add(fake)  # noqa: SLF001 — the widened window
        fake.add_done_callback(store._in_flight.discard)  # noqa: SLF001

        async def txn_body() -> None:
            async with store.transaction():
                await store.claim(record.id)
                await asyncio.sleep(30)  # park; the first cancel lands here

        task_a = asyncio.create_task(txn_body())
        await asyncio.sleep(0.3)  # BEGIN + claim landed
        task_a.cancel()  # 1st cancel: the drain starts, parks on the staller
        await asyncio.sleep(0.15)
        task_a.cancel()  # 2nd cancel: breaks the one-shot shield today
        with contextlib.suppress(asyncio.CancelledError):
            await task_a

        # The innocent bystander: the lock is free, so its acknowledged
        # autocommit write proceeds — and must NOT join the zombie txn.
        innocent = await store.create_instance(ACCOUNT, "innocent")
        release.set()  # the drain proceeds; the ROLLBACK lands
        await asyncio.sleep(0.3)

        probe = await store.get_instance(innocent.id)
        assert probe is not None and probe.task_id == "innocent", (
            f"acknowledged write destroyed by the delayed rollback: {probe}"
        )
        assert probe.status is not None
    finally:
        await store.close()
