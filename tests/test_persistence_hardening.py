"""Batch 3 regression tests (Phase 9 persistence hardening)."""

import asyncio
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
