"""Credential backup and restore (roadmap item 4 — P0).

Export is ciphertext-only; import validates every record by decrypting it
BEFORE writing anything (a non-matching bundle is rejected whole); restored
rows are re-encrypted under the current primary. Restoration is tested in a
separate environment (fresh database + store) as the acceptance criteria
require.
"""

import asyncio
import json
import sqlite3
import threading
from pathlib import Path

import pytest
from cryptography.fernet import MultiFernet

from autoclaimer.credential_store import (
    PRIMARY_KEY_ENV,
    CredentialDecryptError,
    SQLiteCredentialStore,
    generate_fernet_key,
    load_fernet_keys,
)
from autoclaimer.credentials import CredentialProviderError

ACCOUNT = 271485504361996288
OTHER = 380028190142042112
TOKEN_A = "gho_backupTokenAccountA_111222333444555"
TOKEN_B = "gho_backupTokenAccountB_666777888999000"


def _store(path: Path, keys: object) -> SQLiteCredentialStore:
    return SQLiteCredentialStore(path, keys)  # type: ignore[arg-type]


async def _open(path: Path, env: dict[str, str]) -> SQLiteCredentialStore:
    store = _store(path, load_fernet_keys(env))
    await store.open()
    return store


async def test_export_is_ciphertext_only(tmp_path: Path) -> None:
    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    store = await _open(tmp_path / "state.db", env)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        await store.set_token(OTHER, TOKEN_B)
        out = tmp_path / "backup.json"
        count = await store.export_ciphertext(out)
        assert count == 2
        raw = out.read_bytes()
        assert TOKEN_A.encode() not in raw  # never plaintext
        assert TOKEN_B.encode() not in raw
        payload = json.loads(out.read_text())
        assert payload["version"] == 2
        assert len(payload["accounts"]) == 2
        for record in payload["accounts"]:
            assert set(record) == {
                "account_id",
                "encrypted_token",
                "key_version",
                "created_at",
                "updated_at",
            }
    finally:
        store.close()


async def test_restore_in_a_separate_environment(tmp_path: Path) -> None:
    """The acceptance-criteria scenario: export from one database,
    restore into a FRESH environment with the same key material."""
    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    source = await _open(tmp_path / "source.db", env)
    try:
        await source.set_token(ACCOUNT, TOKEN_A)
        await source.set_token(OTHER, TOKEN_B)
        bundle = tmp_path / "backup.json"
        await source.export_ciphertext(bundle)
    finally:
        source.close()

    target = await _open(tmp_path / "target.db", env)  # separate environment
    try:
        imported, skipped = await target.import_ciphertext(bundle)
        assert (imported, skipped) == (2, 0)
        assert target.get_token(ACCOUNT) == TOKEN_A
        assert target.get_token(OTHER) == TOKEN_B  # account isolation kept
        # Idempotent re-import: everything already current.
        imported2, skipped2 = await target.import_ciphertext(bundle)
        assert (imported2, skipped2) == (0, 2)
    finally:
        target.close()


async def test_import_with_wrong_key_is_rejected_whole(tmp_path: Path) -> None:
    export_key = generate_fernet_key()
    source = await _open(tmp_path / "source.db", {PRIMARY_KEY_ENV: export_key})
    try:
        await source.set_token(ACCOUNT, TOKEN_A)
        bundle = tmp_path / "backup.json"
        await source.export_ciphertext(bundle)
    finally:
        source.close()

    wrong = await _open(tmp_path / "target.db", {PRIMARY_KEY_ENV: generate_fernet_key()})
    try:
        with pytest.raises(CredentialDecryptError, match="configured keys"):
            await wrong.import_ciphertext(bundle)
        # Nothing was written: no partial import.
        conn = sqlite3.connect(tmp_path / "target.db")
        try:
            assert conn.execute("SELECT COUNT(*) FROM account_credentials").fetchone()[0] == 0
        finally:
            conn.close()
    finally:
        wrong.close()


async def test_poisoned_bundle_is_rejected_without_partial_import(tmp_path: Path) -> None:
    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    store = await _open(tmp_path / "state.db", env)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        bundle = tmp_path / "backup.json"
        await store.export_ciphertext(bundle)
        # Poison the SECOND record after export: phase-1 validation must
        # reject the whole bundle (nothing written, including record 1).
        payload = json.loads(bundle.read_text())
        payload["accounts"].append(
            {
                "account_id": OTHER,
                "encrypted_token": "!!!not-base64!!!",
                "key_version": 1,
                "created_at": "x",
                "updated_at": "x",
            }
        )
        bundle.write_text(json.dumps(payload))
        with pytest.raises(CredentialProviderError, match="malformed"):
            await store.import_ciphertext(bundle)
        assert sorted(await store.account_ids()) == [ACCOUNT]  # untouched
    finally:
        store.close()


async def test_restore_reencrypts_under_current_primary(tmp_path: Path) -> None:
    """Import into a NEW primary (+ old retired) re-encrypts, so the
    restored store works after the old key is discarded."""
    old_key = generate_fernet_key()
    source = await _open(tmp_path / "source.db", {PRIMARY_KEY_ENV: old_key})
    try:
        await source.set_token(ACCOUNT, TOKEN_A)
        bundle = tmp_path / "backup.json"
        await source.export_ciphertext(bundle)
    finally:
        source.close()

    new_key = generate_fernet_key()
    rotated = await _open(
        tmp_path / "target.db",
        {PRIMARY_KEY_ENV: new_key, "AUTOCLAIMER_FERNET_RETIRED_KEYS": old_key},
    )
    try:
        imported, _ = await rotated.import_ciphertext(bundle)
        assert imported == 1
    finally:
        rotated.close()

    # Old key fully discarded: the restored credential still decrypts.
    only_new = await _open(tmp_path / "verify.db", {PRIMARY_KEY_ENV: new_key})
    try:
        conn = sqlite3.connect(tmp_path / "target.db")
        with conn:
            conn.execute("ATTACH DATABASE ? AS t", (str(tmp_path / "target.db"),))
            row = conn.execute(
                "SELECT encrypted_token, key_version FROM t.account_credentials "
                "WHERE account_id = ?",
                (ACCOUNT,),
            ).fetchone()
        conn.close()
        blob, version = bytes(row[0]), row[1]
        assert version == 2  # primary generation after rotation-style import
        assert only_new._keys.primary.decrypt(blob).decode() == TOKEN_A  # noqa: SLF001
    finally:
        only_new.close()


# --- CLI surface: explicit confirmation required --------------------------


async def test_cli_export_requires_confirm(tmp_path: Path) -> None:
    from autoclaimer.cli import AutoClaimerController, CliSession
    from autoclaimer.config import parse_accounts_config, parse_app_config
    from autoclaimer.mock_adapter import MockAdapter
    from autoclaimer.ratelimit import RateLimiter
    from autoclaimer.registry import TaskRegistry

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": 111111111111111111},
            "tasks": {},
        }
    )
    accounts = parse_accounts_config({"version": 1, "accounts": []}, app)
    from autoclaimer.persistence import TaskStateStore

    creds = await _open(tmp_path / "creds.db", {PRIMARY_KEY_ENV: generate_fernet_key()})
    task_store = TaskStateStore(tmp_path / "state.db")
    await task_store.open()
    try:
        controller = AutoClaimerController(
            app_config=app,
            accounts=accounts,
            store=task_store,
            limiter=RateLimiter(store=task_store),
            registry=TaskRegistry(),
            adapter_factory=lambda account_id: MockAdapter(),
            credential_store=creds,
        )
        output: list[str] = []
        session = CliSession(controller, read_line=None, write=output.append)  # type: ignore[arg-type]

        out_file = tmp_path / "backup.json"
        # Without the literal confirm token: refused, nothing written.
        assert await session.run_once(f"credential export {out_file}") is True
        assert any("confirm" in line for line in output)
        assert not out_file.exists()
        output.clear()
        # With confirmation: exported, and the notice mentions ciphertext.
        assert await session.run_once(f"credential export {out_file} confirm") is True
        assert out_file.exists()
        assert any("CIPHERTEXT" in line for line in output)
        assert any("no plaintext" in line for line in output)
    finally:
        creds.close()
        await task_store.close()


# ChatGPT PR #8 audit: BUG-014/012/013/015 + VERIFY-002


async def test_cli_export_bad_path_survives(tmp_path: Path) -> None:
    """BUG-014: an invalid export destination is a typed CLI error —
    the session stays alive and the controller is NOT shut down."""
    from autoclaimer.cli import AutoClaimerController, CliSession
    from autoclaimer.config import parse_accounts_config, parse_app_config
    from autoclaimer.mock_adapter import MockAdapter
    from autoclaimer.persistence import TaskStateStore
    from autoclaimer.ratelimit import RateLimiter
    from autoclaimer.registry import TaskRegistry

    app = parse_app_config({"version": 1, "channels": {"roll_claim": 1}, "tasks": {}})
    accounts = parse_accounts_config({"version": 1, "accounts": []}, app)
    creds = await _open(tmp_path / "creds.db", {PRIMARY_KEY_ENV: generate_fernet_key()})
    task_store = TaskStateStore(tmp_path / "state.db")
    await task_store.open()
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=task_store,
        limiter=RateLimiter(store=task_store),
        registry=TaskRegistry(),
        adapter_factory=lambda account_id: MockAdapter(),
        credential_store=creds,
    )
    output: list[str] = []
    session = CliSession(controller, read_line=None, write=output.append)  # type: ignore[arg-type]
    try:
        bad = tmp_path / "missing-parent" / "backup.json"
        assert await session.run_once(f"credential export {bad} confirm") is True
        assert any("error" in line.lower() for line in output)
        assert controller.closed is False  # runtime NOT torn down
    finally:
        creds.close()
        await task_store.close()


async def test_import_does_not_block_the_event_loop(tmp_path: Path) -> None:
    """BUG-012: while another connection holds the database write lock,
    a heartbeat coroutine keeps progressing during an import — the
    upserts run off the event loop."""
    import threading

    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    source = await _open(tmp_path / "source.db", env)
    try:
        await source.set_token(ACCOUNT, TOKEN_A)
        bundle = tmp_path / "b.json"
        await source.export_ciphertext(bundle)
    finally:
        source.close()

    target = await _open(tmp_path / "target.db", env)
    try:
        hold = threading.Event()
        held = threading.Event()
        other = sqlite3.connect(
            tmp_path / "target.db", isolation_level=None, check_same_thread=False
        )
        other.execute("BEGIN IMMEDIATE")

        def release_later() -> None:
            held.set()
            import time

            time.sleep(0.3)  # hold the write lock while the import runs
            other.execute("ROLLBACK")
            other.close()

        import threading as _t

        _t.Thread(target=release_later, daemon=True).start()

        heartbeats = [0]

        async def heartbeat() -> None:
            while not hold.is_set():
                heartbeats[0] += 1
                await asyncio.sleep(0.01)

        hb = asyncio.create_task(heartbeat())
        await asyncio.to_thread(held.wait, 2.0)  # lock is held now
        await asyncio.sleep(0.05)
        before = heartbeats[0]
        imported, _ = await target.import_ciphertext(bundle)
        hold.set()
        await hb
        assert imported == 1
        assert heartbeats[0] > before, "event loop froze during import"
    finally:
        target.close()


async def test_import_rejects_tampered_account_id(tmp_path: Path) -> None:
    """BUG-013: the account mapping is authenticated; editing the JSON
    account_id while leaving the ciphertext intact is rejected whole."""
    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    source = await _open(tmp_path / "source.db", env)
    try:
        await source.set_token(ACCOUNT, TOKEN_A)
        bundle = tmp_path / "b.json"
        await source.export_ciphertext(bundle)
    finally:
        source.close()

    payload = json.loads(bundle.read_text())
    payload["accounts"][0]["account_id"] = OTHER  # tamper: remap A -> B
    bundle.write_text(json.dumps(payload))

    target = await _open(tmp_path / "target.db", env)
    try:
        with pytest.raises(CredentialProviderError, match="account"):
            await target.import_ciphertext(bundle)
        assert not await target.has_token(OTHER)
        assert not await target.has_token(ACCOUNT)  # rejected whole
    finally:
        target.close()


async def test_import_rejects_duplicate_account_ids(tmp_path: Path) -> None:
    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    source = await _open(tmp_path / "source.db", env)
    try:
        await source.set_token(ACCOUNT, TOKEN_A)
        bundle = tmp_path / "b.json"
        await source.export_ciphertext(bundle)
    finally:
        source.close()

    payload = json.loads(bundle.read_text())
    payload["accounts"].append(dict(payload["accounts"][0]))  # duplicate
    bundle.write_text(json.dumps(payload))

    target = await _open(tmp_path / "target.db", env)
    try:
        with pytest.raises(CredentialProviderError, match="duplicate"):
            await target.import_ciphertext(bundle)
    finally:
        target.close()


async def test_export_malformed_stored_ciphertext_is_typed(tmp_path: Path) -> None:
    """BUG-015: a TEXT value in encrypted_token during export raises the
    typed error instead of a raw TypeError."""
    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    store = await _open(tmp_path / "state.db", env)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        conn = sqlite3.connect(tmp_path / "state.db")
        with conn:
            conn.execute(
                "UPDATE account_credentials SET encrypted_token = ? WHERE account_id = ?",
                ("not-binary", ACCOUNT),
            )
        conn.close()
        from autoclaimer.credentials import CredentialProviderError as _CPE

        with pytest.raises(_CPE, match="malformed"):
            await store.export_ciphertext(tmp_path / "b.json")
    finally:
        store.close()


async def test_import_replaces_malformed_stored_row(tmp_path: Path) -> None:
    """BUG-015 + documented policy: importing over a corrupt stored row
    replaces it (the restore is the repair path)."""
    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    source = await _open(tmp_path / "source.db", env)
    try:
        await source.set_token(ACCOUNT, TOKEN_A)
        bundle = tmp_path / "b.json"
        await source.export_ciphertext(bundle)
    finally:
        source.close()

    target = await _open(tmp_path / "target.db", env)
    try:
        await target.set_token(ACCOUNT, TOKEN_B)
        conn = sqlite3.connect(tmp_path / "target.db")
        with conn:
            conn.execute(
                "UPDATE account_credentials SET encrypted_token = ? WHERE account_id = ?",
                ("not-binary", ACCOUNT),
            )
        conn.close()
        imported, _skipped = await target.import_ciphertext(bundle)
        assert imported == 1  # replaced, not crashed
        assert target.get_token(ACCOUNT) == TOKEN_A
    finally:
        target.close()


class _BlockFirstEncrypt:
    """MultiFernet proxy blocking the first encrypt() call — under the
    VERIFY-002 fix that call happens INSIDE the upsert's write
    transaction, giving a deterministic in-transaction pause."""

    def __init__(
        self,
        inner: MultiFernet,
        entered: threading.Event,
        release: threading.Event,
    ) -> None:
        self.inner = inner
        self.entered = entered
        self.release = release
        self.fired = False

    def encrypt(self, token: bytes) -> bytes:
        if not self.fired:
            self.fired = True
            self.entered.set()
            assert self.release.wait(5.0)
        result: bytes = self.inner.encrypt(token)
        return result

    def decrypt(self, token: bytes) -> bytes:
        result: bytes = self.inner.decrypt(token)
        return result


async def test_import_concurrent_update_is_not_lost(tmp_path: Path) -> None:
    """VERIFY-002: the upsert's read+write share ONE transaction. A
    concurrent set_token either completes before it (import, as the
    authoritative restore, overwrites) or waits and lands after — the
    import never silently discards a committed update mid-transaction."""
    from typing import cast

    from autoclaimer.credential_store import FernetKeys as _FK

    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    source = await _open(tmp_path / "source.db", env)
    try:
        await source.set_token(ACCOUNT, TOKEN_A)
        bundle = tmp_path / "b.json"
        await source.export_ciphertext(bundle)
    finally:
        source.close()

    target = await _open(tmp_path / "target.db", env)
    writer = await _open(tmp_path / "target.db", env)  # separate connection
    try:
        entered = threading.Event()
        release = threading.Event()
        real_keys = target._keys  # noqa: SLF001
        blocking = _BlockFirstEncrypt(real_keys.multi, entered, release)

        target._keys = _FK(  # noqa: SLF001
            multi=cast(MultiFernet, blocking),
            primary=cast(MultiFernet, blocking),
            version=real_keys.version,
        )

        import_task = asyncio.create_task(target.import_ciphertext(bundle))
        assert await asyncio.to_thread(entered.wait, 5.0)  # inside the txn
        set_task = asyncio.create_task(writer.set_token(ACCOUNT, TOKEN_B))
        await asyncio.sleep(0.2)  # set_token parks on the write lock
        release.set()
        await import_task
        await set_task  # serialized after the import's commit
        assert target.get_token(ACCOUNT) == TOKEN_B  # committed update kept
    finally:
        target.close()
        writer.close()


async def test_export_cleanup_failure_is_contained(tmp_path: Path) -> None:
    """BUG-014 edge (re-review): when even the temp-file CLEANUP fails
    (inaccessible directory), the typed export error must still be the
    one that reaches the caller — never the raw PermissionError."""
    import os
    import sys

    if sys.platform != "linux" or os.geteuid() == 0:  # pragma: no cover
        pytest.skip("requires a non-root POSIX environment")

    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    store = await _open(tmp_path / "state.db", env)
    locked = tmp_path / "locked"
    locked.mkdir()
    locked.chmod(0o000)
    try:
        from autoclaimer.credentials import CredentialProviderError as _CPE

        with pytest.raises(_CPE, match="credential export failed"):
            await store.export_ciphertext(locked / "b.json")
    finally:
        locked.chmod(0o700)
        store.close()


# PR #8 remediation round: BUG-016 (uncontained decrypt/decode in export),
# VERIFY-003 (export connection guard), VERIFY-004 (mudae_state orphans)


def _write_raw_ciphertext(db: Path, account_id: int, blob: bytes) -> None:
    conn = sqlite3.connect(db)
    with conn:
        conn.execute(
            "INSERT INTO account_credentials "
            "(account_id, encrypted_token, key_version, created_at, updated_at) "
            "VALUES (?, ?, 1, '2026-01-01T00:00:00+00:00', '2026-01-01T00:00:00+00:00') "
            "ON CONFLICT(account_id) DO UPDATE SET encrypted_token = excluded.encrypted_token",
            (account_id, blob),
        )
    conn.close()


async def test_export_invalid_fernet_ciphertext_is_typed(tmp_path: Path) -> None:
    """BUG-016 (a): a bytes row that is not a valid Fernet envelope
    (e.g. encrypted under an unloaded key) raises the TYPED error naming
    the account — never a raw InvalidToken."""
    from autoclaimer.credentials import CredentialProviderError as _CPE

    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    store = await _open(tmp_path / "state.db", env)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        _write_raw_ciphertext(tmp_path / "state.db", ACCOUNT, b"garbage-not-fernet")
        with pytest.raises(_CPE, match=str(ACCOUNT)) as excinfo:
            await store.export_ciphertext(tmp_path / "b.json")
        assert "garbage-not-fernet" not in str(excinfo.value)  # no ciphertext echo
    finally:
        store.close()


async def test_export_invalid_utf8_payload_is_typed(tmp_path: Path) -> None:
    """BUG-016 (b): a VALID envelope decrypting to non-UTF-8 bytes raises
    the typed error — never a raw UnicodeDecodeError."""
    from cryptography.fernet import Fernet

    from autoclaimer.credentials import CredentialProviderError as _CPE

    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    store = await _open(tmp_path / "state.db", env)
    try:
        # Encrypt non-UTF-8 bytes directly into the stored row.
        key = Fernet(env[PRIMARY_KEY_ENV])
        _write_raw_ciphertext(tmp_path / "state.db", ACCOUNT, key.encrypt(b"\xff\xfe\x00not-utf8"))
        with pytest.raises(_CPE, match=str(ACCOUNT)):
            await store.export_ciphertext(tmp_path / "b.json")
    finally:
        store.close()


async def test_export_failure_leaves_existing_backup_intact(tmp_path: Path) -> None:
    """A failing export must never damage a previously written bundle:
    the failure happens in the read phase, before any file write."""
    from autoclaimer.credentials import CredentialProviderError as _CPE

    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    store = await _open(tmp_path / "state.db", env)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        target = tmp_path / "b.json"
        await store.export_ciphertext(target)  # good bundle
        good = target.read_text()
        _write_raw_ciphertext(tmp_path / "state.db", ACCOUNT, b"garbage-not-fernet")
        with pytest.raises(_CPE):
            await store.export_ciphertext(target)  # fails in _dump
        assert target.read_text() == good  # untouched
    finally:
        store.close()


async def test_cli_export_corrupt_row_survives(tmp_path: Path) -> None:
    """BUG-016 (c): the CLI reports the typed error, the controller stays
    open, and no ciphertext appears in the output."""
    from autoclaimer.cli import AutoClaimerController, CliSession
    from autoclaimer.config import parse_accounts_config, parse_app_config
    from autoclaimer.mock_adapter import MockAdapter
    from autoclaimer.persistence import TaskStateStore
    from autoclaimer.ratelimit import RateLimiter
    from autoclaimer.registry import TaskRegistry

    app = parse_app_config({"version": 1, "channels": {"roll_claim": 1}, "tasks": {}})
    accounts = parse_accounts_config({"version": 1, "accounts": []}, app)
    creds = await _open(tmp_path / "creds.db", {PRIMARY_KEY_ENV: generate_fernet_key()})
    await creds.set_token(ACCOUNT, TOKEN_A)
    _write_raw_ciphertext(tmp_path / "creds.db", ACCOUNT, b"garbage-not-fernet")
    task_store = TaskStateStore(tmp_path / "state.db")
    await task_store.open()
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=task_store,
        limiter=RateLimiter(store=task_store),
        registry=TaskRegistry(),
        adapter_factory=lambda account_id: MockAdapter(),
        credential_store=creds,
    )
    output: list[str] = []
    session = CliSession(controller, read_line=None, write=output.append)  # type: ignore[arg-type]
    try:
        target = tmp_path / "b.json"
        assert await session.run_once(f"credential export {target} confirm") is True
        joined = "\n".join(output)
        assert "error" in joined.lower()
        assert "garbage-not-fernet" not in joined  # no ciphertext echo
        assert controller.closed is False
    finally:
        creds.close()
        await task_store.close()


class _BlockFirstDecrypt:
    """MultiFernet proxy blocking the first decrypt() — used to park the
    EXPORT thread mid-iteration with its SELECT cursor still open."""

    def __init__(
        self, inner: MultiFernet, entered: threading.Event, release: threading.Event
    ) -> None:
        self.inner = inner
        self.entered = entered
        self.release = release
        self.fired = False

    def encrypt(self, token: bytes) -> bytes:
        result: bytes = self.inner.encrypt(token)
        return result

    def decrypt(self, token: bytes) -> bytes:
        if not self.fired:
            self.fired = True
            self.entered.set()
            assert self.release.wait(5.0)
        result: bytes = self.inner.decrypt(token)
        return result


async def test_export_holds_connection_guard_against_writers(tmp_path: Path) -> None:
    """VERIFY-003: with the export parked mid-iteration, a concurrent
    set_token on the same store must WAIT (guard held) rather than commit
    underneath the open SELECT cursor — proving the export serializes
    with every other credential operation on the connection. The bundle
    is a consistent snapshot (pre-write state), and the concurrent write
    lands afterwards."""
    from typing import cast

    from autoclaimer.credential_store import FernetKeys as _FK

    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    store = await _open(tmp_path / "state.db", env)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        entered = threading.Event()
        release = threading.Event()
        real_keys = store._keys  # noqa: SLF001
        blocking = _BlockFirstDecrypt(real_keys.multi, entered, release)
        store._keys = _FK(  # noqa: SLF001
            multi=cast(MultiFernet, blocking),
            primary=cast(MultiFernet, blocking),
            version=real_keys.version,
        )

        await store.set_token(OTHER, TOKEN_A)  # second row, pre-write state
        export_task = asyncio.create_task(store.export_ciphertext(tmp_path / "b.json"))
        assert await asyncio.to_thread(entered.wait, 5.0)  # parked mid-iteration
        set_task = asyncio.create_task(store.set_token(OTHER, TOKEN_B))
        await asyncio.sleep(0.2)  # give set_token its chance to misbehave
        release.set()
        await export_task
        await set_task  # must complete WITHOUT exception (waits on the guard)
        assert store.get_token(OTHER) == TOKEN_B  # the write landed
        # The bundle is a CONSISTENT SNAPSHOT: OTHER captured at its
        # pre-write value (TOKEN_A), not a torn mid-write state.
        restored = await _open(tmp_path / "snapshot.db", env)
        try:
            await restored.import_ciphertext(tmp_path / "b.json")
            assert restored.get_token(OTHER) == TOKEN_A
            assert restored.get_token(ACCOUNT) == TOKEN_A
        finally:
            restored.close()
    finally:
        store.close()
