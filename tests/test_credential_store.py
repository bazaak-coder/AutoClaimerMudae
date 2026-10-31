"""Encrypted credential storage: real Fernet + real SQLite persistence.

Covers round-trips, account isolation, plaintext/key absence from the
database, fail-closed key handling, wrong-key/corruption safety, key
rotation (including interrupted-and-resumed rotation), reopen
persistence, and adapter integration through the provider boundary.
"""

import asyncio
import logging
import sqlite3
import threading
from pathlib import Path
from typing import cast

import pytest
from cryptography.fernet import Fernet, MultiFernet

from autoclaimer.credential_store import (
    PRIMARY_KEY_ENV,
    RETIRED_KEYS_ENV,
    CredentialDecryptError,
    CredentialKeyError,
    CredentialNotFoundError,
    FernetKeys,
    SQLiteCredentialStore,
    generate_fernet_key,
    load_fernet_keys,
)
from autoclaimer.credentials import CredentialProvider, CredentialProviderError
from autoclaimer.persistence import PersistenceError, TaskStateStore

ACCOUNT = 271485504361996288
OTHER = 380028190142042112
TOKEN_A = "gho_AAAAtokenForAccountA_0123456789abcdef"
TOKEN_B = "gho_BBBBdifferentForB_fedcba9876543210"


def make_store(tmp_path: Path, environ: dict[str, str] | None = None) -> SQLiteCredentialStore:
    env = environ if environ is not None else {PRIMARY_KEY_ENV: generate_fernet_key()}
    return SQLiteCredentialStore(tmp_path / "state.db", load_fernet_keys(env))


async def opened(tmp_path: Path, environ: dict[str, str] | None = None) -> SQLiteCredentialStore:
    store = make_store(tmp_path, environ)
    await store.open()
    return store


def raw_rows(path: Path) -> list[tuple[int, bytes, int]]:
    conn = sqlite3.connect(path)
    try:
        return list(
            conn.execute("SELECT account_id, encrypted_token, key_version FROM account_credentials")
        )
    finally:
        conn.close()


# ----- key loading (fail closed) -----


def test_missing_key_fails_closed() -> None:
    with pytest.raises(CredentialKeyError, match="fail closed"):
        load_fernet_keys({})


def test_invalid_key_fails_without_echoing_material() -> None:
    with pytest.raises(CredentialKeyError) as excinfo:
        load_fernet_keys({PRIMARY_KEY_ENV: "not-a-valid-fernet-key!!"})
    assert "not-a-valid-fernet-key" not in str(excinfo.value)


def test_retired_keys_load_and_primary_encrypts() -> None:
    env = {
        PRIMARY_KEY_ENV: generate_fernet_key(),
        RETIRED_KEYS_ENV: f"{generate_fernet_key()}, {generate_fernet_key()}",
    }
    keys = load_fernet_keys(env)
    assert keys.version == 3  # 1 primary + 2 retired
    blob = keys.multi.encrypt(b"x")
    assert keys.multi.decrypt(blob) == b"x"  # primary used for new writes


def test_generate_key_never_touches_state() -> None:
    key = generate_fernet_key()
    Fernet(key)  # valid
    assert isinstance(key, str)


# ----- round trips (real encryption + real SQLite) -----


async def test_add_and_retrieve_round_trip(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        assert store.get_token(ACCOUNT) == TOKEN_A
    finally:
        store.close()


async def test_update_replaces_token(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        await store.set_token(ACCOUNT, TOKEN_B)
        assert store.get_token(ACCOUNT) == TOKEN_B
        rows = raw_rows(tmp_path / "state.db")
        assert len(rows) == 1  # upsert, not a second row
    finally:
        store.close()


async def test_delete_removes_token(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        assert await store.delete_token(ACCOUNT) is True
        assert await store.delete_token(ACCOUNT) is False  # idempotent
        with pytest.raises(CredentialNotFoundError):
            store.get_token(ACCOUNT)
    finally:
        store.close()


async def test_missing_account_credential_is_explicit(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        with pytest.raises(CredentialNotFoundError):
            store.get_token(ACCOUNT)
    finally:
        store.close()


async def test_multi_account_isolation(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        await store.set_token(OTHER, TOKEN_B)
        assert store.get_token(ACCOUNT) == TOKEN_A
        assert store.get_token(OTHER) == TOKEN_B
        assert store.get_token(ACCOUNT) != store.get_token(OTHER)
        assert sorted(await store.account_ids()) == [ACCOUNT, OTHER]
    finally:
        store.close()


async def test_invalid_account_ids_rejected(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        for bad in (0, -1, True, 1.5, "123"):
            with pytest.raises(ValueError):
                await store.set_token(bad, TOKEN_A)  # type: ignore[arg-type]
            with pytest.raises(ValueError):
                store.get_token(bad)  # type: ignore[arg-type]
    finally:
        store.close()


# ----- storage hygiene: no plaintext, no keys, in the DB -----


async def test_database_never_contains_plaintext_token(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
    finally:
        store.close()
    raw = (tmp_path / "state.db").read_bytes()
    assert TOKEN_A.encode() not in raw
    rows = raw_rows(tmp_path / "state.db")
    assert len(rows) == 1
    assert TOKEN_A.encode() not in bytes(rows[0][1])
    # ciphertext IS present (encrypted, not just absent)
    assert bytes(rows[0][1]) != TOKEN_A.encode()


async def test_fernet_key_not_in_database(tmp_path: Path) -> None:
    key = generate_fernet_key()
    store = await opened(tmp_path, {PRIMARY_KEY_ENV: key})
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
    finally:
        store.close()
    raw = (tmp_path / "state.db").read_bytes()
    assert key.encode() not in raw
    assert key.encode("ascii") not in raw


async def test_store_implements_provider_protocol(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        assert isinstance(store, CredentialProvider)
    finally:
        store.close()


# ----- wrong key / corruption safety -----


async def test_wrong_key_cannot_decrypt_and_message_is_safe(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
    finally:
        store.close()

    other = await opened(tmp_path, {PRIMARY_KEY_ENV: generate_fernet_key()})
    try:
        with pytest.raises(CredentialDecryptError) as excinfo:
            other.get_token(ACCOUNT)
        message = str(excinfo.value)
        assert TOKEN_A not in message
        # ciphertext fragments must not leak either
        rows = raw_rows(tmp_path / "state.db")
        assert bytes(rows[0][1][:24]).decode("latin-1") not in message
    finally:
        other.close()


async def test_corrupted_ciphertext_is_a_safe_explicit_error(tmp_path: Path) -> None:
    key = generate_fernet_key()
    store = await opened(tmp_path, {PRIMARY_KEY_ENV: key})
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
    finally:
        store.close()
    conn = sqlite3.connect(tmp_path / "state.db")
    with conn:
        conn.execute(
            "UPDATE account_credentials SET encrypted_token = ? WHERE account_id = ?",
            (b"garbage" * 8, ACCOUNT),
        )
    conn.close()

    # Reopen with the ORIGINAL key: the corrupted blob must fail safely.
    store3 = SQLiteCredentialStore(
        tmp_path / "state.db",
        FernetKeys(multi=MultiFernet([Fernet(key)]), primary=MultiFernet([Fernet(key)]), version=1),
    )
    await store3.open()
    try:
        with pytest.raises(CredentialDecryptError):
            store3.get_token(ACCOUNT)
    finally:
        store3.close()


async def test_non_blob_ciphertext_is_a_safe_error(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        conn = sqlite3.connect(tmp_path / "state.db")
        with conn:
            conn.execute(
                "INSERT INTO account_credentials VALUES (?, ?, ?, ?, ?)", (ACCOUNT, 42, 1, "x", "x")
            )
        conn.close()
        with pytest.raises(CredentialDecryptError):
            store.get_token(ACCOUNT)
    finally:
        store.close()


async def test_database_failure_does_not_expose_secrets(tmp_path: Path) -> None:
    store = await opened(tmp_path)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        original = store._connection  # noqa: SLF001

        def broken() -> sqlite3.Connection:
            raise sqlite3.OperationalError("disk I/O error (simulated)")

        store._connection = broken  # type: ignore[method-assign]
        with pytest.raises(CredentialProviderError) as excinfo:
            store.get_token(ACCOUNT)
        assert TOKEN_A not in str(excinfo.value)
    finally:
        store._connection = original  # type: ignore[method-assign]
        store.close()


# ----- reopen persistence -----


async def test_reopen_preserves_encrypted_credentials(tmp_path: Path) -> None:
    env = {PRIMARY_KEY_ENV: generate_fernet_key()}
    store = await opened(tmp_path, env)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
    finally:
        store.close()
    reopened = await opened(tmp_path, env)  # same key material
    try:
        assert reopened.get_token(ACCOUNT) == TOKEN_A
    finally:
        reopened.close()


async def test_coexists_with_task_state_store(tmp_path: Path) -> None:
    """Same file: the v5 migration creates the credentials table next to
    task state; both stores operate independently."""
    db = tmp_path / "state.db"
    task_store = TaskStateStore(db)
    await task_store.open()
    cred = await opened(tmp_path)
    try:
        await cred.set_token(ACCOUNT, TOKEN_A)
        record = await task_store.create_instance(ACCOUNT, "probe")
        assert record.status.value == "pending"
        assert cred.get_token(ACCOUNT) == TOKEN_A
    finally:
        cred.close()
        await task_store.close()


# ----- key rotation -----


async def test_rotation_reencrypts_under_new_primary(tmp_path: Path) -> None:
    old_key = generate_fernet_key()
    env_old = {PRIMARY_KEY_ENV: old_key}
    store = await opened(tmp_path, env_old)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        await store.set_token(OTHER, TOKEN_B)
    finally:
        store.close()

    new_key = generate_fernet_key()
    env_new = {PRIMARY_KEY_ENV: new_key, RETIRED_KEYS_ENV: old_key}
    rotated = await opened(tmp_path, env_new)
    try:
        assert rotated.get_token(ACCOUNT) == TOKEN_A  # old key still decrypts
        count = await rotated.rotate()
        assert count == 2
        assert rotated.get_token(ACCOUNT) == TOKEN_A  # new primary now
        assert rotated.get_token(OTHER) == TOKEN_B
        rows = raw_rows(tmp_path / "state.db")
        assert {r[2] for r in rows} == {2}  # key_version bumped
        # Verify WITHOUT the retired key: new-primary-only MultiFernet.
        only_new = SQLiteCredentialStore(
            tmp_path / "state.db",
            FernetKeys(
                multi=MultiFernet([Fernet(new_key)]),
                primary=MultiFernet([Fernet(new_key)]),
                version=1,
            ),
        )
        await only_new.open()
        try:
            assert only_new.get_token(ACCOUNT) == TOKEN_A
        finally:
            only_new.close()
    finally:
        rotated.close()


async def test_interrupted_rotation_resumes_and_never_loses_credentials(tmp_path: Path) -> None:
    old_key = generate_fernet_key()
    store = await opened(tmp_path, {PRIMARY_KEY_ENV: old_key})
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        await store.set_token(OTHER, TOKEN_B)
    finally:
        store.close()

    new_key = generate_fernet_key()
    env = {PRIMARY_KEY_ENV: new_key, RETIRED_KEYS_ENV: old_key}
    resuming = await opened(tmp_path, env)
    try:
        # Simulate interruption after the FIRST row commits: partially
        # rotated state must stay consistent and resumable.
        real_rotate_row = resuming._rotate_row  # noqa: SLF001

        calls = [0]

        def flaky_row(account_id: int) -> bool:
            calls[0] += 1
            migrated = real_rotate_row(account_id)
            if calls[0] == 1:
                raise asyncio.CancelledError("simulated interruption after first row")
            return migrated

        resuming._rotate_row = flaky_row  # type: ignore[method-assign]
        with pytest.raises(asyncio.CancelledError):
            await resuming.rotate()
        # Partial state: exactly one row migrated, both still decryptable
        # (retired key still loaded).
        rows = raw_rows(tmp_path / "state.db")
        assert sorted(r[2] for r in rows) == [1, 2]
        assert resuming.get_token(ACCOUNT) == TOKEN_A
        assert resuming.get_token(OTHER) == TOKEN_B

        # Resume: complete the rotation; then retire the old key safely.
        resuming._rotate_row = real_rotate_row  # type: ignore[method-assign]
        remaining = await resuming.rotate()
        assert remaining == 1
        rows = raw_rows(tmp_path / "state.db")
        assert {r[2] for r in rows} == {2}
    finally:
        resuming.close()


async def test_rotation_requires_the_old_key_to_decrypt(tmp_path: Path) -> None:
    old_key = generate_fernet_key()
    store = await opened(tmp_path, {PRIMARY_KEY_ENV: old_key})
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
    finally:
        store.close()
    # Rotate WITHOUT listing the old key: explicit decrypt failure, no
    # silent loss.
    lost = await opened(tmp_path, {PRIMARY_KEY_ENV: generate_fernet_key()})
    try:
        with pytest.raises(CredentialDecryptError, match="missing retired key"):
            await lost.rotate()
        # The row is untouched, still decryptable with the old key.
        rows = raw_rows(tmp_path / "state.db")
        assert rows[0][2] == 1
    finally:
        lost.close()


# ----- provider boundary / no-leak guarantees -----


async def test_no_token_in_logs(tmp_path: Path, caplog: pytest.LogCaptureFixture) -> None:
    store = await opened(tmp_path)
    try:
        with caplog.at_level(logging.DEBUG, logger="autoclaimer.credential_store"):
            await store.set_token(ACCOUNT, TOKEN_A)
            store.get_token(ACCOUNT)
            await store.delete_token(ACCOUNT)
        for record in caplog.records:
            assert TOKEN_A not in record.getMessage()
    finally:
        store.close()


async def test_persistence_error_mapping_preserved(tmp_path: Path) -> None:
    """Admin operations map sqlite failures to the typed store errors."""
    store = await opened(tmp_path)
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        conn = sqlite3.connect(tmp_path / "state.db")
        with conn:
            conn.execute("DROP TABLE account_credentials")
        conn.close()
        with pytest.raises((PersistenceError, CredentialProviderError)):
            store.get_token(ACCOUNT)
        with pytest.raises((PersistenceError, CredentialProviderError)):
            await store.has_token(ACCOUNT)
    finally:
        store.close()


# ChatGPT PR #5 review: rotation must not overwrite concurrent updates
# (BUG-007) and malformed ciphertext rows must fail typed (BUG-009).


class _BlockingMulti:
    """MultiFernet proxy that blocks the FIRST decrypt call (the window
    between rotation's row read and its write)."""

    def __init__(
        self, inner: MultiFernet, entered: threading.Event, release: threading.Event
    ) -> None:
        self.inner = inner
        self.entered = entered
        self.release = release

    def decrypt(self, token: bytes) -> bytes:
        if not self.entered.is_set():
            self.entered.set()
            assert self.release.wait(5.0)
        return self.inner.decrypt(token)

    def encrypt(self, token: bytes) -> bytes:
        return self.inner.encrypt(token)


async def test_rotation_never_overwrites_a_concurrent_token_update(tmp_path: Path) -> None:
    """A token updated while rotation is mid-row must never revert.

    Store A (rotation) reads the old ciphertext and pauses; store B
    (separate instance, same database — cross-process equivalent) writes
    a NEW token; A resumes. The final stored token must be NEW."""
    import threading

    old_key = generate_fernet_key()
    new_key = generate_fernet_key()

    seeder = SQLiteCredentialStore(
        tmp_path / "state.db", load_fernet_keys({PRIMARY_KEY_ENV: old_key})
    )
    await seeder.open()
    old_token = "gho_OLDtokenThatWasRevoked_000111222333"
    await seeder.set_token(ACCOUNT, old_token)
    seeder.close()

    entered = threading.Event()
    release = threading.Event()
    rotating = SQLiteCredentialStore(
        tmp_path / "state.db",
        load_fernet_keys({PRIMARY_KEY_ENV: new_key, RETIRED_KEYS_ENV: old_key}),
    )
    await rotating.open()
    real_keys = rotating._keys  # noqa: SLF001
    from autoclaimer.credential_store import FernetKeys as _FK

    rotating._keys = _FK(  # noqa: SLF001
        multi=cast(MultiFernet, _BlockingMulti(real_keys.multi, entered, release)),
        primary=cast(MultiFernet, _BlockingMulti(real_keys.primary, entered, release)),
        version=real_keys.version,
    )

    writer = SQLiteCredentialStore(
        tmp_path / "state.db", load_fernet_keys({PRIMARY_KEY_ENV: new_key})
    )
    await writer.open()
    new_token = "gho_NEWtokenThatMustSurvive_444555666777"
    try:
        rotate_task = asyncio.create_task(rotating.rotate())
        assert await asyncio.to_thread(entered.wait, 5.0)  # A read the row, paused

        write_task = asyncio.create_task(writer.set_token(ACCOUNT, new_token))
        await asyncio.sleep(0.15)  # B commits (unserialized) or parks on the lock
        release.set()  # A resumes and writes its re-encrypted OLD token
        await write_task
        await rotate_task

        assert writer.get_token(ACCOUNT) == new_token, "rotation reverted the token"
    finally:
        rotating.close()
        writer.close()


async def test_rotation_handles_malformed_text_ciphertext_typed(tmp_path: Path) -> None:
    key = generate_fernet_key()
    store = SQLiteCredentialStore(tmp_path / "state.db", load_fernet_keys({PRIMARY_KEY_ENV: key}))
    await store.open()
    try:
        await store.set_token(ACCOUNT, TOKEN_A)
        conn = sqlite3.connect(tmp_path / "state.db")
        with conn:
            conn.execute(
                "UPDATE account_credentials SET encrypted_token = ? WHERE account_id = ?",
                ("not-a-binary-ciphertext", ACCOUNT),
            )
        conn.close()
        with pytest.raises(CredentialDecryptError, match="malformed"):
            await store.rotate()
    finally:
        store.close()
