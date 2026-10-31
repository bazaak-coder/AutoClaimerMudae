"""Fernet-encrypted account-token storage (SQLite-backed).

Design (owner task 2026-10-31):

- Tokens are encrypted with ``cryptography.fernet`` (authenticated
  encryption) BEFORE they touch SQLite; the database never sees
  plaintext. The table lives in the SHARED state database (created by
  the schema-v5 migration in :mod:`autoclaimer.persistence`) so one
  file carries task state and credentials — but all credential logic
  lives HERE, not in task-state code.
- The Fernet key(s) NEVER live in the database, config files, or logs.
  They come from the environment: ``AUTOCLAIMER_FERNET_KEY`` (primary,
  required) and ``AUTOCLAIMER_FERNET_RETIRED_KEYS`` (comma-separated,
  optional — kept only so rotation can decrypt pre-rotation rows).
  Missing/invalid key material FAILS CLOSED: no plaintext fallback,
  no silent key generation.
- This store implements the :class:`autoclaimer.credentials.CredentialProvider`
  protocol, so Discord adapters request tokens through the existing
  boundary; task handlers never see keys, ciphertext, or this module.
- Concurrency: a dedicated SQLite connection guarded by a thread-level
  ``RLock`` (same discipline as ``TaskStateStore``). ``get_token`` is a
  synchronous primary-key READ — it cannot block on the task-state
  store's writers under SQLite's rollback journal (readers take the
  SHARED lock). Administrative writes run on worker threads.
- Error hygiene: no exception, log line, or return value produced here
  ever contains a token, a ciphertext, or key material.
"""

from __future__ import annotations

import asyncio
import logging
import os
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any

from cryptography.fernet import Fernet, InvalidToken, MultiFernet

from .credentials import CredentialProviderError
from .persistence import _CREDENTIALS_DDL, PersistenceError, _map_sqlite_error, utc_now

logger = logging.getLogger(__name__)

#: Environment variable holding the PRIMARY Fernet key (url-safe base64,
#: 32 bytes). Required whenever encrypted credentials are used.
PRIMARY_KEY_ENV = "AUTOCLAIMER_FERNET_KEY"

#: Optional comma-separated RETIRED keys (oldest rotation generation
#: first is irrelevant; order matters only for primary = first of the
#: primary env var). Kept until every row is re-encrypted and verified.
RETIRED_KEYS_ENV = "AUTOCLAIMER_FERNET_RETIRED_KEYS"

_MAX_SNOWFLAKE = 2**63 - 1


class CredentialKeyError(CredentialProviderError):
    """Encryption key material is missing or invalid (fail closed)."""


class CredentialNotFoundError(CredentialProviderError):
    """No credential is stored for the requested account."""


class CredentialDecryptError(CredentialProviderError):
    """A stored credential could not be decrypted (wrong/rotated-away
    key, or corrupted ciphertext). Never carries token or key material."""


def _validate_account_id(account_id: int) -> int:
    # bool is a subclass of int — reject it explicitly.
    if type(account_id) is not int or not 0 < account_id <= _MAX_SNOWFLAKE:
        raise ValueError(
            f"account id must be a positive Discord snowflake integer, got {account_id!r}"
        )
    return account_id


def _parse_fernet_key(material: str, *, source: str) -> Fernet:
    """Validate ONE key without ever echoing key material in errors."""
    try:
        return Fernet(material.encode("ascii"))
    except (ValueError, TypeError) as exc:
        # Fernet's own messages describe the expected shape only; they
        # do not include the offending key. Keep it that way.
        raise CredentialKeyError(
            f"{source} is not a valid Fernet key (expected url-safe base64 "
            f"of exactly 32 bytes): {exc}"
        ) from exc


@dataclass(frozen=True)
class FernetKeys:
    """Loaded key material: the multi-key view and the primary-only view.

    ``multi`` decrypts with ANY loaded key and encrypts with the
    primary; ``primary`` (single-key view) answers "is this ciphertext
    already current?" cryptographically — rotation never trusts
    ``key_version`` bookkeeping alone (a row written under an unlisted
    key still reports a stale version, and version numbers can lie).
    """

    multi: MultiFernet
    primary: MultiFernet
    version: int


def load_fernet_keys(environ: dict[str, str] | None = None) -> FernetKeys:
    """Load keys from the environment: primary (required) + retired.

    The primary encrypts new writes; retired keys decrypt
    pre-rotation rows until rotation completes.
    """
    env = os.environ if environ is None else environ
    raw_primary = env.get(PRIMARY_KEY_ENV, "").strip()
    if not raw_primary:
        raise CredentialKeyError(
            f"{PRIMARY_KEY_ENV} is not set: encrypted account credentials "
            "are unavailable (fail closed). Generate a key for FIRST-TIME "
            "setup with the CLI 'credential generate-key' command and store "
            "it outside the database (e.g. a secret manager or env file)."
        )
    primary = _parse_fernet_key(raw_primary, source=PRIMARY_KEY_ENV)
    keys = [primary]
    raw_retired = env.get(RETIRED_KEYS_ENV, "").strip()
    if raw_retired:
        for i, material in enumerate(k.strip() for k in raw_retired.split(",")):
            if not material:
                continue
            keys.append(_parse_fernet_key(material, source=f"{RETIRED_KEYS_ENV} entry {i + 1}"))
    return FernetKeys(
        multi=MultiFernet(keys),
        primary=MultiFernet([primary]),
        version=len(keys),
    )


def generate_fernet_key() -> str:
    """A fresh Fernet key for FIRST-TIME setup (display only).

    Never persists, never overwrites, never touches existing rows —
    losing the active key makes existing credentials unrecoverable.
    """
    return Fernet.generate_key().decode("ascii")


class SQLiteCredentialStore:
    """Encrypted per-account token store; a drop-in CredentialProvider.

    Same database file as task state (schema v5 table), dedicated
    connection + guard. Admin operations are async (worker threads);
    ``get_token`` is the protocol's synchronous read.
    """

    def __init__(
        self,
        path: str | Path,
        keys: FernetKeys,
        *,
        clock: Callable[[], datetime] = utc_now,
    ) -> None:
        self._path = Path(path)
        self._keys = keys
        self._clock = clock
        self._conn: sqlite3.Connection | None = None
        self._guard = threading.RLock()
        self._closed = False

    # ----- lifecycle -----

    async def open(self) -> None:
        """Open the connection and ensure the credentials table exists.

        The table is owned by the shared migration chain; creating it
        here is idempotent and lets a credentials-only tool (e.g. CLI
        setup) run before any task-state store opens the file.
        """

        def _connect() -> sqlite3.Connection:
            conn = sqlite3.connect(self._path, check_same_thread=False, isolation_level=None)
            conn.execute("PRAGMA busy_timeout = 5000")
            with self._guard:
                conn.execute(_CREDENTIALS_DDL)
            return conn

        try:
            self._conn = await asyncio.to_thread(_connect)
        except sqlite3.Error as exc:
            raise _map_sqlite_error(exc) from exc

    def close(self) -> None:
        """Close the connection; safe to call more than once.

        SYNCHRONOUS by protocol requirement: ``CredentialProvider.close``
        is a sync hook the controller calls during shutdown. Closing one
        dedicated connection is a single fast C call under the guard.
        """
        self._closed = True
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                self._guarded(conn.close)
            except sqlite3.Error as exc:
                raise _map_sqlite_error(exc) from exc

    def _guarded(self, fn: Any, *args: Any) -> Any:
        with self._guard:
            return fn(*args)

    def _connection(self) -> sqlite3.Connection:
        if self._conn is None or self._closed:
            raise CredentialProviderError("credential store is not open")
        return self._conn

    # ----- provider protocol (sync read) -----

    def get_token(self, account_id: int) -> str:
        """Decrypt and return the token for *account_id*.

        Synchronous primary-key read on the dedicated connection. Raises
        CredentialProviderError subclasses; messages never contain the
        token, the ciphertext, or key material.
        """
        _validate_account_id(account_id)
        try:
            conn = self._connection()
            with self._guard:
                row = conn.execute(
                    "SELECT encrypted_token FROM account_credentials WHERE account_id = ?",
                    (account_id,),
                ).fetchone()
        except sqlite3.Error as exc:
            raise CredentialProviderError(
                f"credential database read failed for account {account_id}: {type(exc).__name__}"
            ) from exc
        if row is None:
            raise CredentialNotFoundError(f"no stored credential for account {account_id}")
        blob = row[0]
        if not isinstance(blob, (bytes, bytearray)):
            raise CredentialDecryptError(f"stored credential for account {account_id} is malformed")
        try:
            return self._keys.multi.decrypt(bytes(blob)).decode("utf-8")
        except InvalidToken as exc:
            raise CredentialDecryptError(
                f"stored credential for account {account_id} could not be "
                "decrypted: the configured Fernet key is wrong, was rotated "
                "away, or the data is corrupted"
            ) from exc
        except UnicodeDecodeError as exc:
            raise CredentialDecryptError(
                f"stored credential for account {account_id} decrypted to invalid text"
            ) from exc

    # ----- administration (async writes) -----

    async def set_token(self, account_id: int, token: str) -> None:
        """Encrypt *token* with the PRIMARY key and upsert the row.

        The plaintext token exists only in this call's frame — never in
        the database, a log line, or an exception.
        """
        _validate_account_id(account_id)
        if not token or not isinstance(token, str):
            raise ValueError("token must be a non-empty string")
        blob = self._keys.multi.encrypt(token.encode("utf-8"))
        now = self._clock().isoformat()

        def _write() -> None:
            conn = self._connection()
            with self._guard:
                conn.execute("BEGIN IMMEDIATE")
                try:
                    conn.execute(
                        "INSERT INTO account_credentials "
                        "(account_id, encrypted_token, key_version, created_at, updated_at) "
                        "VALUES (?, ?, ?, ?, ?) "
                        "ON CONFLICT(account_id) DO UPDATE SET "
                        "encrypted_token = excluded.encrypted_token, "
                        "key_version = excluded.key_version, "
                        "updated_at = excluded.updated_at",
                        (account_id, blob, self._keys.version, now, now),
                    )
                    conn.execute("COMMIT")
                except BaseException:
                    conn.execute("ROLLBACK")
                    raise

        try:
            await asyncio.to_thread(_write)
        except sqlite3.Error as exc:
            raise _map_sqlite_error(exc) from exc
        logger.info(
            "account credential stored (encrypted)",
            extra={"account_id": account_id, "operation": "credential.set"},
        )

    async def delete_token(self, account_id: int) -> bool:
        """Delete the stored credential; True if a row was removed.

        Removing an account from accounts.json does NOT auto-delete its
        credential (a config typo must not destroy a secret): deletion
        is always this explicit operation.
        """
        _validate_account_id(account_id)

        def _delete() -> int:
            conn = self._connection()
            with self._guard:
                cur = conn.execute(
                    "DELETE FROM account_credentials WHERE account_id = ?", (account_id,)
                )
                return cur.rowcount

        try:
            removed = await asyncio.to_thread(_delete)
        except sqlite3.Error as exc:
            raise _map_sqlite_error(exc) from exc
        if removed:
            logger.info(
                "account credential deleted",
                extra={"account_id": account_id, "operation": "credential.delete"},
            )
        return bool(removed)

    async def has_token(self, account_id: int) -> bool:
        """Whether a credential row exists for *account_id* (no decryption)."""
        _validate_account_id(account_id)

        def _probe() -> bool:
            conn = self._connection()
            with self._guard:
                row = conn.execute(
                    "SELECT 1 FROM account_credentials WHERE account_id = ?", (account_id,)
                ).fetchone()
                return row is not None

        try:
            return await asyncio.to_thread(_probe)
        except sqlite3.Error as exc:
            raise _map_sqlite_error(exc) from exc

    def _rotate_row(self, account_id: int) -> bool:
        """Re-encrypt ONE row under the primary — or verify it is already
        current. Selection is CRYPTOGRAPHIC: a row is current only if it
        decrypts under the primary-only view; ``key_version`` metadata is
        written for operators but never trusted as the migration oracle.
        Per-row verify-then-commit makes an interrupted rotation
        resumable; un-migrated rows stay decryptable via retired keys.

        Returns True if the row was re-encrypted, False if already current.
        """
        conn = self._connection()
        with self._guard:
            row = conn.execute(
                "SELECT encrypted_token FROM account_credentials WHERE account_id = ?",
                (account_id,),
            ).fetchone()
            if row is None:
                return False
            blob = bytes(row[0])
            try:
                self._keys.primary.decrypt(blob)
                return False  # already current under the primary key
            except InvalidToken:
                pass
            token = self._keys.multi.decrypt(blob)  # any loaded (retired) key
            new_blob = self._keys.multi.encrypt(token)  # primary
            # Verify the re-encrypted row decrypts back BEFORE commit.
            if self._keys.primary.decrypt(new_blob) != token:  # pragma: no cover - AEAD
                raise CredentialProviderError(
                    f"rotation verification failed for account {account_id}"
                )
            conn.execute("BEGIN IMMEDIATE")
            try:
                conn.execute(
                    "UPDATE account_credentials SET encrypted_token = ?, "
                    "key_version = ?, updated_at = ? WHERE account_id = ?",
                    (new_blob, self._keys.version, self._clock().isoformat(), account_id),
                )
                conn.execute("COMMIT")
            except BaseException:
                conn.execute("ROLLBACK")
                raise
            return True

    async def rotate(self) -> int:
        """Re-encrypt every pre-rotation row under the PRIMARY key.

        Per-row verify-then-commit: each row is decrypted (any loaded
        key), re-encrypted with the primary, and the new ciphertext is
        DECRYPTED AND COMPARED before the row update commits. An
        interrupted rotation is safely resumable — un-migrated rows
        still decrypt via the retired keys listed in the environment;
        re-running completes them. Keep the retired keys configured
        until this reports zero remaining.

        Returns the number of rows re-encrypted.
        """
        reencrypted = 0

        def _row_ids() -> list[int]:
            conn = self._connection()
            with self._guard:
                return [
                    int(r[0])
                    for r in conn.execute(
                        "SELECT account_id FROM account_credentials ORDER BY account_id"
                    )
                ]

        try:
            ids = await asyncio.to_thread(_row_ids)
            for account_id in ids:
                try:
                    migrated = await asyncio.to_thread(self._rotate_row, account_id)
                except InvalidToken as exc:
                    raise CredentialDecryptError(
                        f"rotation could not decrypt the stored credential for "
                        f"account {account_id} (missing retired key?)"
                    ) from exc
                if migrated:
                    reencrypted += 1
        except sqlite3.Error as exc:
            raise _map_sqlite_error(exc) from exc
        logger.info(
            "credential rotation complete",
            extra={"operation": "credential.rotate", "outcome": str(reencrypted)},
        )
        return reencrypted

    async def account_ids(self) -> tuple[int, ...]:
        """All accounts with stored credentials (management/inspection)."""

        def _ids() -> tuple[int, ...]:
            conn = self._connection()
            with self._guard:
                return tuple(
                    int(r[0]) for r in conn.execute("SELECT account_id FROM account_credentials")
                )

        try:
            return await asyncio.to_thread(_ids)
        except sqlite3.Error as exc:
            raise _map_sqlite_error(exc) from exc


__all__ = [
    "PRIMARY_KEY_ENV",
    "RETIRED_KEYS_ENV",
    "CredentialDecryptError",
    "CredentialKeyError",
    "CredentialNotFoundError",
    "FernetKeys",
    "SQLiteCredentialStore",
    "generate_fernet_key",
    "load_fernet_keys",
    "PersistenceError",
]
