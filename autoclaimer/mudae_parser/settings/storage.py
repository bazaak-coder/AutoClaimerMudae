"""Persistence for parser settings overrides.

Narrow interface (:class:`ParserSettingsStorage` protocol) + the
simplest reliable implementation this framework needs today:
:class:`JSONFileSettingsStorage` — one JSON document under the
gitignored runtime ``state/`` directory (the same convention as
``state/libzent.db``; runtime state never belongs in git).

Durability properties:
  * atomic writes — serialize to a temp file in the same directory,
    ``os.replace`` into place (atomic on Windows and POSIX); an
    interrupted write can never produce a half-written settings file;
  * schema version — the document carries ``schema_version``; loaders
    treat unknown-future versions as "no overrides" (never corrupt
    defaults) and known-old documents load what they can;
  * deterministic serialization — sorted keys, stable field order,
    UTF-8, so diffs and repeated saves are idempotent;
  * corruption recovery — invalid JSON/quits are surfaced as
    :class:`~parsers.mudae.exceptions.CorruptSettingsError` to the
    manager, which falls back to defaults and records a diagnostic
    (the corrupt file is preserved alongside for inspection).

Only OVERRIDES are persisted — never defaults (they live in version
control), never credentials or Discord data (nothing here is
account-related).
"""
from __future__ import annotations

import json
import os
import tempfile
import time
from typing import Any, Optional, Protocol

from ..exceptions import CorruptSettingsError

__all__ = [
    "SCHEMA_VERSION", "ParserSettingsStorage", "JSONFileSettingsStorage",
    "MemorySettingsStorage",
]

SCHEMA_VERSION = 1


class ParserSettingsStorage(Protocol):
    """Storage contract the settings manager depends on."""

    def load(self) -> Optional[dict]:
        """Return the stored document, or None when nothing is stored.

        Raises CorruptSettingsError when a file exists but cannot be
        understood — the manager decides the fallback."""
        ...

    def save(self, document: dict) -> None:
        """Persist the document atomically (or raise)."""
        ...


def _normalize_document(raw: Any) -> Optional[dict]:
    """Validate the loaded shape; None when empty, raise when junk."""
    if raw is None:
        return None
    if not isinstance(raw, dict):
        raise CorruptSettingsError(
            f"settings document is {type(raw).__name__}, expected object")
    version = raw.get("schema_version")
    if not isinstance(version, int):
        raise CorruptSettingsError("settings document lacks an integer "
                                   "schema_version")
    if version > SCHEMA_VERSION:
        # written by a NEWER build — do not guess at its meaning
        raise CorruptSettingsError(
            f"settings schema_version {version} is newer than supported "
            f"{SCHEMA_VERSION}")
    for key in ("patterns", "strings"):
        section = raw.get(key, {})
        if section is None:
            continue
        if not isinstance(section, dict):
            raise CorruptSettingsError(
                f"settings section {key!r} is corrupt")
        for setting_id, payload in section.items():
            if not isinstance(setting_id, str) or \
                    not isinstance(payload, dict):
                raise CorruptSettingsError(
                    f"settings entry {key}/{setting_id!r} is corrupt")
    return raw


class JSONFileSettingsStorage:
    """File-backed storage (atomic replace, corruption-preserving)."""

    def __init__(self, path: str) -> None:
        self._path = os.path.abspath(path)

    @property
    def path(self) -> str:
        return self._path

    def load(self) -> Optional[dict]:
        if not os.path.exists(self._path):
            return None
        try:
            with open(self._path, "r", encoding="utf-8") as handle:
                raw = json.load(handle)
        except (json.JSONDecodeError, UnicodeDecodeError, OSError) as exc:
            # preserve the corrupt file for inspection, never delete
            self._quarantine()
            raise CorruptSettingsError(
                f"settings file {self._path} is unreadable: {exc}") from exc
        return _normalize_document(raw)

    def save(self, document: dict) -> None:
        document = dict(document)
        document["schema_version"] = SCHEMA_VERSION
        document["saved_at"] = time.time()
        text = json.dumps(
            document, ensure_ascii=False, sort_keys=True, indent=2)
        directory = os.path.dirname(self._path) or "."
        os.makedirs(directory, exist_ok=True)
        # atomic: temp file in the SAME directory, then replace
        fd, tmp_path = tempfile.mkstemp(
            dir=directory, prefix=".parser-settings-", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(text)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(tmp_path, self._path)
        except OSError:
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            raise

    def _quarantine(self) -> None:
        """Rename a corrupt file aside (best effort)."""
        try:
            os.replace(self._path,
                       f"{self._path}.corrupt-{int(time.time())}")
        except OSError:
            pass


class MemorySettingsStorage:
    """In-memory storage for tests and ephemeral stacks (no disk)."""

    def __init__(self) -> None:
        self.document: Optional[dict] = None
        self.save_calls = 0
        #: set to True in tests to simulate a failing backend
        self.fail_save = False

    def load(self) -> Optional[dict]:
        if self.document is None:
            return None
        return _normalize_document(json.loads(
            json.dumps(self.document)))  # deep-copy guard

    def save(self, document: dict) -> None:
        if self.fail_save:
            from ..exceptions import SettingsPersistenceError

            raise SettingsPersistenceError("simulated storage failure")
        self.save_calls += 1
        self.document = json.loads(json.dumps(document))
