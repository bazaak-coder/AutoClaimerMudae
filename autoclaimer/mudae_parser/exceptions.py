"""Exception hierarchy for the Mudae parser engine.

Two audiences, kept deliberately separate:

* :class:`MudaeParserError` and friends — raised for *programmer* errors
  (bad API usage: unknown setting IDs, unsupported locales, malformed
  message objects). These fail loudly and never masquerade as
  "message did not match".
* *Data* problems (unknown message formats, non-matching patterns,
  rejected candidate expressions) are NOT exceptions — they come back as
  typed results (:class:`~parsers.mudae.models.ParseResult` with
  diagnostics, :class:`~parsers.mudae.settings.models.ValidationReport`
  / ``UpdateResult``) so a future dashboard can render them.

Expected nonmatches never raise: parsing an unrelated message returns an
UNKNOWN-family result, not an exception.
"""
from __future__ import annotations

__all__ = [
    "MudaeParserError",
    "InvalidMessageError",
    "UnknownSettingError",
    "DuplicateSettingError",
    "UnsupportedLocaleError",
    "SettingsPersistenceError",
    "CorruptSettingsError",
]


class MudaeParserError(Exception):
    """Base class for all Mudae parser engine errors."""


class InvalidMessageError(MudaeParserError):
    """The message input itself is unusable (not a string, not
    message-like, empty after normalization). Not raised for messages
    that are merely *unknown* — those return an UNKNOWN-family result."""


class UnknownSettingError(MudaeParserError, KeyError):
    """No setting exists under the requested stable ID."""


class DuplicateSettingError(MudaeParserError):
    """Two built-in (or persisted) settings claim the same stable ID."""


class UnsupportedLocaleError(MudaeParserError):
    """No locale pack is registered for the requested locale code."""


class SettingsPersistenceError(MudaeParserError):
    """Saving persisted overrides failed (storage backend error)."""


class CorruptSettingsError(MudaeParserError):
    """The persisted settings file is unreadable/invalid JSON.

    Raised by storage loaders only when the caller asks for strict
    behavior; the settings manager treats it as "start from defaults"
    and records a diagnostic instead.
    """
