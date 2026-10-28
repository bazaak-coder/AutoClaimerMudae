"""Typed settings models for regex patterns and raw message strings.

Two explicit kinds, one shared metadata base — never a giant untyped
dict. All setting models are frozen: callers mutate configuration only
through :class:`~parsers.mudae.settings.manager.MudaeParserSettingsManager`,
which validates, persists and publishes atomically.

Identity is the stable ``setting_id`` — editing an expression never
changes identity. Defaults carry a ``default_version`` so a future
software update can reason about outdated user overrides (see
``parsers/mudae/docs`` — versioning note: a shipped default bump does
not rewrite overrides; the catalog exposes both for diffing).
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional, Tuple

from ..enums import (
    CaseSensitivity,
    MessageFamily,
    MatchMode,
    TargetSource,
)

__all__ = [
    "SettingBase", "RegexPatternSetting", "MessageStringSetting",
    "ValidationIssue", "ValidationReport", "UpdateResult",
]


@dataclass(frozen=True)
class SettingBase:
    """Metadata shared by every configurable parser setting."""

    #: stable identity — survives expression edits, never reused
    setting_id: str
    #: human-readable name for the future dashboard catalog
    name: str
    description: str = ""
    #: settings group / message family this rule belongs to
    family: MessageFamily = MessageFamily.CLASSIFICATION
    #: which message part this setting reads
    target: TargetSource = TargetSource.CONTENT
    locale: str = "en"
    enabled: bool = True
    #: lower runs first (deterministic ordering inside a family)
    priority: int = 100
    #: version tag of the shipped default this setting tracks
    default_version: int = 1


@dataclass(frozen=True)
class RegexPatternSetting(SettingBase):
    """One configurable regex extraction/classification pattern.

    ``required_groups``/``optional_groups`` declare the named capture
    groups this pattern's extraction contract needs. A candidate
    expression that compiles but lacks a required group is REJECTED at
    validation time (a user renaming the character-name group must not
    silently break character extraction). Patterns used purely for
    classification/counting (e.g. sphere markers) declare no groups.
    """

    expression: str = ""
    case_sensitive: bool = True
    required_groups: Tuple[str, ...] = ()
    optional_groups: Tuple[str, ...] = ()


@dataclass(frozen=True)
class MessageStringSetting(SettingBase):
    """One configurable raw Discord message string (literal text).

    The stored ``text`` is the verbatim string as observed in the
    dataset — punctuation, emoji, spacing and capitalization preserved;
    nothing is silently lowercased. ``match_mode`` decides comparison;
    the string is NEVER interpreted as a regex. Case-insensitive mode
    compares without altering the stored raw text.
    """

    text: str = ""
    match_mode: MatchMode = MatchMode.CONTAINS
    case_sensitive: bool = True


# ----------------------------------------------------------- API results

@dataclass(frozen=True)
class ValidationIssue:
    """One finding from validating a candidate setting."""

    code: str
    message: str
    #: which requirement failed (syntax/group/length/target/mode/...)
    subject: Optional[str] = None

    def to_dict(self) -> dict:
        return {"code": self.code, "message": self.message,
                "subject": self.subject}


@dataclass(frozen=True)
class ValidationReport:
    """Structured outcome of validating candidate settings."""

    ok: bool
    issues: Tuple[ValidationIssue, ...] = ()

    def to_dict(self) -> dict:
        return {"ok": self.ok,
                "issues": [issue.to_dict() for issue in self.issues]}


@dataclass(frozen=True)
class UpdateResult:
    """Structured outcome of one settings-manager transaction.

    ok=False → nothing changed (active config and stored overrides are
    exactly as before); errors explain why.
    """

    ok: bool
    setting_id: str
    errors: Tuple[ValidationIssue, ...] = ()
    #: non-fatal notes (e.g. fallback applied at load time)
    notes: Tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {"ok": self.ok, "setting_id": self.setting_id,
                "errors": [e.to_dict() for e in self.errors],
                "notes": list(self.notes)}
