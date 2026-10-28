"""Structured diagnostics for parsing and configuration loading.

Diagnostics are data, not exceptions: a parse of an unknown message, a
rejected persisted override, or an oversized input all surface here so
callers (and the future dashboard) can render them without catching.

Codes are stable strings; treat them as an API.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import List, Optional, Tuple

from .enums import Severity

#: diagnostic codes (stable API)
CODE_UNKNOWN_FORMAT = "unknown_format"
CODE_OVERSIZED_INPUT = "oversized_input"
CODE_AMBIGUOUS_MATCH = "ambiguous_match"
CODE_NO_MATCH = "no_match"
CODE_REJECTED_OVERRIDE = "rejected_override"
CODE_CORRUPT_STORE = "corrupt_store"
CODE_OUTDATED_STORE = "outdated_store"
CODE_DISABLED_RULE = "disabled_rule"
CODE_SKIPPED_SOURCE = "skipped_source"
CODE_LOAD_FALLBACK = "load_fallback"

__all__ = [
    "Diagnostic", "Diagnostics",
    "CODE_UNKNOWN_FORMAT", "CODE_OVERSIZED_INPUT", "CODE_AMBIGUOUS_MATCH",
    "CODE_NO_MATCH", "CODE_REJECTED_OVERRIDE", "CODE_CORRUPT_STORE",
    "CODE_OUTDATED_STORE", "CODE_DISABLED_RULE", "CODE_SKIPPED_SOURCE",
    "CODE_LOAD_FALLBACK",
]


@dataclass(frozen=True)
class Diagnostic:
    """One structured observation (never an exception by itself)."""

    severity: Severity
    code: str
    message: str
    #: related setting ID, rule ID or source path when applicable
    subject: Optional[str] = None
    detail: Optional[str] = None

    def to_dict(self) -> dict:
        """JSON-friendly representation (future dashboard boundary)."""
        return {
            "severity": self.severity.value,
            "code": self.code,
            "message": self.message,
            "subject": self.subject,
            "detail": self.detail,
        }


class Diagnostics:
    """Mutable collector handed around during one parse / load."""

    def __init__(self) -> None:
        self._items: List[Diagnostic] = []

    def add(self, severity: Severity, code: str, message: str,
            subject: Optional[str] = None,
            detail: Optional[str] = None) -> Diagnostic:
        item = Diagnostic(severity, code, message, subject, detail)
        self._items.append(item)
        return item

    def info(self, code: str, message: str, **kw) -> Diagnostic:
        return self.add(Severity.INFO, code, message, **kw)

    def warning(self, code: str, message: str, **kw) -> Diagnostic:
        return self.add(Severity.WARNING, code, message, **kw)

    def error(self, code: str, message: str, **kw) -> Diagnostic:
        return self.add(Severity.ERROR, code, message, **kw)

    @property
    def items(self) -> Tuple[Diagnostic, ...]:
        return tuple(self._items)

    def has(self, code: str) -> bool:
        return any(item.code == code for item in self._items)

    def to_list(self) -> List[dict]:
        return [item.to_dict() for item in self._items]

    def __len__(self) -> int:
        return len(self._items)

    def __repr__(self) -> str:  # pragma: no cover - debug aid
        return f"<Diagnostics {len(self._items)} item(s)>"
