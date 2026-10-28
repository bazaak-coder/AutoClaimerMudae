"""Rule contract: how one message family is recognized and extracted.

A rule binds, by stable setting ID:
  * string markers  — literal classification gates (from the catalog's
    message-string settings);
  * patterns        — regex extraction sources (from the catalog's
    pattern settings);
  * a trusted handler implemented in this package (never configurable
    through persisted settings — only the patterns/strings are).

The engine (``parsers/mudae/registry.py`` + ``parser.py``) runs rules
in priority order against a normalized message; the first rule whose
gates pass and whose handler claims the message assigns the family.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Optional, Tuple

from ..enums import MessageFamily
from ..models import (
    CharacterInfo,
    CooldownInfo,
    KakeraEvent,
    KakeralootInfo,
    MarriageInfo,
    TimerInfo,
    TradeInfo,
    WishlistInfo,
)

__all__ = [
    "PatternBinding", "StringBinding", "RuleResult", "RuleContext", "Rule",
]


@dataclass(frozen=True)
class PatternBinding:
    """One pattern setting a rule consumes."""

    setting_id: str
    #: False → rule still matches without it (handler degrades)
    required: bool = False
    #: "first" → only the first match is delivered; "all" → every match
    multiplicity: str = "first"


@dataclass(frozen=True)
class StringBinding:
    """One message-string marker a rule gates on.

    required markers must ALL match; at least one marker (required or
    not) must match when the rule declares any markers at all.
    """

    setting_id: str
    required: bool = False


@dataclass
class RuleResult:
    """Typed payloads a handler produces for one matched message."""

    matched: bool = False
    character: Optional[CharacterInfo] = None
    kakera_events: Tuple[KakeraEvent, ...] = ()
    cooldowns: Tuple[CooldownInfo, ...] = ()
    timer: Optional[TimerInfo] = None
    wishlist: Optional[WishlistInfo] = None
    kakeraloot: Optional[KakeralootInfo] = None
    trade: Optional[TradeInfo] = None
    marriage: Optional[MarriageInfo] = None
    usernames: Tuple[str, ...] = ()


class RuleContext:
    """Read-only view the engine hands a handler: marker answers,
    pattern group-dicts, parser context."""

    __slots__ = ("markers", "group_dicts", "account_username",
                 "raw_sources")

    def __init__(self, markers: Dict[str, bool],
                 group_dicts: Dict[str, List[Dict[str, Any]]],
                 account_username: Optional[str],
                 raw_sources: Dict[str, List[str]]) -> None:
        self.markers = markers
        self.group_dicts = group_dicts
        self.account_username = account_username
        self.raw_sources = raw_sources

    def marker(self, setting_id: str) -> bool:
        return self.markers.get(setting_id, False)

    def groups(self, setting_id: str) -> Optional[Dict[str, Any]]:
        """First match's group dict (None when the pattern missed)."""
        found = self.group_dicts.get(setting_id)
        return found[0] if found else None

    def all_groups(self, setting_id: str) -> Tuple[Dict[str, Any], ...]:
        """Every match's group dict (multi-entry patterns like keys)."""
        return tuple(self.group_dicts.get(setting_id, ()))


@dataclass(frozen=True)
class Rule:
    """One registered family rule (trusted, code-defined)."""

    rule_id: str
    family: MessageFamily
    handler: Callable[[RuleContext], RuleResult]
    patterns: Tuple[PatternBinding, ...] = ()
    strings: Tuple[StringBinding, ...] = ()
    #: lower runs first
    priority: int = 100
    #: human-facing note for diagnostics/catalog
    description: str = ""
