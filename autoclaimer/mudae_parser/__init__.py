"""Mudae message parser engine (``parsers.mudae``).

Public surface::

    from parsers.mudae import MudaeParser, MudaeParserSettingsManager

``MudaeParser`` parses Discord messages into typed results; every
regex pattern and identifying message string is configurable at
runtime through ``MudaeParserSettingsManager`` (validated, atomic,
persisted under ``state/parser_settings.json``) — the seam a future
dashboard Parser Manager will consume. English defaults ship built-in
and grounded in the CuratedMudaeDataset.
"""
from __future__ import annotations

from .diagnostics import Diagnostics, Diagnostic
from .enums import (
    CaseSensitivity,
    CooldownKind,
    KakeraEventKind,
    MatchMode,
    MessageFamily,
    SettingOrigin,
    Severity,
    TargetSource,
)
from .exceptions import (
    CorruptSettingsError,
    DuplicateSettingError,
    InvalidMessageError,
    MudaeParserError,
    SettingsPersistenceError,
    UnknownSettingError,
    UnsupportedLocaleError,
)
from .models import (
    CharacterInfo,
    CooldownInfo,
    KakeralootInfo,
    KakeraEvent,
    KeyInfo,
    MarriageInfo,
    ParseResult,
    TimerInfo,
    TradeInfo,
    WishlistInfo,
)
from .parser import MudaeParser
from .settings import (
    EffectiveConfig,
    JSONFileSettingsStorage,
    MemorySettingsStorage,
    MessageStringSetting,
    MudaeParserSettingsManager,
    PreviewResult,
    RegexPatternSetting,
    SCHEMA_VERSION,
    UpdateResult,
    ValidationReport,
    default_storage_path,
)

__all__ = [
    "MudaeParser", "MudaeParserSettingsManager", "EffectiveConfig",
    "RegexPatternSetting", "MessageStringSetting", "ValidationReport",
    "UpdateResult", "PreviewResult", "JSONFileSettingsStorage",
    "MemorySettingsStorage", "SCHEMA_VERSION", "default_storage_path",
    "MessageFamily", "TargetSource", "MatchMode", "CaseSensitivity",
    "SettingOrigin", "Severity", "CooldownKind", "KakeraEventKind",
    "ParseResult", "CharacterInfo", "KeyInfo", "CooldownInfo",
    "KakeraEvent", "WishlistInfo", "KakeralootInfo", "TradeInfo",
    "MarriageInfo", "TimerInfo", "Diagnostic", "Diagnostics",
    "MudaeParserError", "InvalidMessageError", "UnknownSettingError",
    "DuplicateSettingError", "UnsupportedLocaleError",
    "SettingsPersistenceError", "CorruptSettingsError",
]
