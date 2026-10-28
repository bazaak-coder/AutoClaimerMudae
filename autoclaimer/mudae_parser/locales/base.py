"""Locale packs: per-language default patterns and message strings.

A locale pack is pure data — the compiled, validated defaults for one
Mudae language. English (``en``) ships with the framework; future
locales register another pack with the same shape (see
``parsers/mudae/locales/__init__.py``). Defaults are version-controlled
Python so they are diffable, testable and available with no settings
file present.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple

from ..exceptions import DuplicateSettingError
from ..settings.models import (
    MessageStringSetting,
    RegexPatternSetting,
)

__all__ = ["LocalePack"]


@dataclass(frozen=True)
class LocalePack:
    """All built-in defaults for one locale."""

    code: str
    patterns: Tuple[RegexPatternSetting, ...]
    strings: Tuple[MessageStringSetting, ...]

    def __post_init__(self) -> None:
        seen: Dict[str, str] = {}
        for setting in self.patterns:
            if setting.setting_id in seen:
                raise DuplicateSettingError(
                    f"duplicate pattern id {setting.setting_id!r} in "
                    f"locale {self.code!r}")
            seen[setting.setting_id] = "pattern"
        for setting in self.strings:
            if setting.setting_id in seen:
                raise DuplicateSettingError(
                    f"duplicate string id {setting.setting_id!r} in "
                    f"locale {self.code!r}")
            seen[setting.setting_id] = "string"

    def pattern_map(self) -> Dict[str, RegexPatternSetting]:
        return {p.setting_id: p for p in self.patterns}

    def string_map(self) -> Dict[str, MessageStringSetting]:
        return {s.setting_id: s for s in self.strings}
