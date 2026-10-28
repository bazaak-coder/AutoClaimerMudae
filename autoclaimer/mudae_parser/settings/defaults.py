"""Built-in defaults access — the floor under every configuration.

Defaults come exclusively from version-controlled locale packs. They are
available with no settings file, never erased by invalid overrides, and
deterministic across restarts (plain module data, no runtime state).

Versioning: each default carries ``default_version``. When a software
update changes a shipped default, bump the version and keep the stable
ID — existing overrides keep working (they are validated against the
SAME required-groups contract), and the catalog exposes
``default_version`` so a future dashboard can offer "reset to the new
default" with a diff. Overrides are never auto-rewritten by an update.
"""
from __future__ import annotations

from typing import Dict

from ..enums import MessageFamily, TargetSource
from ..exceptions import UnsupportedLocaleError
from ..locales import get_locale
from ..settings.models import (
    MessageStringSetting,
    RegexPatternSetting,
)

__all__ = ["default_patterns", "default_strings"]


def default_patterns(locale: str = "en"
                     ) -> Dict[str, RegexPatternSetting]:
    """All built-in regex defaults for a locale, keyed by stable ID."""
    return get_locale(locale).pattern_map()


def default_strings(locale: str = "en"
                    ) -> Dict[str, MessageStringSetting]:
    """All built-in message-string defaults for a locale."""
    return get_locale(locale).string_map()


def supported_targets() -> tuple:
    """Every target any default binds (validation allowlist source)."""
    return tuple(TargetSource)


def supported_families() -> tuple:
    return tuple(MessageFamily)
