"""Username extraction (family: username).

Cross-cutting: the engine ALWAYS runs the username patterns after a
family match (or on unknown messages) — usernames are a helper signal,
not a family of their own at parse time. Extraction reads the message
text only; the caller-supplied ``account_username`` context is carried
separately and never overwrites an extracted name (or vice versa).
"""
from __future__ import annotations

from ..enums import MessageFamily
from .base import PatternBinding, Rule, RuleContext, RuleResult

__all__ = ["USERNAME_PATTERN_IDS"]


#: pattern settings the engine runs as the cross-cutting username pass
USERNAME_PATTERN_IDS = ("usernames.leading_bold",
                        "usernames.kakeraloot_title")


def extract_usernames(group_dicts: dict) -> tuple:
    """Collect distinct usernames from prepared group dicts, in order."""
    seen = []
    for setting_id in USERNAME_PATTERN_IDS:
        for groups in group_dicts.get(setting_id, ()):
            value = groups.get("username")
            if isinstance(value, str) and value.strip() \
                    and value.strip() not in seen:
                seen.append(value.strip())
    return tuple(seen)
