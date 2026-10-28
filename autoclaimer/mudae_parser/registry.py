"""Rule registry: the ordered set of rules the engine runs.

Registration is code-only (trusted). The registry validates at import
time that every rule binding references a setting that exists in the
default catalog — a typo in a rule's setting ID fails loudly at
startup, never at parse time.
"""
from __future__ import annotations

from typing import Dict, Tuple

from .enums import MessageFamily
from .exceptions import MudaeParserError
from .rules.base import Rule
from .rules import (
    CHARACTER_RULE,
    CLAIM_EVENT_RULE,
    CLAIM_TIMER_RULE,
    DK_READY_RULE,
    KAKERA_REACTION_RULE,
    KAKERALOOT_PAYOUT_RULE,
    KAKERALOOT_PROFILE_RULE,
    MARRIAGE_RULE,
    REACT_COOLDOWN_RULE,
    ROULETTE_LIMIT_RULE,
    RT_AVAILABLE_RULE,
    RT_COOLDOWN_RULE,
    TRADE_GIFT_RULE,
    TRADE_GIVE_CONFIRM_RULE,
    TRADE_INSUFFICIENT_RULE,
    WISHLIST_RULE,
)
from .rules.usernames import USERNAME_PATTERN_IDS
from .settings.defaults import default_patterns, default_strings

__all__ = ["DEFAULT_RULES", "validate_rule_bindings"]


def _all_rules() -> Tuple[Rule, ...]:
    return (
        CHARACTER_RULE,
        CLAIM_EVENT_RULE,
        KAKERA_REACTION_RULE,
        ROULETTE_LIMIT_RULE,
        RT_COOLDOWN_RULE,
        RT_AVAILABLE_RULE,
        DK_READY_RULE,
        REACT_COOLDOWN_RULE,
        CLAIM_TIMER_RULE,
        TRADE_INSUFFICIENT_RULE,
        TRADE_GIFT_RULE,
        TRADE_GIVE_CONFIRM_RULE,
        KAKERALOOT_PAYOUT_RULE,
        KAKERALOOT_PROFILE_RULE,
        WISHLIST_RULE,
        MARRIAGE_RULE,
    )


def validate_rule_bindings(rules: Tuple[Rule, ...],
                           locale: str = "en") -> None:
    """Fail loudly when a rule binds an unknown setting ID."""
    known_patterns = set(default_patterns(locale))
    known_strings = set(default_strings(locale))
    problems = []
    seen_ids: Dict[str, str] = {}
    for rule in rules:
        if rule.rule_id in seen_ids:
            problems.append(f"duplicate rule id {rule.rule_id!r}")
        seen_ids[rule.rule_id] = rule.family.value
        for binding in rule.patterns:
            if binding.setting_id not in known_patterns \
                    and binding.setting_id not in USERNAME_PATTERN_IDS:
                problems.append(
                    f"rule {rule.rule_id!r} binds unknown pattern "
                    f"{binding.setting_id!r}")
        for binding in rule.strings:
            if binding.setting_id not in known_strings:
                problems.append(
                    f"rule {rule.rule_id!r} binds unknown string "
                    f"{binding.setting_id!r}")
    if problems:
        raise MudaeParserError(
            "rule registry validation failed: " + "; ".join(problems))


#: rules in execution order (lower priority first)
DEFAULT_RULES: Tuple[Rule, ...] = tuple(
    sorted(_all_rules(), key=lambda rule: rule.priority))

# fail loudly AT IMPORT when a rule binds an unknown setting ID — a
# typo must never silently disable a family at parse time
validate_rule_bindings(DEFAULT_RULES)
