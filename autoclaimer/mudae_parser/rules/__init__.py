"""Parser rules: trusted handlers bound to configurable settings."""
from __future__ import annotations

from .base import (
    PatternBinding,
    Rule,
    RuleContext,
    RuleResult,
    StringBinding,
)
from .characters import CHARACTER_RULE
from .cooldowns import (
    CLAIM_TIMER_RULE,
    DK_READY_RULE,
    REACT_COOLDOWN_RULE,
    ROULETTE_LIMIT_RULE,
    RT_AVAILABLE_RULE,
    RT_COOLDOWN_RULE,
)
from .kakera import (
    CLAIM_EVENT_RULE,
    KAKERA_REACTION_RULE,
    KAKERALOOT_PAYOUT_RULE,
    KAKERALOOT_PROFILE_RULE,
    MARRIAGE_RULE,
    TRADE_GIFT_RULE,
    TRADE_GIVE_CONFIRM_RULE,
    TRADE_INSUFFICIENT_RULE,
    WISHLIST_RULE,
)
from .usernames import USERNAME_PATTERN_IDS, extract_usernames

__all__ = [
    "PatternBinding", "Rule", "RuleContext", "RuleResult", "StringBinding",
    "CHARACTER_RULE", "KAKERA_REACTION_RULE", "KAKERALOOT_PAYOUT_RULE",
    "KAKERALOOT_PROFILE_RULE", "TRADE_INSUFFICIENT_RULE", "TRADE_GIFT_RULE",
    "TRADE_GIVE_CONFIRM_RULE", "WISHLIST_RULE", "MARRIAGE_RULE",
    "ROULETTE_LIMIT_RULE", "RT_COOLDOWN_RULE", "RT_AVAILABLE_RULE",
    "DK_READY_RULE", "REACT_COOLDOWN_RULE", "CLAIM_TIMER_RULE",
    "CLAIM_EVENT_RULE",
    "USERNAME_PATTERN_IDS", "extract_usernames",
]
