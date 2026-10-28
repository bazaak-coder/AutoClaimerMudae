"""Cooldown + timer rules (families: cooldown, timer).

Every rule pairs an identifying literal marker (classification) with a
regex for the variable fields (extraction) — the two setting kinds
working together explicitly. Durations are typed: missing duration is
``None``, never zero, and never inferred from unrelated numbers.
"""
from __future__ import annotations

from typing import Optional

from ..enums import CooldownKind, MessageFamily
from ..models import (
    CooldownInfo,
    TimerInfo,
    parse_duration_minutes,
)
from .base import PatternBinding, Rule, RuleContext, RuleResult, StringBinding

__all__ = [
    "ROULETTE_LIMIT_RULE", "RT_COOLDOWN_RULE", "RT_AVAILABLE_RULE",
    "DK_READY_RULE", "REACT_COOLDOWN_RULE", "CLAIM_TIMER_RULE",
]


def _username(groups: Optional[dict]) -> Optional[str]:
    if not groups:
        return None
    value = groups.get("username")
    return value.strip() if isinstance(value, str) and value.strip() \
        else None


def handle_roulette_limit(ctx: RuleContext) -> RuleResult:
    groups = ctx.groups("cooldowns.roulette_limit")
    if not groups:
        return RuleResult(matched=False)
    limit: Optional[int] = None
    if groups.get("limit"):
        try:
            limit = int(groups["limit"])
        except ValueError:
            limit = None
    cooldown = CooldownInfo(
        kind=CooldownKind.ROULETTE,
        remaining=parse_duration_minutes(
            minutes=groups.get("minutes")),
        username=_username(groups),
        metadata={"uses_per_hour": limit, "tag": "($vote)"},
    )
    return RuleResult(matched=True, cooldowns=(cooldown,))


ROULETTE_LIMIT_RULE = Rule(
    rule_id="cooldown.roulette_limit",
    family=MessageFamily.COOLDOWN,
    handler=handle_roulette_limit,
    patterns=(PatternBinding("cooldowns.roulette_limit", required=True),),
    strings=(StringBinding("cooldowns.roulette_marker", required=True),),
    priority=40,
    description="Roulette hourly-use limit replies",
)


def handle_rt_cooldown(ctx: RuleContext) -> RuleResult:
    groups = ctx.groups("cooldowns.rt")
    if not groups:
        return RuleResult(matched=False)
    cooldown = CooldownInfo(
        kind=CooldownKind.RT,
        remaining=parse_duration_minutes(
            minutes=groups.get("minutes"),
            hours=groups.get("hours")),
        metadata={"tag": "($rtu)"},
    )
    return RuleResult(matched=True, cooldowns=(cooldown,))


RT_COOLDOWN_RULE = Rule(
    rule_id="cooldown.rt",
    family=MessageFamily.COOLDOWN,
    handler=handle_rt_cooldown,
    patterns=(PatternBinding("cooldowns.rt", required=True),),
    strings=(StringBinding("cooldowns.rt_marker", required=True),),
    priority=41,
    description="$rt cooldown replies ($rtu)",
)


def handle_rt_available(ctx: RuleContext) -> RuleResult:
    # exact-match marker only: ready state, no duration in the message
    cooldown = CooldownInfo(
        kind=CooldownKind.RT,
        remaining=None,  # missing ≠ zero: the message states no duration
        metadata={"available": True},
    )
    return RuleResult(matched=True, cooldowns=(cooldown,))


RT_AVAILABLE_RULE = Rule(
    rule_id="cooldown.rt_available",
    family=MessageFamily.COOLDOWN,
    handler=handle_rt_available,
    patterns=(),
    strings=(StringBinding("cooldowns.rt_available", required=True),),
    priority=42,
    description="$rt ready replies (exact match, no duration)",
)


def handle_dk_ready(ctx: RuleContext) -> RuleResult:
    cooldown = CooldownInfo(
        kind=CooldownKind.DAILY,
        remaining=None,  # ready state: no duration stated
        metadata={"available": True},
    )
    return RuleResult(matched=True, cooldowns=(cooldown,))


DK_READY_RULE = Rule(
    rule_id="cooldown.dk_ready",
    family=MessageFamily.COOLDOWN,
    handler=handle_dk_ready,
    patterns=(),
    strings=(StringBinding("cooldowns.dk_ready", required=True),),
    priority=46,
    description="$dk ready replies (exact match, no duration)",
)


def handle_react_cooldown(ctx: RuleContext) -> RuleResult:
    groups = ctx.groups("cooldowns.react")
    if not groups:
        return RuleResult(matched=False)
    cooldown = CooldownInfo(
        kind=CooldownKind.KAKERA_REACT,
        remaining=parse_duration_minutes(
            minutes=groups.get("minutes"),
            hours=groups.get("hours")),
        username=_username(groups),
        metadata={"tag": "($ku)"},
    )
    return RuleResult(matched=True, cooldowns=(cooldown,))


REACT_COOLDOWN_RULE = Rule(
    rule_id="cooldown.react",
    family=MessageFamily.COOLDOWN,
    handler=handle_react_cooldown,
    patterns=(PatternBinding("cooldowns.react", required=True),),
    strings=(StringBinding("cooldowns.react_marker", required=False),),
    priority=43,
    description="Kakera reaction cooldown replies ($ku)",
)


def handle_claim_timer(ctx: RuleContext) -> RuleResult:
    """$tu-style timer: one message, several typed cooldowns + a
    TimerInfo summary. Handles both the \"can claim now\" and the
    \"can't claim yet\" variants."""
    claim = ctx.groups("cooldowns.claim_timer")
    rolls = ctx.groups("cooldowns.rolls_line")
    react_state = ctx.groups("cooldowns.react_state")
    if not (claim or rolls or react_state):
        return RuleResult(matched=False)

    cooldowns = []
    can_claim: Optional[bool] = None
    claim_reset: Optional[object] = None
    if claim:
        can_claim = "can't" not in (claim.get("state") or "")
        remaining = parse_duration_minutes(
            minutes=claim.get("claim_minutes"),
            hours=claim.get("claim_hours"))
        claim_reset = remaining
        cooldowns.append(CooldownInfo(
            kind=CooldownKind.CLAIM,
            remaining=remaining,
            username=_username(claim),
            metadata={"available": can_claim}))

    rolls_reset = None
    rolls_left = None
    mk_rolls = None
    if rolls:
        try:
            rolls_left = int(rolls.get("rolls", 0))
        except (TypeError, ValueError):
            rolls_left = None
        try:
            mk_rolls = int(rolls.get("mk_rolls", 0))
        except (TypeError, ValueError):
            mk_rolls = None
        rolls_reset = parse_duration_minutes(
            minutes=rolls.get("reset_minutes"))
        cooldowns.append(CooldownInfo(
            kind=CooldownKind.ROLLS_RESET, remaining=rolls_reset,
            metadata={"rolls_left": rolls_left, "mk_rolls": mk_rolls}))

    can_react: Optional[bool] = None
    react_power: Optional[int] = None
    if react_state:
        state_text = react_state.get("react") or ""
        can_react = "can't" not in state_text
        if not can_react:
            cooldowns.append(CooldownInfo(
                kind=CooldownKind.KAKERA_REACT,
                remaining=parse_duration_minutes(
                    minutes=react_state.get("react_minutes"))))
        try:
            react_power = int(react_state.get("power", 0))
        except (TypeError, ValueError):
            react_power = None

    timer = TimerInfo(
        can_claim=can_claim,
        claim_resets_in=claim_reset,
        rolls_left=rolls_left,
        mk_rolls=mk_rolls,
        rolls_reset_in=rolls_reset,
        can_react=can_react,
        react_power_pct=react_power,
    )
    return RuleResult(matched=True, cooldowns=tuple(cooldowns),
                      timer=timer,
                      usernames=(_username(claim),) if _username(claim)
                      else ())


CLAIM_TIMER_RULE = Rule(
    rule_id="cooldown.claim_timer",
    family=MessageFamily.TIMER,
    handler=handle_claim_timer,
    patterns=(
        PatternBinding("cooldowns.claim_timer", required=True),
        PatternBinding("cooldowns.rolls_line"),
        PatternBinding("cooldowns.react_state"),
        PatternBinding("cooldowns.reaction_power"),
    ),
    strings=(
        StringBinding("cooldowns.claim_wait_marker", required=False),
        StringBinding("cooldowns.claim_ready_marker", required=False),
        StringBinding("cooldowns.timer_marker", required=False),
    ),
    priority=44,
    description="$tu-style claim/rolls timer replies",
)
