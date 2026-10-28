"""Kakera + kakeraloot + trade + marriage + wishlist rules.

Each rule is small and self-contained; the family split keeps an
administrator's override scoped (editing the loot payout pattern can
never affect character parsing — different settings, different rule).
"""
from __future__ import annotations

from typing import Optional

from ..enums import (
    CooldownKind,
    KakeraEventKind,
    MessageFamily,
)
from ..models import (
    KakeraEvent,
    KakeralootInfo,
    MarriageInfo,
    TradeInfo,
    WishlistInfo,
    parse_duration_minutes,
    parse_number,
)
from .base import PatternBinding, Rule, RuleContext, RuleResult, StringBinding

__all__ = [
    "KAKERA_REACTION_RULE", "KAKERALOOT_PAYOUT_RULE",
    "KAKERALOOT_PROFILE_RULE", "TRADE_INSUFFICIENT_RULE", "TRADE_GIFT_RULE",
    "TRADE_GIVE_CONFIRM_RULE", "WISHLIST_RULE", "MARRIAGE_RULE",
]


def _int(groups: Optional[dict], key: str) -> Optional[int]:
    if not groups:
        return None
    number = parse_number(groups.get(key))
    return number if isinstance(number, int) else None


# ------------------------------------------------------- kakera reactions
def handle_kakera_reaction(ctx: RuleContext) -> RuleResult:
    groups = ctx.groups("kakera.reaction_gain")
    sphere_groups = ctx.groups("kakera.sphere_click")
    if not groups and not sphere_groups:
        return RuleResult(matched=False)
    events = []
    usernames = []
    if groups:
        username = (groups.get("username") or "").strip() or None
        events.append(KakeraEvent(
            kind=KakeraEventKind.REACTION,
            amount=_int(groups, "amount"),
            username=username,
            spheres=_int(ctx.groups("kakera.spheres_gained"),
                         "spheres"),
            metadata={"tag": "($k)"},
        ))
        if username:
            usernames.append(username)
    if sphere_groups:
        sphere_username = (sphere_groups.get("username") or "").strip() \
            or None
        events.append(KakeraEvent(
            kind=KakeraEventKind.SPHERE_CLICK,
            amount=_int(sphere_groups, "spheres"),
            username=sphere_username,
        ))
        if sphere_username:
            usernames.append(sphere_username)
    return RuleResult(matched=True, kakera_events=tuple(events),
                      usernames=tuple(usernames))


KAKERA_REACTION_RULE = Rule(
    rule_id="kakera.reaction",
    family=MessageFamily.KAKERA,
    handler=handle_kakera_reaction,
    patterns=(
        # either/or: kakera reaction OR sphere click (handler decides;
        # neither binding is individually required)
        PatternBinding("kakera.reaction_gain"),
        PatternBinding("kakera.sphere_click"),
        PatternBinding("kakera.spheres_gained"),
    ),
    # no string-marker gate: paid reactions and sphere clicks carry no
    # ($k)/(Free) text — the distinctive emoji patterns classify alone
    strings=(),
    priority=30,
    description="Kakera/sphere reaction gains ($k, sphere buttons)",
)


# ----------------------------------------------------------------- claim
def handle_claim_event(ctx: RuleContext) -> RuleResult:
    groups = ctx.groups("claim.added_to_collection")
    if not groups:
        return RuleResult(matched=False)
    amount = _int(groups, "kakera")
    return RuleResult(
        matched=True,
        kakera_events=(KakeraEvent(
            kind=KakeraEventKind.CLAIM_GAIN,
            amount=amount,
            username=(groups.get("username") or "").strip() or None,
            metadata={"character":
                      (groups.get("character") or "").strip() or None},
        ),) if amount is not None else ())


CLAIM_EVENT_RULE = Rule(
    rule_id="claim.added_to_collection",
    family=MessageFamily.CLAIM,
    handler=handle_claim_event,
    patterns=(PatternBinding("claim.added_to_collection",
                             required=True),),
    strings=(),
    priority=25,
    description="Claim events (custom claim-message servers)",
)


# --------------------------------------------------------- kakeraloot
def handle_kakeraloot_payout(ctx: RuleContext) -> RuleResult:
    gain = ctx.groups("kakeraloot.payout_kakera")
    stacked = ctx.groups("kakeraloot.rolls_stacked")
    if not (gain or stacked):
        return RuleResult(matched=False)
    amount = _int(gain, "amount") if gain else None
    stacked_value = None
    stock_value = None
    if stacked:
        parsed_stacked = parse_number(stacked.get("stacked"))
        parsed_stock = parse_number(stacked.get("stock"))
        stacked_value = (float(parsed_stacked)
                         if isinstance(parsed_stacked, float) else
                         parsed_stacked)
        stock_value = (float(parsed_stock)
                       if isinstance(parsed_stock, float) else parsed_stock)
    info = KakeralootInfo(
        kakera_gained=amount,
        rolls_stacked=stacked_value,
        stock=stock_value,
    )
    event = KakeraEvent(
        kind=KakeraEventKind.LOOT, amount=amount)
    return RuleResult(matched=True, kakeraloot=info,
                      kakera_events=(event,) if amount is not None else ())


KAKERALOOT_PAYOUT_RULE = Rule(
    rule_id="kakeraloot.payout",
    family=MessageFamily.KAKERALOOT,
    handler=handle_kakeraloot_payout,
    patterns=(
        PatternBinding("kakeraloot.payout_kakera", required=True),
        PatternBinding("kakeraloot.rolls_stacked"),
    ),
    strings=(StringBinding("kakeraloot.payout_marker", required=False),),
    priority=70,
    description="$kl payout lines (kakera gained, rolls stacked, stock)",
)


def handle_kakeraloot_profile(ctx: RuleContext) -> RuleResult:
    groups = ctx.groups("usernames.kakeraloot_title")
    if not groups:
        return RuleResult(matched=False)
    info = KakeralootInfo(
        username=(groups.get("username") or "").strip() or None)
    return RuleResult(matched=True, kakeraloot=info,
                      usernames=(info.username,) if info.username else ())


KAKERALOOT_PROFILE_RULE = Rule(
    rule_id="kakeraloot.profile",
    family=MessageFamily.KAKERALOOT,
    handler=handle_kakeraloot_profile,
    patterns=(PatternBinding("usernames.kakeraloot_title", required=True),),
    strings=(StringBinding("kakeraloot.profile_marker", required=True),),
    priority=71,
    description="Kakeraloot profile embeds (title username)",
)


# -------------------------------------------------------------- trade
def handle_trade_insufficient(ctx: RuleContext) -> RuleResult:
    groups = ctx.groups("trade.insufficient")
    if not groups:
        return RuleResult(matched=False)
    amount = _int(groups, "amount")
    trade = TradeInfo(kind=KakeraEventKind.TRADE_DEFICIT, amount=amount)
    return RuleResult(
        matched=True, trade=trade,
        kakera_events=(KakeraEvent(
            kind=KakeraEventKind.TRADE_DEFICIT, amount=amount),))


TRADE_INSUFFICIENT_RULE = Rule(
    rule_id="trade.insufficient",
    family=MessageFamily.TRADE,
    handler=handle_trade_insufficient,
    patterns=(PatternBinding("trade.insufficient", required=True),),
    strings=(StringBinding("trade.insufficient_marker", required=True),),
    priority=60,
    description="Kakera deficit replies",
)


def handle_trade_gift(ctx: RuleContext) -> RuleResult:
    groups = ctx.groups("trade.gift")
    if not groups:
        return RuleResult(matched=False)
    trade = TradeInfo(
        kind=KakeraEventKind.GIFT,
        amount=_int(groups, "amount"),
        from_user_id=int(groups["from_id"]) if groups.get("from_id")
        else None,
        to_user_id=int(groups["to_id"]) if groups.get("to_id") else None,
    )
    return RuleResult(
        matched=True, trade=trade,
        kakera_events=(KakeraEvent(
            kind=KakeraEventKind.GIFT, amount=trade.amount),))


TRADE_GIFT_RULE = Rule(
    rule_id="trade.gift",
    family=MessageFamily.TRADE,
    handler=handle_trade_gift,
    patterns=(PatternBinding("trade.gift", required=True),),
    strings=(StringBinding("trade.gift_marker", required=True),),
    priority=61,
    description="Gift notifications",
)


def handle_trade_give_confirm(ctx: RuleContext) -> RuleResult:
    groups = ctx.groups("trade.give_confirm")
    if not groups:
        return RuleResult(matched=False)
    trade = TradeInfo(
        kind=KakeraEventKind.GIVE_CONFIRM,
        amount=_int(groups, "amount"),
        username=(groups.get("username") or "").strip() or None,
    )
    return RuleResult(
        matched=True, trade=trade,
        usernames=(trade.username,) if trade.username else ())


TRADE_GIVE_CONFIRM_RULE = Rule(
    rule_id="trade.give_confirm",
    family=MessageFamily.TRADE,
    handler=handle_trade_give_confirm,
    patterns=(PatternBinding("trade.give_confirm", required=True),),
    strings=(StringBinding("trade.give_confirm_marker", required=True),),
    priority=62,
    description="Give-confirmation prompts (y/n)",
)


# ------------------------------------------------------------ wishlist
def handle_wishlist(ctx: RuleContext) -> RuleResult:
    stock_groups = ctx.groups("wishlist.stock")
    refill_groups = (ctx.groups("wishlist.refill")
                     or ctx.groups("wishlist.refill_alt"))
    if not (stock_groups or refill_groups):
        return RuleResult(matched=False)
    info = WishlistInfo(
        stock=_int(stock_groups, "stock") if stock_groups else None,
        refill_in=parse_duration_minutes(
            minutes=refill_groups.get("minutes") if refill_groups else None,
            hours=refill_groups.get("hours") if refill_groups else None,
        ),
    )
    return RuleResult(matched=True, wishlist=info)


WISHLIST_RULE = Rule(
    rule_id="wishlist.refill",
    family=MessageFamily.WISHLIST,
    handler=handle_wishlist,
    patterns=(
        PatternBinding("wishlist.stock"),
        PatternBinding("wishlist.refill", required=True),
        PatternBinding("wishlist.refill_alt"),
    ),
    strings=(StringBinding("wishlist.refill_marker", required=True),),
    priority=80,
    description="Wishlist stock + refill countdown",
)


# ------------------------------------------------------------- marriage
def handle_marriage(ctx: RuleContext) -> RuleResult:
    pair = ctx.groups("marriage.pair")
    bonus = ctx.groups("marriage.bonus")
    if not pair and not bonus:
        return RuleResult(matched=False)
    spheres = _int(ctx.groups("kakera.spheres_gained"), "spheres")
    info = MarriageInfo(
        user_a=(pair.get("user_a") or "").strip() or None
        if pair else None,
        character=(pair.get("character") or "").strip() or None
        if pair else None,
        kakera_bonus=_int(bonus, "amount") if bonus else None,
        tier=(bonus.get("tier") or "").strip() or None if bonus else None,
        spheres=spheres,
    )
    events = []
    if info.kakera_bonus is not None:
        events.append(KakeraEvent(
            kind=KakeraEventKind.MARRIAGE_BONUS,
            amount=info.kakera_bonus,
            metadata={"tier": info.tier} if info.tier else {}))
    return RuleResult(matched=True, marriage=info,
                      kakera_events=tuple(events))


MARRIAGE_RULE = Rule(
    rule_id="marriage.event",
    family=MessageFamily.MARRIAGE,
    handler=handle_marriage,
    patterns=(
        PatternBinding("marriage.pair"),
        PatternBinding("marriage.bonus", required=True),
        PatternBinding("kakera.spheres_gained"),
    ),
    strings=(StringBinding("marriage.marker", required=True),),
    priority=90,
    description="Marriage notifications + bonuses",
)
