"""Character rule (family: character) — roll/info embeds.

Multi-signal by design: the name comes from the embed author, series
and stats from the description, ownership from the footer, keys from
key entries inside the description, spheres from <:sw:..> markers.
Dataset evidence (rolls/marriage/wishlist/other embeds)::

    emb{author=<NAME> | desc=<SERIES> Claims: #N Likes: #N
        **N**<:kakera:E> | footer=Belongs to <U>}

The handler claims a message only when the embed actually looks like a
character card (a stats signal present) — markers alone never produce
a phantom character.
"""
from __future__ import annotations

from typing import Optional

from ..enums import MessageFamily
from ..models import CharacterInfo, parse_number
from .base import PatternBinding, Rule, RuleContext, RuleResult, StringBinding
from .keys import extract_keys

__all__ = ["CHARACTER_RULE", "handle_character"]


def handle_character(ctx: RuleContext) -> RuleResult:
    claims = ctx.groups("characters.claims_rank") \
        or ctx.groups("characters.claims_rank_alt")
    likes = ctx.groups("characters.likes_rank") \
        or ctx.groups("characters.likes_rank_alt")
    kakera = ctx.groups("characters.kakera_value")
    # a character card needs at least one stats signal
    if not (claims or likes or kakera):
        return RuleResult(matched=False)

    name_groups = ctx.groups("characters.name")
    series_groups = ctx.groups("characters.series")
    owner_groups = ctx.groups("characters.owned_by")

    character = CharacterInfo(
        name=_text(name_groups, "name"),
        series=_clean_series(_text(series_groups, "series")),
        claims_rank=_rank(claims, "claims"),
        likes_rank=_rank(likes, "likes"),
        kakera_value=_rank(kakera, "kakera"),
        keys=extract_keys(ctx.all_groups("keys.entry")),
        starwish_count=len(
            ctx.all_groups("characters.starwish_marker")),
        claimed_by=_text(owner_groups, "owner"),
        claimable=True if ctx.marker("characters.claimable_marker")
        else (False if owner_groups else None),
    )
    return RuleResult(matched=True, character=character)


def _text(groups: Optional[dict], key: str) -> Optional[str]:
    if not groups:
        return None
    value = groups.get(key)
    return value.strip() if isinstance(value, str) else None


def _clean_series(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    return value.strip().rstrip("|").strip() or None


def _rank(groups: Optional[dict], key: str) -> Optional[int]:
    if not groups:
        return None
    number = parse_number(groups.get(key))
    return number if isinstance(number, int) else None


CHARACTER_RULE = Rule(
    rule_id="character.embed",
    family=MessageFamily.CHARACTER,
    handler=handle_character,
    patterns=(
        PatternBinding("characters.name"),
        PatternBinding("characters.series"),
        PatternBinding("characters.claims_rank"),
        PatternBinding("characters.likes_rank"),
        PatternBinding("characters.claims_rank_alt"),
        PatternBinding("characters.likes_rank_alt"),
        PatternBinding("characters.kakera_value"),
        PatternBinding("characters.owned_by"),
        PatternBinding("keys.entry", multiplicity="all"),
        PatternBinding("characters.starwish_marker", multiplicity="all"),
    ),
    strings=(
        StringBinding("characters.roll_marker", required=False),
        StringBinding("characters.roll_marker_alt", required=False),
        StringBinding("characters.owned_marker", required=False),
        StringBinding("characters.claimable_marker", required=False),
    ),
    priority=10,
    description="Roll/info character embeds (name, series, ranks, "
                "kakera value, keys, spheres, ownership footer)",
)
