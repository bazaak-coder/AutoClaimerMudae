"""Built-in default MESSAGE STRINGS — English locale.

Literal Discord text used to *classify* messages before regex
extraction runs. Every string is verbatim from the
CuratedMudaeDataset (punctuation/spacing preserved); match modes were
chosen from the corpus:

  * EXACT   — fixed full-text replies with no variable part
              ("$rt is available!");
  * CONTAINS — stable identifying phrase inside a dynamic message
              ("the roulette is limited to" — the username, use count
              and minutes around it change);
  * PREFIX  — stable opening of a longer dynamic text.

These are IDENTIFICATION anchors, not extraction: the variable fields
around them are parsed by the regex patterns in ``patterns.py``.
"""
from __future__ import annotations

from ...enums import (
    MessageFamily,
    MatchMode,
    TargetSource,
)
from ...settings.models import MessageStringSetting as S

__all__ = ["DEFAULT_STRINGS"]

DEFAULT_STRINGS = (
    # ------------------------------------------------- cooldowns (misc.jsonl)
    S(
        setting_id="cooldowns.roulette_marker",
        name="Roulette limit marker",
        description="Identifies the roulette hourly-limit reply.",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        text="the roulette is limited to",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=40,
    ),
    S(
        setting_id="cooldowns.rt_marker",
        name="$rt cooldown marker",
        description="Identifies the $rt cooldown reply ($rtu).",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        text="The cooldown of $rt is not over.",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=41,
    ),
    S(
        setting_id="cooldowns.rt_available",
        name="$rt available",
        description="Fixed reply when $rt is ready again.",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        text="$rt is available!",
        match_mode=MatchMode.EXACT,
        case_sensitive=True,
        priority=42,
    ),
    S(
        setting_id="cooldowns.dk_ready",
        name="$dk ready",
        description="Fixed reply when $dk (daily kakera) is ready "
                    "(live-verified).",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        text="$dk is ready!",
        match_mode=MatchMode.EXACT,
        case_sensitive=True,
        priority=47,
    ),
    S(
        setting_id="cooldowns.react_marker",
        name="Reaction cooldown marker",
        description="Identifies the kakera reaction cooldown reply ($ku).",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        text="You can't react to kakera for",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=43,
    ),
    S(
        setting_id="cooldowns.claim_wait_marker",
        name="Claim wait marker",
        description="Identifies the \"can't claim yet\" state of a "
                    "$tu-style timer.",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        text="you can't claim for another",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=44,
    ),
    S(
        setting_id="cooldowns.claim_ready_marker",
        name="Claim ready marker",
        description="Identifies the \"can claim right now\" state of a "
                    "$tu-style timer.",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        text="__can__ claim right now!",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=45,
    ),
    S(
        setting_id="cooldowns.timer_marker",
        name="Timer reply marker",
        description="Identifies $tu-style timer replies via the rolls "
                    "inventory phrase.",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        text="rolls (+**",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=46,
    ),

    # ------------------------------------------- kakera (kakera_reaction.jsonl)
    S(
        setting_id="kakera.reaction_marker",
        name="Kakera reaction marker",
        description="Identifies kakera reaction gain replies via the "
                    "trailing ($k) tag.",
        family=MessageFamily.KAKERA,
        target=TargetSource.CONTENT,
        text="($k)",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=30,
    ),
    S(
        setting_id="kakera.reaction_prefix",
        name="Kakera reaction prefix",
        description="Stable opening of reaction replies (handles the "
                    "kakeraD \"turns into\" variant too, which contains "
                    "the same segment).",
        family=MessageFamily.KAKERA,
        target=TargetSource.CONTENT,
        text="(Free) **",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=31,
    ),

    # ------------------------------------------------------ characters
    S(
        setting_id="characters.roll_marker",
        name="Roll embed marker",
        description="Identifies roll/info character embeds via the "
                    "claims-rank label in the description.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        text="Claims:",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=10,
    ),
    S(
        setting_id="characters.roll_marker_alt",
        name="Roll embed marker (info layout)",
        description="Info-layout variant using \"Claim Rank:\".",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        text="Claim Rank:",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=11,
    ),
    S(
        setting_id="characters.owned_marker",
        name="Ownership footer marker",
        description="Prefix of the claim-ownership footer.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_FOOTER,
        text="Belongs to ",
        match_mode=MatchMode.PREFIX,
        case_sensitive=True,
        priority=12,
    ),
    S(
        setting_id="characters.claimable_marker",
        name="Claimable roll marker",
        description="Suffix marking an open roll (server-dependent "
                    "claim style; live-verified on a react-to-claim "
                    "server).",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        text="React with any emoji to claim!",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=13,
    ),

    # -------------------------------------------------------------- trade
    S(
        setting_id="trade.insufficient_marker",
        name="Deficit marker",
        description="Identifies the kakera-deficit trade reply.",
        family=MessageFamily.TRADE,
        target=TargetSource.CONTENT,
        text="You need an additional",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=60,
    ),
    S(
        setting_id="trade.gift_marker",
        name="Gift marker",
        description="Identifies gift notifications.",
        family=MessageFamily.TRADE,
        target=TargetSource.CONTENT,
        text="just gifted",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=61,
    ),
    S(
        setting_id="trade.give_confirm_marker",
        name="Give confirm marker",
        description="Identifies give-confirmation prompts.",
        family=MessageFamily.TRADE,
        target=TargetSource.CONTENT,
        text="do you really want to give",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=62,
    ),

    # ---------------------------------------------------------- kakeraloot
    S(
        setting_id="kakeraloot.payout_marker",
        name="Loot payout marker",
        description="Identifies $kl payout replies via the stacked-rolls "
                    "phrase.",
        family=MessageFamily.KAKERALOOT,
        target=TargetSource.CONTENT,
        text="rolls stacked",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=70,
    ),
    S(
        setting_id="kakeraloot.profile_marker",
        name="Loot profile marker",
        description="Identifies Kakeraloot profile embeds via the title "
                    "suffix.",
        family=MessageFamily.KAKERALOOT,
        target=TargetSource.EMBED_TITLE,
        text=" - Kakeraloots",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=71,
    ),

    # ---------------------------------------------------------- wishlist
    S(
        setting_id="wishlist.refill_marker",
        name="Refill marker",
        description="Identifies wishlist refill countdown replies.",
        family=MessageFamily.WISHLIST,
        target=TargetSource.CONTENT,
        text="min before the refill",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=80,
    ),

    # ---------------------------------------------------------- marriage
    S(
        setting_id="marriage.marker",
        name="Marriage marker",
        description="Identifies marriage notifications.",
        family=MessageFamily.MARRIAGE,
        target=TargetSource.CONTENT,
        text="are now married!",
        match_mode=MatchMode.CONTAINS,
        case_sensitive=True,
        priority=90,
    ),
)
