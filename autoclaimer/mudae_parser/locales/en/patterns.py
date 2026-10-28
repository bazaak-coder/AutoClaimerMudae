"""Built-in default REGEX patterns — English locale.

Every expression here is grounded in the supplied CuratedMudaeDataset
(file noted per group; verbatim example_raw evidence in the dataset
tests). Named capture groups are the extraction contract — the
validator enforces their presence on every override.

Conventions:
  * numbers always match Mudae's ``[\\d,]+`` (comma separators), plus
    optional decimals where the dataset shows them;
  * custom emoji are anchored by name AND id (``<:kakera:4698..>``)
    where the id is stable across the corpus, by name alone where
    variants exist (``<:kakeraP:..>`` vs ``<:kakeraP:1097..>``);
  * ``^`` anchors are safe: message content sources are single strings
    matched with finditer against the full text.
"""
from __future__ import annotations

from ...enums import (
    CaseSensitivity,
    MessageFamily,
    TargetSource,
)
from ...settings.models import RegexPatternSetting as P

__all__ = ["DEFAULT_PATTERNS"]

# ------------------------------------------------- characters (rolls.jsonl)
# desc: "<SERIES> | Claims: #11,225 | Likes: #24,770 | **8,658**<:kakera:..>"
# name lives in the embed author (dataset skeleton: author=<NAME>),
# ownership in the footer ("Belongs to <U>"). Series may be followed by
# sphere emojis (<:sw:..>) and ends at the first | or newline.
DEFAULT_PATTERNS = (
    P(
        setting_id="characters.name",
        name="Character name",
        description="Waifu/husbando name from a roll embed's author field.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_AUTHOR_NAME,
        expression=r"^(?P<name>.+)$",
        required_groups=("name",),
        priority=10,
    ),
    P(
        setting_id="characters.series",
        name="Series",
        description="Series line at the start of a roll embed description "
                    "(before the stats, optional sphere emojis).",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        expression=r"^(?P<series>.+?)(?:\s*<:[A-Za-z0-9_]+:\d+>)*"
                   r"\s*(?:\||\n)",
        required_groups=("series",),
        priority=11,
    ),
    P(
        setting_id="characters.claims_rank",
        name="Claims rank",
        description="\"Claims: #N\" rank in a roll embed description.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        expression=r"Claims:\s*#(?P<claims>[\d,]+)",
        required_groups=("claims",),
        priority=12,
    ),
    P(
        setting_id="characters.likes_rank",
        name="Likes rank",
        description="\"Likes: #N\" rank in a roll embed description.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        expression=r"Likes:\s*#(?P<likes>[\d,]+)",
        required_groups=("likes",),
        priority=13,
    ),
    P(
        setting_id="characters.claims_rank_alt",
        name="Claims rank (info layout)",
        description="\"Claim Rank: #N\" variant used by $im-style embeds.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        expression=r"Claim Rank:\s*#(?P<claims>[\d,]+)",
        required_groups=("claims",),
        priority=14,
    ),
    P(
        setting_id="characters.likes_rank_alt",
        name="Likes rank (info layout)",
        description="\"Like Rank: #N\" variant used by $im-style embeds.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        expression=r"Like Rank:\s*#(?P<likes>[\d,]+)",
        required_groups=("likes",),
        priority=15,
    ),
    P(
        setting_id="characters.kakera_value",
        name="Kakera value",
        description="Base kakera value \"**8,658**<:kakera:..>\" in a "
                    "character embed.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        expression=r"\*{1,3}(?P<kakera>[\d,]+(?:\.\d+)?)\*{0,3}"
                   r"\s*<:kakera:\d+>",
        required_groups=("kakera",),
        priority=16,
    ),
    P(
        setting_id="characters.owned_by",
        name="Owned by (footer)",
        description="\"Belongs to <U>\" claim-ownership footer of a "
                    "character embed.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_FOOTER,
        expression=r"^Belongs to (?P<owner>.+)$",
        required_groups=("owner",),
        priority=17,
    ),
    P(
        setting_id="characters.starwish_marker",
        name="Starwish marker",
        description="Starwish indicator (<:sw:..> — $sw = $starwish, "
                    "wiki-verified abbreviation) next to the series; "
                    "counted per match (classification/counting only — "
                    "no capture groups required). Spheres are the "
                    "separate <:sp:..> currency.",
        family=MessageFamily.CHARACTER,
        target=TargetSource.EMBED_DESCRIPTION,
        expression=r"<:sw:\d+>",
        required_groups=(),
        priority=18,
    ),

    # ------------------------------------------------------ keys (rolls.jsonl)
    # "<:chaoskey:690110264166842421> (**26**) +5% kakera value"  (rolls)
    # "<:chaoskey:690110264166842421>  (1,476)"                    ($im info)
    P(
        setting_id="keys.entry",
        name="Key entry",
        description="One key entry in a character embed. Wiki-verified "
                    "tiers (mudae.fandom.com/wiki/Keys): bronze (1-2), "
                    "silver (3-5), gold (6-9), chaos (10+).",
        family=MessageFamily.KEYS,
        target=TargetSource.EMBED_DESCRIPTION,
        expression=r"<:(?P<key_type>bronze|silver|gold|chaos)key:\d+>"
                   r"\s*\(\*{0,2}(?P<key_number>[\d,]+)\*{0,2}\)"
                   r"(?:\s*\+(?P<value_pct>\d+)%\s*kakera value)?",
        required_groups=("key_number",),
        optional_groups=("value_pct", "key_type"),
        priority=20,
    ),

    # ------------------------------------------- kakera reactions
    # (kakera_reaction.jsonl)
    # "<:kakeraP:609264156347990016>(Free) **htbao7 +700** ($k)"
    # "<:kakeraP:1097914822462545951> (Free) **rubenpfdez05 +750** ($k)"
    P(
        setting_id="kakera.reaction_gain",
        name="Kakera reaction gain",
        description="Kakera reaction claim: \"<:kakeraP:..>(Free) "
                    "<user> +N\" — any kakera button colour "
                    "(kakeraP/Y/O/T/...), with or without the (Free) "
                    "marker (paid reactions omit it — live-verified).",
        family=MessageFamily.KAKERA,
        target=TargetSource.CONTENT,
        expression=r"<:kakera[A-Za-z]*:\d+>\s*(?:\(Free\)\s*)?"
                   r"\*\*(?P<username>.+?)\s*\+(?P<amount>[\d,]+)\*\*",
        required_groups=("username", "amount"),
        priority=30,
    ),
    P(
        setting_id="kakera.sphere_click",
        name="Sphere button gain",
        description="Sphere reaction gain: \"<:spT:..> <user> +N "
                    "(1/15)\" (ouro sphere buttons — live-verified).",
        family=MessageFamily.KAKERA,
        target=TargetSource.CONTENT,
        expression=r"<:sp[A-Za-z]*:\d+>\s*\*\*(?P<username>.+?)"
                   r"\s*\+(?P<spheres>[\d,]+)\*\*",
        required_groups=("username", "spheres"),
        priority=32,
    ),
    P(
        setting_id="kakera.spheres_gained",
        name="Spheres gained",
        description="Sphere amount riding on a kakera event "
                    "(\"**+24** <:sp:..>\").",
        family=MessageFamily.KAKERA,
        target=TargetSource.CONTENT,
        expression=r"\*\*\+(?P<spheres>[\d,]+)\*\*\s*<:sp:\d+>",
        required_groups=("spheres",),
        priority=31,
    ),

    # ------------------------------------------------ cooldowns (misc/timers)
    P(
        setting_id="cooldowns.roulette_limit",
        name="Roulette hourly limit",
        description="\"<U>, the roulette is limited to N uses per hour. "
                    "**N** min left.\"",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        expression=r"^\*\*(?P<username>[^*\n]+)\*\*, the roulette is "
                   r"limited to (?P<limit>\d+) uses per hour\.\s*"
                   r"\*\*(?P<minutes>\d+)\*\* min left\.",
        required_groups=("username", "minutes"),
        optional_groups=("limit",),
        priority=40,
    ),
    P(
        setting_id="cooldowns.rt",
        name="$rt cooldown",
        description="\"The cooldown of $rt is not over. Time left: "
                    "**[Nh ]N** min. ($rtu)\".",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        expression=r"The cooldown of \$rt is not over\. Time left: "
                   r"\*\*(?:(?P<hours>\d+)h\s*)?(?P<minutes>\d+)\*\* min\.",
        required_groups=("minutes",),
        optional_groups=("hours",),
        priority=41,
    ),
    P(
        setting_id="cooldowns.react",
        name="Kakera reaction cooldown",
        description="\"<U>, You can't react to kakera for **[Nh ]N** "
                    "min. ($ku)\".",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        expression=r"^\*\*(?P<username>[^*\n]+)\*\*, You can't react to "
                   r"kakera for \*\*(?:(?P<hours>\d+)h\s*)?"
                   r"(?P<minutes>\d+)\*\* min\.",
        required_groups=("username", "minutes"),
        optional_groups=("hours",),
        priority=42,
    ),
    P(
        setting_id="cooldowns.claim_timer",
        name="Claim timer",
        description="$tu claim status: \"you can't claim for another "
                    "\"**[Nh ]N** min\" or \"you __can__ claim right now! "
                    "The next claim reset is in **[Nh ]N** min\".",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        expression=r"^\*\*(?P<username>[^*\n]+)\*\*, you (?P<state>"
                   r"can't claim for another|__can__ claim right now!"
                   r"(?: The next claim reset is in)?"
                   r")\s*\*{0,2}(?:(?P<claim_hours>\d+)h\s*)?"
                   r"(?P<claim_minutes>\d+)\*{0,2} min\.?",
        required_groups=("username", "state", "claim_minutes"),
        optional_groups=("claim_hours",),
        priority=43,
    ),
    P(
        setting_id="cooldowns.rolls_line",
        name="Rolls line",
        description="Rolls inventory of a $tu timer: \"You have **N** "
                    "rolls (+**N** $mk) left. Next rolls reset in "
                    "**N** min.\"",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        expression=r"You have \*\*(?P<rolls>\d+)\*\* rolls"
                   r"\s*\(\+\*\*(?P<mk_rolls>\d+)\*\* \$mk\) left\."
                   r"\s*Next rolls reset in \*\*(?P<reset_minutes>\d+)"
                   r"\*\* min\.",
        required_groups=("rolls", "reset_minutes"),
        optional_groups=("mk_rolls",),
        priority=44,
    ),
    P(
        setting_id="cooldowns.react_state",
        name="Reaction state",
        description="Reaction-power line of a $tu timer: \"You __can__ "
                    "react to kakera right now!\" / \"You can't react to "
                    "kakera for **N** min.\" + \"Power: **N%**\".",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        expression=r"You (?P<react>__can__ react to kakera right now!|"
                   r"can't react to kakera for \*\*(?:\d+h\s*)?"
                   r"(?P<react_minutes>\d+)\*\* min)\.?\s*"
                   r"Power: \*\*(?P<power>\d+)%\*\*",
        required_groups=("react", "power"),
        optional_groups=("react_minutes",),
        priority=45,
    ),

    # -------------------------------------------------------- timers extras
    P(
        setting_id="cooldowns.reaction_power",
        name="Reaction power",
        description="Standalone reaction-power mention "
                    "(\"Power: **N%**\").",
        family=MessageFamily.COOLDOWN,
        target=TargetSource.CONTENT,
        expression=r"Power: \*\*(?P<power>\d+)%\*\*",
        required_groups=("power",),
        priority=46,
    ),

    # -------------------------------------------------------- usernames
    P(
        setting_id="usernames.leading_bold",
        name="Leading username",
        description="Leading bold username of a Mudae reply: "
                    "\"**<U>, ...\".",
        family=MessageFamily.USERNAME,
        target=TargetSource.CONTENT,
        expression=r"^\*\*(?P<username>[^*\n]{1,64})\*\*,",
        required_groups=("username",),
        priority=50,
    ),
    P(
        setting_id="usernames.kakeraloot_title",
        name="Kakeraloot profile username",
        description="Username in a Kakeraloot profile embed title: "
                    "\"<U> - Kakeraloots\".",
        family=MessageFamily.USERNAME,
        target=TargetSource.EMBED_TITLE,
        expression=r"^(?P<username>.+) - Kakeraloots$",
        required_groups=("username",),
        priority=51,
    ),

    # -------------------------------------------------------------- trade
    P(
        setting_id="claim.added_to_collection",
        name="Claim (custom claim message)",
        description="Custom-claim-message form (live-verified, "
                    "$setclaimmessage servers): \"「<U>」added「<char>」"
                    "into the collection.\" + kakera gained.",
        family=MessageFamily.CLAIM,
        target=TargetSource.CONTENT,
        expression=r"「\*\*(?P<username>[^*」\n]+)\*\*」added「\*\*"
                   r"(?P<character>[^*」\n]+)\*\*」into the collection"
                   r"(?:.*\*\*\+(?P<kakera>[\d,]+)\*\*"
                   r"<:kakera:\d+>)?",
        required_groups=("username", "character"),
        optional_groups=("kakera",),
        priority=25,
    ),
    P(
        setting_id="trade.insufficient",
        name="Kakera deficit",
        description="\"You need an additional **N**<:kakera:..>!\".",
        family=MessageFamily.TRADE,
        target=TargetSource.CONTENT,
        expression=r"You need an additional \*\*(?P<amount>[\d,]+)\*\*"
                   r"\s*<:kakera:\d+>",
        required_groups=("amount",),
        priority=60,
    ),
    P(
        setting_id="trade.gift",
        name="Gift notification",
        description="\"<@U> just gifted **N**<:kakera:..>to <@U>\".",
        family=MessageFamily.TRADE,
        target=TargetSource.CONTENT,
        expression=r"<@(?P<from_id>\d+)> just gifted "
                   r"\*\*(?P<amount>[\d,]+)\*\*\s*<:(?:kakera|sp):\d+>"
                   r"\s*to <@(?P<to_id>\d+)>",
        required_groups=("from_id", "to_id"),
        optional_groups=("amount",),
        priority=61,
    ),
    P(
        setting_id="trade.give_confirm",
        name="Give confirmation",
        description="\"<U>, do you really want to give **N <:kakera:..> "
                    "**? (y/n/yes/no)\".",
        family=MessageFamily.TRADE,
        target=TargetSource.CONTENT,
        expression=r"^\*\*(?P<username>[^*\n]+)\*\*, do you really want "
                   r"to give \*\*(?P<amount>[\d,]+)\s*"
                   r"<:(?:kakera|sp):\d+>\s*\*\*\? \(y/n/yes/no\)$",
        required_groups=("username", "amount"),
        priority=62,
    ),

    # ---------------------------------------------------------- kakeraloot
    P(
        setting_id="kakeraloot.payout_kakera",
        name="Loot payout kakera",
        description="\"<:morekakera:..> **+N** kakera\" payout line.",
        family=MessageFamily.KAKERALOOT,
        target=TargetSource.CONTENT,
        expression=r"<:morekakera:\d+>\s*\*\*\+(?P<amount>[\d,]+)\*\*"
                   r" kakera",
        required_groups=("amount",),
        priority=70,
    ),
    P(
        setting_id="kakeraloot.rolls_stacked",
        name="Rolls stacked",
        description="\"<:rollstack:..> **+N** rolls stacked (Stock: N)\".",
        family=MessageFamily.KAKERALOOT,
        target=TargetSource.CONTENT,
        expression=r"<:rollstack:\d+>\s*\*\*\+(?P<stacked>[\d,.]+)\*\*"
                   r" rolls stacked \(Stock: (?P<stock>[\d,.]+)\)",
        required_groups=("stacked",),
        optional_groups=("stock",),
        priority=71,
    ),

    # ---------------------------------------------------------- wishlist
    P(
        setting_id="wishlist.stock",
        name="Wishlist stock",
        description="\"Stock: **N** <:sp:..>\" line of a wishlist "
                    "overview.",
        family=MessageFamily.WISHLIST,
        target=TargetSource.CONTENT,
        expression=r"Stock: \*\*(?P<stock>[\d,]+)\*\* <:sp:\d+>",
        required_groups=("stock",),
        priority=80,
    ),
    P(
        setting_id="wishlist.refill",
        name="Refill timer",
        description="\"**[Nh ]N** min before the refill\".",
        family=MessageFamily.WISHLIST,
        target=TargetSource.CONTENT,
        expression=r"\*\*(?:(?P<hours>\d+)h\s*)?(?P<minutes>\d+)\*\*"
                   r" min before the refill",
        required_groups=("minutes",),
        optional_groups=("hours",),
        priority=81,
    ),
    P(
        setting_id="wishlist.refill_alt",
        name="Refill timer ($oh variant)",
        description="\"Time to wait before the refill: **[Nh ]N** "
                    "min.\" (insufficient-$oh reply).",
        family=MessageFamily.WISHLIST,
        target=TargetSource.CONTENT,
        expression=r"before the refill: \*\*(?:(?P<hours>\d+)h\s*)?"
                   r"(?P<minutes>\d+)\*\* min",
        required_groups=("minutes",),
        optional_groups=("hours",),
        priority=82,
    ),

    # ---------------------------------------------------------- marriage
    P(
        setting_id="marriage.pair",
        name="Marriage pair",
        description="\"**<U>** and **<character>** are now married!\".",
        family=MessageFamily.MARRIAGE,
        target=TargetSource.CONTENT,
        expression=r"\*{0,2}(?P<user_a>[^*\n]+?)\*{0,2} and \*\*"
                   r"(?P<character>[^*\n]+)\*\* are now married!",
        required_groups=("user_a", "character"),
        priority=90,
    ),
    P(
        setting_id="marriage.bonus",
        name="Marriage bonus",
        description="\"**+N**<:kakera:..>(Emerald IV bonus)\" line.",
        family=MessageFamily.MARRIAGE,
        target=TargetSource.CONTENT,
        expression=r"\*\*\+(?P<amount>[\d,]+)\*\*\s*<:kakera:\d+>"
                   r"\((?P<tier>[A-Za-z]+ [IVX]+) bonus\)",
        required_groups=("amount", "tier"),
        priority=91,
    ),
)
