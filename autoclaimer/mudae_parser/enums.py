"""Finite categories for the Mudae parser engine.

Every enum here is a stable, named vocabulary shared by the parser
rules, the settings models and the future dashboard catalog. Values are
plain strings so persisted settings and JSON representations stay
human-readable and stable across schema evolution.
"""
from __future__ import annotations

import enum


@enum.unique
class MessageFamily(enum.Enum):
    """What kind of Mudae message (or message group) was recognized.

    Doubles as the settings *group* for pattern/string catalog entries
    (characters, kakera, keys, cooldowns, usernames, ... classification).
    """

    CHARACTER = "character"          # roll/info embeds (name, series, kakera)
    KAKERA = "kakera"                # kakera/sphere reaction gains
    KEYS = "keys"                    # key entries inside character embeds
    COOLDOWN = "cooldown"            # roulette/rt/daily/react cooldowns
    TIMER = "timer"                  # claim/rolls timer status ($tu)
    CLAIM = "claim"                  # claim events (incl. custom messages)
    USERNAME = "username"            # username extraction helpers
    TRADE = "trade"                  # trade / gift / give confirmations
    WISHLIST = "wishlist"            # wishlist stock + refill timers
    KAKERALOOT = "kakeraloot"        # $kl payouts + loot profile embeds
    MARRIAGE = "marriage"            # marriage bonus messages
    CLASSIFICATION = "classification"  # generic Mudae-identifying markers
    UNKNOWN = "unknown"              # not recognized as any known family


@enum.unique
class TargetSource(enum.Enum):
    """Where a piece of text came from inside a Discord message.

    Rules bind each pattern/string setting to exactly one target so the
    same literal text in an embed title and a component label is never
    confused across rules (collision prevention by source).
    """

    CONTENT = "content"                    # plain message content
    EMBED_AUTHOR_NAME = "embed_author"     # embed author name
    EMBED_TITLE = "embed_title"            # embed title
    EMBED_DESCRIPTION = "embed_description"
    EMBED_FIELD_NAME = "embed_field_name"
    EMBED_FIELD_VALUE = "embed_field_value"
    EMBED_FOOTER = "embed_footer"          # embed footer text
    COMPONENT_LABEL = "component_label"    # button label / placeholder /
                                           # option label (best effort)


@enum.unique
class MatchMode(enum.Enum):
    """How a literal message string compares to candidate text.

    Literal only — a message string is NEVER interpreted as a regex.
    """

    EXACT = "exact"
    CONTAINS = "contains"
    PREFIX = "prefix"


@enum.unique
class CaseSensitivity(enum.Enum):
    """Explicit case behaviour for patterns and message strings."""

    SENSITIVE = "sensitive"
    INSENSITIVE = "insensitive"


@enum.unique
class SettingOrigin(enum.Enum):
    """Where the currently effective value of a setting comes from."""

    BUILTIN = "builtin"    # shipped default (version-controlled)
    OVERRIDE = "override"  # user-edited persisted value
    CUSTOM = "custom"      # explicitly added user rule (future)


@enum.unique
class Severity(enum.Enum):
    """Diagnostic severity levels."""

    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@enum.unique
class CooldownKind(enum.Enum):
    """Typed cooldown categories observed in the dataset."""

    ROULETTE = "roulette"          # roulette hourly-use limit
    RT = "rt"                      # $rt cooldown ($rtu)
    DAILY = "daily"                # $daily/$dk reset timer
    KAKERA_REACT = "kakera_react"  # kakera reaction power ($ku)
    CLAIM = "claim"                # claim availability / reset
    ROLLS_RESET = "rolls_reset"    # rolls reset timer
    REFILL = "refill"              # wishlist refill timer


@enum.unique
class KakeraEventKind(enum.Enum):
    """Typed kakera/sphere amount events observed in the dataset."""

    REACTION = "reaction"        # kakera button reaction gain ($k)
    SPHERE_CLICK = "sphere_click"  # ouro-sphere button gain (live-verified)
    CLAIM_GAIN = "claim_gain"    # kakera gained on claiming
    LOOT = "loot"                # $kl payout
    TRADE_DEFICIT = "trade_deficit"  # "You need an additional N"
    GIFT = "gift"                # "<@U> just gifted N"
    GIVE_CONFIRM = "give_confirm"    # "do you really want to give N"
    MARRIAGE_BONUS = "marriage_bonus"


__all__ = [
    "MessageFamily", "TargetSource", "MatchMode", "CaseSensitivity",
    "SettingOrigin", "Severity", "CooldownKind", "KakeraEventKind",
]
