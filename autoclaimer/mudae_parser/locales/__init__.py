"""Locale registry — the future-locales extension point.

Only English ships today. Registering a new locale is additive:

    register_locale(LocalePack(code="fr", patterns=..., strings=...))

A pack must cover the SAME stable setting IDs the rules bind (rules are
locale-independent); missing IDs simply leave that locale without a
default for them and the manager reports it at load. Do not invent
families or targets in a locale pack — validation is shared.
"""
from __future__ import annotations

from typing import Dict

from .base import LocalePack
from .en import ENGLISH_LOCALE

__all__ = ["register_locale", "get_locale", "available_locales",
           "DEFAULT_LOCALE", "LocalePack"]

DEFAULT_LOCALE = "en"

_REGISTRY: Dict[str, LocalePack] = {ENGLISH_LOCALE.code: ENGLISH_LOCALE}


def register_locale(pack: LocalePack, *, replace: bool = False) -> None:
    """Add (or replace) a locale pack. Raises on duplicate unless
    ``replace`` is explicit — silent replacement would mask typos."""
    if pack.code in _REGISTRY and not replace:
        raise ValueError(f"locale {pack.code!r} is already registered")
    _REGISTRY[pack.code] = pack


def get_locale(code: str) -> LocalePack:
    from ..exceptions import UnsupportedLocaleError

    try:
        return _REGISTRY[code]
    except KeyError:
        known = ", ".join(sorted(_REGISTRY))
        raise UnsupportedLocaleError(
            f"no locale pack registered for {code!r} (available: {known})"
        ) from None


def available_locales() -> tuple:
    return tuple(sorted(_REGISTRY))
