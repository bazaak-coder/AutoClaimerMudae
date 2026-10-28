"""Key extraction helpers (family: keys).

Key entries live inside character embed descriptions in two dataset
forms; this module turns captured group dicts into typed
:class:`KeyInfo` models. Kept separate from the character rule so the
key grammar can evolve (tiers, per-key types) without touching embed
classification.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, Optional, Tuple

from ..models import KeyInfo, parse_number

__all__ = ["extract_keys"]


def extract_keys(group_dicts: Iterable[Dict[str, Any]]
                 ) -> Tuple[KeyInfo, ...]:
    """``keys.entry`` captures → typed KeyInfo tuple (sorted by number)."""
    out = []
    for groups in group_dicts:
        number = parse_number(groups.get("key_number"))
        if number is None or not isinstance(number, int):
            continue
        pct: Optional[int] = None
        pct_text = groups.get("value_pct")
        if pct_text is not None:
            try:
                pct = int(pct_text)
            except (TypeError, ValueError):
                pct = None
        key_type = groups.get("key_type")
        out.append(KeyInfo(
            key_number=number,
            key_type=key_type if isinstance(key_type, str) else None,
            value_pct=pct))
    return tuple(sorted(out, key=lambda key: key.key_number))
