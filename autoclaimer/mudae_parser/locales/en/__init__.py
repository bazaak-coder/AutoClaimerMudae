"""English locale pack — default patterns and message strings."""
from __future__ import annotations

from ..base import LocalePack
from .messages import DEFAULT_STRINGS
from .patterns import DEFAULT_PATTERNS

__all__ = ["ENGLISH_LOCALE", "LOCALE_CODE"]

LOCALE_CODE = "en"

ENGLISH_LOCALE = LocalePack(
    code=LOCALE_CODE,
    patterns=DEFAULT_PATTERNS,
    strings=DEFAULT_STRINGS,
)
