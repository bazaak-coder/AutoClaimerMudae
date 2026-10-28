"""Input normalization: Discord message objects → typed text sources.

The parser never touches a Discord library class. A message is any
duck-typed object with optional ``content`` / ``embeds`` /
``components`` attributes (discord.py-self messages satisfy this
shape); a plain string is accepted as bare ``CONTENT``. Each piece of
extractable text becomes a :class:`TextSource` carrying its
:class:`TargetSource` kind and a stable ``path`` (e.g.
``embeds[0].description``) so rules bind patterns to the right source
and identical text in unrelated fields can never collide.

Component handling is deliberately conservative: only structures that
actually expose label-ish text (button ``label``, select ``placeholder``,
option ``label``) are harvested, everything else is skipped silently —
the dataset shows Mudae's buttons are emoji-only (``kakeraY``, pager
arrows), so components mostly contribute nothing, and that is fine.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, List, Optional, Sequence, Tuple

from .diagnostics import CODE_OVERSIZED_INPUT, CODE_SKIPPED_SOURCE, Diagnostics
from .enums import TargetSource

#: hard bound on total harvested text per message (regex safety). A
#: generous Mudae message is a few KB (embed limit is 4096 chars/field);
#: anything past this bound is almost certainly abuse, not Mudae.
MAX_MESSAGE_TEXT_CHARS = 200_000

#: hard bound per single source value (Discord embeds cap at 4096; the
#: bound leaves headroom for library quirks without inviting abuse).
MAX_SOURCE_CHARS = 8_192

__all__ = [
    "TextSource", "NormalizedMessage", "normalize_message",
    "MAX_MESSAGE_TEXT_CHARS", "MAX_SOURCE_CHARS",
]


@dataclass(frozen=True)
class TextSource:
    """One harvestable text value with its origin."""

    value: str
    source: TargetSource
    #: stable location, e.g. ``content`` / ``embeds[0].description`` /
    #: ``embeds[0].fields[2].value`` / ``components[0].1.label``
    path: str

    def clip(self) -> "TextSource":
        if len(self.value) <= MAX_SOURCE_CHARS:
            return self
        return TextSource(
            self.value[:MAX_SOURCE_CHARS], self.source, self.path)


def _clean(value: Any) -> Optional[str]:
    """Coerce a candidate field to stripped text (None when not text)."""
    if not isinstance(value, str):
        return None
    return value if value.strip() else None


def _harvest_embed(embed: Any, index: int, out: List[TextSource]) -> None:
    """Harvest one embed, duck-typed across BOTH observed shapes:

    * library messages (discord.py-self): nested ``author.name`` /
      ``footer.text`` objects;
    * the framework's rich DTO (adapters.model.DiscordEmbed): FLAT
      ``author_name`` / ``footer_text`` string attributes.
    """
    base = f"embeds[{index}]"
    # author name: nested first, then flat
    author = getattr(embed, "author", None)
    if author is not None and not isinstance(author, str):
        text = _clean(getattr(author, "name", None))
        if text is not None:
            out.append(TextSource(
                text, TargetSource.EMBED_AUTHOR_NAME,
                f"{base}.author.name"))
    else:
        text = _clean(getattr(embed, "author_name", None))
        if text is not None:
            out.append(TextSource(
                text, TargetSource.EMBED_AUTHOR_NAME,
                f"{base}.author_name"))
    for attr, source in (
        ("title", TargetSource.EMBED_TITLE),
        ("description", TargetSource.EMBED_DESCRIPTION),
    ):
        text = _clean(getattr(embed, attr, None))
        if text is not None:
            out.append(TextSource(text, source, f"{base}.{attr}"))
    fields = getattr(embed, "fields", None) or ()
    for findex, field_obj in enumerate(fields):
        for attr, source in (
            ("name", TargetSource.EMBED_FIELD_NAME),
            ("value", TargetSource.EMBED_FIELD_VALUE),
        ):
            text = _clean(getattr(field_obj, attr, None))
            if text is not None:
                out.append(TextSource(
                    text, source, f"{base}.fields[{findex}].{attr}"))
    # footer text: nested first, then flat
    footer = getattr(embed, "footer", None)
    if footer is not None and not isinstance(footer, str):
        text = _clean(getattr(footer, "text", None))
        if text is not None:
            out.append(TextSource(text, TargetSource.EMBED_FOOTER,
                                  f"{base}.footer.text"))
    else:
        text = _clean(getattr(embed, "footer_text", None))
        if text is not None:
            out.append(TextSource(text, TargetSource.EMBED_FOOTER,
                                  f"{base}.footer_text"))


def _harvest_component(comp: Any, path: str, out: List[TextSource]) -> None:
    """Best-effort label harvest; unsupported shapes are skipped."""
    # action row: children under `children` (discord.py 2.x) or
    # `components` (older layouts)
    children = getattr(comp, "children", None)
    if children is None:
        children = getattr(comp, "components", None)
    if children is not None:
        for cindex, child in enumerate(children):
            _harvest_component(child, f"{path}.{cindex}", out)
        return
    label = _clean(getattr(comp, "label", None))
    if label is not None:
        out.append(TextSource(label, TargetSource.COMPONENT_LABEL,
                              f"{path}.label"))
        return
    placeholder = _clean(getattr(comp, "placeholder", None))
    if placeholder is not None:
        out.append(TextSource(placeholder, TargetSource.COMPONENT_LABEL,
                              f"{path}.placeholder"))
        return
    options = getattr(comp, "options", None) or ()
    for oindex, option in enumerate(options):
        option_label = _clean(getattr(option, "label", None))
        if option_label is not None:
            out.append(TextSource(
                option_label, TargetSource.COMPONENT_LABEL,
                f"{path}.options[{oindex}].label"))


class NormalizedMessage:
    """All harvestable text of one message, bounded and path-addressed."""

    __slots__ = ("sources", "oversized")

    def __init__(self, sources: Sequence[TextSource],
                 oversized: bool = False) -> None:
        self.sources: Tuple[TextSource, ...] = tuple(sources)
        self.oversized = oversized

    def by_source(self, source: TargetSource) -> Tuple[TextSource, ...]:
        return tuple(s for s in self.sources if s.source is source)

    def first(self, source: TargetSource) -> Optional[TextSource]:
        for s in self.sources:
            if s.source is source:
                return s
        return None

    def kinds(self) -> Tuple[TargetSource, ...]:
        seen = []
        for s in self.sources:
            if s.source not in seen:
                seen.append(s.source)
        return tuple(seen)

    def total_chars(self) -> int:
        return sum(len(s.value) for s in self.sources)


def normalize_message(message: Any,
                      diagnostics: Optional[Diagnostics] = None
                      ) -> NormalizedMessage:
    """Build a :class:`NormalizedMessage` from a message or plain string.

    Raises :class:`~parsers.mudae.exceptions.InvalidMessageError` only
    for unusable input types (None, numbers, objects exposing no text
    at all). Unknown-but-well-formed messages return an EMPTY source
    list — classification later reports them as unknown format, not as
    an input error.
    """
    from .exceptions import InvalidMessageError

    if message is None:
        raise InvalidMessageError("message is None")
    if isinstance(message, str):
        text = message.strip()
        sources = [TextSource(text, TargetSource.CONTENT, "content")] \
            if text else []
        return NormalizedMessage(sources)

    out: List[TextSource] = []
    content = _clean(getattr(message, "content", None))
    if content is not None:
        out.append(TextSource(content, TargetSource.CONTENT, "content"))
    embeds = getattr(message, "embeds", None) or ()
    if not isinstance(embeds, Sequence) and not hasattr(embeds, "__iter__"):
        embeds = ()
    for index, embed in enumerate(embeds):
        # guard the harvest: one broken embed must not kill the message
        try:
            _harvest_embed(embed, index, out)
        except Exception:
            if diagnostics is not None:
                diagnostics.warning(
                    CODE_SKIPPED_SOURCE,
                    "embed could not be inspected; skipped",
                    subject=f"embeds[{index}]")
    components = getattr(message, "components", None) or ()
    if not isinstance(components, Sequence) and not hasattr(
            components, "__iter__"):
        components = ()
    for rindex, row in enumerate(components):
        try:
            _harvest_component(row, f"components[{rindex}]", out)
        except Exception:
            if diagnostics is not None:
                diagnostics.warning(
                    CODE_SKIPPED_SOURCE,
                    "component row could not be inspected; skipped",
                    subject=f"components[{rindex}]")

    if not out and not hasattr(message, "content") \
            and not hasattr(message, "embeds"):
        raise InvalidMessageError(
            "message exposes no content/embeds/components attributes; "
            "pass a message-like object or a plain string")

    clipped = [s.clip() for s in out]
    total = sum(len(s.value) for s in clipped)
    oversized = total > MAX_MESSAGE_TEXT_CHARS
    if oversized:
        budget = MAX_MESSAGE_TEXT_CHARS
        kept: List[TextSource] = []
        for s in clipped:
            if budget <= 0:
                break
            kept.append(TextSource(
                s.value[:budget], s.source, s.path))
            budget -= len(s.value)
        clipped = kept
        if diagnostics is not None:
            diagnostics.warning(
                CODE_OVERSIZED_INPUT,
                "message text exceeded the parse-size bound; truncated",
                detail=f"limit={MAX_MESSAGE_TEXT_CHARS}")
    return NormalizedMessage(clipped, oversized)
