"""AutoClaimer's thin interface to the vendored Mudae parser.

The parser (``autoclaimer.mudae_parser``, vendored from LibzentSuite —
see its VENDORED.md) reports WHAT a message says; this module adapts
AutoClaimer's message/event shapes to it and exposes the small typed
surface task code will consume. Per the error-handling policy
(agents.md section 17.2.4), parsed bot responses are DOMAIN OUTCOMES:
durations stay RELATIVE here (``timedelta``) — anchoring them to
absolute UTC deadlines and persisting them belongs to the later Mudae
task/state layer, not to parsing.

Design facts verified against the vendored source:

- ``MudaeParser()`` with no arguments builds hermetic in-memory defaults
  (no filesystem, no clock, no network) — parsing is deterministic.
- ``parse()`` accepts a plain ``str`` or any duck-typed message exposing
  ``content``/``embeds``/``components`` (each optional); AutoClaimer's
  :class:`~autoclaimer.adapters.ChannelMessage` (a ``content``-only
  frozen dataclass) is therefore directly parseable. ``None`` raises
  ``InvalidMessageError``.
- ``$mu`` replies and the ``$tu`` claim line are TEXTUALLY IDENTICAL
  ("**user**, you __can__ claim right now! The next claim reset is in
  **[Nh ]N** min." / "**user**, you can't claim for another **[Nh ]N**
  min."); the message does not encode which command produced it. The
  vendored ``cooldown.claim_timer`` rule parses both variants into a
  CLAIM-kind cooldown carrying ``metadata["available"]`` and the
  relative duration, plus a :class:`TimerInfo` summary. A SECOND,
  overlapping rule was deliberately NOT added: the engine runs every
  rule and flags multi-matches as ambiguous diagnostics
  (``parser._run_rules``), so a duplicate pattern would warn on every
  claim-status message. The dedicated ``$mu`` surface therefore lives
  here, as the typed :class:`ClaimStatus` view.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any

from .adapters import ChannelMessage, MessageCreatedEvent
from .mudae_parser import CooldownKind, InvalidMessageError, MudaeParser, ParseResult

__all__ = [
    "ClaimStatus",
    "RollsResetStatus",
    "anchor_deadline",
    "claim_status",
    "parse_mudae",
    "roll_reset_status",
    "InvalidMessageError",
    "ParseResult",
]

_PARSER: MudaeParser | None = None


def _parser() -> MudaeParser:
    """Process-wide hermetic parser (immutable defaults; thread-safe)."""
    global _PARSER
    if _PARSER is None:
        _PARSER = MudaeParser()
    return _PARSER


def parse_mudae(
    message: str | ChannelMessage | MessageCreatedEvent | Any,
    *,
    account_username: str | None = None,
) -> ParseResult:
    """Parse one Mudae message into the vendored typed result.

    Accepts what AutoClaimer produces: a :class:`MessageCreatedEvent`
    (unwrapped to its ``.message``), a :class:`ChannelMessage`, a plain
    content string, or any duck-typed message with optional
    ``content``/``embeds``/``components`` (nested library embeds and
    flat DTO embeds are both harvested by the parser). Raises
    :class:`InvalidMessageError` for ``None`` or objects exposing no
    message attributes at all.

    Deterministic by construction: no clock, network, filesystem, or
    database access (``account_username`` is echoed context only).
    """
    if isinstance(message, MessageCreatedEvent):
        message = message.message
    return _parser().parse(message, account_username=account_username)


@dataclass(frozen=True)
class ClaimStatus:
    """Dedicated ``$mu``/claim-line view over a :class:`ParseResult`.

    ``available`` mirrors the message state (``__can__ claim right
    now!`` vs ``can't claim for another``). ``reported`` is the RELATIVE
    duration exactly as stated — the remaining claim cooldown when
    unavailable, or the time until the next claim reset when available.
    Missing duration stays ``None`` (never zero): the parser preserves
    that distinction and so does this view. Deadline math is the task
    layer's job.
    """

    available: bool
    reported: timedelta | None
    username: str | None


def claim_status(result: ParseResult) -> ClaimStatus | None:
    """Extract the claim status from a parse result, if present.

    Returns ``None`` when the parsed message carried no claim-line
    cooldown (unrelated or unrecognized messages — callers must treat
    that as "no verified information", never as available/unavailable).

    ``$mu`` semantics (owner-confirmed 2026-10-29): ``$mu`` is a LOOKUP
    command for the claim cooldown; its reply is parsed by the existing
    claim-timer rule, which supersedes any dedicated ``$mu`` parser
    entry (none exists, by design).
    """
    for cooldown in result.cooldowns:
        if cooldown.kind is not CooldownKind.CLAIM:
            continue
        available = cooldown.metadata.get("available")
        if not isinstance(available, bool):
            continue
        return ClaimStatus(
            available=available,
            reported=cooldown.remaining,
            username=cooldown.username,
        )
    return None


@dataclass(frozen=True)
class RollsResetStatus:
    """Typed view over an ``$rtu`` reply (the ``$rt`` reset cooldown).

    ``$rtu`` rides the existing ``cooldowns.rt`` rule — no dedicated
    parser entry (same supersession as ``$mu``). ``available=True`` is
    the ready state ("$rt is available!") and carries NO duration
    (``reported is None``: missing ≠ zero — never fabricate a deadline
    from it). ``$rt``'s cooldown is perk-dependent (owner-observed range
    5h–20h): the deadline must always be LEARNED from a ``$rtu`` reply
    after ``$rt`` is used, never assumed from a fixed interval.
    """

    kind: CooldownKind
    available: bool
    reported: timedelta | None


def roll_reset_status(result: ParseResult) -> RollsResetStatus | None:
    """Extract the ``$rt`` reset status from a parse result, if present.

    Returns ``None`` for messages that are not ``$rtu``/``$rt`` state
    replies (callers: no verified information → look it up, do not
    guess).
    """
    for cooldown in result.cooldowns:
        if cooldown.kind is not CooldownKind.RT:
            continue
        if cooldown.metadata.get("available") is True:
            return RollsResetStatus(kind=CooldownKind.RT, available=True, reported=None)
        if cooldown.remaining is not None:
            return RollsResetStatus(
                kind=CooldownKind.RT, available=False, reported=cooldown.remaining
            )
    return None


def anchor_deadline(reported: timedelta, *, now: datetime | None = None) -> datetime:
    """Anchor a parsed RELATIVE duration to an absolute UTC deadline.

    This is the single boundary where parser-provided durations become
    deadlines (agents.md section 17.2.4): the Mudae task/state layer
    calls this before persisting. ``now`` is injectable for
    determinism; the system default is UTC now. Naive ``now`` values are
    rejected — deadlines are timezone-aware UTC by contract.
    """
    current = now if now is not None else datetime.now(UTC)
    if current.tzinfo is None:
        raise ValueError("anchor_deadline requires a timezone-aware `now` (UTC)")
    return current.astimezone(UTC) + reported
