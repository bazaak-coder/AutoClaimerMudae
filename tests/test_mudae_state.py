"""Mudae domain state: typed views, UTC anchoring, durable deadlines.

Owner-confirmed semantics (2026-10-29):

- ``$mu`` is a LOOKUP command for the claim cooldown. Its reply is
  textually identical to the ``$tu`` claim line and is parsed by the
  EXISTING ``cooldown.claim_timer`` rule — that rule supersedes; no new
  parser entry exists or will be added for ``$mu``.
- ``$rtu`` likewise rides the existing ``cooldowns.rt`` rule. ``$rt``'s
  cooldown is perk-dependent (5h–20h observed range), so the deadline
  must always be LEARNED from a ``$rtu`` reply after ``$rt`` is used —
  never assumed from a fixed interval, and never fabricated when a reply
  is missing.
"""

from datetime import UTC, datetime, timedelta

import pytest

from autoclaimer.mudae import (
    ClaimStatus,
    RollsResetStatus,
    anchor_deadline,
    claim_status,
    parse_mudae,
    roll_reset_status,
)
from autoclaimer.mudae_parser import CooldownKind

NOW = datetime(2026, 10, 29, 12, 0, 0, tzinfo=UTC)

RT_COOLDOWN_REPLY = "The cooldown of $rt is not over. Time left: **2h 59** min."
RT_AVAILABLE_REPLY = "$rt is available!"
CLAIM_UNAVAILABLE = "**ExampleUser**, you can't claim for another **1h 42** min."


# --- typed views over $rtu replies ---


def test_rtu_cooldown_reply_yields_typed_roll_reset_status() -> None:
    status = roll_reset_status(parse_mudae(RT_COOLDOWN_REPLY))
    assert status == RollsResetStatus(
        kind=CooldownKind.RT, available=False, reported=timedelta(hours=2, minutes=59)
    )


def test_rtu_available_reply_is_available_with_missing_duration() -> None:
    """Ready state carries NO duration: missing ≠ zero. Callers must not
    fabricate a deadline from it."""
    status = roll_reset_status(parse_mudae(RT_AVAILABLE_REPLY))
    assert status is not None
    assert status.available is True
    assert status.reported is None


def test_roll_reset_status_ignores_unrelated_messages() -> None:
    assert roll_reset_status(parse_mudae("some unrelated message")) is None
    assert roll_reset_status(parse_mudae(CLAIM_UNAVAILABLE)) is None  # claim, not rt


def test_rtu_long_perk_cooldown_parses_up_to_20h() -> None:
    """Perk-dependent range (owner: 5h–20h observed) — 19h59 must parse."""
    reply = "The cooldown of $rt is not over. Time left: **19h 59** min."
    status = roll_reset_status(parse_mudae(reply))
    assert status is not None
    assert status.reported == timedelta(hours=19, minutes=59)


# --- anchoring: relative durations -> absolute UTC deadlines ---


def test_anchor_deadline_converts_relative_to_absolute_utc() -> None:
    deadline = anchor_deadline(timedelta(hours=2, minutes=59), now=NOW)
    assert deadline == NOW + timedelta(hours=2, minutes=59)
    assert deadline.tzinfo is not None


def test_anchor_deadline_rejects_naive_clock() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        anchor_deadline(timedelta(minutes=5), now=datetime(2026, 10, 29, 12, 0, 0))


def test_anchor_deadline_default_clock_is_utc_now() -> None:
    before = datetime.now(UTC)
    deadline = anchor_deadline(timedelta(seconds=30))
    after = datetime.now(UTC)
    assert before + timedelta(seconds=30) <= deadline <= after + timedelta(seconds=30)


def test_claim_status_view_matches_mu_semantics() -> None:
    """$mu lookup semantics: the claim line parses to ClaimStatus with the
    remaining claim cooldown; anchoring it yields the claim deadline."""
    status = claim_status(parse_mudae(CLAIM_UNAVAILABLE))
    assert status == ClaimStatus(
        available=False, reported=timedelta(hours=1, minutes=42), username="ExampleUser"
    )
    assert anchor_deadline(status.reported or timedelta(), now=NOW) == NOW + timedelta(
        hours=1, minutes=42
    )
