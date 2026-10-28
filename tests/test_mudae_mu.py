"""Dedicated $mu parsing: the confirmed response formats, pinned exactly.

Baseline fixtures are the owner-confirmed templates (Phase 3 spec,
2026-10-28) — never invented wording:

- Format A: "**username**, you __can__ claim right now! The next claim
  reset is in **Nh N** min."
- Format B: "**username**, you can't claim for another **[Nh ]N** min."

The duration is RELATIVE at this boundary; deadline anchoring belongs to
the later Mudae task/state layer. Missing duration is distinct from a
valid zero duration (vendored parser invariant, pinned here).
"""

from datetime import timedelta

from autoclaimer.mudae import ClaimStatus, claim_status, parse_mudae

AVAILABLE = (
    "**ExampleUser**, you __can__ claim right now! The next claim reset is in **1h 42** min."
)
UNAVAILABLE_HM = "**ExampleUser**, you can't claim for another **1h 42** min."
UNAVAILABLE_M = "**ExampleUser**, you can't claim for another **42** min."


def status(message: str) -> ClaimStatus | None:
    return claim_status(parse_mudae(message))


# --- confirmed formats ---


def test_format_a_available_with_hours_and_minutes() -> None:
    result = status(AVAILABLE)
    assert result is not None
    assert result.available is True
    assert result.reported == timedelta(hours=1, minutes=42)
    assert result.username == "ExampleUser"


def test_format_b_unavailable_with_hours_and_minutes() -> None:
    result = status(UNAVAILABLE_HM)
    assert result is not None
    assert result.available is False
    assert result.reported == timedelta(hours=1, minutes=42)
    assert result.username == "ExampleUser"


def test_format_b_unavailable_minutes_only() -> None:
    result = status(UNAVAILABLE_M)
    assert result is not None
    assert result.available is False
    assert result.reported == timedelta(minutes=42)


# --- duration semantics ---


def test_zero_minutes_is_a_valid_zero_not_missing() -> None:
    result = status("**ExampleUser**, you can't claim for another **0** min.")
    assert result is not None
    assert result.available is False
    assert result.reported == timedelta(0)  # stated zero ≠ missing (None)


def test_zero_minutes_after_nonzero_hours() -> None:
    result = status("**ExampleUser**, you can't claim for another **2h 0** min.")
    assert result is not None
    assert result.available is False
    assert result.reported == timedelta(hours=2)  # "0" is minutes, never seconds


def test_various_integer_values_parse_exactly() -> None:
    for message, expected in (
        ("**u**, you can't claim for another **3h 7** min.", timedelta(hours=3, minutes=7)),
        ("**u**, you can't claim for another **59** min.", timedelta(minutes=59)),
        ("**u**, you can't claim for another **12h 59** min.", timedelta(hours=12, minutes=59)),
    ):
        result = status(message)
        assert result is not None
        assert result.reported == expected


def test_username_extraction_allows_underscores_and_digits() -> None:
    result = status("**libos_terezent42**, you can't claim for another **5** min.")
    assert result is not None
    assert result.username == "libos_terezent42"


# --- malformed and missing durations ---


def test_hours_without_minutes_is_not_recognized() -> None:
    # The minutes group is required by the confirmed formats.
    assert status("**ExampleUser**, you can't claim for another **1h** min.") is None


def test_non_numeric_duration_is_not_recognized() -> None:
    assert status("**u**, you can't claim for another **xh 42** min.") is None
    assert status("**u**, you can't claim for another **1h 4x** min.") is None


def test_missing_duration_is_not_recognized() -> None:
    # Never fabricated as zero, never silently accepted.
    assert status("**ExampleUser**, you can't claim for another ** min.") is None
    assert status("**ExampleUser**, you can't claim for another.") is None


# --- wording integrity ---


def test_altered_wording_is_not_recognized() -> None:
    assert status("**ExampleUser**, you cannot claim for another **42** min.") is None
    assert status("**ExampleUser**, you really can't claim for another **42** min.") is None
    # Deliberate acceptance (documented): the vendored pattern does not
    # anchor the end of the claim line — genuine $tu output continues
    # with further lines (rolls/react state), so an end anchor would
    # break multi-line messages. Consequence: trailing characters after
    # "min" (e.g. pluralized "minutes!") still match. Recorded as a
    # limitation with the follow-up: an end-of-line anchor behind a
    # verified multi-line $tu fixture set.
    accepted = status("**ExampleUser**, you can't claim for another **42** minutes!")
    assert accepted is not None
    assert accepted.reported == timedelta(minutes=42)


def test_whitespace_acceptance_is_deliberate_and_limited() -> None:
    # Leading/trailing whitespace is accepted (message normalization
    # strips outer whitespace — deliberate).
    result = status(f"  {UNAVAILABLE_M}  ")
    assert result is not None
    assert result.reported == timedelta(minutes=42)
    # Inner spacing is strict: doubled spaces do not match.
    assert status("**ExampleUser**, you can't  claim for another **42** min.") is None


def test_unrelated_text_is_not_classified_as_claim_status() -> None:
    assert status("Remember that you can't claim for another user's trust.") is None
    assert status("**News**, you can't subscribe for another **42** min.") is None
    assert status("you can't claim for another **42** min.") is None  # no username prefix
    assert status("") is None
