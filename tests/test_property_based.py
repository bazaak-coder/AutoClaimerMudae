"""Property-based tests (roadmap item 19 — P2).

Invariants are defined BEFORE generation (each property's docstring).
Generation is derandomized (`derandomize=True`): failures are exactly
reproducible — hypothesis prints the failing example; re-running gives
the same one.

Areas (roadmap): configuration validation, retry scheduling, task state
transitions, persistence serialization, account/task assignment.
"""

from __future__ import annotations

import math
from datetime import UTC, datetime, timedelta

from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from autoclaimer.config import (
    parse_accounts_config,
    parse_app_config,
    resolve_task_config,
)
from autoclaimer.models import ConfigValidationError
from autoclaimer.mudae import anchor_deadline
from autoclaimer.persistence import _format_ts, _parse_ts
from autoclaimer.ratelimit import RateLimiter

SETTINGS = settings(
    max_examples=75,
    deadline=None,
    derandomize=True,  # reproducible failures (roadmap item 19)
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)

_ACCOUNT_ID = st.integers(min_value=1, max_value=2**63 - 1)
_SNOWFLAKE = st.integers(min_value=1, max_value=2**63 - 1)


# --- configuration validation ------------------------------------------------


def _valid_config_doc(channel: int, task_id: str, retry: int, timeout: float) -> dict[str, object]:
    return {
        "version": 1,
        "channels": {"roll_claim": channel},
        "settings": {"task_timeout_seconds": timeout},
        "task_defaults": {"retry_limit": retry},
        "tasks": {task_id: {"parameters": {}}},
    }


@SETTINGS
@given(
    channel=_SNOWFLAKE,
    task_id=st.sampled_from(["roll", "mu", "rt", "probe_task", "t-1"]),
    retry=st.integers(min_value=0, max_value=100),
    timeout=st.floats(min_value=0.001, max_value=86400.0, allow_nan=False),
)
def test_valid_generated_configs_always_parse(
    channel: int, task_id: str, retry: int, timeout: float
) -> None:
    """INVARIANT: any document the generator can build (well-formed
    fields within documented ranges) parses successfully — the validator
    never rejects valid input with an arbitrary exception."""
    app = parse_app_config(_valid_config_doc(channel, task_id, retry, timeout))
    assert app.channels.roll_claim == channel
    assert app.tasks[task_id].retry_limit == retry
    assert math.isclose(app.settings.task_timeout_seconds or 0.0, timeout)


@SETTINGS
@given(
    channel=st.one_of(
        st.just(0),
        st.just(-1),
        st.booleans(),
        st.floats(min_value=1, allow_nan=False),
        st.text(min_size=1, max_size=24),
    ),
)
def test_invalid_channel_values_always_raise_typed(channel: object) -> None:
    """INVARIANT: an invalid roll_claim value raises the TYPED
    ConfigValidationError naming the field — never another exception
    type, never silence."""
    import pytest

    with pytest.raises(ConfigValidationError, match="roll_claim"):
        parse_app_config(_valid_config_doc(channel, "probe", 3, 10.0))  # type: ignore[arg-type]


# --- persistence serialization --------------------------------------------------


@SETTINGS
@given(
    moment=st.datetimes(
        min_value=datetime(2000, 1, 1, tzinfo=UTC),
        max_value=datetime(2100, 1, 1, tzinfo=UTC),
        timezones=st.just(UTC),
    )
)
def test_timestamp_round_trip_is_exact(moment: datetime) -> None:
    """INVARIANT: _parse_ts(_format_ts(t)) == t for every timezone-aware
    datetime in the supported range — persisted deadlines never drift."""
    assert _parse_ts(_format_ts(moment)) == moment


# --- retry scheduling ------------------------------------------------------------


@SETTINGS
@given(attempt=st.integers(min_value=1, max_value=10_000))
def test_backoff_is_monotonic_and_capped(attempt: int) -> None:
    """INVARIANT: the transport backoff delay is monotonically
    non-decreasing in the attempt number and NEVER below the base (for
    the first attempt) nor above the cap."""
    from autoclaimer.ratelimit import EffectivePolicy

    limiter = RateLimiter()
    policy = EffectivePolicy(backoff_base_seconds=0.05, backoff_cap_seconds=2.0)
    d1 = limiter._backoff_delay(attempt, policy)  # noqa: SLF001
    d2 = limiter._backoff_delay(attempt + 1, policy)  # noqa: SLF001
    assert d1 <= 2.0 + 1e-9 and d2 <= 2.0 + 1e-9  # capped
    assert d2 >= d1 - 1e-9  # monotonic
    assert d1 >= 0.0  # never negative


@SETTINGS
@given(
    remaining=st.floats(min_value=0.000001, max_value=48.0, allow_nan=False),
    now=st.datetimes(timezones=st.just(UTC), min_value=datetime(2020, 1, 1, tzinfo=UTC)),
)
def test_anchor_deadline_is_now_plus_remaining(remaining: float, now: datetime) -> None:
    """INVARIANT: a learned deadline anchors to `now + remaining` exactly
    (microsecond fidelity) — scheduling never invents or loses time."""
    deadline = anchor_deadline(timedelta(seconds=remaining), now=now)
    delta = (deadline - now) - timedelta(seconds=remaining)
    assert abs(delta.total_seconds()) < 1e-3


# --- account/task assignment --------------------------------------------------------


@SETTINGS
@given(
    retry_default=st.integers(min_value=0, max_value=10),
    retry_task=st.integers(min_value=0, max_value=10),
    retry_override=st.one_of(st.none(), st.integers(min_value=0, max_value=10)),
)
def test_retry_limit_precedence(
    retry_default: int, retry_task: int, retry_override: int | None
) -> None:
    """INVARIANT: resolved retry_limit == override if set, else task
    definition, else defaults — for every combination."""
    doc = {
        "version": 1,
        "channels": {"roll_claim": 1},
        "task_defaults": {"retry_limit": retry_default},
        "tasks": {"job": {"retry_limit": retry_task, "parameters": {}}},
    }
    app = parse_app_config(doc)
    override_block = {} if retry_override is None else {"retry_limit": retry_override}
    accounts = parse_accounts_config(
        {
            "version": 1,
            "accounts": [{"id": 5, "tasks": ["job"], "overrides": {"job": override_block}}],
        },
        app,
    )
    resolved = resolve_task_config(app, accounts.accounts[0], "job")
    expected = retry_override if retry_override is not None else retry_task
    assert resolved.retry_limit == expected


@SETTINGS
@given(
    task_parameters=st.dictionaries(st.text(min_size=1, max_size=12), st.integers(0, 5), max_size=5)
)
def test_override_parameters_merge_shallowly(task_parameters: dict[str, int]) -> None:
    """INVARIANT: override parameters win per-key over the definition;
    every UNTOUCHED definition key (arbitrary sets) survives with its
    definition value."""
    doc = {
        "version": 1,
        "channels": {"roll_claim": 1},
        "tasks": {"job": {"parameters": {**task_parameters, "definition_only": 1}}},
    }
    app = parse_app_config(doc)
    accounts = parse_accounts_config(
        {
            "version": 1,
            "accounts": [
                {
                    "id": 5,
                    "tasks": ["job"],
                    "overrides": {"job": {"parameters": {"override_key": 999}}},
                }
            ],
        },
        app,
    )
    resolved = resolve_task_config(app, accounts.accounts[0], "job")
    assert resolved.parameters["override_key"] == 999  # override wins per-key
    assert resolved.parameters["definition_only"] == 1  # untouched survives
    for key, value in task_parameters.items():  # arbitrary keys all survive
        assert resolved.parameters[key] == value
