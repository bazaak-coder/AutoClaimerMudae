"""Account health monitoring (roadmap item 6 — P1).

An OBSERVATION layer over account runtime, intentionally separate from
enforcement:

- **Scheduling effects** (defined once, matching existing policy):
  ``AUTH_FAILED`` and ``DISABLED`` accounts do not schedule — enforced
  where it already lives (account-fatal stop, ``enabled`` flag).
  ``RATE_LIMITED``/``DEGRADED``/``DISCONNECTED`` are informational:
  deferral stays owned by the rate limiter's cooldowns and the worker's
  retry policy, never by health bookkeeping.
- **Persistence**: in-memory per session by design — connection state
  does not survive restarts, so a fresh session starts at ``UNKNOWN`` /
  ``CONNECTING`` and re-learns. Nothing here is written to the store.
- **Failure behavior**: a health update can never break execution —
  every recording is a synchronous attribute update wrapped defensively
  by the callers; unknown accounts are auto-registered.
- **Security**: recorded error text passes a redaction filter and is
  length-bounded; tokens never appear in health output.
"""

from __future__ import annotations

import logging
from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
from datetime import datetime
from enum import Enum

from .logging_setup import redact_text
from .persistence import utc_now

logger = logging.getLogger(__name__)

#: Consecutive task failures (without an intervening success) after which
#: an account is reported DEGRADED. Informational only — the retry policy
#: still owns deferral and terminal decisions.
DEGRADED_THRESHOLD = 3

_ERROR_MAX = 200


class AccountHealthState(Enum):
    """Suggested-state model from the roadmap, plus ``UNKNOWN`` for a
    fresh session that has not observed anything yet."""

    UNKNOWN = "unknown"
    CONNECTING = "connecting"
    HEALTHY = "healthy"
    DISCONNECTED = "disconnected"
    DEGRADED = "degraded"
    AUTH_FAILED = "auth_failed"
    RATE_LIMITED = "rate_limited"
    DISABLED = "disabled"


@dataclass(frozen=True)
class AccountHealthSnapshot:
    """Immutable per-account view (safe to expose through the CLI)."""

    account_id: int
    state: AccountHealthState
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    consecutive_failures: int = 0
    total_successes: int = 0
    total_failures: int = 0
    last_error: str = ""


@dataclass
class _AccountHealth:
    state: AccountHealthState = AccountHealthState.UNKNOWN
    last_success_at: datetime | None = None
    last_failure_at: datetime | None = None
    consecutive_failures: int = 0
    total_successes: int = 0
    total_failures: int = 0
    last_error: str = ""


class AccountHealthMonitor:
    """Event-driven, in-memory account health for one manager session."""

    def __init__(
        self,
        *,
        clock: Callable[[], datetime] = utc_now,
        redactor: Callable[[str], str] = redact_text,
    ) -> None:
        self._clock = clock
        self._redactor = redactor
        self._accounts: dict[int, _AccountHealth] = {}

    # ----- queries -----

    def snapshot(self) -> Mapping[int, AccountHealthSnapshot]:
        """Immutable per-account views for every account seen this session."""
        return {
            account_id: AccountHealthSnapshot(
                account_id=account_id,
                state=health.state,
                last_success_at=health.last_success_at,
                last_failure_at=health.last_failure_at,
                consecutive_failures=health.consecutive_failures,
                total_successes=health.total_successes,
                total_failures=health.total_failures,
                last_error=health.last_error,
            )
            for account_id, health in self._accounts.items()
        }

    def _health(self, account_id: int) -> _AccountHealth:
        health = self._accounts.get(account_id)
        if health is None:
            health = _AccountHealth()
            self._accounts[account_id] = health
        return health

    # ----- events -----

    def worker_started(self, account_id: int) -> None:
        """A worker spawned / an account (re-)enabled: connecting."""
        health = self._health(account_id)
        if health.state not in (AccountHealthState.AUTH_FAILED, AccountHealthState.DISABLED):
            health.state = AccountHealthState.CONNECTING
        self._log(account_id, health.state)

    def record_success(self, account_id: int) -> None:
        """A task (or its platform operations) succeeded: healthy; any
        failure streak is broken. An ``AUTH_FAILED`` latch is NOT cleared
        here — a fatal account does not execute work anyway, and only the
        explicit revive path (re-enable) may clear it. BUG-036: the
        ``DISABLED`` latch equally survives a late in-flight success
        (the scheduler stays disabled; the report must not diverge)."""
        health = self._health(account_id)
        if health.state not in (AccountHealthState.AUTH_FAILED, AccountHealthState.DISABLED):
            health.state = AccountHealthState.HEALTHY
        health.last_success_at = self._clock()
        health.consecutive_failures = 0
        health.total_successes += 1
        self._log(account_id, health.state)

    def record_failure(
        self, account_id: int, error: BaseException | str, *, fatal: bool = False
    ) -> None:
        """A task failed. ``fatal=True`` (account-fatal: revoked
        credentials, invalidated session) latches ``AUTH_FAILED`` until
        an explicit revive; otherwise a first failure is a temporary
        ``DISCONNECTED`` and a streak (≥ DEGRADED_THRESHOLD consecutive)
        reports ``DEGRADED``."""
        health = self._health(account_id)
        health.last_failure_at = self._clock()
        health.total_failures += 1
        health.consecutive_failures += 1
        health.last_error = self._redactor(str(error))[:_ERROR_MAX]
        if fatal:
            # More specific than DISABLED: the credential is dead
            # regardless of the operator's disable.
            health.state = AccountHealthState.AUTH_FAILED
        elif health.state in (AccountHealthState.AUTH_FAILED, AccountHealthState.DISABLED):
            # BUG-036 + review finding: a late in-flight failure keeps
            # EITHER latch — AUTH_FAILED previously leaked into
            # DEGRADED/DISCONNECTED, masking the dead-credential state
            # that only an explicit revive may clear.
            pass
        elif health.consecutive_failures >= DEGRADED_THRESHOLD:
            health.state = AccountHealthState.DEGRADED
        else:
            health.state = AccountHealthState.DISCONNECTED
        self._log(account_id, health.state)

    def record_rate_limited(self, account_id: int) -> None:
        """Transport retries were exhausted by server-directed cooldowns:
        the account is rate-limited (deferral itself stays with the
        limiter). The next success clears it. BUG-036: like success, this
        never overwrites the ``DISABLED`` latch."""
        health = self._health(account_id)
        if health.state not in (AccountHealthState.AUTH_FAILED, AccountHealthState.DISABLED):
            health.state = AccountHealthState.RATE_LIMITED
        self._log(account_id, health.state)

    def record_connected(self, account_id: int) -> None:
        """A gateway connection transition to CONNECTED (ROADMAP item 26:
        lifecycle wiring). Latch-preserving like every other observation:
        ``AUTH_FAILED``/``DISABLED`` survive a reconnect, and unresolved
        failure states (``RATE_LIMITED``/``DEGRADED``) are NOT cleared by
        a mere connection — only a successful task (``record_success``)
        clears those."""
        health = self._health(account_id)
        if health.state in (
            AccountHealthState.UNKNOWN,
            AccountHealthState.CONNECTING,
            AccountHealthState.DISCONNECTED,
        ):
            health.state = AccountHealthState.HEALTHY
        self._log(account_id, health.state)

    def record_disconnected(self, account_id: int) -> None:
        """A gateway connection transition to DISCONNECTED (recoverable
        loss — the adapter's reconnect logic owns recovery). Latch-
        preserving AND failure-state-preserving: a dead credential or
        operator disable is not a connectivity observation, and
        RATE_LIMITED/DEGRADED survive blips too — otherwise a
        disconnect→connect pair would launder them into HEALTHY without
        any successful task (review finding).
        """
        health = self._health(account_id)
        if health.state in (
            AccountHealthState.UNKNOWN,
            AccountHealthState.CONNECTING,
            AccountHealthState.HEALTHY,
        ):
            health.state = AccountHealthState.DISCONNECTED
        self._log(account_id, health.state)

    def record_disabled(self, account_id: int) -> None:
        """The account was disabled by configuration or control command."""
        health = self._health(account_id)
        health.state = AccountHealthState.DISABLED
        self._log(account_id, health.state)

    def record_revived(self, account_id: int) -> None:
        """Explicit re-enable after a fatal stop or disable: connecting
        again (the fatal latch clears only through this path)."""
        health = self._health(account_id)
        health.state = AccountHealthState.CONNECTING
        health.consecutive_failures = 0
        self._log(account_id, health.state)

    # ----- reporting -----

    def _log(self, account_id: int, state: AccountHealthState) -> None:
        logger.info(
            "account health state",
            extra={
                "account_id": account_id,
                "operation": "health",
                "outcome": state.value,
            },
        )


def snapshot_for(
    snapshot: Mapping[int, AccountHealthSnapshot], account_id: int
) -> AccountHealthSnapshot:
    """The view for *account_id* (UNKNOWN when nothing was observed)."""
    return snapshot.get(
        account_id,
        AccountHealthSnapshot(account_id=account_id, state=AccountHealthState.UNKNOWN),
    )


def with_state(snapshot: AccountHealthSnapshot, state: AccountHealthState) -> AccountHealthSnapshot:
    """A copy of *snapshot* with a different state (reporting helper)."""
    return replace(snapshot, state=state)
