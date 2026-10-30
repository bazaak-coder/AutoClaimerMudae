"""Central asynchronous rate limiting for AutoClaimer.

Distinct concepts (never one delay for everything):

1. **Per-account pacing** — a configurable minimum interval between
   operations for one account (local policy).
2. **Scope cooldowns** — deadlines keyed by explicit scope strings
   (``account:{snowflake}``, ``route:{name}``, ``task:{id}``,
   ``channel:{id}``, ``global``). Unrelated scopes never block each other.
3. **Concurrency caps** — a maximum number of simultaneous operations per
   account (semaphore permits).
4. **Server-directed cooldowns** — from
   :class:`~autoclaimer.adapters.CooldownError`; persisted through the
   Phase 3 state store as UTC wall-clock deadlines so they survive
   restarts. Server feedback always wins: deadlines only ever extend and
   the server-directed flag is sticky, so no local setting can shorten a
   server-imposed cooldown.
5. **Local pacing policies** — ``RateLimitPolicy`` values parsed at three
   configuration layers; field-wise precedence is
   per-account override -> task policy -> global default -> built-in
   defaults, mirroring the Phase 1 task-settings precedence.

Safety properties: all waiting uses ``asyncio`` sleeps (never blocking),
permits are released in ``finally`` blocks so cancellation cannot leak
them, deadlines are re-checked after every wait (extensions observed, no
busy-waiting), and pace slots are reserved atomically so simultaneous
workers queue deterministically.

The limiter responds conservatively to platform feedback only. It contains
no enforcement evasion, threshold probing, or account rotation.
"""

from __future__ import annotations

import asyncio
import logging
import math
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass, field, fields
from datetime import datetime, timedelta
from enum import Enum
from typing import TypeVar

from .adapters import CooldownError, RecoverableAdapterError
from .models import AccountsConfig, AppConfig, RateLimitPolicy
from .persistence import PersistenceError, TaskStateStore, utc_now

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

GLOBAL_SCOPE = "global"

#: Ceiling for any single recorded cooldown (repo-sweep: hostile
#: Retry-After headers clamped; real Mudae windows are 5-20 h).
_MAX_COOLDOWN_SECONDS = 86400.0


def account_scope(account_id: int) -> str:
    return f"account:{account_id}"


def route_scope(name: str) -> str:
    return f"route:{name}"


def task_scope(task_id: str) -> str:
    return f"task:{task_id}"


def channel_scope(channel_id: int) -> str:
    return f"channel:{channel_id}"


class RateLimitError(Exception):
    """Base class for rate-limiter failures."""


class RetryLimitExceededError(RateLimitError):
    """A bounded-retry loop exhausted its attempts."""


class CooldownScopeKind(Enum):
    """Scope kinds a server-directed cooldown can be recorded on."""

    ACCOUNT = "account"
    ROUTE = "route"
    GLOBAL = "global"


@dataclass(frozen=True)
class EffectivePolicy:
    """Fully resolved local policy (all layers merged)."""

    min_interval_seconds: float = 0.0
    max_concurrency: int | None = None
    retry_limit: int = 3
    backoff_base_seconds: float = 1.0
    backoff_cap_seconds: float = 60.0


def resolve_policy(
    global_policy: RateLimitPolicy | None = None,
    task_policy: RateLimitPolicy | None = None,
    account_policy: RateLimitPolicy | None = None,
) -> EffectivePolicy:
    """Merge policy layers field-wise: account -> task -> global -> defaults."""
    merged: dict[str, object] = {}
    # Later layers win; the built-in defaults layer seeds every field.
    for layer in (EffectivePolicy(), global_policy, task_policy, account_policy):
        if layer is None:
            continue
        for name, value in {f.name: getattr(layer, f.name) for f in fields(layer)}.items():
            if value is not None:
                merged[name] = value
    # Values originate from identically-named typed fields; safe by construction.
    return EffectivePolicy(**merged)  # type: ignore[arg-type]


def policy_for(
    app_config: AppConfig, accounts: AccountsConfig, account_id: int, task_id: str
) -> EffectivePolicy:
    """Resolve the effective policy from a parsed configuration."""
    account = next((a for a in accounts.accounts if a.id == account_id), None)
    task = app_config.tasks.get(task_id)
    return resolve_policy(
        app_config.ratelimit,
        task.ratelimit if task is not None else None,
        account.ratelimit if account is not None else None,
    )


@dataclass(frozen=True)
class CooldownInfo:
    scope: str
    remaining: timedelta
    server_directed: bool


@dataclass(frozen=True)
class PermitInfo:
    scope: str
    limit: int
    in_use: int
    waiting: int


@dataclass(frozen=True)
class RateLimiterStatus:
    """Read-only snapshot of limiter state."""

    cooldowns: tuple[CooldownInfo, ...] = ()
    permits: tuple[PermitInfo, ...] = ()
    next_pacing: Mapping[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class OperationLease:
    """What one admitted operation acquired."""

    scopes: tuple[str, ...]
    policy: EffectivePolicy


@dataclass
class _CooldownEntry:
    deadline: datetime
    server_directed: bool


class RateLimiter:
    """Coordinates pacing, cooldowns, and permits across concurrent workers."""

    def __init__(
        self,
        *,
        default_policy: EffectivePolicy | None = None,
        store: TaskStateStore | None = None,
        mono: Callable[[], float] = time.monotonic,
        wall: Callable[[], datetime] = utc_now,
        sleep: Callable[[float], Awaitable[None]] | None = None,
    ) -> None:
        self._default_policy = default_policy or EffectivePolicy()
        self._store = store
        self._mono = mono
        self._wall = wall
        self._sleep = sleep or asyncio.sleep
        self._cooldowns: dict[str, _CooldownEntry] = {}
        self._permits: dict[str, asyncio.Semaphore] = {}
        self._permit_limits: dict[str, int] = {}
        self._permit_in_use: dict[str, int] = {}
        self._waiting: dict[str, int] = {}
        self._pace_next: dict[str, float] = {}
        self._pace_lock = asyncio.Lock()
        self._cooldown_lock = asyncio.Lock()

    # ----- policy -----

    def _policy(self, override: EffectivePolicy | None) -> EffectivePolicy:
        return override or self._default_policy

    # ----- cooldowns -----

    async def record_cooldown(
        self,
        scope: str,
        *,
        retry_after_seconds: float | None = None,
        until: datetime | None = None,
        server_directed: bool,
    ) -> datetime:
        """Record a cooldown; deadlines only extend, server flag is sticky."""
        if until is not None:
            if retry_after_seconds is not None:
                raise ValueError("Provide exactly one of retry_after_seconds or until.")
            if until.tzinfo is None:
                raise ValueError("until must be a timezone-aware datetime")
            deadline = until
        elif retry_after_seconds is not None:
            # Repo-sweep fix: a hostile/corrupt Retry-After header can be
            # inf/NaN/1e309 — timedelta(seconds=…) would raise inside
            # this recording path, bypassing the retry machinery and
            # failing the task permanently. Clamp to a sane ceiling.
            try:
                seconds = float(retry_after_seconds)
            except (TypeError, ValueError):
                seconds = 1.0
            if not math.isfinite(seconds) or seconds < 0:
                seconds = 1.0
            deadline = self._wall() + timedelta(seconds=min(seconds, _MAX_COOLDOWN_SECONDS))
        else:
            raise ValueError("Provide exactly one of retry_after_seconds or until.")
        async with self._cooldown_lock:
            existing = self._cooldowns.get(scope)
            if existing is not None:
                deadline = max(deadline, existing.deadline)
                server_directed = server_directed or existing.server_directed
            self._cooldowns[scope] = _CooldownEntry(deadline, server_directed)
        if self._store is not None:
            # Repo-sweep fix: a transient store failure here previously
            # escaped, discarded the CooldownError the caller was
            # handling, and terminally killed the task — the in-memory
            # record (which gates this process) still stands.
            try:
                await self._store.set_scope_cooldown(
                    scope, deadline, server_directed=server_directed
                )
            except PersistenceError:
                logger.warning(
                    "could not persist scope cooldown; in-memory stands",
                    extra={"operation": "ratelimit.record_cooldown", "outcome": "store_error"},
                )
        return deadline

    async def load_persisted_cooldowns(self) -> None:
        """Adopt still-active cooldowns from the state store (startup)."""
        if self._store is None:
            return
        for scope, (deadline, server_directed) in (
            await self._store.active_scope_cooldowns()
        ).items():
            await self.record_cooldown(scope, until=deadline, server_directed=server_directed)

    def _active_deadline(self, scope: str) -> datetime | None:
        entry = self._cooldowns.get(scope)
        if entry is None:
            return None
        if entry.deadline <= self._wall():
            self._cooldowns.pop(scope, None)  # prune expired (bounded memory)
            return None
        return entry.deadline

    async def _wait_for_cooldowns(self, scopes: Sequence[str]) -> None:
        for scope in sorted(set(scopes)):
            while True:
                deadline = self._active_deadline(scope)
                if deadline is None:
                    break
                remaining = (deadline - self._wall()).total_seconds()
                if remaining <= 0:
                    break
                # Sleep the remaining time, then re-check: the deadline may
                # have been extended while we slept (never shortened).
                await self._sleep(remaining)

    # ----- permits -----

    def _semaphore(self, scope: str, limit: int) -> asyncio.Semaphore:
        existing = self._permits.get(scope)
        if existing is None:
            existing = asyncio.Semaphore(limit)
            self._permits[scope] = existing
            self._permit_limits[scope] = limit
        return existing

    async def _acquire_permit(self, scope: str, limit: int | None) -> bool:
        if limit is None:
            return False
        semaphore = self._semaphore(scope, limit)
        self._waiting[scope] = self._waiting.get(scope, 0) + 1
        acquired = False
        try:
            await semaphore.acquire()
            acquired = True
        finally:
            self._waiting[scope] -= 1
        if acquired:  # cancelled mid-acquire: no permit consumed, nothing to do
            self._permit_in_use[scope] = self._permit_in_use.get(scope, 0) + 1
        return acquired

    def _release_permit(self, scope: str) -> None:
        self._permit_in_use[scope] = self._permit_in_use.get(scope, 0) - 1
        semaphore = self._permits.get(scope)
        if semaphore is not None:
            semaphore.release()

    # ----- pacing -----

    async def _pace(self, scope: str, interval_seconds: float) -> None:
        if interval_seconds <= 0:
            return
        async with self._pace_lock:
            now = self._mono()
            start = max(now, self._pace_next.get(scope, now))
            self._pace_next[scope] = start + interval_seconds
            wait = start - now
        if wait > 0:
            await self._sleep(wait)

    # ----- public API -----

    async def wait_scopes(self, account_id: int, route: str | None = None) -> None:
        """Wait out active cooldowns for an account's scopes, holding nothing.

        Schedulers call this BEFORE acquiring execution capacity so that
        accounts parked on long server cooldowns do not starve unrelated
        accounts (the in-``operation`` gate still re-checks afterwards).
        """
        scopes = [account_scope(account_id), GLOBAL_SCOPE]
        if route is not None:
            scopes.append(route_scope(route))
        await self._wait_for_cooldowns(scopes)

    @asynccontextmanager
    async def operation(
        self,
        *,
        account_id: int,
        route: str | None = None,
        policy: EffectivePolicy | None = None,
    ) -> AsyncIterator[OperationLease]:
        """Admit one operation: cooldown gate -> permit -> pacing slot.

        Cancellation at any point releases the permit (if acquired) and
        holds no locks; nothing needs explicit release by the caller.
        """
        effective = self._policy(policy)
        account = account_scope(account_id)
        scopes = [account, GLOBAL_SCOPE] + ([route_scope(route)] if route else [])
        await self._wait_for_cooldowns(scopes)
        permit_scope = account
        acquired = await self._acquire_permit(permit_scope, effective.max_concurrency)
        try:
            # A cooldown recorded while we waited for the permit must
            # still gate the operation (no bypass via queue position).
            await self._wait_for_cooldowns(scopes)
            await self._pace(account, effective.min_interval_seconds)
            # Repo-sweep fix: a cooldown recorded while THIS waiter slept
            # its reserved pace slot was never re-checked (the post-permit
            # check happens before the sleep) — one more gate here closes
            # the last bypass window.
            await self._wait_for_cooldowns(scopes)
            yield OperationLease(scopes=tuple(sorted(set(scopes))), policy=effective)
        finally:
            if acquired:
                self._release_permit(permit_scope)

    async def run(
        self,
        operation_fn: Callable[[], Awaitable[_T]],
        *,
        account_id: int,
        route: str | None = None,
        policy: EffectivePolicy | None = None,
        cooldown_scope_kinds: Sequence[CooldownScopeKind] = (
            CooldownScopeKind.ACCOUNT,
            CooldownScopeKind.ROUTE,
        ),
    ) -> _T:
        """Run ``operation_fn`` under the limiter with bounded retries.

        CooldownError: records server-directed cooldowns on the configured
        scope kinds (persisted when a store is attached) and retries while
        attempts remain. Other recoverable errors back off exponentially.
        Permanent errors and UncertainOutcomeError propagate immediately —
        possibly-applied mutations are never blindly repeated.
        """
        effective = self._policy(policy)
        attempts_allowed = effective.retry_limit + 1
        attempt = 0
        while True:
            attempt += 1
            try:
                async with self.operation(account_id=account_id, route=route, policy=effective):
                    return await operation_fn()
            except CooldownError as exc:
                for kind in cooldown_scope_kinds:
                    await self._record_server_cooldown(kind, account_id, route, exc)
                if attempt >= attempts_allowed:
                    raise RetryLimitExceededError(
                        f"Retry limit ({effective.retry_limit}) exhausted after "
                        f"{attempt} attempts; last error: {exc}"
                    ) from exc
            except RecoverableAdapterError as exc:
                if attempt >= attempts_allowed:
                    raise RetryLimitExceededError(
                        f"Retry limit ({effective.retry_limit}) exhausted after "
                        f"{attempt} attempts; last error: {exc}"
                    ) from exc
                await self._sleep(self._backoff_delay(attempt, effective))
            # PermanentAdapterError and unrelated exceptions propagate.

    async def _record_server_cooldown(
        self,
        kind: CooldownScopeKind,
        account_id: int,
        route: str | None,
        exc: CooldownError,
    ) -> None:
        if kind is CooldownScopeKind.ACCOUNT:
            scope = account_scope(account_id)
        elif kind is CooldownScopeKind.ROUTE:
            if route is None:
                return
            scope = route_scope(route)
        else:
            scope = GLOBAL_SCOPE
        await self.record_cooldown(
            scope, retry_after_seconds=exc.retry_after_seconds, server_directed=True
        )

    def _backoff_delay(self, attempt: int, policy: EffectivePolicy) -> float:
        base = policy.backoff_base_seconds * (2 ** (attempt - 1))
        return float(min(base, policy.backoff_cap_seconds))

    # ----- status -----

    def status(self) -> RateLimiterStatus:
        """Read-only snapshot: active cooldowns, permits, and pacing queue."""
        now_wall = self._wall()
        cooldowns = tuple(
            CooldownInfo(
                scope=scope,
                remaining=entry.deadline - now_wall,
                server_directed=entry.server_directed,
            )
            for scope, entry in sorted(self._cooldowns.items())
            if entry.deadline > now_wall
        )
        permits = tuple(
            PermitInfo(
                scope=scope,
                limit=self._permit_limits[scope],
                in_use=self._permit_in_use.get(scope, 0),
                waiting=self._waiting.get(scope, 0),
            )
            for scope in sorted(self._permits)
        )
        now_mono = self._mono()
        next_pacing = {
            scope: max(0.0, at - now_mono) for scope, at in sorted(self._pace_next.items())
        }
        return RateLimiterStatus(
            cooldowns=cooldowns, permits=permits, next_pacing=dict(next_pacing)
        )
