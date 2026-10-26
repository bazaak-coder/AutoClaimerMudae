"""Tests for the central rate limiter: scopes, pacing, permits, cooldowns,
bounded retries, persistence, cancellation, and races."""

import asyncio
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.adapters import (
    CooldownError,
    PermanentAdapterError,
    RecoverableAdapterError,
)
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.models import RateLimitPolicy
from autoclaimer.persistence import TaskStateStore
from autoclaimer.ratelimit import (
    GLOBAL_SCOPE,
    EffectivePolicy,
    RateLimiter,
    RetryLimitExceededError,
    account_scope,
    channel_scope,
    policy_for,
    resolve_policy,
    route_scope,
    task_scope,
)

ACCOUNT = 271485504361996288
OTHER_ACCOUNT = 380028190142042112
BASE_WALL = datetime(2026, 10, 26, 12, 0, 0, tzinfo=UTC)


class TimeMachine:
    """Injectable clocks whose sleep advances both time bases instantly."""

    def __init__(self) -> None:
        self.mono = 0.0
        self.wall = BASE_WALL
        self.sleeps: list[float] = []

    def now_mono(self) -> float:
        return self.mono

    def now_wall(self) -> datetime:
        return self.wall

    def advance(self, seconds: float) -> None:
        self.mono += seconds
        self.wall += timedelta(seconds=seconds)

    async def sleep(self, seconds: float) -> None:
        self.sleeps.append(seconds)
        self.mono += seconds
        self.wall += timedelta(seconds=seconds)


def make_limiter(
    tm: TimeMachine, *, policy: EffectivePolicy | None = None, store: TaskStateStore | None = None
) -> RateLimiter:
    return RateLimiter(
        default_policy=policy,
        store=store,
        mono=tm.now_mono,
        wall=tm.now_wall,
        sleep=tm.sleep,
    )


def pacing(min_interval: float, **kwargs: Any) -> EffectivePolicy:
    return EffectivePolicy(min_interval_seconds=min_interval, **kwargs)


# --- scope keys ---


def test_scope_keys_are_distinct_and_structured() -> None:
    assert account_scope(ACCOUNT) == f"account:{ACCOUNT}"
    assert route_scope("claim") == "route:claim"
    assert task_scope("claim_test") == "task:claim_test"
    assert channel_scope(123) == "channel:123"
    assert GLOBAL_SCOPE == "global"
    assert len({account_scope(ACCOUNT), route_scope("claim"), GLOBAL_SCOPE}) == 3


# --- policy resolution ---


def test_resolve_policy_fieldwise_precedence() -> None:
    merged = resolve_policy(
        RateLimitPolicy(min_interval_seconds=2.0, retry_limit=5),
        RateLimitPolicy(min_interval_seconds=1.0, max_concurrency=2),
        RateLimitPolicy(min_interval_seconds=0.5),
    )
    assert merged.min_interval_seconds == 0.5  # account wins
    assert merged.max_concurrency == 2  # from task layer
    assert merged.retry_limit == 5  # inherited from global
    assert merged.backoff_base_seconds == 1.0  # built-in default


def test_resolve_policy_defaults_when_nothing_configured() -> None:
    assert resolve_policy() == EffectivePolicy()


def test_policy_for_resolves_from_parsed_config() -> None:
    app = parse_app_config(
        {
            "version": 1,
            "ratelimit": {"max_concurrency": 4},
            "tasks": {"claim_test": {"ratelimit": {"min_interval_seconds": 3}}},
        }
    )
    accounts = parse_accounts_config(
        {
            "version": 1,
            "accounts": [
                {"id": ACCOUNT, "tasks": ["claim_test"], "ratelimit": {"min_interval_seconds": 1}},
                {"id": OTHER_ACCOUNT, "tasks": []},
            ],
        },
        app,
    )
    assert policy_for(app, accounts, ACCOUNT, "claim_test").min_interval_seconds == 1.0
    assert policy_for(app, accounts, OTHER_ACCOUNT, "claim_test").min_interval_seconds == 3.0


# --- independent scopes ---


async def test_unrelated_route_scopes_do_not_block_each_other() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm)
    await limiter.record_cooldown(
        route_scope("claim"), retry_after_seconds=30.0, server_directed=True
    )
    async with limiter.operation(account_id=ACCOUNT, route="react"):  # no wait expected
        pass
    assert tm.sleeps == []


async def test_shared_route_scope_blocks_all_accounts() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm)
    await limiter.record_cooldown(
        route_scope("claim"), retry_after_seconds=10.0, server_directed=True
    )
    for account in (ACCOUNT, OTHER_ACCOUNT):
        async with limiter.operation(account_id=account, route="claim"):
            pass
    assert tm.mono == pytest.approx(10.0)


# --- per-account pacing ---


async def test_per_account_pacing_queues_operations() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm, policy=pacing(1.0))
    for _ in range(3):
        async with limiter.operation(account_id=ACCOUNT):
            pass
    # First op runs immediately; each subsequent op waits one interval slot.
    assert tm.sleeps == pytest.approx([1.0, 1.0])
    assert tm.mono == pytest.approx(2.0)


async def test_global_scope_cooldown_blocks_every_account() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm)
    await limiter.record_cooldown(GLOBAL_SCOPE, retry_after_seconds=3.0, server_directed=True)
    async with limiter.operation(account_id=ACCOUNT):
        pass
    assert tm.mono == pytest.approx(3.0)


async def test_pacing_is_per_account_not_global() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm, policy=pacing(5.0))
    async with limiter.operation(account_id=ACCOUNT):
        pass
    async with limiter.operation(account_id=OTHER_ACCOUNT):
        pass  # unrelated account: no inherited wait
    assert tm.sleeps == []


# --- concurrency limits ---


async def test_concurrency_limit_admits_one_and_queues_waiters() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm, policy=EffectivePolicy(max_concurrency=1))
    inside = asyncio.Event()
    release = asyncio.Event()

    async def holder() -> None:
        async with limiter.operation(account_id=ACCOUNT):
            inside.set()
            await release.wait()

    async def waiter() -> str:
        async with limiter.operation(account_id=ACCOUNT):
            return "admitted"

    first = asyncio.create_task(holder())
    await inside.wait()
    pending = asyncio.create_task(waiter())
    await asyncio.sleep(0)  # let the waiter reach semaphore.acquire
    status = limiter.status()
    assert status.permits[0].in_use == 1
    assert status.permits[0].waiting == 1
    assert status.permits[0].limit == 1
    release.set()
    assert await pending == "admitted"
    await first
    assert limiter.status().permits[0].in_use == 0
    assert limiter.status().permits[0].waiting == 0


# --- cooldowns: extension, overlap, server precedence ---


async def test_cooldown_extension_and_overlap_race() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm)
    await limiter.record_cooldown(
        route_scope("claim"), retry_after_seconds=5.0, server_directed=True
    )
    # A concurrent shorter cooldown cannot shorten; a longer one extends.
    await asyncio.gather(
        limiter.record_cooldown(
            route_scope("claim"), retry_after_seconds=3.0, server_directed=False
        ),
        limiter.record_cooldown(
            route_scope("claim"), retry_after_seconds=9.0, server_directed=True
        ),
    )
    info = limiter.status().cooldowns[0]
    assert info.remaining.total_seconds() == pytest.approx(9.0)
    assert info.server_directed is True
    async with limiter.operation(account_id=ACCOUNT, route="claim"):
        pass
    assert tm.mono == pytest.approx(9.0)


async def test_local_cannot_shorten_server_cooldown() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm)
    await limiter.record_cooldown(
        account_scope(ACCOUNT), retry_after_seconds=10.0, server_directed=True
    )
    await limiter.record_cooldown(
        account_scope(ACCOUNT), retry_after_seconds=2.0, server_directed=False
    )
    info = limiter.status().cooldowns[0]
    assert info.remaining.total_seconds() == pytest.approx(10.0)
    assert info.server_directed is True


async def test_waiter_observes_extension_recorded_during_wait() -> None:
    # Real clock with tiny durations: the waiter must actually suspend so a
    # concurrent extension can land mid-wait.
    limiter = RateLimiter()  # default: real monotonic/UTC clocks, asyncio.sleep
    loop = asyncio.get_running_loop()
    await limiter.record_cooldown(
        route_scope("claim"), retry_after_seconds=0.10, server_directed=True
    )

    async def op() -> None:
        async with limiter.operation(account_id=ACCOUNT, route="claim"):
            pass

    started = loop.time()
    waiter = asyncio.create_task(op())
    await asyncio.sleep(0.02)  # waiter is now suspended inside its cooldown wait
    await limiter.record_cooldown(
        route_scope("claim"), retry_after_seconds=0.30, server_directed=True
    )
    await waiter
    elapsed = loop.time() - started
    assert elapsed >= 0.30  # never proceeded before the extended deadline
    assert limiter.status().cooldowns == ()  # expired by the time we check


# --- cancellation and permit release ---


async def test_cancellation_during_pacing_releases_permit() -> None:
    # Real clock, small interval: the victim must be mid-pacing when cancelled.
    limiter = RateLimiter(
        default_policy=EffectivePolicy(min_interval_seconds=0.05, max_concurrency=2)
    )
    first_started = asyncio.Event()

    async def first() -> None:
        async with limiter.operation(account_id=ACCOUNT):
            first_started.set()
            await asyncio.sleep(5)

    async def second() -> None:
        async with limiter.operation(account_id=ACCOUNT):
            pass

    holder = asyncio.create_task(first())
    await first_started.wait()
    victim = asyncio.create_task(second())
    await asyncio.sleep(0.01)  # victim is pacing-waiting its reserved slot
    victim.cancel()
    with pytest.raises(asyncio.CancelledError):
        await victim
    status = limiter.status()
    assert status.permits[0].in_use == 1  # only the holder keeps its permit
    holder.cancel()
    with pytest.raises(asyncio.CancelledError):
        await holder
    assert limiter.status().permits[0].in_use == 0


async def test_cancellation_during_cooldown_wait_is_clean() -> None:
    limiter = RateLimiter(default_policy=EffectivePolicy(max_concurrency=1))
    await limiter.record_cooldown(
        account_scope(ACCOUNT), retry_after_seconds=0.30, server_directed=True
    )

    async def op() -> None:
        async with limiter.operation(account_id=ACCOUNT):
            pass

    victim = asyncio.create_task(op())
    await asyncio.sleep(0.02)
    victim.cancel()
    with pytest.raises(asyncio.CancelledError):
        await victim
    assert limiter.status().permits == ()  # nothing was acquired or leaked


# --- bounded retries ---


async def test_recoverable_errors_retry_then_succeed_with_backoff() -> None:
    tm = TimeMachine()
    limiter = make_limiter(
        tm, policy=EffectivePolicy(retry_limit=3, backoff_base_seconds=1.0, backoff_cap_seconds=4.0)
    )
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            raise RecoverableAdapterError("transient")
        return "ok"

    assert await limiter.run(flaky, account_id=ACCOUNT) == "ok"
    assert attempts == 3
    # backoff: 1.0 after attempt 1, 2.0 after attempt 2
    assert tm.sleeps == pytest.approx([1.0, 2.0])


async def test_retry_limit_is_bounded() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm, policy=EffectivePolicy(retry_limit=2, backoff_base_seconds=0.0))

    async def always_fails() -> None:
        raise RecoverableAdapterError("down")

    with pytest.raises(RetryLimitExceededError, match="Retry limit \\(2\\)"):
        await limiter.run(always_fails, account_id=ACCOUNT)


async def test_permanent_errors_do_not_retry() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm)
    attempts = 0

    async def broken() -> None:
        nonlocal attempts
        attempts += 1
        raise PermanentAdapterError("forbidden")

    with pytest.raises(PermanentAdapterError):
        await limiter.run(broken, account_id=ACCOUNT)
    assert attempts == 1
    assert tm.sleeps == []


async def test_server_cooldown_is_honored_and_persisted(tmp_path: Path) -> None:
    tm = TimeMachine()
    store = TaskStateStore(tmp_path / "state.db", clock=tm.now_wall)
    await store.open()
    limiter = make_limiter(tm, policy=EffectivePolicy(retry_limit=2), store=store)
    attempts = 0

    async def cooled() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise CooldownError(30.0)
        return "done"

    assert await limiter.run(cooled, account_id=ACCOUNT, route="claim") == "done"
    assert attempts == 2
    assert tm.mono == pytest.approx(30.0)  # waited the server-directed window
    rows = await store._run(store._fetchall_sync, "SELECT scope FROM scope_cooldowns", ())
    assert {row["scope"] for row in rows} == {
        account_scope(ACCOUNT),
        route_scope("claim"),
    }
    # The deadline has now passed exactly, so the active view is empty.
    assert await store.active_scope_cooldowns() == {}
    await store.close()


async def test_cooldown_exhausting_retry_limit_raises() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm, policy=EffectivePolicy(retry_limit=1))

    async def always_cooled() -> None:
        raise CooldownError(5.0)

    with pytest.raises(RetryLimitExceededError):
        await limiter.run(always_cooled, account_id=ACCOUNT)


# --- persistence and restart recovery ---


async def test_cooldowns_survive_restart(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    tm = TimeMachine()
    store = TaskStateStore(db, clock=tm.now_wall)
    await store.open()
    first = make_limiter(tm, store=store)
    await first.record_cooldown(
        route_scope("claim"), retry_after_seconds=60.0, server_directed=True
    )
    await store.close()

    tm.advance(20)  # process restarts 20s into the cooldown
    restarted_store = TaskStateStore(db, clock=tm.now_wall)
    await restarted_store.open()
    restarted = make_limiter(tm, store=restarted_store)
    await restarted.load_persisted_cooldowns()
    info = restarted.status().cooldowns[0]
    assert info.remaining.total_seconds() == pytest.approx(40.0)
    assert info.server_directed is True
    async with restarted.operation(account_id=ACCOUNT, route="claim"):
        pass
    # The 60s deadline set 20s before restart leaves exactly 40s to wait.
    assert restarted.status().cooldowns == ()
    assert tm.sleeps[-1] == pytest.approx(40.0)
    await restarted_store.close()


async def test_expired_persisted_cooldowns_do_not_block(tmp_path: Path) -> None:
    db = tmp_path / "state.db"
    tm = TimeMachine()
    store = TaskStateStore(db, clock=tm.now_wall)
    await store.open()
    limiter = make_limiter(tm, store=store)
    await limiter.record_cooldown(
        route_scope("claim"), retry_after_seconds=5.0, server_directed=True
    )
    await store.close()

    tm.advance(6)
    store2 = TaskStateStore(db, clock=tm.now_wall)
    await store2.open()
    restarted = make_limiter(tm, store=store2)
    await restarted.load_persisted_cooldowns()
    assert restarted.status().cooldowns == ()
    async with restarted.operation(account_id=ACCOUNT, route="claim"):
        pass
    assert tm.sleeps == []
    await store2.close()


# --- simultaneous workers / races / deadlock regression ---


async def test_many_simultaneous_workers_complete_without_deadlock() -> None:
    tm = TimeMachine()
    limiter = make_limiter(
        tm, policy=EffectivePolicy(min_interval_seconds=0.01, max_concurrency=3, retry_limit=2)
    )
    done: list[int] = []

    async def worker(account: int) -> None:
        async with limiter.operation(account_id=account, route="claim"):
            done.append(account)

    await asyncio.gather(*(worker(a) for a in (ACCOUNT, OTHER_ACCOUNT) for _ in range(10)))
    assert len(done) == 20
    status = limiter.status()
    assert all(p.in_use == 0 and p.waiting == 0 for p in status.permits)
    assert status.cooldowns == ()


async def test_concurrent_cooldown_recording_is_atomic() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm)
    await asyncio.gather(
        *(
            limiter.record_cooldown(
                route_scope("claim"), retry_after_seconds=float(s), server_directed=(s % 2 == 0)
            )
            for s in range(1, 21)
        )
    )
    info = limiter.status().cooldowns[0]
    assert info.remaining.total_seconds() == pytest.approx(20.0)  # max wins
    assert info.server_directed is True  # any server flag sticks


async def test_status_snapshot_reflects_state_and_is_a_copy() -> None:
    tm = TimeMachine()
    limiter = make_limiter(tm, policy=EffectivePolicy(min_interval_seconds=1.0, max_concurrency=1))
    await limiter.record_cooldown(
        route_scope("claim"), retry_after_seconds=5.0, server_directed=True
    )
    status = limiter.status()
    assert status.cooldowns[0].scope == route_scope("claim")
    assert status.cooldowns[0].server_directed is True
    assert status.next_pacing == {}
    # Snapshots are per-call copies: later activity does not mutate them.
    async with limiter.operation(account_id=ACCOUNT):
        pass
    assert status.next_pacing == {}
    assert limiter.status().next_pacing != {}
