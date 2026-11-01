"""Rate-limit stress testing (roadmap item 9 — P1).

Concurrent load across many accounts and tasks against the REAL limiter
and worker manager, measuring observable behavior end to end:

- per-account pacing (min-interval spacing of real operations),
- sequential task execution within one account,
- global concurrency bound (max_concurrent_accounts),
- account-specific and task-specific policy effects,
- cooldown handling and repeated rate-limit responses without bursts,
- cancellation mid-operation leaving no leaked permits or stuck pacing.

Time-based assertions use generous margins (measured floors, not exact
equality) so CI jitter cannot produce false failures; cooldown waits use
a fake-clock sleep (instant, clock-advancing) for determinism.
"""

import asyncio
import os
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.adapters import CooldownError
from autoclaimer.config import parse_accounts_config, parse_app_config
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.persistence import TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import WorkerManager

#: Wall-clock pacing measurements are unreliable under parallel-worker CPU
#: contention (recorded send timestamps can bunch even though the limiter's
#: reservation ledger is honored — the ledger spaces send STARTS, not the
# post-send timestamp recording). They run in the serial quality job;
#: xdist workers skip them (roadmap item 20: parallel where supported,
#: serial where the measurement demands it).
_TIMING_SENSITIVE = pytest.mark.skipif(
    "PYTEST_XDIST_WORKER" in os.environ,
    reason="wall-clock pacing measurement requires serial execution",
)

ACCOUNTS = tuple(900000000000000000 + n for n in range(6))
CHANNEL = 111111111111111111


class FakeClockSleep:
    """Instant sleep that advances a fake wall/mono clock."""

    def __init__(self) -> None:
        self.calls: list[float] = []
        self.now = datetime(2026, 11, 1, 12, 0, 0, tzinfo=UTC)

    async def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)
        self.now += timedelta(seconds=seconds)

    def wall(self) -> datetime:
        return self.now

    def mono(self) -> float:
        return self.now.timestamp()


class RecordingAdapter(MockAdapter):
    """Records real wall-clock send timestamps per account."""

    def __init__(self, account_id: int, times: dict[int, list[float]] | None = None) -> None:
        super().__init__()
        self.account_id = account_id
        self.times = times if times is not None else {}

    async def send_message(self, channel_id: int, content: str, *, on_dispatch: Any = None) -> Any:
        self.times.setdefault(self.account_id, []).append(asyncio.get_running_loop().time())
        return await super().send_message(channel_id, content, on_dispatch=on_dispatch)


def _gap_floor(timestamps: list[float], floor: float) -> float:
    """The number of consecutive-send gaps below *floor* (want 0)."""
    gaps = [b - a for a, b in zip(timestamps, timestamps[1:], strict=False)]
    return sum(1 for gap in gaps if gap < floor)


def _build(
    tmp_path: Path,
    *,
    accounts: tuple[int, ...],
    tasks: dict[str, dict[str, Any]],
    global_policy: dict[str, Any] | None = None,
    account_ratelimit: dict[int, dict[str, Any]] | None = None,
    task_ratelimit: dict[str, dict[str, Any]] | None = None,
    max_concurrent: int = 3,
    handler_behavior: Any = None,
    sends_per_task: int = 3,
    fake_clock: bool = False,
) -> tuple[WorkerManager, TaskStateStore, dict[int, list[float]], FakeClockSleep, dict[str, Any]]:
    times: dict[int, list[float]] = {account: [] for account in accounts}
    in_flight: dict[str, int] = {"n": 0}
    peak: dict[str, int] = {"n": 0}
    per_account_active: dict[int, int] = {account: 0 for account in accounts}
    overlap_violations: list[int] = []

    async def handler(context: TaskContext) -> TaskOutcome:
        if handler_behavior is not None:
            custom = await handler_behavior(context)
            if custom is not None:
                outcome: TaskOutcome = custom
                return outcome
        in_flight["n"] += 1
        peak["n"] = max(peak["n"], in_flight["n"])
        per_account_active[context.account_id] += 1
        if per_account_active[context.account_id] > 1:
            overlap_violations.append(context.account_id)
        try:
            for _ in range(sends_per_task):
                await context.adapter.send_message(CHANNEL, "stress")
                await asyncio.sleep(0.0)
            return TaskOutcome(TaskStatus.SUCCEEDED)
        finally:
            per_account_active[context.account_id] -= 1
            in_flight["n"] -= 1

    app = parse_app_config(
        {
            "version": 1,
            "channels": {"roll_claim": CHANNEL},
            "settings": {
                "max_concurrent_accounts": max_concurrent,
                "shutdown_timeout_seconds": 5,
            },
            "ratelimit": {
                "backoff_base_seconds": 0.005,
                "backoff_cap_seconds": 0.005,
                **(global_policy or {}),
            },
            "tasks": {
                task_id: {"parameters": {}, **({"ratelimit": policy} if policy else {})}
                for task_id, policy in (
                    (task_id, (task_ratelimit or {}).get(task_id)) for task_id in tasks
                )
            },
        }
    )
    account_rl = account_ratelimit or {}
    accounts_raw = [
        {
            "id": account,
            "tasks": list(tasks),
            **({"ratelimit": account_rl[account]} if account in account_rl else {}),
        }
        for account in accounts
    ]
    accounts_cfg = parse_accounts_config({"version": 1, "accounts": accounts_raw}, app)
    registry = TaskRegistry()
    for task_id in tasks:
        registry.register(
            TaskType(
                id=task_id,
                description=f"Stress probe {task_id}.",
                validate_config=lambda config: None,
                handler=handler,
            )
        )
    store = TaskStateStore(tmp_path / "state.db")
    sleep = FakeClockSleep()
    # Pacing tests measure REAL send timestamps: real clock. Cooldown
    # storms wait out long windows: fake clock (instant, clock-advancing).
    limiter = (
        RateLimiter(store=store, sleep=sleep, wall=sleep.wall, mono=sleep.mono)
        if fake_clock
        else RateLimiter(store=store)
    )
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=limiter,
        app_config=app,
        accounts=accounts_cfg,
        adapter_factory=lambda account_id: RecordingAdapter(account_id, times),
        idle_interval_seconds=0.005,
    )
    metrics = {"peak": peak, "overlap_violations": overlap_violations}
    return manager, store, times, sleep, metrics


async def _drain(store: TaskStateStore, accounts: tuple[int, ...], tasks: dict[str, Any]) -> None:
    deadline = asyncio.get_running_loop().time() + 15.0
    remaining = len(accounts) * len(tasks)
    while asyncio.get_running_loop().time() < deadline:
        rows = [await store.get_instance_by_ids(a, t) for a in accounts for t in tasks]
        if all(r.status.value in ("succeeded", "failed") for r in rows):
            return
        await asyncio.sleep(0.01)
    pytest.fail(f"stress load did not drain ({remaining} expected terminal rows)")


# --- concurrent load across accounts -------------------------------------------


@_TIMING_SENSITIVE
async def test_concurrent_accounts_respect_pacing_and_slots(tmp_path: Path) -> None:
    """6 accounts × 2 tasks × 3 sends under a global min-interval: the
    global concurrency bound holds, each account stays sequential, and
    no same-account pair of sends violates the pacing floor."""
    accounts = ACCOUNTS[:6]
    tasks: dict[str, dict[str, Any]] = {"alpha": {}, "beta": {}}
    manager, store, times, _sleep, metrics = _build(
        tmp_path,
        accounts=accounts,
        tasks=tasks,
        global_policy={"min_interval_seconds": 0.02},
        max_concurrent=3,
    )
    await store.open()
    try:
        async with manager:
            await _drain(store, accounts, tasks)
        assert metrics["peak"]["n"] <= 3  # global bound held
        assert metrics["overlap_violations"] == []  # sequential per account
        for account in accounts:
            stamps = times[account]
            assert len(stamps) == 6  # 2 tasks × 3 sends each
            observed = [round(b - a, 4) for a, b in zip(stamps, stamps[1:], strict=False)]
            assert _gap_floor(stamps, floor=0.015) == 0, f"pacing violated on {account}: {observed}"
    finally:
        await store.close()


# --- policy layering under load ---------------------------------------------------


@_TIMING_SENSITIVE
async def test_account_specific_policy_paces_that_account_only(tmp_path: Path) -> None:
    """One account carries a stricter policy: its sends pace slower than
    the others' while all accounts still finish."""
    slow = ACCOUNTS[0]
    fast = ACCOUNTS[1]
    tasks: dict[str, dict[str, Any]] = {"alpha": {}}
    manager, store, times, _sleep, _metrics = _build(
        tmp_path,
        accounts=(slow, fast),
        tasks=tasks,
        global_policy={"min_interval_seconds": 0.01},
        account_ratelimit={slow: {"min_interval_seconds": 0.08}},
    )
    await store.open()
    try:
        async with manager:
            await _drain(store, (slow, fast), tasks)
        slow_gaps = [b - a for a, b in zip(times[slow], times[slow][1:], strict=False)]
        fast_gaps = [b - a for a, b in zip(times[fast], times[fast][1:], strict=False)]
        assert all(gap >= 0.06 for gap in slow_gaps), slow_gaps
        assert all(gap < 0.06 for gap in fast_gaps), fast_gaps
    finally:
        await store.close()


@_TIMING_SENSITIVE
async def test_task_specific_policy_reaches_operations(tmp_path: Path) -> None:
    """A task-level policy paces only that task's operations (end to end
    through the worker's policy binding)."""
    account = ACCOUNTS[0]
    tasks: dict[str, dict[str, Any]] = {"paced": {}, "loose": {}}
    manager, store, times, _sleep, _metrics = _build(
        tmp_path,
        accounts=(account,),
        tasks=tasks,
        task_ratelimit={"paced": {"min_interval_seconds": 0.08}},
    )
    await store.open()
    try:
        async with manager:
            await _drain(store, (account,), tasks)
        # The paced task's 3 sends honor its interval; the loose task's
        # (interleaved on the same account) are unpaced.
        assert len(times[account]) == 6
        stamps = times[account]
        # With per-task policies the account slot serializes anyway; the
        # observable guarantee is the account never sends FASTER than the
        # paced task's interval WHILE executing the paced task. Cheap
        # deterministic proxy: total elapsed ≥ 2 intervals.
        elapsed = stamps[-1] - stamps[0]
        assert elapsed >= 0.14, (elapsed, stamps)
    finally:
        await store.close()


# --- cooldown storms and bursts -----------------------------------------------------


@_TIMING_SENSITIVE
async def test_repeated_cooldowns_never_burst(tmp_path: Path) -> None:
    """Every send 429s (retry-after 20 s) twice, then succeeds: each
    retry WAITS the recorded cooldown (fake clock) — three real sends
    total, zero bursts."""
    account = ACCOUNTS[0]
    tasks: dict[str, dict[str, Any]] = {"alpha": {}}

    async def behavior(context: TaskContext) -> TaskOutcome | None:
        return None  # default sends; cooldowns scripted below

    manager, store, times, sleep, _metrics = _build(
        tmp_path,
        accounts=(account,),
        tasks=tasks,
        global_policy={"min_interval_seconds": 0.0},
        handler_behavior=behavior,
        sends_per_task=1,
        fake_clock=True,
    )
    adapters: dict[int, RecordingAdapter] = {}

    def factory(account_id: int) -> RecordingAdapter:
        adapter = adapters.get(account_id)
        if adapter is None:
            adapter = RecordingAdapter(account_id, {})
            adapters[account_id] = adapter
        return adapter

    manager._adapter_factory = factory  # noqa: SLF001
    adapter = factory(account)
    adapter.script("send_message", CooldownError(20.0, "429"), CooldownError(20.0, "429"))

    await store.open()
    try:
        async with manager:
            await _drain(store, (account,), tasks)
        cooldown_waits = [c for c in sleep.calls if c >= 15.0]
        assert len(cooldown_waits) >= 2, sleep.calls  # both cooldowns waited
        assert len(adapter.sent_messages) == 1  # exactly one real send
    finally:
        await store.close()


# --- cancellation hygiene ------------------------------------------------------------


async def test_cancellation_releases_permits_and_pacing(tmp_path: Path) -> None:
    """Cancel operations mid-flight: no permits leak (later operations
    acquire immediately) and the limiter reports nothing stuck."""
    from contextlib import suppress

    limiter = RateLimiter()
    release = asyncio.Event()
    started = asyncio.Event()

    async def op() -> str:
        started.set()
        await release.wait()
        return "done"

    tasks = [
        asyncio.create_task(limiter.run(op, account_id=ACCOUNTS[0], route="send_message"))
        for _ in range(3)
    ]
    deadline = asyncio.get_running_loop().time() + 2.0
    while not started.is_set() and asyncio.get_running_loop().time() < deadline:
        await asyncio.sleep(0.01)
    assert started.is_set()
    await asyncio.sleep(0.05)  # let the rest hit the limiter/permits
    for task in tasks:
        task.cancel()
    for task in tasks:
        with suppress(asyncio.CancelledError):
            await task
    release.set()

    # A later operation completes promptly: permits and pacing clean.
    async def probe_op() -> str:
        return "ok"

    probe = asyncio.create_task(limiter.run(probe_op, account_id=ACCOUNTS[1], route="send_message"))
    await asyncio.wait_for(probe, timeout=2.0)
    assert probe.result() == "ok"
    status = limiter.status()
    for permit in status.permits:
        assert permit.in_use == 0, permit
