"""Adversarial scenario regressions (Phase 9 batch 7).

S3: response-lost mutations must never be blindly repeated.
"""

import asyncio
from typing import Any

import pytest

from autoclaimer.adapters import RecoverableAdapterError, UncertainOutcomeError
from autoclaimer.mock_adapter import MockAdapter
from autoclaimer.ratelimit import EffectivePolicy, RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType
from autoclaimer.workers import RateLimitedAdapter

ACCOUNT = 271485504361996288


async def test_response_lost_press_is_never_repeated() -> None:
    """Server accepted the press but the response was lost (InvalidData):
    exactly one platform attempt; the uncertainty surfaces."""

    inner = MockAdapter()
    # The Discord adapter translates InvalidData -> UncertainOutcomeError;
    # the mock simulates that boundary by raising the translated error.
    inner.script("press_component", UncertainOutcomeError("uncertain interaction outcome"))
    limiter = RateLimiter(default_policy=EffectivePolicy(retry_limit=5))
    adapter = RateLimitedAdapter(inner, limiter, ACCOUNT)
    with pytest.raises(UncertainOutcomeError, match="uncertain"):
        await adapter.press_component(1, 2, "claim")
    assert len(inner.calls) == 1  # never auto-retried the mutation
    assert inner.interactions == []  # no result recorded
    await adapter.close()


async def test_worker_records_uncertain_outcome_as_permanent() -> None:
    """At the task level, an uncertain mutation terminates (no blind re-run);
    the message documents the uncertainty for the operator."""
    from pathlib import Path

    from autoclaimer.config import parse_accounts_config, parse_app_config
    from autoclaimer.persistence import TaskInstanceState, TaskStateStore
    from autoclaimer.workers import WorkerManager

    tmp = Path("/tmp/opencode/s3-case")
    tmp.mkdir(parents=True, exist_ok=True)
    db = tmp / "state.db"
    db.unlink(missing_ok=True)

    app = parse_app_config(
        {
            "version": 1,
            "settings": {"shutdown_timeout_seconds": 2},
            "ratelimit": {"retry_limit": 5},
            "tasks": {"press": {"parameters": {}}},
        }
    )
    accounts = parse_accounts_config(
        {"version": 1, "accounts": [{"id": ACCOUNT, "tasks": ["press"]}]}, app
    )

    mock = MockAdapter()
    mock.script("press_component", UncertainOutcomeError("uncertain interaction outcome"))

    async def handler(context: TaskContext) -> TaskOutcome:
        await context.adapter.press_component(1, 2, "claim")
        return TaskOutcome(TaskStatus.SUCCEEDED)

    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="press",
            description="press probe",
            validate_config=lambda config: None,
            handler=handler,
        )
    )
    store = TaskStateStore(db)
    await store.open()
    manager = WorkerManager(
        registry=registry,
        store=store,
        limiter=RateLimiter(store=store),
        app_config=app,
        accounts=accounts,
        adapter_factory=lambda account_id: mock,
        idle_interval_seconds=0.02,
    )
    await manager.start()
    deadline = asyncio.get_running_loop().time() + 3.0
    row: Any = None
    while asyncio.get_running_loop().time() < deadline:
        row = await store.get_instance_by_ids(ACCOUNT, "press")
        if row.status is TaskInstanceState.FAILED:
            break
        await asyncio.sleep(0.01)
    await manager.stop()
    await store.close()
    assert row is not None and row.status is TaskInstanceState.FAILED
    assert "uncertain" in (row.last_outcome.message or "")
    presses = [c for c in mock.calls if c.operation == "press_component"]
    assert len(presses) == 1  # one platform attempt, never repeated


async def test_recoverable_still_retries_within_bounds() -> None:
    """Contrast: a known-safe transient failure still retries (not regress)."""
    inner = MockAdapter()
    for _ in range(5):
        inner.script("send_message", RecoverableAdapterError("transient"))
    limiter = RateLimiter(default_policy=EffectivePolicy(retry_limit=2, backoff_base_seconds=0.0))
    adapter = RateLimitedAdapter(inner, limiter, ACCOUNT)
    from autoclaimer.ratelimit import RetryLimitExceededError

    with pytest.raises(RetryLimitExceededError):
        await adapter.send_message(1, "x")
    assert len([c for c in inner.calls if c.operation == "send_message"]) == 3  # 1+2 retries
    await adapter.close()
