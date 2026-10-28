"""Micro-benchmarks for AutoClaimer's hot paths.

Run from the project root with the venv interpreter:

    .venv/bin/python scripts/benchmark.py

Deliberately NOT part of the pytest suite: timings are machine-dependent
reporting, not assertions. Measures the real components (no mocks except
where a live client would be required):

- EventDispatcher fan-out (dispatch + per-subscriber pump delivery)
- TaskStateStore claim -> record_success cycle (async SQLite round trip)
- RateLimiter.run overhead (cooldown gate + permit + pacing slot)
- TaskRegistry.execute overhead (validation + handler dispatch)

Numbers are informational; the knowledgebase records measured values.
"""

from __future__ import annotations

import asyncio
import tempfile
import time
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path

from autoclaimer.adapters import ChannelMessage, EventDispatcher, MessageCreatedEvent
from autoclaimer.persistence import TaskStateStore
from autoclaimer.ratelimit import RateLimiter
from autoclaimer.registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType

ACCOUNT = 271485504361996288


def make_event(index: int) -> MessageCreatedEvent:
    return MessageCreatedEvent(
        account_id=ACCOUNT,
        received_at=datetime.now(UTC),
        message=ChannelMessage(
            id=index,
            channel_id=1000 + index,
            author_id=42,
            content=f"benchmark message {index}",
            has_components=False,
            created_at=None,
        ),
    )


async def bench_dispatcher(subscribers: int, events: int) -> float:
    """Events dispatched and fully delivered per second.

    Methodology note: ``EventDispatcher.dispatch`` takes the fast
    non-yielding path while queues are non-full, so a tight producer
    loop would starve the pump tasks (queues fill, oldest events drop).
    Real producers (library event tasks) yield between events; the
    benchmark mirrors that by dispatching in bounded batches (512,
    below the 1024 queue) and draining between batches. Drop count is
    asserted zero so the measurement stays valid.
    """
    dispatcher = EventDispatcher(queue_size=1024, name="bench")
    delivered: list[int] = [0] * subscribers
    unsubscribes: list[Callable[[], None]] = []

    def make_consumer(slot: int) -> Callable[[object], None]:
        def consumer(event: object) -> None:
            delivered[slot] += 1

        return consumer

    for slot in range(subscribers):
        unsubscribes.append(dispatcher.subscribe(make_consumer(slot)))

    batch_size = 512
    sent = 0
    start = time.perf_counter()
    while sent < events:
        chunk = min(batch_size, events - sent)
        for _ in range(chunk):
            await dispatcher.dispatch(make_event(sent))
            sent += 1
        target = sent
        deadline = time.perf_counter() + 30.0
        while any(count < target for count in delivered):
            if time.perf_counter() > deadline:
                raise RuntimeError("pumps failed to drain a batch within 30s")
            await asyncio.sleep(0)
    elapsed = time.perf_counter() - start
    await dispatcher.stop()
    for unsubscribe in unsubscribes:
        unsubscribe()
    if dispatcher.dropped:
        raise RuntimeError(f"benchmark invalid: {dispatcher.dropped} events dropped")
    return events / elapsed


async def bench_store(cycles: int, root: Path) -> float:
    """claim -> record_success cycles per second (one SQLite store)."""
    store = TaskStateStore(root / "bench-state.db")
    await store.open()
    records = []
    for index in range(cycles):
        record = await store.create_instance(ACCOUNT, f"bench_task_{index}")
        records.append(record)
    start = time.perf_counter()
    for record in records:
        await store.claim(record.id)
        await store.record_success(record.id, TaskOutcome(TaskStatus.SUCCEEDED, message="bench"))
    elapsed = time.perf_counter() - start
    await store.close()
    return cycles / elapsed


async def bench_limiter(ops: int) -> float:
    """Rate-limited no-op operations per second (zero pacing/cooldowns)."""
    limiter = RateLimiter(store=None)
    start = time.perf_counter()
    for _ in range(ops):
        await limiter.run(_no_op, account_id=ACCOUNT, route="send_message")
    elapsed = time.perf_counter() - start
    return ops / elapsed


async def _no_op() -> int:
    return 1


async def bench_registry(runs: int) -> float:
    """Registry executions per second (validate + handler + outcome)."""
    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="bench",
            description="Benchmark task.",
            validate_config=lambda config: None,
            handler=_bench_handler,
        )
    )
    context = TaskContext(
        account_id=ACCOUNT,
        task_id="bench",
        config={"payload": "x"},
        adapter=None,  # type: ignore[arg-type]  # handler never touches it
        credentials=None,
    )
    start = time.perf_counter()
    for _ in range(runs):
        await registry.execute(context)
    elapsed = time.perf_counter() - start
    return runs / elapsed


async def _bench_handler(context: TaskContext) -> TaskOutcome:
    return TaskOutcome(TaskStatus.SUCCEEDED, message="bench")


async def main() -> None:
    # Drop warnings (if a stress batch ever overflows) must not flood the
    # report; the dispatcher benchmark asserts zero drops for validity.
    import logging

    logging.disable(logging.WARNING)
    try:
        await _run_benchmarks()
    finally:
        logging.disable(logging.NOTSET)


async def _run_benchmarks() -> None:
    with tempfile.TemporaryDirectory(prefix="autoclaimer-bench-") as tmp:
        results: list[tuple[str, float]] = [
            (
                "EventDispatcher fan-out (1 subscriber, 20k events)",
                await bench_dispatcher(subscribers=1, events=20_000),
            ),
            (
                "EventDispatcher fan-out (4 subscribers, 20k events)",
                await bench_dispatcher(subscribers=4, events=20_000),
            ),
            (
                "TaskStateStore claim+success cycle (5k)",
                await bench_store(5_000, Path(tmp)),
            ),
            ("RateLimiter.run overhead (10k ops)", await bench_limiter(10_000)),
            ("TaskRegistry.execute overhead (10k runs)", await bench_registry(10_000)),
        ]

    width = max(len(name) for name, _ in results)
    print(f"{'benchmark':<{width}}   ops/sec")
    print("-" * (width + 20))
    for name, rate in results:
        print(f"{name:<{width}}   {rate:>12,.0f}")


if __name__ == "__main__":
    asyncio.run(main())
