"""Worker manager and scheduler: concurrent account workers.

Execution model:

- One worker loop per *enabled account* (not one asyncio task per pending
  operation), so the task count stays bounded by the account count.
- Tasks execute **sequentially within each account**; overall concurrent
  executions are bounded by ``settings.max_concurrent_accounts``.
- Queue ordering is deterministic: highest task ``priority`` first, then
  earliest ``next_attempt_at`` (retry time), then instance id.
- Every platform operation performed by a task handler goes through the
  central rate limiter via :class:`RateLimitedAdapter`, one route per
  operation kind — scheduling policy stays out of task handlers.

State and duplication:

- The SQLite store's atomic ``claim()`` is the single authority: an
  instance can never execute twice concurrently in one process (a second
  claim raises and the candidate is skipped).
- Shutdown with an uncertain remote outcome: graceful stop waits up to
  ``settings.shutdown_timeout_seconds`` for in-flight operations, then
  hard-cancels the worker; the instance stays ``running`` because the
  claim already committed and the outcome is unknown. On the next start,
  ``recover_interrupted()`` resets it to ``pending`` and it re-executes —
  at-least-once semantics; task handlers must tolerate duplicate side
  effects.

Account controls: enable/disable, pause/resume (per account and global)
gate *new* claims only; in-flight work is never cancelled mid-transaction.
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import TypeVar

from .adapters import ChannelMessage, InteractionResult, PlatformAdapter, SentMessage
from .config import resolve_task_config
from .credentials import CredentialProvider
from .models import AccountProfile, AccountsConfig, AppConfig
from .persistence import (
    PersistenceError,
    StateTransitionError,
    TaskInstanceRecord,
    TaskInstanceState,
    TaskStateStore,
    utc_now,
)
from .ratelimit import EffectivePolicy, RateLimiter, RetryLimitExceededError, policy_for
from .registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus

logger = logging.getLogger(__name__)

_T = TypeVar("_T")

_ACCOUNT_QUEUE_STATES = (TaskInstanceState.PENDING, TaskInstanceState.WAITING)


class WorkerState(Enum):
    RUNNING = "running"
    PAUSED = "paused"
    STOPPED = "stopped"


@dataclass(frozen=True)
class WorkerInfo:
    """Read-only snapshot of one account worker."""

    account_id: int
    state: WorkerState
    enabled: bool
    queued: int


@dataclass(frozen=True)
class ManagerStatus:
    workers: tuple[WorkerInfo, ...]
    globally_paused: bool


class RateLimitedAdapter:
    """Routes every platform operation through the central rate limiter."""

    def __init__(
        self,
        inner: PlatformAdapter,
        limiter: RateLimiter,
        account_id: int,
        policy: EffectivePolicy | None = None,
    ) -> None:
        self._inner = inner
        self._limiter = limiter
        self._account_id = account_id
        self._policy = policy

    async def _run(self, op_name: str, fn: Callable[[], Awaitable[_T]]) -> _T:
        return await self._limiter.run(
            fn,
            account_id=self._account_id,
            route=op_name,
            policy=self._policy,
        )

    async def send_message(self, channel_id: int, content: str) -> SentMessage:
        return await self._run(
            "send_message", lambda: self._inner.send_message(channel_id, content)
        )

    async def fetch_recent_messages(
        self, channel_id: int, limit: int = 50
    ) -> tuple[ChannelMessage, ...]:
        return await self._run(
            "fetch_recent_messages",
            lambda: self._inner.fetch_recent_messages(channel_id, limit),
        )

    async def add_reaction(self, channel_id: int, message_id: int, emoji: str) -> None:
        await self._run(
            "add_reaction",
            lambda: self._inner.add_reaction(channel_id, message_id, emoji),
        )

    async def press_component(
        self, channel_id: int, message_id: int, component_id: str
    ) -> InteractionResult:
        return await self._run(
            "press_component",
            lambda: self._inner.press_component(channel_id, message_id, component_id),
        )

    async def close(self) -> None:
        await self._inner.close()


class _AccountWorker:
    """One account's scheduling loop."""

    def __init__(self, account_id: int) -> None:
        self.account_id = account_id
        self.wake = asyncio.Event()
        self.task: asyncio.Task[None] | None = None

    def nudge(self) -> None:
        self.wake.set()


class WorkerManager:
    """Runs account workers concurrently with bounded, ordered scheduling."""

    def __init__(
        self,
        *,
        registry: TaskRegistry,
        store: TaskStateStore,
        limiter: RateLimiter,
        app_config: AppConfig,
        accounts: AccountsConfig,
        adapter_factory: Callable[[int], PlatformAdapter],
        credentials: CredentialProvider | None = None,
        idle_interval_seconds: float = 30.0,
        wall: Callable[[], datetime] = utc_now,
    ) -> None:
        self._registry = registry
        self._store = store
        self._limiter = limiter
        self._app_config = app_config
        self._accounts = accounts
        self._adapter_factory = adapter_factory
        self._credentials = credentials
        self._idle_interval = idle_interval_seconds
        self._wall = wall
        self._enabled: dict[int, bool] = {a.id: a.enabled for a in accounts.accounts}
        self._paused: set[int] = set()
        self._globally_paused = False
        self._shutdown = asyncio.Event()
        self._stopped = False
        self._started = False
        self._workers: dict[int, _AccountWorker] = {}
        self._adapters: dict[int, RateLimitedAdapter] = {}
        self._execution_slots = asyncio.Semaphore(app_config.settings.max_concurrent_accounts)

    # ----- lifecycle -----

    async def start(self) -> None:
        """Recover interrupted work, materialize instances, start workers."""
        if self._started:
            return
        self._started = True
        await self._store.recover_interrupted()
        for account_id in list(self._enabled):
            if self._enabled[account_id]:
                await self._materialize(account_id)
                self._spawn_worker(account_id)

    async def stop(self, *, timeout: float | None = None) -> None:
        """Graceful shutdown: drain workers, then close adapters.

        In-flight operations get ``timeout`` (default:
        ``settings.shutdown_timeout_seconds``) to finish; afterwards workers
        are cancelled and their instances deliberately stay ``running``
        (uncertain outcome; recovered on next start).
        """
        if self._stopped:
            return
        self._stopped = True
        effective_timeout = (
            self._app_config.settings.shutdown_timeout_seconds if timeout is None else timeout
        )
        self._shutdown.set()
        for worker in self._workers.values():
            worker.nudge()
        tasks = [w.task for w in self._workers.values() if w.task is not None]
        if tasks:
            try:
                await asyncio.wait_for(asyncio.gather(*tasks), timeout=effective_timeout)
            except TimeoutError:
                for task in tasks:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
        for adapter in self._adapters.values():
            await adapter.close()
        self._adapters.clear()

    async def __aenter__(self) -> WorkerManager:
        await self.start()
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        await self.stop()

    def _spawn_worker(self, account_id: int) -> None:
        worker = self._workers.get(account_id)
        if worker is not None and worker.task is not None and not worker.task.done():
            return
        worker = _AccountWorker(account_id)
        worker.task = asyncio.create_task(
            self._worker_loop(worker), name=f"autoclaimer-worker-{account_id}"
        )
        self._workers[account_id] = worker

    async def _materialize(self, account_id: int) -> None:
        profile = self._profile(account_id)
        if profile is None:
            return
        for task_id in profile.tasks:
            await self._store.create_instance(account_id, task_id)

    def _profile(self, account_id: int) -> AccountProfile | None:
        return next((a for a in self._accounts.accounts if a.id == account_id), None)

    # ----- worker loop -----

    async def _worker_loop(self, worker: _AccountWorker) -> None:
        while not self._shutdown.is_set():
            gated = (
                self._globally_paused
                or not self._enabled.get(worker.account_id, False)
                or worker.account_id in self._paused
            )
            if gated or self._shutdown.is_set():
                worker.wake.clear()
                await self._wait_for_wake(worker, None)
                continue
            record = await self._next_due(worker.account_id)
            if record is None:
                worker.wake.clear()
                await self._wait_for_wake(worker, await self._soon_delay(worker.account_id))
                continue
            await self._execute(record)

    async def _wait_for_wake(self, worker: _AccountWorker, timeout: float | None) -> None:
        if self._shutdown.is_set():
            return
        try:
            if timeout is None:
                await worker.wake.wait()
            else:
                await asyncio.wait_for(worker.wake.wait(), timeout=timeout)
        except TimeoutError:
            pass

    def _priority(self, task_id: str) -> int:
        task = self._app_config.tasks.get(task_id)
        return task.priority if task is not None else 0

    def _order(self, record: TaskInstanceRecord) -> tuple[int, str, int]:
        due_at = record.next_attempt_at or record.created_at
        return (-self._priority(record.task_id), due_at.isoformat(), record.id)

    async def _next_due(self, account_id: int) -> TaskInstanceRecord | None:
        records = await self._store.due_instances()
        candidates = [r for r in records if r.account_id == account_id]
        candidates.sort(key=self._order)
        return candidates[0] if candidates else None

    async def _soon_delay(self, account_id: int) -> float:
        """Seconds until this account's soonest retry, capped at the idle interval."""
        soonest: datetime | None = None
        for record in await self._store.list_instances_by_account(
            account_id, TaskInstanceState.WAITING
        ):
            if record.next_attempt_at is not None and (
                soonest is None or record.next_attempt_at < soonest
            ):
                soonest = record.next_attempt_at
        if soonest is None:
            return self._idle_interval
        return max(0.0, min((soonest - self._wall()).total_seconds(), self._idle_interval))

    async def _execute(self, record: TaskInstanceRecord) -> None:
        account_id = record.account_id
        async with self._execution_slots:
            if self._shutdown.is_set():
                return  # not claimed yet: instance stays pending
            try:
                claimed = await self._store.claim(record.id)
            except StateTransitionError:
                return  # already claimed or terminal: never execute twice
            except PersistenceError:
                logger.exception("Store error while claiming instance %s", record.id)
                return
            profile = self._profile(account_id)
            if profile is None:
                return
            resolved = resolve_task_config(self._app_config, profile, record.task_id)
            adapter = self._adapter_for(account_id)
            context = TaskContext(
                account_id=account_id,
                task_id=record.task_id,
                config=resolved.parameters,
                adapter=adapter,
                credentials=self._credentials,
            )
            try:
                outcome = await self._registry.execute(context)
            except RetryLimitExceededError as exc:
                await self._record(
                    record.id,
                    TaskOutcome(TaskStatus.FAILED_PERMANENT, message=str(exc)),
                    retry_at=None,
                )
                return
            except asyncio.CancelledError:
                raise  # uncertain outcome: instance stays running
            except Exception as exc:
                logger.exception("Task handler crashed for instance %s", record.id)
                await self._record(
                    record.id,
                    TaskOutcome(
                        TaskStatus.FAILED_PERMANENT,
                        message=f"{type(exc).__name__}: {exc}",
                    ),
                    retry_at=None,
                )
                return
        if outcome.status is TaskStatus.SUCCEEDED:
            await self._record(record.id, outcome, retry_at=None)
        elif outcome.status is TaskStatus.FAILED_RECOVERABLE:
            delay = self._retry_delay(claimed.attempt_count, account_id, record.task_id)
            await self._record(record.id, outcome, retry_at=self._wall() + timedelta(seconds=delay))
        else:
            await self._record(record.id, outcome, retry_at=None)

    async def _record(
        self, instance_id: int, outcome: TaskOutcome, *, retry_at: datetime | None
    ) -> None:
        if outcome.status is TaskStatus.SUCCEEDED:
            await self._store.record_success(instance_id, outcome)
        else:
            await self._store.record_failure(instance_id, outcome, retry_at=retry_at)

    def _retry_delay(self, attempts: int, account_id: int, task_id: str) -> float:
        policy = policy_for(self._app_config, self._accounts, account_id, task_id)
        base = policy.backoff_base_seconds * (2 ** max(0, attempts - 1))
        return float(min(base, policy.backoff_cap_seconds))

    def _adapter_for(self, account_id: int) -> RateLimitedAdapter:
        """Per-account rate-limited adapter.

        The limiter's default policy governs operations; per-task policy
        resolution at the operation level is a future refinement.
        """
        adapter = self._adapters.get(account_id)
        if adapter is None:
            adapter = RateLimitedAdapter(
                self._adapter_factory(account_id),
                self._limiter,
                account_id,
            )
            self._adapters[account_id] = adapter
        return adapter

    # ----- account controls -----

    def set_account_enabled(self, account_id: int, enabled: bool) -> None:
        """Enable/disable an account. Disabling stops new work only."""
        self._enabled[account_id] = enabled
        if enabled and account_id not in self._workers and not self._stopped:
            # Materialize instances and spawn the worker for this account.
            self._spawn_worker_enabled(account_id)
        worker = self._workers.get(account_id)
        if worker is not None:
            worker.nudge()

    def _spawn_worker_enabled(self, account_id: int) -> None:
        asyncio.get_running_loop().create_task(self._enable_account(account_id))

    async def _enable_account(self, account_id: int) -> None:
        await self._materialize(account_id)
        self._spawn_worker(account_id)

    def pause_account(self, account_id: int) -> None:
        self._paused.add(account_id)

    def resume_account(self, account_id: int) -> None:
        self._paused.discard(account_id)
        worker = self._workers.get(account_id)
        if worker is not None:
            worker.nudge()

    def pause_all(self) -> None:
        self._globally_paused = True

    def resume_all(self) -> None:
        self._globally_paused = False
        for worker in self._workers.values():
            worker.nudge()

    def wake(self, account_id: int) -> None:
        worker = self._workers.get(account_id)
        if worker is not None:
            worker.nudge()

    # ----- introspection -----

    async def queue(self, account_id: int) -> tuple[TaskInstanceRecord, ...]:
        """Pending/waiting instances for an account in scheduling order."""
        records: list[TaskInstanceRecord] = []
        for state in _ACCOUNT_QUEUE_STATES:
            records.extend(await self._store.list_instances_by_account(account_id, state))
        records.sort(key=self._order)
        return tuple(records)

    async def task_status(self, instance_id: int) -> TaskInstanceRecord:
        return await self._store.get_instance(instance_id)

    async def status(self) -> ManagerStatus:
        infos = []
        for account_id, worker in sorted(self._workers.items()):
            queued = len(
                [
                    r
                    for state in _ACCOUNT_QUEUE_STATES
                    for r in await self._store.list_instances_by_account(account_id, state)
                ]
            )
            state = (
                WorkerState.STOPPED
                if self._stopped or (worker.task is not None and worker.task.done())
                else (
                    WorkerState.PAUSED
                    if self._globally_paused or account_id in self._paused
                    else WorkerState.RUNNING
                )
            )
            infos.append(
                WorkerInfo(
                    account_id=account_id,
                    state=state,
                    enabled=self._enabled.get(account_id, False),
                    queued=queued,
                )
            )
        return ManagerStatus(workers=tuple(infos), globally_paused=self._globally_paused)
