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

from .adapters import (
    AccountFatalError,
    ChannelInfo,
    EventConsumer,
    InteractionResult,
    PlatformAdapter,
    SentMessage,
)
from .config import resolve_task_config
from .credentials import CredentialProvider
from .models import AccountProfile, AccountsConfig, AppConfig, ConfigValidationError
from .persistence import (
    DatabaseBusyError,
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
        logger.debug(
            "platform operation",
            extra={
                "account_id": self._account_id,
                "operation": op_name,
            },
        )
        return await self._limiter.run(
            fn,
            account_id=self._account_id,
            route=op_name,
            policy=self._policy,
        )

    async def send_message(
        self, channel_id: int, content: str, *, on_dispatch: Callable[[], None] | None = None
    ) -> SentMessage:
        return await self._run(
            "send_message",
            lambda: self._inner.send_message(channel_id, content, on_dispatch=on_dispatch),
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

    def subscribe(
        self,
        consumer: EventConsumer,
        *,
        on_events_lost: Callable[[int], None] | None = None,
    ) -> Callable[[], None]:
        """Inbound events are not rate-limited operations; pass through.

        ``on_events_lost`` forwards to the inner adapter so the loss/
        reconciliation signal is uniform across adapter implementations.
        """
        return self._inner.subscribe(consumer, on_events_lost=on_events_lost)

    async def get_channel(self, channel_id: int) -> ChannelInfo | None:
        """Cache-only channel lookup: not a platform operation, never limited."""
        return await self._inner.get_channel(channel_id)

    async def validate_channel(self, channel_id: int) -> bool:
        """Cache-only visibility check: not a platform operation, never limited."""
        return await self._inner.validate_channel(channel_id)

    def with_policy(self, policy: EffectivePolicy) -> RateLimitedAdapter:
        """A per-execution view of this adapter bound to *policy*.

        External-audit BUG-004 fix: the account-shared adapter carries no
        policy, so account/task ratelimit overrides were parsed but never
        applied to platform operations (the limiter default — global-only
        — was used). The worker resolves the effective policy per
        (account, task) — where the task id is known — and hands the
        handler this view. Shares the inner client, limiter, and account
        id; only the policy differs.
        """
        return RateLimitedAdapter(self._inner, self._limiter, self._account_id, policy=policy)


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
        self._fatal_accounts: set[int] = set()
        self._globally_paused = False
        self._shutdown = asyncio.Event()
        self._stopped = False
        self._started = False
        self._workers: dict[int, _AccountWorker] = {}
        self._adapters: dict[int, RateLimitedAdapter] = {}
        self._execution_slots = asyncio.Semaphore(app_config.settings.max_concurrent_accounts)

    # ----- lifecycle -----

    async def start(self) -> None:
        """Recover interrupted work, materialize instances, start workers.

        External audit BUG-001 fix: startup state is transactional —
        ``_started`` flips True only after the whole initialization
        sequence succeeds; a failure (e.g. transient ``DatabaseBusyError``
        in ``recover_interrupted``) leaves the manager startable again
        instead of silently no-op-ing every retry.
        """
        if self._started:
            return
        try:
            await self._start_sequence()
        except BaseException:
            # Re-raise after cleanup: callers see the failure and may
            # retry (the manager is NOT marked started).
            raise
        self._started = True

    async def _start_sequence(self) -> None:
        await self._store.recover_interrupted()
        # Audit fix (agents.md section 17.2.1): validate resolved task
        # parameters against the registry BEFORE spawning workers — a
        # deterministic configuration defect is a startup/task-config
        # failure, never a mid-run handler crash. Fail-soft per instance:
        # the invalid instance is abandoned; unrelated work continues.
        for account_id in list(self._enabled):
            if not self._enabled[account_id]:
                continue
            profile = self._profile(account_id)
            if profile is None:
                continue
            for task_id in profile.tasks:
                try:
                    resolved = resolve_task_config(self._app_config, profile, task_id)
                except ConfigValidationError:
                    continue  # handled at execution via _terminal_without_claim
                # Repo-sweep fix: `enabled: false` (task definition or
                # account override) is now CONSUMED — disabled assignments
                # materialize nothing and never execute (previously the
                # flag was parsed and silently ignored).
                if not resolved.enabled:
                    logger.info(
                        "task disabled by configuration; skipping",
                        extra={
                            "account_id": account_id,
                            "task_id": task_id,
                            "operation": "startup.validate",
                            "outcome": "disabled",
                        },
                    )
                    continue
                # Repo-sweep fix: an UNREGISTERED task type is a wiring
                # gap, not a parameter defect — do NOT create-and-fail
                # the instance (terminal rows are never re-created, so
                # the previous behavior permanently poisoned state.db for
                # the repo's own demo config).
                if task_id not in self._registry:
                    logger.error(
                        "task type not registered; skipping materialization",
                        extra={
                            "account_id": account_id,
                            "task_id": task_id,
                            "operation": "startup.validate",
                            "outcome": "unregistered_task_type",
                        },
                    )
                    continue
                try:
                    self._registry.validate_config(task_id, resolved.parameters)
                except Exception as exc:
                    logger.error(
                        "task parameters rejected at startup; instance will fail without executing",
                        extra={
                            "account_id": account_id,
                            "task_id": task_id,
                            "operation": "startup.validate",
                            "outcome": "invalid_parameters",
                        },
                    )
                    try:
                        await self._store.create_instance(account_id, task_id)
                        record = await self._store.get_instance_by_ids(account_id, task_id)
                        await self._terminal_without_claim(
                            record, f"invalid task parameters at startup: {exc}"
                        )
                    except PersistenceError:
                        logger.exception(
                            "could not abandon invalid instance at startup",
                            extra={
                                "account_id": account_id,
                                "task_id": task_id,
                                "operation": "startup.validate",
                            },
                        )
        # Adopt still-active persisted cooldowns BEFORE any worker can act:
        # server-directed deadlines must survive restarts (audit F1; a
        # no-op when the limiter has no store attached).
        await self._limiter.load_persisted_cooldowns()
        for account_id in list(self._enabled):
            if self._enabled[account_id]:
                await self._materialize(account_id)
                self._spawn_worker(account_id)
        logger.info("framework started")

    async def stop(self, *, timeout: float | None = None) -> None:
        """Graceful shutdown: drain workers, then close adapters.

        In-flight operations get ``timeout`` (default:
        ``settings.shutdown_timeout_seconds``) to finish; afterwards workers
        are cancelled and their instances deliberately stay ``running``
        (uncertain outcome; recovered on next start). The manager can be
        :meth:`start`\\ ed again afterwards (restartable). Cancellation of
        ``stop()`` itself completes the teardown and re-raises, so the
        manager is never left half-stopped (audit B2/B3).
        """
        if self._stopped:
            return
        self._stopped = True
        effective_timeout = (
            self._app_config.settings.shutdown_timeout_seconds if timeout is None else timeout
        )
        self._shutdown.set()
        try:
            for worker in self._workers.values():
                worker.nudge()
            tasks = [w.task for w in self._workers.values() if w.task is not None]
            if tasks:
                try:
                    await asyncio.wait_for(
                        asyncio.gather(*tasks, return_exceptions=True),
                        timeout=effective_timeout,
                    )
                except TimeoutError:
                    for task in tasks:
                        task.cancel()
                    await asyncio.gather(*tasks, return_exceptions=True)
        finally:
            # Adapters close in the finally: outer cancellation of stop()
            # must never leak live clients (N1). Adapter close() is
            # idempotent, so a retried stop() is harmless.
            for adapter in list(self._adapters.values()):
                try:
                    await adapter.close()
                except Exception:
                    logger.exception("adapter close failed during shutdown")
            self._adapters.clear()
            # Workers stay registered (their done tasks report STOPPED via
            # status()); _spawn_worker replaces finished workers on restart.
            # Reset lifecycle so the manager can be started again.
            self._shutdown.clear()
            self._stopped = False
            self._started = False
            logger.info("framework stopped")

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
        worker.task.add_done_callback(self._worker_finished)
        self._workers[account_id] = worker

    def _worker_finished(self, task: asyncio.Task[None]) -> None:
        """Fatal safety net: a dead worker task must never pass silently."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(
                "account worker task died: %s",
                exc,
                extra={"operation": "worker.task", "outcome": "worker_died"},
            )

    async def _materialize(self, account_id: int) -> None:
        profile = self._profile(account_id)
        if profile is None:
            return
        for task_id in profile.tasks:
            try:
                resolved = resolve_task_config(self._app_config, profile, task_id)
            except ConfigValidationError:
                # Startup validation already failed the instance; nothing
                # to materialize here.
                continue
            # Repo-sweep fix: `enabled: false` (task or override) is
            # consumed at materialization — disabled assignments never
            # get an instance row, on start() or on runtime enable.
            if not resolved.enabled:
                continue
            # Unregistered task types are a wiring gap, never a row.
            if task_id not in self._registry:
                continue
            await self._store.create_instance(account_id, task_id)

    def _profile(self, account_id: int) -> AccountProfile | None:
        return next((a for a in self._accounts.accounts if a.id == account_id), None)

    # ----- worker loop -----

    async def _worker_loop(self, worker: _AccountWorker) -> None:
        while not self._shutdown.is_set():
            if worker.account_id in self._fatal_accounts:
                logger.info(
                    "account worker stopped after fatal adapter failure",
                    extra={
                        "account_id": worker.account_id,
                        "operation": "worker.loop",
                        "outcome": "account_fatal_stop",
                    },
                )
                return  # task completes: status() reports STOPPED, no respawn
            try:
                await self._worker_iteration(worker)
            except asyncio.CancelledError:
                raise  # shutdown: propagate immediately
            except Exception:
                # A transient store/limiter failure must not silently kill
                # the account worker; log with context and back off one
                # idle interval before retrying (no busy-spin).
                logger.exception(
                    "worker loop iteration failed",
                    extra={
                        "account_id": worker.account_id,
                        "operation": "worker.loop",
                        "outcome": "iteration_error",
                    },
                )
                worker.wake.clear()
                await self._wait_for_wake(worker, self._idle_interval)

    async def _worker_iteration(self, worker: _AccountWorker) -> None:
        if self._shutdown.is_set():
            return
        gated = (
            self._globally_paused
            or not self._enabled.get(worker.account_id, False)
            or worker.account_id in self._paused
        )
        if gated or self._shutdown.is_set():
            worker.wake.clear()
            await self._wait_for_wake(worker, None)
            return
        record = await self._next_due(worker.account_id)
        if record is None:
            worker.wake.clear()
            await self._wait_for_wake(worker, await self._soon_delay(worker.account_id))
            return
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
        # SQL-side account filter: no more full-table scans per account.
        records = await self._store.due_instances(account_id)
        candidates = sorted(records, key=self._order)
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
        # Resolve configuration BEFORE claiming: a validation failure must
        # not strand a claimed (running) row forever (audit B1).
        profile = self._profile(account_id)
        if profile is None or record.task_id not in profile.tasks:
            await self._terminal_without_claim(
                record, "task not assigned to this account in current config"
            )
            return
        try:
            resolved = resolve_task_config(self._app_config, profile, record.task_id)
        except ConfigValidationError as exc:
            await self._terminal_without_claim(record, f"invalid task config: {exc}")
            return
        adapter = await self._adapter_for(account_id)
        # External-audit BUG-004 fix: bind the RESOLVED policy (global →
        # task → account) to the adapter used for THIS execution — the
        # account-shared adapter is policy-less, and handlers driving it
        # previously operated under the limiter default (global-only),
        # silently ignoring every per-task/per-account ratelimit block.
        policy = policy_for(self._app_config, self._accounts, account_id, record.task_id)
        task_adapter = adapter.with_policy(policy)
        # Wait out account cooldowns BEFORE taking a global execution slot
        # so parked accounts cannot starve others (audit B6).
        await self._limiter.wait_scopes(account_id)
        async with self._execution_slots:
            if self._shutdown.is_set():
                return  # not claimed yet: instance stays pending
            try:
                claimed = await self._store.claim(record.id)
            except StateTransitionError:
                return  # already claimed or terminal: never execute twice
            except PersistenceError:
                # Re-raise: the worker loop backs off one idle interval
                # instead of hot-spinning on the next poll (audit B4).
                raise
            context = TaskContext(
                account_id=account_id,
                task_id=record.task_id,
                config=resolved.parameters,
                adapter=task_adapter,
                credentials=self._credentials,
            )
            try:
                outcome = await self._registry.execute(context)
            except AccountFatalError as exc:
                # Credentials dead / gateway session invalidated / CAPTCHA:
                # fail this instance, then stop the whole account (owner
                # policy 2026-10-28) — never retry, never continue with
                # the account's remaining tasks.
                logger.error(
                    "account-fatal adapter failure; stopping account",
                    extra={
                        "account_id": account_id,
                        "task_id": record.task_id,
                        "operation": "task.execute",
                        "outcome": "account_fatal",
                    },
                )
                await self._record(
                    record.id,
                    TaskOutcome(
                        TaskStatus.FAILED_PERMANENT,
                        message=f"account-fatal: {exc}",
                    ),
                    retry_at=None,
                )
                await self._handle_account_fatal(account_id, str(exc))
                return
            except RetryLimitExceededError as exc:
                logger.warning(
                    "task execution exhausted retries",
                    extra={
                        "account_id": account_id,
                        "task_id": record.task_id,
                        "operation": "task.execute",
                        "outcome": "failed_permanent",
                    },
                )
                await self._record(
                    record.id,
                    TaskOutcome(TaskStatus.FAILED_PERMANENT, message=str(exc)),
                    retry_at=None,
                )
                return
            except asyncio.CancelledError:
                raise  # uncertain outcome: instance stays running
            except PersistenceError as exc:
                # Audit fix: a persistence fault raised INSIDE a handler
                # (deadline read/write) is a transient infrastructure
                # condition (§17.2.5 typed errors) — bounded recoverable
                # retry, never a terminal kill of a recurring task.
                logger.error(
                    "task handler hit a persistence error; recoverable",
                    extra={
                        "account_id": account_id,
                        "task_id": record.task_id,
                        "operation": "task.execute",
                        "outcome": "persistence_error",
                    },
                    exc_info=True,
                )
                await self._record(
                    record.id,
                    TaskOutcome(
                        TaskStatus.FAILED_RECOVERABLE,
                        message=f"persistence error: {type(exc).__name__}: {exc}",
                    ),
                    retry_at=None,
                )
                return
            except Exception as exc:
                logger.exception(
                    "task handler crashed",
                    extra={
                        "account_id": account_id,
                        "task_id": record.task_id,
                        "operation": "task.execute",
                        "outcome": "failed_permanent",
                    },
                )
                await self._record(
                    record.id,
                    TaskOutcome(
                        TaskStatus.FAILED_PERMANENT,
                        message=f"{type(exc).__name__}: {exc}",
                    ),
                    retry_at=None,
                )
                return
        logger.info(
            "task execution finished",
            extra={
                "account_id": account_id,
                "task_id": record.task_id,
                "operation": "task.execute",
                "outcome": outcome.status.value,
            },
        )
        if outcome.retry_at is not None and outcome.retry_at.tzinfo is None:
            # Consolidated review #6: validate ONCE for every status —
            # `_outcome_json` serializes retry_at regardless of status,
            # and persistence would reject a naive timestamp mid-record,
            # stranding the claimed row in `running`. A naive deadline is
            # a handler CONTRACT VIOLATION (§17.2.5): fail terminally
            # with an explicit message; supervision continues.
            logger.error(
                "handler returned a timezone-naive retry_at; failing attempt terminally",
                extra={
                    "account_id": account_id,
                    "task_id": record.task_id,
                    "operation": "task.execute",
                    "outcome": "invalid_retry_at",
                },
            )
            await self._record(
                record.id,
                TaskOutcome(
                    TaskStatus.FAILED_PERMANENT,
                    message="handler returned a timezone-naive retry_at; "
                    "deadline rejected at the worker boundary",
                ),
                retry_at=None,
            )
            return
        if outcome.status is TaskStatus.SUCCEEDED:
            if outcome.retry_at is not None:
                # Milestone 7 (recurring tasks): a successful ROUND with
                # a learned deadline stays alive — rescheduled with a
                # fresh attempt window instead of going terminal.
                await self._reschedule(record.id, outcome, outcome.retry_at)
                logger.info(
                    "task round succeeded; rescheduled at learned deadline",
                    extra={
                        "account_id": account_id,
                        "task_id": record.task_id,
                        "operation": "task.execute",
                        "outcome": "rescheduled",
                    },
                )
                return
            await self._record(record.id, outcome, retry_at=None)
        elif outcome.status is TaskStatus.FAILED_RECOVERABLE:
            # Worker-level retries are bounded by the RESOLVED TASK
            # CONFIG's retry_limit (repo-sweep fix: the task_defaults →
            # task → override chain was computed, test-asserted, and
            # then ignored — the ratelimit-layer policy bound was used
            # instead); beyond it the instance fails permanently (audit B8).
            resolved_limit = resolve_task_config(
                self._app_config, profile, record.task_id
            ).retry_limit
            if claimed.attempt_count >= resolved_limit + 1:
                await self._record(
                    record.id,
                    TaskOutcome(
                        TaskStatus.FAILED_PERMANENT,
                        message=f"worker retry limit ({resolved_limit}) exhausted",
                    ),
                    retry_at=None,
                )
                return
            if outcome.retry_at is not None:
                # Milestone 4 (retry ownership): a LEARNED absolute UTC
                # deadline (parsed from a Mudae reply, anchored at the
                # domain boundary) supersedes generic transport backoff.
                # An already-expired deadline means "due now". Naive
                # values were rejected above, for every status.
                retry_at = max(outcome.retry_at, self._wall())
            else:
                delay = self._retry_delay(claimed.attempt_count, account_id, record.task_id)
                retry_at = self._wall() + timedelta(seconds=delay)
            await self._record(record.id, outcome, retry_at=retry_at)
        else:
            await self._record(record.id, outcome, retry_at=None)

    async def _record(
        self, instance_id: int, outcome: TaskOutcome, *, retry_at: datetime | None
    ) -> None:
        if outcome.status is TaskStatus.SUCCEEDED:
            await self._store.record_success(instance_id, outcome)
        else:
            await self._store.record_failure(instance_id, outcome, retry_at=retry_at)

    async def _reschedule(self, instance_id: int, outcome: TaskOutcome, retry_at: datetime) -> None:
        """Reschedule a recurring round, with one bounded retry.

        Audit fix: a transient ``DatabaseBusyError`` after the durable
        claim previously stranded the row in ``running`` until restart.
        One retry (idempotent on success — ``reschedule_success`` treats
        an identical re-record as a no-op) survives transient contention;
        a second failure propagates to the worker loop guard (log +
        backoff; the row recovers on restart via ``recover_interrupted``).
        """
        try:
            await self._store.reschedule_success(instance_id, outcome, retry_at)
        except DatabaseBusyError:
            logger.warning(
                "reschedule hit a busy database; retrying once",
                extra={"operation": "task.record", "outcome": "busy_retry"},
            )
            await asyncio.sleep(0.5)
            await self._store.reschedule_success(instance_id, outcome, retry_at)

    def _retry_delay(self, attempts: int, account_id: int, task_id: str) -> float:
        policy = policy_for(self._app_config, self._accounts, account_id, task_id)
        # Clamp the exponent: 2**1024 overflows float beyond ~1025 attempts.
        exponent = min(max(0, attempts - 1), 62)
        base = policy.backoff_base_seconds * (2**exponent)
        return float(min(base, policy.backoff_cap_seconds))

    async def _handle_account_fatal(self, account_id: int, message: str) -> None:
        """Stop an account after a fatal adapter error (owner policy).

        Fails every queued instance for the account, disables it, marks
        it fatal (the worker loop exits, no adapter rebuilds), closes its
        adapter, and leaves a loud log trail. Re-enabling the account via
        ``set_account_enabled(account_id, True)`` is the explicit revive
        path once the underlying credential problem is fixed.
        """
        try:
            failed = await self._store.fail_account_queue(account_id, f"account-fatal: {message}")
        except PersistenceError:
            logger.exception(
                "could not sweep queue for fatal account",
                extra={
                    "account_id": account_id,
                    "operation": "account.fatal",
                    "outcome": "sweep_failed",
                },
            )
            failed = -1
        self._enabled[account_id] = False
        self._fatal_accounts.add(account_id)
        adapter = self._adapters.pop(account_id, None)
        if adapter is not None:
            try:
                await adapter.close()
            except Exception:
                logger.exception(
                    "adapter close failed after account-fatal error",
                    extra={
                        "account_id": account_id,
                        "operation": "account.fatal",
                        "outcome": "close_failed",
                    },
                )
        worker = self._workers.get(account_id)
        if worker is not None:
            worker.nudge()  # wake a parked loop so it observes the fatal flag
        logger.error(
            "account stopped after fatal adapter failure (queued instances failed: %s)",
            failed,
            extra={
                "account_id": account_id,
                "operation": "account.fatal",
                "outcome": "account_stopped",
            },
        )

    async def _terminal_without_claim(self, record: TaskInstanceRecord, reason: str) -> None:
        """Fail a never-claimed instance terminally (invalid config etc.)."""
        logger.warning(
            "task instance failed validation before execution",
            extra={
                "account_id": record.account_id,
                "task_id": record.task_id,
                "operation": "task.validate",
                "outcome": "failed_permanent",
            },
        )
        try:
            await self._store.record_abandoned(record.id, reason)
        except PersistenceError:
            logger.exception("could not record abandonment for instance %s", record.id)

    async def _adapter_for(self, account_id: int) -> RateLimitedAdapter:
        """Per-account rate-limited adapter.

        The limiter's default policy governs operations; per-task policy
        resolution at the operation level is a future refinement. The
        factory contract: each call must return a usable, unclosed adapter
        (evicted adapters are rebuilt through it). Accounts marked fatal
        never rebuild — their adapter is closed and dropped by
        ``_handle_account_fatal`` (a fresh adapter would re-login with
        the same dead token).
        """
        if account_id in self._fatal_accounts:
            # Defensive: a fatal account must never build a new adapter.
            raise AccountFatalError(
                f"account {account_id} is stopped after a fatal adapter failure"
            )
        adapter = self._adapters.get(account_id)
        if adapter is not None and not getattr(adapter._inner, "is_healthy", True):
            # Belt-and-braces: evict terminal adapters (the Discord adapter
            # also self-heals; this covers non-healing implementations).
            self._adapters.pop(account_id, None)
            await adapter.close()
            adapter = None
        if adapter is None:
            adapter = RateLimitedAdapter(
                self._adapter_factory(account_id),
                self._limiter,
                account_id,
            )
            self._adapters[account_id] = adapter
        return adapter

    # ----- account controls -----

    def account_enabled(self, account_id: int) -> bool:
        """Whether *account_id* is currently enabled."""
        return self._enabled.get(account_id, False)

    def set_account_enabled(self, account_id: int, enabled: bool) -> None:
        """Enable/disable an account. Disabling stops new work only.

        Policy: an unexpectedly dead worker is reported STOPPED by
        ``status()`` and is NOT auto-restarted; enabling (or
        disable-then-enable) always yields a live worker — a registered
        but finished task is replaced (PR #1 finding 6). A stopped or
        never-started manager defers spawning to ``start()``.
        """
        if account_id not in self._enabled:
            raise ValueError(f"unknown account {account_id}; not present in configuration")
        if enabled:
            # Explicit revive path after an account-fatal stop: the owner
            # re-enables the account once the credential problem is fixed.
            self._fatal_accounts.discard(account_id)
        self._enabled[account_id] = enabled
        if enabled and self._started and not self._stopped:
            worker = self._workers.get(account_id)
            dead = worker is None or worker.task is None or worker.task.done()
            if dead:
                # Runtime enable on a RUNNING manager: materialize + spawn
                # a replacement (also revives registered-but-dead workers).
                self._spawn_worker_enabled(account_id)
                return  # the fresh worker starts work on its own
        worker = self._workers.get(account_id)
        if worker is not None:
            worker.nudge()

    def _spawn_worker_enabled(self, account_id: int) -> None:
        task = asyncio.get_running_loop().create_task(self._enable_account(account_id))
        task.add_done_callback(self._detached_task_finished)

    def _detached_task_finished(self, task: asyncio.Task[None]) -> None:
        """Detached tasks must never fail silently (audit fix)."""
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            logger.error(
                "background account-enable failed: %s",
                exc,
                extra={"operation": "worker.enable", "outcome": "enable_failed"},
            )

    async def _enable_account(self, account_id: int) -> None:
        await self._materialize(account_id)
        if self._stopped or not self._started:
            return  # start() will spawn workers for enabled accounts
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
