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
from collections.abc import Awaitable, Callable, Mapping
from contextlib import suppress
from dataclasses import dataclass
from datetime import datetime, timedelta
from enum import Enum
from typing import Any, TypeVar

from .adapters import (
    AccountFatalError,
    ChannelInfo,
    EventConsumer,
    InteractionResult,
    PlatformAdapter,
    SentMessage,
)
from .audit import AuditLog
from .config import resolve_task_config
from .credentials import CredentialProvider
from .health import AccountHealthMonitor, AccountHealthSnapshot
from .models import (
    AccountProfile,
    AccountsConfig,
    AppConfig,
    ConfigValidationError,
    ResolvedTaskConfig,
)
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
        on_rate_limited: Callable[[], None] | None = None,
    ) -> None:
        self._inner = inner
        self._limiter = limiter
        self._account_id = account_id
        self._policy = policy
        self._on_rate_limited = on_rate_limited

    async def _run(self, op_name: str, fn: Callable[[], Awaitable[_T]]) -> _T:
        logger.debug(
            "platform operation",
            extra={
                "account_id": self._account_id,
                "operation": op_name,
            },
        )
        try:
            return await self._limiter.run(
                fn,
                account_id=self._account_id,
                route=op_name,
                policy=self._policy,
            )
        except RetryLimitExceededError:
            # Transport retries exhausted — most often a cooldown storm:
            # surface the account as rate-limited (observation only; the
            # limiter keeps owning deferral).
            if self._on_rate_limited is not None:
                self._on_rate_limited()
            raise

    async def send_message(
        self, channel_id: int, content: str, *, on_dispatch: Callable[[], None] | None = None
    ) -> SentMessage:
        return await self._run(
            "send_message",
            lambda: self._inner.send_message(channel_id, content, on_dispatch=on_dispatch),
        )

    async def add_reaction(
        self,
        channel_id: int,
        message_id: int,
        emoji: str,
        *,
        on_dispatch: Callable[[], None] | None = None,
    ) -> None:
        await self._run(
            "add_reaction",
            lambda: self._inner.add_reaction(
                channel_id, message_id, emoji, on_dispatch=on_dispatch
            ),
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

        BUG-031: the per-execution view is the ONLY adapter handlers ever
        see, so it must also carry the health ``on_rate_limited`` hook —
        dropping it made the RATE_LIMITED state unreachable from real
        task execution.
        """
        return RateLimitedAdapter(
            self._inner,
            self._limiter,
            self._account_id,
            policy=policy,
            on_rate_limited=self._on_rate_limited,
        )


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
        health: AccountHealthMonitor | None = None,
    ) -> None:
        self._registry = registry
        self._store = store
        self._limiter = limiter
        self._app_config = app_config
        self._accounts = accounts
        self._adapter_factory = adapter_factory
        # Account health observation (roadmap item 6): visibility only —
        # enforcement stays with the fatal-stop / enabled / limiter paths.
        self._health = health if health is not None else AccountHealthMonitor()
        # Structured audit events (roadmap item 11): auxiliary — emit
        # failures never break the audited operation.
        self._audit = AuditLog(store)
        # BUG-030: accounts whose enabled flag was changed at runtime
        # (enable/disable command, account-fatal stop). Runtime control
        # outranks a later reload; the reload reports the shadowing
        # instead of silently applying (or ignoring) the config value.
        self._runtime_enabled_overrides: set[int] = set()
        self._credentials = credentials
        self._idle_interval = idle_interval_seconds
        self._wall = wall
        # LOW-033: strong references for fire-and-forget audit emissions
        # (the event loop holds only weak task references — an
        # unreferenced emission can be garbage-collected mid-flight).
        self._audit_tasks: set[asyncio.Task[None]] = set()
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
        await self._apply_configuration()
        # Adopt still-active persisted cooldowns BEFORE any worker can act:
        # server-directed deadlines must survive restarts (audit F1; a
        # no-op when the limiter has no store attached).
        await self._limiter.load_persisted_cooldowns()
        # BUG-033: startup is all-or-nothing. Materialize EVERY account
        # before the first worker spawns — every await that can fail now
        # happens while no worker exists, so a failed start (with
        # ``_started`` False) never leaves live workers behind. A retry
        # would otherwise run ``recover_interrupted`` against a live
        # worker's claimed row and execute it twice (duplicate side
        # effects). The spawn loop itself is synchronous and cannot fail
        # partway.
        for account_id in list(self._enabled):
            if self._enabled[account_id]:
                await self._materialize(account_id)
        for account_id in list(self._enabled):
            if self._enabled[account_id]:
                self._spawn_worker(account_id)
        logger.info("framework started")

    async def _apply_configuration(self) -> None:
        """Validate the CURRENT configuration and materialize instances.

        Shared by startup and runtime reload (roadmap item 13): fail-soft
        per instance — an invalid assignment is abandoned, unrelated work
        continues.
        """
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
                    self._registry.validate_config(
                        task_id,
                        self._execution_parameters(self._app_config, resolved, task_id),
                    )
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

    def update_configuration(self, app: AppConfig, accounts: AccountsConfig) -> None:
        """Atomically adopt a newly validated configuration (roadmap 13).

        One SYNCHRONOUS method with no yield points: worker iterations
        either see the old (app, accounts) pair or the new one — never a
        mix. In-flight executions are generation-pinned: each captures
        ONE (app, accounts) reference before its first await and reads
        EVERY piece of its configuration from it — parameters, channels,
        rate-limit policy, and retry bounds — so a swap landing between
        the execution's awaits can never mix generations. After the
        swap, new/changed assignments apply from the next iteration;
        call :meth:`reload_materialize` for materialization and worker
        spawning.
        """
        self._app_config = app
        self._accounts = accounts

    async def reload_materialize(self) -> list[str]:
        """Post-swap materialization: create instances for newly assigned
        tasks, spawn workers for newly added accounts, nudge everyone so
        changes take effect promptly.

        BUG-030: the reloaded ``enabled`` flag is AUTHORITATIVE for every
        account the operator did not control at runtime — previously the
        startup-time overlay won forever and the reload silently ignored
        configuration changes. Accounts with a runtime override (explicit
        enable/disable, account-fatal stop) keep their runtime value;
        each shadowing is returned as an operator-facing note.

        Returns notes (runtime overrides that shadowed the config).
        """
        notes: list[str] = []
        for profile in self._accounts.accounts:
            if profile.id in self._runtime_enabled_overrides:
                runtime = self._enabled.get(profile.id, profile.enabled)
                if runtime != profile.enabled:
                    notes.append(
                        f"account {profile.id}: runtime enabled={runtime} kept "
                        f"(config enabled={profile.enabled} shadowed — "
                        "use enable/disable to change it)"
                    )
                continue  # runtime control outranks the reload
            self._enabled[profile.id] = profile.enabled
        # BUG-032: an account REMOVED from the configuration parks its
        # worker (the iteration gate stops all execution) WITHOUT
        # retiring its rows — removal may be temporary and terminal
        # retirement is irreversible. The parked worker is dormant (it
        # waits on its wake event, not a poll loop); re-adding the
        # account in a later reload resumes it.
        configured_ids = {profile.id for profile in self._accounts.accounts}
        for account_id in list(self._enabled):
            if account_id not in configured_ids:
                self._enabled[account_id] = False
        for profile in self._accounts.accounts:
            if self._enabled.get(profile.id, profile.enabled):
                await self._materialize(profile.id)
                worker = self._workers.get(profile.id)
                if worker is None or worker.task is None or worker.task.done():
                    # RACE-001: a stop() can complete while this reload is
                    # suspended at materialize — spawning then resurrects
                    # a worker for a STOPPED manager (the audit probe
                    # observed 20 post-stop executions). Same guard the
                    # enable path already applies; _start_sequence's spawn
                    # loop runs before _started flips, so the guard lives
                    # HERE, not in _spawn_worker.
                    if not self._stopped and self._started:
                        self._spawn_worker(profile.id)
        for worker in self._workers.values():
            worker.nudge()
        return notes

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
            # LOW-033 (review F1): bounded drain of outstanding audit
            # emissions so control events fired just before shutdown still
            # land. INSIDE the try: a cancellation while it waits runs the
            # finally below (adapters closed, lifecycle reset) and then
            # re-raises — stop()'s documented contract (audit B2/B3).
            if self._audit_tasks:
                await asyncio.wait(set(self._audit_tasks), timeout=min(1.0, effective_timeout))
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
            # BUG-034 (review round): a cancelled stop() can interrupt the
            # worker drain before it ran. Nudge every worker (a gated
            # worker — globally paused, disabled, or removed — waits on
            # its wake event with NO timeout), then cancel any worker
            # still alive: the interrupted drain's own cancel never
            # reached them, and a worker re-entering its gated wait after
            # _shutdown.clear() would park forever. Instances of
            # cancelled mid-execution workers deliberately stay running
            # (uncertain outcome; recovered on next start) — the same
            # semantics as the graceful-budget timeout path above.
            for worker in self._workers.values():
                worker.nudge()
            stragglers = [
                w.task for w in self._workers.values() if w.task is not None and not w.task.done()
            ]
            for task in stragglers:
                task.cancel()
            # BUG-037: AWAIT the cancelled workers before closing adapters
            # and resetting lifecycle state — stop() must not return while
            # a worker is still unwinding: an immediate restart would run
            # recover_interrupted() against the still-live worker's
            # claimed row (duplicate-execution race). Mid-execution
            # instances deliberately stay running (uncertain outcome;
            # recovered on next start), matching the budget-timeout path.
            if stragglers:
                await asyncio.gather(*stragglers, return_exceptions=True)
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
        self._health.worker_started(account_id)
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
        return self._profile_in(self._accounts, account_id)

    @staticmethod
    def _profile_in(accounts: AccountsConfig, account_id: int) -> AccountProfile | None:
        """Profile lookup against a SPECIFIC configuration generation
        (reload race: an execution must read its account from the same
        snapshot as its task configuration)."""
        return next((a for a in accounts.accounts if a.id == account_id), None)

    def _execution_parameters(
        self, app: AppConfig, resolved: ResolvedTaskConfig, task_id: str
    ) -> dict[str, Any]:
        """The parameter mapping a task's validator and handler see.

        Global-channel integration: a task declaring a ``channel_role``
        receives its resolved channel ID injected as ``channel`` — the
        ONE place effective channels reach task execution. Handlers read
        ``config["channel"]`` and never resolve fallbacks themselves;
        users cannot set ``channel`` anywhere (config parsing rejects
        it), so per-task values cannot defeat the global configuration.

        *app* pins the configuration generation (reload race): the
        channel is read from the SAME snapshot that resolved the
        parameters, never from a later swap.
        """
        parameters = dict(resolved.parameters)
        task_type = self._registry.get(task_id)  # membership pre-checked by callers
        if task_type.channel_role is not None:
            parameters["channel"] = app.channels.effective().for_role(task_type.channel_role)
        return parameters

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
        await self._execute(worker, record)

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

    async def _execute(self, worker: _AccountWorker, record: TaskInstanceRecord) -> None:
        account_id = record.account_id
        # Review F2 (BUG-032 follow-up): re-check the account gate — an
        # iteration already past _worker_iteration's gate when a reload
        # removed or disabled the account must PARK (no claim, no
        # retirement), never terminally fail the row through the missing
        # profile. Same pacing as the disabled-task gate below.
        if not self._enabled.get(account_id, False):
            logger.info(
                "account disabled/removed by configuration; skipping execution",
                extra={
                    "account_id": account_id,
                    "task_id": record.task_id,
                    "operation": "task.execute",
                    "outcome": "account_parked",
                },
            )
            worker.wake.clear()
            await self._wait_for_wake(worker, self._idle_interval)
            return
        # Resolve configuration BEFORE claiming: a validation failure must
        # not strand a claimed (running) row forever (audit B1).
        # Generation snapshot (reload race): ONE (app, accounts) reference
        # captured before the first await feeds EVERY configuration read
        # of this execution — parameters, channels, policy, retry bounds.
        # Reading self._app_config again after the awaits (wait_scopes,
        # slot, claim) could mix generations when a reload swaps the
        # configuration mid-execution (old parameters on the new channel,
        # a swapped retry bound); the captured references are immutable,
        # so this execution stays wholly on its own generation.
        app = self._app_config
        accounts = self._accounts
        profile = self._profile_in(accounts, account_id)
        if profile is None or record.task_id not in profile.tasks:
            await self._terminal_without_claim(
                record, "task not assigned to this account in current config"
            )
            return
        # ChatGPT PR #5 review (BUG-010): a persisted instance can
        # OUTLIVE the registry entry that created it (the task is still
        # defined and assigned in config, but the type is no longer
        # registered — e.g. a removed plugin). Retire it terminally
        # BEFORE claiming: parameter resolution below would otherwise
        # raise outside the handler-error boundary and strand the
        # claimed row in `running` until restart. Documented trade-off:
        # stale instances become terminal instead of waiting for a
        # future re-registration.
        if record.task_id not in self._registry:
            await self._terminal_without_claim(
                record, "task type is no longer registered; instance retired"
            )
            return
        try:
            resolved = resolve_task_config(app, profile, record.task_id)
        except ConfigValidationError as exc:
            await self._terminal_without_claim(record, f"invalid task config: {exc}")
            return
        # BUG-029: `enabled: false` gates EXECUTION, not just
        # materialization. A due waiting row (recurring reschedule,
        # recoverable retry, crash recovery) outlives the config change
        # and would otherwise keep executing — indefinitely for
        # recurring tasks, whose every success reschedules a new round.
        # The row is left pending/waiting (pause semantics: re-enabling
        # resumes it); the loop paces one idle interval because the row
        # stays due while disabled (no hot spin).
        if not resolved.enabled:
            logger.info(
                "task disabled by configuration; skipping execution",
                extra={
                    "account_id": account_id,
                    "task_id": record.task_id,
                    "operation": "task.execute",
                    "outcome": "disabled",
                },
            )
            worker.wake.clear()
            await self._wait_for_wake(worker, self._idle_interval)
            return
        adapter = await self._adapter_for(account_id)
        # External-audit BUG-004 fix: bind the RESOLVED policy (global →
        # task → account) to the adapter used for THIS execution — the
        # account-shared adapter is policy-less, and handlers driving it
        # previously operated under the limiter default (global-only),
        # silently ignoring every per-task/per-account ratelimit block.
        policy = policy_for(app, accounts, account_id, record.task_id)
        task_adapter = adapter.with_policy(policy)
        # Wait out account cooldowns BEFORE taking a global execution slot
        # so parked accounts cannot starve others (audit B6).
        await self._limiter.wait_scopes(account_id)
        async with self._execution_slots:
            if self._shutdown.is_set():
                return  # not claimed yet: instance stays pending
            try:
                claimed = await self._store.claim(record.id)
                await self._audit.emit(
                    category="task",
                    action="started",
                    account_id=account_id,
                    task_id=record.task_id,
                    detail=f"attempt {claimed.attempt_count}",
                )
            except StateTransitionError:
                return  # already claimed or terminal: never execute twice
            except PersistenceError:
                # Re-raise: the worker loop backs off one idle interval
                # instead of hot-spinning on the next poll (audit B4).
                raise
            context = TaskContext(
                account_id=account_id,
                task_id=record.task_id,
                config=self._execution_parameters(app, resolved, record.task_id),
                adapter=task_adapter,
                credentials=self._credentials,
            )
            try:
                outcome = await self._run_handler(context, resolved)
            except AccountFatalError as exc:
                self._health.record_failure(account_id, exc, fatal=True)
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
                await self._handle_account_fatal_after(
                    record,
                    TaskOutcome(
                        TaskStatus.FAILED_PERMANENT,
                        message=f"account-fatal: {exc}",
                    ),
                    account_id,
                    str(exc),
                )
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
                # CLOSURE-001: this previously passed retry_at=None,
                # TERMINALLY failing the row on its first transient fault
                # (a failed_recoverable outcome persisted on a terminal
                # row) — now the same bounded retry the handler-reported
                # recoverable outcome gets.
                await self._record_recoverable(
                    record,
                    claimed,
                    TaskOutcome(
                        TaskStatus.FAILED_RECOVERABLE,
                        message=f"persistence error: {type(exc).__name__}: {exc}",
                    ),
                    resolved_limit=resolve_task_config(app, profile, record.task_id).retry_limit,
                    app=app,
                    accounts=accounts,
                )
                return
            except Exception as exc:
                self._health.record_failure(account_id, exc)
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
            self._health.record_success(account_id)
            await self._audit.emit(
                category="task",
                action="completed",
                account_id=account_id,
                task_id=record.task_id,
                detail=outcome.message,
            )
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
            self._health.record_failure(account_id, outcome.message)
            # Worker-level retries are bounded by the RESOLVED TASK
            # CONFIG's retry_limit (repo-sweep fix: the task_defaults →
            # task → override chain was computed, test-asserted, and
            # then ignored — the ratelimit-layer policy bound was used
            # instead); beyond it the instance fails permanently (audit B8).
            await self._record_recoverable(
                record,
                claimed,
                outcome,
                resolved_limit=resolve_task_config(app, profile, record.task_id).retry_limit,
                app=app,
                accounts=accounts,
            )
        else:
            # Terminal failure returned by the handler (PERMANENT).
            self._health.record_failure(account_id, outcome.message)
            await self._audit.emit(
                category="task",
                action="failed",
                account_id=account_id,
                task_id=record.task_id,
                detail=outcome.message,
            )
            await self._record(record.id, outcome, retry_at=None)

    async def _run_handler(self, context: TaskContext, resolved: ResolvedTaskConfig) -> TaskOutcome:
        """Execute the handler under the resolved timeout (roadmap item 7).

        Timeout semantics (defined before implementation):
        - A timed-out handler is CANCELLED and the child task AWAITED, so
          its cleanup runs and it cannot continue in the background.
        - A timeout is DISTINCT from a handler raising TimeoutError itself
          (``asyncio.wait`` + ``task.result()`` preserves exception
          identity — only a still-pending task after the deadline is a
          timeout).
        - A timeout returns FAILED_RECOVERABLE with a distinct message, so
          the single-owner retry bound and rescheduling apply unchanged.
        - Cancellation of THIS coroutine (shutdown) cancels the child too —
          no leaked handler tasks.
        """
        timeout = resolved.timeout_seconds
        if timeout is None:
            return await self._registry.execute(context)

        task: asyncio.Task[TaskOutcome] = asyncio.create_task(self._registry.execute(context))
        try:
            _done, pending = await asyncio.wait({task}, timeout=timeout)
        except asyncio.CancelledError:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task
            raise
        if pending:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task  # cleanup completes; nothing keeps running
            logger.warning(
                "task execution timed out",
                extra={
                    "account_id": context.account_id,
                    "task_id": context.task_id,
                    "operation": "task.execute",
                    "outcome": "task_timeout",
                },
            )
            return TaskOutcome(
                TaskStatus.FAILED_RECOVERABLE,
                message=f"task execution timed out after {timeout}s",
            )
        return task.result()  # handler exceptions keep their identity

    async def _handle_account_fatal_after(
        self,
        record: TaskInstanceRecord,
        outcome: TaskOutcome,
        account_id: int,
        message: str,
    ) -> None:
        """Record the fatal outcome, then ALWAYS contain the account.

        ERR-003: the outcome write and the containment were sequential —
        a PersistenceError from ``_record`` escaped the except-block and
        ``_handle_account_fatal`` never ran (account stayed enabled,
        adapter stayed open, row stranded running). Containment is the
        owner policy (2026-10-28: never retry, never continue) and must
        be unconditional; a failed outcome write leaves the row in the
        documented restart-recovery bucket.
        """
        try:
            await self._record(record.id, outcome, retry_at=None)
        except PersistenceError:
            logger.exception(
                "could not record the account-fatal outcome (row recovers at restart)",
                extra={
                    "account_id": account_id,
                    "task_id": record.task_id,
                    "operation": "task.record",
                    "outcome": "fatal_record_failed",
                },
            )
        await self._handle_account_fatal(account_id, message)

    async def _record_recoverable(
        self,
        record: TaskInstanceRecord,
        claimed: TaskInstanceRecord,
        outcome: TaskOutcome,
        *,
        resolved_limit: int,
        app: AppConfig,
        accounts: AccountsConfig,
    ) -> None:
        """Record a FAILED_RECOVERABLE outcome under the retry bound.

        CLOSURE-001: shared by handler-reported recoverable failures and
        handler-raised PersistenceError (both recoverable; the latter was
        previously terminal-killed via ``retry_at=None``).

        CLOSURE-003: the audit event follows the LIMIT DECISION — an
        exhausted attempt emits ``task.failed`` (with the exhaustion
        detail), never a phantom ``task.retried`` for a retry that will
        not happen.
        """
        account_id = record.account_id
        if claimed.attempt_count >= resolved_limit + 1:
            await self._audit.emit(
                category="task",
                action="failed",
                account_id=account_id,
                task_id=record.task_id,
                detail=(
                    f"worker retry limit ({resolved_limit}) exhausted; "
                    f"last failure: {outcome.message}"
                ),
            )
            await self._record(
                record.id,
                TaskOutcome(
                    TaskStatus.FAILED_PERMANENT,
                    # Carry the LAST failure reason into the terminal
                    # row (roadmap items 7/8): the exhaustion alone
                    # hides whether it was timeouts, cooldowns, or
                    # handler-reported failures.
                    message=(
                        f"worker retry limit ({resolved_limit}) exhausted; "
                        f"last failure: {outcome.message}"
                    ),
                ),
                retry_at=None,
            )
            return
        if outcome.retry_at is not None:
            # Milestone 4 (retry ownership): a LEARNED absolute UTC
            # deadline (parsed from a Mudae reply, anchored at the
            # domain boundary) supersedes generic transport backoff.
            # An already-expired deadline means "due now". Naive values
            # were rejected above, for every status.
            retry_at = max(outcome.retry_at, self._wall())
        else:
            delay = self._retry_delay(
                claimed.attempt_count,
                account_id,
                record.task_id,
                app=app,
                accounts=accounts,
            )
            retry_at = self._wall() + timedelta(seconds=delay)
        await self._audit.emit(
            category="task",
            action="retried",
            account_id=account_id,
            task_id=record.task_id,
            detail=outcome.message,
        )
        await self._record(record.id, outcome, retry_at=retry_at)

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

    def _retry_delay(
        self,
        attempts: int,
        account_id: int,
        task_id: str,
        *,
        app: AppConfig,
        accounts: AccountsConfig,
    ) -> float:
        # app/accounts: the execution's own generation (reload race) —
        # the backoff policy must not swap mid-execution either.
        policy = policy_for(app, accounts, account_id, task_id)
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
        # BUG-030: a fatal stop is runtime control — a later reload must
        # not silently resurrect the account (revive-only policy).
        self._runtime_enabled_overrides.add(account_id)
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
                on_rate_limited=lambda: self._health.record_rate_limited(account_id),
            )
            self._adapters[account_id] = adapter
        return adapter

    def health(self) -> Mapping[int, AccountHealthSnapshot]:
        """Per-account health views for this session (roadmap item 6)."""
        return self._health.snapshot()

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
            self._health.record_revived(account_id)
        else:
            self._health.record_disabled(account_id)
        self._enabled[account_id] = enabled
        # BUG-030: explicit runtime control — outranks a later reload
        # (which reports the shadowing instead of applying the config).
        self._runtime_enabled_overrides.add(account_id)
        self._audit_emit(
            category="account",
            action="enabled" if enabled else "disabled",
            account_id=account_id,
        )
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
        self._audit_emit(category="account", action="paused", account_id=account_id)

    def resume_account(self, account_id: int) -> None:
        self._paused.discard(account_id)
        worker = self._workers.get(account_id)
        if worker is not None:
            worker.nudge()
        self._audit_emit(category="account", action="resumed", account_id=account_id)

    def pause_all(self) -> None:
        self._globally_paused = True
        self._audit_emit(category="runtime", action="paused")

    def resume_all(self) -> None:
        self._globally_paused = False
        for worker in self._workers.values():
            worker.nudge()
        self._audit_emit(category="runtime", action="resumed")

    def _audit_emit(self, **event: Any) -> None:
        """Fire-and-forget audit emission from sync control-plane methods
        (roadmap item 11): best-effort, never raises, no-op without a
        running loop (sync unit-test callers).

        LOW-033: the pending emission is strongly referenced (see
        ``_audit_tasks``) so the collector can never drop it mid-flight;
        ``stop()`` gives outstanding emissions a bounded chance to land.
        """
        try:
            task: asyncio.Task[None] = asyncio.ensure_future(self._audit.emit(**event))
        except RuntimeError:
            return  # no running loop: skip (the operation itself succeeded)
        self._audit_tasks.add(task)
        task.add_done_callback(self._audit_tasks.discard)

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
