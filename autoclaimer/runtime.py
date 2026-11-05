"""Production composition root (ROADMAP item 26).

Constructs and owns every runtime dependency — configuration, persistence,
credentials, task registry, rate limiter, health monitor, lifecycle wiring,
and the controller — WITHOUT executing anything. The design note
(ownership graph plus startup/shutdown sequences, with per-resource
failure and cancellation behavior) lives in ``docs/architecture.md``.

Startup posture:

- **Production** (default): fail closed. A missing/invalid encryption
  key, a missing per-account token, or an enabled account assigned to an
  unregistered task aborts startup with an actionable error AFTER every
  already-opened resource is closed in reverse ownership order. Nothing
  silently degrades; no account ever logs in during construction (the
  worker opens adapters lazily on first execution).
- **Demo** (``--demo``): the historical mock composition — ping task,
  ``MockAdapter``, credential management available when a key is present
  and a warning when it is not.
"""

from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path

from .adapters import PlatformAdapter
from .audit import AuditLog
from .cli import AutoClaimerController
from .config import load_config
from .credential_store import CredentialKeyError, SQLiteCredentialStore, load_fernet_keys
from .discord_adapter import DiscordAdapter, LifecycleTransition
from .health import AccountHealthMonitor
from .logging_setup import setup_logging
from .mock_adapter import MockAdapter
from .models import AccountsConfig, AppConfig
from .mudae_tasks import register_mudae_tasks
from .persistence import TaskStateStore
from .ratelimit import RateLimiter, resolve_policy
from .registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType

logger = logging.getLogger(__name__)


class RuntimeStartupError(Exception):
    """Startup cannot proceed: the message is the actionable operator fix."""


@dataclass
class Runtime:
    """An assembled, owned runtime. Construction is separate from
    execution: nothing here has logged into Discord or claimed work.

    ``shutdown()`` delegates to the controller's terminal one-shot
    teardown (workers drained within the shutdown budget, adapters
    closed — which drains every lifecycle observer — then the credential
    and state stores). Idempotent.
    """

    app: AppConfig
    accounts: AccountsConfig
    store: TaskStateStore
    credential_store: SQLiteCredentialStore | None
    registry: TaskRegistry
    limiter: RateLimiter
    health: AccountHealthMonitor
    controller: AutoClaimerController
    audit: AuditLog
    adapter_factory: Callable[[int], PlatformAdapter]
    demo: bool = False

    async def shutdown(self) -> None:
        """See the controller's shutdown contract; safe to call twice."""
        await self.controller.shutdown()


def build_demo_registry() -> TaskRegistry:
    """Placeholder task set exercising the mock adapter."""

    async def ping_handler(context: TaskContext) -> TaskOutcome:
        sent = await context.adapter.send_message(1, "ping")
        return TaskOutcome(TaskStatus.SUCCEEDED, data={"message_id": sent.message_id})

    registry = TaskRegistry()
    registry.register(
        TaskType(
            id="ping",
            description="Send one message through the mock adapter.",
            validate_config=lambda config: None,
            handler=ping_handler,
        )
    )
    return registry


def _make_lifecycle_hook(
    account_id: int,
    health: AccountHealthMonitor,
    audit: AuditLog,
) -> Callable[[LifecycleTransition], Awaitable[None] | None]:
    """Gateway transitions → account health (sync) + audit event (the
    returned coroutine is tracked and drained by the adapter's close)."""

    def hook(transition: LifecycleTransition) -> Awaitable[None] | None:
        if transition is LifecycleTransition.CONNECTED:
            health.record_connected(account_id)
            return audit.emit(category="account", action="connected", account_id=account_id)
        if transition is LifecycleTransition.DISCONNECTED:
            health.record_disconnected(account_id)
            return audit.emit(category="account", action="disconnected", account_id=account_id)
        # FATAL: the adapter already refused re-login; mirror the worker's
        # containment observation (idempotent latch) and audit it. When a
        # worker IS mid-execution it records the same fatal moments later —
        # the latch is unchanged and only the failure STATISTICS count
        # twice (accepted: the hook is the only recorder when the fatal
        # lands while no execution is in flight).
        health.record_failure(account_id, "gateway connection fatal", fatal=True)
        return audit.emit(category="account", action="fatal", account_id=account_id)

    return hook


async def build_runtime(
    config_path: str,
    accounts_path: str,
    *,
    demo: bool = False,
    environ: dict[str, str] | None = None,
) -> Runtime:
    """Assemble the runtime per the design note; raise
    :class:`RuntimeStartupError` (after closing everything already opened)
    when production prerequisites are missing."""
    app, accounts = load_config(config_path, accounts_path)
    setup_logging(app.logging)
    # The state DB lives next to the config (never relocatable by log_file).
    db_path = Path(config_path).resolve().parent / "state.db"

    store: TaskStateStore | None = None
    credential_store: SQLiteCredentialStore | None = None
    try:
        store = TaskStateStore(db_path)
        await store.open()

        if demo:
            # Historical posture: credential MANAGEMENT available when a
            # key is present; a warning (not an abort) when it is not.
            try:
                credential_store = SQLiteCredentialStore(db_path, load_fernet_keys(environ))
                await credential_store.open()
            except CredentialKeyError:
                logger.warning(
                    "encrypted credential storage disabled: no valid Fernet key "
                    "(set AUTOCLAIMER_FERNET_KEY; see README)",
                    extra={"operation": "cli.start", "outcome": "credential_key_missing"},
                )
                credential_store = None
            registry = build_demo_registry()
            limiter = RateLimiter(default_policy=resolve_policy(app.ratelimit), store=store)
            health = AccountHealthMonitor()
            controller = AutoClaimerController(
                app_config=app,
                accounts=accounts,
                store=store,
                limiter=limiter,
                registry=registry,
                adapter_factory=lambda account_id: MockAdapter(),
                credentials=credential_store,
                credential_store=credential_store,
                idle_interval_seconds=5.0,
                config_path=config_path,
                accounts_path=accounts_path,
                health=health,
            )
            return Runtime(
                app=app,
                accounts=accounts,
                store=store,
                credential_store=credential_store,
                registry=registry,
                limiter=limiter,
                health=health,
                controller=controller,
                audit=AuditLog(store),
                adapter_factory=lambda account_id: MockAdapter(),
                demo=True,
            )

        # ----- production: fail closed at every boundary -----
        try:
            keys = load_fernet_keys(environ)
        except CredentialKeyError as exc:
            raise RuntimeStartupError(
                "production mode requires encrypted account tokens: set "
                "AUTOCLAIMER_FERNET_KEY to a Fernet key (generate one in the "
                "demo CLI with `credential generate-key`), then store each "
                f"enabled account's token with `credential set <id>`. ({exc})"
            ) from exc
        credential_store = SQLiteCredentialStore(db_path, keys)
        await credential_store.open()
        provider: SQLiteCredentialStore = credential_store

        enabled = [profile for profile in accounts.accounts if profile.enabled]
        stored = set(await provider.account_ids())
        missing_tokens = [str(profile.id) for profile in enabled if profile.id not in stored]
        if missing_tokens:
            raise RuntimeStartupError(
                "no stored token for enabled account(s): "
                + ", ".join(missing_tokens)
                + " — store each with `credential set <account_id>` "
                "(or disable the account in accounts.json)"
            )

        registry = TaskRegistry()
        register_mudae_tasks(registry, store=store)
        unregistered = sorted(
            {task_id for profile in enabled for task_id in profile.tasks if task_id not in registry}
        )
        if unregistered:
            raise RuntimeStartupError(
                "enabled account(s) assign unregistered task id(s): "
                + ", ".join(unregistered)
                + " — remove them from accounts.json or register matching task types"
            )

        limiter = RateLimiter(default_policy=resolve_policy(app.ratelimit), store=store)
        health = AccountHealthMonitor()
        audit = AuditLog(store)
        send_jitter_range = app.settings.send_jitter_seconds

        def adapter_factory(account_id: int) -> PlatformAdapter:
            # One isolated adapter per account; the credential store is the
            # SHARED provider; construction never logs in (lazy start).
            return DiscordAdapter(
                account_id=account_id,
                credentials=provider,
                send_jitter_range=send_jitter_range,
                on_lifecycle=_make_lifecycle_hook(account_id, health, audit),
            )

        controller = AutoClaimerController(
            app_config=app,
            accounts=accounts,
            store=store,
            limiter=limiter,
            registry=registry,
            adapter_factory=adapter_factory,
            credentials=provider,
            credential_store=provider,
            config_path=config_path,
            accounts_path=accounts_path,
            health=health,
        )
        return Runtime(
            app=app,
            accounts=accounts,
            store=store,
            credential_store=credential_store,
            registry=registry,
            limiter=limiter,
            health=health,
            controller=controller,
            audit=audit,
            adapter_factory=adapter_factory,
            demo=False,
        )
    except BaseException:
        # Reverse ownership order; every close is idempotent and guarded —
        # cleanup must never fail, never mask the actionable startup error.
        if credential_store is not None:
            try:
                credential_store.close()
            except Exception:  # pragma: no cover - cleanup best effort
                logger.exception("credential store close during aborted startup failed")
        if store is not None:
            try:
                await store.close()
            except Exception:  # pragma: no cover - cleanup best effort
                logger.exception("state store close during aborted startup failed")
        raise
