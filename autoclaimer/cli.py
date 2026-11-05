"""Interactive CLI: a thin, validated front end to the framework.

The CLI contains **no orchestration logic**. :class:`AutoClaimerController`
delegates to the worker manager, SQLite store, and rate limiter;
:class:`CliSession` parses and validates input, renders listings, and
handles EOF, Ctrl+C, invalid commands, and shutdown-while-active. All I/O
is injectable so tests drive the session programmatically.

Commands (REPL)::

    start | stop                 start/stop the framework (workers)
    pause | resume               pause/resume ALL workers
    pause <id> | resume <id>     pause/resume one account
    enable <id> | disable <id>   enable/disable an account
    accounts                     configured accounts and assignments
    tasks [state]                instances by state (default: all)
    queue [id]                   scheduling order with priorities
    cooldowns                    active cooldowns and permits
    errors                       recent failures and retry times
    status                       worker status snapshot
    credential ...               encrypted-token administration (set/has/delete/
                                  accounts/rotate/export/import/generate-key)
    integrity                    read-only database diagnostic
    help                         usage
    quit | shutdown              graceful shutdown, then exit
"""

from __future__ import annotations

import asyncio
import getpass
import json
import logging
import os
import shlex
import signal
from collections.abc import Awaitable, Callable
from contextlib import suppress
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from .adapters import AdapterError, PlatformAdapter
from .audit import AuditLog
from .credential_store import SQLiteCredentialStore
from .credentials import CredentialProvider, CredentialProviderError
from .health import AccountHealthMonitor
from .logging_setup import redact_text
from .models import AccountsConfig, AppConfig
from .persistence import PersistenceError, TaskInstanceState, TaskStateStore
from .ratelimit import RateLimiter, RateLimitError
from .registry import TaskRegistry
from .workers import WorkerManager

logger = logging.getLogger(__name__)

LineReader = Callable[[str], Awaitable[str]]
LineWriter = Callable[[str], None]


class AutoClaimerController:
    """Owns framework components; every action delegates to them."""

    def __init__(
        self,
        *,
        app_config: AppConfig,
        accounts: AccountsConfig,
        store: TaskStateStore,
        limiter: RateLimiter,
        registry: TaskRegistry,
        adapter_factory: Callable[[int], PlatformAdapter],
        credentials: CredentialProvider | None = None,
        credential_store: SQLiteCredentialStore | None = None,
        idle_interval_seconds: float = 30.0,
        config_path: str = "config.json",
        accounts_path: str = "accounts.json",
        health: AccountHealthMonitor | None = None,
    ) -> None:
        self._app_config = app_config
        self._accounts = accounts
        # Review finding (startup-path defaults): bare `reload`/`doctor`
        # used to fall back to CWD-relative config.json/accounts.json —
        # an operator who started with explicit paths could silently
        # adopt an UNRELATED config sitting in the working directory.
        # The STARTUP paths are the defaults for both commands.
        self._startup_config_path = config_path
        self._startup_accounts_path = accounts_path
        # Follow-up review finding: the last SUCCESSFULLY APPLIED logging
        # settings, tracked separately from ``_app_config.logging`` — a
        # reload whose logging reconfiguration failed must retry those
        # settings on the next reload (they were never applied), even
        # though the config itself was adopted.
        self._last_applied_logging = app_config.logging
        self._store = store
        self._limiter = limiter
        self._credentials = credentials
        # Encrypted credential administration (optional: absent when no
        # valid Fernet key is configured — credential commands then
        # explain instead of failing opaquely).
        self._credential_store = credential_store
        self._audit = AuditLog(store)
        self._manager = WorkerManager(
            registry=registry,
            health=health,
            store=store,
            limiter=limiter,
            app_config=app_config,
            accounts=accounts,
            adapter_factory=adapter_factory,
            credentials=credentials,
            idle_interval_seconds=idle_interval_seconds,
        )
        self._closed = False

    @property
    def closed(self) -> bool:
        return self._closed

    @property
    def manager(self) -> WorkerManager:
        return self._manager

    # ----- framework lifecycle -----

    async def start(self) -> str:
        await self._manager.start()
        return "framework started"

    async def stop(self) -> str:
        await self._manager.stop()
        return "framework stopped (graceful)"

    async def shutdown(self) -> None:
        """Stop workers and close the store; idempotent.

        Repo-sweep fixes: the credential provider's ``close()`` hook is
        finally honored (providers holding keyring/session handles leak
        otherwise), and a persistence failure during store close is
        logged instead of escaping the REPL's finally block."""
        if self._closed:
            return
        self._closed = True
        try:
            await self._manager.stop()
        finally:
            if self._credentials is not None:
                try:
                    self._credentials.close()
                except Exception:
                    logger.exception("credential provider close failed")
            if self._credential_store is not None:
                # close() is idempotent — double close via the provider
                # path (when the store IS the provider) is harmless.
                try:
                    self._credential_store.close()
                except Exception:
                    logger.exception("credential store close failed")
            try:
                await self._store.close()  # always released (N7)
            except Exception:
                logger.exception("store close failed during shutdown")

    # ----- credential administration (encrypted store) -----

    def credential_store_ready(self) -> bool:
        return self._credential_store is not None

    async def credential_set(self, account_id: int, token: str) -> str:
        assert self._credential_store is not None
        await self._credential_store.set_token(account_id, token)
        await self._audit.emit(category="credential", action="set", account_id=account_id)
        return f"credential stored (encrypted) for account {account_id}"

    async def credential_delete(self, account_id: int) -> str:
        assert self._credential_store is not None
        removed = await self._credential_store.delete_token(account_id)
        if removed:
            await self._audit.emit(category="credential", action="deleted", account_id=account_id)
        return (
            f"credential deleted for account {account_id}"
            if removed
            else f"no stored credential for account {account_id}"
        )

    async def credential_has(self, account_id: int) -> str:
        assert self._credential_store is not None
        present = await self._credential_store.has_token(account_id)
        return (
            f"account {account_id}: credential stored (encrypted)"
            if present
            else f"account {account_id}: no stored credential"
        )

    async def credential_accounts(self) -> list[str]:
        assert self._credential_store is not None
        ids = await self._credential_store.account_ids()
        return [
            f"{len(ids)} account(s) with stored (encrypted) credentials",
            *(f"  {account_id}" for account_id in sorted(ids)),
        ]

    async def credential_rotate(self) -> str:
        assert self._credential_store is not None
        count = await self._credential_store.rotate()
        await self._audit.emit(
            category="credential", action="rotated", detail=f"{count} re-encrypted"
        )
        return f"rotation complete: {count} credential(s) re-encrypted under the primary key"

    async def credential_export(self, path: str) -> str:
        """Ciphertext-only export (roadmap item 4). Confirmation is
        enforced by the CLI layer; this stays a single loud operation."""
        assert self._credential_store is not None
        count = await self._credential_store.export_ciphertext(path)
        await self._audit.emit(
            category="credential", action="exported", detail=f"{count} records (ciphertext)"
        )
        return (
            f"exported {count} credential(s) as CIPHERTEXT to {path} — the file "
            "contains no plaintext tokens; protect it exactly like the "
            "database (it is only usable together with the key)"
        )

    async def credential_import(self, path: str) -> str:
        assert self._credential_store is not None
        imported, skipped = await self._credential_store.import_ciphertext(path)
        await self._audit.emit(
            category="credential",
            action="imported",
            detail=f"{imported} imported, {skipped} current",
        )
        return (
            f"imported {imported} credential(s) from {path} "
            f"({skipped} already current); every record was validated and "
            "re-encrypted under the current primary key"
        )

    async def integrity_report(self) -> list[str]:
        """Read-only database diagnostic (roadmap item 2)."""
        known = {account.id for account in self._accounts.accounts}
        findings = await self._store.integrity_report(known_account_ids=known)
        if not findings:
            return ["integrity: OK — database, task rows, cooldowns, and state rows are consistent"]
        lines = [f"integrity: {len(findings)} finding(s) (nothing was modified)"]
        for finding in findings:
            lines.append(
                f"  [{finding.severity}] {finding.table} {finding.key}: "
                f"{finding.problem}\n    → {finding.hint}"
            )
        return lines

    async def retry_task(self, account_id: int, task_id: str) -> str:
        """Dead-letter manual retry (roadmap item 8)."""
        record = await self._store.get_instance_by_ids(account_id, task_id)
        await self._store.requeue_instance(record.id)
        await self._audit.emit(
            category="task", action="requeued", account_id=account_id, task_id=task_id
        )
        self._manager.wake(account_id)  # pick the requeued row up promptly
        return f"requeued {account_id}/{task_id} (attempt budget reset)"

    async def history_report(self, account_id: int, task_id: str) -> list[str]:
        """Execution history for one task instance (newest first)."""
        record = await self._store.get_instance_by_ids(account_id, task_id)
        rows = await self._store.task_history(record.id)
        lines = [f"history for {account_id}/{task_id}:"]
        if not rows:
            lines.append("  (no recorded attempts)")
        for attempt, status, message, recorded_at in rows:
            suffix = f" — {message}" if message else ""
            lines.append(f"  #{attempt} {status} at {recorded_at}{suffix}")
        return lines

    async def export_history(
        self,
        path: str,
        *,
        account_id: int | None,
        since: datetime | None,
        until: datetime | None,
    ) -> str:
        """Write the execution-history export (ROADMAP item 22).

        JSON Lines (one object per line — streamable, append-friendly),
        written ATOMICALLY (temp file + os.replace, off-loop) and never
        over an existing target. Rows were redacted at write; the export
        is a read-only snapshot ride on the store's serialized
        connection — active workers queue briefly, never interrupt.
        """
        target = Path(path)
        if target.exists():
            raise ValueError(f"refusing to overwrite existing file: {target}")
        rows = await self._store.export_history(account_id=account_id, since=since, until=until)
        payload = "".join(json.dumps(row, ensure_ascii=True) + "\n" for row in rows)
        try:
            await asyncio.to_thread(self._atomic_write, target, payload)
        except OSError as exc:
            # Containment (review finding, Major): a filesystem failure —
            # unwritable directory, full disk — is an operator fix, not a
            # reason to kill the REPL and every worker with it.
            raise ValueError(f"export write failed: {exc}") from exc
        await self._audit.emit(
            category="runtime",
            action="history_exported",
            detail=f"{len(rows)} rows to {target}",
        )
        return f"exported {len(rows)} history row(s) to {target}"

    @staticmethod
    def _atomic_write(target: Path, payload: str) -> None:
        """Temp-file + CONDITIONAL publish (review finding: os.replace
        overwrites unconditionally — a file appearing between the
        exists() check and the publish would be silently lost).
        ``os.link(tmp, target)`` fails with FileExistsError when the
        target exists, making the no-overwrite guarantee atomic with the
        publish. Hard links are unsupported on some filesystems
        (PRoot/Android mounts) — those OSErrors fail closed with a clear
        message rather than risking an overwrite."""
        import tempfile

        target.parent.mkdir(parents=True, exist_ok=True)
        fd, tmp = tempfile.mkstemp(dir=target.parent, prefix=f".{target.name}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as handle:
                handle.write(payload)
            try:
                os.link(tmp, target)  # conditional publish: FileExistsError wins
            except FileExistsError:
                raise ValueError(f"refusing to overwrite existing file: {target}") from None
            except OSError as exc:
                raise ValueError(
                    f"cannot safely publish {target} on this filesystem "
                    f"(hard links unsupported: {exc})"
                ) from exc
            os.unlink(tmp)
        except BaseException:
            with suppress(OSError):
                os.unlink(tmp)
            raise

    @property
    def default_config_path(self) -> str:
        """Startup config path — the default for bare `reload`/`doctor`."""
        return self._startup_config_path

    @property
    def default_accounts_path(self) -> str:
        """Startup accounts path — the default for bare `reload`/`doctor`."""
        return self._startup_accounts_path

    async def reload_config(self, config_path: str, accounts_path: str) -> list[str]:
        """Safe runtime configuration reload (roadmap item 13).

        Validate the new files FULLY first — an invalid configuration is
        rejected and the running one is untouched. Then apply the
        reloadable surface atomically and report restart-required
        settings (never partially applied). In-flight executions keep
        the snapshot they were claimed with.
        """
        from .config import load_config
        from .logging_setup import setup_logging

        new_app, new_accounts = load_config(config_path, accounts_path)  # full validation

        old = self._app_config
        lines: list[str] = []
        restart_required: list[str] = []
        if old.settings.max_concurrent_accounts != new_app.settings.max_concurrent_accounts:
            restart_required.append(
                f"max_concurrent_accounts {old.settings.max_concurrent_accounts} -> "
                f"{new_app.settings.max_concurrent_accounts}"
            )
        if old.settings.send_jitter_seconds != new_app.settings.send_jitter_seconds:
            restart_required.append("settings.send_jitter_seconds")
        # Compare against the last APPLIED logging, NOT _app_config.logging:
        # the failed reload adopted the config, so identical-looking
        # settings must still retry (they never landed).
        logging_changed = self._last_applied_logging != new_app.logging

        # Atomic adoption (single synchronous swap — no partial states).
        # Review finding: this runs BEFORE the logging reconfiguration —
        # an unwritable log path used to abort the whole reload before
        # the VALID configuration was applied, and raised out of the
        # command. Adoption is not conditional on logging.
        self._manager.update_configuration(new_app, new_accounts)
        self._app_config = new_app
        self._accounts = new_accounts
        # BUG-030: reload_materialize applies account enabled changes and
        # returns notes for any runtime override that shadowed the config.
        lines.extend(await self._manager.reload_materialize())
        if logging_changed:
            try:
                setup_logging(new_app.logging)  # idempotent: replaces handlers
                self._last_applied_logging = new_app.logging  # only on success
                lines.append(f"logging reconfigured (level {new_app.logging.level})")
            except OSError as exc:
                # setup_logging is atomic (old handlers survive a failed
                # reconfiguration): the session keeps usable logging and
                # the operator learns the new settings are broken.
                lines.append(
                    f"logging reconfiguration FAILED ({exc}); previous logging kept — "
                    "fix settings.logging and reload again"
                )
        await self._audit.emit(
            category="configuration",
            action="changed",
            detail=f"reloaded from {config_path}",
        )
        lines.insert(0, "configuration reloaded (validated; applied atomically)")
        if restart_required:
            lines.append(
                "restart required for: "
                + "; ".join(restart_required)
                + " (running values kept until restart)"
            )
        return lines

    async def backup_database(self, path: str) -> str:
        """Consistent backup via VACUUM INTO (roadmap item 14)."""
        size = await self._store.backup_to(path)
        return f"backup written: {path} ({size} bytes, consistent snapshot)"

    async def database_stats_report(self) -> list[str]:
        """Growth-monitoring snapshot (roadmap item 14)."""
        stats = await self._store.database_stats()
        rows = ", ".join(f"{table}={count}" for table, count in stats["rows"].items())
        return [
            f"database: {stats['path']}",
            f"  size {stats['size_bytes']} bytes | pages {stats['page_count']} "
            f"({stats['page_size']} B each, {stats['freelist_pages']} free)",
            f"  journal {stats['journal_mode']} (rollback journal; no WAL checkpoints)",
            f"  rows: {rows}",
        ]

    async def doctor_report(self, config_path: str, accounts_path: str) -> list[str]:
        """Operational diagnostics (roadmap item 15) — strictly READ-ONLY.

        Checks configuration (parsed WITHOUT migrating — doctor never
        rewrites files), database integrity, credential availability
        (count only, never secrets), task registration wiring, account
        health, and worker/scheduler status. Every WARN/FAIL carries an
        actionable hint.
        """
        from .config import _load_json, parse_accounts_config, parse_app_config
        from .models import ConfigValidationError

        lines: list[str] = ["doctor:"]

        # 1. Configuration validity (read-only parse — no migration).
        try:
            app = parse_app_config(_load_json(config_path))
            accounts_cfg = parse_accounts_config(_load_json(accounts_path), app)
            lines.append(
                f"  [OK] configuration — {len(app.tasks)} task(s), "
                f"{len(accounts_cfg.accounts)} account(s), channels resolved"
            )
        except (ConfigValidationError, OSError) as exc:
            lines.append(f"  [FAIL] configuration — {exc}")
            lines.append("      → fix the reported field and re-run doctor")
            return lines  # nothing else can be checked meaningfully

        # 2. Database accessibility + integrity (read-only diagnostics).
        try:
            known = {account.id for account in accounts_cfg.accounts}
            findings = await self._store.integrity_report(known_account_ids=known)
            if findings:
                for finding in findings:
                    lines.append(
                        f"  [{'WARN' if finding.severity == 'warning' else 'FAIL'}] "
                        f"database — {finding.table} {finding.key}: {finding.problem}"
                    )
                    lines.append(f"      → {finding.hint}")
            else:
                lines.append("  [OK] database — accessible, integrity clean")
        except Exception as exc:  # typed store failures
            lines.append(f"  [FAIL] database — {type(exc).__name__}: {exc}")
            lines.append("      → check the file permissions/path of state.db")

        # 3. Credential store availability (no secrets displayed).
        if self._credential_store is not None:
            try:
                ids = await self._credential_store.account_ids()
                lines.append(f"  [OK] credentials — encrypted store ready, {len(ids)} token(s)")
            except Exception as exc:
                lines.append(f"  [FAIL] credentials — {type(exc).__name__}: {exc}")
        else:
            lines.append(
                "  [WARN] credentials — no valid FERNET key configured "
                "(credential commands disabled)"
            )
            lines.append("      → set AUTOCLAIMER_FERNET_KEY (see 'credential generate-key')")

        # 4. Registered task definitions vs configuration wiring.
        registered = set(self._manager._registry.type_ids)  # noqa: SLF001
        configured = set(app.tasks)
        unregistered = sorted(configured - registered)
        if unregistered:
            lines.append(
                f"  [WARN] tasks — configured but not registered: {', '.join(unregistered)}"
            )
            lines.append("      → instances for these tasks are retired, not executed")
        else:
            lines.append(f"  [OK] tasks — {len(registered)} registered type(s)")

        # 5. Account health + 6. worker/scheduler status.
        try:
            health = self._manager.health()
            status = await self._manager.status()
            by_account = {info.account_id: info for info in status.workers}
            for profile in accounts_cfg.accounts:
                state = health[profile.id].state.value if profile.id in health else "unknown"
                worker = by_account.get(profile.id)
                queued = worker.queued if worker is not None else 0
                marker = "OK" if state in ("healthy", "connecting", "unknown") else "WARN"
                lines.append(
                    f"  [{marker}] account {profile.id} — health={state}, "
                    f"worker={worker.state.value if worker else 'not spawned'}, queued={queued}"
                )
            lines.append(f"  [OK] scheduler — {len(status.workers)} worker(s) tracked")
        except Exception as exc:  # pragma: no cover - defensive
            lines.append(f"  [WARN] runtime — status unavailable: {type(exc).__name__}: {exc}")
        return lines

    async def audit_report(self, limit: int) -> list[str]:
        """Recent audit events, newest first (roadmap item 11)."""
        events = await self._store.audit_events(limit=limit)
        if not events:
            return ["audit: no events recorded yet"]
        lines = [f"audit: {len(events)} most recent event(s)"]
        for timestamp, category, action, account_id, task_id, detail in events:
            who = f" account={account_id}" if account_id is not None else ""
            what = f" task={task_id}" if task_id else ""
            suffix = f" — {detail}" if detail else ""
            lines.append(f"  {timestamp} {category}.{action}{who}{what}{suffix}")
        return lines

    def health_report(self) -> list[str]:
        """Per-account health (roadmap item 6): state, last success,
        consecutive failures, and the last (redacted) error."""
        health = self._manager.health()
        if not health:
            return ["health: no accounts observed yet"]
        lines = ["health:"]
        for account in sorted(health):
            snapshot = health[account]
            lines.append(
                f"  account {account}: {snapshot.state.value}"
                f" | successes {snapshot.total_successes}"
                f" | failures {snapshot.total_failures}"
                f" (streak {snapshot.consecutive_failures})"
                f" | last success "
                f"{snapshot.last_success_at.isoformat() if snapshot.last_success_at else '—'}"
            )
            if snapshot.last_error:
                lines.append(f"    last error: {snapshot.last_error}")
        return lines

    # ----- account controls (valid before start) -----

    def known_account(self, account_id: int) -> bool:
        return any(a.id == account_id for a in self._accounts.accounts)

    def pause_all(self) -> str:
        self._manager.pause_all()
        return "all workers paused"

    def resume_all(self) -> str:
        self._manager.resume_all()
        return "all workers resumed"

    def pause_account(self, account_id: int) -> str:
        self._manager.pause_account(account_id)
        return f"account {account_id} paused"

    def resume_account(self, account_id: int) -> str:
        self._manager.resume_account(account_id)
        return f"account {account_id} resumed"

    def enable_account(self, account_id: int) -> str:
        self._manager.set_account_enabled(account_id, True)
        return f"account {account_id} enabled"

    def disable_account(self, account_id: int) -> str:
        self._manager.set_account_enabled(account_id, False)
        return f"account {account_id} disabled (in-flight work finishes)"

    # ----- listings -----

    def accounts_listing(self) -> list[str]:
        lines = ["accounts:"]
        for profile in self._accounts.accounts:
            status_flag = "enabled" if self._manager.account_enabled(profile.id) else "disabled"
            tasks = ", ".join(profile.tasks) if profile.tasks else "(no tasks)"
            lines.append(f"  {profile.id}  {status_flag}  tasks: {tasks}")
        return lines if len(lines) > 1 else ["accounts: (none configured)"]

    async def tasks_listing(self, state: TaskInstanceState | None = None) -> list[str]:
        records = await self._store.list_instances(state)
        lines = [f"tasks{' [' + state.value + ']' if state else ''}: {len(records)}"]
        for r in records:
            outcome = ""
            if r.last_outcome is not None:
                outcome = f"  last: {r.last_outcome.status.value} {r.last_outcome.message}".rstrip()
            lines.append(
                f"  #{r.id} acct={r.account_id} task={r.task_id} {r.status.value} "
                f"attempts={r.attempt_count}{outcome}"
            )
        return lines

    async def queue_listing(self, account_id: int | None) -> list[str]:
        if account_id is None:
            lines = ["queue (all accounts):"]
            for profile in self._accounts.accounts:
                lines.extend(await self._queue_for(profile.id, indent="  "))
            return lines
        return await self._queue_for(account_id, indent="")

    async def _queue_for(self, account_id: int, *, indent: str) -> list[str]:
        records = await self._manager.queue(account_id)
        if not records:
            return [f"{indent}account {account_id}: (empty)"]
        priority = self._app_config.tasks
        lines = [f"{indent}account {account_id}:"]
        for r in records:
            prio = priority[r.task_id].priority if r.task_id in priority else 0
            retry = f" retry_at={r.next_attempt_at.isoformat()}" if r.next_attempt_at else ""
            lines.append(
                f"{indent}  #{r.id} task={r.task_id} priority={prio} {r.status.value}{retry}"
            )
        return lines

    def cooldowns_listing(self) -> list[str]:
        status = self._limiter.status()
        lines = [f"cooldowns: {len(status.cooldowns)}"]
        for c in status.cooldowns:
            source = "server" if c.server_directed else "local"
            lines.append(f"  {c.scope}  remaining={c.remaining.total_seconds():.1f}s  ({source})")
        lines.append(f"permits: {len(status.permits)}")
        for p in status.permits:
            lines.append(f"  {p.scope}  in_use={p.in_use} waiting={p.waiting} limit={p.limit}")
        return lines

    async def errors_listing(self) -> list[str]:
        failed = await self._store.list_instances(TaskInstanceState.FAILED)
        waiting = await self._store.list_instances(TaskInstanceState.WAITING)
        lines = [f"recent errors: {len(failed)} failed, {len(waiting)} awaiting retry"]
        for r in failed:
            message = r.last_outcome.message if r.last_outcome else ""
            lines.append(f"  #{r.id} acct={r.account_id} task={r.task_id} failed: {message}")
        for r in waiting:
            retry = r.next_attempt_at.isoformat() if r.next_attempt_at else "?"
            lines.append(f"  #{r.id} acct={r.account_id} task={r.task_id} retry_at={retry}")
        return lines

    async def status_listing(self) -> list[str]:
        status = await self._manager.status()
        lines = [f"globally paused: {status.globally_paused}"]
        for w in status.workers:
            lines.append(
                f"  account {w.account_id}: {w.state.value} enabled={w.enabled} queued={w.queued}"
            )
        return lines


class CliSession:
    """Command loop: parsing, validation, EOF/Ctrl+C handling."""

    def __init__(
        self,
        controller: AutoClaimerController,
        *,
        read_line: LineReader,
        write: LineWriter,
        read_secret: LineReader | None = None,
    ) -> None:
        self._controller = controller
        self._read_line = read_line
        # Secret input has its OWN reader (BUG-008 fix): the plain line
        # reader echoes on a TTY; the default secret reader disables echo
        # via getpass. Injectable separately for deterministic tests.
        self._read_secret = read_secret if read_secret is not None else stdin_secret_reader
        self._write = write

    def _emit(self, lines: list[str] | str) -> None:
        for line in lines if isinstance(lines, list) else [lines]:
            self._write(redact_text(line))  # never print credential-shaped values

    async def run(self) -> None:
        """Read commands until quit/EOF/Ctrl+C, then shut down gracefully."""
        self._emit("AutoClaimer CLI — type 'help' for commands")
        try:
            while True:
                try:
                    line = await self._read_line("autoclaimer> ")
                except (EOFError, StopAsyncIteration):
                    self._emit("EOF — shutting down")
                    break
                except KeyboardInterrupt:
                    self._emit("interrupted — shutting down")
                    break
                if not await self.run_once(line):
                    break
        finally:
            # Shutdown always runs, even if a command escaped error handling.
            await self._controller.shutdown()
            self._emit("shutdown complete")

    async def run_once(self, line: str) -> bool:
        """Parse and execute one command; False means exit the loop."""
        try:
            parts = shlex.split(line.strip())
        except ValueError:
            self._emit("error: unbalanced quoting in command")
            return True
        if not parts:
            return True
        command, args = parts[0].lower(), parts[1:]
        if command in ("quit", "exit", "shutdown"):
            return False
        if command == "help":
            self._emit(USAGE)
            return True
        handler = self._HANDLERS.get(command)
        if handler is None:
            self._emit(f"error: unknown command {command!r} — type 'help'")
            return True
        try:
            result = await handler(self, args)
            if isinstance(result, str):
                self._emit(result)
            elif isinstance(result, list):
                self._emit(result)
        except ValueError as exc:
            self._emit(f"error: {exc}")
        except (PersistenceError, RateLimitError, AdapterError, CredentialProviderError) as exc:
            # Transient store/limiter/adapter/credential problems must not
            # kill the REPL or skip the graceful shutdown path.
            self._emit(f"error: {type(exc).__name__}: {exc}")
        return True

    # ----- command handlers (validated) -----

    async def _cmd_start(self, args: list[str]) -> str:
        self._no_args(args)
        return await self._controller.start()

    async def _cmd_stop(self, args: list[str]) -> str:
        self._no_args(args)
        return await self._controller.stop()

    async def _cmd_pause(self, args: list[str]) -> str:
        if not args:
            return self._controller.pause_all()
        self._exact_args(args, 1)  # audit BUG-003: no silently ignored ids
        return self._controller.pause_account(self._account_arg(args[0]))

    async def _cmd_resume(self, args: list[str]) -> str:
        if not args:
            return self._controller.resume_all()
        self._exact_args(args, 1)
        return self._controller.resume_account(self._account_arg(args[0]))

    async def _cmd_enable(self, args: list[str]) -> str:
        self._exact_args(args, 1)
        return self._controller.enable_account(self._account_arg(args[0]))

    async def _cmd_disable(self, args: list[str]) -> str:
        self._exact_args(args, 1)
        return self._controller.disable_account(self._account_arg(args[0]))

    async def _cmd_accounts(self, args: list[str]) -> list[str]:
        self._no_args(args)
        return self._controller.accounts_listing()

    async def _cmd_tasks(self, args: list[str]) -> list[str]:
        if not args:
            return await self._controller.tasks_listing(None)
        self._exact_args(args, 1)
        return await self._controller.tasks_listing(self._state_arg(args[0]))

    async def _cmd_queue(self, args: list[str]) -> list[str]:
        if not args:
            return await self._controller.queue_listing(None)
        self._exact_args(args, 1)
        return await self._controller.queue_listing(self._account_arg(args[0]))

    async def _cmd_cooldowns(self, args: list[str]) -> list[str]:
        self._no_args(args)
        return self._controller.cooldowns_listing()

    async def _cmd_errors(self, args: list[str]) -> list[str]:
        self._no_args(args)
        return await self._controller.errors_listing()

    async def _cmd_status(self, args: list[str]) -> list[str]:
        self._no_args(args)
        return await self._controller.status_listing()

    # ----- argument validation -----

    def _no_args(self, args: list[str]) -> None:
        if args:
            raise ValueError(f"command takes no arguments (got {len(args)})")

    def _exact_args(self, args: list[str], count: int) -> None:
        if len(args) != count:
            raise ValueError(f"command takes exactly {count} argument(s) (got {len(args)})")

    def _account_arg(self, raw: str) -> int:
        try:
            account_id = int(raw)
        except ValueError:
            raise ValueError(f"account id must be an integer, got {raw!r}") from None
        if not self._controller.known_account(account_id):
            raise ValueError(f"unknown account {account_id}")
        return account_id

    def _any_account_arg(self, raw: str) -> int:
        """Snowflake validation WITHOUT requiring the account in config:
        credentials may be registered before the account is added."""
        try:
            account_id = int(raw)
        except ValueError:
            raise ValueError(f"account id must be an integer, got {raw!r}") from None
        if account_id <= 0:
            raise ValueError("account id must be a positive integer")
        return account_id

    async def _cmd_credential(self, args: list[str]) -> str | list[str]:
        """Encrypted credential administration.

        Stored-credential actions require the Fernet key; ``generate-key``
        works keyless by design (first-time setup)."""
        from .credential_store import CredentialKeyError, generate_fernet_key

        if not args:
            return (
                "usage: credential set|has|delete <account> | accounts | rotate | "
                "generate-key | export|import <path> confirm"
            )
        action = args[0].lower()
        if action == "generate-key":
            self._exact_args(args, 1)
            return (
                "new Fernet key (store it OUTSIDE the database, e.g. a secret "
                "manager; NEVER overwrite an in-use key — existing credentials "
                "become unrecoverable):\n  "
                + generate_fernet_key()
                + ("\nset it as AUTOCLAIMER_FERNET_KEY and restart.")
            )
        if action == "set":
            self._exact_args(args, 2)
            account_id = self._any_account_arg(args[1])
            if not self._controller.credential_store_ready():
                raise CredentialKeyError(
                    "credential storage unavailable: set AUTOCLAIMER_FERNET_KEY "
                    "(see 'credential generate-key') and restart"
                )
            try:
                token = await self._read_secret(f"token for account {account_id} (input hidden): ")
            except (EOFError, KeyboardInterrupt):
                raise ValueError("no token entered — nothing stored") from None
            token = token.strip()
            if not token:
                raise ValueError("empty token — nothing stored")
            return await self._controller.credential_set(account_id, token)
        if action == "has":
            self._exact_args(args, 2)
            account_id = self._any_account_arg(args[1])
            if not self._controller.credential_store_ready():
                raise CredentialKeyError(
                    "credential storage unavailable: set AUTOCLAIMER_FERNET_KEY and restart"
                )
            return await self._controller.credential_has(account_id)
        if action == "delete":
            self._exact_args(args, 2)
            account_id = self._any_account_arg(args[1])
            if not self._controller.credential_store_ready():
                raise CredentialKeyError(
                    "credential storage unavailable: set AUTOCLAIMER_FERNET_KEY and restart"
                )
            return await self._controller.credential_delete(account_id)
        if action == "accounts":
            self._exact_args(args, 1)
            if not self._controller.credential_store_ready():
                raise CredentialKeyError(
                    "credential storage unavailable: set AUTOCLAIMER_FERNET_KEY and restart"
                )
            return await self._controller.credential_accounts()
        if action == "rotate":
            self._exact_args(args, 1)
            if not self._controller.credential_store_ready():
                raise CredentialKeyError(
                    "credential rotation unavailable: set AUTOCLAIMER_FERNET_KEY "
                    "(new primary) and AUTOCLAIMER_FERNET_RETIRED_KEYS (old keys)"
                )
            return await self._controller.credential_rotate()
        if action in ("export", "import"):
            # Destructive-adjacent operations: an explicit literal
            # ``confirm`` is required so they can never run by accident.
            if len(args) != 3 or args[2] != "confirm":
                raise ValueError(
                    f"credential {action} requires an explicit confirmation: "
                    f"'credential {action} <path> confirm'"
                )
            if not self._controller.credential_store_ready():
                raise CredentialKeyError(
                    f"credential {action} unavailable: set AUTOCLAIMER_FERNET_KEY and restart"
                )
            if action == "export":
                return await self._controller.credential_export(args[1])
            return await self._controller.credential_import(args[1])
        raise ValueError(
            f"unknown credential action {action!r}; "
            "expected set, has, delete, accounts, rotate, generate-key, "
            "export, or import"
        )

    async def _cmd_integrity(self, args: list[str]) -> list[str]:
        """Read-only database diagnostic: reports findings, modifies nothing."""
        self._exact_args(args, 0)
        return await self._controller.integrity_report()

    async def _cmd_health(self, args: list[str]) -> list[str]:
        """Per-account health: state, streaks, last success, last error."""
        self._exact_args(args, 0)
        return self._controller.health_report()

    async def _cmd_retry(self, args: list[str]) -> str:
        """Dead-letter manual retry: failed -> pending, fresh attempts."""
        self._exact_args(args, 2)
        account_id = self._account_arg(args[0])
        return await self._controller.retry_task(account_id, args[1])

    async def _cmd_doctor(self, args: list[str]) -> list[str]:
        """Operational diagnostics: config, database, credentials, tasks,
        workers — read-only, with actionable hints."""
        if len(args) not in (0, 2):
            raise ValueError("usage: doctor [config.json accounts.json]")
        config_path = args[0] if args else self._controller.default_config_path
        accounts_path = args[1] if args else self._controller.default_accounts_path
        return await self._controller.doctor_report(config_path, accounts_path)

    async def _cmd_backup(self, args: list[str]) -> str:
        """Consistent database backup (VACUUM INTO; refuses to overwrite)."""
        self._exact_args(args, 1)
        return await self._controller.backup_database(args[0])

    async def _cmd_export(self, args: list[str]) -> str:
        """Execution-history export (JSON Lines; refuses to overwrite).

        usage: export <path> [account <id>] [since <date>] [until <date>]
        Dates are UTC (YYYY-MM-DD or full ISO); until is exclusive.
        """
        if not args:
            raise ValueError("usage: export <path> [account <id>] [since <date>] [until <date>]")
        path = args[0]
        rest = args[1:]
        if len(rest) % 2 != 0:
            raise ValueError("filters come in key/value pairs: account|since|until <value>")
        account_id: int | None = None
        since: datetime | None = None
        until: datetime | None = None
        for key, value in zip(rest[::2], rest[1::2], strict=True):
            if key == "account":
                # Any snowflake: history may outlive a deconfigured
                # account, so the filter must not require known accounts.
                try:
                    account_id = int(value)
                except ValueError:
                    raise ValueError(f"account id must be an integer, got {value!r}") from None
                if account_id <= 0:
                    raise ValueError(f"account id must be positive, got {value!r}")
            elif key == "since":
                since = self._parse_export_date(value, inclusive_start=True)
            elif key == "until":
                until = self._parse_export_date(value, inclusive_start=False)
            else:
                raise ValueError(f"unknown export filter {key!r} (account|since|until)")
        return await self._controller.export_history(
            path, account_id=account_id, since=since, until=until
        )

    @staticmethod
    def _parse_export_date(value: str, *, inclusive_start: bool) -> datetime:
        """UTC date or full ISO timestamp; bare dates get the day's
        inclusive start (since) or exclusive end (until)."""
        for fmt in ("%Y-%m-%d",):
            try:
                day = datetime.strptime(value, fmt).replace(tzinfo=UTC)
                return day if inclusive_start else day + timedelta(days=1)
            except ValueError:
                continue
        try:
            parsed = datetime.fromisoformat(value)
        except ValueError as exc:
            raise ValueError(
                f"invalid date {value!r}: use YYYY-MM-DD or a full ISO timestamp"
            ) from exc
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=UTC)
        return parsed.astimezone(UTC)

    async def _cmd_dbstats(self, args: list[str]) -> list[str]:
        """Database growth snapshot (sizes, pages, row counts)."""
        self._exact_args(args, 0)
        return await self._controller.database_stats_report()

    async def _cmd_reload(self, args: list[str]) -> list[str]:
        """Runtime configuration reload (validated, atomic, audited)."""
        if len(args) not in (0, 2):
            raise ValueError("usage: reload [config.json accounts.json]")
        config_path = args[0] if args else self._controller.default_config_path
        accounts_path = args[1] if args else self._controller.default_accounts_path
        return await self._controller.reload_config(config_path, accounts_path)

    async def _cmd_audit(self, args: list[str]) -> list[str]:
        """Recent audit events (newest first)."""
        limit = 20
        if args:
            self._exact_args(args, 1)
            try:
                limit = int(args[0])
            except ValueError:
                raise ValueError(f"limit must be an integer, got {args[0]!r}") from None
            if not 0 < limit <= 1000:
                raise ValueError("limit must be in (0, 1000]")
        else:
            self._exact_args(args, 0)
        return await self._controller.audit_report(limit)

    async def _cmd_history(self, args: list[str]) -> list[str]:
        """Execution history for one task (newest first)."""
        self._exact_args(args, 2)
        account_id = self._account_arg(args[0])
        return await self._controller.history_report(account_id, args[1])

    def _state_arg(self, raw: str) -> TaskInstanceState:
        try:
            return TaskInstanceState(raw)
        except ValueError:
            valid = ", ".join(s.value for s in TaskInstanceState)
            raise ValueError(f"unknown task state {raw!r}; valid: {valid}") from None

    _HANDLERS: dict[str, Callable[[CliSession, list[str]], Awaitable[Any]]] = {
        "start": _cmd_start,
        "stop": _cmd_stop,
        "pause": _cmd_pause,
        "resume": _cmd_resume,
        "enable": _cmd_enable,
        "disable": _cmd_disable,
        "accounts": _cmd_accounts,
        "tasks": _cmd_tasks,
        "queue": _cmd_queue,
        "cooldowns": _cmd_cooldowns,
        "errors": _cmd_errors,
        "status": _cmd_status,
        "credential": _cmd_credential,
        "integrity": _cmd_integrity,
        "health": _cmd_health,
        "retry": _cmd_retry,
        "history": _cmd_history,
        "audit": _cmd_audit,
        "reload": _cmd_reload,
        "backup": _cmd_backup,
        "export": _cmd_export,
        "dbstats": _cmd_dbstats,
        "doctor": _cmd_doctor,
    }


USAGE = """commands:
  start | stop                    start/stop the framework
  pause | resume                  pause/resume all workers
  pause <account> | resume <account>
  enable <account> | disable <account>
  accounts                        list configured accounts and tasks
  tasks [state]                   pending|running|waiting|succeeded|failed|cancelled
  queue [account]                 scheduling order with priorities
  cooldowns                       active cooldowns and permits
  errors                          recent failures and retry times
  status                          worker status snapshot
  credential set <account>        store a token (Fernet-encrypted; prompts for the token)
  credential has <account>        whether a credential is stored
  credential delete <account>     delete the stored credential
  credential accounts             accounts with stored credentials
  credential rotate               re-encrypt all credentials under the primary key
  credential export <path> confirm
                                  write all credentials as CIPHERTEXT (no plaintext)
  credential import <path> confirm
                                  restore credentials (validated, re-encrypted under primary)
  credential generate-key         print a fresh Fernet key (first-time setup only)
  integrity                       read-only database diagnostic (never modifies data)
  health                          account health: state, streaks, last success/error
  retry <account> <task>          requeue a dead-lettered (failed) task with fresh attempts
  history <account> <task>        execution history (attempts, outcomes, timestamps)
  audit [limit]                  recent audit events (lifecycle, controls, credentials)
  reload [config accounts]       validate + atomically apply new configuration at runtime
  backup <path>                  consistent database backup (VACUUM INTO; refuses to overwrite)
  dbstats                        database growth snapshot (size, pages, rows)
  doctor [config accounts]       read-only diagnostics: config, DB, credentials, tasks, workers
  help                            this help
  quit | shutdown                 graceful shutdown and exit"""


async def stdin_reader(prompt: str) -> str:
    """Default line reader: blocking ``input`` off the event loop."""
    return await asyncio.to_thread(input, prompt)


async def stdin_secret_reader(prompt: str) -> str:
    """Default SECRET reader (BUG-008 fix): ``getpass`` disables terminal
    echo, so an entered token is not displayed or captured in terminal
    recordings. Without a controlling TTY, getpass prints an explicit
    warning and falls back to echoed stdin — piped/automated use stays
    possible, visibly."""
    return await asyncio.to_thread(getpass.getpass, prompt)


def install_termination_handlers(
    trigger: Callable[[], None],
    *,
    signals: tuple[signal.Signals, ...] = (signal.SIGINT, signal.SIGTERM),
) -> Callable[[], None]:
    """Install loop-level graceful-termination handlers (roadmap item 10).

    Semantics (failure behavior defined before implementation):

    - The FIRST signal invokes *trigger* once — the composition's
      graceful path (drain workers, close adapters and stores).
    - A SECOND signal restores DEFAULT handling: the operator can always
      force-exit a stuck graceful shutdown. Guaranteed escape hatch.
    - The handler never raises into the event loop; *trigger* runs as a
      plain callback (schedule async work yourself, e.g. task.cancel()).
    - Returns an uninstall callable (tests / embedding).

    Known limitation (documented in docs/running.md): a blocked
    interactive stdin read lives on a worker thread and cannot be
    cancelled — exit may wait for the pending line.
    """
    loop = asyncio.get_running_loop()
    fired: list[bool] = []

    def _handle(sig: signal.Signals) -> None:
        if fired:
            # Second signal: restore default (force) handling for ALL
            # installed signals and re-raise so the process dies now.
            uninstall()
            os.kill(os.getpid(), sig)
            return
        fired.append(True)
        logger.info(
            "termination signal received; starting graceful shutdown",
            extra={"operation": "signal", "outcome": sig.name},
        )
        try:
            trigger()
        except Exception:  # pragma: no cover - trigger is task.cancel()
            logger.exception("termination trigger failed")

    def uninstall() -> None:
        for sig in signals:
            try:
                loop.remove_signal_handler(sig)
            except (NotImplementedError, RuntimeError):  # pragma: no cover - win
                pass

    for sig in signals:
        try:
            loop.add_signal_handler(sig, _handle, sig)
        except (NotImplementedError, RuntimeError):
            # Review finding (Major): Windows event loops do not support
            # loop-level signal handlers — the unguarded install aborted
            # CLI STARTUP before the session task even ran. Degrade to
            # the platform default (Ctrl+C still interrupts the runner).
            logger.warning(
                "event loop does not support signal handlers (%s); using platform default handling",
                sig.name,
                extra={"operation": "signal", "outcome": "handler_unsupported"},
            )
    return uninstall
