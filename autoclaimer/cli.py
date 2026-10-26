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
    help                         usage
    quit | shutdown              graceful shutdown, then exit
"""

from __future__ import annotations

import asyncio
import shlex
from collections.abc import Awaitable, Callable
from typing import Any

from .adapters import PlatformAdapter
from .credentials import CredentialProvider
from .logging_setup import redact_text
from .models import AccountsConfig, AppConfig
from .persistence import TaskInstanceState, TaskStateStore
from .ratelimit import RateLimiter
from .registry import TaskRegistry
from .workers import WorkerManager

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
        idle_interval_seconds: float = 30.0,
    ) -> None:
        self._app_config = app_config
        self._accounts = accounts
        self._store = store
        self._limiter = limiter
        self._manager = WorkerManager(
            registry=registry,
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
        """Stop workers and close the store; idempotent."""
        if self._closed:
            return
        self._closed = True
        await self._manager.stop()
        await self._store.close()

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
    ) -> None:
        self._controller = controller
        self._read_line = read_line
        self._write = write

    def _emit(self, lines: list[str] | str) -> None:
        for line in lines if isinstance(lines, list) else [lines]:
            self._write(redact_text(line))  # never print credential-shaped values

    async def run(self) -> None:
        """Read commands until quit/EOF/Ctrl+C, then shut down gracefully."""
        self._emit("AutoClaimer CLI — type 'help' for commands")
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
        return self._controller.pause_account(self._account_arg(args[0]))

    async def _cmd_resume(self, args: list[str]) -> str:
        if not args:
            return self._controller.resume_all()
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
  help                            this help
  quit | shutdown                 graceful shutdown and exit"""


async def stdin_reader(prompt: str) -> str:
    """Default line reader: blocking ``input`` off the event loop."""
    return await asyncio.to_thread(input, prompt)
