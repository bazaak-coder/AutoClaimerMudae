"""Runnable demo entry: ``python -m autoclaimer``.

Wires the interactive CLI to the current configuration files with the
deterministic mock adapter (the real Discord adapter arrives in the next
phase). Logs go through the rotating, redacting setup.
"""

from __future__ import annotations

import asyncio
import sys
from pathlib import Path

from .cli import AutoClaimerController, CliSession, stdin_reader
from .config import load_config
from .logging_setup import setup_logging
from .mock_adapter import MockAdapter
from .persistence import TaskStateStore
from .ratelimit import RateLimiter, resolve_policy
from .registry import TaskContext, TaskOutcome, TaskRegistry, TaskStatus, TaskType


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


async def async_main(config_path: str, accounts_path: str) -> int:
    app, accounts = load_config(config_path, accounts_path)
    logger = setup_logging(app.logging)
    store = TaskStateStore(Path(app.logging.log_file).parent / "state.db")
    await store.open()
    limiter = RateLimiter(default_policy=resolve_policy(app.ratelimit), store=store)
    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store,
        limiter=limiter,
        registry=build_demo_registry(),
        adapter_factory=lambda account_id: MockAdapter(),
        idle_interval_seconds=5.0,
    )
    logger.info("cli session starting", extra={"operation": "cli.start"})
    session = CliSession(
        controller, read_line=stdin_reader, write=lambda line: print(line, flush=True)
    )
    await session.run()
    return 0


def main() -> None:
    config_path = sys.argv[1] if len(sys.argv) > 1 else "config.json"
    accounts_path = sys.argv[2] if len(sys.argv) > 2 else "accounts.json"
    try:
        raise SystemExit(asyncio.run(async_main(config_path, accounts_path)))
    except KeyboardInterrupt:
        raise SystemExit(0) from None


if __name__ == "__main__":
    main()
