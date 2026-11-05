"""Process entry point: ``python -m autoclaimer``.

PRODUCTION by default (ROADMAP item 26): the composition root in
``autoclaimer.runtime`` builds the real Discord/Mudae runtime — fail
closed when credentials, keys, or task registrations are missing.
``--demo`` selects the historical mock composition (ping task,
``MockAdapter``, credential management with a warn-and-continue key
posture). The entry point stays thin: argument parsing, composition,
session, and the signal path.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
import sys

from .cli import CliSession, install_termination_handlers, stdin_reader
from .models import ConfigValidationError
from .runtime import RuntimeStartupError, build_runtime

logger = logging.getLogger(__name__)


async def async_main(config_path: str, accounts_path: str, *, demo: bool = False) -> int:
    try:
        runtime = await build_runtime(config_path, accounts_path, demo=demo)
    except RuntimeStartupError as exc:
        # Everything opened has already been closed (reverse ownership
        # order); the message is the actionable operator fix.
        print(f"startup failed: {exc}", file=sys.stderr, flush=True)
        return 2
    except ConfigValidationError as exc:
        # Typed configuration errors carry file/line context — surface
        # them with the same clean exit instead of a traceback.
        print(f"configuration invalid: {exc}", file=sys.stderr, flush=True)
        return 2
    logger.info(
        "cli session starting (%s)",
        "demo" if demo else "production",
        extra={"operation": "cli.start", "outcome": "demo" if demo else "production"},
    )
    session = CliSession(
        runtime.controller, read_line=stdin_reader, write=lambda line: print(line, flush=True)
    )
    # Roadmap item 10 (graceful shutdown hardening): SIGINT *and* SIGTERM
    # trigger the SAME graceful path — cancelling the session task runs
    # run()'s finally-block (workers drained within the shutdown budget,
    # adapters and stores closed — which also drains every lifecycle
    # observer). A SECOND signal restores default handling so an operator
    # can always force-exit. Known limitation: a blocked interactive
    # stdin read may delay exit until the pending line arrives (a worker
    # thread cannot be cancelled).
    session_task = asyncio.create_task(session.run())

    def _request_termination() -> None:
        session_task.cancel()

    install_termination_handlers(_request_termination)
    try:
        await session_task
    except asyncio.CancelledError:
        pass  # signal-triggered: graceful shutdown already ran
    return 0


def main() -> None:
    parser = argparse.ArgumentParser(
        prog="autoclaimer",
        description=(
            "AutoClaimer: multi-account Mudae automation. Production runtime "
            "by default; --demo selects the mock composition (no Discord)."
        ),
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="run the mock-adapter demo composition instead of the production runtime",
    )
    parser.add_argument("config", nargs="?", default="config.json", help="path to config.json")
    parser.add_argument(
        "accounts", nargs="?", default="accounts.json", help="path to accounts.json"
    )
    args = parser.parse_args()
    try:
        raise SystemExit(asyncio.run(async_main(args.config, args.accounts, demo=args.demo)))
    except KeyboardInterrupt:
        raise SystemExit(0) from None


if __name__ == "__main__":
    main()
