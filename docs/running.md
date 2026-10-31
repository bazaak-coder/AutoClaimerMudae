# Running AutoClaimer

## Prerequisites

- Python ≥ 3.11 (CI matrix: 3.11–3.14)
- Dependencies install via `uv sync --all-extras` (or `pip install -e .[dev]`)
- A `config.json` + `accounts.json` pair ([configuration.md](configuration.md))
- For real Discord operation: `AUTOCLAIMER_FERNET_KEY` and stored tokens
  ([credentials.md](credentials.md))

## Booting

### Demo entry point (shipped)

```
python -m autoclaimer [config.json] [accounts.json]
```

This runs the **demo composition**: the deterministic mock adapter, a `ping`
task, the interactive CLI, and — when a valid `AUTOCLAIMER_FERNET_KEY` is
present — the encrypted credential store, so `credential …` commands work
out of the box. It sends no Discord traffic. Use it to validate configuration
and manage credentials.

### Real Discord composition (library)

Production wiring composes the same components with the real adapter. The
composition root is a tracked follow-up (see the
[roadmap](../ROADMAP.md)); until it ships, a deployment looks like:

```python
import asyncio
from pathlib import Path

from autoclaimer.cli import AutoClaimerController, CliSession, stdin_reader
from autoclaimer.config import load_config
from autoclaimer.credential_store import SQLiteCredentialStore, load_fernet_keys
from autoclaimer.discord_adapter import DiscordAdapter
from autoclaimer.logging_setup import setup_logging
from autoclaimer.mudae_tasks import register_mudae_tasks
from autoclaimer.persistence import TaskStateStore
from autoclaimer.ratelimit import RateLimiter, resolve_policy
from autoclaimer.registry import TaskRegistry


async def main() -> None:
    app, accounts = load_config("config.json", "accounts.json")
    setup_logging(app.logging)

    db = Path("state.db")
    store = TaskStateStore(db)
    await store.open()

    credentials = SQLiteCredentialStore(db, load_fernet_keys())
    await credentials.open()

    registry = TaskRegistry()
    register_mudae_tasks(registry, store=store)

    controller = AutoClaimerController(
        app_config=app,
        accounts=accounts,
        store=store,
        limiter=RateLimiter(default_policy=resolve_policy(app.ratelimit), store=store),
        registry=registry,
        adapter_factory=lambda account_id: DiscordAdapter(
            account_id=account_id, credentials=credentials
        ),
        credentials=credentials,
        credential_store=credentials,
    )
    session = CliSession(
        controller, read_line=stdin_reader, write=lambda line: print(line, flush=True)
    )
    await session.run()


asyncio.run(main())
```

## CLI reference

The interactive REPL (`help` inside the session shows the same):

| Command | Effect |
|---|---|
| `start` / `stop` | Start / gracefully stop the framework |
| `pause` / `resume` | Pause/resume all workers, or one account with an id |
| `enable <account>` / `disable <account>` | Account-level controls |
| `accounts` | Configured accounts and their task assignments |
| `tasks [state]` | Task instances by state (`pending`/`running`/`waiting`/`succeeded`/`failed`/`cancelled`) |
| `queue [account]` | Scheduling order with priorities |
| `cooldowns` | Active cooldowns and permits |
| `errors` | Recent failures and retry times |
| `status` | Worker status snapshot |
| `credential …` | Encrypted-token management ([credentials.md](credentials.md)) |
| `help` | Command help |
| `quit` / `exit` / `shutdown` | Graceful shutdown and exit |

Input rules: account ids must be known accounts for account-control commands
(credential commands accept any snowflake so tokens can be registered before
config); extra/unknown arguments are errors, never silently ignored; store,
limiter, adapter, and credential failures are reported without killing the
session.

## Logging

- Rotating file log (see `logging` in [configuration.md](configuration.md))
  plus an optional stderr mirror; `discord.*` library logs are routed through
  the same redaction.
- Every path is scrubbed: Discord-shaped tokens, webhook URLs, and
  authorization/token/password/key-value secrets — including tracebacks and
  rotation-failure dumps.

## Shutdown and recovery

| Scenario | Behavior |
|---|---|
| `quit` / `exit` / Ctrl-C / EOF | Graceful: workers drain within `shutdown_timeout_seconds`, stores and adapters close, credentials provider released. |
| Crash / kill mid-execution | Any `running` row is recovered to `pending` at next start (at-least-once semantics — a handler may re-run). |
| Crash mid-transaction | SQLite rollback-journal recovery; per-step migrations are transactional; startup shape-verification rejects tampered schemas. |
| Restart between rounds | Learned Mudae windows, cooldowns, and retry schedules persist in `state.db` and resume on schedule. |

### Crash recovery by interruption point (verified by SIGKILL tests)

AutoClaimer runs SQLite's **rollback journal** (not WAL — the shared-memory
mapping is unreliable in the target environment; recovery semantics below
are the hot-journal model, pinned by test). Abrupt termination
(`SIGKILL` — no cleanup of any kind runs) has exactly these outcomes,
each covered by `tests/test_crash_recovery.py`:

| Interruption point | On-disk state after kill | Behavior at next start |
|---|---|---|
| During an open write transaction | Hot journal present; uncommitted changes not in the database | Next `open()` rolls the journal back — the row is exactly as before; the store is fully usable. |
| After a committed state update | Change durable (journal checkpointed) | The acknowledged change survives; nothing is lost. |
| During task claiming (claim committed, no outcome yet) | Row stranded `running` with the attempt burned | `recover_interrupted()` flips it to `pending` (attempt count preserved); it becomes due exactly once. |
| During completion (success write mid-transaction) | Claim durable, completion rolled back | Row returns to `pending` and re-executes — documented **at-least-once** semantics; a phantom success is impossible. |
| Any other persisted status | — | Only `running` is touched by recovery; `pending`/`waiting`/terminal rows are exactly as persisted. |

Duplicate-execution guarantees: a recovered row is claimable again, but
exactly one claimer wins at a time (claim is an atomic compare-and-set) and
terminal rows are never re-claimable. Full kill-signal coverage at the
process boundary (SIGTERM/SIGINT handling) is roadmap item 10.

### Database diagnostics and recovery

The interactive CLI command **`integrity`** runs a strictly read-only
diagnostic over `state.db`:

- `PRAGMA integrity_check` (page-level corruption),
- every `task_instances` row decoded (corrupt outcome JSON, malformed
  timestamps, unsupported statuses),
- `scope_cooldowns` and `mudae_state` row validation,
- orphaned account references (rows for accounts absent from
  `accounts.json` — reported as warnings; leftovers are harmless).

Findings are printed with actionable hints. **Nothing is ever modified,
repaired, or deleted automatically** — potentially recoverable data is only
touched by an explicit operator action.

**Recovery procedure:**

1. Stop the application (so nothing races your repair).
2. Run `integrity` and read each finding's hint.
3. Page-level corruption (`database` findings): restore `state.db` from a
   backup. There is no in-place repair for a damaged disk image; the
   rollback journal already recovered everything it could at open.
4. Malformed rows (per-row findings): remove or fix the specific row with
   the `sqlite3` CLI — e.g.
   `DELETE FROM scope_cooldowns WHERE scope = 'account:9';`. Deleted
   cooldowns/state windows are safely re-learned from Mudae on the next
   round; deleted task rows are re-created from configuration at startup.
5. Orphaned account rows (warnings): harmless; remove explicitly only if
   the account is gone for good (`credential delete <id>` for tokens,
   `sqlite3` for task rows).
6. Restart and re-run `integrity` — expect `OK`.

Verified by test: startup recovery, retry-schedule persistence, restart
during active work, transaction cancellation at every boundary. Full
kill-signal coverage (SIGTERM/SIGINT/forced) is a tracked roadmap item
(graceful shutdown hardening, item 10).
