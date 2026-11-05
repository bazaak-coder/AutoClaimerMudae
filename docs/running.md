# Running AutoClaimer

## Prerequisites

- Python ≥ 3.11 (CI matrix: 3.11–3.14)
- Dependencies install via `uv sync --all-extras` (or `pip install -e .[dev]`)
- A `config.json` + `accounts.json` pair ([configuration.md](configuration.md))
- For real Discord operation: `AUTOCLAIMER_FERNET_KEY` and stored tokens
  ([credentials.md](credentials.md))

## Booting

### Production entry point (default)

```
python -m autoclaimer [config.json] [accounts.json]
```

This runs the **production composition** (ROADMAP item 26): the real
Discord adapters (one isolated client per account, logged in lazily by
the worker on first execution), the Mudae task registry
(`roll`/`mu`/`rt`), the central rate limiter, durable SQLite state next
to the config file, and gateway-lifecycle wiring into account health and
the audit trail. The interactive CLI is the control surface; SIGINT/
SIGTERM run the established graceful path.

**Prerequisites** (startup FAILS CLOSED with the fix in the error
message when any is missing — nothing half-starts, nothing leaks):

1. A Fernet key in `AUTOCLAIMER_FERNET_KEY` (generate one with
   `credential generate-key` in `--demo` mode, or see
   [credentials.md](credentials.md)).
2. A stored token for every **enabled** account:
   `credential set <account_id>` (disabled accounts are exempt).
3. Every task assigned to an enabled account must have a registered
   task type — a task defined in `config.json` but not registered is
   reported and aborts startup.

Failure modes: an invalid key, a missing token, or an unregistered task
each print one actionable line and exit with code 2 after closing
everything already opened (in reverse ownership order — the design note
in [architecture.md](architecture.md#composition-root-roadmap-item-26--required-design-note)
has the full ownership graph and sequences).

### Demo entry point

```
python -m autoclaimer --demo [config.json] [accounts.json]
```

The historical mock composition: the deterministic mock adapter, a
`ping` task, no Discord traffic, and credential MANAGEMENT available
when a valid key is present (a warning — not an abort — when it is not).
Use it to validate configuration and manage credentials safely.

### Custom composition (library)

`autoclaimer.runtime.build_runtime(config, accounts, *, demo=False)` is
the composition root — call it directly to assemble and inspect the
runtime without running it (construction never logs into Discord). The
ownership graph, startup/shutdown sequences, and per-resource failure
behavior are documented in
[architecture.md](architecture.md#composition-root-roadmap-item-26--required-design-note).

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
| `audit [limit]` | Structured audit trail: task lifecycle, account/runtime controls, credential operations — persisted (survives restart), secret-free, newest first |
| `reload [config accounts]` | Runtime configuration reload: full validation first, atomic apply of the reloadable surface, restart-required settings reported (not applied), `configuration.changed` audited. In-flight executions keep the snapshot they were claimed with. |
| `retry <account> <task>` | Dead-letter manual retry: failed → pending with a fresh attempt budget (worker woken immediately) |
| `history <account> <task>` | Bounded execution history: attempt, outcome, message, timestamp per row (newest first) |
| `status` | Worker status snapshot |
| `health` | Per-account health: state (`healthy`/`connecting`/`disconnected`/`degraded`/`auth_failed`/`rate_limited`/`disabled`), success/failure totals, current streak, last success, last (redacted) error |
| `integrity` | Read-only database diagnostic: reports findings, never modifies data |
| `doctor [config accounts]` | Read-only diagnostics: config, database, credentials, tasks, workers — with actionable hints |
| `dbstats` | Database growth snapshot: size, pages, row counts |
| `backup <path>` | Consistent database backup (VACUUM INTO; refuses to overwrite) |
| `credential …` | Encrypted-token management ([credentials.md](credentials.md)) |
| `help` | Command help |
| `quit` / `exit` / `shutdown` | Graceful shutdown and exit |

Input rules: account ids must be known accounts for account-control commands
(credential commands accept any snowflake so tokens can be registered before
config); extra/unknown arguments are errors, never silently ignored; store,
limiter, adapter, and credential failures are reported without killing the
session.

### Database maintenance

AutoClaimer runs SQLite's **rollback journal** (not WAL — the shared-memory
mapping is unreliable in the target environment), so there are no WAL
checkpoints to manage: the journal is removed automatically after every
commit.

- **Growth monitoring:** `dbstats` reports file size, page/free-page
  counts, the live journal mode, and per-table row counts. Growth is
  structurally bounded: audit events (latest 1000) and per-task history
  (latest 50 attempts) are pruned on write.
- **Backup:** `backup <path>` writes a consistent, compacted snapshot
  (`VACUUM INTO`) through the store's serialized connection — safe while
  idle, and safe to run live (it waits for concurrent writes; it never
  produces a torn copy). Existing targets are never overwritten.
- **Restoration:** stop the application, replace `state.db` with the
  backup file, restart. Credentials in the backup decrypt only with the
  key that encrypted them — keep key and backup pairing in mind when
  restoring across environments.

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
| **SIGTERM** | Graceful (same path as Ctrl-C): first signal drains and exits 0; a blocked task is left honestly `running` and recovers on the next start. |
| **Second signal** (SIGINT/SIGTERM again) | Force-exit escape hatch: default handling is restored, so a stuck graceful shutdown can always be killed. |
| **SIGKILL** | No guarantees (no handler can run): tested separately — the database stays consistent (rollback journal) and restart recovers. |
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
terminal rows are never re-claimable. Kill-signal behavior at the process
boundary (SIGTERM/SIGINT, graceful-then-forced) is implemented and covered
by the signal-shutdown test suite.

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
during active work, transaction cancellation at every boundary, and
kill-signal coverage (SIGTERM/SIGINT/forced second-signal exit).
