# AutoClaimer — Phase 1: project foundation

AutoClaimer is a lightweight, standalone Python framework for configuring and
scheduling tasks across multiple Discord accounts (targeting the Mudae bot).
This phase implements the typed configuration layer only. It intentionally
does not connect to Discord or execute real tasks, and `discord.py-self` is
deliberately not a dependency yet.

## Requirements

- Python 3.11+

## Install development dependencies

The system Python is PEP 668 externally-managed (Alpine); use a virtual
environment. In this workspace a ready venv exists at
`/workspace/autoclaimer-venv` (the project lives on FUSE, which cannot hold
venv symlinks, so the venv deliberately lives outside the project tree):

```bash
python3 -m venv /workspace/autoclaimer-venv   # first time only
/workspace/autoclaimer-venv/bin/pip install -e ".[dev]"
```

## Run tests and quality gates

```bash
V=/workspace/autoclaimer-venv/bin
$V/ruff check .
$V/ruff format --check .
$V/mypy .
$V/pytest
```

A `uv.lock` is provided for reproducible resolution (`uv sync --locked` from
a uv-managed environment, or `uv pip sync`-style workflows).

## Validate the sample configuration

From the project root:

```bash
python -c "from autoclaimer.config import load_config; a, p = load_config('config.json', 'accounts.json'); print(f'Valid: {len(a.tasks)} tasks, {len(p.accounts)} accounts')"
```

## Package layout

| Module | Status | Purpose |
|---|---|---|
| `autoclaimer/models.py` | implemented | Typed, frozen dataclass models and `ConfigValidationError`. |
| `autoclaimer/config.py` | implemented | JSON loading, structural validation, cross-file checks, precedence resolution. |
| `autoclaimer/credentials.py` | implemented (interface only) | `CredentialProvider` protocol; secure backends provide implementations. |
| `autoclaimer/registry.py` | implemented | Task type registry: stable ids, descriptions, config schemas, async handlers. |
| `autoclaimer/adapters.py` | implemented | `PlatformAdapter` protocol, typed results, recoverable/permanent/cooldown errors. |
| `autoclaimer/discord_adapter.py` | implemented | Concrete discord.py-self adapter (one client per account; verified against 2.1.0). |
| `autoclaimer/mock_adapter.py` | implemented | Script-driven deterministic mock; no Discord connection, no credentials. |
| `autoclaimer/ratelimit.py` | implemented | Central async rate limiter: scopes, pacing, permits, cooldowns, retries. |
| `autoclaimer/workers.py` | implemented | Worker manager: concurrent account workers, ordered scheduling, controls. |
| `autoclaimer/persistence.py` | implemented | Async SQLite task-instance state store, migrations, recovery. |
| `autoclaimer/cli.py` | implemented | Interactive command session + controller (thin delegation, no orchestration). |
| `autoclaimer/logging_setup.py` | implemented | Rotating log files, structured context, credential redaction on every path. |

Planned boundaries are documented, not implemented; no stub modules exist to
avoid speculative code.

## Configuration model

Two files: `config.json` (global settings, task defaults, task definitions)
and `accounts.json` (per-account assignments and overrides).

- Account IDs are integers (Discord snowflakes).
- Task IDs are short identifier strings.

### Precedence (explicit)

```
global defaults (task_defaults)  <  task definition  <  account override
```

- A setting omitted at an outer layer falls back to the layer above it.
- Account overrides preserve explicit `false`; an omitted override key means
  "not overridden", not "false".

### Validation

- Unknown keys are rejected to catch typos early.
- Account IDs must be positive integer snowflakes; task IDs follow a strict
  identifier pattern.
- Duplicate account IDs and duplicate task assignments are rejected.
- References to undefined tasks (in `tasks` or `overrides`) are rejected.
- Malformed JSON errors include file, line, and column.
- Unsupported config versions are rejected (currently version 1 only).

## Credentials

Credentials must **never** be stored in plaintext JSON configuration. The
parsers reject credential-like keys (`token`, `password`, `secret`, `cookie`,
`authorization`, `api_key`, `credential`, ...) in any `parameters` block as a
safety net. Account tokens are supplied at runtime by an implementation of
the `CredentialProvider` protocol in `autoclaimer/credentials.py`; this
project ships no secret-management implementation by design.

## Discord notifications

Commit messages, PRs, reviews, and comments are announced on Discord via a
native repository webhook (Settings → Webhooks) that posts GitHub's event
payload to the Discord `/github` endpoint as `application/json`. It is
managed entirely in GitHub's server-side settings; no workflow files, local
hooks, or webhook URLs exist in this repository.

## Phase 2: task registry and adapter contracts

- **Task types** (`registry.TaskType`) combine a stable id, description,
  config validator, and async handler; they are registered in a
  `TaskRegistry` — behaviour is never hard-coded into central conditionals.
- **Task instances** are per-account: `registry.TaskContext` carries the
  account id, task id, validated config, adapter, and optional credential
  provider. `TaskRegistry.execute` validates config, then awaits the
  handler and returns a structured `TaskOutcome`
  (`succeeded` / `failed_recoverable` / `failed_permanent`); adapter and
  handler exceptions propagate to the caller.
- **Adapters** (`adapters.PlatformAdapter`) expose the minimum async
  operations (send message, react, press component, subscribe to events,
  close) with explicit results and an error hierarchy:
  `RecoverableAdapterError`, `PermanentAdapterError`, `CooldownError`
  (carries `retry_after_seconds`). Concrete adapters own client lifecycle.
- **Mock adapter** (`mock_adapter.MockAdapter`) is deterministic and fully
  test-controlled: scripted return values or exception instances per
  operation (FIFO), simulated latency, synthetic event emission, and full call
  recording as structured test data.

## Phase 3: SQLite runtime persistence

- **Stdlib-only async**: `sqlite3` dispatched via `asyncio.to_thread` —
  no aiosqlite dependency (it is itself a thread wrapper; the project stays
  zero-runtime-dependency). One connection, serialized by an asyncio lock;
  task-scoped transactions via a ContextVar let nested store calls join the
  enclosing transaction without letting concurrent coroutines bypass the lock.
- **States**: `pending`, `running`, `waiting`, `succeeded`, `failed`,
  `cancelled`. `pending`/`waiting` resume as-is after restart; `running`
  rows are recovered to `pending` (attempt counts preserved) by
  `recover_interrupted()`; the rest are terminal.
- **Persisted per instance**: identity (account snowflake + task id,
  unique), status, attempt count, UTC created/updated timestamps, last
  outcome (status + message only), retry time (`next_attempt_at`), and
  cooldown deadline + scope. Parameterized SQL everywhere; guarded
  transitions refuse illegal state changes and make terminal records
  idempotent; migrations run transactionally with `PRAGMA user_version`.
- **Clocks**: all timestamps and deadlines are timezone-aware UTC strings;
  remaining cooldown = deadline − now(UTC), so deadlines survive restarts
  (no persisted monotonic values).
- **Errors**: `sqlite3` faults map to `PersistenceError` /
  `DatabaseBusyError` (busy timeout) with causes preserved. No credentials
  are ever stored. Default rollback journal (WAL's shared memory is
  unreliable under PRoot).

## Phase 4: central rate limiter

`autoclaimer/ratelimit.py` coordinates concurrent workers with five
distinct concepts (never one delay for everything):

1. **Per-account pacing** — `min_interval_seconds` reserves spaced slots
   atomically, so simultaneous workers queue deterministically.
2. **Scope cooldowns** — explicit scope keys (`account:{snowflake}`,
   `route:{name}`, `task:{id}`, `channel:{id}`, `global`); unrelated scopes
   never block each other.
3. **Concurrency caps** — `max_concurrency` semaphore permits per account,
   released in `finally` so cancellation cannot leak them.
4. **Server-directed cooldowns** — `CooldownError.retry_after_seconds`
   recorded on configurable scope kinds, persisted via the state store's
   `scope_cooldowns` table (UTC wall-clock; restart-safe). Deadlines only
   extend and the server flag is sticky, so local settings can never
   shorten a server-imposed cooldown.
5. **Local pacing policies** — `RateLimitPolicy` parsed at three config
   layers; field-wise precedence: account override → task policy → global
   default → built-ins (server feedback outranks all of them).

`limiter.operation(...)` is an async context manager (cooldown gate →
permit → pacing slot); `limiter.run(...)` adds bounded retries with
exponential backoff — cooldown/recoverable errors retry up to
`retry_limit`, permanent errors fail immediately. All waits use
`asyncio` sleeps with deadline re-checks (no busy-wait, no blocking),
monotonic clock for in-process pacing, UTC wall-clock for persisted
cooldowns. `limiter.status()` exposes read-only cooldown/permit/pacing
snapshots. The limiter only responds conservatively to platform feedback;
it contains no evasion, probing, or rotation logic.

## Phase 5: worker manager and scheduler

- **Execution model**: one worker loop per enabled account (bounded task
  count, never one asyncio task per pending operation); tasks run
  sequentially within an account; total concurrent executions bounded by
  `settings.max_concurrent_accounts`.
- **Scheduling**: deterministic order — highest task `priority` first
  (configurable per task definition), then earliest retry time, then
  instance id. `WorkerManager.start()` recovers interrupted rows and
  materializes one instance per assigned (account, task) pair.
- **Duplication**: the store's atomic `claim()` is the single authority;
  an instance cannot execute twice in a running process.
- **Uncertain shutdown outcome**: graceful stop drains workers within
  `settings.shutdown_timeout_seconds`; hard-cancelled in-flight ops stay
  `running` (claim committed, outcome unknown) and are re-run after
  restart — at-least-once semantics; handlers must tolerate duplicate
  side effects.
- **Rate limiting**: `RateLimitedAdapter` routes every platform operation
  through the central limiter (route scope per operation kind); cooldowns
  and retries are handled there, keeping scheduling policy out of task
  handlers.
- **Controls**: `set_account_enabled`, `pause_account`/`resume_account`,
  `pause_all`/`resume_all` (gate new claims only — in-flight work and
  open transactions are never killed mid-flight), `queue(account)`,
  `task_status(id)`, `status()` snapshots, and graceful `stop()` that
  closes adapters.

## Phase 6: interactive CLI and logging

- **CLI** (`python -m autoclaimer`, mock-adapter demo): a command loop
  (`start/stop`, `pause/resume` globally or per account,
  `enable/disable <account>`, `accounts`, `tasks [state]`, `queue
  [account]`, `cooldowns`, `errors`, `status`, `help`, `quit`). The
  controller owns components and delegates every action to the worker
  manager, store, and limiter — no orchestration logic in the CLI. Input
  is validated (snowflake ids, known accounts, task states, argument
  counts, shell-style quoting); EOF, Ctrl+C, and `quit` all drain workers
  gracefully and close the store; shutdown while workers are active
  cancels them after `shutdown_timeout_seconds` (uncertain instances stay
  `running` and recover on restart — the manager is restartable).
- **Logging**: `logging_setup.setup_logging` configures a
  `RotatingFileHandler` (size + retention from the optional `logging`
  config block: `log_file`, `max_bytes`, `backup_count`, `level`) plus an
  optional stderr mirror. The formatter appends structured context
  (`account_id`, `task_id`, `operation`, `outcome`, `scope`) and then
  redacts the *entire* formatted line — including exception tracebacks —
  scrubbing Discord-shaped tokens, webhook URLs, and
  authorization/token/password/secret values. CLI output is redacted with
  the same filter; authentication tokens are never printed.

## Phase 7: discord.py-self adapter (event-driven)

`autoclaimer/discord_adapter.py` implements the Phase 2 `PlatformAdapter`
contract with one isolated `discord.Client` per account, verified against
**discord.py-self 2.1.0** (installed source — no invented APIs). The
runtime is **event-driven: no history polling** — `Messageable.history` is
never called and historical retrieval is not part of the contract.

- Lifecycle: `Client.login(token)` → background `Client.connect(reconnect=True)`
  → `wait_until_ready()`; operations lazily ensure readiness; `close()` is
  idempotent (stops event delivery, closes the client, awaits tasks); a
  dead connection task surfaces as a recoverable error until restart.
- Outbound operations: `Messageable.send`, the light
  `http.add_reaction` route, and `Button.click()` for components
  (`str` result = link button → `acknowledged=False`).
- Inbound events: the fork's single-handler `Client.event()` registration
  (by coroutine name) hosts the adapter's bridges for `on_message`,
  `on_reaction_add(reaction, user)`, `on_raw_reaction_add(raw)`, and
  `on_raw_message_edit(raw)`. Events normalize to framework types —
  `MessageCreatedEvent`, `ReactionAddedEvent` (cached message context
  included when present), `RawReactionAddedEvent` (fires for **every**
  reaction — including uncached messages — carrying identifiers, emoji,
  and the reacted-to author id when provided), and `MessageEditedEvent`
  (pre/post snapshots; served from the raw edit event so uncached edits
  deliver too, with `before=None` when not cached) — carrying identifiers,
  content, and UTC timestamps; no library objects cross the boundary.
  **Cache/ordering semantics verified in the fork**: the rich
  `reaction_add` fires only for gateway-cached messages; raw reaction
  dispatch precedes the rich event, so a cached reaction reaches
  consumers twice (raw then rich) unless they subscribe selectively;
  edit events deliver exactly once (the raw bridge replaces
  `on_message_edit`; the fork passes the same updated-message object to
  both).
- Dispatching: a shared `EventDispatcher` gives each subscriber its own
  bounded queue (default 64) and pump task — consumer exceptions are
  isolated and logged, one slow consumer never blocks others, overflow
  drops the OLDEST event (counted + logged; fresh state wins).
  Per-subscriber delivery is FIFO in dispatch order; cross-event-type
  ordering follows the library's task-per-event scheduling. Subscribers
  register before `start()` (no startup-loss window); reconnects never
  duplicate handlers (one-time registration on the persistent client);
  `close()` stops all pumps — stragglers are delivered to no one.
- Pacing: a small random delay precedes every mutating action
  (`jitter_range`, default 0.4–1.2s). This is conservative pacing for
  personal, non-abusive use — **not** detection evasion.
- Light endpoints only: channels resolve from the gateway cache; the one
  moderate read is `get_message` (needed to obtain component objects).
  Heavy endpoints (`fetch_user`, `fetch_channel`, …) are never called.
- Error translation: `RateLimited`/HTTP 429 → `CooldownError(retry_after)`
  (recorded and honored by the central limiter); `CaptchaRequired` →
  permanent failure (CAPTCHAs are never bypassed); `Forbidden`/`NotFound`/
  `LoginFailure` → permanent; 5xx, `ConnectionClosed`, `GatewayNotFound`,
  `InvalidData` (uncertain interaction outcome) → recoverable.
- No Discord objects leak past the adapter; tokens come from the
  credential provider at login time and are never stored, logged, or
  persisted.

### Platform restrictions (read this)

User-account automation may violate Discord's Terms of Service. This
project is built for the owner's personal use without abusing the API: no
anti-detection, CAPTCHA bypass, rate-limit evasion, enforcement probing,
or account rotation is implemented; actual rate-limit responses are
honored (stop/defer via the limiter). Unsupported: voice, heavy REST
fetches, and any operation the library does not expose for user accounts.

### Secure local development credentials

Never put tokens in JSON files, CLI arguments, source, logs, or SQLite. A
minimal local provider reads them from the environment:

```python
import os
from autoclaimer.credentials import CredentialProvider


class EnvironmentCredentials:
    """Reads tokens from AUTOCLAIMER_TOKEN_<ACCOUNT_ID> env vars."""

    def get_token(self, account_id: int) -> str:
        token = os.environ.get(f"AUTOCLAIMER_TOKEN_{account_id}")
        if not token:
            raise RuntimeError(f"missing env var AUTOCLAIMER_TOKEN_{account_id}")
        return token

    def close(self) -> None:
        return None
```

Set env vars in your shell profile or a secrets manager — never commit
them. Opt-in live checks (`tests/integration/test_discord_live.py`) are
skipped unless `AUTOCLAIMER_LIVE_DISCORD_TOKEN` is set explicitly.

## Next milestone (Phase 8, not started)

SQLite state-store interface and migrations, then the task registry, mock
adapter, and concurrent worker manager.
