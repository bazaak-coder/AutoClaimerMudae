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
| `autoclaimer/mudae_parser/` | implemented (vendored) | LibzentSuite's stdlib-only Mudae parser, byte-identical at revision `41b0775`; rules/locales/settings engine with its 80-test ported suite. See its VENDORED.md. |
| `autoclaimer/mudae.py` | implemented | Thin interface: `parse_mudae()` (str/ChannelMessage/MessageCreatedEvent/embed shapes), typed `ClaimStatus` ($mu lookups ride the claim-timer rule — no dedicated entry) and `RollsResetStatus` ($rtu replies; perk-dependent 5–20h — always learned, never assumed), and `anchor_deadline()` (relative→absolute UTC at the single persistence boundary). Durations stay relative until anchored. |
| `autoclaimer/mudae_tasks.py` | implemented | The Mudae workload (milestone 7): reply matching (`await_mudae_reply`/`await_mudae_event` — subscribe-before-send, per-message-id dedup, loss/timeout → bounded re-query) and recurring task handlers `roll`/`rt`/`mu` via `register_mudae_tasks(registry, store=..., clock=...)`. Roll gates on the learned window, claims claimable cards by reacting once (rival pairs ignored; owned "Belongs to" cards never reacted; a lost confirmation is an UNCERTAIN outcome, never retried), learns quota/cooldown deadlines. `$rt` use is followed by the mandatory `$rtu` check. `$mu` lookups clear (available) or set (cooldown) the claim deadline. Recurring rounds reschedule at learned deadlines (fresh attempt windows via `reschedule_success`). |

Planned boundaries are documented, not implemented; no stub modules exist to
avoid speculative code.

## Configuration model

Two files: `config.json` (global settings, global channels, task defaults,
task definitions) and `accounts.json` (per-account assignments and
overrides).

- Account IDs are integers (Discord snowflakes).
- Task IDs are short identifier strings.

### Precedence (explicit)

```
global defaults (task_defaults)  <  task definition  <  account override
```

- A setting omitted at an outer layer falls back to the layer above it.
- Account overrides preserve explicit `false`; an omitted override key means
  "not overridden", not "false".

### Global channels (every account, configured once)

Channels are **global**: every account uses the same channel configuration.
There are no per-account or per-task channel settings — a `channel` key in
task `parameters` or account `overrides` is rejected with an actionable
error (breaking change; previously `channel` was a per-task parameter).

```json
{
  "version": 1,
  "channels": {
    "roll_claim": 123456789012345678,
    "mu": null,
    "rt": null
  }
}
```

| Setting | Purpose | Required | Falls back to |
|---|---|---|---|
| `roll_claim` | Mudae rolling AND claim interactions | yes | — |
| `mu` | `$mu` commands | no | `roll_claim` |
| `rt` | `$rt` and `$rtu` commands (same channel for both) | no | `roll_claim` |

- `mu`/`rt` may be omitted or `null` (identical effect: use `roll_claim`).
- An explicitly configured `mu` or `rt` is always respected.
- Channel IDs must be positive integer snowflakes; strings, floats, and
  booleans are rejected (`true` is not the snowflake `1`).
- A config without a `channels` section is rejected with instructions —
  channel IDs are never invented or defaulted silently.
- Fallback resolution happens in exactly one place
  (`ChannelsConfig.effective()`); task handlers receive the resolved
  channel and never implement fallback logic themselves.

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
safety net.

### Encrypted account-token storage (Fernet + SQLite)

`autoclaimer/credential_store.py` ships an encrypted token store backed by
the same `state.db` as task state (schema v5 table `account_credentials`):

- Tokens are encrypted with **Fernet** (authenticated encryption from the
  `cryptography` package) **before** they touch SQLite. The database never
  contains plaintext tokens, and the encryption key never lives in the
  database, `config.json`, `accounts.json`, task state, or logs.
- The key comes from the environment:
  - `AUTOCLAIMER_FERNET_KEY` — the primary key (required). Missing or
    invalid keys **fail closed**: credentials are simply unavailable;
    there is no plaintext fallback and no automatic key generation.
  - `AUTOCLAIMER_FERNET_RETIRED_KEYS` — optional comma-separated retired
    keys, kept only so rotation can decrypt pre-rotation rows.
- First-time setup: in the CLI run `credential generate-key`, store the
    printed key outside the database (secret manager, restricted env file),
    export it as `AUTOCLAIMER_FERNET_KEY`, and restart. Generating a key
    never overwrites or invalidates an existing one — but losing the active
    key makes existing credentials unrecoverable.
- Managing tokens (interactive CLI, `state.db` next to `config.json`):

  ```
  credential set <account_id>        # prompts for the token (hidden input);
                                      #  encrypts and upserts
  credential has <account_id>
  credential delete <account_id>
  credential accounts
  credential rotate
  ```

  Token entry uses a dedicated hidden-input reader (`getpass`): typed
  characters are not echoed on a terminal. Without a controlling TTY,
  getpass prints an explicit warning and reads echoed stdin — piped use
  stays possible, visibly. The token is never accepted as a command
  argument (keeping it out of shell history).

- Adapters request tokens at login through the existing
  `CredentialProvider` protocol — task handlers never see keys, ciphertext,
  or credential rows. A missing/undecryptable credential is an
  account-fatal error (the account stops; other accounts continue).
- Removing an account from `accounts.json` does **not** delete its stored
  token (a config typo must not destroy a secret) — deletion is the
  explicit `credential delete` command.
- **Key rotation** (implemented and tested, including interrupted-and-
  resumed rotation): set the new key as `AUTOCLAIMER_FERNET_KEY`, move the
  old key into `AUTOCLAIMER_FERNET_RETIRED_KEYS`, restart, run
  `credential rotate`. Every row is decrypted with any loaded key,
  re-encrypted under the primary, verified by decrypting the new ciphertext
  back, and committed row-by-row (resumable; an interrupted pass leaves
  remaining rows decryptable via the retired key). Rotation is complete
  when it reports `0` re-encrypted; only then may the retired key be
  discarded. Rotation fails loudly if a row cannot be decrypted (missing
  retired key) — it never deletes or overwrites credentials it cannot read.
- **Backups**: back up `state.db` AND the key material together; a database
  without its key is unrecoverable ciphertext, and a key without the
  database is useless. Store both with equivalent secrecy.

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
  close) plus cache-only channel discovery
  (:meth:`~autoclaimer.adapters.PlatformAdapter.get_channel` /
  :meth:`~autoclaimer.adapters.PlatformAdapter.validate_channel` —
  gateway-cache lookups normalized to
  :class:`~autoclaimer.adapters.ChannelInfo`, never a REST fetch) with
  explicit results and an error hierarchy:
  `RecoverableAdapterError`, `PermanentAdapterError`,
  `CooldownError` (carries `retry_after_seconds`),
  `UncertainOutcomeError`, and `AccountFatalError` (stops the whole
  account). Concrete adapters own client lifecycle.
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
- **Mudae deadlines** (schema v3, `mudae_state` table): durable
  per-account claim and `$rt` reset deadlines as absolute UTC
  timestamps. A fresh lookup REPLACES the stored deadline (the reply is
  the current truth); corrupt rows are contained per the same policy as
  `task_instances`/`scope_cooldowns` (skipped with a typed, named error
  log). The vestigial instance-level `cooldown_until`/`cooldown_scope`
  columns remain untouched and unused by design (audit F2 decision:
  Mudae deadlines live in `mudae_state`, keeping `task_instances`
  generic).

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
- Pacing: a random delay precedes every mutating action. Sends use a
  dedicated, tighter humanization range (`send_jitter_range`, default
  0.203–0.567s) configurable as `settings.send_jitter_seconds:
  [low, high]` in config.json; reactions and component presses use the
  general `jitter_range` (default 0.4–1.2s). This is conservative pacing for
  personal, non-abusive use — **not** detection evasion.
- Light endpoints only: channels resolve from the gateway cache; the one
  moderate read is `get_message` (needed to obtain component objects).
  Heavy endpoints (`fetch_user`, `fetch_channel`, …) are never called.
- Error translation: `RateLimited`/HTTP 429 → `CooldownError(retry_after)`
  (recorded and honored by the central limiter; a 429 means the request
  was rejected, not applied — post-cooldown retry is safe even for
  mutations). **Mutating calls** (`send_message`, `add_reaction`,
  `Button.click`) translate with mutation awareness: a completed 5xx
  response or an unrecognized library error maps to
  `UncertainOutcomeError` — the action may have been applied and is
  never auto-retried (PR #2 review fix); read paths (`fetch_message`)
  and pre-mutation failures (readiness) keep safe bounded retries.
  `CaptchaRequired`, `LoginFailure`/`AuthFailure` (one class in this
  fork), and `ConnectionClosed` with a fatal close code (4004, 4010-4014
  — the only codes the library itself refuses to reconnect on; the
  client is already terminal when they surface) →
  **`AccountFatalError`**: the adapter latches the verdict permanently
  (no re-login, no client rebuild) and the worker manager fails the
  account's queued instances, disables the account, and stops its
  worker — never retries. CAPTCHAs are never bypassed.
  `Forbidden`/`NotFound` → per-operation permanent; 5xx on reads,
  non-fatal `ConnectionClosed` and `GatewayNotFound` on reads or
  pre-mutation phases → recoverable (bounded retries); non-fatal
  connection failures during a mutation → uncertain (outcome unknown);
  `InvalidData` (uncertain interaction outcome) → uncertain.
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
from autoclaimer.credentials import CredentialProvider, CredentialProviderError


class EnvironmentCredentials:
    """Reads tokens from AUTOCLAIMER_TOKEN_<ACCOUNT_ID> env vars."""

    def get_token(self, account_id: int) -> str:
        token = os.environ.get(f"AUTOCLAIMER_TOKEN_{account_id}")
        if not token:
            # The protocol's typed error: the adapter maps it to an
            # account-fatal stop (never a generic permanent failure).
            raise CredentialProviderError(f"missing env var AUTOCLAIMER_TOKEN_{account_id}")
        return token

    def close(self) -> None:
        return None
```

Set env vars in your shell profile or a secrets manager — never commit
them. Opt-in live checks (`tests/integration/test_discord_live.py`) are
skipped unless `AUTOCLAIMER_LIVE_DISCORD_TOKEN` is set explicitly.

## Phase 8: integration and release audit

End-to-end lifecycle verified against the real components (mock adapters):
config load/validate → credential resolution (provider contract; tokens
never logged) → SQLite open + interrupted-work recovery → adapter/scheduler
init → concurrent workers → sequential per-account execution →
limiter-honored server cooldowns (retried inside `limiter.run`) →
persisted outcomes, retry schedules, and scope cooldowns → pause/resume/
disable/queue controls → clean shutdown → restart recovery. See
`tests/integration/test_lifecycle.py`.

### Reliability findings (fixed, with regression tests)

- Worker tasks had no exception observation: a transient store failure
  silently killed the account worker while `status()` reported RUNNING.
  Now each loop iteration is guarded (logged with account context, one
  idle-interval back-off) and task death is logged by a done-callback.
- Runtime `set_account_enabled(True)` used a detached spawn that could
  race with shutdown and leak a worker task against a closed store;
  enables now bootstrap only on a running manager, and detached tasks
  are observed.

### Supported versions and recovery behaviour

- Python ≥ 3.11 (developed and CI-tested on 3.14); discord.py-self 2.1.x
  (pinned `>=2.1.0,<2.2`; the adapter is verified against 2.1.0 source).
- Recovery: `pending`/`waiting` instances resume as-is; `running`
  instances (uncertain outcome after a crash or hard shutdown) are reset
  to `pending` at startup with attempt counts preserved — at-least-once
  semantics; task handlers must tolerate duplicate side effects.
  Persisted UTC cooldowns survive restarts (remaining = deadline − now).
- Known limitations: per-account concurrency permits only; limiter
  default policy governs adapter operations (per-task policies not yet
  wired per-operation); `reaction_add` rich events fire only for cached
  messages (raw events cover the rest); CAPTCHA/auth-fatal errors stop
  the account permanently (never bypassed, never retried — re-enable is
  the explicit revive path after the credential is fixed); user-account
  automation may violate Discord's ToS (see platform restrictions above).

## Benchmarks

Hot-path micro-benchmarks (not part of the pytest suite — timings are
reporting, not assertions):

```bash
.venv/bin/python scripts/benchmark.py
```

Reference numbers (Python 3.13.5, this workspace, 2026-10-28):
dispatcher fan-out ~145k events/s (1 subscriber) / ~80k (4 subscribers);
SQLite claim+success cycle ~505/s; limiter run overhead ~60k ops/s;
registry execute overhead ~665k/s. Methodology notes live in the script
docstrings; measured values in the knowledgebase.

## Next milestone (composition root / live deployment wiring, not started)

The Mudae task handlers shipped (milestone 7); the remaining work is the
live entry point wiring config + credentials + Discord adapters + the
mudae task registry together (see knowledgebase audit notes).
