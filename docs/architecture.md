# Architecture

AutoClaimer is a small, explicitly-layered async Python application. This page
maps the layers, the runtime flows, and the design rules they follow.

## Module map

```
autoclaimer/
├── __main__.py          Entry point / demo composition
├── cli.py               Controller + interactive REPL
├── config.py            JSON loading, validation, override resolution
├── models.py            Typed frozen configuration models
├── registry.py          Task types, TaskContext, validation + dispatch
├── workers.py           WorkerManager, per-account loops, RateLimitedAdapter
├── ratelimit.py         Pacing, permits, scoped cooldowns, policy layering
├── persistence.py       SQLite task-state store + migrations (schema v7)
├── credential_store.py  Fernet-encrypted token store (CredentialProvider)
├── credentials.py       CredentialProvider protocol
├── discord_adapter.py   discord.py-self adapter (events + operations)
├── adapters.py          Platform contracts + event dispatcher
├── mock_adapter.py      Deterministic test double
├── mudae_tasks.py       roll / mu / rt handlers + reply correlation
├── mudae_parser/        Vendored Mudae message parser (byte-identical upstream)
└── logging_setup.py     Rotating, redacting, structured logging
```

Design rules (enforced by `agents.md`): every runtime feature defines its
**failure**, **cancellation**, **persistence**, and **recovery** behavior
before implementation; failures are classified with a single owning layer;
uncertain mutations are never blindly retried.

## Composition and lifecycle

```
config.json ─┐
accounts.json┴─> load_config() ─> AppConfig / AccountsConfig (validated)
                     │
              TaskStateStore(state.db)      RateLimiter(store)
                     │                            │
              TaskRegistry ── register_mudae_tasks(store)
                     │                            │
              WorkerManager(registry, store, limiter, app, accounts,
                            adapter_factory, credentials)
                     │
              AutoClaimerController ── CliSession (REPL)
```

1. **Startup** (`WorkerManager.start`) is transactional: interrupted rows are
   recovered (`running → pending`), instances are materialized per assignment,
   resolved task configs are validated against the registry (fail-soft per
   instance), and one worker loop is spawned per enabled account.
2. **Per-account loops** claim due instances one at a time (sequential per
   account), bounded globally by `max_concurrent_accounts` execution slots.
3. **Shutdown** (`stop`) cancels and drains workers within
   `shutdown_timeout_seconds`; anything still `running` is recovered to
   `pending` on the next start.

## Task execution flow

```
due_instances(account)
  → _execute(record)
      ├─ pre-claim checks: assignment, registration, resolved config
      ├─ resolve ratelimit policy (account → task → global) and bind it
      │  to the handler's adapter view (RateLimitedAdapter.with_policy)
      ├─ wait out account cooldowns (before taking a global slot)
      ├─ claim(row)                     # pending|waiting → running
      ├─ inject config: parameters + resolved global channel
      ├─ registry.execute(TaskContext)  # validator, then handler
      └─ record outcome: succeeded / failed_recoverable (+retry_at)
                           / failed_permanent / account-fatal stop
```

Failure ownership: the **worker** owns retry counting and terminal states; the
**rate limiter** owns transport retries, pacing and cooldowns; the **adapter**
owns connection lifecycle and account-fatal classification. A
`FAILED_RECOVERABLE` outcome is retried up to the resolved task's
`retry_limit`; a learned `retry_at` deadline supersedes generic backoff.

## Event flow (Discord → handler)

```
discord.py-self gateway events
  → DiscordAdapter._bridge_*      (normalize: message / reaction added)
  → EventDispatcher               (bounded per-subscriber queues,
                                    consumer isolation, loss callbacks)
  → _Waiter (mudae_tasks)          (filter: channel, author, time window,
                                    message-id dedup, reaction target,
                                    dispatch-gating)
  → handler dialogue loop          (reply correlation, cooldown parsing)
```

Correlation guarantees: waiters subscribe **before** the triggering operation;
outbound sends and claim reactions carry an `on_dispatch` wire-boundary
callback, so traffic arriving before the operation is on the wire cannot pass
as its reply. Event loss during a wait fails fast to a conservative
(bounded-requery / uncertain) path — never a fabricated success.

## Rate limiting

`RateLimiter` serializes every platform operation through:

1. cooldown gates (account, route, global scopes — server-directed
   cooldowns are authoritative and persisted),
2. permits (`max_concurrency`, cancellation-safe),
3. pacing slots (`min_interval_seconds`, per account).

Policies merge field-wise with precedence **account → task → global →
defaults** and are bound to each execution's adapter view, so overrides
actually reach the operations they configure.

## Persistence

One SQLite database (`state.db`), schema **v7**, migrated step-by-step at open
(each migration is a single transaction; post-migration shape verification
catches tampered/foreign schemas):

| Table | Contents |
|---|---|
| `task_instances` | One row per (account, task): status, attempts, last outcome, retry time. |
| `scope_cooldowns` | Active rate-limit cooldowns (survive restarts). |
| `mudae_state` | Learned Mudae deadlines per account (claim / rt / rolls). |
| `account_credentials` | Fernet-encrypted account tokens (see [credentials.md](credentials.md)). |

Threaded SQLite access is serialized by a per-store connection guard, and
transaction cancellation always drains and rolls back — cancellation can
abandon an *await*, never the database's consistency.

## Security model

- Tokens: Fernet-encrypted at rest; the key never touches the database,
  configuration, or logs; retrieval goes through the `CredentialProvider`
  boundary only; handlers never see key material or ciphertext.
- Configuration: credential-like keys are rejected anywhere in task
  parameters (recursively).
- Logging: every output path passes a redaction filter (tokens, webhooks,
  authorization headers, key/value secrets), including rotation-failure dumps.
- CLI: destructive credential operations are explicit; token entry uses
  hidden input; tokens are never command arguments.

## Composition root (ROADMAP item 26 — required design note)

`autoclaimer/runtime.py` (`build_runtime`) constructs and owns every
dependency; `__main__` stays a thin entry (argument parsing →
composition → session → signals). **Construction is separate from
execution**: nothing logs into Discord or claims work during
`build_runtime` — the worker opens each account's `DiscordAdapter`
lazily on first execution.

### Ownership graph (arrows = owns/closes; order matters)

```
build_runtime() → Runtime (owns everything below)
  TaskStateStore          opened 1st, closed LAST
  SQLiteCredentialStore   opened 2nd, closed 2nd-last (IS the CredentialProvider)
  TaskRegistry            pure (register_mudae_tasks(registry, store))
  RateLimiter             store-backed cooldowns
  AccountHealthMonitor    shared: injected into the controller → WorkerManager
  AutoClaimerController   → WorkerManager → RateLimitedAdapter(per account)
                                       → DiscordAdapter(per account, lazy login)
  AuditLog(store)         ×2: the manager's and the composition's (lifecycle)
```

### Startup sequence — each step's failure closes everything opened
before it, in reverse order, then raises `RuntimeStartupError` with the
actionable operator fix

1. `load_config` — validation failure aborts with NOTHING opened (no
   `state.db` is even created).
2. `setup_logging`.
3. **Production**: `load_fernet_keys()` — missing/invalid key aborts
   with setup instructions. **Demo**: warn and continue (historical
   posture).
4. `TaskStateStore.open()`.
5. **Production**: `SQLiteCredentialStore.open()` + preflight — every
   ENABLED account must have a stored token (missing ids listed with the
   `credential set` hint; disabled accounts are exempt).
6. Registry: `register_mudae_tasks(registry, store)`.
7. **Production**: registry preflight — every enabled account's assigned
   task id must be REGISTERED (a task defined in config but with no
   registered type is listed and aborts startup — fail closed, not a
   silently skipped task).
8. `RateLimiter`, `AccountHealthMonitor`, lifecycle wiring, and the
   controller (with `health` injected) — per-account
   `DiscordAdapter(account_id, credentials=store,
   send_jitter_range=settings.send_jitter_seconds, on_lifecycle=hook)`,
   one isolated adapter per account, one shared credential provider.

### Lifecycle wiring (gateway → health/audit)

`DiscordAdapter.on_lifecycle` fires on TRANSITIONS only (deduplicated):
READY/connect → `CONNECTED`; non-fatal connection death or gateway
disconnect → `DISCONNECTED`; the account-fatal latch → `FATAL` (exactly
once). The hook records health synchronously (`record_connected` /
`record_disconnected` / fatal → `record_failure(fatal=True)` — all
latch-preserving: `AUTH_FAILED`/`DISABLED` survive reconnects) and
returns the audit emission coroutine; the ADAPTER tracks that awaitable
in a bounded set and drains it in `close()` — observers are never
fire-and-forget, and a raising observer is logged and dropped, never
breaking the adapter. Mid-flight reconnect blips are observed via
`on_ready`/`on_disconnect` bridges (the `connect(reconnect=True)` task
survives them, so its done-callback alone cannot see them).

### Shutdown sequence (reuses the established machinery)

Signal → session-task cancel → `CliSession.run()` finally →
`controller.shutdown()`: workers drained within the shutdown budget →
adapters closed (each close drains that adapter's lifecycle observers
under a 5s deadline, itself bounded by the manager's per-adapter close
timeout — an operator shutdown budget under ~5s cuts the drain short,
cancelling straggling observers) → credential store closed → state
store closed. Teardown emits NO lifecycle transitions (observer code
never runs during close): an operator-initiated shutdown therefore
records no final `account.disconnected`. Every
step is idempotent and exception-guarded; `controller.shutdown()` is
terminal/one-shot; `WorkerManager.stop()` remains restartable.
**Cancellation**: an outer cancellation of teardown completes the
synchronous cleanup and re-raises (adapters) or completes the bounded
drain first; partial-startup cleanup (which never constructed a
controller) closes components directly in reverse ownership order.

### Failure/cancellation behavior per resource

| Resource | Failure behavior | Cancellation behavior |
|---|---|---|
| Config load | typed `ConfigValidationError`; nothing opened | n/a (sync) |
| Fernet key (production) | `RuntimeStartupError` with setup steps | n/a (sync) |
| State store open | typed `PersistenceError`; cleanup closes it | guard-drained |
| Credential store open/preflight | actionable abort; store + db closed | idempotent sync close |
| Registry/limiter/controller | pure construction; cannot fail open | n/a |
| DiscordAdapter construction | offline; login deferred to worker | n/a at build time |
| Lifecycle observers | logged + dropped, adapter unaffected; a sync-raising observer consumes its transition (logged) | tracked, drained ≤5s in close (further bounded by the manager's per-adapter close timeout); stragglers cancelled AND reaped |
