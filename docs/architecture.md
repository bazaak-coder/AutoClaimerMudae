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

One SQLite database (`state.db`), schema **v5**, migrated step-by-step at open
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
