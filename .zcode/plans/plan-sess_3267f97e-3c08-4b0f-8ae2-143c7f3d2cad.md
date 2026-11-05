# ROADMAP item 26 — Production Composition Root (Phase 6, P1)

## Context

`python -m autoclaimer` currently boots a demo composition (ping task + MockAdapter). The owner's re-added ROADMAP Phase 6 (PR #27, merged content) specifies a production composition root: real Discord/Mudae runtime by default, demo behind `--demo`, fail-closed credentials, lifecycle→health/audit wiring, thin `__main__`, deterministic mock-based composition tests, docs. The acceptance criteria and a **required design note** (ownership graph + startup/shutdown sequences) are in ROADMAP.md Phase 6.

Exploration established: `SQLiteCredentialStore` IS a `CredentialProvider` (drop-in for `DiscordAdapter(credentials=…)`); `register_mudae_tasks(registry, store=…)` registers roll/rt/mu; `RateLimiter(default_policy=resolve_policy(app.ratelimit), store=store)`; signal handling (`install_termination_handlers`) and `CliSession.run()`'s finally→`controller.shutdown()` are reusable as-is. Three gaps must be filled: (1) no connection-lifecycle hook on `DiscordAdapter`, (2) no `record_connected/disconnected` on `AccountHealthMonitor`, (3) `AutoClaimerController` cannot inject a health monitor (WorkerManager can).

## Design note (required by the roadmap; lands in docs/architecture.md)

**Ownership graph** (arrows = owns/closes):

```
runtime.build_runtime()  →  Runtime (owns everything below, in order)
  TaskStateStore        (opened 1st; closed last)
  SQLiteCredentialStore (opened 2nd; IS the CredentialProvider; closed 2nd-last)
  TaskRegistry          (pure; register_mudae_tasks(registry, store))
  RateLimiter           (stateless-ish; store-backed cooldowns)
  AccountHealthMonitor  (owned by WorkerManager via new injection)
  AutoClaimerController → WorkerManager → RateLimitedAdapter(per account)
                         → DiscordAdapter(per account, lazy login)
  AuditLog(store)       ×2 (manager's + composition's for lifecycle events)
```

**Startup sequence** (each step's failure closes everything opened before it, in reverse):
1. `load_config` (validation failure → nothing opened)
2. `setup_logging`
3. **Production only**: `load_fernet_keys()` — missing/invalid → actionable error, abort (demo: warn-and-continue, current behavior)
4. `TaskStateStore.open()`
5. **Production only**: `SQLiteCredentialStore.open()` + preflight `enabled accounts ⊆ credential_store.account_ids()` (missing tokens listed, abort)
6. Registry + `register_mudae_tasks`
7. **Production only**: registry preflight — every enabled account's assigned task id must be registered (unregistered listed, abort)
8. `RateLimiter`, `AccountHealthMonitor`, lifecycle wiring (hook → health.record_connected/disconnected + audit emit), `AutoClaimerController` (new `health=` passthrough)
9. Return `Runtime` — nothing has logged into Discord (adapters constructed lazily-login by the worker's `_adapter_for`)

**Shutdown sequence** (reuses existing machinery): signal → session-task cancel → `CliSession.run()` finally → `controller.shutdown()` (manager.stop → drain workers ≤ budget, close adapters → credential store close → state store close — each already idempotent and exception-guarded). Lifecycle callbacks: emitted inline within the client's event tasks (bridges) → they die with the client on `close()`; the composition's callback is sync-record (health) + tracked-task (audit) with a bounded tracked set drained on shutdown. **Cancellation behavior**: adapter `close()` is idempotent and cancellation-safe (existing); `controller.shutdown()` is terminal/one-shot (existing); partial-startup cleanup never constructs a controller, so it closes components directly in reverse ownership order.

## Implementation

**1. `autoclaimer/discord_adapter.py` — lifecycle hook (new surface)**
- `LifecycleTransition` enum: `CONNECTED`, `DISCONNECTED`, `FATAL`.
- `__init__` kwarg: `on_lifecycle: Callable[[LifecycleTransition], Awaitable[None] | None] | None = None`.
- Emit points (transition-only, deduped): READY observed → CONNECTED; non-fatal connection death (`_connect_finished` recoverable path) → DISCONNECTED; fatal latch sites (`start`/`_await_ready`/`_connect_finished`) → FATAL. Emissions are awaited inline inside the existing client-event/bridge tasks with per-call exception isolation (logged, never break the adapter); guarded `if self._closed: return` so teardown doesn't fire hooks. Registration rides `_register_bridges` (adds `on_ready`/`on_disconnect` bridges — no name clash with the existing four).
- No behavior change when `on_lifecycle is None`.

**2. `autoclaimer/health.py` — two methods**
- `record_connected(account_id)` → HEALTHY, and `record_disconnected(account_id)` → DISCONNECTED — both latch-preserving (never overwrite AUTH_FAILED/DISABLED; same pattern as `record_rate_limited`), with `_log` context.

**3. `autoclaimer/cli.py` — health injection**
- `AutoClaimerController.__init__` gains `health: AccountHealthMonitor | None = None`, passed through to `WorkerManager` (which already accepts it).

**4. NEW `autoclaimer/runtime.py` — the composition root**
- `RuntimeStartupError(Exception)` carrying actionable, redaction-safe messages.
- `@dataclass Runtime`: app, accounts, store, credential_store (None in demo), registry, limiter, health, controller, audit, demo flag, adapter_factory.
- `build_runtime(config_path, accounts_path, *, demo: bool = False) -> Runtime` — the startup sequence above; production factory: `lambda account_id: DiscordAdapter(account_id=account_id, credentials=credential_store, send_jitter_range=tuple(app.settings.send_jitter_seconds), on_lifecycle=make_lifecycle_callback(account_id))` — one `DiscordAdapter` per account, shared credential store, lazy login (construction only). Demo factory: MockAdapter + ping registry (moved from `__main__`), warn-and-continue credential posture (exact current behavior).
- Lifecycle callback: sync `health.record_*` + audit emit as a tracked task (bounded set; `Runtime.drain_lifecycle_tasks()` called from the session-end path; failures logged).
- `db_path = Path(config_path).resolve().parent / "state.db"` (existing rule; fix the docs snippet that used CWD).

**5. `autoclaimer/__main__.py` — thin entry**
- `argparse` (first in repo): `autoclaimer [--demo] [config.json] [accounts.json]`; **production default**, `--demo` = today's behavior.
- `async_main` → `build_runtime(...)` + existing session/signal flow unchanged. Startup failure → actionable stderr message, exit code 2, nothing leaked.

**6. Docs**
- `docs/architecture.md`: new "Composition root" section = the design note above (ownership graph + sequences + per-resource failure/cancellation behavior).
- `docs/running.md`: Booting section rewritten — production startup (prerequisites: Fernet key + `credential set` per enabled account; failure modes), `--demo` escape hatch; fix the library snippet's `state.db` path to the config-dir rule.
- `README.md`: package table row for `runtime.py`; "Next milestone" section retired.
- Knowledgebase entry (agents.md §4).

**7. Tests (deterministic, mock-based; no Discord in CI)**
- NEW `tests/test_composition.py`, reusing `test_cli.py`'s `build_cli`/tmp-file patterns: production build (registry has roll/rt/mu; adapters are distinct per account, share the credential store identity, nothing logged in); fail-closed matrix (missing/invalid key, missing tokens, unregistered task ids — each: actionable error + all opened components closed, verified via store state); demo flag behavior (ping registry, MockAdapter, key-missing tolerated); partial-startup cleanup (component failure injected at each boundary → reverse-order closure); shutdown idempotence.
- `tests/test_discord_adapter.py` additions: lifecycle emission via the existing fake-client pattern (on_ready→CONNECTED, disconnect→DISCONNECTED, fatal→FATAL; dedupe; closed-guard; hook exception isolation; None-hook no-op).
- `tests/test_account_health.py`: connected/disconnected latch-preservation.
- Live login/reconnect verification stays env-gated (`tests/integration/test_discord_live.py`); add one gated connected-transition smoke there.

## Verification

- Full gates per agents.md §10 (ruff, format, mypy strict, pytest, `git diff --check`) — expect ~850+ tests.
- Manual: `python -m autoclaimer --demo` (works as today); `python -m autoclaimer` without a Fernet key (actionable error, exit 2, no state leaked); local `coderabbit review --agent` pass before push (new house practice).
- Own PR (branch `feat/production-composition-root`), CI matrix green, then independent review per the completion gate; findings fixed and re-reviewed before merge is requested.

## Out of scope (unchanged behavior)

Worker/scheduler/limiter/persistence internals; audit table schema; manager restart semantics; demo composition behavior beyond relocation.