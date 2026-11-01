# AutoClaimer — Development Backlog

**Purpose:** Track reliability, security, maintainability, and future capabilities for AutoClaimer.

**Guiding principle:** Every runtime feature must explicitly define its **failure behavior**, **cancellation behavior**, **persistence requirements**, and **recovery behavior** before implementation.

> Also codified in `agents.md` §17.1 as a mandatory development standard.

**Backlog status:** Proposed — not yet implemented or merged.

## Priority Definitions

| Priority | Meaning |
|---|---|
| **P0 — Critical** | Security, data integrity, or runtime reliability risks. |
| **P1 — High** | Important safeguards needed before relying on long-running automation. |
| **P2 — Medium** | Operational improvements and maintainability. |
| **P3 — Future** | Optional features and architectural extensions. |

---

## Phase 1 — Persistence and Recovery Reliability

**Priority: P0**

### [x] 1. SQLite Crash-Recovery Audit

Investigate database behavior under abrupt process termination, rather than relying exclusively on graceful shutdown tests.

**Acceptance criteria:**

- [x] Test termination during an active transaction. — `test_kill_during_active_transaction_rolls_back` (hot-journal rollback, store usable)
- [x] Test termination during state updates. — uncommitted rolled back / committed durable (`test_kill_after_committed_update_is_durable`)
- [x] Test termination during task claiming and completion. — `test_kill_while_claimed_recovers_without_stranding`, `test_kill_during_completion_rolls_back_and_reschedules` (at-least-once)
- [x] Verify SQLite WAL recovery after process restart. — the store deliberately runs the rollback journal (WAL's shared memory is unreliable in the target environment); hot-journal recovery verified instead and the mode pinned by test (`test_rollback_journal_mode_is_the_deployment_mode`)
- [x] Verify no permanently stranded task states. — `test_recovery_leaves_terminal_states_untouched` (status matrix)
- [x] Verify eligible tasks are recovered without duplicate execution. — `test_recovered_task_cannot_be_double_claimed` (atomic claim CAS; terminal rows never re-claimable)
- [x] Document expected behavior for each interruption point. — `docs/running.md` → "Crash recovery by interruption point"

**Completed:** 2026-10-31 (`tests/test_crash_recovery.py` — real child-process SIGKILL at each interruption point).

**Dependencies:** Existing SQLite persistence and runtime recovery implementation.

### [x] 2. Database Integrity Checks

Add safeguards for detecting database corruption and inconsistent persisted state.

**Acceptance criteria:**

- [x] Run `PRAGMA integrity_check` through a diagnostic operation. — `integrity` CLI command → `TaskStateStore.integrity_report()`; heavy corruption that fails the scan itself is contained as a finding, never a crash
- [x] Detect orphaned task and account references. — rows for accounts absent from accounts.json (warnings with hints; opt-in via the caller's account set)
- [x] Detect invalid or unsupported persisted states. — corrupt outcome JSON, malformed timestamps, invalid cooldown flags/state rows
- [x] Provide actionable diagnostic output. — severity + table + key + problem + a concrete hint per finding
- [x] Never automatically delete potentially recoverable user data. — the diagnostic is strictly read-only (asserted by tests)
- [x] Document a recovery procedure. — `docs/running.md` → "Database diagnostics and recovery"

**Completed:** 2026-10-31 (`tests/test_integrity.py` — 6 scenarios incl. page corruption, CLI surface, and read-only assertions).

**Dependencies:** SQLite persistence layer.

### [x] 3. Credential Recovery Documentation

Document what happens when the encryption key is lost, replaced, or becomes inaccessible.

**Acceptance criteria:**

- [x] Explain the consequences of losing the encryption key. — `docs/credentials.md` → "Consequences of losing the key"
- [x] Document how to restore access to credentials. — replace-key procedure AND backup-restore path, step by step
- [x] Explain which data can and cannot be recovered. — key-dependency table (tokens vs tasks/cooldowns/state) + explicit never-recoverable case
- [x] Ensure recovery instructions never expose plaintext credentials. — every artifact in the procedures is ciphertext, a key, or metadata
- [x] Document secure key storage and backup recommendations. — storage rules, separation/equivalent-secrecy rule, two independent copies, tested restore

**Completed:** 2026-10-31 (documentation; the procedures reference the item-4 export/import commands).

**Dependencies:** Credential store and configuration documentation.

### [x] 4. Credential Backup and Restore

Establish a safe procedure for backing up and restoring credential-related data.

**Acceptance criteria:**

- [x] Define which credential files and database records require backup. — `docs/credentials.md` (the `state.db` `account_credentials` rows and/or the export bundle, always paired with the key)
- [x] Never export plaintext credentials by default. — export is ciphertext-only by construction (byte-level test assertion); there is no plaintext export path at all
- [x] Require explicit confirmation for sensitive export operations. — literal `confirm` argument enforced for both `credential export` and `credential import` (tested)
- [x] Validate restored encrypted data before accepting it. — phase-1 decrypt validation of every record; a non-matching or malformed bundle is rejected whole with zero rows written (tested)
- [x] Test restoration using a separate temporary environment. — round-trip test exports from one database and restores into a fresh store/database; plus a re-encryption-under-new-primary test proving the old key can be discarded afterwards
- [x] Document key and backup compatibility requirements. — bundle usable only with the export-time primary or any still-configured retired key; restored rows re-encrypted under the current primary

**Completed:** 2026-10-31 (`tests/test_credential_backup.py` — 6 scenarios). Also fixed during testing: import idempotency now compares the underlying TOKEN, not ciphertext (Fernet never repeats ciphertexts).

**Dependencies:** Credential recovery documentation.

---

## Phase 2 — Worker and Adapter Resilience

**Priority: P1**

### [x] 5. Fault-Injection Testing

Introduce systematic failure testing across workers, adapters, persistence, and rate limiting.

**Acceptance criteria:**

- [x] Simulate adapter disconnections. — disconnect storm (heals inside the transport layer within one worker attempt) and never-healing disconnects (transport bound exhausts → terminal with the exhaustion reason persisted)
- [x] Simulate database read and write failures. — `DatabaseBusyError` injected on the scheduling read (worker backs off, survives, completes) and on outcome recording (row stays honestly `running`; restart recovers and re-executes)
- [x] Simulate delayed responses and timeouts. — a bounded handler against a stalled adapter: recoverable classification, bounded retries, terminal with exhaustion reason — never a hang
- [x] Simulate task cancellation at critical execution points. — graceful stop while a handler is mid-execution: no leaked asyncio tasks, row recoverable, restart completes it
- [x] Simulate rate-limit exceptions and repeated cooldowns. — repeated server-directed `CooldownError`s waited out via the limiter (fake-clock verified: two ≥30 s deferrals, exactly one real send, no burst)
- [x] Verify worker recovery and shutdown behavior. — every scenario asserts subsequent work still completes and `stop()` drains within budget
- [x] Verify no leaked asynchronous tasks. — `asyncio.all_tasks()` baseline comparison after every scenario (drain-tolerant)
- [x] Verify failures are observable through structured logs. — classified "task handler crashed" record with `exc_info` carrying the injected reason + structured operation/outcome extras

**Completed:** 2026-11-01 (`tests/test_fault_injection.py` — 9 scenarios). Note: `RecordingSleep` doubles as a fake clock (sleep advances wall/mono) — cooldown waits loop until the wall deadline, so an instant sleep alone would busy-spin in real time.

**Dependencies:** Existing integration test infrastructure.

### [x] 6. Account Health Monitoring

Introduce an account health model that distinguishes connection state from account usability.

**Suggested states:** `HEALTHY`, `CONNECTING`, `DISCONNECTED`, `DEGRADED`, `AUTH_FAILED`, `RATE_LIMITED`, `DISABLED` (+ `UNKNOWN` for a fresh session).

**Acceptance criteria:**

- [x] Track account connection state independently. — per-account in-memory monitor (`autoclaimer/health.py`), one snapshot view per account
- [x] Record the last successful interaction. — `last_success_at` (+ totals) per account
- [x] Track consecutive failures. — streak counter with `DEGRADED` at the threshold (3); reset on success
- [x] Detect authentication failures. — account-fatal events latch `AUTH_FAILED` (only the explicit revive path clears it)
- [x] Distinguish temporary disconnections from permanent failures. — `DISCONNECTED` (transient) vs `AUTH_FAILED` (latched) vs `DEGRADED` (streak)
- [x] Expose account health through CLI status commands. — new `health` command: state, totals, streak, last success, last error
- [x] Avoid logging tokens or other credentials. — error text passes the redaction filter and is length-bounded (tested)
- [x] Define how health changes affect task scheduling. — documented + asserted: `AUTH_FAILED`/`DISABLED` never schedule (existing fatal-stop/enabled enforcement); `RATE_LIMITED`/`DEGRADED`/`DISCONNECTED` are informational — deferral stays with the limiter and retry policy. Observation layer, not an enforcement layer.

**Completed:** 2026-11-01 (`autoclaimer/health.py`, worker/adapter wiring, CLI `health`; `tests/test_account_health.py` — 11 scenarios incl. worker-level end-to-end and CLI exposure).

**Dependencies:** Discord adapter and account runtime.

### [x] 7. Task Execution Timeouts

Prevent individual tasks from remaining active indefinitely.

**Acceptance criteria:**

- [x] Support configurable task timeouts. — `settings.task_timeout_seconds` (default) + per-task `timeout_seconds` override; precedence task → settings; `null`/absent disables (backward compatible); validated (finite, (0, 86400], booleans rejected)
- [x] Ensure timeout cancellation propagates correctly. — `asyncio.wait` + explicit cancel-and-await: the handler's cancellation cleanup provably runs
- [x] Persist timeout outcomes. — FAILED_RECOVERABLE with a distinct message via the normal outcome recording; terminal rows now carry the last failure reason ("…; last failure: task execution timed after Xs")
- [x] Distinguish timeout failures from ordinary exceptions. — a still-pending task after the deadline is a timeout; a handler *raising* `TimeoutError` keeps its exception identity (handler-crash path, different message) — tested both ways
- [x] Ensure cleanup executes after cancellation. — cleanup-tracking handler: the `except CancelledError` block provably runs
- [x] Prevent a timed-out task from continuing in the background. — the child task is cancelled AND awaited; stray-task assertion in tests
- [x] Test timeout behavior during adapter operations. — timeout fires mid `send_message` on a stalled adapter: op cancelled (nothing sent), outcome recorded

Also fixed en passant: the retry-exhaustion terminal message now includes the last failure reason (observability for every recoverable failure — groundwork for item 8), and shutdown during the timeout window leaks no handler tasks (worker-cancel propagation).

**Completed:** 2026-11-01 (`tests/test_task_timeouts.py` — 12 scenarios incl. config matrix, precedence, cancel/cleanup, adapter-op timeout, shutdown-in-window).

**Dependencies:** Worker lifecycle and task persistence.

### [x] 8. Dead-Letter Handling

Introduce a controlled mechanism for tasks that repeatedly fail.

**Acceptance criteria:**

- [x] Define configurable maximum retry attempts. — existing `task_defaults` → task → override chain (pinned again here: retry_limit 1 → exactly 2 attempts)
- [x] Persist the final failure reason. — terminal rows carry `…; last failure: <reason>` (item-7 fix, asserted)
- [x] Keep exhausted tasks inspectable. — `tasks failed` listing + new `history <account> <task>` command (attempt, status, message, timestamp per row)
- [x] Prevent automatic infinite retry loops. — worker retry bound (asserted: attempt_count never exceeds limit+1); restart does NOT resurrect dead-lettered rows
- [x] Support explicit manual retry. — `retry <account> <task>` CLI → `requeue_instance`: failed → pending with a fresh attempt budget, wake the worker, requeue event in history; non-failed rows are rejected with a typed error
- [x] Preserve task execution history. — schema v6 `task_history`: one bounded row per recorded outcome (success/waiting/failed/requeued), retention 50 per instance, newest-first query
- [x] Ensure dead-letter transitions survive restart. — tested across two restarts (dead-letter persists; requeue persists; row pending and due)

Auxiliary contract (tested): a history-write failure is logged and swallowed — the `task_instances` outcome write is the source of truth and never breaks.

**Completed:** 2026-11-01 (schema v6 migration + shape verification; `tests/test_dead_letter.py` — 7 scenarios).

**Dependencies:** Retry scheduler and persistent task state.

### [x] 9. Rate-Limit Stress Testing

Validate rate-limiter behavior under concurrent account and task workloads.

**Acceptance criteria:**

- [x] Test concurrent requests across multiple accounts. — 6 accounts × 2 tasks × 3 sends under one global min-interval
- [x] Test sequential tasks within an account. — per-account overlap violations asserted zero under concurrent load
- [x] Verify account-specific policies. — strict account paced at its interval while the other stayed fast (measured real gaps)
- [x] Verify task-specific policies. — task-level interval reached operations end to end through the worker's policy binding
- [x] Verify global limits. — `max_concurrent_accounts` peak never exceeded 3 slots under 12 concurrent task executions
- [x] Verify cooldown handling. — repeated server-directed 429s waited out via the limiter (fake-clock verified)
- [x] Test repeated rate-limit responses. — two consecutive cooldown scripts then success: exactly one real send
- [x] Verify cancellation does not bypass limiter cleanup. — mid-flight cancellations left every permit at `in_use == 0` and a probe operation completed immediately
- [x] Confirm that retry scheduling does not create request bursts. — cooldown waits ≥ the recorded window; no sub-interval send pairs on any account

Measurement discipline: pacing tests use the REAL clock with generous floors (no exact-equality timing); cooldown storms use the fake clock (instant, clock-advancing) — mixing them silently destroys pacing measurement (found while building the suite).

**Completed:** 2026-11-01 (`tests/test_rate_limit_stress.py` — 5 scenarios).

**Dependencies:** Existing rate limiter and worker system.

---

## Phase 3 — Runtime Operations and Maintainability

**Priority: P2**

### [x] 10. Graceful Shutdown and Restart Hardening

Standardize shutdown and recovery behavior across different termination scenarios.

**Acceptance criteria:**

- [x] Test SIGINT. — real child process, full composition, termination mid-task
- [x] Test SIGTERM. — same; previously SIGTERM was completely unhandled (instant death, no cleanup) — now wired to the same graceful path as Ctrl-C (`install_termination_handlers` + `__main__` wiring)
- [x] Test graceful shutdown during active tasks. — blocked handler mid-execution: exit 0, GRACEFUL marker, row honestly `running`
- [x] Test forced process termination separately. — SIGKILL: no marker, non-zero exit, database consistent, restart recovers
- [x] Verify adapters close correctly. — in-process assertion: every adapter `closed` after shutdown
- [x] Verify worker tasks do not leak. — in-process `asyncio.all_tasks()` baseline; process-level: a leaked-task child would hang the exit timeout
- [x] Verify eligible tasks resume after restart. — restart with a working handler completes the interrupted row (both after graceful AND forced termination)
- [x] Document which termination scenarios cannot guarantee graceful cleanup. — `docs/running.md` shutdown matrix: SIGKILL guarantees nothing; a blocked interactive stdin read may delay signal-triggered exit; second signal = documented force-exit escape hatch (tested)

**Completed:** 2026-11-01 (`tests/test_signal_shutdown.py` — 5 scenarios; `install_termination_handlers` in cli.py; `__main__` signal wiring).

**Dependencies:** Runtime lifecycle and persistence.

### [x] 11. Structured Audit Events

Introduce a consistent event trail for important runtime operations.

**Suggested events:** Account connected/disconnected; task assigned/started/completed; task failed/retried; account enabled/disabled; runtime paused/resumed; credential updated; configuration changed.

**Acceptance criteria:**

- [x] Use structured event fields. — schema v7 `audit_events`: timestamp, category, action, account_id, task_id, detail (typed columns, not prose)
- [x] Include timestamps and relevant account/task identifiers. — asserted per event
- [x] Never include credentials or authentication tokens. — detail redacted at the emit boundary (tested with a token-shaped adversarial detail); credential events carry ids/actions only
- [x] Separate audit events from debug logging. — dedicated `autoclaimer.audit` logger + persisted table, queryable via `audit [limit]` CLI — not buried in debug output
- [x] Ensure important events remain useful after restart. — events persist across restarts (tested); bounded retention (latest 1000)

Events wired: task started/completed/failed/retried/requeued, account paused/resumed/enabled/disabled, runtime paused/resumed, credential set/deleted/imported/exported/rotated. `configuration.changed` is reserved for the runtime-reload feature (item 13) — nothing changes configuration at runtime today. Emission is auxiliary (tested): an audit failure never breaks the audited operation.

**Completed:** 2026-11-01 (`autoclaimer/audit.py`, schema v7, worker/controller emit sites, CLI `audit`; `tests/test_audit_events.py` — 8 scenarios).

**Dependencies:** Logging and persistence architecture.

### [x] 12. Configuration Migration Framework

Prepare for future configuration schema changes without breaking existing installations.

**Acceptance criteria:**

- [x] Introduce an explicit configuration schema version. — `CURRENT_CONFIG_VERSION` in config.py; the parser names it in errors; `accounts.json` follows the application version (no independent migrations — documented)
- [x] Detect outdated configuration versions. — `load_config` checks before parsing
- [x] Provide sequential migrations. — `register_config_migration(from_version, migrator)` + pure `migrate_config_document` chain (version bumped by the framework, not migrators)
- [x] Validate migrated configuration. — the migrated document goes through the FULL parser before anything is written
- [x] Preserve existing user settings. — asserted (migrators copy and extend, never drop)
- [x] Back up configuration before migration. — `<name>.vN.bak` beside the original; existing backups are never overwritten (tested)
- [x] Test migration from supported previous versions. — the production chain is empty until v2 exists; the machinery is fully exercised through the PUBLIC registration API with a schema-compatible sample migration (the documented pattern for the first real schema change), incl. a two-step chain
- [x] Fail safely when a migration cannot complete. — raising migrator OR invalid migrated document leaves the original file byte-identical (no backup, no .tmp); missing-chain and future-version errors are actionable

Order of operations is fail-safe by construction: migrate purely → validate → backup → atomic tmp+rename.

**Completed:** 2026-11-01 (`tests/test_config_migration.py` — 10 scenarios).

**Dependencies:** Configuration models and validation.

### [x] 13. Safe Runtime Configuration Reload

Allow selected settings to change without restarting the entire application.

**Acceptance criteria:**

- [x] Identify reloadable and restart-required settings. — reloadable: task definitions (enabled/parameters/retry/timeout), global channels, ratelimit layers, logging (handlers reconfigured idempotently), account assignments, shutdown timeout; restart-required (reported, never partially applied): `max_concurrent_accounts` (execution slots built at start), `send_jitter_seconds` (cached adapters)
- [x] Validate new configuration before applying it. — full `load_config` validation first; an invalid reload is rejected and the running configuration keeps serving (tested)
- [x] Apply changes atomically where possible. — single synchronous no-yield swap of (app, accounts) on the manager — worker iterations see the old pair or the new one, never a mix
- [x] Avoid partially applied configuration. — restart-required settings are reported while the running values are kept; nothing is half-applied
- [x] Ensure active tasks use a well-defined configuration snapshot. — each execution resolves its configuration at claim time; a reload mid-execution does not change the running task's channel (tested)
- [x] Record configuration changes in audit events. — `configuration.changed` (the event reserved in item 11, now wired)

Surface: CLI `reload [config accounts]` (defaults config.json/accounts.json) — validates, applies atomically, reports restart-required items, nudges workers, materializes new assignments.

**Completed:** 2026-11-01 (`WorkerManager.update_configuration`/`reload_materialize`, controller `reload_config`, CLI `reload`; `tests/test_config_reload.py` — 5 scenarios).

**Dependencies:** Configuration migration framework and runtime controls.

### [x] 14. SQLite Maintenance

Introduce database maintenance procedures for long-running installations.

**Acceptance criteria:**

- [x] Document WAL checkpoint behavior. — the deployment deliberately runs the ROLLBACK journal, not WAL (shared-memory mapping unreliable in the target environment — `persistence._connect`); there are no WAL checkpoints, the journal is auto-removed after each commit, and `dbstats` surfaces the live journal mode so a silent switch cannot go unnoticed
- [x] Monitor database file growth. — `dbstats` CLI + `database_stats()`: file size, page/free-page counts, per-table row counts; growth is structurally bounded by audit/history retention (verified end to end)
- [x] Provide safe backup procedures. — `backup <path>` CLI + `backup_to()`: consistent snapshot via `VACUUM INTO` on the guarded connection (compacted, self-contained); refuses to overwrite existing targets
- [x] Avoid maintenance during conflicting active transactions. — the backup serializes through the connection guard: a concurrent write completes first or waits within the busy timeout — never a torn copy (tested against an open write transaction on a second connection)
- [x] Test maintenance while the runtime is idle. — idle backup + restoration verified on a seeded store
- [x] Document restoration procedures. — `docs/running.md` "Database maintenance": stop (or back up live), replace `state.db` with the backup, remember key↔backup pairing for credentials

**Completed:** 2026-11-01 (`tests/test_db_maintenance.py` — 5 scenarios).

**Dependencies:** SQLite persistence.

### [ ] 15. CLI Diagnostics

Provide a diagnostic command for troubleshooting installations (`autoclaimer doctor`).

**Acceptance criteria:**

- [ ] Check configuration validity.
- [ ] Check database accessibility and integrity.
- [ ] Check credential-store availability without displaying secrets.
- [ ] Check registered task definitions.
- [ ] Report account connection status.
- [ ] Report worker and scheduler status.
- [ ] Provide actionable failure explanations.
- [ ] Avoid modifying application state during ordinary diagnostics.

**Dependencies:** Existing CLI, persistence, and runtime components.

---

## Phase 4 — Automated Quality and Security Checks

**Priority: P1/P2**

### [ ] 16. Dependency Security Scanning

Add dependency vulnerability checks to CI.

**Acceptance criteria:**

- [ ] Scan production dependencies.
- [ ] Scan development dependencies.
- [ ] Report vulnerable dependency versions.
- [ ] Define a policy for critical and high-severity findings.
- [ ] Keep dependency lockfiles synchronized.
- [ ] Avoid silently accepting known vulnerabilities.

**Dependencies:** GitHub Actions workflow.

### [ ] 17. Static Analysis Enforcement

Enforce consistent code-quality checks in CI.

**Acceptance criteria:**

- [ ] Run Ruff.
- [ ] Run mypy.
- [ ] Fail CI when enforced checks fail.
- [ ] Keep configuration centralized.
- [ ] Document intentional exceptions.

**Dependencies:** Existing project configuration.

### [ ] 18. Critical-Path Coverage

Improve test coverage around sensitive runtime components.

**Priority areas:** Credential storage; SQLite persistence; worker lifecycle; rate limiter; retry scheduler; configuration validation; Discord adapter lifecycle.

**Acceptance criteria:**

- [ ] Define meaningful coverage targets for critical modules.
- [ ] Enforce targets in CI.
- [ ] Prefer behavior and failure-path coverage over arbitrary line coverage.
- [ ] Ensure important regressions have dedicated tests.

**Dependencies:** Existing test suite.

### [ ] 19. Property-Based Testing

Use generated inputs to test invariants that are difficult to cover with example-based tests.

**Suggested areas:** Configuration validation; retry scheduling; task state transitions; persistence serialization; account/task assignment.

**Acceptance criteria:**

- [ ] Define invariants before generating test cases.
- [ ] Include malformed and boundary inputs.
- [ ] Ensure failures are reproducible through saved examples or seeds.
- [ ] Keep test execution practical for CI.

**Dependencies:** Test infrastructure.

### [ ] 20. Test Isolation

Ensure tests do not depend on execution order or shared external state.

**Acceptance criteria:**

- [ ] Isolate SQLite databases per test.
- [ ] Isolate temporary configuration and credential files.
- [ ] Avoid real Discord connections in ordinary CI.
- [ ] Ensure asynchronous tasks are cleaned up.
- [ ] Ensure tests can run independently.
- [ ] Verify parallel test execution where supported.

**Dependencies:** Existing test infrastructure.

---

## Phase 5 — Future Capabilities

**Priority: P3**

### [ ] 21. Simulation Mode

Provide a way to test task scheduling without performing real Discord actions.

**Acceptance criteria:**

- [ ] Support mock account execution.
- [ ] Simulate task outcomes.
- [ ] Simulate cooldowns and failures.
- [ ] Display scheduling decisions.
- [ ] Never send real Discord messages in simulation mode.
- [ ] Clearly distinguish simulated from real execution.

### [ ] 22. Execution History Export

Allow users to export task execution history.

**Acceptance criteria:**

- [ ] Support a machine-readable export format.
- [ ] Include timestamps, task outcomes, and failure reasons.
- [ ] Exclude credentials and sensitive authentication data.
- [ ] Support date and account filters.
- [ ] Avoid interrupting active workers.

### [ ] 23. Controlled Task Discovery

Prepare a safe mechanism for discovering and registering task implementations.

**Acceptance criteria:**

- [ ] Define a task registration contract.
- [ ] Validate task metadata.
- [ ] Prevent duplicate registrations.
- [ ] Reject invalid task definitions.
- [ ] Keep discovery separate from execution.
- [ ] Avoid arbitrary code execution through configuration.

### [ ] 24. Task Dependencies

Allow tasks to depend on the completion of other tasks.

**Acceptance criteria:**

- [ ] Define dependency relationships.
- [ ] Detect circular dependencies.
- [ ] Persist dependency state.
- [ ] Prevent execution before prerequisites are satisfied.
- [ ] Define behavior when a prerequisite fails.
- [ ] Ensure dependency scheduling survives restart.

### [ ] 25. Metrics Endpoint

Provide optional runtime metrics for external monitoring.

**Potential metrics:** Active accounts; connected accounts; active workers; queued tasks; successful and failed executions; retry counts; rate-limit events; task execution duration.

**Acceptance criteria:**

- [ ] Metrics contain no credentials or sensitive content.
- [ ] Metrics collection does not block workers.
- [ ] Endpoint exposure is configurable.
- [ ] Document network-access and authentication considerations.

---

## Recommended Implementation Order

| Order | Task | Priority | Reason |
|---|---|---|---|
| 1 | SQLite crash-recovery audit | P0 | Protect persisted runtime state |
| 2 | Database integrity checks | P0 | Detect corrupted or inconsistent state |
| 3 | Credential recovery documentation | P0 | Prevent avoidable credential loss |
| 4 | Fault-injection testing | P1 | Validate failure handling |
| 5 | Account health monitoring | P1 | Improve multi-account reliability |
| 6 | Task execution timeouts | P1 | Prevent stuck tasks |
| 7 | Dead-letter handling | P1 | Prevent endless retry cycles |
| 8 | Rate-limit stress testing | P1 | Validate concurrent execution |
| 9 | Dependency security scanning | P1 | Reduce supply-chain risk |
| 10 | Configuration migration framework | P2 | Protect future upgrades |

---

## Repository Health Rules

These remain ongoing development requirements rather than one-time tasks:

- All changes go through pull requests before entering the main branch.
- Every PR receives a bug, security, and fragile-code review.
- Use `gh` for GitHub CLI workflows and CI operations.
- Keep `/tests` as the central test location.
- Add regression tests for confirmed bugs.
- Never treat a passing CI run as proof that security or correctness is guaranteed.
- Do not store credentials in source code, logs, test fixtures, or ordinary configuration.
- Keep mock-based tests separate from tests requiring real external services.
- Document environmental limitations when a test cannot be executed.
- Avoid introducing abstractions without a concrete use case.

## Definition of Done

A task is not complete merely because its implementation exists. Before marking an item complete:

- Its acceptance criteria must be satisfied.
- Relevant success and failure paths must have tests.
- Cancellation and shutdown behavior must be checked where applicable.
- Persistence and restart behavior must be verified where applicable.
- Security implications must be reviewed.
- Documentation must reflect actual behavior.
- CI must pass.
- The PR must receive an independent review.
- Any remaining limitations must be documented.
