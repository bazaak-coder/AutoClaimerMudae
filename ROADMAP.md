# AutoClaimer — Development Backlog

**Purpose:** Track reliability, security, maintainability, and future capabilities for AutoClaimer.

**Guiding principle:** Every runtime feature must explicitly define its **failure behavior**, **cancellation behavior**, **persistence requirements**, and **recovery behavior** before implementation.

> Also codified in `agents.md` §17.1 as a mandatory development standard.

**Backlog status:** Phase 1 (items 1–4) is implemented in PR #8 (open, CI-green, audited through seven review rounds) and pending merge. Items 5–20 are implemented on the stacked PRs #9–#11 (open, CI-green). Items 21–25, 26, and 27 are not started; item 26 is planned.

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
- [x] Verify journal-based crash recovery after process restart. — WAL is deliberately NOT used (its shared memory mapping is unreliable in the target environment; see `persistence._connect`), so WAL recovery was NOT tested and is NOT claimed: rollback-journal (hot-journal) recovery is what was verified, with the deployment mode pinned by test (`test_rollback_journal_mode_is_the_deployment_mode`)
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

**Completed:** 2026-10-31 (`tests/test_integrity.py` — 15 scenarios incl. page corruption, CLI surface, and read-only assertions).

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

**Completed:** 2026-10-31 (`tests/test_credential_backup.py` — 23 scenarios). Also fixed during testing: import idempotency now compares the underlying TOKEN, not ciphertext (Fernet never repeats ciphertexts).

**Dependencies:** Credential recovery documentation.

---

## Phase 2 — Worker and Adapter Resilience

**Priority: P1**

### [ ] 5. Fault-Injection Testing

Introduce systematic failure testing across workers, adapters, persistence, and rate limiting.

**Acceptance criteria:**

- [ ] Simulate adapter disconnections.
- [ ] Simulate database read and write failures.
- [ ] Simulate delayed responses and timeouts.
- [ ] Simulate task cancellation at critical execution points.
- [ ] Simulate rate-limit exceptions and repeated cooldowns.
- [ ] Verify worker recovery and shutdown behavior.
- [ ] Verify no leaked asynchronous tasks.
- [ ] Verify failures are observable through structured logs.

**Dependencies:** Existing integration test infrastructure.

### [ ] 6. Account Health Monitoring

Introduce an account health model that distinguishes connection state from account usability.

**Suggested states:** `HEALTHY`, `CONNECTING`, `DISCONNECTED`, `DEGRADED`, `AUTH_FAILED`, `RATE_LIMITED`, `DISABLED`.

**Acceptance criteria:**

- [ ] Track account connection state independently.
- [ ] Record the last successful interaction.
- [ ] Track consecutive failures.
- [ ] Detect authentication failures.
- [ ] Distinguish temporary disconnections from permanent failures.
- [ ] Expose account health through CLI status commands.
- [ ] Avoid logging tokens or other credentials.
- [ ] Define how health changes affect task scheduling.

**Dependencies:** Discord adapter and account runtime.

### [ ] 7. Task Execution Timeouts

Prevent individual tasks from remaining active indefinitely.

**Acceptance criteria:**

- [ ] Support configurable task timeouts.
- [ ] Ensure timeout cancellation propagates correctly.
- [ ] Persist timeout outcomes.
- [ ] Distinguish timeout failures from ordinary exceptions.
- [ ] Ensure cleanup executes after cancellation.
- [ ] Prevent a timed-out task from continuing in the background.
- [ ] Test timeout behavior during adapter operations.

**Dependencies:** Worker lifecycle and task persistence.

### [ ] 8. Dead-Letter Handling

Introduce a controlled mechanism for tasks that repeatedly fail.

**Acceptance criteria:**

- [ ] Define configurable maximum retry attempts.
- [ ] Persist the final failure reason.
- [ ] Keep exhausted tasks inspectable.
- [ ] Prevent automatic infinite retry loops.
- [ ] Support explicit manual retry.
- [ ] Preserve task execution history.
- [ ] Ensure dead-letter transitions survive restart.

**Dependencies:** Retry scheduler and persistent task state.

### [ ] 9. Rate-Limit Stress Testing

Validate rate-limiter behavior under concurrent account and task workloads.

**Acceptance criteria:**

- [ ] Test concurrent requests across multiple accounts.
- [ ] Test sequential tasks within an account.
- [ ] Verify account-specific policies.
- [ ] Verify task-specific policies.
- [ ] Verify global limits.
- [ ] Verify cooldown handling.
- [ ] Test repeated rate-limit responses.
- [ ] Verify cancellation does not bypass limiter cleanup.
- [ ] Confirm that retry scheduling does not create request bursts.

**Dependencies:** Existing rate limiter and worker system.

---

## Phase 3 — Runtime Operations and Maintainability

**Priority: P2**

### [ ] 10. Graceful Shutdown and Restart Hardening

Standardize shutdown and recovery behavior across different termination scenarios.

**Acceptance criteria:**

- [ ] Test SIGINT.
- [ ] Test SIGTERM.
- [ ] Test graceful shutdown during active tasks.
- [ ] Test forced process termination separately.
- [ ] Verify adapters close correctly.
- [ ] Verify worker tasks do not leak.
- [ ] Verify eligible tasks resume after restart.
- [ ] Document which termination scenarios cannot guarantee graceful cleanup.

**Dependencies:** Runtime lifecycle and persistence.

### [ ] 11. Structured Audit Events

Introduce a consistent event trail for important runtime operations.

**Suggested events:** Account connected/disconnected; task assigned/started/completed; task failed/retried; account enabled/disabled; runtime paused/resumed; credential updated; configuration changed.

**Acceptance criteria:**

- [ ] Use structured event fields.
- [ ] Include timestamps and relevant account/task identifiers.
- [ ] Never include credentials or authentication tokens.
- [ ] Separate audit events from debug logging.
- [ ] Ensure important events remain useful after restart.

**Dependencies:** Logging and persistence architecture.

### [ ] 12. Configuration Migration Framework

Prepare for future configuration schema changes without breaking existing installations.

**Acceptance criteria:**

- [ ] Introduce an explicit configuration schema version.
- [ ] Detect outdated configuration versions.
- [ ] Provide sequential migrations.
- [ ] Validate migrated configuration.
- [ ] Preserve existing user settings.
- [ ] Back up configuration before migration.
- [ ] Test migration from supported previous versions.
- [ ] Fail safely when a migration cannot complete.

**Dependencies:** Configuration models and validation.

### [ ] 13. Safe Runtime Configuration Reload

Allow selected settings to change without restarting the entire application.

**Acceptance criteria:**

- [ ] Identify reloadable and restart-required settings.
- [ ] Validate new configuration before applying it.
- [ ] Apply changes atomically where possible.
- [ ] Avoid partially applied configuration.
- [ ] Ensure active tasks use a well-defined configuration snapshot.
- [ ] Record configuration changes in audit events.

**Dependencies:** Configuration migration framework and runtime controls.

### [ ] 14. SQLite Maintenance

Introduce database maintenance procedures for long-running installations.

**Acceptance criteria:**

- [ ] Document WAL checkpoint behavior.
- [ ] Monitor database file growth.
- [ ] Provide safe backup procedures.
- [ ] Avoid maintenance during conflicting active transactions.
- [ ] Test maintenance while the runtime is idle.
- [ ] Document restoration procedures.

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
