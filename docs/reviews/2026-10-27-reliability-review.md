# Reliability Review: Risks and Recommended Fixes

**Review date:** 2026-10-27  
**Reviewed base:** `main` at `3406bf31`  
**Scope:** Source-level review of adapter lifecycle, event dispatch, retries, queues, SQLite transaction ownership, worker recovery, and CI workflow.

> This is a review report, not a claim that every issue has been reproduced. The findings below should be validated with focused regression tests before implementation or merge. This branch adds documentation only; it does not change runtime behavior.

## Summary

The framework has a useful separation of concerns and a green CI run on the reviewed base. Before implementing real task handlers, address the following risks:

1. Discord adapter startup may wait indefinitely if its connection task fails before readiness.
2. A cached reaction produces both raw and rich normalized events, creating a duplicate-processing hazard.
3. Retrying mutating operations after ambiguous transport failures can repeat side effects.
4. Bounded subscriber queues drop old events, which may silently discard actionable triggers.
5. The SQLite transaction `ContextVar` can be inherited by child tasks and may let them bypass the transaction lock.
6. A worker that dies unexpectedly may remain registered and not restart when the account is enabled again.
7. CI and repository workflow do not yet consistently enforce review and supported-Python coverage.

## Finding 1 — Adapter startup may hang after an early connection failure

**Area:** `autoclaimer/discord_adapter.py` — startup/connect lifecycle  
**Severity:** High; confirm with a regression test.

The adapter starts a background connection task and waits for readiness. If the connection task exits with an exception before readiness is signaled, the readiness waiter may remain blocked unless the failure path explicitly wakes it or the startup code races both conditions.

### Recommended fix

- Wait concurrently for the ready event and connection-task completion.
- If the connection task finishes before readiness, surface its exception as a typed adapter error.
- Apply a bounded startup timeout and cancel/clean up the connection task on timeout.
- Ensure repeated `start()`/ `close()` calls remain idempotent.

### Required tests

- `connect()` fails before ready: `start()` fails promptly and preserves the cause.
- Readiness wins the race: startup succeeds.
- Startup timeout: no background task or client is leaked.
- Close during startup: startup exits promptly and cleanup happens once.

## Finding 2 — Raw and rich reaction events can duplicate logical work

**Area:** `autoclaimer/adapters.py`, `autoclaimer/discord_adapter.py`  
**Severity:** High; contract/design issue.

The adapter emits a raw reaction event and then a rich reaction event for a cached reaction. Both are broadcast to subscribers. The behavior is documented and tested, but a consumer subscribing broadly or registering a handler for both types can process the same reaction twice.

### Recommended fix

Choose and document one canonical task-trigger event for reactions. If both raw and rich forms are needed, make the distinction hard to misuse—for example, a single normalized event with optional cached message context, or explicit typed subscriptions that only deliver one chosen representation. If dual dispatch is retained, expose a stable event identity and provide deliberate deduplication rather than relying on every handler to remember the ordering.

### Required tests

- A cached reaction results in exactly one canonical task-trigger delivery.
- An uncached reaction still produces a usable event.
- Raw/rich ordering is documented and cannot accidentally invoke the same handler twice.
- Multiple subscribers receive the documented contract consistently.

## Finding 3 — Automatic retry of a mutating request can duplicate side effects

**Area:** `autoclaimer/ratelimit.py`, `autoclaimer/discord_adapter.py`  
**Severity:** High; correctness risk.

The limiter retries recoverable adapter errors. For a mutating operation, a timeout, connection loss, or some server errors can occur after the server has accepted the action but before the client receives confirmation. In that situation, the result is unknown—not proof that the action failed. Retrying a send or component interaction may repeat the side effect.

### Recommended fix

- Distinguish known-not-applied failures from unknown-outcome failures.
- Do not automatically retry unknown-outcome mutations unless the operation is idempotent or reconciliation can establish that the first attempt did not succeed.
- Persist an attempt identifier and operation state where useful: pending, confirmed, known-failed, or outcome-unknown.
- Provide per-operation retry policy rather than assuming all recoverable errors are safe to retry.
- Keep server-directed rate-limit cooldown handling separate from ambiguous transport failures.

### Required tests

- A fake adapter simulates “server accepted, response lost”; the framework does not blindly issue the mutation a second time.
- Known-safe transient failures still retry within the configured limit.
- Server-directed cooldown is recorded and honored.
- Uncertain outcomes survive shutdown/restart without being silently marked as confirmed failure.

## Finding 4 — Subscriber queue overflow drops old events

**Area:** event dispatcher in `autoclaimer/adapters.py`  
**Severity:** Medium to high depending on event type.

A bounded queue protects memory and prevents one slow subscriber from blocking others, but dropping the oldest event can lose a message/reaction that is a task trigger. A counter alone does not define whether the application can safely continue after losing such an event.

### Recommended fix

- Define overflow behavior per event category.
- For replaceable state notifications, coalescing or drop-oldest may be acceptable.
- For actionable triggers, prefer explicit overflow reporting and a recovery/reconciliation strategy; do not silently treat a dropped event as processed.
- Expose queue depth, dropped-event count, and subscriber identity through status/metrics.
- Ensure a subscriber exception or cancellation does not block delivery to other subscribers.

### Required tests

- Queue overflow follows the policy for each event category.
- Overflow is observable through status/logging.
- Slow or failing subscribers do not stall unrelated subscribers.
- Shutdown closes subscriptions and releases waiting consumers.

## Finding 5 — Transaction context can be inherited by child asyncio tasks

**Area:** `autoclaimer/persistence.py` — `TaskStateStore` transaction handling  
**Severity:** Medium; concurrency hazard.

The store uses a `ContextVar` to identify calls made inside a transaction. Python propagates context variables into child tasks created within that context. A child task may therefore inherit the “inside transaction” flag and take a code path that bypasses the store lock even though it is not the task that owns the transaction.

This review did not establish that the current code creates child tasks inside transactions; the concern is the safety of the transaction API under future or external use.

### Recommended fix

- Track the owning asyncio task and only allow that task to use the connection under the transaction lock; or redesign the API so transaction-scoped operations are explicit and cannot escape to child tasks.
- Reject nested/concurrent use from a non-owner task with a clear exception.
- Keep transaction begin/commit/rollback in a single well-defined ownership boundary.

### Required tests

- A child task created inside a transaction cannot bypass the lock.
- Concurrent store operations serialize correctly.
- Exceptions roll back and release the lock.
- Cancellation during a transaction does not leave the connection in a transaction or deadlock later operations.

## Finding 6 — Unexpectedly dead account worker may not restart

**Area:** `autoclaimer/workers.py` — worker registry and account enablement  
**Severity:** Medium.

The manager logs an unexpected worker exception. However, worker objects can remain registered after their task has completed. If the account-enable path checks only whether an account exists in the worker registry rather than whether its task is still alive, re-enabling the account may not spawn a replacement.

### Recommended fix

- Make liveness part of the spawn decision: replace missing, cancelled, or completed worker tasks.
- Define whether a worker should restart automatically after an unexpected failure or transition the account into an explicit failed state requiring operator action.
- Apply bounded backoff to prevent crash loops and retain clear failure telemetry.
- Keep intentional shutdown cancellation distinct from unexpected termination.

### Required tests

- Worker crashes unexpectedly; the manager either restarts it or exposes a deliberate failed state.
- Re-enabling an account whose worker task is done has the documented effect.
- Shutdown cancellation does not trigger an unwanted restart.
- Repeated failures do not create duplicate workers or unobserved task exceptions.

## Finding 7 — CI and review process need stronger enforcement

**Area:** `.github/workflows/ci.yml`, repository branch settings, README  
**Severity:** Medium; process/release risk.

At the time of review, the latest CI run on `3406bf31` succeeded. The workflow checks Ruff lint/format, strict mypy, and pytest, but the configured interpreter is Python 3.14 while the project metadata declares Python 3.11 and later. The default branch was also reported as unprotected, allowing changes to land without a required PR review.

### Recommended fix

- Add a CI matrix for the Python versions the project explicitly supports, or narrow `requires-python` to the versions actually tested.
- Keep lockfile verification and locked dependency installation.
- Protect `main` with required CI checks and PR review where repository settings permit.
- Use feature branches and PRs for reliability changes; do not merge until regression tests and review are complete.
- Update README phase/status notes so the documented project state matches the current code.

### Required checks

- CI passes on every supported Python version.
- `uv sync --locked --all-extras`, Ruff, strict mypy, and pytest pass.
- Build/install smoke test remains green.
- PR review and required checks are configured on the default branch.

## Suggested implementation order

1. **Startup lifecycle** — prevent hangs and leaks.
2. **Reaction event contract** — prevent duplicate task execution.
3. **Unknown-outcome retry policy** — prevent duplicate mutations.
4. **Transaction ownership** — prevent cross-task connection access.
5. **Worker recovery policy** — avoid silently dead workers.
6. **Queue overflow policy and observability** — avoid silent loss of actionable events.
7. **CI matrix, branch protection, and documentation cleanup.**

## Acceptance criteria before task handlers

- Each finding has a focused regression test.
- Event delivery semantics are unambiguous and documented.
- Mutating actions are not retried blindly when their outcome is unknown.
- Startup, cancellation, shutdown, and restart leave no leaked tasks.
- Store transaction ownership is enforced.
- Worker death is observable and recovery behavior is deterministic.
- All supported-Python CI jobs pass.
- A reviewer checks the implementation diff and the tests before merge.

## Scope and limitations

This review report documents risks identified from source inspection. It does not implement fixes, claim that every risk has been reproduced, or certify the project as production-ready. Each finding should be confirmed against the current implementation and addressed with regression tests before the related change is merged.
