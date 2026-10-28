AGENTS.md — AutoClaimer Development & Agent Operating Standards

1. Purpose and Authority

This document defines the mandatory development, testing, security, documentation, and Git workflow for every AI agent and human contributor working on AutoClaimer.

AutoClaimer is a lightweight, standalone Python project for configurable multi-account Discord automation targeting Mudae. Keep its architecture independent of unrelated projects, with minimal unnecessary dependencies and resource overhead.

Current status (update as phases complete): Phase 1 (typed configuration foundation, credential-provider interface, tooling), Phase 2 (task registry, platform adapter contracts, deterministic mock adapter), Phase 3 (async SQLite runtime persistence), Phase 4 (central rate limiter with scope cooldowns, pacing, permits, and bounded retries), Phase 5 (worker manager with concurrent account workers, deterministic priority scheduling, account controls, and graceful shutdown), Phase 6 (interactive CLI with rotating, redacting, structured logging), and Phase 7 (discord.py-self adapter verified against 2.1.0, with error translation, conservative jitter pacing, and cache-only channel resolution) are implemented and green. The single runtime dependency is discord.py-self (owner-selected); Mudae-specific task handlers are not implemented yet. The README package table is the authoritative module status.

Priority order:

1. The owner's explicit instructions.
2. Repository-specific instructions and established architecture.
3. This document.
4. Individual agent preferences.

Never override an explicit owner restriction for convenience. When requirements conflict or an important decision is unclear, explain the conflict and request clarification.

2. Repository Inspection Before Work

Before implementing a change:

1. Read this document and any applicable nested "AGENTS.md" files.
2. Inspect the repository structure, relevant source files, configuration, tests, documentation, and dependency declarations.
3. Check "git status --short --branch", the current branch, recent commits, and the remote configuration.
4. Identify existing uncommitted changes, untracked files, and other agents' work.
5. Check Python, operating system, installed tools, dependencies, and environment compatibility.
6. Inspect relevant entries in the append-only knowledgebase.
7. Verify the workspace environment (see section 16): the git pointer file, the external git directory, the virtualenv, and which tool invocations actually work on this machine.
8. Understand existing behavior and identify the tests needed to verify the change.

Do not assume functionality is missing merely because it is not immediately apparent.

Never overwrite, discard, reset, or reformat unrelated work. Do not use destructive Git commands without explicit authorization.

If repository access or environmental limitations prevent reliable inspection, report the blocker before proceeding with dependent work.

3. Python Implementation Standards

3.1 Dataclasses and typing

Use "dataclasses" when they improve readability, maintainability, type safety, or accessibility of structured data.

Prefer them for appropriate configuration objects, account/profile models, task definitions, execution results, runtime state, and data-transfer objects.

from dataclasses import dataclass, field


@dataclass(slots=True)
class TaskResult:
    account_id: int
    task_name: str
    success: bool
    error: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

- Use "default_factory" for mutable defaults.
- Use "slots=True" when compatible with supported Python versions.
- Use "frozen=True" for genuinely immutable objects.
- Add explicit type annotations.
- Validate untrusted input at system boundaries.
- Avoid unnecessary models and abstractions.

3.2 General practices

- Follow PEP 8 and modern Python conventions.
- Prefer small, cohesive functions and descriptive names.
- Use "pathlib.Path", context managers, and structured logging.
- Handle exceptions at meaningful boundaries without silently swallowing errors; the full
  classification, ownership, and recovery policy is section 17.
- Preserve useful exception context and diagnostic information.
- Avoid mutable global state, hidden side effects, blocking asynchronous code, and uncontrolled background tasks.
- Handle cancellation, timeouts, retries, concurrency, and resource cleanup deliberately.
- Avoid unnecessary dependencies and premature abstractions.
- Preserve established interfaces unless a change is justified and documented.
- Never silently change configuration formats, persistence structures, or public APIs.

3.3 Architecture

Keep configuration, account management, scheduling, execution, persistence, logging, and user-facing interfaces appropriately separated.

Extend existing functionality when practical instead of duplicating it. Explain significant architectural changes in the implementation plan and document migration requirements when applicable.

4. Append-Only Knowledgebase

Every implementation session that changes project files must add an informative entry to the append-only knowledgebase.

Use the existing knowledgebase if present. Otherwise, establish "knowledgebase/" with a clear index and suitable sections, such as:

In this repository the knowledgebase is gitignored by owner instruction: entries are local workspace documentation and must never be committed, pushed, or rewritten. Keep "knowledgebase/README.md" as the append-only index.

knowledgebase/
├── README.md
├── decisions/
├── changes/
├── debugging/
└── environment/

Use dated, descriptive filenames consistent with repository conventions.

Each implementation entry must document:

1. Summary and motivation.
2. Every added, modified, renamed, or deleted file.
3. Important functions, classes, interfaces, and behavioral changes.
4. Design decisions and alternatives considered.
5. Dependency, tooling, lockfile, and CI changes.
6. Commands executed and their actual test results.
7. Failures, diagnostic output, and likely causes.
8. Relevant environmental constraints.
9. Git branch and commit information, if applicable.
10. Outstanding defects, limitations, and follow-up work.

Append-only rules

- Create new entries rather than rewriting historical records.
- Correct earlier findings with a new entry referencing the original.
- Record failed approaches when they help future debugging.
- Distinguish verified facts from hypotheses.
- Redact credentials, tokens, cookies, and secrets.
- Keep indexes and references consistent with the existing documentation scheme.

A change is not fully documented until its knowledgebase entry is saved. If documentation cannot be completed, explicitly report the limitation.

5. Developer Tools and Dependency Management

Use "uv" where compatible with the project for Python dependency and environment management.

Workspace reality (this machine): the project lives on a normal Linux filesystem (ext4, user-owned) with a project-local virtualenv at ".venv" (see section 16). The system Python is 3.13.5 and externally-managed (PEP 668); do not install into it — always invoke the ".venv/bin" binaries for project work. "uv" is installed inside the project venv (".venv/bin/uv") and is not on PATH in non-interactive shells — invoke it by path. Maintain "uv.lock" with "uv lock" (the old "UV_LINK_MODE=copy" workaround is obsolete on ext4). "uv sync" is strict by default and prunes extraneous packages — including a pip-installed "uv" itself; pass "--inexact" to keep it. Network egress to PyPI is slow (~200 KB/s observed); long installs belong in the background and reuse pip's wheel cache.

Use Ruff, mypy, and pytest as the primary development quality tools.

Recommended development dependencies:

- "ruff" — linting and formatting.
- "mypy" — static type checking.
- "pytest" — automated testing.
- "pytest-asyncio" — asynchronous testing when needed.

Configure tools centrally in "pyproject.toml". Adapt commands to the actual package layout rather than assuming every repository uses the same structure.

Ruff

uv run ruff check .
uv run ruff format --check .

When formatting or fixes are needed, limit changes to the authorized scope and inspect the diff afterward.

Mypy

uv run mypy .

Use meaningful annotations and targeted suppressions. Do not disable type checking globally to conceal defects.

Dependencies and lockfiles

Maintain "pyproject.toml" and "uv.lock" when the project uses uv.

uv lock
uv sync --locked

- Generate lockfiles through uv, not by manually editing their contents.
- Review dependency changes before accepting them.
- Avoid unrelated upgrades.
- Keep runtime and development dependencies appropriately separated.
- Do not commit virtual environments, caches, credentials, or machine-specific configuration.
- Report incompatibilities instead of silently bypassing them.

6. Test Organization: "/tests"

Keep the automated test suite in the repository-root "/tests" directory.

Recommended organization:

tests/
├── conftest.py
├── unit/
├── integration/
├── regression/
└── security/

Adapt this layout to the project's actual needs. Do not create empty directories merely to satisfy a template.

- "unit/": isolated tests for individual functions, classes, and components.
- "integration/": interactions between components, persistence, and external-service adapters, using controlled fixtures or mocks where appropriate.
- "regression/": tests reproducing previously discovered bugs.
- "security/": tests for authorization boundaries, secret handling, unsafe input, concurrency limits, and other relevant security properties.

Use "pytest" discovery conventions and keep test dependencies separate from runtime dependencies.

Test important success cases, error cases, edge cases, boundary conditions, and regressions. For asynchronous components, cover cancellation, exceptions, timeouts, concurrent operations, and cleanup when applicable.

Prefer deterministic tests with no dependency on real Discord accounts, live messages, production databases, or unpredictable network services.

Never weaken assertions or remove tests merely to make CI pass.

A skipped test is not a passed test. An unexecuted test must be reported as unverified.

7. Git and GitHub CLI ("gh")

Use Git for version control and GitHub CLI ("gh") for authorized GitHub operations.

Before changes

git status --short --branch
git branch --show-current
git log -5 --oneline
gh repo view

Verify the repository, branch, and remote before operating. If "gh" is unavailable or unauthenticated, report the limitation and use other permitted local checks where possible.

Workspace git topology (this machine): git metadata lives in "/home/libosterelezent/autoclaimer.git", outside the tree (a deliberate legacy of the FUSE-era setup, kept as the working topology). The project root contains a ".git" *pointer file* ("gitdir: /home/libosterelezent/autoclaimer.git") so plain git commands work in place; git hard-ignores the pointer, and neither the pointer nor the external git directory must ever be committed or deleted. Never delete "/home/libosterelezent/autoclaimer.git" — it is the repository. The external git directory does not travel with workspace copies: if it is lost (observed twice during machine migrations), rebuild it by cloning fresh, moving the clone's ".git" into place, and setting "core.worktree" plus repo-local "core.filemode=false" again — a fresh clone writes "core.filemode=true", which overrides the global "false" and makes every tracked file appear mode-modified. Commit identity is configured globally ("Liboster" / libosterelezent@zohomail.in) and "gh auth setup-git" provides the HTTPS credential helper; if the pointer file is ever broken, run "gh" commands from outside the project directory.

During changes

- Keep changes focused and reviewable.
- Use a feature branch for each logical change where appropriate.
- Avoid unrelated refactoring.
- Inspect diffs before staging and committing.
- Stage specific files instead of indiscriminately staging everything.
- Never include another contributor's unfinished changes or expose secrets.
- Never rewrite shared history or overwrite another agent's work.

Useful checks:

git diff --check
git diff
git status --short

Commit policy

Commit only when authorized by the owner or the established task instructions, and only after the applicable quality gates pass.

Use descriptive commit messages explaining the purpose and important changes.

If mandatory tests or checks fail, do not commit the change as a completed implementation. Preserve the diagnostics and report the blocker.

8. Mandatory Pull Request Workflow

All changes intended for the remote repository must go through a pull request (PR), followed by automated verification and independent review.

Do not push directly to the protected default branch or bypass the PR process.

The owner's existing restriction on remote operations remains in force: no remote push, PR creation, merge, release, or other remote mutation is permitted until explicitly authorized. Once the owner authorizes the remote workflow, follow these phases.

Established practice to date (owner-directed): commits land on "main" directly, granularly per section 7.4, and pushes happen only on an explicit per-batch "push" instruction from the owner. That practice satisfies section 7.5's authorization requirement; the PR workflow below applies when the owner requests it (for example for externally reviewed or multi-contributor changes).

Push classification rule (owner directive, 2026-10-28)

Before any authorized push, the batch must be classified by work type and severity, and the classification recorded (knowledgebase entry and, when a PR is used, its description):

1. Breaking change — removes, renames, or resemantics public API; changes configuration format or persistence schema incompatibly; alters documented behavioral contracts; major dependency bumps; security-boundary rework. Severity: HIGH by default.
2. Feature implementation change — additive features, defect fixes, documentation, and tests that preserve existing contracts. Severity: MEDIUM when default runtime behavior changes (new default delays/pacing, changed retry or recovery behavior), when new configuration defaults apply out of the box, or when authentication/lifecycle/persistence paths are touched; LOW otherwise (pure docs, tests-only, or internal additive changes with no observable default change).

Routing by classification:

- HIGH (breaking) and MEDIUM (feature) batches go through the full PR workflow below (phases A-E, independent review, owner-approved merge).
- LOW (feature) batches may follow the established owner-directed direct push per section 7.4 on explicit instruction.
- When classification is uncertain, classify upward (PR). Mixed batches route by their highest-severity component. Owner instructions always override this rule.

Phase A — Prepare the change

1. Implement the change on an appropriate feature branch.
2. Add or update tests and knowledgebase entries.
3. Run local quality checks and relevant tests.
4. Review the diff for unrelated changes, secrets, regressions, and unsafe behavior.
5. Commit authorized changes with descriptive messages.
6. Report any failed checks before proposing a PR.

Phase B — Publish and open the PR

After explicit owner authorization:

1. Push the feature branch to the correct repository.
2. Create a PR using "gh pr create".
3. Provide a meaningful title and description.
4. Explain the motivation, implementation, compatibility implications, and test results.
5. Link related issues or prior decisions when relevant.
6. Include a test checklist and identify known limitations.
7. Never include credentials, sensitive logs, or private account data.

Use the GitHub CLI to inspect PRs and checks:

gh pr view
gh pr checks
gh run list

Use the actual PR number or branch where the command requires it. Verify the command's result rather than assuming the operation succeeded.

Phase C — Independent review

After the PR is available, arrange an independent review by one or more agents that did not implement the change.

Reviewers must examine the full diff and relevant surrounding code, not merely the PR description.

At minimum, review the following areas:

Correctness and logic

- Incorrect assumptions and edge cases.
- State transitions and inconsistent runtime state.
- Race conditions, deadlocks, cancellation, and concurrency issues.
- Incorrect error handling and resource leaks.
- Off-by-one errors, retry behavior, and boundary conditions.

Security

- Credential exposure and unsafe logging.
- Input validation and trust boundaries.
- Improper authorization or account isolation.
- Unsafe filesystem and database operations.
- Dependency and supply-chain risks.
- Unbounded concurrency, retry loops, or request generation.
- Potential misuse of external services or sensitive account data.

Fragile implementation

- Broad exception handling and silent failures.
- Hidden coupling and circular dependencies.
- Unclear ownership of shared state.
- Incorrect cleanup and shutdown behavior.
- Platform-dependent assumptions.
- Unstable tests and timing-sensitive logic.
- Backward compatibility and migration risks.
- Missing diagnostics, observability, or recovery paths.
- Overly complicated abstractions and unnecessary dependencies.

Testing and maintainability

- Missing regression tests.
- Tests that do not exercise the claimed behavior.
- Weak assertions and untested failure paths.
- Type-checking and linting gaps.
- Documentation and configuration inconsistencies.

Reviewers must inspect suspicious or fragile areas even when the code appears to work and all existing tests pass.

Each finding should include:

1. Severity: critical, high, medium, or low.
2. File and relevant line or code location.
3. The failure mode or security risk.
4. Conditions under which it can occur.
5. Evidence or a reproduction where practical.
6. A recommended correction.
7. A regression test where appropriate.

Reviewers must distinguish confirmed defects from potential risks and explain their reasoning. Do not invent vulnerabilities or approve code without examining the relevant implementation.

Phase D — Correct findings and repeat

The implementation agent must evaluate every review finding.

- Fix confirmed defects and justified security issues.
- Add regression tests for reproducible defects.
- Address important fragility and maintainability concerns.
- Explain why a finding is not applicable when rejecting it.
- Document significant decisions and corrections in the knowledgebase.
- Push additional commits only when the owner has authorized remote updates.

After corrections, rerun applicable tests and checks. Request another review of the updated diff when necessary.

A previous approval does not automatically approve subsequent material changes.

Phase E — Merge gate

The PR is eligible for merge only when:

- Required CI checks pass.
- Independent review is complete.
- No unresolved critical or high-severity findings remain.
- Medium- and low-severity findings are fixed or explicitly dispositioned with justification.
- Relevant regression tests are present.
- The final diff has been inspected.
- Required documentation and knowledgebase entries are complete.
- The owner has approved the merge when owner approval is required.

Agents must not merge a PR without explicit owner authorization.

If checks fail or a serious finding remains unresolved, stop the merge workflow and notify the owner. Do not bypass branch protections or force a merge to satisfy a deadline.

9. GitHub Actions and CI Workflows

Store CI workflow definitions under:

.github/
└── workflows/
    ├── ci.yml
    └── security.yml

Create only workflows that provide useful, maintainable coverage. A separate security workflow is optional; security checks may initially be included in the main CI workflow.

Use "gh" to inspect repository settings, workflow runs, check results, and PR status, and to manage authorized GitHub operations.

Workflow YAML must still be created and reviewed as repository code. "gh" does not replace the need to maintain correct workflow definitions.

Recommended CI stages

On pull requests and relevant pushes to the default branch:

1. Install a supported Python version.
2. Install dependencies from the committed lockfile.
3. Run Ruff linting.
4. Run Ruff formatting checks.
5. Run mypy.
6. Run pytest.
7. Upload useful diagnostics or coverage results when justified.

Start with one maintainable CI workflow. Add a Python-version matrix when the project officially supports multiple versions and the extra runtime is worthwhile.

Recommended baseline:

uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run pytest

Use the project's actual package paths and supported Python version. Add coverage thresholds only after meaningful baseline coverage exists; do not create arbitrary thresholds that encourage superficial tests.

Workflow security

- Use minimal GitHub Actions permissions.
- Pin third-party actions to verified immutable commit SHAs where practical.
- Avoid executing untrusted pull-request code with privileged secrets.
- Never expose account tokens or deployment credentials to ordinary test jobs.
- Do not permit routine CI to perform live Discord account actions.
- Avoid automatic deployments, publishing, or remote mutations unless separately authorized.
- Validate workflow syntax and behavior.
- Confirm actual GitHub check results with "gh" when authorized and available.

Local YAML validation is not equivalent to a successful GitHub Actions run.

10. Mandatory Verification Gate

Before declaring a change ready for review, run all applicable checks.

On this machine (authoritative, run from the project root):

V=.venv/bin
$V/ruff check .
$V/ruff format --check .
$V/mypy .
$V/pytest
git diff --check

Portable equivalent (uv-managed environments): "uv sync --locked", then prefix each tool with "uv run".

Adapt commands to the actual project configuration. If a lockfile has not yet been established, document and perform the initial dependency setup rather than claiming locked installation succeeded.

If any mandatory check fails:

1. Stop the completion and merge workflow.
2. Do not push or merge unless an explicit owner-authorized diagnostic action is needed.
3. Preserve the relevant output and traceback.
4. Identify whether the failure is caused by implementation, dependencies, tests, configuration, external services, or the environment.
5. Document the failure in the knowledgebase.
6. Inform the owner of the evidence, impact, and recommended remediation.
7. Resume only when the issue is corrected or the owner explicitly authorizes a different course of action.

Never conceal failures by disabling tests, weakening assertions, removing checks, or reporting only successful commands.

11. Environment Suitability

Evaluate whether the current environment can reliably implement and validate the requested change.

Check relevant Python versions, operating-system compatibility, permissions, disk space, network access, dependencies, system libraries, databases, event-loop assumptions, and differences between local and CI environments.

If the environment appears unsuitable, notify the owner immediately.

Include:

- The failing command.
- The observed error.
- The affected platform or dependency.
- Whether the cause is confirmed or only suspected.
- The impact on implementation and verification.
- The recommended remediation.

Distinguish environmental failures from actual code defects. Do not silently skip tests or substitute weaker checks without reporting the limitation.

Do not alter system packages, global Python installations, boot configuration, permissions, or security settings without appropriate owner approval.

12. Security and Account Safety

- Never hardcode credentials or expose tokens, cookies, authorization headers, or secrets.
- Never commit local credential databases or sensitive account data.
- Use mocks and fixtures for automated tests.
- Validate untrusted input.
- Apply bounded concurrency, sensible timeouts, and controlled retries.
- Handle cancellation, network errors, and resource cleanup.
- Respect applicable platform rules, API constraints, and rate limits.
- Never use CI to perform unapproved production actions.
- Do not intentionally bypass platform safeguards or account restrictions.
- Review changes affecting authentication, persistence, account isolation, or task execution with additional care.
- Document significant risks and mitigations before consequential operations.

13. Required Implementation Workflow

Every substantive change follows this sequence:

1. Inspect: Repository, existing code, Git state, knowledgebase, and environment.
2. Plan: Intended behavior, files affected, tests, compatibility, and risks.
3. Implement: Focused changes, appropriate dataclasses, typing, and regression tests.
4. Document: Append a detailed knowledgebase entry.
5. Verify: Run the applicable quality gates and inspect the diff.
6. Commit: Only when authorized and mandatory checks pass.
7. Prepare PR: Only after explicit authorization for remote operations.
8. Review: Independent agents examine correctness, security, and fragile code.
9. Correct: Fix findings, add tests, and rerun checks.
10. Merge: Only after all required gates pass and the owner authorizes the merge.
11. Report: Provide verified results and outstanding limitations.

If a phase is blocked, report the blocker instead of silently bypassing it.

14. Final Agent Report

Every completed task must report:

1. Implemented behavior.
2. Files added, modified, renamed, or deleted.
3. Knowledgebase entry paths.
4. Commands executed and actual results.
5. Environment limitations and unverified assumptions.
6. Git branch and commit identifiers, where applicable.
7. PR number and CI/review status, if applicable.
8. Outstanding defects and review findings.
9. Whether changes are uncommitted, committed locally, or published remotely.
10. Whether any remote operation or merge remains pending owner approval.

Never claim that tests passed without executing them. Never describe an unreviewed PR as fully approved.

15. Definition of Done

- [ ] Existing architecture and repository instructions were inspected.
- [ ] The implementation is focused and maintainable.
- [ ] Dataclasses and typing are used appropriately.
- [ ] Relevant automated and regression tests are present.
- [ ] Required local quality checks pass.
- [ ] Knowledgebase documentation is appended.
- [ ] Dependencies and lockfiles are consistent.
- [ ] No secrets or unrelated changes are included.
- [ ] The PR has been independently reviewed (applies to the PR workflow; owner-directed direct pushes still require the full local gate).
- [ ] Confirmed defects and justified security findings have been addressed.
- [ ] Required CI checks pass (CI: `.github/workflows/ci.yml` — locked install with extras, ruff, format, mypy, pytest; first green run 40393344534 confirmed 2026-10-26; check "gh run list" for the latest state).
- [ ] Push classification (breaking vs feature, severity) recorded per section 8 before any push.
- [ ] No unresolved critical or high-severity findings remain.
- [ ] Owner approval has been obtained for required remote operations and merging.
- [ ] The final report accurately describes the verified state.

16. Workspace Environment (Linux)

This project is developed on a Linux machine (ext4 root and home, user "libosterelezent"). Treat the following as stable facts until corrected by the owner:

Paths

- Project tree: "/home/libosterelezent/Workspace/AutoClaimer" (ext4, user-owned).
- Git metadata: "/home/libosterelezent/autoclaimer.git" (outside the tree; see section 7). The in-tree ".git" pointer file links them.
- Virtualenv: "/home/libosterelezent/Workspace/AutoClaimer/.venv" (project-local, gitignored; holds ruff, mypy, pytest, pytest-asyncio, and the editable project install).

Known constraints

- The system Python is 3.13.5 and externally-managed (PEP 668); never pip-install into it and never pass "--break-system-packages"; use the project venv (section 10).
- "uv" lives in the project venv (".venv/bin/uv"); it is not on PATH in non-interactive shells. "uv sync" is strict and would prune a pip-installed uv from the venv — use "--inexact" or the standalone copy at "~/.local/bin/uv".
- File modes across the tree are 0777, inherited from the migration through a Windows/FUSE host; "core.filemode=false" is set globally, but a fresh clone resets the repo-local value to "true" — re-set it after any gitdir rebuild (section 7).
- If a tracked file vanishes or shows unexpected mode-only modifications, verify against HEAD and record the incident in the knowledgebase.
- Network egress to PyPI is slow (~200 KB/s observed); prefer pip's wheel cache and run long installs in the background.
- Prefer "/tmp/opencode" for throwaway artifacts.

Remote

- Repository: "libosterelezent/AutoClaimerMudae" (private). Discord notifications flow through a native GitHub repo webhook in repository settings; no workflow files or local hook dispatchers exist.

Final principle: Correctness, security, reproducibility, traceability, and owner control take precedence over speed. A passing test suite is necessary but not sufficient: independent review must examine the implementation's logic, security boundaries, and fragile areas before a change can be merged.

17. Error Handling, Failure Classification, and Recovery

17.1 Core principles

- Classify before recovering. Every handled failure must have a defined category, an owning layer, and an explicit recovery policy. Do not treat every failure as transient or retryable.
- Catch narrowly by default. Catch the most specific exception type that supports the intended recovery. Never use broad exception handling merely to suppress errors, keep execution moving, or avoid fixing an underlying defect.
- Preserve failure context. Include the operation, account/task identifiers where applicable, relevant lifecycle state, and a traceback for unexpected exceptions. Never include Discord tokens, credentials, authorization headers, or sensitive message content in logs.
- Fail explicitly when recovery is unsafe. If the system cannot determine whether an operation succeeded, represent that uncertainty rather than silently marking it successful or blindly repeating a potentially mutating action.
- Make recovery observable. Retries, cooldown deferrals, fatal transitions, event-loss recovery, and abandoned operations must produce structured logs or state transitions appropriate to their severity.

17.2 Required error categories and ownership

Every failure must be assigned to one of these categories. The layer that detects a failure may translate it into a typed error, but retry ownership must remain unambiguous.

1. Configuration and validation errors

   - Validate configuration structure, required fields, identifier types, ranges, and incompatible settings before starting workers.
   - Validate channel and resource identifiers before use, and resolve resources at the appropriate adapter boundary.
   - Treat invalid configuration as a startup or task-configuration failure, not as a transient Discord error.
   - Do not repeatedly retry deterministic validation failures. Report the invalid field and expected format without exposing secrets.
   - Credential format checks do not prove credential validity. Authentication must be verified through the supported login flow.

2. Credential and Discord client lifecycle errors

   - The client lifecycle layer owns login, connection, readiness, gateway disconnection, reconnection, and account-level fatal state.
   - Distinguish recoverable disconnections from unrecoverable authentication failures, CAPTCHA/security challenges, and other fatal account conditions.
   - Do not repeatedly re-login or reconnect after a classified fatal, unrecoverable failure. Stop or disable the affected account according to the established lifecycle policy.
   - Do not allow a dead or failed client to remain represented as healthy or ready. Publish the state transition and ensure dependent workers stop, wait, or recover according to policy.

3. Discord adapter and interaction errors

   - The adapter translates library-specific failures into stable, typed AutoClaimer errors. Do not leak library exceptions throughout the task layer when a meaningful typed classification is available.
   - Distinguish rate limits, permission or validation failures, recoverable transport failures, fatal client failures, and uncertain operation outcomes.
   - Do not classify an HTTP/server error as automatically safe to retry when a mutating request may have reached Discord before the response was lost.
   - The adapter reports what happened or what can be established; it does not decide Mudae-specific claim/roll cooldown policy.

4. Mudae domain outcomes

   - Treat parsed bot responses as domain outcomes, not automatically as Python exceptions.
   - Represent successful actions, claim or roll cooldowns, already-claimed results, unsupported responses, malformed messages, and missing responses distinctly.
   - The Mudae task/domain layer owns interpretation of Mudae messages and the scheduling of claim/roll retries from verified domain information.
   - Convert parser-provided relative durations into timezone-aware absolute UTC deadlines at this boundary before persisting them.
   - A missing duration is not zero. Never invent a cooldown deadline from a failed parse, absent response, or assumption.
   - If a response is malformed or unrecognized, use a bounded, explicit fallback or reconciliation policy. Do not retry indefinitely or record an unverified success.

5. Internal runtime, persistence, and programming errors

   - Use typed persistence and runtime errors where callers need to distinguish recovery behavior.
   - Treat invariant violations, impossible state transitions, unexpected AttributeError/TypeError, data corruption, and programming defects as defects to investigate, not routine transient failures.
   - Preserve cancellation semantics. Do not convert task cancellation into an ordinary failure, retry, or successful completion.
   - If durable state cannot be read or written safely, do not claim that the corresponding operation was durably recorded. Fail or suspend the affected work according to the persistence contract.

17.3 Single-owner retry policy

Each operation must have one clearly documented retry owner. Lower layers may expose typed errors and retry metadata, but must not independently retry in ways that conflict with the owning layer.

- Client lifecycle: owns connection and readiness recovery, subject to the fatal-error policy.
- Discord adapter/rate limiter: owns transport-level rate-limit handling, pacing, and explicitly classified transient interaction failures.
- Mudae task/domain layer: owns claim/roll cooldown scheduling, domain-specific retry limits, and response reconciliation.
- Worker/runtime: owns task supervision, worker restart policy, shutdown behavior, and bounded recovery from unexpected handler failures.
- Configuration and validation: do not retry deterministic invalid input.

Before retrying a mutating operation, determine whether the outcome is known:

- Known not to have occurred: retry only if the error classification and retry policy permit it.
- Known to have occurred: record or reconcile the confirmed outcome; do not repeat the mutation unnecessarily.
- Outcome uncertain: do not blindly replay the action. Reconcile through an authoritative response, supported Mudae lookup, or another explicitly designed recovery mechanism. If reconciliation is unavailable, preserve the uncertainty and apply a bounded policy.

Retries must be bounded by attempt/time limits or a documented scheduling policy. Use the existing rate limiter and cooldown state rather than introducing competing retry loops. Apply backoff where appropriate, and ensure a retry cannot bypass a server-directed cooldown. Persist retry schedules and absolute cooldown deadlines whenever restart correctness requires them.

17.4 "except Exception" policy

Broad catches are permitted only at deliberate safety boundaries where allowing an unexpected exception to escape would violate a defined supervision or cleanup contract. Examples include:

- Top-level worker iteration or handler supervision.
- Per-subscriber event-dispatch isolation.
- Adapter event-bridge boundaries that must log malformed external payloads without corrupting unrelated event flows.
- Best-effort cleanup during shutdown or account-fatal handling.

Every broad catch must satisfy all of the following:

1. Have a documented reason why a broad boundary is necessary.
2. Log the exception with its traceback and sufficient non-sensitive operation context.
3. Apply an explicit policy: propagate, mark the task failed, stop/disable the account, trigger bounded recovery, or continue only after safely isolating the failure.
4. Avoid silently swallowing the error or reporting success after a failed operation.
5. Preserve asyncio.CancelledError propagation and normal shutdown semantics. Never accidentally swallow cancellation in a broad handler.
6. Have tests covering the recovery behavior where practical.

Outside these boundaries, catch specific exception types. Do not add "except Exception: pass", log-and-ignore handlers without a defined policy, or generic retry-on-any-error loops.

Review existing broad catches whenever their surrounding behavior changes. Narrow them when reliable specific exception types become available — for example, when parser and event-payload validation contracts make malformed input distinguishable from programming defects. Do not narrow a safety boundary blindly if doing so would leave worker supervision, dispatcher isolation, or resource cleanup unprotected.

17.5 Unexpected exceptions and diagnostics

- Use "logger.exception(...)" inside an exception handler when a traceback is needed, or explicitly include exception information in structured logging.
- Keep log fields useful and bounded: error category, operation, account/task identifiers, attempt number, client/worker state, and recovery decision.
- Redact tokens, credentials, sensitive headers, and other secrets from exception messages and contextual fields. Never log full credential-bearing configuration objects.
- Use tracemalloc only when investigating Python memory allocations or suspected memory leaks. It is not an exception-reporting, traceback, or general debugging substitute.
- Do not enable expensive diagnostics globally as a substitute for reproducing a defect. Add targeted instrumentation or deterministic tests when investigating a suspected race, leak, stale client, or event-dispatch failure.

17.6 Persistence, cooldowns, and restart recovery

- Persist cooldowns as absolute, timezone-aware UTC deadlines. Calculate remaining duration from the deadline and the current UTC time when needed.
- A deadline at or before the current UTC time is expired; it must not block work after restart.
- Load persisted cooldowns before spawning workers that could perform affected operations.
- Treat malformed persisted state as an explicit, diagnosable state-validation or persistence error. Do not silently fabricate a deadline or reinterpret invalid data without a documented migration policy.
- For crashes between sending a mutating action and persisting its result, assume the outcome may be uncertain. Reconcile before repeating non-idempotent actions whenever possible.
- Do not claim exactly-once execution solely because events are queued or state is stored in SQLite. Document the actual delivery and execution guarantees.

17.7 Required tests for error-handling changes

When modifying error classification, retry behavior, lifecycle handling, or recovery, add or update deterministic tests as appropriate:

- Fatal authentication/client errors stop the affected account and do not trigger repeated login attempts.
- Recoverable disconnects follow the lifecycle reconnection policy.
- Rate limits preserve their cooldown and cannot be bypassed by retries or restart.
- Uncertain mutation outcomes do not trigger unguarded duplicate actions.
- Malformed or unsupported Mudae responses do not produce fabricated success or cooldown state.
- Unexpected worker/handler failures are observed, logged, and handled according to the supervision contract.
- Cancellation propagates correctly through retries, worker shutdown, and adapter cleanup.
- Corrupt persisted state fails explicitly and does not silently become valid-looking state.
- Event-dispatch failures remain isolated without silently losing track of required reconciliation.

Run the targeted tests first, then the relevant integration tests and full configured quality checks. Report the failure classification, recovery decision, tests run, and any remaining uncertainty with every significant error-handling change.