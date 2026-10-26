AGENTS.md — AutoClaimer Development & Agent Operating Standards

1. Purpose and Authority

This document defines the mandatory development, testing, security, documentation, and Git workflow for every AI agent and human contributor working on AutoClaimer.

AutoClaimer is a lightweight, standalone Python project for configurable multi-account Discord automation targeting Mudae. Keep its architecture independent of unrelated projects, with minimal unnecessary dependencies and resource overhead.

Current status (update as phases complete): Phase 1 (typed configuration foundation, credential-provider interface, tooling), Phase 2 (task registry, platform adapter contracts, deterministic mock adapter), Phase 3 (async SQLite runtime persistence), Phase 4 (central rate limiter with scope cooldowns, pacing, permits, and bounded retries), Phase 5 (worker manager with concurrent account workers, deterministic priority scheduling, account controls, and graceful shutdown), and Phase 6 (interactive CLI with rotating, redacting, structured logging) are implemented and green. Runtime dependencies are still zero; discord.py-self is deliberately not integrated yet. The README package table is the authoritative module status. The next phase is real Discord adapter integration, not yet started.

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
- Handle exceptions at meaningful boundaries without silently swallowing errors.
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

Workspace reality (this machine): the system Python is an externally-managed (PEP 668) Alpine 3.14-series interpreter; do not install into it. All quality tooling lives in the virtualenv at "/workspace/autoclaimer-venv" (see section 16). The bare "pytest"/"ruff"/"mypy" on PATH belong to the system interpreter and cannot import the project — always invoke the venv binaries. "uv.lock" is maintained by running "uv lock" from the venv; there is no project-local .venv (FUSE cannot hold one).

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

Workspace git topology (this machine): git metadata lives in "/workspace/autoclaimer.git" because the project tree sits on FUSE, which cannot host git object storage. The project root contains a ".git" *pointer file* ("gitdir: /workspace/autoclaimer.git") so plain git commands work in place; git hard-ignores the pointer, and it must never be committed or deleted. Never delete "/workspace/autoclaimer.git" — it is the repository. FUSE occasionally drops worktree files (observed once); if a tracked file vanishes, restore it from HEAD and note the incident. Commit identity is configured repo-locally (libosterelezent / noreply address) and "gh auth setup-git" provides the HTTPS credential helper.

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

On this machine (authoritative):

V=/workspace/autoclaimer-venv/bin
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
- [ ] Required CI checks pass (no CI is configured in this repository yet — the local gate in section 10 is authoritative until CI exists).
- [ ] No unresolved critical or high-severity findings remain.
- [ ] Owner approval has been obtained for required remote operations and merging.
- [ ] The final report accurately describes the verified state.

16. Workspace Environment (Android/PRoot)

This project is developed on an Android device inside a PRoot Alpine guest. Treat the following as stable facts until corrected by the owner:

Paths

- Project tree: "/sdcard/Download/AutoClaimer" (FUSE: no exec bits, no symlinks, git cannot write objects there).
- Git metadata: "/workspace/autoclaimer.git" (ext4). The in-tree ".git" pointer file links them.
- Virtualenv: "/workspace/autoclaimer-venv" (ext4; holds ruff, mypy, pytest, pytest-asyncio, uv, and the editable project install).

Known constraints

- The system Python is externally-managed (PEP 668); never pip-install into it and never pass "--break-system-packages".
- Bare "pytest"/"ruff"/"mypy"/"uv" on PATH run against the system interpreter and cannot import the project; always use the venv binaries (section 10).
- FUSE cannot store executable bits or symlinks: venvs and git hooks must live under "/workspace"; in-tree hook scripts are non-executable.
- FUSE has silently dropped a tracked file once (".githooks/post-commit"); on any unexpected worktree deletion, restore from HEAD, verify, and record the incident in the knowledgebase.
- Prefer "/tmp/opencode" for throwaway artifacts and "/workspace" for anything needing exec bits or symlinks.

Remote

- Repository: "libosterelezent/AutoClaimerMudae" (private). Discord notifications flow through a native GitHub repo webhook in repository settings; no workflow files or local hook dispatchers exist.

Final principle: Correctness, security, reproducibility, traceability, and owner control take precedence over speed. A passing test suite is necessary but not sufficient: independent review must examine the implementation's logic, security boundaries, and fragile areas before a change can be merged.