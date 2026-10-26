AGENTS.md — AutoClaimer Development & Agent Operating Standards

1. Purpose and Scope

This document defines the mandatory rules for every AI agent, coding agent, and human contributor working on AutoClaimer.

AutoClaimer is a lightweight, standalone Python project for configurable multi-account Discord automation targeting Mudae. Keep its architecture independent of unrelated projects and avoid introducing unnecessary dependencies, abstractions, or resource overhead.

Every agent must follow these instructions before inspecting, modifying, testing, or committing project files.

Priority order:

1. Owner's explicit instructions.
2. Repository-specific instructions and established project architecture.
3. This document.
4. Individual agent preferences.

Never override an explicit owner restriction with an assumption about what would be convenient.

---

2. Mandatory Pre-Implementation Inspection

Before writing or changing code:

1. Inspect the repository structure and identify the project's current implementation.
2. Read this file and any applicable nested "AGENTS.md" files.
3. Inspect relevant source files, tests, configuration files, dependency declarations, and existing documentation.
4. Run "git status" and inspect the current branch and recent commit history.
5. Identify existing uncommitted changes, untracked files, and other agents' work.
6. Check the installed Python version, operating system, available dependencies, and relevant developer tools.
7. Determine how the project is currently tested and which commands are authoritative.
8. Consult the append-only knowledgebase for previous design decisions, failures, workarounds, and known environmental limitations.

Do not assume that a feature is missing simply because its implementation is not immediately obvious.

Do not overwrite, revert, discard, or reformat unrelated changes. Never use destructive Git operations to obtain a clean working tree without explicit owner authorization.

If repository access is unavailable or the environment cannot be inspected, report the limitation before proceeding with any work that depends on it.

---

3. Python Coding Standards

3.1 Dataclasses

Use "dataclasses" whenever they improve readability, maintainability, type safety, or the accessibility of structured data.

Prefer dataclasses for:

- Configuration and validated settings.
- Account and profile representations.
- Task definitions and execution results.
- Structured runtime state.
- Internal data-transfer objects.
- Typed records passed between modules.

Example:

from dataclasses import dataclass, field


@dataclass(slots=True)
class TaskResult:
    account_id: int
    task_name: str
    success: bool
    error: str | None = None
    metadata: dict[str, object] = field(default_factory=dict)

Guidelines:

- Use "default_factory" for mutable defaults.
- Use "slots=True" when appropriate and compatible with the project's supported Python versions.
- Use "frozen=True" for genuinely immutable value objects.
- Add explicit type annotations.
- Validate untrusted input at system boundaries.
- Keep initialization and validation logic straightforward.
- Avoid creating dataclasses for trivial values when they add no meaningful benefit.

Do not replace established data models indiscriminately. Introduce or modify models only when there is a clear implementation benefit.

3.2 General Python Practices

- Follow PEP 8 and modern Python typing conventions.
- Prefer small, cohesive functions with descriptive names.
- Use "pathlib.Path" for filesystem operations.
- Use context managers for files, database connections, and other managed resources.
- Handle exceptions at meaningful boundaries.
- Preserve original exceptions when adding context.
- Use the project's established logging system instead of scattering "print()" statements throughout runtime code.
- Avoid mutable global state and hidden side effects.
- Use asynchronous APIs consistently where the architecture requires them.
- Prevent blocking I/O from unnecessarily blocking the event loop.
- Ensure background tasks have explicit lifecycle and cancellation handling.
- Avoid broad "except Exception" blocks unless they are deliberate containment boundaries with appropriate logging and recovery behavior.
- Never silently swallow errors.
- Avoid unnecessary dependencies and premature abstractions.
- Keep public interfaces documented and stable where practical.

3.3 Architecture and Maintainability

Before introducing a new module, class, manager, or abstraction, explain why it is necessary in the implementation plan.

Prefer the smallest change that solves the actual problem without weakening existing behavior.

Maintain clear separation between configuration, account management, task scheduling, execution, logging, persistence, and user-facing interfaces as appropriate to the established architecture.

Do not duplicate existing functionality when it can be extended cleanly.

Never silently change public APIs, configuration formats, persisted data structures, or behavioral contracts. Document migrations and compatibility implications when a change is necessary.

---

4. Append-Only Knowledgebase — Mandatory

Every implementation session that changes project files must produce a corresponding informative entry in the repository's append-only knowledgebase.

Use the existing knowledgebase directory if one exists. Otherwise, create a clearly named "knowledgebase/" directory.

Never replace or rewrite historical entries to make the project history appear cleaner.

4.1 Suggested Structure

knowledgebase/
├── README.md
├── decisions/
├── changes/
├── debugging/
└── environment/

Adapt this structure to the repository if a knowledgebase already exists. Avoid redundant documentation files.

Use dated entries with descriptive filenames, for example:

knowledgebase/changes/2026-10-26-task-scheduler.md
knowledgebase/debugging/2026-10-26-async-test-failure.md
knowledgebase/environment/2026-10-26-python-version-check.md

Use the repository's local date and established naming conventions.

4.2 Required Change-Log Contents

Each change entry must include:

1. Summary: What changed and why.
2. Files affected: Every added, modified, renamed, or deleted project file.
3. Implementation details: Important functions, classes, configuration keys, or interfaces affected.
4. Design decisions: Why the chosen approach was selected.
5. Dependencies and tooling: Any changes to packages, development tools, workflows, or lockfiles.
6. Testing: Commands executed, results, and relevant test coverage.
7. Failures: Exact errors, likely causes, and diagnostic evidence.
8. Environmental constraints: Python version, operating-system limitations, missing tools, or reproducibility issues when relevant.
9. Git state: Branch, relevant commit identifier if available, and whether changes were committed.
10. Remaining work: Known defects, limitations, follow-up tasks, or unverified assumptions.

Include relevant command output and tracebacks when they are needed to reproduce or diagnose a problem. Redact credentials, tokens, cookies, and other secrets.

4.3 Append-Only Rules

- Add a new entry for each implementation session.
- Never delete or silently modify historical findings.
- Correct earlier mistakes with a new dated correction that references the original entry.
- Update an index by appending a dated reference where practical.
- Do not record secrets or complete credential-bearing environment dumps.
- Record failed approaches when they provide useful debugging information.
- Distinguish observed facts from hypotheses.

A change is not fully documented until its knowledgebase entry is saved.

If the knowledgebase cannot be updated, disclose this explicitly. Do not claim full completion while omitting required documentation.

---

5. Developer Tooling and Quality Gates

Use "uv" for Python environment and dependency management where supported by the project. Use Ruff and mypy for code quality and static analysis.

Recommended development dependencies:

- "ruff" — linting and formatting.
- "mypy" — static type checking.
- "pytest" — automated tests.
- "pytest-asyncio" — asynchronous tests when needed.
- Additional testing plugins only when justified.

Do not add a tool merely to satisfy this list if it is incompatible with the project. Report incompatibilities and propose a supported alternative.

5.1 Ruff

Configure Ruff centrally in "pyproject.toml".

Use Ruff for:

- Linting.
- Import ordering.
- Formatting checks.
- Identifying common Python defects.

Typical commands:

uv run ruff check .
uv run ruff format --check .

When formatting is required:

uv run ruff format .
uv run ruff check . --fix

Run automated fixes only on files within the authorized scope. Review the resulting diff before accepting changes.

Do not blindly run formatters across the entire repository when that would modify unrelated files or other agents' work.

5.2 Mypy

Configure mypy in "pyproject.toml" and progressively enforce useful type-checking standards.

Run:

uv run mypy .

If the project has an established source-package layout, prefer checking the actual package and relevant test modules rather than assuming that the repository root is a valid mypy target.

Guidelines:

- Add useful annotations rather than suppressing diagnostics.
- Avoid unnecessary "Any".
- Use targeted suppressions only when justified and documented.
- Do not disable type checking globally to conceal failures.
- Distinguish genuine defects from missing third-party stubs or environment-specific import problems.

5.3 Testing

Use pytest and the project's established test conventions.

Typical command:

uv run pytest

Tests must cover relevant success cases, error cases, boundary conditions, and regressions.

For asynchronous code, test cancellation, exceptions, timeouts, concurrency, and resource cleanup when applicable.

For account and task execution code, prefer isolated tests with mocked external interactions. Do not use real accounts, send live Discord messages, or perform production actions merely to test ordinary logic.

Do not claim that a test passed unless it was actually executed and its result was observed.

---

6. Dependency Management with uv

Use "uv" to manage the project environment and dependencies where compatible.

Expected project artifacts may include:

pyproject.toml
uv.lock

The precise configuration must match the existing project.

Rules:

1. Declare dependencies and supported Python versions explicitly.
2. Separate runtime dependencies from development dependencies.
3. Generate or update "uv.lock" through "uv" rather than hand-editing it.
4. Review dependency changes before accepting them.
5. Keep the lockfile consistent with the dependency declarations.
6. Prefer reproducible installations using the committed lockfile.
7. Avoid upgrading unrelated packages during a focused feature change.
8. Do not install unreviewed packages or execute untrusted setup scripts.
9. Preserve existing project constraints and report incompatible dependencies.
10. Never commit credentials, local virtual environments, caches, or machine-specific paths.

Typical commands:

uv lock
uv sync --locked
uv run pytest

If a lockfile update is necessary, inspect the diff and explain significant version changes in the knowledgebase.

Do not force a lockfile update simply to eliminate a warning if doing so would introduce unrelated dependency changes.

---

7. Git, GitHub CLI, and Change Management

Use Git and the GitHub CLI ("gh") when they are available and authorized for the task.

7.1 Before Editing

Inspect the repository:

git status --short --branch
git branch --show-current
git log -5 --oneline

Check the relevant files and recent commits before deciding how to implement a change.

If the GitHub CLI is available, inspect repository context where necessary:

gh repo view

Do not assume the current directory, repository, remote, or branch is the intended target. Verify them first.

7.2 During Implementation

- Keep changes focused and reviewable.
- Use feature branches when appropriate and consistent with the owner's workflow.
- Avoid unrelated refactoring.
- Review staged files before committing.
- Never include secrets, local caches, unrelated generated files, or another agent's unfinished changes.
- Never overwrite another contributor's work.
- Use Git history and diffs to understand changes rather than guessing.

Review changes with:

git diff --check
git diff
git status --short

Stage specific files whenever possible instead of staging everything indiscriminately.

7.3 GitHub Workflows

Add or modify GitHub Actions workflows only when they are useful and authorized.

For CI workflows:

- Pin third-party actions to verified immutable commit SHAs where practical.
- Use least-privilege "permissions".
- Avoid exposing secrets to untrusted pull-request code.
- Use locked dependency installation where appropriate.
- Run relevant linting, formatting checks, type checking, and tests.
- Avoid workflows that deploy, publish packages, or perform live account actions without explicit authorization.
- Keep workflows reproducible and consistent with the local development environment.

Validate YAML syntax and workflow logic using available tools. Do not claim that a workflow has passed on GitHub merely because its YAML parses locally.

7.4 Commit Policy

Commit changes only when authorized by the task and the repository's existing workflow.

Granular commit rule (mandatory): one commit per file or tightly-coupled
logical change, as the work happens — never batch unrelated changes into a
single large commit. Each commit's message must be short and state what was
edited (and why when it is not obvious), for example
"docs(readme): document venv setup for FUSE workspaces". Use the message
body only when the subject line cannot carry the context.

Use detailed, descriptive commit messages that explain both the change and its purpose.

Example:

feat(scheduler): add isolated task execution state

- Introduce typed task execution results.
- Separate per-account scheduling state.
- Add regression tests for cancellation and failures.
- Document implementation and verification results.

Before committing:

1. Complete the relevant quality gates.
2. Inspect the full diff.
3. Verify that only intended files are staged.
4. Confirm the knowledgebase entry is included.
5. Ensure no secrets or unrelated changes are present.

If tests fail, do not commit the implementation as a completed change. Report the failure and wait for the appropriate corrective action or explicit owner direction.

7.5 No Remote Push Without Owner Approval

Remote pushes are prohibited unless the owner explicitly authorizes them.

This includes ordinary pushes, force pushes, pushing tags, and publishing branches through alternative Git tooling.

The existence of a configured remote, a GitHub CLI session, or a request to finish implementation does not constitute push authorization.

Do not use "gh pr create", release publication, or equivalent remote-mutating operations as a workaround for this restriction.

Local commits may be created when authorized, but they must remain local until the owner grants permission to push.

If the owner has not authorized commits, leave the changes uncommitted and provide a clear summary of their status.

---

8. Mandatory Test-and-Verification Gate

Before declaring implementation complete, committing changes when authorized, or preparing a proposed release, run the applicable verification suite.

A normal verification sequence is:

uv sync --locked
uv run ruff check .
uv run ruff format --check .
uv run mypy .
uv run pytest
git diff --check

Adapt commands to the repository's actual package layout and configured quality gates. If no lockfile exists yet, establish dependency management deliberately rather than pretending the locked installation succeeded.

8.1 Failure Policy

A failed mandatory quality gate blocks completion.

If a required test, lint check, formatter check, type check, dependency synchronization, or other applicable verification fails:

1. Stop the completion and release workflow.
2. Do not push.
3. Do not claim the implementation is ready.
4. Preserve the diagnostic output.
5. Identify the failing command and relevant traceback or error messages.
6. Determine whether the failure is caused by code, dependencies, test assumptions, external services, or the environment.
7. Document the failure in the knowledgebase.
8. Inform the owner with the observed evidence and recommended next step.
9. Wait for authorization when the next step requires a decision or environmental change.

Do not skip failing tests, weaken assertions, remove tests, disable lint rules globally, or add broad suppressions simply to obtain a green result.

Do not conceal a failed check by running a different command and reporting only the successful result.

8.2 When Tests Are Unavailable

If tests cannot run because dependencies are missing, the required runtime is unavailable, or the environment is unstable:

- Record the exact limitation.
- Identify the checks that were not executed.
- Run only independent checks that remain meaningful.
- Label those checks accurately.
- Do not report the overall verification as passed.

A skipped test is not a passed test.

A partially verified implementation must be reported as partially verified.

---

9. Environment Stability and Suitability

The agent must actively evaluate whether the owner's current development environment is suitable for the requested work.

Check, where relevant:

- Python and dependency versions.
- Operating-system and architecture compatibility.
- Filesystem permissions and available disk space.
- Network or package-index availability.
- Missing compilers, system libraries, or external tools.
- Database availability and migration state.
- Event-loop and asynchronous runtime assumptions.
- Platform-specific path, process, and signal behavior.
- CI versus local environment differences.
- Whether failures reproduce consistently.
- Whether a required external service is unavailable or behaving differently from expected.

9.1 Mandatory Owner Notification

If evidence indicates that the current platform or environment is unsuitable or unstable for the task, notify the owner immediately and directly.

Use a factual, declarative report such as:

«Environment blocker: The current environment is not suitable for reliably validating this implementation. The test suite fails during dependency initialization because the required system library is unavailable. This is an environment-level failure, not yet evidence of a defect in the implementation. Testing is blocked until the dependency is installed or the project is validated in a compatible environment.»

Include:

- The failing command.
- The observed error.
- The affected platform or dependency.
- Whether the failure is confirmed to be environmental or remains uncertain.
- The consequences for implementation and testing.
- The recommended remediation.

Do not silently skip the test, substitute a weaker check, or present an unsupported workaround as equivalent verification.

Do not label an environment as unsuitable without evidence. Clearly distinguish a confirmed incompatibility from a suspected cause.

Do not automatically alter system packages, global Python installations, boot configuration, permissions, or security settings to repair the environment without owner approval.

---

10. Security and Account-Safety Requirements

AutoClaimer interacts with Discord accounts and external services. Protect credentials and avoid unnecessary risk.

- Never hardcode account tokens or credentials.
- Never print tokens, cookies, authorization headers, or secrets in logs.
- Keep secrets out of Git history and knowledgebase entries.
- Use appropriate secret storage and restrictive file permissions.
- Avoid committing local account databases or credential files.
- Use mocks and fixtures for automated tests.
- Avoid uncontrolled concurrency, unbounded retries, and request storms.
- Handle network failures, timeouts, cancellation, and service errors explicitly.
- Respect applicable platform rules, API constraints, and service rate limits.
- Do not build tests that intentionally evade platform safeguards or attempt to bypass account restrictions.
- Do not initiate destructive account actions or production tasks without explicit authorization.
- Treat configuration files, external responses, and account data as potentially untrusted inputs.

When a task introduces a security or operational risk, describe the risk and the mitigation before proceeding with consequential actions.

---

11. Required Implementation Workflow

Every substantive change must follow this sequence.

Phase A — Inspect

- Read repository instructions and relevant code.
- Inspect Git state and existing changes.
- Check the environment and applicable knowledgebase entries.
- Identify the actual problem and expected behavior.

Phase B — Plan

- Define the intended behavior.
- Identify the files likely to change.
- Consider compatibility and migration requirements.
- Identify appropriate tests and verification commands.
- Flag environmental or security risks early.

Phase C — Implement

- Make the smallest appropriate change.
- Follow existing architecture and Python conventions.
- Use dataclasses and type annotations where beneficial.
- Add or update tests alongside implementation.
- Update configuration and dependencies only when necessary.

Phase D — Document

- Record every changed file and significant implementation decision.
- Append a detailed knowledgebase entry.
- Include diagnostic information for failures.
- Never rewrite previous historical entries.

Phase E — Verify

- Run the relevant test suite and quality checks.
- Review diffs and inspect unintended changes.
- Resolve failures or report blockers.
- Never claim checks passed when they were not executed.

Phase F — Commit and Report

- Commit only when authorized and all mandatory quality gates pass.
- Use a detailed commit message.
- Do not push without explicit owner approval.
- Report the result, tests, known limitations, knowledgebase entry, and Git status.

If any phase is blocked, stop at the appropriate boundary and inform the owner rather than silently bypassing the requirement.

---

12. Final Agent Report

Every completed implementation task must end with a concise, evidence-based report containing:

1. Implemented: What behavior changed.
2. Files changed: Added, modified, deleted, or renamed files.
3. Knowledgebase: The exact entry paths created.
4. Verification: Commands executed and their actual results.
5. Environment: Any incompatibilities, instability, or unverified assumptions.
6. Git: Branch, commit identifier if applicable, and whether changes remain uncommitted.
7. Blockers: Outstanding defects or failed checks.
8. Remote status: Explicitly state that no remote push occurred unless the owner authorized it.

Never use vague claims such as "everything works" without evidence.

Never report a task as complete when required implementation, documentation, or verification remains outstanding.

---

13. Definition of Done

A change is complete only when all applicable requirements are satisfied:

- [ ] Repository instructions and existing implementation were inspected.
- [ ] The change is focused and follows project conventions.
- [ ] Dataclasses and type annotations are used appropriately.
- [ ] Relevant automated tests were added or updated.
- [ ] Required linting, formatting, type checks, and tests passed.
- [ ] Environment-related limitations were investigated and reported.
- [ ] Every changed file is documented in an append-only knowledgebase entry.
- [ ] Dependencies, workflows, and lockfiles are consistent where applicable.
- [ ] The diff contains no unrelated changes or exposed secrets.
- [ ] Git changes are staged and committed only as authorized.
- [ ] No remote push occurred without explicit owner approval.
- [ ] The final report accurately reflects the verified state.

Final principle: Correctness, reproducibility, traceability, and owner control take precedence over speed. When evidence is insufficient, report the uncertainty instead of guessing. When a mandatory check fails, stop and explain the failure rather than concealing it.