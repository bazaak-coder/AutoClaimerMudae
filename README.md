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
| `autoclaimer/mock_adapter.py` | implemented | Script-driven deterministic mock; no Discord connection, no credentials. |
| `autoclaimer/ratelimit.py` | planned | Rate limiting / request pacing. |
| `autoclaimer/workers.py` | planned | Worker orchestration across accounts. |
| `autoclaimer/persistence.py` | planned | SQLite state store and migrations. |
| `autoclaimer/cli.py` | planned | Command-line entry point. |

Planned boundaries are documented, not implemented; no stub modules exist to
avoid speculative code.

## Configuration model

Two files: `config.json` (global settings, task defaults, task definitions)
and `accounts.json` (per-account assignments and overrides).

- Account IDs are integers (Discord snowflakes).
- Task IDs are short identifier strings.

### Precedence (explicit)

```
global defaults (task_defaults)  <  task definition  <  account override
```

- A setting omitted at an outer layer falls back to the layer above it.
- Account overrides preserve explicit `false`; an omitted override key means
  "not overridden", not "false".

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
safety net. Account tokens are supplied at runtime by an implementation of
the `CredentialProvider` protocol in `autoclaimer/credentials.py`; this
project ships no secret-management implementation by design.

## Discord notifications

Commit messages, PRs, reviews, and comments are sent to a Discord webhook as
`application/json`:

- **GitHub events (pushes, PRs, comments):** a native repository webhook
  (Settings → Webhooks) posts GitHub's payload to the Discord `/github`
  endpoint, which renders it natively. Managed server-side; no workflow
  files involved.
- **Local commits:** `.githooks/post-commit` sends the commit message after
  each commit. It reads the URL from `webhook.url` at the repo root
  (gitignored — never commit it). Because the project lives on FUSE, which
  cannot store the executable bit, activate the hook with an executable
  copy outside the project tree after `git init`:

  ```bash
  git config core.hooksPath /workspace/autoclaimer-githooks
  mkdir -p /workspace/autoclaimer-githooks
  cp .githooks/post-commit /workspace/autoclaimer-githooks/post-commit
  chmod +x /workspace/autoclaimer-githooks/post-commit
  ```

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
  operations (send message, fetch recent messages, react, press component,
  close) with explicit results and an error hierarchy:
  `RecoverableAdapterError`, `PermanentAdapterError`, `CooldownError`
  (carries `retry_after_seconds`). Concrete adapters own client lifecycle.
- **Mock adapter** (`mock_adapter.MockAdapter`) is deterministic and fully
  test-controlled: scripted return values or exception instances per
  operation (FIFO), simulated latency, seeded channel state, and full call
  recording as structured test data.

## Next milestone (Phase 3, not started)

SQLite state-store interface and migrations, then the task registry, mock
adapter, and concurrent worker manager.
