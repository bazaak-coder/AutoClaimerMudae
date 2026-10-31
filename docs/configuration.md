# Configuration

AutoClaimer reads two JSON files: **`config.json`** (global) and
**`accounts.json`** (per-account assignments). Both are validated at load
with actionable errors (file/line/column for malformed JSON, named settings
for semantic problems). Unknown keys are rejected everywhere.

Minimal complete `config.json`:

```json
{
  "version": 1,
  "channels": {
    "roll_claim": 123456789012345678
  },
  "tasks": {
    "roll": {
      "parameters": { "author_id": 123456789012345678, "username": "ExampleUser" }
    }
  }
}
```

---

## `config.json` reference

### `version` *(required)*

Must be `1`.

### `channels` *(required)*

Global channel configuration shared by **every account**. There are no
per-account or per-task channel settings.

| Field | Purpose | Required | Fallback |
|---|---|---|---|
| `roll_claim` | Mudae rolling **and** claim interactions | yes | — |
| `mu` | `$mu` commands | no | `roll_claim` |
| `rt` | `$rt` and `$rtu` (always the same channel) | no | `roll_claim` |

Rules:

- IDs must be positive integer snowflakes. Strings, floats, and **booleans
  are rejected** (`true` is not the snowflake `1`).
- `mu`/`rt` may be omitted or explicitly `null` — identical effect: use
  `roll_claim`. An explicitly configured value is always respected.
- A config **without** a `channels` section is rejected with instructions;
  channel IDs are never invented or defaulted silently.
- Setting a `channel` key inside any task `parameters` or account
  `overrides` is **rejected** — see the deprecation notice below.

> **⚠ DEPRECATED** since 2026-10-31 — the per-task `channel` parameter
> (previously allowed in task `parameters` and account `overrides`) is gone.
> It silently defeated the every-account-same-channels guarantee. Configure
> channels once in `channels`; every account follows. Existing configs must
> remove the parameter — the parser rejects it with a migration hint.

### `settings`

| Field | Default | Meaning |
|---|---|---|
| `log_level` | `"INFO"` | `CRITICAL`/`ERROR`/`WARNING`/`INFO`/`DEBUG` |
| `max_concurrent_accounts` | `3` (1–64) | Global bound on concurrently executing accounts |
| `shutdown_timeout_seconds` | `15` (1–600) | Graceful-shutdown budget |
| `send_jitter_seconds` | `[0.203, 0.567]` | Humanization delay pair `[low, high]` (≤ 60 s) applied before each send |

### `task_defaults`

Defaults inherited by every task definition: `enabled` (bool, default
`true`), `retry_limit` (int, default `3`).

### `tasks`

Map of task id → definition. Ids follow `[a-zA-Z][a-zA-Z0-9_-]{0,63}`.

| Field | Default | Meaning |
|---|---|---|
| `enabled` | from `task_defaults` | Materialize/execute this task at all |
| `retry_limit` | from `task_defaults` | Worker-level retry bound for recoverable failures |
| `parameters` | `{}` | Task parameters (see [index](index.md#currently-supported-tasks)); credential-like keys rejected recursively |
| `ratelimit` | — | Policy override layer (fields below) |
| `priority` | `0` | Scheduling tie-break (higher first) |

### `ratelimit` (global layer)

All fields optional; unspecified fields inherit from the next layer.

| Field | Meaning |
|---|---|
| `min_interval_seconds` | Minimum spacing between operations (pacing) |
| `max_concurrency` | Permit bound |
| `retry_limit` | Transport-level retry bound |
| `backoff_base_seconds` / `backoff_cap_seconds` | Exponential backoff window |

Layer precedence (field-wise): **account → task → global → built-in
defaults**. Server-directed cooldowns always outrank local policy.

### `logging`

| Field | Default | Meaning |
|---|---|---|
| `log_file` | `"autoclaimer.log"` | Rotating log path |
| `max_bytes` | `1048576` | Rotation size |
| `backup_count` | `3` | Retained rotations |
| `level` | `"INFO"` | File log level |

---

## `accounts.json` reference

```json
{
  "version": 1,
  "accounts": [
    {
      "id": 271485504361996288,
      "enabled": true,
      "tasks": ["roll", "mu", "rt"],
      "overrides": {
        "roll": { "enabled": false }
      },
      "ratelimit": { "min_interval_seconds": 1.5 }
    }
  ]
}
```

| Field | Meaning |
|---|---|
| `id` | Account snowflake (positive integer; duplicates rejected) |
| `enabled` | Whether the account's worker runs |
| `tasks` | Assigned task ids (must exist in `config.json`; duplicates rejected) |
| `overrides` | Per-task `{enabled?, retry_limit?, parameters?}` — merged over the task definition (shallow) |
| `ratelimit` | Account-level policy layer (wins over task/global) |

Rules worth knowing:

- An omitted override key means *not overridden*; an explicit `false` is
  honored (preserve-false semantics).
- Override `parameters` merge shallowly over task parameters.
- Removing an account from `accounts.json` does **not** delete its stored
  credential — that is an explicit `credential delete` (typo safety).

## Validation summary

- Unknown keys rejected at every level (typo catching).
- Snowflakes must be positive true integers (booleans/floats/strings rejected).
- Cross-references validated: task ids in assignments/overrides must exist;
  duplicate accounts/task-assignments rejected.
- Credential-like keys rejected anywhere in `parameters`, recursively
  (nested dicts and lists included).
