# AutoClaimer — Application Documentation

AutoClaimer is a lightweight, multi-account Discord automation framework built
around **Mudae** interactions. It runs configurable per-account task schedules
(rolling, claiming, cooldown lookups), enforces polite rate limits across
every outbound action, persists all state in a single SQLite database, and
stores account tokens only in **Fernet-encrypted** form.

**Documentation pages:**

| Page | Contents |
|---|---|
| [architecture.md](architecture.md) | System design, module map, execution and event flow |
| [configuration.md](configuration.md) | Complete `config.json` / `accounts.json` reference |
| [credentials.md](credentials.md) | Encrypted token storage, keys, rotation, backups |
| [running.md](running.md) | Booting the application, CLI reference, shutdown & recovery |

---

## Feature overview

- **Global channel configuration** — every account follows one shared channel
  setup: a mandatory `roll_claim` channel plus optional dedicated `mu` / `rt`
  channels that fall back to `roll_claim`.
- **Multi-account workers** — one sequential task stream per account, bounded
  global concurrency (`max_concurrent_accounts`), pause/resume/enable/disable
  per account or globally.
- **Mudae task handlers** — `$wa`-style rolling with one-shot claim reactions,
  `$mu` claim-cooldown lookups, and `$rt`/`$rtu` reset-window learning with a
  positive ✅-reaction confirmation gate.
- **Learned scheduling** — the handlers learn real cooldown windows from Mudae
  replies and reschedule at those learned deadlines (persisted across
  restarts).
- **Central rate limiter** — per-account pacing, permits, scoped cooldowns
  (account / route / global), server-directed cooldown handling with bounded
  retries, and per-account/per-task policy overrides.
- **Durable persistence** — single `state.db` (SQLite) holds task lifecycle,
  retry schedules, cooldowns, learned Mudae deadlines, and encrypted
  credentials; interrupted work is recovered at startup.
- **Encrypted credential store** — tokens are Fernet-encrypted before they
  touch disk; the encryption key lives only in the environment.
- **Redacting logging** — rotating log files with credential scrubbing on
  every path (messages, tracebacks, even handler-failure dumps).
- **Interactive CLI** — start/stop, account controls, queue/cooldown/error
  inspection, and credential management with hidden token input.

## Currently supported tasks

Registered by `register_mudae_tasks(registry, store=..., clock=...)`:

| Task id | Purpose | Channel | Notes |
|---|---|---|---|
| `roll` | Send the roll command; claim claimable cards | `roll_claim` | Reacts once with the claim emoji to claimable, unowned cards; verifies the username/character confirmation pair; learns the roll-quota window. |
| `mu` | Look up the claim cooldown (`$mu`) | `mu` (falls back to `roll_claim`) | Clears the claim deadline when available; learns it when cooling down. |
| `rt` | Use `$rt` and learn the reset window via `$rtu` | `rt` (falls back to `roll_claim`) | `$rt` only counts as used after Mudae's ✅ reaction confirms OUR message; `$rtu` reports the perk-dependent reset window (never assumed). |

Task parameters (validated per task):

| Parameter | Applies to | Default | Meaning |
|---|---|---|---|
| `author_id` | all | *required* | Mudae bot's snowflake ID — replies are matched by author. |
| `username` | roll, mu | *required* | The account's Discord username, used for reply attribution. |
| `reply_timeout_seconds` | all | `10.0` (max `120`) | How long to wait for a Mudae reply. |
| `round_interval_seconds` | roll | `2.0` | Fallback cadence between rounds (learned windows supersede it). |
| `round_interval_seconds` | mu, rt | `300.0` | Conservative fallback for lookup-style tasks. |
| `command` | roll | `"$wa"` | The roll command to send (≤ 2000 chars). |
| `claim` | roll | `true` | Whether to react to claimable cards. |
| `claim_emoji` | roll | `"❤️"` | Emoji used for the claim reaction. |

`channel` is **not** a task parameter — channels are configured globally (see
[configuration.md](configuration.md#channels)) and injected per task at
execution time.

A demo `ping` task (mock adapter only) is also available via
`build_demo_registry()` for smoke-testing the framework.

## Quick start

1. Install into a virtualenv (`uv sync --all-extras` or `pip install -e .[dev]`).
2. Write `config.json` and `accounts.json` (see
   [configuration.md](configuration.md)).
3. Generate a Fernet key and store account tokens (see
   [credentials.md](credentials.md)).
4. Boot the application (see [running.md](running.md)).

## Highlighted planned features

From the tracked [ROADMAP.md](../ROADMAP.md) (proposed, not yet implemented):

- **SQLite crash-recovery audit** (P0) — verified behavior under abrupt
  process termination, at every interruption point.
- **Database integrity checks** (P0) — `PRAGMA integrity_check` diagnostics,
  orphan/invalid-state detection, documented recovery procedure.
- **Account health monitoring** (P1) — per-account states (`HEALTHY`,
  `DISCONNECTED`, `AUTH_FAILED`, …) with last-success and consecutive-failure
  tracking, surfaced through the CLI.
- **Task execution timeouts** (P1) — configurable bounds so a handler can
  never stay active indefinitely.
- **Dead-letter handling** (P1) — exhausted, inspectable, manually retryable
  terminal state for repeatedly failing tasks.
- **CLI diagnostics** (`autoclaimer doctor`) (P2) — one command that checks
  configuration, database, credentials, and adapter readiness.
- **Simulation mode** (P3) — exercise scheduling and rate-limit decisions
  without sending real Discord actions.

## Documentation conventions

- These documents describe the **current merged application** on `main`.
- **Deprecation policy:** when behavior documented here is deprecated or
  replaced, a deprecation notice is added **directly below the affected
  section**, in this form:

  > **⚠ DEPRECATED** since `<version or date>` — `<what changed, what to use
  > instead, and until when the old behavior keeps working>`.

  The section itself is kept (not silently rewritten) until the deprecated
  behavior is removed, then the notice is updated to say so.
