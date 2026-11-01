"""Typed configuration models and validation errors.

The models intentionally use only the Python standard library. Parsing and
validation are performed by config.py so callers receive actionable errors.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class ConfigValidationError(ValueError):
    """Raised when configuration data is structurally or semantically invalid."""


@dataclass(frozen=True)
class ChannelsConfig:
    """Global Discord channel settings shared by EVERY account.

    ``roll_claim`` (Mudae rolling + claim interactions) is mandatory.
    ``mu`` and ``rt`` are optional dedicated channels; when omitted or
    ``None`` they fall back to ``roll_claim``. Channel configuration is
    global by design — per-account or per-task channel overrides do not
    exist (config parsing rejects a ``channel`` task parameter).
    """

    roll_claim: int
    mu: int | None = None
    rt: int | None = None

    def effective(self) -> ResolvedChannels:
        """Resolve fallbacks ONCE, centrally (explicit None handling)."""
        return ResolvedChannels(
            roll_claim=self.roll_claim,
            mu=self.mu if self.mu is not None else self.roll_claim,
            rt=self.rt if self.rt is not None else self.roll_claim,
        )


@dataclass(frozen=True)
class ResolvedChannels:
    """All three channel IDs resolved — the only form tasks consume."""

    roll_claim: int
    mu: int
    rt: int

    def for_role(self, role: str) -> int:
        """The channel ID for a task's declared channel role."""
        value = getattr(self, role, None)
        if not isinstance(value, int):
            raise ValueError(f"unknown channel role {role!r} (expected roll_claim, mu, or rt)")
        return value


@dataclass(frozen=True)
class AppSettings:
    log_level: str = "INFO"
    max_concurrent_accounts: int = 3
    shutdown_timeout_seconds: int = 15
    # Task execution timeout (roadmap item 7): a handler exceeding this
    # is cancelled and failed with a distinct, recoverable outcome.
    # ``None`` (default) disables timeouts — backward compatible.
    task_timeout_seconds: float | None = None
    # Conservative humanization delay (low, high seconds) applied before
    # every send_message; deliberately tighter than the general mutating-
    # action jitter (owner directive 2026-10-28: "minimal, humanize the
    # interaction").
    send_jitter_seconds: tuple[float, float] = (0.203, 0.567)


@dataclass(frozen=True)
class RateLimitPolicy:
    """Local pacing policy; ``None`` fields inherit from the next outer layer.

    Layer precedence (field-wise): per-account override -> task policy ->
    global default -> built-in defaults. Server-directed cooldowns always
    take precedence over these local settings.
    """

    min_interval_seconds: float | None = None
    max_concurrency: int | None = None
    retry_limit: int | None = None
    backoff_base_seconds: float | None = None
    backoff_cap_seconds: float | None = None


@dataclass(frozen=True)
class LoggingSettings:
    """Rotating log file configuration (size limit + retention)."""

    log_file: str = "autoclaimer.log"
    max_bytes: int = 1_048_576
    backup_count: int = 3
    level: str = "INFO"


@dataclass(frozen=True)
class TaskDefaults:
    enabled: bool = True
    retry_limit: int = 3


@dataclass(frozen=True)
class TaskDefinition:
    id: str
    enabled: bool = True
    retry_limit: int = 3
    # Per-task timeout override (roadmap item 7): wins over
    # ``settings.task_timeout_seconds``; ``None`` inherits the default.
    timeout_seconds: float | None = None
    parameters: dict[str, Any] = field(default_factory=dict)
    ratelimit: RateLimitPolicy | None = None
    priority: int = 0


@dataclass(frozen=True)
class TaskOverride:
    enabled: bool | None = None
    retry_limit: int | None = None
    parameters: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True)
class AccountProfile:
    """One Discord account. ``id`` is the account's snowflake ID."""

    id: int
    enabled: bool = True
    tasks: tuple[str, ...] = ()
    overrides: dict[str, TaskOverride] = field(default_factory=dict)
    ratelimit: RateLimitPolicy | None = None


@dataclass(frozen=True)
class AppConfig:
    version: int
    settings: AppSettings
    task_defaults: TaskDefaults
    tasks: dict[str, TaskDefinition]
    channels: ChannelsConfig
    ratelimit: RateLimitPolicy = field(default_factory=RateLimitPolicy)
    logging: LoggingSettings = field(default_factory=LoggingSettings)


@dataclass(frozen=True)
class AccountsConfig:
    version: int
    accounts: tuple[AccountProfile, ...]


@dataclass(frozen=True)
class ResolvedTaskConfig:
    """Effective settings for one task assigned to one account."""

    account_id: int
    task_id: str
    enabled: bool
    retry_limit: int
    parameters: dict[str, Any]
    # Resolved execution timeout: task definition -> settings default
    # (roadmap item 7). ``None`` disables the timeout.
    timeout_seconds: float | None = None
