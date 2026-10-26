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
class AppSettings:
    log_level: str = "INFO"
    max_concurrent_accounts: int = 3
    shutdown_timeout_seconds: int = 15


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
class TaskDefaults:
    enabled: bool = True
    retry_limit: int = 3


@dataclass(frozen=True)
class TaskDefinition:
    id: str
    enabled: bool = True
    retry_limit: int = 3
    parameters: dict[str, Any] = field(default_factory=dict)
    ratelimit: RateLimitPolicy | None = None


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
    ratelimit: RateLimitPolicy = field(default_factory=RateLimitPolicy)


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
