"""JSON loading, structural validation, and configuration inheritance."""

from __future__ import annotations

import json
import math
import re
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .models import (
    AccountProfile,
    AccountsConfig,
    AppConfig,
    AppSettings,
    ConfigValidationError,
    LoggingSettings,
    RateLimitPolicy,
    ResolvedTaskConfig,
    TaskDefaults,
    TaskDefinition,
    TaskOverride,
)

_ID_PATTERN = re.compile(r"^[a-zA-Z][a-zA-Z0-9_-]{0,63}$")
_LOG_LEVELS = {"CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"}
_MAX_SNOWFLAKE = 2**63 - 1
# Substring heuristics for plaintext credentials that must never appear in
# JSON configuration. Credentials belong to a credential provider instead.
_CREDENTIAL_KEY_HINTS = (
    "token",
    "password",
    "secret",
    "cookie",
    "authorization",
    "api_key",
    "apikey",
    "credential",
)


def _object(value: Any, where: str) -> Mapping[str, Any]:
    if not isinstance(value, dict):
        raise ConfigValidationError(f"{where} must be a JSON object.")
    return value


def _only_keys(data: Mapping[str, Any], allowed: set[str], where: str) -> None:
    extra = sorted(set(data) - allowed)
    if extra:
        raise ConfigValidationError(f"{where} contains unknown key(s): {', '.join(extra)}.")


def _required(data: Mapping[str, Any], key: str, where: str) -> Any:
    if key not in data:
        raise ConfigValidationError(f"{where} is missing required key '{key}'.")
    return data[key]


def _bool(value: Any, where: str) -> bool:
    if type(value) is not bool:
        raise ConfigValidationError(f"{where} must be true or false.")
    return value


def _int(value: Any, where: str, *, minimum: int = 0, maximum: int | None = None) -> int:
    # bool is a subclass of int, so explicitly reject it.
    if type(value) is not int or value < minimum:
        raise ConfigValidationError(f"{where} must be an integer >= {minimum}.")
    if maximum is not None and value > maximum:
        raise ConfigValidationError(f"{where} must be an integer <= {maximum}.")
    return value


def _id(value: Any, where: str) -> str:
    if not isinstance(value, str) or not _ID_PATTERN.fullmatch(value):
        raise ConfigValidationError(
            f"{where} must start with a letter and contain only letters, "
            "digits, underscores, or hyphens (maximum 64 characters)."
        )
    return value


def _snowflake(value: Any, where: str) -> int:
    if type(value) is not int or not 0 < value <= _MAX_SNOWFLAKE:
        raise ConfigValidationError(f"{where} must be a positive integer Discord snowflake ID.")
    return value


def _parameters(value: Any, where: str) -> dict[str, Any]:
    obj = _object(value, where)
    for key in obj:
        lowered = key.lower()
        if any(hint in lowered for hint in _CREDENTIAL_KEY_HINTS):
            raise ConfigValidationError(
                f"{where} contains credential-like key '{key}'. Credentials "
                "must be supplied by a secure credential provider and never "
                "stored in plaintext JSON configuration."
            )
    # Ensure values can be represented as JSON, don't contain NaN/Infinity,
    # and stay bounded (64 KiB) so configs cannot balloon memory.
    try:
        encoded = json.dumps(obj, allow_nan=False)
        if len(encoded) > 65536:
            raise ConfigValidationError(f"{where} exceeds the 64 KiB size limit.")
    except (TypeError, ValueError) as exc:
        raise ConfigValidationError(f"{where} must contain JSON-compatible values: {exc}") from exc
    return dict(obj)


_RATELIMIT_KEYS = {
    "min_interval_seconds",
    "max_concurrency",
    "retry_limit",
    "backoff_base_seconds",
    "backoff_cap_seconds",
}


def _duration(value: Any, where: str, *, minimum: float = 0.0) -> float:
    # bool is a subclass of int, so explicitly reject it; accept int or
    # float — but never NaN/Infinity (JSON parsers accept both).
    if type(value) not in (int, float) or not math.isfinite(value) or value < minimum:
        raise ConfigValidationError(f"{where} must be a finite number >= {minimum}.")
    return float(value)


def _jitter_pair(value: Any) -> tuple[float, float]:
    """Validate an optional (low, high) jitter range for send humanization.

    Exactly two finite numbers, ``0 <= low <= high <= 60``; absent means
    the built-in default (0.203, 0.567).
    """
    default: tuple[float, float] = (0.203, 0.567)
    if value is None:
        return default
    if not isinstance(value, (list, tuple)) or len(value) != 2:
        raise ConfigValidationError(
            "config.settings.send_jitter_seconds must be a pair [low, high] of seconds."
        )
    low = _duration(value[0], "config.settings.send_jitter_seconds[0]")
    high = _duration(value[1], "config.settings.send_jitter_seconds[1]")
    if high > 60.0:
        raise ConfigValidationError(
            "config.settings.send_jitter_seconds high must not exceed 60 seconds."
        )
    if low > high:
        raise ConfigValidationError("config.settings.send_jitter_seconds low must not exceed high.")
    return (low, high)


def _parse_ratelimit(raw: Any, where: str) -> RateLimitPolicy:
    obj = _object(raw, where)
    _only_keys(obj, _RATELIMIT_KEYS, where)
    max_concurrency = None
    if "max_concurrency" in obj:
        max_concurrency = _int(obj["max_concurrency"], f"{where}.max_concurrency", minimum=1)
    retry_limit = None
    if "retry_limit" in obj:
        retry_limit = _int(obj["retry_limit"], f"{where}.retry_limit", maximum=100)
    return RateLimitPolicy(
        min_interval_seconds=(
            _duration(obj["min_interval_seconds"], f"{where}.min_interval_seconds")
            if "min_interval_seconds" in obj
            else None
        ),
        max_concurrency=max_concurrency,
        retry_limit=retry_limit,
        backoff_base_seconds=(
            _duration(obj["backoff_base_seconds"], f"{where}.backoff_base_seconds")
            if "backoff_base_seconds" in obj
            else None
        ),
        backoff_cap_seconds=(
            _duration(obj["backoff_cap_seconds"], f"{where}.backoff_cap_seconds")
            if "backoff_cap_seconds" in obj
            else None
        ),
    )


def _parse_logging(raw: Any, where: str) -> LoggingSettings:
    obj = _object(raw, where)
    _only_keys(obj, {"log_file", "max_bytes", "backup_count", "level"}, where)
    log_file = obj.get("log_file", "autoclaimer.log")
    if not isinstance(log_file, str) or not log_file.strip():
        raise ConfigValidationError(f"{where}.log_file must be a non-empty string.")
    if ".." in Path(log_file).parts or ".." in {
        seg for seg in log_file.replace(chr(92), "/").split("/")
    }:
        raise ConfigValidationError(
            f"{where}.log_file must not traverse out of its directory ('..' rejected)."
        )
    level = obj.get("level", "INFO")
    if not isinstance(level, str) or level.upper() not in _LOG_LEVELS:
        raise ConfigValidationError(
            f"{where}.level must be one of {', '.join(sorted(_LOG_LEVELS))}."
        )
    return LoggingSettings(
        log_file=log_file,
        max_bytes=_int(
            obj.get("max_bytes", 1_048_576),
            f"{where}.max_bytes",
            minimum=1024,
            maximum=1_073_741_824,
        ),
        backup_count=_int(
            obj.get("backup_count", 3),
            f"{where}.backup_count",
            minimum=1,
            maximum=100,
        ),
        level=level.upper(),
    )


def _load_json(path: str | Path) -> dict[str, Any]:
    file_path = Path(path)
    try:
        with file_path.open("r", encoding="utf-8") as handle:
            data = json.load(handle)
    except FileNotFoundError as exc:
        raise ConfigValidationError(f"Configuration file not found: {file_path}") from exc
    except json.JSONDecodeError as exc:
        raise ConfigValidationError(
            f"Invalid JSON in {file_path} at line {exc.lineno}, column {exc.colno}: {exc.msg}"
        ) from exc
    except OSError as exc:
        raise ConfigValidationError(f"Could not read {file_path}: {exc}") from exc
    return dict(_object(data, str(file_path)))


def valid_task_id(value: object) -> bool:
    """Public predicate: whether *value* is a well-formed task identifier."""
    return isinstance(value, str) and _ID_PATTERN.fullmatch(value) is not None


def parse_app_config(raw: Mapping[str, Any]) -> AppConfig:
    data = _object(dict(raw), "config")
    _only_keys(
        data,
        {"version", "settings", "task_defaults", "tasks", "ratelimit", "logging"},
        "config",
    )
    version = _int(_required(data, "version", "config"), "config.version", minimum=1)
    if version != 1:
        raise ConfigValidationError(
            f"Unsupported config.version {version}; supported version is 1."
        )

    s = _object(data.get("settings", {}), "config.settings")
    _only_keys(
        s,
        {
            "log_level",
            "max_concurrent_accounts",
            "shutdown_timeout_seconds",
            "send_jitter_seconds",
        },
        "config.settings",
    )
    log_level = s.get("log_level", "INFO")
    if not isinstance(log_level, str) or log_level.upper() not in _LOG_LEVELS:
        raise ConfigValidationError(
            f"config.settings.log_level must be one of {', '.join(sorted(_LOG_LEVELS))}."
        )
    settings = AppSettings(
        log_level=log_level.upper(),
        max_concurrent_accounts=_int(
            s.get("max_concurrent_accounts", 3),
            "config.settings.max_concurrent_accounts",
            minimum=1,
            maximum=64,
        ),
        shutdown_timeout_seconds=_int(
            s.get("shutdown_timeout_seconds", 15),
            "config.settings.shutdown_timeout_seconds",
            minimum=1,
            maximum=600,
        ),
        send_jitter_seconds=_jitter_pair(s.get("send_jitter_seconds")),
    )

    td = _object(data.get("task_defaults", {}), "config.task_defaults")
    _only_keys(td, {"enabled", "retry_limit"}, "config.task_defaults")
    defaults = TaskDefaults(
        enabled=_bool(td.get("enabled", True), "config.task_defaults.enabled"),
        retry_limit=_int(td.get("retry_limit", 3), "config.task_defaults.retry_limit"),
    )

    raw_tasks = _object(data.get("tasks", {}), "config.tasks")
    tasks: dict[str, TaskDefinition] = {}
    for raw_id, raw_definition in raw_tasks.items():
        task_id = _id(raw_id, f"config.tasks key {raw_id!r}")
        t = _object(raw_definition, f"config.tasks.{task_id}")
        _only_keys(
            t,
            {"enabled", "retry_limit", "parameters", "ratelimit", "priority"},
            f"config.tasks.{task_id}",
        )
        tasks[task_id] = TaskDefinition(
            id=task_id,
            enabled=_bool(t.get("enabled", defaults.enabled), f"config.tasks.{task_id}.enabled"),
            retry_limit=_int(
                t.get("retry_limit", defaults.retry_limit), f"config.tasks.{task_id}.retry_limit"
            ),
            parameters=_parameters(t.get("parameters", {}), f"config.tasks.{task_id}.parameters"),
            ratelimit=(
                _parse_ratelimit(t["ratelimit"], f"config.tasks.{task_id}.ratelimit")
                if "ratelimit" in t
                else None
            ),
            priority=_int(t.get("priority", 0), f"config.tasks.{task_id}.priority"),
        )

    return AppConfig(
        version=version,
        settings=settings,
        task_defaults=defaults,
        tasks=tasks,
        ratelimit=(
            _parse_ratelimit(data["ratelimit"], "config.ratelimit")
            if "ratelimit" in data
            else RateLimitPolicy()
        ),
        logging=(
            _parse_logging(data["logging"], "config.logging")
            if "logging" in data
            else LoggingSettings()
        ),
    )


def parse_accounts_config(raw: Mapping[str, Any], app_config: AppConfig) -> AccountsConfig:
    data = _object(dict(raw), "accounts")
    _only_keys(data, {"version", "accounts"}, "accounts")
    version = _int(_required(data, "version", "accounts"), "accounts.version", minimum=1)
    if version != 1:
        raise ConfigValidationError(
            f"Unsupported accounts.version {version}; supported version is 1."
        )
    raw_accounts = _required(data, "accounts", "accounts")
    if not isinstance(raw_accounts, list):
        raise ConfigValidationError("accounts.accounts must be a JSON array.")

    profiles: list[AccountProfile] = []
    seen: set[int] = set()
    for index, raw_profile in enumerate(raw_accounts):
        where = f"accounts.accounts[{index}]"
        p = _object(raw_profile, where)
        _only_keys(p, {"id", "enabled", "tasks", "overrides", "ratelimit"}, where)
        profile_id = _snowflake(_required(p, "id", where), f"{where}.id")
        if profile_id in seen:
            raise ConfigValidationError(f"Duplicate account id '{profile_id}'.")
        seen.add(profile_id)

        enabled = _bool(p.get("enabled", True), f"{where}.enabled")
        raw_task_ids = p.get("tasks", [])
        if not isinstance(raw_task_ids, list) or any(not isinstance(x, str) for x in raw_task_ids):
            raise ConfigValidationError(f"{where}.tasks must be an array of task ID strings.")
        if len(raw_task_ids) != len(set(raw_task_ids)):
            raise ConfigValidationError(f"{where}.tasks contains duplicate task IDs.")
        for task_id in raw_task_ids:
            if task_id not in app_config.tasks:
                raise ConfigValidationError(f"{where}.tasks references unknown task '{task_id}'.")

        raw_overrides = _object(p.get("overrides", {}), f"{where}.overrides")
        overrides: dict[str, TaskOverride] = {}
        for task_id, raw_override in raw_overrides.items():
            if task_id not in app_config.tasks:
                raise ConfigValidationError(
                    f"{where}.overrides references unknown task '{task_id}'."
                )
            o = _object(raw_override, f"{where}.overrides.{task_id}")
            _only_keys(o, {"enabled", "retry_limit", "parameters"}, f"{where}.overrides.{task_id}")
            override_enabled = None
            if "enabled" in o:
                override_enabled = _bool(o["enabled"], f"{where}.overrides.{task_id}.enabled")
            override_retry = None
            if "retry_limit" in o:
                override_retry = _int(o["retry_limit"], f"{where}.overrides.{task_id}.retry_limit")
            overrides[task_id] = TaskOverride(
                enabled=override_enabled,
                retry_limit=override_retry,
                parameters=_parameters(
                    o.get("parameters", {}), f"{where}.overrides.{task_id}.parameters"
                ),
            )

        profiles.append(
            AccountProfile(
                id=profile_id,
                enabled=enabled,
                tasks=tuple(raw_task_ids),
                overrides=overrides,
                ratelimit=(
                    _parse_ratelimit(p["ratelimit"], f"{where}.ratelimit")
                    if "ratelimit" in p
                    else None
                ),
            )
        )

    return AccountsConfig(version=version, accounts=tuple(profiles))


def load_config(
    config_path: str | Path, accounts_path: str | Path
) -> tuple[AppConfig, AccountsConfig]:
    """Load and validate both files, including cross-file task references."""
    app = parse_app_config(_load_json(config_path))
    accounts = parse_accounts_config(_load_json(accounts_path), app)
    return app, accounts


def resolve_task_config(
    app: AppConfig, account: AccountProfile, task_id: str
) -> ResolvedTaskConfig:
    """Resolve task defaults -> task definition -> account override.

    Account assignments determine which task IDs are considered. This function
    does not silently assign a task that isn't listed on the account.
    """
    if task_id not in account.tasks:
        raise ConfigValidationError(f"Task '{task_id}' is not assigned to account '{account.id}'.")
    try:
        task = app.tasks[task_id]
    except KeyError as exc:
        raise ConfigValidationError(f"Unknown task '{task_id}'.") from exc

    override = account.overrides.get(task_id, TaskOverride())
    parameters = dict(task.parameters)
    parameters.update(override.parameters)

    return ResolvedTaskConfig(
        account_id=account.id,
        task_id=task_id,
        enabled=task.enabled if override.enabled is None else override.enabled,
        retry_limit=task.retry_limit if override.retry_limit is None else override.retry_limit,
        parameters=parameters,
    )
