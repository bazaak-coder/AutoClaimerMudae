"""Tests for configuration parsing, validation, precedence, and loading."""

import json
from pathlib import Path
from typing import Any

import pytest

from autoclaimer.config import (
    load_config,
    parse_accounts_config,
    parse_app_config,
    resolve_task_config,
)
from autoclaimer.models import ConfigValidationError, RateLimitPolicy

SNOWFLAKE_A = 271485504361996288
SNOWFLAKE_B = 380028190142042112


def test_logging_block_defaults_and_override() -> None:
    default_app = parse_app_config(base_app())
    assert default_app.logging.log_file == "autoclaimer.log"
    assert default_app.logging.max_bytes == 1_048_576
    assert default_app.logging.backup_count == 3
    assert default_app.logging.level == "INFO"

    raw = base_app()
    raw["logging"] = {
        "log_file": "/tmp/ac.log",
        "max_bytes": 2048,
        "backup_count": 5,
        "level": "debug",
    }
    settings = parse_app_config(raw).logging
    assert settings.log_file == "/tmp/ac.log"
    assert settings.max_bytes == 2048
    assert settings.backup_count == 5
    assert settings.level == "DEBUG"  # normalized uppercase


@pytest.mark.parametrize(
    "bad",
    [
        {"level": "LOUD"},
        {"max_bytes": 512},
        {"backup_count": 0},
        {"log_file": "  "},
        {"log_file": 42},
        {"surprise": 1},
    ],
)
def test_logging_invalid_values_are_rejected(bad: dict[str, Any]) -> None:
    raw = base_app()
    raw["logging"] = bad
    with pytest.raises(ConfigValidationError):
        parse_app_config(raw)


def test_task_priority_defaults_and_parses() -> None:
    app = parse_app_config(base_app())
    assert app.tasks["claim_test"].priority == 0
    raw = base_app()
    raw["tasks"]["claim_test"]["priority"] = 10
    assert parse_app_config(raw).tasks["claim_test"].priority == 10


@pytest.mark.parametrize("bad_priority", [True, -1, 1.5, "high"])
def test_task_priority_invalid_values_are_rejected(bad_priority: object) -> None:
    raw = base_app()
    raw["tasks"]["claim_test"]["priority"] = bad_priority
    with pytest.raises(ConfigValidationError, match="priority"):
        parse_app_config(raw)


def test_ratelimit_global_defaults_are_optional() -> None:
    app = parse_app_config(base_app())
    assert app.ratelimit == RateLimitPolicy()


def test_ratelimit_parses_at_all_three_layers() -> None:
    raw = base_app()
    raw["ratelimit"] = {"min_interval_seconds": 2, "max_concurrency": 5}
    raw["tasks"]["claim_test"]["ratelimit"] = {"retry_limit": 1}
    app = parse_app_config(raw)
    accounts = parse_accounts_config(
        {
            "version": 1,
            "accounts": [
                {
                    "id": SNOWFLAKE_A,
                    "tasks": ["claim_test"],
                    "ratelimit": {"min_interval_seconds": 0.5, "backoff_base_seconds": 1},
                }
            ],
        },
        app,
    )
    assert app.ratelimit == RateLimitPolicy(min_interval_seconds=2.0, max_concurrency=5)
    assert app.tasks["claim_test"].ratelimit == RateLimitPolicy(retry_limit=1)
    assert accounts.accounts[0].ratelimit == RateLimitPolicy(
        min_interval_seconds=0.5, backoff_base_seconds=1.0
    )


def test_ratelimit_accepts_int_for_duration_fields() -> None:
    raw = base_app()
    raw["ratelimit"] = {"min_interval_seconds": 1, "backoff_cap_seconds": 30}
    app = parse_app_config(raw)
    assert app.ratelimit.min_interval_seconds == 1.0
    assert app.ratelimit.backoff_cap_seconds == 30.0


@pytest.mark.parametrize(
    "bad",
    [
        {"min_interval_seconds": -0.5},
        {"min_interval_seconds": True},
        {"max_concurrency": 0},
        {"retry_limit": -1},
        {"backoff_base_seconds": "soon"},
        {"surprise_key": 1},
    ],
)
def test_ratelimit_invalid_values_are_rejected(bad: dict[str, Any]) -> None:
    raw = base_app()
    raw["ratelimit"] = bad
    with pytest.raises(ConfigValidationError):
        parse_app_config(raw)


def base_app() -> dict[str, Any]:
    return {
        "version": 1,
        "settings": {"log_level": "INFO", "max_concurrent_accounts": 2},
        "task_defaults": {"enabled": True, "retry_limit": 4},
        "tasks": {
            "claim_test": {
                "enabled": True,
                "retry_limit": 2,
                "parameters": {"mode": "safe", "limit": 10},
            }
        },
    }


def test_send_jitter_defaults_and_parses() -> None:
    from autoclaimer.models import AppSettings

    assert parse_app_config(base_app()).settings.send_jitter_seconds == (0.203, 0.567)

    raw = base_app()
    raw["settings"] = {**raw["settings"], "send_jitter_seconds": [0.25, 0.9]}
    parsed = parse_app_config(raw).settings
    assert parsed.send_jitter_seconds == (0.25, 0.9)
    assert isinstance(parsed, AppSettings)


@pytest.mark.parametrize(
    "bad",
    [
        [0.203],
        [0.203, 0.4, 0.9],
        [0.6, 0.2],
        [-0.1, 0.5],
        ["0.2", "0.5"],
        [0.2, float("nan")],
        [0.2, float("inf")],
        0.203,
    ],
)
def test_send_jitter_invalid_values_are_rejected(bad: Any) -> None:
    raw = base_app()
    raw["settings"] = {**raw["settings"], "send_jitter_seconds": bad}
    with pytest.raises(ConfigValidationError):
        parse_app_config(raw)


def base_accounts() -> dict[str, Any]:
    return {
        "version": 1,
        "accounts": [
            {
                "id": SNOWFLAKE_A,
                "enabled": True,
                "tasks": ["claim_test"],
                "overrides": {"claim_test": {"enabled": False, "parameters": {"limit": 5}}},
            }
        ],
    }


# --- Precedence: global defaults -> task definition -> account override ---


def test_parses_and_resolves_account_overrides() -> None:
    app = parse_app_config(base_app())
    accounts = parse_accounts_config(base_accounts(), app)
    resolved = resolve_task_config(app, accounts.accounts[0], "claim_test")
    assert resolved.enabled is False
    assert resolved.retry_limit == 2
    assert resolved.parameters == {"mode": "safe", "limit": 5}


def test_task_definition_wins_over_global_defaults() -> None:
    raw = base_app()
    del raw["tasks"]["claim_test"]["enabled"]
    del raw["tasks"]["claim_test"]["retry_limit"]
    app = parse_app_config(raw)
    task = app.tasks["claim_test"]
    assert task.enabled is True  # from task_defaults
    assert task.retry_limit == 4  # from task_defaults


def test_built_in_defaults_used_when_layers_omitted() -> None:
    raw = base_app()
    del raw["task_defaults"]
    raw["tasks"]["claim_test"] = {"parameters": {"mode": "safe"}}
    app = parse_app_config(raw)
    task = app.tasks["claim_test"]
    assert task.enabled is True
    assert task.retry_limit == 3


def test_retry_limit_override_wins_over_task_definition() -> None:
    raw = base_accounts()
    raw["accounts"][0]["overrides"]["claim_test"]["retry_limit"] = 9
    app = parse_app_config(base_app())
    accounts = parse_accounts_config(raw, app)
    resolved = resolve_task_config(app, accounts.accounts[0], "claim_test")
    assert resolved.retry_limit == 9


def test_override_parameters_merge_shallow_over_definition() -> None:
    app = parse_app_config(base_app())
    accounts = parse_accounts_config(base_accounts(), app)
    resolved = resolve_task_config(app, accounts.accounts[0], "claim_test")
    assert resolved.parameters["mode"] == "safe"  # kept from definition
    assert resolved.parameters["limit"] == 5  # replaced by override


# --- Validation: IDs, references, duplicates, types, unknown keys ---


def test_unknown_task_reference_is_rejected() -> None:
    app = parse_app_config(base_app())
    raw = base_accounts()
    raw["accounts"][0]["tasks"] = ["missing_task"]
    with pytest.raises(ConfigValidationError, match="unknown task"):
        parse_accounts_config(raw, app)


def test_unknown_task_override_reference_is_rejected() -> None:
    app = parse_app_config(base_app())
    raw = base_accounts()
    raw["accounts"][0]["overrides"]["missing_task"] = {"enabled": False}
    with pytest.raises(ConfigValidationError, match="unknown task"):
        parse_accounts_config(raw, app)


def test_duplicate_account_ids_are_rejected() -> None:
    app = parse_app_config(base_app())
    raw = base_accounts()
    raw["accounts"].append(dict(raw["accounts"][0]))
    with pytest.raises(ConfigValidationError, match="Duplicate account id"):
        parse_accounts_config(raw, app)


def test_duplicate_task_ids_on_one_account_are_rejected() -> None:
    app = parse_app_config(base_app())
    raw = base_accounts()
    raw["accounts"][0]["tasks"] = ["claim_test", "claim_test"]
    with pytest.raises(ConfigValidationError, match="duplicate task IDs"):
        parse_accounts_config(raw, app)


def test_boolean_is_not_accepted_as_integer() -> None:
    raw = base_app()
    raw["settings"]["max_concurrent_accounts"] = True
    with pytest.raises(ConfigValidationError, match="integer"):
        parse_app_config(raw)


def test_unknown_keys_are_rejected() -> None:
    raw = base_app()
    raw["settings"]["surprise"] = 1
    with pytest.raises(ConfigValidationError, match="unknown key"):
        parse_app_config(raw)


@pytest.mark.parametrize("bad_id", [True, 0, -5, "12345", 2**63, 1.5])
def test_account_id_must_be_positive_integer_snowflake(bad_id: object) -> None:
    app = parse_app_config(base_app())
    raw = base_accounts()
    raw["accounts"][0]["id"] = bad_id
    with pytest.raises(ConfigValidationError, match="snowflake"):
        parse_accounts_config(raw, app)


def test_unsupported_version_is_rejected() -> None:
    raw = base_app()
    raw["version"] = 2
    with pytest.raises(ConfigValidationError, match="Unsupported config.version"):
        parse_app_config(raw)


def test_resolve_task_config_rejects_unassigned_task() -> None:
    app = parse_app_config(base_app())
    accounts = parse_accounts_config(base_accounts(), app)
    with pytest.raises(ConfigValidationError, match="not assigned"):
        resolve_task_config(app, accounts.accounts[0], "other_task")


# --- Credential policy: no plaintext credentials in configuration ---


@pytest.mark.parametrize("bad_key", ["token", "user_password", "api_key", "botToken"])
def test_credential_like_task_parameters_are_rejected(bad_key: str) -> None:
    raw = base_app()
    raw["tasks"]["claim_test"]["parameters"] = {bad_key: "super-secret"}
    with pytest.raises(ConfigValidationError, match="credential-like key"):
        parse_app_config(raw)


def test_credential_like_override_parameters_are_rejected() -> None:
    app = parse_app_config(base_app())
    raw = base_accounts()
    raw["accounts"][0]["overrides"]["claim_test"] = {"parameters": {"secret": "leak"}}
    with pytest.raises(ConfigValidationError, match="credential-like key"):
        parse_accounts_config(raw, app)


# --- JSON loading error reporting ---


def test_json_files_load_from_disk(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    accounts_path = tmp_path / "accounts.json"
    config_path.write_text(json.dumps(base_app()), encoding="utf-8")
    accounts_path.write_text(json.dumps(base_accounts()), encoding="utf-8")
    app, accounts = load_config(config_path, accounts_path)
    assert app.version == 1
    assert accounts.accounts[0].id == SNOWFLAKE_A


def test_missing_file_is_reported(tmp_path: Path) -> None:
    with pytest.raises(ConfigValidationError, match="not found"):
        load_config(tmp_path / "absent.json", tmp_path / "accounts.json")


def test_malformed_json_reports_position(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    accounts_path = tmp_path / "accounts.json"
    config_path.write_text('{"version": 1,\n  "settings": }', encoding="utf-8")
    accounts_path.write_text(json.dumps(base_accounts()), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="line 2, column 15"):
        load_config(config_path, accounts_path)


def test_non_object_json_root_is_rejected(tmp_path: Path) -> None:
    config_path = tmp_path / "config.json"
    accounts_path = tmp_path / "accounts.json"
    config_path.write_text("[]", encoding="utf-8")
    accounts_path.write_text(json.dumps(base_accounts()), encoding="utf-8")
    with pytest.raises(ConfigValidationError, match="must be a JSON object"):
        load_config(config_path, accounts_path)


# External audit BUG-002: nested credential keys are rejected recursively


@pytest.mark.parametrize(
    "parameters",
    [
        {"options": {"bot_token": "sensitive"}},
        {"auth": {"token": "secret"}},
        {"settings": [{"api_key": "secret"}]},
        {"nested": {"credentials": {"password": "secret"}}},
    ],
)
def test_nested_credentials_are_rejected(parameters: dict[str, Any]) -> None:
    raw = base_app()
    raw["tasks"]["claim_test"]["parameters"] = parameters
    with pytest.raises(ConfigValidationError):
        parse_app_config(raw)


def test_legitimate_nested_parameters_still_parse() -> None:
    raw = base_app()
    raw["tasks"]["claim_test"]["parameters"] = {
        "behavior": "safe",
        "limits": {"max": 10, "labels": ["a", "b"]},
    }
    parsed = parse_app_config(raw)
    assert parsed.tasks["claim_test"].parameters["limits"]["max"] == 10
