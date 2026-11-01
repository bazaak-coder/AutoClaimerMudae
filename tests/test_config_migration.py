"""Configuration migration framework (roadmap item 12 — P2).

Only schema version 1 exists today, so the production chain is empty;
these tests exercise the FULL machinery through the public registration
API — the documented pattern for the first future schema change:

- pure sequential migration with version bumping and setting preservation
- on-disk migration: backup beside the original, atomic rewrite, no .tmp
- fail-safe: a raising migrator or an invalid migrated document leaves
  the original file byte-identical
- backups are never overwritten; already-current files are untouched
- no-path and future-version errors are actionable
"""

import json
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

import autoclaimer.config as config_module
from autoclaimer.config import (
    CURRENT_CONFIG_VERSION,
    load_config,
    migrate_config_document,
    parse_app_config,
    register_config_migration,
)
from autoclaimer.models import ConfigValidationError


@pytest.fixture(autouse=True)
def _clean_chain() -> Any:
    """Tests register sample migrators; never leak them into other tests."""
    saved = dict(config_module._CONFIG_MIGRATIONS)
    yield
    config_module._CONFIG_MIGRATIONS.clear()
    config_module._CONFIG_MIGRATIONS.update(saved)


def _write_config(path: Path, document: dict[str, Any]) -> None:
    path.write_text(json.dumps(document, indent=2), encoding="utf-8")


def _accounts_file(path: Path) -> None:
    path.write_text(
        json.dumps({"version": 1, "accounts": [{"id": 123, "tasks": ["job"]}]}),
        encoding="utf-8",
    )


BASE_V1 = {
    "version": 1,
    "channels": {"roll_claim": 111},
    "settings": {"max_concurrent_accounts": 5, "log_level": "DEBUG"},
    "tasks": {"job": {"parameters": {"author_id": 222}}},
}


def _v1_to_v2(doc: Mapping[str, Any]) -> dict[str, Any]:
    """Sample future migration: NORMALIZES the channels section to spell
    every optional channel explicitly (schema-compatible with the
    current parser — a real v2 migrator ships together with its parser
    support)."""
    out = dict(doc)
    channels = dict(out.get("channels", {}))
    channels.setdefault("mu", None)
    channels.setdefault("rt", None)
    out["channels"] = channels
    return out


def _v2_to_v3(doc: Mapping[str, Any]) -> dict[str, Any]:
    out = dict(doc)
    out["settings"] = dict(out.get("settings", {}))
    return out


# --- pure chain ---------------------------------------------------------------


def test_pure_chain_applies_sequentially_and_bumps_versions() -> None:
    register_config_migration(1, _v1_to_v2)
    register_config_migration(2, _v2_to_v3)
    migrated, applied = migrate_config_document(BASE_V1, target=3)
    assert applied == [1, 2]
    assert migrated["version"] == 3
    # User settings preserved; the migration's normalization present.
    assert migrated["settings"]["max_concurrent_accounts"] == 5
    assert migrated["settings"]["log_level"] == "DEBUG"
    assert migrated["channels"]["mu"] is None
    assert migrated["channels"]["rt"] is None


def test_missing_step_is_actionable_without_io() -> None:
    register_config_migration(1, _v1_to_v2)
    with pytest.raises(ConfigValidationError, match="no registered migration"):
        migrate_config_document(BASE_V1, target=3)


# --- on-disk migration -----------------------------------------------------------


def test_load_migrates_on_disk_with_backup(tmp_path: Path) -> None:
    register_config_migration(1, _v1_to_v2)
    config_module.CURRENT_CONFIG_VERSION = 2
    try:
        cfg = tmp_path / "config.json"
        _write_config(cfg, BASE_V1)
        _accounts_file(tmp_path / "accounts.json")

        app, accounts = load_config(cfg, tmp_path / "accounts.json")

        # File rewritten to the new version (atomically — no .tmp left).
        on_disk = json.loads(cfg.read_text())
        assert on_disk["version"] == 2
        assert on_disk["channels"]["mu"] is None  # normalized
        assert on_disk["channels"]["rt"] is None
        assert not (tmp_path / "config.json.tmp").exists()
        # Backup holds the ORIGINAL, byte-preserved pre-migration content.
        backup = tmp_path / "config.json.v1.bak"
        assert backup.exists()
        assert json.loads(backup.read_text()) == BASE_V1
        # The migrated config parses and user settings survived.
        assert app.settings.max_concurrent_accounts == 5
        assert app.settings.log_level == "DEBUG"
        assert accounts.accounts[0].id == 123
    finally:
        config_module.CURRENT_CONFIG_VERSION = CURRENT_CONFIG_VERSION


def test_current_file_is_not_touched(tmp_path: Path) -> None:
    register_config_migration(1, _v1_to_v2)
    cfg = tmp_path / "config.json"
    _write_config(cfg, BASE_V1)
    _accounts_file(tmp_path / "accounts.json")
    before = cfg.read_text()
    load_config(cfg, tmp_path / "accounts.json")
    assert cfg.read_text() == before  # identical: nothing to migrate
    assert not list(tmp_path.glob("*.bak"))


def test_existing_backup_is_never_overwritten(tmp_path: Path) -> None:
    register_config_migration(1, _v1_to_v2)
    config_module.CURRENT_CONFIG_VERSION = 2
    try:
        cfg = tmp_path / "config.json"
        _write_config(cfg, BASE_V1)
        _accounts_file(tmp_path / "accounts.json")
        backup = tmp_path / "config.json.v1.bak"
        backup.write_text('{"sentinel": true}', encoding="utf-8")

        load_config(cfg, tmp_path / "accounts.json")
        assert json.loads(backup.read_text()) == {"sentinel": True}
    finally:
        config_module.CURRENT_CONFIG_VERSION = CURRENT_CONFIG_VERSION


# --- fail-safe paths -----------------------------------------------------------------


def test_raising_migrator_leaves_file_untouched(tmp_path: Path) -> None:
    def explode(doc: Mapping[str, Any]) -> dict[str, Any]:
        raise RuntimeError("migration bug")

    register_config_migration(1, explode)
    config_module.CURRENT_CONFIG_VERSION = 2
    try:
        cfg = tmp_path / "config.json"
        _write_config(cfg, BASE_V1)
        _accounts_file(tmp_path / "accounts.json")
        before = cfg.read_text()
        with pytest.raises(RuntimeError, match="migration bug"):
            load_config(cfg, tmp_path / "accounts.json")
        assert cfg.read_text() == before  # byte-identical
        assert not list(tmp_path.glob("*.bak"))
        assert not list(tmp_path.glob("*.tmp"))
    finally:
        config_module.CURRENT_CONFIG_VERSION = CURRENT_CONFIG_VERSION


def test_invalid_migrated_document_leaves_file_untouched(tmp_path: Path) -> None:
    def invalid(doc: Mapping[str, Any]) -> dict[str, Any]:
        out = dict(doc)
        out["channels"] = {"roll_claim": "not-an-integer"}  # fails validation
        return out

    register_config_migration(1, invalid)
    config_module.CURRENT_CONFIG_VERSION = 2
    try:
        cfg = tmp_path / "config.json"
        _write_config(cfg, BASE_V1)
        _accounts_file(tmp_path / "accounts.json")
        before = cfg.read_text()
        with pytest.raises(ConfigValidationError, match="roll_claim"):
            load_config(cfg, tmp_path / "accounts.json")
        assert cfg.read_text() == before
        assert not list(tmp_path.glob("*.tmp"))
    finally:
        config_module.CURRENT_CONFIG_VERSION = CURRENT_CONFIG_VERSION


def test_future_version_is_rejected(tmp_path: Path) -> None:
    cfg = tmp_path / "config.json"
    _write_config(cfg, {**BASE_V1, "version": 99})
    _accounts_file(tmp_path / "accounts.json")
    with pytest.raises(ConfigValidationError, match="Unsupported config.version"):
        load_config(cfg, tmp_path / "accounts.json")


def test_no_path_error_when_target_ahead_of_chain(tmp_path: Path) -> None:
    register_config_migration(1, _v1_to_v2)
    config_module.CURRENT_CONFIG_VERSION = 3
    try:
        cfg = tmp_path / "config.json"
        _write_config(cfg, BASE_V1)
        _accounts_file(tmp_path / "accounts.json")
        before = cfg.read_text()
        with pytest.raises(ConfigValidationError, match="no registered migration"):
            load_config(cfg, tmp_path / "accounts.json")
        assert cfg.read_text() == before
    finally:
        config_module.CURRENT_CONFIG_VERSION = CURRENT_CONFIG_VERSION


# --- parser uses the constant -----------------------------------------------------------


def test_parser_message_names_the_supported_version() -> None:
    with pytest.raises(ConfigValidationError, match="supported version is 1"):
        parse_app_config({**BASE_V1, "version": 2})
