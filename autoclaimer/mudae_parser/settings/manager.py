"""``MudaeParserSettingsManager`` — the engine's configuration layer.

Owns the whole configuration lifecycle with three deliberately separate
states:

  * STORED    — what the user asked to save (persisted overrides only);
  * VALIDATED — candidates that passed schema/regex/group validation;
  * ACTIVE    — the published, immutable snapshot the parser parses
    against (one consistent snapshot per parse operation).

Responsiveness contract (atomic updates): every mutation is a
transaction — build candidate → validate → construct candidate active
config → persist → publish. A validation failure leaves everything
untouched; a persistence failure leaves the previously active config
and the stored overrides exactly as they were (tested rollback).

The manager contains NO extraction logic; the parser contains NO
persistence. Concurrency: updates serialize on a lock; parses read the
immutable ``effective_config`` snapshot without locking (a concurrent
update swaps the reference — each parse sees exactly one snapshot).
"""
from __future__ import annotations

import copy
import dataclasses
import os
import re
import threading
from types import MappingProxyType
from typing import Any, Dict, List, Mapping, Optional, Tuple, Union

from ..diagnostics import (
    CODE_CORRUPT_STORE,
    CODE_OUTDATED_STORE,
    CODE_REJECTED_OVERRIDE,
    Diagnostics,
)
from ..enums import (
    CaseSensitivity,
    MessageFamily,
    SettingOrigin,
    TargetSource,
)
from ..exceptions import (
    SettingsPersistenceError,
    UnknownSettingError,
)
from ..locales import DEFAULT_LOCALE, get_locale
from .defaults import default_patterns, default_strings
from .models import (
    MessageStringSetting,
    RegexPatternSetting,
    UpdateResult,
    ValidationIssue,
    ValidationReport,
)
from .storage import (
    JSONFileSettingsStorage,
    MemorySettingsStorage,
    ParserSettingsStorage,
    SCHEMA_VERSION,
)
from . import validator as _validator

__all__ = [
    "MudaeParserSettingsManager", "EffectiveConfig", "CompiledPattern",
    "PreviewResult", "SettingSummary",
]

_DEFAULT_STORAGE_NAME = "parser_settings.json"


def default_storage_path() -> str:
    """``<repo>/state/parser_settings.json`` (gitignored runtime state)."""
    root = os.path.abspath(os.path.join(
        os.path.dirname(__file__), "..", "..", ".."))
    return os.path.join(root, "state", _DEFAULT_STORAGE_NAME)


# ----------------------------------------------------------- active config

class CompiledPattern:
    """A validated pattern setting + its compiled regex (read-only)."""

    __slots__ = ("setting", "regex")

    def __init__(self, setting: RegexPatternSetting) -> None:
        self.setting = setting
        self.regex = _validator.compile_pattern(setting)


class EffectiveConfig:
    """Immutable published snapshot (patterns + strings + generation)."""

    __slots__ = ("locale", "generation", "_patterns", "_strings")

    def __init__(self, locale: str, generation: int,
                 patterns: Mapping[str, CompiledPattern],
                 strings: Mapping[str, MessageStringSetting]) -> None:
        self.locale = locale
        self.generation = generation
        self._patterns = MappingProxyType(dict(patterns))
        self._strings = MappingProxyType(dict(strings))

    @property
    def patterns(self) -> Mapping[str, CompiledPattern]:
        return self._patterns

    @property
    def strings(self) -> Mapping[str, MessageStringSetting]:
        return self._strings

    def pattern(self, setting_id: str) -> Optional[CompiledPattern]:
        compiled = self._patterns.get(setting_id)
        if compiled is None or not compiled.setting.enabled:
            return None
        return compiled

    def string(self, setting_id: str) -> Optional[MessageStringSetting]:
        setting = self._strings.get(setting_id)
        if setting is None or not setting.enabled:
            return None
        return setting

    def snapshot_id(self) -> str:
        return f"{self.locale}#{self.generation}"


# ------------------------------------------------------------ API payloads

class PreviewResult:
    """Outcome of a preview (never mutates active or stored state)."""

    def __init__(self, validation: ValidationReport,
                 matches: Optional[List[Dict[str, Any]]] = None,
                 matched: Optional[bool] = None) -> None:
        self.validation = validation
        self.matches = matches or []
        self.matched = matched

    def to_dict(self) -> dict:
        return {
            "validation": self.validation.to_dict(),
            "matches": self.matches,
            "matched": self.matched,
        }


@dataclasses.dataclass(frozen=True)
class SettingSummary:
    """Catalog row: one setting's identity, origin and both values."""

    kind: str                 # "pattern" | "string"
    setting_id: str
    name: str
    description: str
    family: MessageFamily
    target: TargetSource
    locale: str
    enabled: bool
    origin: SettingOrigin
    priority: int
    default_version: int
    effective: str
    default: str
    #: pattern extras
    case_sensitive: Optional[bool] = None
    required_groups: Tuple[str, ...] = ()
    #: string extras
    match_mode: Optional[str] = None

    def to_dict(self) -> dict:
        base = {
            "kind": self.kind, "setting_id": self.setting_id,
            "name": self.name, "description": self.description,
            "family": self.family.value, "target": self.target.value,
            "locale": self.locale, "enabled": self.enabled,
            "origin": self.origin.value, "priority": self.priority,
            "default_version": self.default_version,
            "effective": self.effective, "default": self.default,
        }
        if self.kind == "pattern":
            base["case_sensitive"] = self.case_sensitive
            base["required_groups"] = list(self.required_groups)
        else:
            base["match_mode"] = self.match_mode
        return base


# ----------------------------------------------------------------- manager

class MudaeParserSettingsManager:
    """Loads defaults+overrides, validates, publishes, mutates, saves.

    Usage::

        manager = MudaeParserSettingsManager()   # state/parser_settings.json
        parser = MudaeParser(manager)
        report = manager.validate_pattern_expr(
            "cooldowns.roulette_limit", r"^...(?P<minutes>\\d+)...")
        result = manager.update_pattern("cooldowns.roulette_limit", expr)
        if result.ok: ...
    """

    def __init__(self, storage: Optional[ParserSettingsStorage] = None,
                 locale: str = DEFAULT_LOCALE) -> None:
        self._locale = locale
        self._defaults_patterns = default_patterns(locale)
        self._defaults_strings = default_strings(locale)
        self._storage: ParserSettingsStorage = (
            storage if storage is not None
            else JSONFileSettingsStorage(default_storage_path()))
        self._lock = threading.Lock()
        self._generation = 0
        self._load_diagnostics = Diagnostics()
        #: stored override payloads (the user's requested values)
        self._stored_patterns: Dict[str, Dict[str, Any]] = {}
        self._stored_strings: Dict[str, Dict[str, Any]] = {}
        self._active = self._build_active()

    # ------------------------------------------------------------ loading
    @property
    def locale(self) -> str:
        return self._locale

    @property
    def schema_version(self) -> int:
        return SCHEMA_VERSION

    @property
    def effective_config(self) -> EffectiveConfig:
        """The published snapshot (immutable; swap-on-update)."""
        return self._active

    @property
    def load_diagnostics(self) -> Diagnostics:
        return self._load_diagnostics

    def reload(self) -> Diagnostics:
        """Re-run the load (external file change / fresh storage)."""
        with self._lock:
            self._load_diagnostics = Diagnostics()
            self._active = self._build_active()
            return self._load_diagnostics

    def _build_active(self) -> EffectiveConfig:
        """defaults + stored overrides → validated active snapshot."""
        self._read_stored()
        self._generation += 1
        patterns: Dict[str, CompiledPattern] = {}
        for setting_id, default in self._defaults_patterns.items():
            merged, origin = self._merge_pattern(setting_id, default)
            if origin is SettingOrigin.OVERRIDE:
                report = _validator.validate_pattern(merged)
                if not report.ok:
                    self._load_diagnostics.warning(
                        CODE_REJECTED_OVERRIDE,
                        "stored pattern override rejected; using "
                        "built-in default",
                        subject=setting_id,
                        detail="; ".join(i.message for i in report.issues))
                    merged = default
            patterns[setting_id] = CompiledPattern(merged)
        strings: Dict[str, MessageStringSetting] = {}
        for setting_id, default in self._defaults_strings.items():
            merged, _origin = self._merge_string(setting_id, default)
            report = _validator.validate_string(merged)
            if not report.ok:
                self._load_diagnostics.warning(
                    CODE_REJECTED_OVERRIDE,
                    "stored string override rejected; using built-in "
                    "default",
                    subject=setting_id,
                    detail="; ".join(i.message for i in report.issues))
                merged = default
            strings[setting_id] = merged
        if len(patterns) > _validator.MAX_PATTERNS:
            raise SettingsPersistenceError(  # programmer error, fail loud
                f"locale {self._locale!r} declares {len(patterns)} "
                f"patterns (> {_validator.MAX_PATTERNS} bound)")
        return EffectiveConfig(
            self._locale, self._generation,
            MappingProxyType(patterns), MappingProxyType(strings))

    def _read_stored(self) -> None:
        self._stored_patterns = {}
        self._stored_strings = {}
        try:
            document = self._storage.load()
        except Exception as exc:  # CorruptSettingsError or IO failure
            self._load_diagnostics.warning(
                CODE_CORRUPT_STORE,
                "persisted settings unreadable; starting from defaults",
                detail=str(exc))
            return
        if document is None:
            return
        if document.get("schema_version", SCHEMA_VERSION) < SCHEMA_VERSION:
            self._load_diagnostics.info(
                CODE_OUTDATED_STORE,
                "persisted settings use an older schema version; "
                "loading compatible fields")
        patterns = document.get("patterns") or {}
        strings = document.get("strings") or {}
        known_p = set(self._defaults_patterns)
        known_s = set(self._defaults_strings)
        self._stored_patterns = {
            key: dict(value) for key, value in patterns.items()
            if key in known_p and isinstance(value, dict)}
        self._stored_strings = {
            key: dict(value) for key, value in strings.items()
            if key in known_s and isinstance(value, dict)}
        dropped_p = set(patterns) - set(self._stored_patterns)
        dropped_s = set(strings) - set(self._stored_strings)
        for key in sorted(dropped_p):
            self._load_diagnostics.info(
                CODE_OUTDATED_STORE,
                "stored pattern id is unknown in this build; ignored",
                subject=key)
        for key in sorted(dropped_s):
            self._load_diagnostics.info(
                CODE_OUTDATED_STORE,
                "stored string id is unknown in this build; ignored",
                subject=key)

    def _merge_pattern(self, setting_id: str,
                       default: RegexPatternSetting
                       ) -> Tuple[RegexPatternSetting, SettingOrigin]:
        payload = self._stored_patterns.get(setting_id)
        if payload is None:
            return default, SettingOrigin.BUILTIN
        changes: Dict[str, Any] = {}
        if isinstance(payload.get("expression"), str):
            changes["expression"] = payload["expression"]
        if isinstance(payload.get("case_sensitive"), bool):
            changes["case_sensitive"] = payload["case_sensitive"]
        if isinstance(payload.get("enabled"), bool):
            changes["enabled"] = payload["enabled"]
        return dataclasses.replace(default, **changes), \
            SettingOrigin.OVERRIDE

    def _merge_string(self, setting_id: str,
                      default: MessageStringSetting
                      ) -> Tuple[MessageStringSetting, SettingOrigin]:
        payload = self._stored_strings.get(setting_id)
        if payload is None:
            return default, SettingOrigin.BUILTIN
        changes: Dict[str, Any] = {}
        if isinstance(payload.get("text"), str):
            changes["text"] = payload["text"]
        if isinstance(payload.get("case_sensitive"), bool):
            changes["case_sensitive"] = payload["case_sensitive"]
        if isinstance(payload.get("enabled"), bool):
            changes["enabled"] = payload["enabled"]
        mode = payload.get("match_mode")
        if isinstance(mode, str):
            try:
                from ..enums import MatchMode
                changes["match_mode"] = MatchMode(mode)
            except ValueError:
                self._load_diagnostics.warning(
                    CODE_REJECTED_OVERRIDE,
                    "stored match_mode invalid; keeping default mode",
                    subject=setting_id, detail=repr(mode))
        return dataclasses.replace(default, **changes), \
            SettingOrigin.OVERRIDE

    # ------------------------------------------------------- read-side API
    def list_patterns(self, family: Optional[MessageFamily] = None
                      ) -> Tuple[RegexPatternSetting, ...]:
        """Effective pattern settings (stable ID sort)."""
        out = [compiled.setting for compiled in
               self._active.patterns.values()
               if family is None or compiled.setting.family is family]
        return tuple(sorted(out, key=lambda s: s.setting_id))

    def list_strings(self, family: Optional[MessageFamily] = None
                     ) -> Tuple[MessageStringSetting, ...]:
        out = [setting for setting in self._active.strings.values()
               if family is None or setting.family is family]
        return tuple(sorted(out, key=lambda s: s.setting_id))

    def get_pattern(self, setting_id: str) -> RegexPatternSetting:
        compiled = self._active.patterns.get(setting_id)
        if compiled is None:
            raise UnknownSettingError(
                f"no pattern setting {setting_id!r}")
        return compiled.setting

    def get_pattern_default(self, setting_id: str) -> RegexPatternSetting:
        default = self._defaults_patterns.get(setting_id)
        if default is None:
            raise UnknownSettingError(
                f"no pattern setting {setting_id!r}")
        return default

    def get_string(self, setting_id: str) -> MessageStringSetting:
        setting = self._active.strings.get(setting_id)
        if setting is None:
            raise UnknownSettingError(
                f"no message-string setting {setting_id!r}")
        return setting

    def get_string_default(self, setting_id: str) -> MessageStringSetting:
        default = self._defaults_strings.get(setting_id)
        if default is None:
            raise UnknownSettingError(
                f"no message-string setting {setting_id!r}")
        return default

    def has_override(self, setting_id: str) -> bool:
        return (setting_id in self._stored_patterns
                or setting_id in self._stored_strings)

    def origin(self, setting_id: str) -> SettingOrigin:
        if setting_id in self._stored_patterns or \
                setting_id in self._stored_strings:
            return SettingOrigin.OVERRIDE
        if setting_id in self._defaults_patterns or \
                setting_id in self._defaults_strings:
            return SettingOrigin.BUILTIN
        raise UnknownSettingError(f"no setting {setting_id!r}")

    def catalog(self) -> Dict[str, List[dict]]:
        """Grouped, JSON-ready catalog for the future dashboard."""
        groups: Dict[str, List[dict]] = {}
        for setting in self.list_patterns():
            summary = SettingSummary(
                kind="pattern", setting_id=setting.setting_id,
                name=setting.name, description=setting.description,
                family=setting.family, target=setting.target,
                locale=setting.locale, enabled=setting.enabled,
                origin=self.origin(setting.setting_id),
                priority=setting.priority,
                default_version=setting.default_version,
                effective=setting.expression,
                default=self.get_pattern_default(
                    setting.setting_id).expression,
                case_sensitive=setting.case_sensitive,
                required_groups=setting.required_groups)
            groups.setdefault(setting.family.value, []).append(
                summary.to_dict())
        for setting in self.list_strings():
            summary = SettingSummary(
                kind="string", setting_id=setting.setting_id,
                name=setting.name, description=setting.description,
                family=setting.family, target=setting.target,
                locale=setting.locale, enabled=setting.enabled,
                origin=self.origin(setting.setting_id),
                priority=setting.priority,
                default_version=setting.default_version,
                effective=setting.text,
                default=self.get_string_default(
                    setting.setting_id).text,
                case_sensitive=setting.case_sensitive,
                match_mode=setting.match_mode.value)
            groups.setdefault(setting.family.value, []).append(
                summary.to_dict())
        return groups

    @staticmethod
    def supported_targets() -> Tuple[TargetSource, ...]:
        return tuple(TargetSource)

    def supported_families(self) -> Tuple[MessageFamily, ...]:
        families = {s.family for s in self.list_patterns()}
        families.update(s.family for s in self.list_strings())
        return tuple(sorted(families, key=lambda f: f.value))

    def available_locales(self) -> Tuple[str, ...]:
        from ..locales import available_locales
        return available_locales()

    # ----------------------------------------------------- write-side API
    def validate_pattern_expr(self, setting_id: str, expression: str,
                              case_sensitive: Optional[bool] = None
                              ) -> ValidationReport:
        """Validate a candidate WITHOUT activating it."""
        try:
            current = self.get_pattern(setting_id)
        except UnknownSettingError:
            return ValidationReport(ok=False, issues=(
                ValidationIssue("unknown_setting",
                                f"no pattern setting {setting_id!r}"),))
        candidate = dataclasses.replace(
            current, expression=expression,
            case_sensitive=(case_sensitive if case_sensitive is not None
                            else current.case_sensitive))
        return _validator.validate_pattern(candidate)

    def validate_string_text(self, setting_id: str, text: str,
                             match_mode: Optional[str] = None,
                             case_sensitive: Optional[bool] = None
                             ) -> ValidationReport:
        from ..enums import MatchMode

        try:
            current = self.get_string(setting_id)
        except UnknownSettingError:
            return ValidationReport(ok=False, issues=(
                ValidationIssue("unknown_setting",
                                f"no message-string setting "
                                f"{setting_id!r}"),))
        mode = current.match_mode
        if match_mode is not None:
            try:
                mode = MatchMode(match_mode)
            except ValueError:
                return ValidationReport(ok=False, issues=(
                    ValidationIssue(
                        "invalid_match_mode",
                        f"unknown match mode {match_mode!r}"),))
        candidate = dataclasses.replace(
            current, text=text, match_mode=mode,
            case_sensitive=(case_sensitive if case_sensitive is not None
                            else current.case_sensitive))
        return _validator.validate_string(candidate)

    def update_pattern(self, setting_id: str,
                       expression: Optional[str] = None,
                       case_sensitive: Optional[bool] = None
                       ) -> UpdateResult:
        """Apply a valid pattern override atomically (or reject)."""
        if expression is None and case_sensitive is None:
            return UpdateResult(
                ok=False, setting_id=setting_id,
                errors=(ValidationIssue(
                    "empty_update", "nothing to update"),))
        current = self.get_pattern(setting_id)  # raises on unknown id
        changes: Dict[str, Any] = {}
        if expression is not None:
            changes["expression"] = expression
        if case_sensitive is not None:
            changes["case_sensitive"] = case_sensitive
        candidate = dataclasses.replace(current, **changes)
        report = _validator.validate_pattern(candidate)
        if not report.ok:
            return UpdateResult(
                ok=False, setting_id=setting_id,
                errors=tuple(report.issues))
        stored_payload = dict(self._stored_patterns.get(setting_id, {}))
        stored_payload.update(changes)
        return self._commit_pattern(setting_id, candidate, stored_payload)

    def set_pattern_enabled(self, setting_id: str, enabled: bool
                            ) -> UpdateResult:
        current = self.get_pattern(setting_id)
        candidate = dataclasses.replace(current, enabled=enabled)
        stored_payload = dict(self._stored_patterns.get(setting_id, {}))
        stored_payload["enabled"] = enabled
        return self._commit_pattern(setting_id, candidate, stored_payload)

    def reset_pattern(self, setting_id: str) -> UpdateResult:
        """Drop the override; restore the current built-in default."""
        default = self.get_pattern_default(setting_id)
        if setting_id not in self._stored_patterns:
            return UpdateResult(ok=True, setting_id=setting_id)
        return self._commit_pattern(
            setting_id, default, None, drop_stored="patterns")

    def update_string(self, setting_id: str,
                      text: Optional[str] = None,
                      match_mode: Optional[str] = None,
                      case_sensitive: Optional[bool] = None
                      ) -> UpdateResult:
        from ..enums import MatchMode

        if text is None and match_mode is None and case_sensitive is None:
            return UpdateResult(
                ok=False, setting_id=setting_id,
                errors=(ValidationIssue(
                    "empty_update", "nothing to update"),))
        current = self.get_string(setting_id)
        changes: Dict[str, Any] = {}
        if text is not None:
            changes["text"] = text
        if case_sensitive is not None:
            changes["case_sensitive"] = case_sensitive
        if match_mode is not None:
            try:
                changes["match_mode"] = MatchMode(match_mode)
            except ValueError:
                return UpdateResult(
                    ok=False, setting_id=setting_id,
                    errors=(ValidationIssue(
                        "invalid_match_mode",
                        f"unknown match mode {match_mode!r}"),))
        candidate = dataclasses.replace(current, **changes)
        report = _validator.validate_string(candidate)
        if not report.ok:
            return UpdateResult(
                ok=False, setting_id=setting_id,
                errors=tuple(report.issues))
        stored_payload = dict(self._stored_strings.get(setting_id, {}))
        stored_payload.update(changes)
        if isinstance(stored_payload.get("match_mode"), MatchMode):
            stored_payload["match_mode"] = \
                stored_payload["match_mode"].value
        return self._commit_string(setting_id, candidate, stored_payload)

    def set_string_enabled(self, setting_id: str, enabled: bool
                           ) -> UpdateResult:
        current = self.get_string(setting_id)
        candidate = dataclasses.replace(current, enabled=enabled)
        stored_payload = dict(self._stored_strings.get(setting_id, {}))
        stored_payload["enabled"] = enabled
        return self._commit_string(setting_id, candidate, stored_payload)

    def reset_string(self, setting_id: str) -> UpdateResult:
        default = self.get_string_default(setting_id)
        if setting_id not in self._stored_strings:
            return UpdateResult(ok=True, setting_id=setting_id)
        return self._commit_string(
            setting_id, default, None, drop_stored="strings")

    # ------------------------------------------------------------- preview
    def preview_pattern(self, setting_id: str, sample: str,
                        expression: Optional[str] = None,
                        case_sensitive: Optional[bool] = None
                        ) -> PreviewResult:
        """Dry-run a pattern (candidate or current) on a sample text.

        Never mutates active or stored state."""
        current = self.get_pattern(setting_id)
        candidate_expr = expression if expression is not None \
            else current.expression
        candidate_cs = case_sensitive if case_sensitive is not None \
            else current.case_sensitive
        report = self.validate_pattern_expr(
            setting_id, candidate_expr, candidate_cs)
        if not report.ok:
            return PreviewResult(report)
        flags = 0 if candidate_cs else re.IGNORECASE
        try:
            regex = re.compile(candidate_expr, flags)
        except re.error as exc:  # pragma: no cover - guarded above
            return PreviewResult(ValidationReport(ok=False, issues=(
                ValidationIssue("invalid_syntax", str(exc)),)))
        matches: List[Dict[str, Any]] = []
        for match in regex.finditer(sample or ""):
            groups = {
                name: value for name, value
                in match.groupdict().items() if value is not None}
            groups["_span"] = list(match.span())
            matches.append(groups)
        return PreviewResult(report, matches=matches)

    def preview_string(self, setting_id: str, sample: str,
                       text: Optional[str] = None,
                       match_mode: Optional[str] = None,
                       case_sensitive: Optional[bool] = None
                       ) -> PreviewResult:
        """Dry-run a message string (candidate or current) on a
        sample. Never mutates active or stored state."""
        current = self.get_string(setting_id)
        candidate_text = text if text is not None else current.text
        candidate_mode = current.match_mode
        if match_mode is not None:
            from ..enums import MatchMode

            try:
                candidate_mode = MatchMode(match_mode)
            except ValueError:
                return PreviewResult(ValidationReport(
                    ok=False, issues=(ValidationIssue(
                        "invalid_match_mode",
                        f"unknown match mode {match_mode!r}"),)))
        candidate_cs = case_sensitive if case_sensitive is not None \
            else current.case_sensitive
        candidate = dataclasses.replace(
            current, text=candidate_text, match_mode=candidate_mode,
            case_sensitive=candidate_cs)
        report = _validator.validate_string(candidate)
        if not report.ok:
            return PreviewResult(report)
        matched = _validator.matches_string(candidate, sample or "")
        return PreviewResult(report, matched=matched)

    # ------------------------------------------------------------ commits
    def _commit_pattern(self, setting_id: str,
                        candidate: RegexPatternSetting,
                        stored_payload: Optional[Dict[str, Any]],
                        drop_stored: Optional[str] = None
                        ) -> UpdateResult:
        return self._commit(
            "patterns", setting_id, CompiledPattern(candidate),
            stored_payload, drop_stored)

    def _commit_string(self, setting_id: str,
                       candidate: MessageStringSetting,
                       stored_payload: Optional[Dict[str, Any]],
                       drop_stored: Optional[str] = None
                       ) -> UpdateResult:
        return self._commit(
            "strings", setting_id, candidate, stored_payload, drop_stored)

    def _commit(self, kind: str, setting_id: str,
                active_value: Any,
                stored_payload: Optional[Dict[str, Any]],
                drop_stored: Optional[str] = None) -> UpdateResult:
        """The transaction: validate already done → persist → publish.

        On any failure the manager state is EXACTLY as before (the
        candidate maps and stored docs are deep copies).
        """
        with self._lock:
            stored_patterns = copy.deepcopy(self._stored_patterns)
            stored_strings = copy.deepcopy(self._stored_strings)
            stored_map = {"patterns": stored_patterns,
                          "strings": stored_strings}
            if drop_stored == "patterns":
                stored_patterns.pop(setting_id, None)
            elif drop_stored == "strings":
                stored_strings.pop(setting_id, None)
            elif stored_payload is not None:
                if kind == "patterns":
                    stored_patterns[setting_id] = stored_payload
                else:
                    stored_strings[setting_id] = stored_payload
            try:
                self._storage.save({
                    "schema_version": SCHEMA_VERSION,
                    "locale": self._locale,
                    "patterns": stored_patterns,
                    "strings": stored_strings,
                })
            except Exception as exc:
                return UpdateResult(
                    ok=False, setting_id=setting_id,
                    errors=(ValidationIssue(
                        "persistence_failed",
                        f"could not persist settings: {exc}"),),
                    notes=("active configuration and stored overrides "
                           "left unchanged"))
            # persist succeeded → publish new snapshot atomically
            self._stored_patterns = stored_patterns
            self._stored_strings = stored_strings
            if kind == "patterns":
                merged = dict(self._active.patterns)
                merged[setting_id] = active_value
                self._generation += 1
                self._active = EffectiveConfig(
                    self._locale, self._generation,
                    MappingProxyType(merged),
                    MappingProxyType(dict(self._active.strings)))
            else:
                merged = dict(self._active.strings)
                merged[setting_id] = active_value
                self._generation += 1
                self._active = EffectiveConfig(
                    self._locale, self._generation,
                    MappingProxyType(dict(self._active.patterns)),
                    MappingProxyType(merged))
            return UpdateResult(ok=True, setting_id=setting_id)
