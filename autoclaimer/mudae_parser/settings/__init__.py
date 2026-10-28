"""Settings layer: configuration models, defaults, validation,
management and persistence for the Mudae parser engine."""
from __future__ import annotations

from .manager import (
    CompiledPattern,
    EffectiveConfig,
    MudaeParserSettingsManager,
    PreviewResult,
    SettingSummary,
    default_storage_path,
)
from .models import (
    MessageStringSetting,
    RegexPatternSetting,
    SettingBase,
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

__all__ = [
    "MudaeParserSettingsManager", "EffectiveConfig", "CompiledPattern",
    "PreviewResult", "SettingSummary", "default_storage_path",
    "RegexPatternSetting", "MessageStringSetting", "SettingBase",
    "ValidationReport", "ValidationIssue", "UpdateResult",
    "ParserSettingsStorage", "JSONFileSettingsStorage",
    "MemorySettingsStorage", "SCHEMA_VERSION",
]
