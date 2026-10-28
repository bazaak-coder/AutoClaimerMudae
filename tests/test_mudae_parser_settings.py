"""Regex pattern configuration tests (Feature A): listing, updating,
validation, capture-group contracts, reset, atomicity, rollback,
persistence, and snapshot consistency."""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from autoclaimer.mudae_parser import (
    MessageFamily,
    MudaeParser,
    MudaeParserSettingsManager,
)
from autoclaimer.mudae_parser.enums import SettingOrigin, TargetSource
from autoclaimer.mudae_parser.exceptions import (
    CorruptSettingsError,
    UnknownSettingError,
)
from autoclaimer.mudae_parser.settings.storage import (
    JSONFileSettingsStorage,
    MemorySettingsStorage,
)

ROULETTE = "cooldowns.roulette_limit"
ROULETTE_MARKER = "cooldowns.roulette_marker"
#: a VALID alternative expression (keeps every required capture group)
VALID_ALT = (r"^\*\*(?P<username>[^*\n]+)\*\*, the roulette is "
             r"limited to (?P<limit>\d+) uses per hour\.\s*"
             r"\*\*(?P<minutes>\d+)\*\* min left\.")


def sample_roulette_message() -> str:
    return ("**emporos21**, the roulette is limited to 107 uses per "
            "hour. **16** min left.")


class CatalogTests(unittest.TestCase):
    def setUp(self):
        self.manager = MudaeParserSettingsManager(
            storage=MemorySettingsStorage())

    def test_list_patterns_grouped(self):
        patterns = self.manager.list_patterns()
        self.assertTrue(patterns)
        ids = {p.setting_id for p in patterns}
        self.assertIn(ROULETTE, ids)
        self.assertIn("characters.name", ids)
        by_family = self.manager.list_patterns(
            MessageFamily.COOLDOWN)
        self.assertTrue(all(
            p.family is MessageFamily.COOLDOWN for p in by_family))

    def test_get_pattern_and_default(self):
        pattern = self.manager.get_pattern(ROULETTE)
        self.assertEqual(pattern.setting_id, ROULETTE)
        default = self.manager.get_pattern_default(ROULETTE)
        self.assertEqual(pattern.expression, default.expression)
        self.assertFalse(self.manager.has_override(ROULETTE))
        self.assertIs(self.manager.origin(ROULETTE),
                      SettingOrigin.BUILTIN)

    def test_unknown_id_raises(self):
        with self.assertRaises(UnknownSettingError):
            self.manager.get_pattern("nope.does_not_exist")

    def test_supported_targets_and_families(self):
        targets = self.manager.supported_targets()
        self.assertIn(TargetSource.EMBED_DESCRIPTION, targets)
        families = self.manager.supported_families()
        self.assertIn(MessageFamily.CHARACTER, families)

    def test_catalog_json_ready(self):
        catalog = self.manager.catalog()
        self.assertIn("cooldown", catalog)
        entry = [row for row in catalog["cooldown"]
                 if row["setting_id"] == ROULETTE][0]
        self.assertEqual(entry["kind"], "pattern")
        self.assertEqual(entry["origin"], "builtin")
        self.assertIn("required_groups", entry)


class UpdateValidationTests(unittest.TestCase):
    def setUp(self):
        self.storage = MemorySettingsStorage()
        self.manager = MudaeParserSettingsManager(storage=self.storage)

    def test_valid_override_applies_and_parses(self):
        new_expr = (r"^\*\*(?P<username>[^*\n]+)\*\*, the roulette is "
                    r"limited to (?P<limit>\d+) uses per hour\.\s*"
                    r"\*\*(?P<minutes>\d+)\*\* min left\.")
        result = self.manager.update_pattern(
            ROULETTE, expression=new_expr, case_sensitive=False)
        self.assertTrue(result.ok, result.to_dict())
        self.assertTrue(self.manager.has_override(ROULETTE))
        self.assertIs(self.manager.origin(ROULETTE),
                      SettingOrigin.OVERRIDE)
        # semantics preserved: still parses the sample
        parse = MudaeParser(self.manager).parse(sample_roulette_message())
        self.assertEqual(parse.cooldowns[0].remaining.total_seconds(),
                         960)

    def test_invalid_syntax_rejected_and_active_unchanged(self):
        before = self.manager.get_pattern(ROULETTE).expression
        result = self.manager.update_pattern(
            ROULETTE, expression=r"^(?P<username>[unclosed")
        self.assertFalse(result.ok)
        codes = {issue.code for issue in result.errors}
        self.assertIn("invalid_syntax", codes)
        self.assertEqual(self.manager.get_pattern(ROULETTE).expression,
                         before)
        self.assertFalse(self.manager.has_override(ROULETTE))

    def test_missing_required_group_rejected(self):
        result = self.manager.update_pattern(
            ROULETTE, expression=r"^\*\*(?P<username>[^*]+)\*\*, "
                                  r"roulette limited")  # no minutes group
        self.assertFalse(result.ok)
        codes = {issue.code for issue in result.errors}
        self.assertIn("missing_required_group", codes)

    def test_case_insensitive_override_still_extracts_case(self):
        """Insensitive MATCHING never mutates extracted names."""
        expr = (r"^\*\*(?P<username>[^*\n]+)\*\*, THE ROULETTE IS "
                r"limited to (?P<limit>\d+) uses per hour\.\s*"
                r"\*\*(?P<minutes>\d+)\*\* min left\.")
        # pattern AND its literal marker gate go insensitive together
        self.assertTrue(self.manager.update_pattern(
            ROULETTE, expression=expr, case_sensitive=False).ok)
        self.assertTrue(self.manager.update_string(
            ROULETTE_MARKER, case_sensitive=False).ok)
        text = ("**Emporos21**, THE ROULETTE IS limited to 9 uses per "
                "hour. **3** min left.")
        parse = MudaeParser(self.manager).parse(text)
        self.assertEqual(parse.cooldowns[0].username, "Emporos21")

    def test_expression_length_bound(self):
        result = self.manager.update_pattern(
            ROULETTE, expression=r"(?P<minutes>\d+)" + "a" * 3000)
        self.assertFalse(result.ok)
        codes = {issue.code for issue in result.errors}
        self.assertIn("expression_too_long", codes)

    def test_nested_quantifier_rejected(self):
        result = self.manager.update_pattern(
            ROULETTE, expression=r"^(a+)+(?P<minutes>\d+)$")
        self.assertFalse(result.ok)
        codes = {issue.code for issue in result.errors}
        self.assertIn("unsafe_construct", codes)

    def test_empty_expression_rejected(self):
        result = self.manager.update_pattern(ROULETTE, expression="  ")
        self.assertFalse(result.ok)

    def test_validate_without_activating(self):
        report = self.manager.validate_pattern_expr(
            ROULETTE, r"^(?P<username>x)$")
        self.assertFalse(report.ok)
        # active config untouched by the failed validation
        self.assertFalse(self.manager.has_override(ROULETTE))

    def test_enable_disable(self):
        parse_text = sample_roulette_message()
        parser = MudaeParser(self.manager)
        self.assertIs(parser.parse(parse_text).family,
                      MessageFamily.COOLDOWN)
        result = self.manager.set_pattern_enabled(ROULETTE, False)
        self.assertTrue(result.ok)
        self.assertIs(parser.parse(parse_text).family,
                      MessageFamily.UNKNOWN)
        self.manager.set_pattern_enabled(ROULETTE, True)
        self.assertIs(parser.parse(parse_text).family,
                      MessageFamily.COOLDOWN)

    def test_reset_restores_default(self):
        self.manager.update_pattern(ROULETTE, expression=r"x "
                                                         r"(?P<minutes>\d+)")
        result = self.manager.reset_pattern(ROULETTE)
        self.assertTrue(result.ok)
        self.assertFalse(self.manager.has_override(ROULETTE))
        self.assertEqual(
            self.manager.get_pattern(ROULETTE).expression,
            self.manager.get_pattern_default(ROULETTE).expression)

    def test_preview_no_side_effects(self):
        preview = self.manager.preview_pattern(
            ROULETTE, sample_roulette_message(),
            expression=VALID_ALT.replace("107", r"(?P<limit>\d+)"))
        self.assertTrue(preview.validation.ok,
                        preview.validation.to_dict())
        self.assertEqual(preview.matches[0]["limit"], "107")
        self.assertFalse(self.manager.has_override(ROULETTE))


class PersistenceAndRollbackTests(unittest.TestCase):
    def test_persistence_failure_rolls_back(self):
        storage = MemorySettingsStorage()
        storage.fail_save = True
        manager = MudaeParserSettingsManager(storage=storage)
        before = manager.get_pattern(ROULETTE).expression
        result = manager.update_pattern(ROULETTE, expression=VALID_ALT)
        self.assertFalse(result.ok)
        codes = {issue.code for issue in result.errors}
        self.assertIn("persistence_failed", codes)
        # nothing changed: active + stored intact
        self.assertEqual(manager.get_pattern(ROULETTE).expression, before)
        self.assertFalse(manager.has_override(ROULETTE))

    def test_json_storage_roundtrip(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "parser_settings.json")
            first = MudaeParserSettingsManager(
                storage=JSONFileSettingsStorage(path))
            result = first.update_pattern(ROULETTE,
                                          expression=VALID_ALT)
            self.assertTrue(result.ok, result.to_dict())
            second = MudaeParserSettingsManager(
                storage=JSONFileSettingsStorage(path))
            self.assertTrue(second.has_override(ROULETTE))
            self.assertEqual(
                second.get_pattern(ROULETTE).expression, VALID_ALT)
            # schema version present in the document
            with open(path, encoding="utf-8") as handle:
                document = json.load(handle)
            self.assertIn("schema_version", document)

    def test_corrupt_file_falls_back_to_defaults(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "parser_settings.json")
            with open(path, "w", encoding="utf-8") as handle:
                handle.write("{not json at all")
            storage = JSONFileSettingsStorage(path)
            manager = MudaeParserSettingsManager(storage=storage)
            # defaults active, diagnostic recorded, parser functional
            self.assertFalse(manager.has_override(ROULETTE))
            self.assertTrue(manager.load_diagnostics.has(
                "corrupt_store"))
            parse = MudaeParser(manager).parse(sample_roulette_message())
            self.assertIs(parse.family, MessageFamily.COOLDOWN)

    def test_future_schema_version_treated_as_absent(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "parser_settings.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({"schema_version": 999, "patterns": {},
                           "strings": {}}, handle)
            manager = MudaeParserSettingsManager(
                storage=JSONFileSettingsStorage(path))
            self.assertFalse(manager.has_override(ROULETTE))

    def test_stored_invalid_override_rejected_at_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "parser_settings.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({
                    "schema_version": 1,
                    "patterns": {ROULETTE: {
                        "expression": "^(?P<unclosed>["}},
                    "strings": {},
                }, handle)
            manager = MudaeParserSettingsManager(
                storage=JSONFileSettingsStorage(path))
            self.assertTrue(manager.load_diagnostics.has(
                "rejected_override"))
            # default active, parse works
            parse = MudaeParser(manager).parse(sample_roulette_message())
            self.assertIs(parse.family, MessageFamily.COOLDOWN)

    def test_unknown_stored_ids_ignored(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "parser_settings.json")
            with open(path, "w", encoding="utf-8") as handle:
                json.dump({
                    "schema_version": 1,
                    "patterns": {"gone.pattern": {"expression": "x"}},
                    "strings": {"gone.string": {"text": "y"}},
                }, handle)
            manager = MudaeParserSettingsManager(
                storage=JSONFileSettingsStorage(path))
            self.assertTrue(manager.load_diagnostics.has(
                "outdated_store"))


class SnapshotConsistencyTests(unittest.TestCase):
    def test_parse_uses_one_snapshot_across_update(self):
        manager = MudaeParserSettingsManager(
            storage=MemorySettingsStorage())
        parser = MudaeParser(manager)
        text = sample_roulette_message()
        snapshot = parser._snapshot()
        # mutate mid-flight; the held snapshot must stay coherent
        manager.set_pattern_enabled(ROULETTE, False)
        result = parser.parse(text, _snapshot=snapshot)
        self.assertIs(result.family, MessageFamily.COOLDOWN)
        # a fresh parse sees the new config
        self.assertIs(parser.parse(text).family, MessageFamily.UNKNOWN)

    def test_generation_advances_on_update(self):
        manager = MudaeParserSettingsManager(
            storage=MemorySettingsStorage())
        generation = manager.effective_config.generation
        manager.set_pattern_enabled(ROULETTE, False)
        self.assertGreater(manager.effective_config.generation,
                           generation)


if __name__ == "__main__":
    unittest.main()
