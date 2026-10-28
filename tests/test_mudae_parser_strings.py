"""Message-string configuration tests (Feature B): literal match
modes, case sensitivity, raw-string preservation, edit/enable/reset,
literal-vs-regex separation, and preview side-effect freedom."""
from __future__ import annotations

import os
import tempfile
import unittest

from autoclaimer.mudae_parser import MudaeParser, MudaeParserSettingsManager
from autoclaimer.mudae_parser.enums import SettingOrigin
from autoclaimer.mudae_parser.settings.storage import (
    JSONFileSettingsStorage,
    MemorySettingsStorage,
)

RT_AVAILABLE = "cooldowns.rt_available"
ROULETTE_MARKER = "cooldowns.roulette_marker"
CLAIM_MARKER = "cooldowns.claim_ready_marker"


class LiteralMatchingTests(unittest.TestCase):
    """Exact/contains/prefix semantics + case rules, through the
    manager API and through parsing."""

    def setUp(self):
        self.manager = MudaeParserSettingsManager(
            storage=MemorySettingsStorage())

    def test_exact_mode(self):
        setting = self.manager.get_string(RT_AVAILABLE)
        self.assertEqual(setting.match_mode.value, "exact")
        preview = self.manager.preview_string(
            RT_AVAILABLE, "$rt is available!")
        self.assertTrue(preview.validation.ok)
        self.assertTrue(preview.matched)
        # near-miss must not match exactly
        preview = self.manager.preview_string(
            RT_AVAILABLE, "$rt is available! extra text")
        self.assertFalse(preview.matched)

    def test_contains_mode(self):
        preview = self.manager.preview_string(
            ROULETTE_MARKER,
            "**user**, the roulette is limited to 9 uses per hour.")
        self.assertTrue(preview.matched)

    def test_prefix_mode(self):
        setting = self.manager.get_string("characters.owned_marker")
        self.assertEqual(setting.match_mode.value, "prefix")
        preview = self.manager.preview_string(
            "characters.owned_marker", "Belongs to joon0520")
        self.assertTrue(preview.matched)
        preview = self.manager.preview_string(
            "characters.owned_marker", "xxBelongs to joon0520")
        self.assertFalse(preview.matched)

    def test_case_sensitive_default_blocks_case_mismatch(self):
        preview = self.manager.preview_string(
            ROULETTE_MARKER, "The Roulette Is Limited To 9 uses")
        self.assertFalse(preview.matched)  # default is case-sensitive

    def test_case_insensitive_override_matches_without_mutation(self):
        result = self.manager.update_string(
            ROULETTE_MARKER, case_sensitive=False)
        self.assertTrue(result.ok, result.to_dict())
        preview = self.manager.preview_string(
            ROULETTE_MARKER, "The Roulette Is Limited To 9 uses")
        self.assertTrue(preview.matched)
        # the STORED raw string was never transformed
        self.assertEqual(
            self.manager.get_string(ROULETTE_MARKER).text,
            self.manager.get_string_default(ROULETTE_MARKER).text)

    def test_extraction_not_altered_by_case_insensitivity(self):
        """An insensitive marker must not rewrite extracted names."""
        parser = MudaeParser(self.manager)
        self.manager.update_string(ROULETTE_MARKER,
                                   case_sensitive=False)
        result = parser.parse(
            "**Emporos21**, the roulette is limited to 9 uses per hour. "
            "**3** min left.")
        self.assertEqual(result.cooldowns[0].username, "Emporos21")

    def test_multiline_message_matched_via_contains(self):
        text = ("Some leading line\nthe roulette is limited to 9 uses "
                "per hour.\nmore lines")
        preview = self.manager.preview_string(ROULETTE_MARKER, text)
        self.assertTrue(preview.matched)

    def test_literal_never_interpreted_as_regex(self):
        """A string containing regex metacharacters stays literal."""
        marker = "$rt is available!"   # $ is a regex anchor!
        preview = self.manager.preview_string(
            RT_AVAILABLE, marker)
        self.assertTrue(preview.matched)               # literal equal
        preview = self.manager.preview_string(
            RT_AVAILABLE, "X" + marker)                # would match if
        self.assertFalse(preview.matched)              # treated as rx

    def test_unicode_emoji_punctuation_preserved(self):
        setting = self.manager.get_string(CLAIM_MARKER)
        self.assertEqual(setting.text, "__can__ claim right now!")
        raw = self.manager.get_string_default(CLAIM_MARKER).text
        self.assertEqual(setting.text, raw)  # verbatim, unmodified


class StringEditingTests(unittest.TestCase):
    def setUp(self):
        self.manager = MudaeParserSettingsManager(
            storage=MemorySettingsStorage())

    def test_update_text_and_modes(self):
        result = self.manager.update_string(
            ROULETTE_MARKER, text="roulette is limited",
            match_mode="prefix")
        self.assertTrue(result.ok, result.to_dict())
        setting = self.manager.get_string(ROULETTE_MARKER)
        self.assertEqual(setting.text, "roulette is limited")
        self.assertEqual(setting.match_mode.value, "prefix")
        self.assertTrue(self.manager.has_override(ROULETTE_MARKER))
        self.assertIs(self.manager.origin(ROULETTE_MARKER),
                      SettingOrigin.OVERRIDE)

    def test_invalid_match_mode_rejected(self):
        result = self.manager.update_string(
            ROULETTE_MARKER, match_mode="suffix")
        self.assertFalse(result.ok)

    def test_empty_text_rejected(self):
        result = self.manager.update_string(ROULETTE_MARKER, text=" ")
        self.assertFalse(result.ok)
        self.assertFalse(self.manager.has_override(ROULETTE_MARKER))

    def test_string_length_bound(self):
        result = self.manager.update_string(
            ROULETTE_MARKER, text="x" * 3000)
        self.assertFalse(result.ok)

    def test_enable_disable_affects_parsing(self):
        parser = MudaeParser(self.manager)
        text = ("**u**, the roulette is limited to 9 uses per hour. "
                "**3** min left.")
        from autoclaimer.mudae_parser.enums import MessageFamily

        self.assertIs(parser.parse(text).family,
                      MessageFamily.COOLDOWN)
        self.manager.set_string_enabled(ROULETTE_MARKER, False)
        self.assertIs(parser.parse(text).family,
                      MessageFamily.UNKNOWN)
        self.manager.set_string_enabled(ROULETTE_MARKER, True)
        self.assertIs(parser.parse(text).family,
                      MessageFamily.COOLDOWN)

    def test_reset_restores_default(self):
        self.manager.update_string(ROULETTE_MARKER,
                                   text="changed", match_mode="prefix")
        result = self.manager.reset_string(ROULETTE_MARKER)
        self.assertTrue(result.ok)
        self.assertFalse(self.manager.has_override(ROULETTE_MARKER))
        default = self.manager.get_string_default(ROULETTE_MARKER)
        current = self.manager.get_string(ROULETTE_MARKER)
        self.assertEqual(current.text, default.text)
        self.assertEqual(current.match_mode, default.match_mode)

    def test_preview_has_no_side_effects(self):
        preview = self.manager.preview_string(
            ROULETTE_MARKER, "irrelevant sample", text="temp change",
            match_mode="exact")
        self.assertTrue(preview.validation.ok)
        self.assertFalse(self.manager.has_override(ROULETTE_MARKER))
        self.assertEqual(
            self.manager.get_string(ROULETTE_MARKER).text,
            self.manager.get_string_default(ROULETTE_MARKER).text)

    def test_invalid_stored_string_falls_back_at_load(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, "settings.json")
            import json

            with open(path, "w", encoding="utf-8") as handle:
                json.dump({
                    "schema_version": 1,
                    "patterns": {},
                    "strings": {ROULETTE_MARKER: {"text": "   "}},
                }, handle)
            manager = MudaeParserSettingsManager(
                storage=JSONFileSettingsStorage(path))
            self.assertTrue(manager.load_diagnostics.has(
                "rejected_override"))
            self.assertEqual(
                manager.get_string(ROULETTE_MARKER).text,
                manager.get_string_default(ROULETTE_MARKER).text)

    def test_persistence_failure_leaves_everything_intact(self):
        storage = MemorySettingsStorage()
        storage.fail_save = True
        manager = MudaeParserSettingsManager(storage=storage)
        result = manager.update_string(ROULETTE_MARKER, text="xyz")
        self.assertFalse(result.ok)
        self.assertFalse(manager.has_override(ROULETTE_MARKER))
        self.assertEqual(
            manager.get_string(ROULETTE_MARKER).text,
            manager.get_string_default(ROULETTE_MARKER).text)


if __name__ == "__main__":
    unittest.main()
