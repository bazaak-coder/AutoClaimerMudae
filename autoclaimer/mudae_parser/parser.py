"""``MudaeParser`` — the public entry point of the Mudae parsing engine.

    from parsers.mudae import MudaeParser, MudaeParserSettingsManager

    manager = MudaeParserSettingsManager()      # defaults, no file needed
    parser = MudaeParser(manager)
    result = parser.parse(message, account_username="nick")

One parse = one immutable configuration snapshot (grabbed before any
rule runs), so a concurrent settings update can never split a message
across old and new rules. Unknown messages return an UNKNOWN-family
result with diagnostics — expected nonmatches never raise. Programmer
errors (unusable input) raise :class:`InvalidMessageError`.
"""
from __future__ import annotations

from typing import Any, Iterable, List, Optional, Sequence, Tuple

from .diagnostics import (
    CODE_AMBIGUOUS_MATCH,
    CODE_DISABLED_RULE,
    CODE_NO_MATCH,
    CODE_UNKNOWN_FORMAT,
    Diagnostics,
)
from .enums import MessageFamily
from .exceptions import InvalidMessageError
from .models import ParseResult
from .normalization import NormalizedMessage, TextSource, normalize_message
from .registry import DEFAULT_RULES
from .rules.base import Rule, RuleContext, RuleResult
from .rules.usernames import USERNAME_PATTERN_IDS, extract_usernames
from .settings.manager import (
    EffectiveConfig,
    MudaeParserSettingsManager,
)

__all__ = ["MudaeParser"]

#: max regex matches per pattern per source (runaway safety)
_MAX_MATCHES_PER_PATTERN = 100


class MudaeParser:
    """Regex-based Mudae message parser driven by validated settings.

    ``source`` is either a :class:`MudaeParserSettingsManager` (live —
    each parse uses the currently published snapshot) or a frozen
    :class:`EffectiveConfig` (hermetic — tests, simulations).
    """

    def __init__(self,
                 source: Optional[MudaeParserSettingsManager
                                  | EffectiveConfig] = None,
                 rules: Optional[Sequence[Rule]] = None) -> None:
        if source is None:
            source = MudaeParserSettingsManager(
                storage=_NoStorage())
        self._source = source
        self._rules: Tuple[Rule, ...] = tuple(
            rules if rules is not None else DEFAULT_RULES)

    # ------------------------------------------------------------ parsing
    def parse(self, message: Any,
              *, account_username: Optional[str] = None,
              _snapshot: Optional[EffectiveConfig] = None) -> ParseResult:
        """Parse one Discord message (duck-typed object or plain str).

        ``account_username`` is caller CONTEXT ONLY: it is echoed on
        the result and never replaces a username extracted from the
        message text (the two live in separate fields).

        ``_snapshot`` is an internal seam: pin one configuration
        snapshot across the parse (used to guarantee/test that an
        update mid-parse can never split a message across configs).
        """
        config = _snapshot if _snapshot is not None \
            else self._snapshot()
        diagnostics = Diagnostics()
        normalized = normalize_message(message, diagnostics)
        if not normalized.sources:
            diagnostics.info(CODE_NO_MATCH,
                             "message exposes no parseable text")
            return ParseResult(
                family=MessageFamily.UNKNOWN,
                account_username=account_username,
                diagnostics=diagnostics)

        matched = self._run_rules(config, normalized, diagnostics,
                                  account_username)
        usernames = self._username_pass(config, normalized, matched)
        result = self._assemble(matched, usernames, account_username,
                                normalized, diagnostics)
        return result

    @property
    def rules(self) -> Tuple[Rule, ...]:
        return self._rules

    @staticmethod
    def _string_ids(rule: Rule) -> Tuple[str, ...]:
        return tuple(binding.setting_id for binding in rule.strings)

    def _snapshot(self) -> EffectiveConfig:
        if isinstance(self._source, MudaeParserSettingsManager):
            return self._source.effective_config
        return self._source

    # ------------------------------------------------------------ engine
    def _run_rules(self, config: EffectiveConfig,
                   normalized: NormalizedMessage,
                   diagnostics: Diagnostics,
                   account_username: Optional[str]
                   ) -> List[Tuple[Rule, RuleResult]]:
        """Run every rule; return the matched (rule, result) pairs in
        priority order. The first is the winner; extras are flagged as
        ambiguous (honest detection beats silent first-wins)."""
        marker_cache: dict = {}
        matched: List[Tuple[Rule, RuleResult]] = []
        for rule in self._rules:
            if not self._gate(rule, config, normalized, marker_cache):
                continue
            group_dicts = self._collect(rule, config, normalized)
            required_ok = all(
                group_dicts.get(binding.setting_id)
                for binding in rule.patterns if binding.required)
            if not required_ok:
                continue
            context = RuleContext(
                markers={setting_id: marker_cache.get(setting_id, False)
                         for setting_id in self._string_ids(rule)},
                group_dicts=group_dicts,
                account_username=account_username,
                raw_sources={},
            )
            try:
                outcome = rule.handler(context)
            except Exception:
                # a broken handler is a configuration defect, not a
                # nonmatch: surface it loudly and skip the rule
                diagnostics.error(
                    "handler_failed",
                    f"rule {rule.rule_id!r} handler raised; rule skipped",
                    subject=rule.rule_id)
                continue
            if outcome.matched:
                matched.append((rule, outcome))
        if len(matched) > 1:
            names = ", ".join(rule.rule_id for rule, _ in matched)
            diagnostics.warning(
                CODE_AMBIGUOUS_MATCH,
                "message matched multiple rules; lowest-priority-order "
                "rule wins",
                detail=names)
        return matched

    def _gate(self, rule: Rule, config: EffectiveConfig,
              normalized: NormalizedMessage, marker_cache: dict) -> bool:
        """String-marker gate: every required marker must match, and
        at least one declared marker must match when any exist."""
        if not rule.strings:
            return True
        any_match = False
        for binding in rule.strings:
            setting_id = binding.setting_id
            if setting_id not in marker_cache:
                setting = config.string(setting_id)
                hit = False
                if setting is not None:
                    for source in normalized.by_source(setting.target):
                        from .settings.validator import matches_string
                        if matches_string(setting, source.value):
                            hit = True
                            break
                marker_cache[setting_id] = hit
            hit = marker_cache[setting_id]
            any_match = any_match or hit
            if binding.required and not hit:
                return False
        return any_match

    def _collect(self, rule: Rule, config: EffectiveConfig,
                 normalized: NormalizedMessage) -> dict:
        """Run the rule's patterns against sources of their declared
        target kind; group dicts per setting ID (first or all)."""
        group_dicts: dict = {}
        for binding in rule.patterns:
            compiled = config.pattern(binding.setting_id)
            if compiled is None:
                continue  # disabled or unknown: handler degrades
            setting = compiled.setting
            found: List[dict] = []
            for source in normalized.by_source(setting.target):
                for match in compiled.regex.finditer(source.value):
                    groups = {key: value for key, value
                              in match.groupdict().items()}
                    found.append(groups)
                    if binding.multiplicity == "first":
                        break
                if binding.multiplicity == "first" and found:
                    break
                if len(found) >= _MAX_MATCHES_PER_PATTERN:
                    break
            if found:
                group_dicts[binding.setting_id] = found[
                    :_MAX_MATCHES_PER_PATTERN]
        return group_dicts

    def _username_pass(self, config: EffectiveConfig,
                       normalized: NormalizedMessage,
                       matched: List[Tuple[Rule, RuleResult]]) -> tuple:
        """Cross-cutting username extraction (message text only)."""
        group_dicts: dict = {}
        for setting_id in USERNAME_PATTERN_IDS:
            compiled = config.pattern(setting_id)
            if compiled is None:
                continue
            setting = compiled.setting
            found = []
            for source in normalized.by_source(setting.target):
                for match in compiled.regex.finditer(source.value):
                    found.append(match.groupdict())
                    if len(found) >= _MAX_MATCHES_PER_PATTERN:
                        break
            if found:
                group_dicts[setting_id] = found
        extracted = list(extract_usernames(group_dicts))
        for _rule, outcome in matched:
            for name in outcome.usernames:
                if name and name not in extracted:
                    extracted.append(name)
        return tuple(extracted)

    def _assemble(self, matched: List[Tuple[Rule, RuleResult]],
                  usernames: tuple, account_username: Optional[str],
                  normalized: NormalizedMessage,
                  diagnostics: Diagnostics) -> ParseResult:
        if not matched:
            diagnostics.info(
                CODE_UNKNOWN_FORMAT,
                "message did not match any known Mudae format")
            return ParseResult(
                family=MessageFamily.UNKNOWN,
                usernames=usernames,
                account_username=account_username,
                sources_seen=normalized.kinds(),
                diagnostics=diagnostics)
        rule, outcome = matched[0]
        return ParseResult(
            family=rule.family,
            matched_rules=tuple(r.rule_id for r, _ in matched),
            sources_seen=normalized.kinds(),
            character=outcome.character,
            kakera_events=outcome.kakera_events,
            cooldowns=outcome.cooldowns,
            timer=outcome.timer,
            wishlist=outcome.wishlist,
            kakeraloot=outcome.kakeraloot,
            trade=outcome.trade,
            marriage=outcome.marriage,
            usernames=usernames,
            account_username=account_username,
            diagnostics=diagnostics,
        )


class _NoStorage:
    """Null storage: a manager with defaults and nothing persisted.

    Keeps MudaeParser() dependency-free for plugins that never touch
    configuration APIs (no state/ directory required).
    """

    def load(self):
        return None

    def save(self, document):  # pragma: no cover - never called
        from .exceptions import SettingsPersistenceError

        raise SettingsPersistenceError(
            "this parser was built without a persistence backend")
