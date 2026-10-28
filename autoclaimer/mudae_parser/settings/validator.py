"""Candidate-setting validation: syntax, groups, targets, safety.

Runs BEFORE anything is activated. A candidate that fails here never
reaches the active configuration; the previously active configuration
is untouched (see the settings manager for the transaction).

What is checked (and why):
  * syntax       — ``re.compile`` must accept the expression;
  * groups       — every ``required_groups`` name must appear as a
                   NAMED group; optional groups may be absent;
  * safety-size  — expression length bound and a total pattern-count
                   bound (registered patterns only — a fleet of tiny
                   patterns is itself a DoS surface);
  * flags        — compiled internally from ``case_sensitive`` only;
                   callers cannot inject arbitrary flags;
  * targets      — must be a known TargetSource;
  * literal mode — match mode must be a known MatchMode; message
                   strings must never be compiled as regex.

Threat-model honesty: Python's ``re`` has NO reliable per-match
timeout. We mitigate by bounding expression length, bounding input
size (normalization layer), and rejecting a small set of known
catastrophic constructs (nested unbounded quantifiers), but a
determined user expression can still be expensive. This is accepted
because settings authors are the same trust level as the operators
running the process; if that changes, an isolated regex engine
(process pool) would be required — deliberately out of scope here.
"""
from __future__ import annotations

import re
from typing import List, Optional, Sequence

from ..enums import MatchMode, TargetSource
from .models import (
    MessageStringSetting,
    RegexPatternSetting,
    ValidationIssue,
    ValidationReport,
)

__all__ = [
    "MAX_PATTERN_CHARS", "MAX_PATTERNS", "MAX_STRING_CHARS",
    "validate_pattern", "validate_string", "compile_pattern",
    "matches_string", "KillerPatternCheck",
]

#: hard bounds (regex/string execution safety)
MAX_PATTERN_CHARS = 2_000
MAX_PATTERNS = 500
MAX_STRING_CHARS = 2_000

#: known catastrophic-backtracking shapes we refuse outright. Not a
#: general proof — a documented best-effort rejection of the classic
#: ``(a+)+`` family: a quantified group whose body itself ends in a
#: quantifier, nested inside another quantifier.
_NESTED_QUANTIFIER = re.compile(
    r"\((?:[^()\\]|\\.)*[+*][^()\\]*\)\s*[+*]{1,2}")


def _issues(ok_needed: bool, issues: List[ValidationIssue]) \
        -> ValidationReport:
    return ValidationReport(ok=ok_needed and not issues,
                            issues=tuple(issues))


def validate_pattern(candidate: RegexPatternSetting) -> ValidationReport:
    """Full validation of one candidate regex pattern setting."""
    issues: List[ValidationIssue] = []
    expression = candidate.expression or ""
    if not expression.strip():
        issues.append(ValidationIssue(
            "empty_expression",
            "pattern expression is empty — a default cannot be replaced "
            "by an empty pattern"))
        return _issues(False, issues)
    if len(expression) > MAX_PATTERN_CHARS:
        issues.append(ValidationIssue(
            "expression_too_long",
            f"expression exceeds {MAX_PATTERN_CHARS} characters",
            subject="length"))
    if candidate.target not in TargetSource:
        issues.append(ValidationIssue(
            "unsupported_target",
            f"unknown target {candidate.target!r}",
            subject="target"))
    if _NESTED_QUANTIFIER.search(expression):
        issues.append(ValidationIssue(
            "unsafe_construct",
            "nested unbounded quantifier detected (catastrophic "
            "backtracking risk); simplify the expression",
            subject="safety"))
    try:
        regex = re.compile(
            expression,
            0 if candidate.case_sensitive else re.IGNORECASE)
    except re.error as exc:
        issues.append(ValidationIssue(
            "invalid_syntax", f"regex does not compile: {exc}",
            subject="syntax"))
        return _issues(False, issues)

    group_names = set(regex.groupindex)
    for required in candidate.required_groups:
        if required not in group_names:
            issues.append(ValidationIssue(
                "missing_required_group",
                f"required capture group (?P<{required}>...) is absent — "
                f"the extraction rule depends on it",
                subject=f"group:{required}"))
    declared_optional = set(candidate.optional_groups)
    unknown = group_names - set(candidate.required_groups) \
        - declared_optional
    if unknown:
        issues.append(ValidationIssue(
            "undeclared_group",
            f"capture groups not declared by the rule: "
            f"{sorted(unknown)} — the handler will ignore them",
            subject="groups"))
    # undeclared groups are a warning, not a rejection
    hard = [i for i in issues if i.code != "undeclared_group"]
    return ValidationReport(ok=not hard, issues=tuple(issues))


def validate_string(candidate: MessageStringSetting) -> ValidationReport:
    """Full validation of one candidate literal message string."""
    issues: List[ValidationIssue] = []
    text = candidate.text
    if not text.strip():
        issues.append(ValidationIssue(
            "empty_string",
            "message string is empty — a default cannot be replaced by "
            "nothing"))
    if len(text) > MAX_STRING_CHARS:
        issues.append(ValidationIssue(
            "string_too_long",
            f"string exceeds {MAX_STRING_CHARS} characters",
            subject="length"))
    if candidate.match_mode not in MatchMode:
        issues.append(ValidationIssue(
            "invalid_match_mode",
            f"unknown match mode {candidate.match_mode!r}",
            subject="match_mode"))
    if candidate.target not in TargetSource:
        issues.append(ValidationIssue(
            "unsupported_target",
            f"unknown target {candidate.target!r}",
            subject="target"))
    return _issues(not issues, issues)


def compile_pattern(setting: RegexPatternSetting) -> re.Pattern:
    """Compile a VALIDATED pattern. Raises only on programmer error
    (calling this before validate_pattern succeeded)."""
    return re.compile(
        setting.expression,
        0 if setting.case_sensitive else re.IGNORECASE)


def matches_string(setting: MessageStringSetting, text: str) -> bool:
    """Literal comparison per the setting's mode + case rule.

    The stored raw text is NEVER modified — case-insensitive matching
    folds both sides at comparison time only.
    """
    if not setting.enabled:
        return False
    needle = setting.text
    haystack = text
    if not setting.case_sensitive:
        needle = needle.casefold()
        haystack = haystack.casefold()
    if setting.match_mode is MatchMode.EXACT:
        return haystack == needle
    if setting.match_mode is MatchMode.PREFIX:
        return haystack.startswith(needle)
    if setting.match_mode is MatchMode.CONTAINS:
        return needle in haystack
    raise ValueError(  # pragma: no cover - guarded by validation
        f"unsupported match mode {setting.match_mode!r}")


class KillerPatternCheck:
    """Namespace for the documented safety limits (introspection aid)."""

    MAX_PATTERN_CHARS = MAX_PATTERN_CHARS
    MAX_PATTERNS = MAX_PATTERNS
    MAX_STRING_CHARS = MAX_STRING_CHARS
