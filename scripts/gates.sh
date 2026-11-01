#!/usr/bin/env bash
# Development quality gates (roadmap item 17 — static analysis enforcement).
#
# Runs EXACTLY what CI runs, with strict exit codes: `set -euo pipefail`
# and NO output pipes — a masked exit code is how gate failures slip
# through locally (found the hard way when CI's newer ruff caught what
# piped local runs had hidden).
#
# Intentional exceptions (centralized in pyproject.toml, documented here):
# - autoclaimer/mudae_parser/** + its three test files: EXCLUDED from
#   ruff/mypy strict — vendored byte-identical upstream code, proven by
#   its own 80-test suite (see autoclaimer/mudae_parser/VENDORED.md).
# - mypy `strict = true` globally; no per-module downgrades exist.

set -euo pipefail
cd "$(dirname "$0")/.."

echo "==> ruff check"
ruff check .

echo "==> ruff format --check"
ruff format --check .

echo "==> mypy (strict)"
mypy .

echo "==> pytest"
pytest -q

echo "==> git diff --check (whitespace)"
git diff --check

echo "ALL GATES PASSED"
