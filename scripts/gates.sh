#!/usr/bin/env bash
# Development quality gates (roadmap item 17 — static analysis enforcement).
#
# Runs the LOCAL equivalents of CI's lint/format/type/test stages, with
# strict exit codes: `set -euo pipefail` and NO output pipes — a masked
# exit code is how gate failures slip through locally (found the hard way
# when CI's newer ruff caught what piped local runs had hidden).
#
# NOT run here (separate CI jobs): the critical-path coverage floor, the
# dependency security audit (pip-audit), and the actions-lockfile
# verification — the success message below does not cover them.
#
# Tools are invoked from the PROJECT virtualenv (.venv/bin), never the
# shell PATH: bare ruff/mypy/pytest may belong to another interpreter and
# silently check the wrong environment (agents.md section 5).
#
# Intentional exceptions (centralized in pyproject.toml, documented here):
# - autoclaimer/mudae_parser/** + its three test files: EXCLUDED from
#   ruff/mypy strict — vendored byte-identical upstream code, proven by
#   its own 80-test suite (see autoclaimer/mudae_parser/VENDORED.md).
# - mypy `strict = true` globally; no per-module downgrades exist.

set -euo pipefail
cd "$(dirname "$0")/.."

V=".venv/bin"
for tool in ruff mypy pytest; do
  if [ ! -x "$V/$tool" ]; then
    echo "missing $V/$tool — create the project virtualenv first (see README)" >&2
    exit 1
  fi
done

echo "==> ruff check"
"$V/ruff" check .

echo "==> ruff format --check"
"$V/ruff" format --check .

echo "==> mypy (strict)"
"$V/mypy" .

echo "==> pytest"
"$V/pytest" -q

echo "==> git diff --check (whitespace)"
git diff --check

echo "ALL LOCAL GATES PASSED (CI additionally runs: coverage gates, dependency audit, actions-lockfile verify)"
