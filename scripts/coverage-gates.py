#!/usr/bin/env python3
"""Critical-path coverage gates (roadmap item 18).

Enforces PER-MODULE floors on the modules the roadmap names as critical
(credential storage, SQLite persistence, worker lifecycle, rate limiter,
configuration validation, Discord adapter lifecycle, plus the newer
operational modules). Floors sit a few points below the CURRENT measured
coverage: they are regression fences, not vanity numbers — a drop below
the floor means a behavior/failure path lost its tests.

Run (from the repo root, after a coverage JSON exists):

    pytest -q --cov=autoclaimer --cov-report=json:coverage.json
    python scripts/coverage-gates.py

Exit code 1 names every module below its floor.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

#: module -> minimum covered %. Floor rationale: current measured
#: coverage minus a small buffer (see the git history of this table).
FLOORS: dict[str, float] = {
    "autoclaimer/credential_store.py": 85.0,  # measured 87.6
    "autoclaimer/persistence.py": 92.0,  # measured 94.5
    "autoclaimer/workers.py": 89.0,  # measured 91.1
    "autoclaimer/ratelimit.py": 94.0,  # measured 96.6
    "autoclaimer/config.py": 92.0,  # measured 94.4
    "autoclaimer/discord_adapter.py": 79.0,  # measured 82.3 (live-login paths skip)
    "autoclaimer/health.py": 95.0,  # measured 97.9
    "autoclaimer/audit.py": 95.0,  # measured 100
    "autoclaimer/cli.py": 77.0,  # measured 79.9
    "autoclaimer/mudae_tasks.py": 88.0,  # measured 90.5
    "autoclaimer/adapters.py": 95.0,  # measured 97.3
    "autoclaimer/logging_setup.py": 90.0,  # measured 93.1
}


def main() -> int:
    report = Path("coverage.json")
    if not report.exists():
        print("coverage.json not found — run pytest with --cov-report=json:coverage.json")
        return 2
    data = json.loads(report.read_text())
    files = data["files"]
    failures: list[str] = []
    print(f"{'module':40} {'floor':>6} {'actual':>7}  status")
    for module, floor in sorted(FLOORS.items()):
        cov = files.get(module)
        if cov is None:
            # Coverage keys are filesystem paths; match by suffix.
            suffix = module.split("/")[-1]
            matches = [
                v for k, v in files.items() if k.endswith(suffix) and "mudae_parser" not in k
            ]
            cov = matches[0] if matches else None
        if cov is None:
            failures.append(f"{module}: no coverage data")
            print(f"{module:40} {floor:>5.0f}% {'—':>7}  MISSING")
            continue
        actual = cov["summary"]["percent_covered"]
        status = "ok" if actual >= floor else "BELOW FLOOR"
        if actual < floor:
            failures.append(f"{module}: {actual:.1f}% < floor {floor:.0f}%")
        print(f"{module:40} {floor:>5.0f}% {actual:>6.1f}%  {status}")
    if failures:
        print("\nCOVERAGE GATES FAILED:")
        for failure in failures:
            print(f"  - {failure}")
        return 1
    print("\nCOVERAGE GATES PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
