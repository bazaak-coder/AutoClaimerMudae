# Vendored: Mudae parser engine

This package is vendored from the owner's sibling repository
**LibzentSuite** (`/home/libosterelezent/Workspace/LibzentSuite`), by
explicit owner instruction (Phase 3, 2026-10-28).

- **Source revision:** `41b0775c110d6e5b9df3147a4ba21d74eddfb22f`
  (parser history: `d21c2aa` engine, `104ef4b` registry,
  `0f53508` rebrand).
- **Source path:** `parsers/mudae/**` → `autoclaimer/mudae_parser/**`.
- **Files:** 26 Python modules, byte-identical to upstream at the
  revision above (verified: no intra-package import references the
  LibzentSuite root; two docstrings mention `parsers.mudae` as example
  prose only — left untouched to keep the diff against upstream empty).
- **Excluded upstream files:** `parsers/__init__.py` (package facade
  re-exporting the LibzentSuite-wide `PARSER_MANAGER`) and
  `parsers/manager.py` (`ParserManager` registry + `parse_event()`
  event-bus seam — LibzentSuite framework concepts AutoClaimer does not
  use; AutoClaimer calls `MudaeParser.parse()` directly). Their test
  file `tests/unit/test_parser_manager.py` (17 tests) was therefore not
  ported.
- **Upstream tests ported:** `test_mudae_parser.py` (39),
  `test_mudae_parser_strings.py` (18), `test_mudae_parser_settings.py`
  (24) → AutoClaimer `tests/`, imports rewritten to
  `autoclaimer.mudae_parser`. The live-capture dataset regression
  self-skips: `parsers/CuratedMudaeDataset/*.jsonl` was never committed
  upstream (the inline fixtures are the durable evidence).
- **Licensing:** LibzentSuite carries no LICENSE file; both repositories
  are owned by the same owner, who directed this copy. No third-party
  code is contained here (stdlib-only, verified by audit 2026-10-28 and
  re-verified during vendoring).
- **Intentional deviations (build/tooling only, none behavioral):**
  - Import root `parsers.mudae` → `autoclaimer.mudae_parser`.
  - Ruff: E501/UP/I relaxed for this subtree; `ruff format` excludes it
    (upstream formatting preserved — no unrelated churn, easy future
    diffs/syncs against upstream).
  - mypy: strict mode relaxed for this subtree (upstream code is typed
    but not to AutoClaimer's strict preset).
  - `default_storage_path()` in `settings/manager.py` resolves relative
    to this module and therefore points inside AutoClaimer if ever
    used; AutoClaimer's interface uses hermetic in-memory settings
    (`MudaeParser()` with no source) and never touches that path.
- **Synchronizing:** when pulling upstream parser changes, diff against
  the revision above, re-apply the exclusions, and re-run the ported
  test files.
