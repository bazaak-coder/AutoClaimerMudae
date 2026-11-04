# Security Policy

## Scope

AutoClaimer is a personal-use, multi-account Discord automation framework
(see the platform-restrictions note in the README: user-account
automation may violate Discord's ToS; this project implements no
evasion, CAPTCHA bypass, or rate-limit circumvention). Security issues
in THIS repository — credential handling, persistence, unsafe input
handling, CI/workflow configuration — are in scope.

## Supported versions

| Version | Supported |
|---|---|
| `main` / `0.2.x` | ✅ |
| older | ❌ |

## Reporting a vulnerability

**Do not open a public issue for a suspected vulnerability.** Use
GitHub's **private vulnerability reporting** on this repository
(Report a vulnerability), or contact the owner directly. Include a
reproduction or proof-of-concept where practical; reports are
triaged with the same discipline as CI failures — no silent
deprioritization.

Deliberately out of scope for reward/fix prioritization: abusing the
tool against Discord itself, ToS-evasion feature requests, and
findings in the vendored `autoclaimer/mudae_parser/` that also exist
upstream (report those to the upstream project).

## Preventive controls in place

- **Credential boundary** (agents.md §12): no personal credential may
  ever be pushed, committed, or shared from the local system through
  any channel; test credentials are format-realistic but
  value-unrelated synthetics. History is secret-scanned with push
  protection enabled.
- **Dependency audit**: every locked dependency (all platforms —
  environment markers stripped) fails the PR and the weekly scheduled
  scan on any known vulnerability (`pip-audit --require-hashes`).
- **CodeQL** static analysis on every PR, push to `main`, and weekly.
- **Actions supply chain**: every workflow action reference is
  version-pinned and verified against `.github/workflows/actions.lock`
  (a per-workflow allowlist with resolved commits and owner/repo IDs)
  by a required CI job; Dependabot bumps go through the same gate.
- **Least privilege**: workflow jobs declare minimal permissions and
  disable checkout credential persistence; no secrets are exposed to
  untrusted code paths.
