# Security policy

## Reporting a vulnerability

Please report security problems privately through GitHub:
**Security > Report a vulnerability** on this repository
(`https://github.com/EquinoxWN/leaderless-quorum-store/security/advisories/new`). Do not open a public issue.

Include what you found, how to reproduce it, and the impact you expect. You will get an answer
within 7 days. Fixes are released as soon as they are ready, and you are credited unless you ask
not to be.

## Supported versions

This project is pre-1.0. Only the latest commit on `main` receives fixes.

## Scope

- **In scope:** Acknowledged writes lost with R + W > N, crashes or unbounded memory use caused by
  requests to `serve`, data races, input that bypasses validation.
- **Out of scope:** The lack of authentication and TLS between nodes (documented; keep `serve` on
  loopback or a private network), data loss on restart (M1 keeps data in memory), and missing
  features listed on the roadmap.

## How this repository protects itself

- Every GitHub Action is pinned to a full commit SHA, and workflows run with read-only
  permissions and without persisted credentials.
- Dependabot update pull requests are paused (`open-pull-requests-limit: 0`) so the repository keeps a single `main` branch; dependencies are updated by hand and checked by the CI audit.
- CI runs lint, tests with the race detector, and a known-vulnerability audit (`make audit`) on
  every push and pull request.
