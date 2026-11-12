# Contributing

Thanks for helping. This is a Go project; issues and pull requests are welcome.

## Set up and check your change

Needs Go 1.23 or newer (CI uses 1.26.10). Caches stay in git-ignored folders inside the repo.

```bash
make setup       # download modules (none beyond the standard library today)
make lint        # gofmt and go vet
make test        # full test suite with the race detector
make tradeoffs   # stale-read table for N=3 and N=5, plus the overlap gate
make audit       # known-vulnerability check (as in CI)
bash scripts/demo.sh   # three replica processes on loopback
```

A pull request is ready when `make lint`, `make test` and `make tradeoffs` pass and CI is green.

## Guidelines

- Keep each change focused; explain the problem it solves in the pull request.
- Add or update a test for every behaviour change, and update `docs/results/` when numbers change.
- Significant design changes need an ADR in `docs/adr/` (copy `0001` for the format).
- Keep function comments to one short phrase; let names and tests explain the rest.
- Report security problems privately, as described in [SECURITY.md](SECURITY.md).

By contributing you agree that your work is released under the [MIT License](LICENSE).
