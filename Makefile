.PHONY: setup lint test bench tradeoffs ci audit

setup:
	go mod download

lint:
	@files="$$(gofmt -l cmd internal)"; test -z "$$files" || (echo "$$files"; echo "run gofmt -w"; exit 1)
	go vet ./...

test:
	go test -race -count=1 ./...

# Stale-read rate and fault tolerance for every R and W at N=3 and N=5 (in-process faults),
# then the gate that fails if any R + W > N setting returned a stale read.
tradeoffs:
	go run ./cmd/leaderless-quorum-store tradeoffs -n 3 -trials 2000 -seed 1 > tradeoffs.txt
	go run ./cmd/leaderless-quorum-store tradeoffs -n 5 -trials 2000 -seed 1 >> tradeoffs.txt
	cat tradeoffs.txt
	bash scripts/overlap_gate.sh tradeoffs.txt

bench: tradeoffs

# Known vulnerabilities in the code paths actually called (Go 1.26+, as in CI).
audit:
	bash scripts/vulncheck.sh

ci: setup lint test tradeoffs
