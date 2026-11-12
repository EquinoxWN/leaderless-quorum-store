#!/usr/bin/env bash
# govulncheck with one documented exception: GO-2026-6859 (net/http, Go standard library),
# fixed in Go 1.26.11 while the portfolio pins Go 1.26.10. Any other finding fails.
# Remove the exception when the pin moves to 1.26.11 (with 1.26.11 the scan is clean).
set -uo pipefail
ALLOWED="GO-2026-6859"
bin="$(pwd)/.tmp/bin"   # git-ignored; go run would hide govulncheck's exit code 3
mkdir -p "$bin"
GOBIN="$bin" go install golang.org/x/vuln/cmd/govulncheck@v1.8.0 || { echo "::error::govulncheck install failed"; exit 1; }
out=$("$bin/govulncheck" ./... 2>&1)
rc=$?
printf '%s\n' "$out"
if [ "$rc" -eq 0 ]; then
  echo "::notice::govulncheck: no vulnerabilities"
  exit 0
fi
ids=$(printf '%s\n' "$out" | grep -oE '^Vulnerability #[0-9]+: GO-[0-9]+-[0-9]+' | grep -oE 'GO-[0-9]+-[0-9]+' | sort -u)
if [ "$rc" -eq 3 ] && [ "$ids" = "$ALLOWED" ]; then
  echo "::warning::govulncheck: only $ALLOWED (Go standard library, fixed in Go 1.26.11; pin is 1.26.10)"
  exit 0
fi
echo "::error::govulncheck failed (exit $rc): ${ids:-no vulnerability ids parsed}"
exit 1
