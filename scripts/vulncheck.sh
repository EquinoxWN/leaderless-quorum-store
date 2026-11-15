#!/usr/bin/env bash
# govulncheck v1.8.0 over all packages; any finding fails.
set -uo pipefail
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
ids=$(printf '%s\n' "$out" | grep -oE '^Vulnerability #[0-9]+: GO-[0-9]+-[0-9]+' | grep -oE 'GO-[0-9]+-[0-9]+' | sort -u | tr '\n' ' ')
echo "::error::govulncheck failed (exit $rc): ${ids:-no vulnerability ids parsed}"
exit 1
