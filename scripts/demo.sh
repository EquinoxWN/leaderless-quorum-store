#!/usr/bin/env bash
# Three replica processes on loopback; quorum writes and reads while nodes are stopped.
# Run from the repository root: bash scripts/demo.sh   (Linux, macOS or Git Bash on Windows)
set -euo pipefail
mkdir -p .tmp/bin
B=".tmp/bin/lqs$(go env GOEXE)"
go build -o "$B" ./cmd/leaderless-quorum-store
declare -A PORT=([a]=7101 [b]=7102 [c]=7103) PID
for id in a b c; do
  "$B" serve -id "$id" -addr "127.0.0.1:${PORT[$id]}" > ".tmp/node-$id.log" 2>&1 &
  PID[$id]=$!
done
# stop ends one node process; on Windows it finds the native process by its listening port.
stop() {
  if command -v taskkill > /dev/null; then
    local wpid
    wpid=$(netstat -ano | awk -v p=":${PORT[$1]}" '$2 ~ p"$" && $4 == "LISTENING" {print $5; exit}')
    if [ -n "$wpid" ]; then taskkill //F //PID "$wpid" > /dev/null; fi
  else
    kill "${PID[$1]}" 2> /dev/null || true
  fi
}
trap 'for n in a b c; do stop $n || true; done' EXIT
for port in 7101 7102 7103; do
  for _ in $(seq 1 50); do
    curl -s -m 1 "http://127.0.0.1:$port/healthz" > /dev/null && break
    sleep 0.2
  done
done
NODES=a=http://127.0.0.1:7101,b=http://127.0.0.1:7102,c=http://127.0.0.1:7103
echo '$ lqs serve -id a|b|c -addr 127.0.0.1:7101|7102|7103   (three processes)'
cat .tmp/node-a.log .tmp/node-b.log .tmp/node-c.log
echo '$ lqs put -nodes $NODES user:42 alice'; "$B" put -nodes $NODES user:42 alice
echo '$ lqs get -nodes $NODES user:42'; "$B" get -nodes $NODES user:42
echo '# stop node a'; stop a
echo '$ lqs put -nodes $NODES -timeout 500ms user:42 bob'; "$B" put -nodes $NODES -timeout 500ms user:42 bob
echo '$ lqs get -nodes $NODES -timeout 500ms user:42'; "$B" get -nodes $NODES -timeout 500ms user:42
echo '# stop node b as well: only c is up'; stop b
echo '$ lqs put -nodes $NODES -timeout 500ms user:42 carol   (R=2 W=2, expected to fail)'
if "$B" put -nodes $NODES -timeout 500ms user:42 carol 2>&1; then echo "unexpected success"; exit 1; else echo "exit $?"; fi
echo '$ lqs put -nodes $NODES -timeout 500ms -r 1 -w 1 user:42 dave'; "$B" put -nodes $NODES -timeout 500ms -r 1 -w 1 user:42 dave
echo '$ lqs get -nodes $NODES -timeout 500ms -r 1 user:42'; "$B" get -nodes $NODES -timeout 500ms -r 1 user:42
