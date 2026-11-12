#!/usr/bin/env bash
# Fails if any R + W > N row of a tradeoffs table shows a stale read. usage: overlap_gate.sh TABLE_FILE
set -euo pipefail
file=${1:?usage: overlap_gate.sh TABLE_FILE}
rows=$(awk -F'|' '$5 ~ /yes/' "$file" | wc -l)
bad=$(awk -F'|' '$5 ~ /yes/ && $6 !~ / 0\// {print}' "$file")
if [ "$rows" -eq 0 ]; then
  echo "::error::overlap gate: no R+W>N rows found in $file"
  exit 1
fi
if [ -n "$bad" ]; then
  echo "::error::overlap gate: stale reads with overlapping quorums: $bad"
  exit 1
fi
echo "::notice::overlap gate: 0 stale reads in all $rows R+W>N settings"
