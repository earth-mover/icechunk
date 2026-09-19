#!/usr/bin/env bash
# Usage: check-shard-counts.sh <dir of shard-count.txt files, each "k/N kept total">
# Only passing shards write one, so a missing, failed or mis-sized shard fails here.
set -euo pipefail

fail() {
  echo "::error::$1"
  exit 1
}

lines=()
while IFS= read -r line; do
  lines+=("$line")
done < <(find "$1" -type f -name shard-count.txt -exec cat {} + 2>/dev/null)
[ "${#lines[@]}" -gt 0 ] || fail "no shard reported a passing run"

read -r first_spec _ total <<<"${lines[0]}"
count=${first_spec#*/}
kept_sum=0
ids=()
for line in "${lines[@]}"; do
  read -r spec kept seen_total <<<"$line"
  echo "shard $spec: kept $kept of $seen_total"
  [ "${spec#*/}" = "$count" ] || fail "shard $spec disagrees on the shard count $count"
  [ "$seen_total" = "$total" ] || fail "shard $spec collected $seen_total tests, another collected $total"
  kept_sum=$((kept_sum + kept))
  ids+=("${spec%/*}")
done

unique=$(($(printf '%s\n' "${ids[@]}" | sort -u | wc -l)))
[ "$unique" -eq "$count" ] || fail "only $unique of $count shards passed"
[ "$kept_sum" -eq "$total" ] || fail "the shards ran $kept_sum of $total tests"
echo "all $count shards passed and ran all $total tests"
