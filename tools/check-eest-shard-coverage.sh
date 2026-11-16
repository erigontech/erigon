#!/usr/bin/env bash
# Fails when the EEST shards stop covering their corpus, so a fixtures bump
# can't silently drop tests. It targets the failures a normal test run does NOT
# surface — a fork quietly dropped while the shard still runs enough tests to
# look green. The partition is read from the live config and corpus
# manifests (.meta/index.json). It also guards manifest naming that controls
# race-binary dispatch in the Makefile.
#
# The fork-split families each run every fork exactly once — spec race shards
# (eest-spec-shards.yml `run` regexes) over blockchain_test, and stable/devnet
# hive consume-enginex shards (test-hive-eest.yml sim-limit) over
# blockchain_test_engine_x. A gap silently never runs a fork; an overlap runs
# it twice.
#
# The devnet check is skipped (not failed) when the devnet corpus isn't staged,
# so a stable-only local run still works; `make check-eest-shards` stages both.
#
# Usage: tools/check-eest-shard-coverage.sh [STABLE_INDEX_JSON [DEVNET_INDEX_JSON]]
set -euo pipefail

here=$(cd "$(dirname "$0")/.." && pwd)
stable_index="${1:-$here/test-fixtures-cache/eest_stable/fixtures/.meta/index.json}"
devnet_index="${2:-$here/test-fixtures-cache/eest_devnet/fixtures/.meta/index.json}"
shards="$here/tools/eest-spec-shards.yml"
hive="$here/.github/workflows/test-hive-eest.yml"

for tool in jq yq awk; do
	command -v "$tool" >/dev/null 2>&1 || { echo "check-eest-shard-coverage: $tool not found in PATH" >&2; exit 1; }
done
[[ -f "$stable_index" ]] || { echo "check-eest-shard-coverage: stable index not found: $stable_index" >&2; exit 1; }

# Live regexes, one "route=regex" per line, read from the shard manifest's `run`
# keys. Strip the mode suffix and dedupe to one entry per fork route.
race_regexes() {
	yq -o=json '.' "$shards" \
		| jq -r '.[]
			| select(.shard | test("^blocktests-stable-race-"))
			| select((.run // "") != "")
			| "\(.shard | sub("^blocktests-stable-race-"; "") | sub("-(sequential|parallel)$"; ""))=\(.run)"' \
		| sort -u
}
check_transaction_race_shards() {
	echo "==== transaction race shards ===="
	local actual expected
	actual=$(yq -o=json '.' "$shards" \
		| jq -r '.[].shard | select(test("^transactiontests-(stable|devnet)(-race)?$"))' \
		| sort)
	expected=$'transactiontests-devnet-race\ntransactiontests-stable-race'
	if [[ "$actual" != "$expected" ]]; then
		echo "  expected stable and devnet transaction shards to use the -race suffix" >&2
		printf '  found: %s\n' "${actual//$'\n'/, }" >&2
		return 1
	fi
	printf '  ok  %s\n' "${actual//$'\n'/$'\n  ok  '}"
}
hive_consume_enginex_regexes() {
	local fixture_set="$1"
	yq -o=json '.jobs.load-matrix.steps[] | select(.id == "load") | .env.SHARDS | from_yaml' "$hive" \
		| jq -r --arg fixture_set "$fixture_set" '.[] | select(.sim=="consume-enginex" and .["fixtures-tarball"]==$fixture_set) | "\(.shard)=\(.["sim-limit"])"' \
		| sort -u
}

# --- 1. partition ---
fork_rows() {
	jq -r --arg f "$2" '.test_cases | map(select(.format==$f)) | group_by(.fork)[] | "\(.[0].fork)\t\(length)\t[fork_\(.[0].fork)-x]"' "$1"
}
test_rows() {
	jq -r --arg f "$2" '.test_cases[] | select(.format==$f) | "\(.fork)\t1\teels/consume-enginex/\(.id)"' "$1"
}
check_partition() {
	local title="$1" format="$2" regexes="$3"
	echo "==== partition: $title (format=$format) ===="
	if [[ -z "${regexes//[[:space:]]/}" ]]; then
		echo "  ERROR: no shard regexes parsed for $title — the scrape returned empty (shard rename / label or quoting change?)" >&2
		return 1
	fi
	printf '%s\n' "$regexes" | sed 's/^/  regex  /'
	jq -rR --arg rx "$regexes" '
		($rx | split("\n") | map(select(. != "") | capture("^(?<name>[^=]*)=(?<re>.*)$"))) as $s
		| split("\t") as [$fork, $cnt, $id]
		| [$fork, $cnt, ($s[] | select(.re as $re | $id | test($re)) | .name)] | join("\t")' \
	| awk -F'\t' '
		{
			m=NF-2
			for (i=3;i<=NF;i++) tot[$i]+=$2
			total+=$2
			if (m==0) gap[$1]+=$2
			if (m>=2) over[$1]+=$2
		}
		END {
			for (f in gap)  { printf "  GAP      %-28s %7d  (matched by no shard)\n", f, gap[f]; gaps++ }
			for (f in over) { printf "  OVERLAP  %-28s %7d  (matched by 2+ shards)\n", f, over[f]; overlaps++ }
			print "  --- per-shard totals ---"
			for (s in tot) printf "  %-18s %7d\n", s, tot[s]
			printf "  total=%d  gaps=%d  overlaps=%d\n", total, gaps+0, overlaps+0
			if (total == 0) { print "  ERROR: no forks found for this format — index format labels changed?" > "/dev/stderr"; exit 1 }
			if (gaps || overlaps) exit 1
		}
	'
}

rc=0
check_transaction_race_shards || rc=1
echo
fork_rows "$stable_index" blockchain_test | check_partition "spec race shards (blocktest --run)" blockchain_test "$(race_regexes)" || rc=1
echo
echo
test_rows "$stable_index" blockchain_test_engine_x | check_partition "hive stable consume-enginex (sim-limit)" blockchain_test_engine_x "$(hive_consume_enginex_regexes eest_stable)" || rc=1
echo
if [[ -f "$devnet_index" ]]; then
	test_rows "$devnet_index" blockchain_test_engine_x | check_partition "hive devnet consume-enginex (sim-limit)" blockchain_test_engine_x "$(hive_consume_enginex_regexes eest_devnet)" || rc=1
	echo
else
	echo "==== devnet check SKIPPED (index not found: $devnet_index) ===="
	echo
fi
if (( rc == 0 )); then
	echo "EEST shard coverage OK."
else
	echo "EEST shard coverage FAILED: fix the transaction race-shard names or race run regexes in tools/eest-spec-shards.yml, or the Hive matrix in .github/workflows/test-hive-eest.yml." >&2
fi
exit $rc
