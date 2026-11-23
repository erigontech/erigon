#!/usr/bin/env bash
# Check that the CI shard filters in each partition cover every fixture case
# exactly once. Also check the transaction shard names used for race-binary
# dispatch in the Makefile.
#
# A local run can check only the staged indices. `make check-eest-shards`
# stages the stable, devnet and witness indices.
#
# Usage: tools/check-eest-shard-coverage.sh [STABLE_INDEX_JSON [DEVNET_INDEX_JSON [ZKEVM_INDEX_JSON]]]
set -euo pipefail

here=$(cd "$(dirname "$0")/.." && pwd)
stable_index="${1:-$here/test-fixtures-cache/eest_stable/fixtures/.meta/index.json}"
devnet_index="${2:-$here/test-fixtures-cache/eest_devnet/fixtures/.meta/index.json}"
witness_index="${3:-$here/test-fixtures-cache/eest_zkevm/fixtures/.meta/index.json}"
hive="$here/.github/workflows/test-hive-eest.yml"

for tool in jq yq awk; do
	command -v "$tool" >/dev/null 2>&1 || { echo "check-eest-shard-coverage: $tool not found in PATH" >&2; exit 1; }
done
[[ -f "$stable_index" ]] || { echo "check-eest-shard-coverage: stable index not found: $stable_index" >&2; exit 1; }
ci_shards=$(yq -o=json '.' "$here/tools/eest-spec-shards.yml" | jq -c '.[] | select(."local-only" != true)')

# Live regexes, one "route=regex" per line, read from the shard manifest's `run`
# keys. Strip the mode suffix and dedupe to one entry per fork route.
race_regexes() {
	jq -r 'select(.shard | test("^blocktests-stable-race-"))
		| select((.run // "") != "")
		| "\(.shard | sub("^blocktests-stable-race-"; "") | sub("-(sequential|parallel)$"; ""))=\(.run)"' <<<"$ci_shards" \
		| sort -u
}
check_transaction_race_shards() {
	echo "==== transaction race shards ===="
	local actual expected
	actual=$(jq -r '.shard | select(test("^transactiontests-(stable|devnet)(-race)?$"))' <<<"$ci_shards" \
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
		| split("\t") as [$group, $cnt, $id]
		| [$group, $cnt, ($s[] | select(.re as $re | $id | test($re)) | .name)] | join("\t")' \
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
			if (total == 0) { print "  ERROR: no cases found for this format — index format labels changed?" > "/dev/stderr"; exit 1 }
			if (gaps || overlaps) exit 1
		}
	'
}

rc=0
check_transaction_race_shards || rc=1
echo
fork_rows "$stable_index" blockchain_test | check_partition "spec race shards (blocktest --run)" blockchain_test "$(race_regexes)" || rc=1
echo
if [[ -f "$witness_index" || $# -ge 3 ]]; then
	witness_regexes=$(jq -r 'select(.shard | startswith("zkevm-witness-race")) |
		"\(.shard)=\(.run // ".*")"' <<<"$ci_shards")
	# zkevmtest matches --run against the relative fixture path followed by the case ID.
	jq -r '.test_cases[] | select(.format=="blockchain_test") |
		(.json_path | ltrimstr("blockchain_tests/")) as $path |
		[($path | split("/")[:2] | join("/")), 1, ($path + "/" + .id)] | @tsv' "$witness_index" \
		| check_partition "zkEVM witness race shards (zkevmtest --run)" blockchain_test "$witness_regexes" || rc=1
else
	echo "==== witness check SKIPPED (index not found: $witness_index) ===="
fi
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
	echo "EEST shard coverage FAILED: check tools/eest-spec-shards.yml and the Hive matrix in .github/workflows/test-hive-eest.yml." >&2
fi
exit $rc
