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
# keys. -sequential/-parallel variants of a fork carry the same regex, so strip
# the mode suffix and dedupe to one entry per fork route.
race_regexes() {
	yq -o=json '.' "$shards" \
		| jq -r '.[]
			| select(.shard | test("^blocktests-stable-race-"))
			| select((.run // "") != "")
			| "\(.shard | sub("^blocktests-stable-race-"; "") | sub("-(sequential|parallel)$"; ""))=\(.run)"' \
		| sort -u
}
# A fork's -sequential/-parallel race shards must carry the same non-empty run
# regex. The partition check only sees the deduped union, so a run dropped or
# emptied on one mode still covers the fork via the other — while that mode hits
# the runner's glob arm with no --run and races the whole corpus.
check_race_mode_parity() {
	echo "==== race shard -sequential/-parallel run parity ===="
	local rows
	rows=$(yq -o=json '.' "$shards" | jq -r '
		.[]
		| select(.shard | test("^blocktests-stable-race-.+-(sequential|parallel)$"))
		| (.shard | capture("^blocktests-stable-race-(?<route>.+)-(?<mode>sequential|parallel)$")) as $m
		| "\($m.route)\t\($m.mode)\t\(.run // "")"')
	if [[ -z "${rows//[[:space:]]/}" ]]; then
		echo "  ERROR: no stable race -sequential/-parallel shards found (shard naming changed?)" >&2
		return 1
	fi
	printf '%s\n' "$rows" | awk -F'\t' '
		{ run[$1 SUBSEP $2]=$3; seen[$1]=1 }
		END {
			for (r in seen) {
				s=run[r SUBSEP "sequential"]; p=run[r SUBSEP "parallel"]
				if (s=="" || p=="") { printf "  MISSING/EMPTY  %-11s sequential=[%s] parallel=[%s]\n", r, s, p; bad++ }
				else if (s != p)    { printf "  DRIFT          %-11s sequential=[%s] parallel=[%s]\n", r, s, p; bad++ }
				else                { printf "  ok  %-11s = %s\n", r, s }
			}
			if (bad) { print "  => -sequential/-parallel of a fork must share one non-empty run regex" > "/dev/stderr"; exit 1 }
		}'
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
	yq -o=json '.jobs.test-hive-eest.strategy.matrix.include' "$hive" \
		| jq -r --arg fixture_set "$fixture_set" '.[] | select(.sim=="consume-enginex" and .["fixtures-tarball"]==$fixture_set) | "\(.shard)=\(.["sim-limit"])"' \
		| sort -u
}

# --- 1. partition ---
check_partition() {
	local title="$1" index="$2" format="$3" idprefix="$4" regexes="$5"
	echo "==== partition: $title (format=$format) ===="
	if [[ -z "${regexes//[[:space:]]/}" ]]; then
		echo "  ERROR: no shard regexes parsed for $title — the scrape returned empty (shard rename / label or quoting change?)" >&2
		return 1
	fi
	printf '%s\n' "$regexes" | sed 's/^/  regex  /'
	jq -r --arg f "$format" '.test_cases | map(select(.format==$f)) | group_by(.fork)[] | "\(.[0].fork)\t\(length)"' "$index" \
	| awk -v idprefix="$idprefix" '
		FNR==NR { i=index($0,"="); name[++k]=substr($0,1,i-1); rx[k]=substr($0,i+1); n=k; next }
		{
			fork=$1; cnt=$2; id=idprefix "[fork_" fork "-x]"; m=0
			for (i=1;i<=n;i++) if (id ~ rx[i]) { m++; tot[name[i]]+=cnt }
			total+=cnt
			if (m==0)      { printf "  GAP      %-28s %7d  (matched by no shard)\n", fork, cnt; gaps++ }
			else if (m>=2) { printf "  OVERLAP  %-28s %7d  (matched by %d shards)\n", fork, cnt, m; overlaps++ }
		}
		END {
			print "  --- per-shard totals ---"
			for (s in tot) printf "  %-18s %7d\n", s, tot[s]
			printf "  total=%d  gaps=%d  overlaps=%d\n", total, gaps+0, overlaps+0
			if (total == 0) { print "  ERROR: no forks found for this format — index format labels changed?" > "/dev/stderr"; exit 1 }
			if (gaps || overlaps) exit 1
		}
	' <(printf '%s\n' "$regexes") -
}

rc=0
check_transaction_race_shards || rc=1
echo
check_partition "spec race shards (blocktest --run)"  "$stable_index" blockchain_test        ""                     "$(race_regexes)" || rc=1
echo
check_race_mode_parity || rc=1
echo
check_partition "hive stable consume-enginex (sim-limit)" "$stable_index" blockchain_test_engine_x "eels/consume-enginex/" "$(hive_consume_enginex_regexes eest_stable)" || rc=1
echo
if [[ -f "$devnet_index" ]]; then
	check_partition "hive devnet consume-enginex (sim-limit)" "$devnet_index" blockchain_test_engine_x "eels/consume-enginex/" "$(hive_consume_enginex_regexes eest_devnet)" || rc=1
	echo
else
	echo "==== devnet check SKIPPED (index not found: $devnet_index) ===="
	echo
fi
if (( rc == 0 )); then
	echo "EEST shard coverage OK."
else
	echo "EEST shard coverage FAILED: fix the race run regexes in tools/eest-spec-shards.yml or the hive matrix in .github/workflows/test-hive-eest.yml." >&2
fi
exit $rc
