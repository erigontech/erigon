#!/usr/bin/env bash
# Runs one shard of the EEST spec-test workflow:
#
#     tools/run-eest-spec-test.sh <shard>
#
# Where <shard> is one of:
#
#   statetests-stable                          state tests vs. eest_stable
#   statetests-devnet                          state tests vs. eest_devnet
#   blocktests-stable-sequential               blockchain tests vs. eest_stable
#   blocktests-devnet                          blockchain tests vs. eest_devnet
#   enginextests-stable-sequential             engine-x tests vs. eest_stable
#   enginextests-benchmark-{1m,5m,10m,30m,60m,100m,150m}-sequential
#                                              engine-x benchmark fixtures per
#                                              gas-target subdir; each value
#                                              maps to one for_osaka_at_<NNNN>M/
#                                              directory under the engine_x
#                                              benchmark fixtures
#   blocktests-stable-race-{pre-cancun,cancun,prague,osaka}-sequential
#                                              race-detector variant of
#                                              blocktests-stable-sequential, split by
#                                              fork via the --run regex so
#                                              each sub-shard fits under ~30
#                                              min. Caller (Makefile / CI) must
#                                              export EVM_BIN to the race-built
#                                              binary; otherwise -race
#                                              detection doesn't fire.
#   blocktests-devnet-race-amsterdam           race-detector variant filtered
#                                              to the Amsterdam fork only.
#   *-parallel                                  any of the above with "-parallel"
#                                              appended runs with
#                                              ERIGON_EXEC3_PARALLEL=true. Every
#                                              other shard runs with
#                                              ERIGON_EXEC3_PARALLEL=false so
#                                              the runtime default in
#                                              dbg.Exec3Parallel can flip without
#                                              redefining the shards.
#
# Each shard maps to one cmd/evm subcommand running with --jsonout. Pass/fail
# is decided here (not by the binary, which always exits 0): the shard fails
# if no tests ran (unexpected — fixture path bug) or if observed failures
# exceed EEST_SPEC_MAX_FAILURES.
#
# Env overrides:
#   EEST_SPEC_MAX_FAILURES   - failure budget; CI sets via matrix include:
#   EEST_SPEC_WORKERS        - parallel worker count
#   EVM_BIN                  - path to evm binary (default build/bin/evm)
#   ERIGON_EXECUTION_TESTS_TMPDIR - if set, used as TMPDIR; on Darwin we
#                                   create a ramdisk if unset

set -euo pipefail

if (( $# != 1 )); then
	echo "usage: $0 <shard>" >&2
	exit 2
fi
shard="$1"

# Resolve fixtures base from the shard name. blocktests-{stable,devnet}-race-*
# inherit from the parent shard (stable/devnet).
case "$shard" in
	*-stable*)    base=test-fixtures-cache/eest_stable/fixtures ;;
	*-devnet*)    base=test-fixtures-cache/eest_devnet/fixtures ;;
	*-benchmark*) base=test-fixtures-cache/eest_benchmark/fixtures ;;
	*) echo "cannot resolve fixtures for shard: $shard" >&2; exit 2 ;;
esac

# Resolve workers + failure budget + exec3-parallel flag from the single-source
# manifest. Both this script and the test-eest-spec.yml load-matrix job read
# tools/eest-spec-shards.json, so adding a shard / tweaking a budget is a
# one-file edit.
manifest=tools/eest-spec-shards.json
budget_row=$(jq -r --arg s "$shard" '.[] | select(.shard == $s) | "\(.workers)\t\(."max-allowed-failures")\t\(."exec3-parallel" // false)"' "$manifest")
if [[ -z "$budget_row" ]]; then
	echo "shard $shard not found in $manifest" >&2
	exit 2
fi
IFS=$'\t' read -r default_workers default_max exec3_parallel <<<"$budget_row"
# Always set ERIGON_EXEC3_PARALLEL explicitly (true or false) so the shard's
# behaviour is pinned to the manifest, independent of whatever dbg.Exec3Parallel
# defaults to at runtime. If the default flips, the shards still run the mode
# they were defined for.
export ERIGON_EXEC3_PARALLEL="$exec3_parallel"

# Strip "-parallel" / "-sequential" suffix for case-arm routing — both variants
# share the same fixture path / regex as the parent shard; only the
# ERIGON_EXEC3_PARALLEL env var differs.
shard_route="${shard%-parallel}"
shard_route="${shard_route%-sequential}"

# Per-shard structural config (cmd / fixture path / extra CLI flags). Match
# against shard_route so "-parallel" variants reuse the same arm as their
# non-parallel parent.
extra=()
case "$shard_route" in
	statetests-stable | statetests-devnet)
		cmd=statetest;   path="$base/state_tests" ;;
	blocktests-stable | blocktests-devnet)
		cmd=blocktest;   path="$base/blockchain_tests" ;;
	blocktests-stable-race-pre-cancun)
		cmd=blocktest;   path="$base/blockchain_tests"
		extra=(--run 'fork_(Frontier|Homestead|Byzantium|ConstantinopleFix|Istanbul|Berlin|London|Paris|Shanghai)') ;;
	blocktests-stable-race-cancun)
		cmd=blocktest;   path="$base/blockchain_tests"; extra=(--run 'fork_Cancun') ;;
	blocktests-stable-race-prague)
		cmd=blocktest;   path="$base/blockchain_tests"; extra=(--run 'fork_Prague') ;;
	blocktests-stable-race-osaka)
		cmd=blocktest;   path="$base/blockchain_tests"; extra=(--run 'fork_Osaka') ;;
	blocktests-devnet-race-amsterdam)
		cmd=blocktest;   path="$base/blockchain_tests"; extra=(--run 'fork_Amsterdam') ;;
	enginextests-stable)
		cmd=enginextest; path="$base/blockchain_tests_engine_x"
		extra=(--pre-alloc-dir "$path/pre_alloc") ;;
	enginextests-benchmark-*)
		# Per-gas-target shard: "...-1m" → for_osaka_at_0001M/, etc. Use
		# shard_route (with any "-parallel" suffix stripped) so the parallel
		# variants resolve to the same gas-target subdir.
		gas="${shard_route##*-}"; gas_num="${gas%m}"
		printf -v gas_dir 'for_osaka_at_%04dM' "$gas_num"
		cmd=enginextest
		path="$base/blockchain_tests_engine_x/$gas_dir"
		extra=(--pre-alloc-dir "$base/blockchain_tests_engine_x/pre_alloc" --time) ;;
	*) echo "unknown shard: $shard (route: $shard_route)" >&2; exit 2 ;;
esac

workers="${EEST_SPEC_WORKERS:-$default_workers}"
max="${EEST_SPEC_MAX_FAILURES:-$default_max}"
evm_bin="${EVM_BIN:-build/bin/evm}"

if [[ ! -x "$evm_bin" ]]; then
	echo "$evm_bin not found or not executable; run 'make evm' first" >&2
	exit 2
fi
if [[ ! -d "$path" ]]; then
	echo "fixture path $path does not exist; run 'make test-fixtures-eest' first" >&2
	exit 2
fi

result_file=$(mktemp)
ramdisk=""
cleanup() {
	# Auto-detach Darwin ramdisk we created in this run; leave existing
	# user-provided ones (ERIGON_EXECUTION_TESTS_TMPDIR) untouched.
	if [[ -n "$ramdisk" && "$(uname -s)" == "Darwin" ]]; then
		hdiutil detach -force "$ramdisk" >/dev/null 2>&1 || true
	fi
	rm -f "$result_file" 2>/dev/null || true
}
trap cleanup EXIT

# Linux: do NOT auto-create a ramdisk. tools/create-ramdisk uses sudo mount
# and never unmounts; CI gets the env var pre-set via the setup-erigon
# action's ramdisk: true input. Local Linux users can opt in by exporting
# ERIGON_EXECUTION_TESTS_TMPDIR or TMPDIR=/dev/shm.
if [[ -z "${ERIGON_EXECUTION_TESTS_TMPDIR:-}" && "$(uname -s)" == "Darwin" ]]; then
	ramdisk=$(bash tools/create-ramdisk) || true
	if [[ -n "$ramdisk" ]]; then
		export ERIGON_EXECUTION_TESTS_TMPDIR="$ramdisk"
		echo "ramdisk: $ramdisk"
	fi
fi
export TMPDIR="${ERIGON_EXECUTION_TESTS_TMPDIR:-${TMPDIR:-/tmp}}"

echo "running: $evm_bin $cmd ${extra[*]:-} --workers $workers --jsonout $path"
echo "tmpdir:  $TMPDIR"
echo "max-allowed-failures: $max"

# Don't fail on non-zero — the runners report all results via JSON regardless,
# and we want to inspect the JSON to drive the pass/fail decision. The grep
# filter strips any init-time log lines (e.g. dbg.envLookup's "[WARN] [env]"
# message when ERIGON_EXEC3_PARALLEL is set fires before cmd/evm sets the log
# handler, and the default log handler writes to stdout) so jq sees only JSON.
raw_file=$(mktemp)
"$evm_bin" "$cmd" --workers "$workers" --jsonout "${extra[@]}" "$path" > "$raw_file" || true
grep -v '^\[[A-Z][A-Z]*\]' "$raw_file" > "$result_file"
rm -f "$raw_file"

total=$(jq 'length' "$result_file")
failed=$(jq '[.[] | select(.pass == false)] | length' "$result_file")
echo "ran $total tests, $failed failed"

if (( total == 0 )); then
	echo "ERROR: no tests were run; expected non-empty fixtures at $path" >&2
	exit 1
fi
if (( failed > max )); then
	echo "ERROR: $failed failures exceed max-allowed $max" >&2
	jq -r '.[] | select(.pass == false) | "  FAIL " + .name' "$result_file" >&2
	exit 1
fi
exit 0
