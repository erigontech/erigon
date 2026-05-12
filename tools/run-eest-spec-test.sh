#!/usr/bin/env bash
# Runs one shard of the EEST spec-test workflow:
#
#     tools/run-eest-spec-test.sh <shard>
#
# Where <shard> is one of:
#
#   statetests-stable                          state tests vs. eest_stable
#   statetests-devnet                          state tests vs. eest_devnet
#   blocktests-stable                          blockchain tests vs. eest_stable
#   blocktests-devnet                          blockchain tests vs. eest_devnet
#   enginextests-stable                        engine-x tests vs. eest_stable
#   enginextests-benchmark-{1m,5m,10m,30m,60m,100m,150m}
#                                              engine-x benchmark fixtures per
#                                              gas-target subdir; each value
#                                              maps to one for_osaka_at_<NNNN>M/
#                                              directory under the engine_x
#                                              benchmark fixtures
#   blocktests-stable-race-{pre-cancun,cancun,prague,osaka}
#                                              race-detector variant of
#                                              blocktests-stable, split by
#                                              fork via the --run regex so
#                                              each sub-shard fits under ~30
#                                              min. Caller (Makefile / CI) must
#                                              export EVM_BIN to the race-built
#                                              binary; otherwise -race
#                                              detection doesn't fire.
#   blocktests-devnet-race-amsterdam           race-detector variant filtered
#                                              to the Amsterdam fork only.
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

# Per-shard config. Worker counts: statetests/blocktests=12 (CPU-bound, scale
# with cores); enginextests=8 (memory-bound knee documented at
# cmd/evm/enginexrunner.go:101 — higher values barely help and risk MDBX
# virtual-memory exhaustion); benchmark=1 (clean per-test wall-time via
# --time, not noised by sibling workers). Failure budgets mirror the values
# committed in .github/workflows/test-eest-spec.yml's matrix.include:; keep
# them in sync.
extra=()
case "$shard" in
	statetests-stable)
		cmd=statetest;    path="$base/state_tests";              default_workers=12; default_max=37 ;;
	statetests-devnet)
		cmd=statetest;    path="$base/state_tests";              default_workers=12; default_max=5253 ;;
	blocktests-stable)
		cmd=blocktest;    path="$base/blockchain_tests";         default_workers=12; default_max=0 ;;
	blocktests-devnet)
		cmd=blocktest;    path="$base/blockchain_tests";         default_workers=12; default_max=6206 ;;

	# Race-detector variants of blocktests-{stable,devnet}, split by fork
	# via --run regex. Caller must export EVM_BIN to the race-built binary.
	# Distribution is documented in the workflow file.
	blocktests-stable-race-pre-cancun)
		cmd=blocktest; path="$base/blockchain_tests"; default_workers=12; default_max=0
		extra=(--run 'fork_(Frontier|Homestead|Byzantium|ConstantinopleFix|Istanbul|Berlin|London|Paris|Shanghai)') ;;
	blocktests-stable-race-cancun)
		cmd=blocktest; path="$base/blockchain_tests"; default_workers=12; default_max=0
		extra=(--run 'fork_Cancun') ;;
	blocktests-stable-race-prague)
		cmd=blocktest; path="$base/blockchain_tests"; default_workers=12; default_max=0
		extra=(--run 'fork_Prague') ;;
	blocktests-stable-race-osaka)
		cmd=blocktest; path="$base/blockchain_tests"; default_workers=12; default_max=0
		extra=(--run 'fork_Osaka') ;;
	blocktests-devnet-race-amsterdam)
		cmd=blocktest; path="$base/blockchain_tests"; default_workers=12; default_max=6140
		extra=(--run 'fork_Amsterdam') ;;

	enginextests-stable)
		cmd=enginextest;  path="$base/blockchain_tests_engine_x"; default_workers=8;  default_max=0
		extra=(--pre-alloc-dir "$path/pre_alloc") ;;

	enginextests-benchmark-1m   | \
	enginextests-benchmark-5m   | \
	enginextests-benchmark-10m  | \
	enginextests-benchmark-30m  | \
	enginextests-benchmark-60m  | \
	enginextests-benchmark-100m | \
	enginextests-benchmark-150m)
		# Per-gas-target benchmark shard. workers=1 so per-test wall-time
		# recorded via --time isn't noised by sibling goroutines competing
		# for CPU/MDBX.
		gas="${shard##*-}"
		gas_num="${gas%m}"
		printf -v gas_dir 'for_osaka_at_%04dM' "$gas_num"
		cmd=enginextest
		path="$base/blockchain_tests_engine_x/$gas_dir"
		default_workers=1
		extra=(--pre-alloc-dir "$base/blockchain_tests_engine_x/pre_alloc" --time)
		case "$shard" in
			enginextests-benchmark-1m)   default_max=0 ;;
			enginextests-benchmark-5m)   default_max=0 ;;
			enginextests-benchmark-10m)  default_max=0 ;;
			enginextests-benchmark-30m)  default_max=0 ;;
			enginextests-benchmark-60m)  default_max=7 ;;
			enginextests-benchmark-100m) default_max=7 ;;
			enginextests-benchmark-150m) default_max=59 ;;
		esac ;;

	*) echo "unknown shard: $shard" >&2; exit 2 ;;
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
# and we want to inspect the JSON to drive the pass/fail decision.
"$evm_bin" "$cmd" --workers "$workers" --jsonout "${extra[@]}" "$path" > "$result_file" || true

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
