#!/usr/bin/env bash
# Runs one shard of the spec-test workflow:
#
#     tools/run-spec-test.sh <suite> <fixtures>
#
#   suite    = statetests | blocktests | enginextests
#   fixtures = stable     | devnet
#
# Each shard maps to one cmd/evm subcommand running with --jsonout. Pass/fail
# is decided here (not by the binary, which always exits 0): the shard fails
# if no tests ran (unexpected — fixture path bug) or if observed failures
# exceed SPEC_MAX_FAILURES.
#
# Env overrides:
#   SPEC_MAX_FAILURES   - failure budget; CI sets via matrix include:
#   SPEC_WORKERS        - parallel worker count
#   EVM_BIN             - path to evm binary (default build/bin/evm)
#   ERIGON_EXECUTION_TESTS_TMPDIR - if set, used as TMPDIR; on Darwin we
#                                   create a ramdisk if unset

set -euo pipefail

if (( $# != 2 )); then
	echo "usage: $0 <suite> <fixtures>" >&2
	exit 2
fi
suite="$1"
fixtures="$2"

case "$fixtures" in
	stable) base=test-fixtures-cache/eest_stable/fixtures ;;
	devnet) base=test-fixtures-cache/eest_devnet/fixtures ;;
	*) echo "unknown fixtures: $fixtures" >&2; exit 2 ;;
esac

# Per-shard config. Worker counts: statetests/blocktests=12 (CPU-bound, scale
# with cores); enginextests=8 (memory-bound knee documented at
# cmd/evm/enginexrunner.go:101 — higher values barely help and risk MDBX
# virtual-memory exhaustion). Failure budgets mirror the values committed in
# .github/workflows/test-spec.yml's matrix.include: keep them in sync.
extra=()
case "$suite-$fixtures" in
	statetests-stable)
		cmd=statetest;    path="$base/state_tests";              default_workers=12; default_max=37 ;;
	statetests-devnet)
		cmd=statetest;    path="$base/state_tests";              default_workers=12; default_max=5253 ;;
	blocktests-stable)
		cmd=blocktest;    path="$base/blockchain_tests";         default_workers=12; default_max=0 ;;
	blocktests-devnet)
		cmd=blocktest;    path="$base/blockchain_tests";         default_workers=12; default_max=6206 ;;
	enginextests-stable)
		cmd=enginextest;  path="$base/blockchain_tests_engine_x"; default_workers=8;  default_max=0
		extra=(--pre-alloc-dir "$path/pre_alloc") ;;
	enginextests-devnet)
		# devnet tarball does not currently ship blockchain_tests_engine_x;
		# the test-spec workflow skips this shard via job-level if:.
		cmd=enginextest;  path="$base/blockchain_tests_engine_x"; default_workers=8;  default_max=0
		extra=(--pre-alloc-dir "$path/pre_alloc") ;;
	*) echo "unknown shard: $suite-$fixtures" >&2; exit 2 ;;
esac

workers="${SPEC_WORKERS:-$default_workers}"
max="${SPEC_MAX_FAILURES:-$default_max}"
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
