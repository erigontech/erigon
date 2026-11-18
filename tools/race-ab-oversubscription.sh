#!/usr/bin/env bash
# Oversubscription A/B for the blocktests-devnet-race-amsterdam shard shape:
# the CI runner gives the shard 4 vCPU, the manifest asks for 12 workers.
# Measure workers=12 against workers=4 under a simulated 4-core budget
# (taskset pins all threads, including cgo/mdbx, like the runner's cgroup).
# Runs the two arms interleaved so slow drift hits both arms equally.
#
# Requires: build/bin/evm.race, eest_devnet fixtures downloaded
# (bash tools/test-fixtures.sh test-fixtures.json test-fixtures-cache eest_devnet).
#
# Env: RUNS (runs per arm, default 3), CORES (pin list, default 0-3).
set -euo pipefail
cd "$(dirname "$0")/.."

runs=${RUNS:-3}
cores=${CORES:-0-3}
fixtures=test-fixtures-cache/eest_devnet/fixtures/blockchain_tests
evm=build/bin/evm.race
[[ -x "$evm" ]] || { echo "$evm not found; run 'make evm.race'" >&2; exit 2; }
[[ -d "$fixtures" ]] || { echo "$fixtures missing; download eest_devnet fixtures first" >&2; exit 2; }

for i in $(seq 1 "$runs"); do
	for w in 12 4; do
		log="/tmp/ab-w${w}-r${i}"
		echo "== run ${i}/${runs} workers ${w} cores ${cores} =="
		taskset -c "$cores" env GOMAXPROCS=4 GOMEMLIMIT=4GiB ERIGON_COMMITMENT_PARALLEL=true \
			/usr/bin/time -v "$evm" blocktest --workers "$w" --run fork_Amsterdam "$fixtures" \
			> "${log}.out" 2> "${log}.time" || true
		echo "workers ${w}: $(grep -m1 'Elapsed (wall clock)' "${log}.time") | $(tail -1 "${log}.out")"
	done
done
