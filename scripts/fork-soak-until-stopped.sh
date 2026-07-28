#!/usr/bin/env bash
# fork-soak-until-stopped.sh — Tier 4 of docs/plans/20260728-fork-test-reshape.md.
#
# Chains many fork transitions on a single long-lived hoodi datadir,
# randomizing each iteration across:
#   - Model: RPC in-process (fork-rpc-transition.sh) vs. process-
#     restart (fork-restart-transition.sh)
#   - Direction: parent → fork or fork → parent
#   - Dwell: 0–DWELL_MAX_SEC between iterations
#
# Seed-replayable: SEED is either supplied via env or generated via
# openssl rand; each per-choice shuffle uses "pass:$SEED-<purpose>-$iter"
# so a failing iteration re-runs deterministically with the same SEED.
#
# stop-on-fail by default (KEEP_GOING=true to continue for statistical
# runs). Composable — assumes the operator started the parent erigon
# via the standard fork-parent launcher.

set -uo pipefail

PARENT_DATADIR="${PARENT_DATADIR:-/erigon/tmp/erigon-hoodi-fork-parent}"
PARENT_RPC="${PARENT_RPC:-http://127.0.0.1:19645}"
FORK_RPC="${FORK_RPC:-http://127.0.0.1:19745}"
ITER="${ITER:-10}"
DWELL_MAX_SEC="${DWELL_MAX_SEC:-300}"
CUT_BUFFER="${CUT_BUFFER:-1000}"
KEEP_GOING="${KEEP_GOING:-false}"
SEED="${SEED:-$(openssl rand -hex 16)}"

RPC_SCRIPT="${RPC_SCRIPT:-scripts/fork-rpc-transition.sh}"
RESTART_SCRIPT="${RESTART_SCRIPT:-scripts/fork-restart-transition.sh}"

command -v openssl >/dev/null 2>&1 || { echo "FAIL: openssl required"; exit 1; }
command -v shuf >/dev/null 2>&1 || { echo "FAIL: shuf required"; exit 1; }
[[ -x "$RPC_SCRIPT" ]] || { echo "FAIL: RPC_SCRIPT=$RPC_SCRIPT not executable"; exit 1; }
[[ -x "$RESTART_SCRIPT" ]] || { echo "FAIL: RESTART_SCRIPT=$RESTART_SCRIPT not executable"; exit 1; }

stage() { echo; echo "===== $(date -u +%H:%M:%S) :: $1 ====="; }
fail() { echo "FAIL: $1" >&2; exit 1; }

# seeded_shuf picks one line from stdin using a per-purpose sub-seed
# so shuffles for different choices don't correlate.
seeded_shuf() {
    local purpose="$1"
    shuf -n 1 --random-source=<(openssl enc -aes-256-ctr -pass "pass:$SEED-$purpose" -nosalt < /dev/zero 2>/dev/null)
}

echo "fork-soak: SEED=$SEED ITER=$ITER DWELL_MAX_SEC=$DWELL_MAX_SEC KEEP_GOING=$KEEP_GOING"
echo "fork-soak: PARENT_DATADIR=$PARENT_DATADIR PARENT_RPC=$PARENT_RPC"

pass_count=0
fail_count=0
current_chain="hoodi" # assume driver starts with parent as current

for ((iter=1; iter<=ITER; iter++)); do
    stage "iter $iter/$ITER"

    model=$(printf 'rpc\nrestart\n' | seeded_shuf "model-$iter")
    dwell_sec=$(seq 0 "$DWELL_MAX_SEC" | seeded_shuf "dwell-$iter")

    # Direction is derived from current_chain: only valid transition is
    # to the "other side" of the parent/fork pair.
    if [[ "$current_chain" == "hoodi" ]]; then
        target="hoodi-fork-soak-$iter"
        direction="parent→fork"
    else
        target="hoodi"
        direction="fork→parent"
    fi

    echo "  iter=$iter model=$model direction=$direction target=$target dwell=${dwell_sec}s"

    iter_ok=true
    case "$model" in
      rpc)
        FORK_CHAIN_NAME="$target" PARENT_DATADIR="$PARENT_DATADIR" \
          PARENT_RPC="$PARENT_RPC" CUT_BUFFER="$CUT_BUFFER" \
          "$RPC_SCRIPT" || iter_ok=false
        ;;
      restart)
        FORK_CHAIN_NAME="$target" PARENT_DATADIR="$PARENT_DATADIR" \
          PARENT_RPC="$PARENT_RPC" FORK_RPC="$FORK_RPC" \
          CUT_BUFFER="$CUT_BUFFER" \
          "$RESTART_SCRIPT" || iter_ok=false
        ;;
      *)
        fail "unknown model=$model — should be rpc or restart"
        ;;
    esac

    if $iter_ok; then
        pass_count=$((pass_count + 1))
        current_chain="$target"
        echo "  iter $iter PASS (running total: pass=$pass_count fail=$fail_count)"
    else
        fail_count=$((fail_count + 1))
        echo "  iter $iter FAIL (running total: pass=$pass_count fail=$fail_count)"
        if [[ "$KEEP_GOING" != "true" ]]; then
            echo
            echo "STOP on first failure. Re-run this iteration deterministically with:"
            echo "  SEED=$SEED ITER=$iter $0"
            exit 1
        fi
    fi

    if (( iter < ITER && dwell_sec > 0 )); then
        echo "  dwelling ${dwell_sec}s before next iter"
        sleep "$dwell_sec"
    fi
done

echo
echo "fork-soak SUMMARY: pass=$pass_count fail=$fail_count of $ITER iters (SEED=$SEED)"
if (( fail_count > 0 )); then
    exit 1
fi
