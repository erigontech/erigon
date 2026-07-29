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
# Fork erigon in the restart-model path reuses the parent's datadir
# AND its RPC port (per fork-restart-transition.sh header) — only
# --chain changes across the restart. Default FORK_RPC to
# $PARENT_RPC so the driver polls the port the fork actually opens.
FORK_RPC="${FORK_RPC:-$PARENT_RPC}"
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

# Detect actual startup chain via eth_chainId. hoodi (560048) means
# parent; anything else means a leftover fork from a previous run.
# The driver's loop can only start from parent (its iter-1 direction
# is parent→fork). Fork-leftover state gets reset here rather than
# manually before every rerun.
HOODI_CHAIN_ID=560048
rpc_startup_chainid_hex=$(curl -s --max-time 5 -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' "$PARENT_RPC" | jq -r .result 2>/dev/null || echo "")
if [[ -z "$rpc_startup_chainid_hex" || "$rpc_startup_chainid_hex" == "null" ]]; then
    fail "startup probe: no RPC response from $PARENT_RPC — is erigon running?"
fi
rpc_startup_chainid=$(printf '%d\n' "$rpc_startup_chainid_hex" 2>/dev/null || echo "0")
if [[ "$rpc_startup_chainid" != "$HOODI_CHAIN_ID" ]]; then
    echo "fork-soak: startup chain_id=$rpc_startup_chainid is NOT hoodi ($HOODI_CHAIN_ID) — resetting via fork→parent transition"
    # Mint a fork-transition UCAN authorising hoodi, fire it.
    reset_ucan="$PARENT_DATADIR/fork-transition-ucan-startup-reset.b64"
    "${INTEGRATION_BIN:-./build/bin/integration}" mint_fork_transition \
        --trust-root-key="${TRUST_ROOT_KEY:-$PARENT_DATADIR/fork-test-trust-root.hex}" \
        --chain=hoodi --validity=1h --out="$reset_ucan" >/dev/null 2>&1
    if [[ ! -s "$reset_ucan" ]]; then
        fail "startup-reset: could not mint UCAN"
    fi
    reset_out=$("${INTEGRATION_BIN:-./build/bin/integration}" set_fork \
        --chain=hoodi --rpcendpoint="$PARENT_RPC" \
        --authority-ucan-file="$reset_ucan" 2>&1)
    if ! echo "$reset_out" | grep -q '"to_chain": "hoodi"'; then
        echo "$reset_out"
        fail "startup-reset transition to hoodi failed"
    fi
    echo "fork-soak: reset complete; proceeding with iter loop from hoodi"
fi
current_chain="hoodi"

for ((iter=1; iter<=ITER; iter++)); do
    stage "iter $iter/$ITER"

    # MODELS controls which transition models the seeded shuffle picks
    # from. Default: rpc only — model=restart is blocked on the
    # genesis.ssz emit in ApplyPostSwapHooks (Caplin refuses to start
    # on a fork datadir without genesis.ssz; the RPC transition emits
    # cl-config.<fork>.yaml + parent-cut.<fork>.json but not the CL
    # genesis SSZ). Restore "rpc restart" once the genesis emit is
    # wired.
    MODELS="${MODELS:-rpc}"
    model=$(printf '%s\n' $MODELS | seeded_shuf "model-$iter")
    dwell_sec=$(seq 0 "$DWELL_MAX_SEC" | seeded_shuf "dwell-$iter")

    # Direction is derived from current_chain: only valid transition is
    # to the "other side" of the parent/fork pair. Fork name embeds
    # SEED + iter so re-runs of the same driver (against the same
    # datadir but with a new SEED) don't collide with erigon's cached
    # fork chain.Config — reusing a name means erigon uses the STORED
    # cutBlock, not the fresh one the driver would pick.
    if [[ "$current_chain" == "hoodi" ]]; then
        target="hoodi-fork-soak-${SEED:0:8}-$iter"
        direction="parent→fork"
    else
        target="hoodi"
        direction="fork→parent"
    fi

    echo "  iter=$iter model=$model direction=$direction target=$target dwell=${dwell_sec}s"

    iter_ok=true
    # target_is_parent is true when the transition returns to the
    # parent chain ("hoodi") — drives the RPC driver's fork→parent
    # branch (no chain.json emit, relaxed unwound_to assertion).
    if [[ "$direction" == "fork→parent" ]]; then
        target_is_parent=true
    else
        target_is_parent=false
    fi

    case "$model" in
      rpc)
        FORK_CHAIN_NAME="$target" PARENT_DATADIR="$PARENT_DATADIR" \
          PARENT_RPC="$PARENT_RPC" CUT_BUFFER="$CUT_BUFFER" \
          TARGET_IS_PARENT="$target_is_parent" \
          "$RPC_SCRIPT" || iter_ok=false
        ;;
      restart)
        FORK_CHAIN_NAME="$target" PARENT_DATADIR="$PARENT_DATADIR" \
          PARENT_RPC="$PARENT_RPC" FORK_RPC="$FORK_RPC" \
          CUT_BUFFER="$CUT_BUFFER" \
          TARGET_IS_PARENT="$target_is_parent" \
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
