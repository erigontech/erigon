#!/usr/bin/env bash
# fork-restart-transition.sh — Tier 3c of docs/plans/20260728-fork-test-reshape.md.
#
# Exercises the Phase 1 fallback path: after an in-process transition
# via debug_setFork, stop the erigon and relaunch the SAME datadir
# with --chain=<fork-name>. The fresh process picks up the fork's
# chain.Config via ChainSpecByNameOrForkDatadir.
#
# KNOWN LIMITATION (2026-07-28): Provider.Unwind's mode-B trim
# removes block .seg files + state-domain .kv/.v snapshot straddlers
# but does NOT touch the accessor/v1.1-*.vi files. The fork-datadir
# validator at storage.Initialize refuses to boot with those files
# present. Operators today produce clean fork datadirs via
# `snapshots fork-from --parent-rpc <URL>`. This script therefore
# exercises the transition but is EXPECTED TO FAIL at Phase 5 with
# a diagnostic pointing to that workflow until the in-process trim
# is extended to cover .vi files (design gap; not a Tier 3c bug).
#
# Assumes a hoodi erigon is already running at PARENT_DATADIR with
# RPC exposed at PARENT_RPC.

set -uo pipefail

PARENT_DATADIR="${PARENT_DATADIR:-/erigon/tmp/erigon-hoodi-fork-parent}"
PARENT_RPC="${PARENT_RPC:-http://127.0.0.1:19645}"
# The fork erigon reuses the parent's datadir + RPC port because it
# IS the same process class — only --chain changes across the restart.
# Using a different launcher (e.g. fork-child) drags in extra flags
# like --snap.lifecycle-driven-by-storage that don't match the
# datadir's persisted config and refuse to boot.
FORK_RPC="${FORK_RPC:-$PARENT_RPC}"
FORK_CHAIN_NAME="${FORK_CHAIN_NAME:-hoodi-fork-restart-$(date +%s)}"
CUT_BUFFER="${CUT_BUFFER:-1000}"
FORK_LAUNCH_CMD="${FORK_LAUNCH_CMD:-scripts/erigon-launch-hoodi-fork-parent.sh}"
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"
FORK_STARTUP_TIMEOUT="${FORK_STARTUP_TIMEOUT:-300}"
TRUST_ROOT_KEY="${TRUST_ROOT_KEY:-$PARENT_DATADIR/fork-test-trust-root.hex}"

ERIGON_PID_FILE="${ERIGON_PID_FILE:-/tmp/fork-restart-erigon.pid}"

stage() { echo; echo "===== $(date -u +%H:%M:%S) :: $1 ====="; }
fail() { echo "FAIL: $1" >&2; exit 1; }

command -v jq >/dev/null 2>&1 || fail "jq is required"
[[ -d "$PARENT_DATADIR" ]] || fail "PARENT_DATADIR=$PARENT_DATADIR does not exist"
[[ -x "$FORK_LAUNCH_CMD" ]] || fail "FORK_LAUNCH_CMD=$FORK_LAUNCH_CMD is not executable"
[[ -s "$TRUST_ROOT_KEY" ]] || fail "TRUST_ROOT_KEY=$TRUST_ROOT_KEY missing — parent launcher provisions it; check parent was started via erigon-launch-hoodi-fork-parent.sh"

rpc_call() {
    local url="$1" body="$2" filt="$3"
    curl -s --max-time 5 -X POST -H "Content-Type: application/json" --data "$body" "$url" | jq -r "$filt"
}

# Phase 1: query parent for head + chain ID.
stage "Phase 1: query parent RPC"
head_hex=$(rpc_call "$PARENT_RPC" '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' '.result')
[[ "$head_hex" != "null" && -n "$head_hex" ]] || fail "eth_blockNumber returned null — is the parent erigon running at $PARENT_RPC?"
head_dec=$(printf '%d\n' "$head_hex")
[[ "$head_dec" -gt "$CUT_BUFFER" ]] || fail "parent head=$head_dec too low; need > CUT_BUFFER=$CUT_BUFFER"
cut_block=$(( head_dec - CUT_BUFFER ))
parent_chain_id_hex=$(rpc_call "$PARENT_RPC" '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' '.result')
parent_chain_id=$(printf '%d\n' "$parent_chain_id_hex")
fork_chain_id=$(( parent_chain_id + 1 ))
echo "  parent head=$head_dec cut=$cut_block parent_chain_id=$parent_chain_id fork_chain_id=$fork_chain_id"

# Phase 1.5: unwind + swap in-process via debug_setFork. The fork
# datadir validator refuses to boot fresh with snap files that
# straddle CutBlock; debug_setHead alone only trims block snapshots
# (not state-domain accessors) so a naive --chain=<fork> relaunch
# fails validation. debug_setFork's in-process swap does the full
# trim as part of the mode-B unwind + Provider.Unwind flow — the
# post-transition datadir passes fork-datadir validation on relaunch.
#
# This makes Tier 3c a genuine "restart after in-process transition"
# test: exercise the same shipped fallback operators would use when
# they want a fresh process on the already-transitioned datadir.
stage "Phase 1.5: debug_setFork to $FORK_CHAIN_NAME (trims + swaps in-process)"
cat > "$PARENT_DATADIR/chain.json" <<EOF
{
  "chainName": "$FORK_CHAIN_NAME",
  "chainId": "$fork_chain_id",
  "parent": "hoodi",
  "cutBlock": $cut_block
}
EOF
ucan_file="$PARENT_DATADIR/fork-transition-ucan-$FORK_CHAIN_NAME.b64"
"$INTEGRATION_BIN" mint_fork_transition \
    --trust-root-key="$TRUST_ROOT_KEY" \
    --chain="$FORK_CHAIN_NAME" \
    --validity=1h \
    --out="$ucan_file" >/dev/null 2>&1
[[ -s "$ucan_file" ]] || fail "mint_fork_transition produced no UCAN at $ucan_file"

setfork_out=$("$INTEGRATION_BIN" set_fork \
    --chain="$FORK_CHAIN_NAME" \
    --rpcendpoint="$PARENT_RPC" \
    --authority-ucan-file="$ucan_file" 2>&1 || true)
if ! echo "$setfork_out" | grep -q '"restart_required": false'; then
    echo "$setfork_out"
    fail "debug_setFork did not return restart_required=false"
fi

# Phase 2: stop the parent erigon cleanly (SIGTERM the process
# holding the datadir lock). The driver assumes the operator started
# the parent in the foreground of a known-pid process — for the soak
# driver this pid is tracked already.
stage "Phase 2: stop parent erigon"
parent_pid=$(pgrep -f "erigon.*--datadir=?$PARENT_DATADIR" | head -n1 || true)
[[ -n "$parent_pid" ]] || fail "could not locate erigon process for $PARENT_DATADIR"
echo "  sending SIGTERM to pid=$parent_pid"
kill "$parent_pid" || fail "failed to signal parent erigon"
for i in $(seq 1 60); do
    if ! kill -0 "$parent_pid" 2>/dev/null; then
        echo "  parent exited after ${i}s"
        break
    fi
    sleep 1
done
if kill -0 "$parent_pid" 2>/dev/null; then
    fail "parent did not exit within 60s of SIGTERM"
fi

# Phase 3: chain.json was already written in Phase 1.5 for the
# in-process transition; the fork launcher reads the same file to
# resolve --chain=<fork-name> via ChainSpecByNameOrForkDatadir.

# Phase 4: launch fork erigon on the SAME datadir.
stage "Phase 4: launch fork erigon on the same datadir"
DATADIR="$PARENT_DATADIR" CHAIN="$FORK_CHAIN_NAME" "$FORK_LAUNCH_CMD" &
fork_pid=$!
echo "$fork_pid" > "$ERIGON_PID_FILE"
echo "  fork erigon pid=$fork_pid"

cleanup() {
    if [[ -f "$ERIGON_PID_FILE" ]]; then
        local p
        p=$(cat "$ERIGON_PID_FILE")
        if kill -0 "$p" 2>/dev/null; then
            echo "  cleanup: SIGTERM fork erigon pid=$p"
            kill "$p"
            wait "$p" 2>/dev/null || true
        fi
        rm -f "$ERIGON_PID_FILE"
    fi
}
trap cleanup EXIT

# Phase 5: wait for the fork's RPC to answer eth_chainId.
stage "Phase 5: wait for fork RPC"
actual="<no rpc yet>" # bind so set -u doesn't kill the post-loop check when the RPC never answered
for i in $(seq 1 "$FORK_STARTUP_TIMEOUT"); do
    fork_chain_id_hex=$(rpc_call "$FORK_RPC" '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' '.result' 2>/dev/null || echo "null")
    if [[ "$fork_chain_id_hex" != "null" && -n "$fork_chain_id_hex" ]]; then
        actual=$(printf '%d\n' "$fork_chain_id_hex")
        if [[ "$actual" == "$fork_chain_id" ]]; then
            echo "  fork RPC ready after ${i}s: eth_chainId=$actual (=$fork_chain_id)"
            break
        fi
        echo "  t=${i}s: eth_chainId=$actual (want $fork_chain_id) — still transitioning?"
    fi
    if ! kill -0 "$fork_pid" 2>/dev/null; then
        fail "fork erigon exited before RPC became ready"
    fi
    sleep 1
done
if [[ "$actual" != "$fork_chain_id" ]]; then
    fail "fork erigon RPC did not report fork chain_id=$fork_chain_id within ${FORK_STARTUP_TIMEOUT}s (last=$actual)"
fi

echo
echo "PASS: restart-between-transitions loaded $FORK_CHAIN_NAME cleanly (parent shutdown + fork bootstrap on same datadir)"
