#!/usr/bin/env bash
# fork-restart-transition.sh — Tier 3c of docs/plans/20260728-fork-test-reshape.md.
#
# Exercises the Phase 1 fallback path: shut down an erigon on the
# parent chain, restart the SAME datadir with --chain=<fork-name>,
# confirm the new process loads the fork's chain.Config via
# chainspec.ChainSpecByNameOrForkDatadir and answers eth_chainId
# with the fork ID.
#
# This is the "restart is required" story operators fall back on when
# debug_setFork returns RestartRequired=true (or the operator prefers
# a clean lifecycle over the in-process swap). It also regressions
# the shipped-today boot path for --chain=<fork-name>.
#
# Assumes a hoodi erigon is already running at PARENT_DATADIR with
# RPC exposed at PARENT_RPC and the driver's caller can launch a
# fresh erigon via FORK_LAUNCH_CMD (typically
# erigon-launch-hoodi-fork-child.sh with matching env). Composable
# with Tier 4 soak driver.

set -uo pipefail

PARENT_DATADIR="${PARENT_DATADIR:-/erigon/tmp/erigon-hoodi-fork-parent}"
PARENT_RPC="${PARENT_RPC:-http://127.0.0.1:19645}"
FORK_RPC="${FORK_RPC:-http://127.0.0.1:19745}"
FORK_CHAIN_NAME="${FORK_CHAIN_NAME:-hoodi-fork-restart-$(date +%s)}"
CUT_BUFFER="${CUT_BUFFER:-1000}"
FORK_LAUNCH_CMD="${FORK_LAUNCH_CMD:-scripts/erigon-launch-hoodi-fork-child.sh}"
FORK_STARTUP_TIMEOUT="${FORK_STARTUP_TIMEOUT:-300}"

ERIGON_PID_FILE="${ERIGON_PID_FILE:-/tmp/fork-restart-erigon.pid}"

stage() { echo; echo "===== $(date -u +%H:%M:%S) :: $1 ====="; }
fail() { echo "FAIL: $1" >&2; exit 1; }

command -v jq >/dev/null 2>&1 || fail "jq is required"
[[ -d "$PARENT_DATADIR" ]] || fail "PARENT_DATADIR=$PARENT_DATADIR does not exist"
[[ -x "$FORK_LAUNCH_CMD" ]] || fail "FORK_LAUNCH_CMD=$FORK_LAUNCH_CMD is not executable"

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

# Phase 3: write chain.json into the datadir so
# chainspec.ChainSpecByNameOrForkDatadir resolves the fork target.
stage "Phase 3: write chain.json for $FORK_CHAIN_NAME"
cat > "$PARENT_DATADIR/chain.json" <<EOF
{
  "chainName": "$FORK_CHAIN_NAME",
  "chainId": "$fork_chain_id",
  "parent": "hoodi",
  "cutBlock": $cut_block
}
EOF

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
