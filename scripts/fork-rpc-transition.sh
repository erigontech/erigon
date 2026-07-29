#!/usr/bin/env bash
# fork-rpc-transition.sh — Tier 3b of docs/plans/20260728-fork-test-reshape.md.
#
# Drives `integration set_fork` against a running parent-chain erigon
# to exercise the in-process debug_setFork RPC path end-to-end:
#
#   1. Assumes a hoodi erigon is already running at PARENT_DATADIR with
#      RPC exposed at PARENT_RPC. Sync state must be past CutBlock.
#   2. Writes a chain.json for FORK_CHAIN_NAME into PARENT_DATADIR so
#      chainspec.ChainSpecByNameOrForkDatadir can resolve the target.
#   3. Runs `integration set_fork --chain=$FORK_CHAIN_NAME` — the
#      running erigon walks the captor list, swaps chain.Config in-
#      process, and returns SetForkResult over JSON-RPC.
#   4. Asserts restart_required=false and unwound_to==CutBlock.
#   5. Confirms the transition took effect via eth_chainId (target ID).
#
# Does NOT start or stop the erigon process — that's the operator's
# responsibility so the script is composable with existing launchers
# (erigon-launch-hoodi-*.sh) and with the fork soak driver Tier 4.

set -uo pipefail

PARENT_DATADIR="${PARENT_DATADIR:-/erigon/tmp/erigon-hoodi-fork-parent}"
PARENT_RPC="${PARENT_RPC:-http://127.0.0.1:19645}"
FORK_CHAIN_NAME="${FORK_CHAIN_NAME:-hoodi-fork-rpc-$(date +%s)}"
CUT_BUFFER="${CUT_BUFFER:-1000}"
TRUST_ROOT_KEY="${TRUST_ROOT_KEY:-$PARENT_DATADIR/fork-test-trust-root.hex}"

INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"

stage() { echo; echo "===== $(date -u +%H:%M:%S) :: $1 ====="; }
fail() { echo "FAIL: $1" >&2; exit 1; }

command -v jq >/dev/null 2>&1 || fail "jq is required"
[[ -d "$PARENT_DATADIR" ]] || fail "PARENT_DATADIR=$PARENT_DATADIR does not exist"
[[ -x "$INTEGRATION_BIN" ]] || fail "INTEGRATION_BIN=$INTEGRATION_BIN is not executable (run: make integration)"
[[ -s "$TRUST_ROOT_KEY" ]] || fail "TRUST_ROOT_KEY=$TRUST_ROOT_KEY missing — the parent launcher provisions it; check the running erigon was started via erigon-launch-hoodi-fork-parent.sh"

rpc_call() {
    curl -s --max-time 5 -X POST -H "Content-Type: application/json" \
        --data "$1" "$PARENT_RPC" | jq -r "$2"
}

# Phase 1: probe parent RPC and pick a CutBlock.
stage "Phase 1: query parent RPC"
head_hex=$(rpc_call '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' '.result')
[[ "$head_hex" != "null" && -n "$head_hex" ]] || fail "eth_blockNumber returned null — is the parent erigon running at $PARENT_RPC?"
head_dec=$(printf '%d\n' "$head_hex")
[[ "$head_dec" -gt "$CUT_BUFFER" ]] || fail "parent head=$head_dec too low; need > CUT_BUFFER=$CUT_BUFFER"
cut_block=$(( head_dec - CUT_BUFFER ))
echo "  parent head=$head_dec cut_block=$cut_block"

parent_chain_id_hex=$(rpc_call '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' '.result')
parent_chain_id=$(printf '%d\n' "$parent_chain_id_hex")
fork_chain_id=$(( parent_chain_id + 1 ))
echo "  parent chain_id=$parent_chain_id fork chain_id=$fork_chain_id"

# Phase 2: write chain.json for the fork target into the parent datadir.
stage "Phase 2: write chain.json for $FORK_CHAIN_NAME"
chain_json="$PARENT_DATADIR/chain.json"
cat > "$chain_json" <<EOF
{
  "chainName": "$FORK_CHAIN_NAME",
  "chainId": "$fork_chain_id",
  "parent": "hoodi",
  "cutBlock": $cut_block
}
EOF
echo "  wrote $chain_json"

# Phase 3a: mint a fork-transition UCAN for this specific target.
stage "Phase 3a: mint fork-transition UCAN"
ucan_file="$PARENT_DATADIR/fork-transition-ucan-$FORK_CHAIN_NAME.b64"
"$INTEGRATION_BIN" mint_fork_transition \
    --trust-root-key="$TRUST_ROOT_KEY" \
    --chain="$FORK_CHAIN_NAME" \
    --validity=1h \
    --out="$ucan_file" 2>&1 | tail -5
[[ -s "$ucan_file" ]] || fail "mint_fork_transition produced no output at $ucan_file"
echo "  UCAN written to $ucan_file ($(wc -c < "$ucan_file") bytes)"

# Phase 3b: run integration set_fork with the UCAN attached.
stage "Phase 3b: integration set_fork --chain=$FORK_CHAIN_NAME"
result_json=$("$INTEGRATION_BIN" set_fork \
    --chain="$FORK_CHAIN_NAME" \
    --rpcendpoint="$PARENT_RPC" \
    --authority-ucan-file="$ucan_file" 2>&1)
echo "$result_json"

# Phase 4: parse + assert the SetForkResult payload.
stage "Phase 4: assert result payload"
if echo "$result_json" | grep -q '^Error:'; then
    fail "integration set_fork returned an error: $result_json"
fi
# Extract the JSON block (integration prints logs to stderr; set_fork's
# actual JSON body goes to stdout — grep for the object).
json_block=$(echo "$result_json" | awk '/^{/,/^}/')
[[ -n "$json_block" ]] || fail "no JSON block in set_fork output"

restart_required=$(echo "$json_block" | jq -r '.restart_required')
unwound_to=$(echo "$json_block" | jq -r '.unwound_to')
to_chain=$(echo "$json_block" | jq -r '.to_chain')

[[ "$restart_required" == "false" ]] || fail "restart_required=$restart_required (want false — in-process swap should succeed)"
[[ "$unwound_to" == "$cut_block" ]] || fail "unwound_to=$unwound_to (want $cut_block)"
[[ "$to_chain" == "$FORK_CHAIN_NAME" ]] || fail "to_chain=$to_chain (want $FORK_CHAIN_NAME)"

# Phase 5: confirm the RPC now reports the fork chain.
stage "Phase 5: confirm eth_chainId now returns fork chain_id"
post_chain_id_hex=$(rpc_call '{"jsonrpc":"2.0","method":"eth_chainId","params":[],"id":1}' '.result')
post_chain_id=$(printf '%d\n' "$post_chain_id_hex")
[[ "$post_chain_id" == "$fork_chain_id" ]] || fail "post-transition eth_chainId=$post_chain_id (want $fork_chain_id) — chain.Config swap did not stick"

echo
echo "PASS: $FORK_CHAIN_NAME transition completed in-process — no restart required"
