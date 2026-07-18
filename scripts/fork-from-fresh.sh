#!/usr/bin/env bash
# fork-from-fresh.sh — orchestrates the F-1 fresh-download fork test:
#   1. Wait for the parent erigon (started by erigon-launch-hoodi-fork-
#      parent.sh) to reach a workable state (>= N retired snapshot chunks).
#   2. Pick a cut block a safe margin below head.
#   3. Run `snapshots fork-from --parent-rpc` against the parent's RPC.
#   4. Verify the fork datadir's shape — chain.json contents, snapshot
#      copy plan, no straddlers, parent-cut.json artefact.
#
# Does NOT boot the fork erigon: that's blocked on the chain.json-from-
# datadir loader (F-1/6). Once that lands, this script's Phase 4 will
# extend to a launcher invocation.

set -u

PARENT_DATADIR="${PARENT_DATADIR:-/erigon/tmp/erigon-hoodi-fork-parent}"
PARENT_RPC="${PARENT_RPC:-http://127.0.0.1:19645}"
PARENT_LOG="${PARENT_LOG:-/tmp/erigon-hoodi-fork-parent.log}"

FORK_DATADIR="${FORK_DATADIR:-/erigon/tmp/erigon-hoodi-fork-child}"
FORK_CHAIN_NAME="${FORK_CHAIN_NAME:-hoodi-fork-$(date +%s)}"

CUT_BUFFER="${CUT_BUFFER:-5000}"        # blocks below head to pick cut
MIN_RETIRED_CHUNKS="${MIN_RETIRED_CHUNKS:-3}"  # wait for this many .kv chunks

SNAPSHOTS_BIN="${SNAPSHOTS_BIN:-./build/bin/snapshots}"

stage() { echo; echo "===== $(date -u +%H:%M:%S) :: $1 ====="; }

# Phase 1: wait for parent to reach a workable state.
stage "Phase 1: wait for parent to have >= $MIN_RETIRED_CHUNKS retired commitment .kv chunks"
POLL_SEC=60
HARDCAP_SEC=10800  # 3h
start=$(date +%s)
while true; do
    if [[ ! -d "$PARENT_DATADIR/snapshots/domain" ]]; then
        elapsed=$(( $(date +%s) - start ))
        echo "  t=${elapsed}s: no snapshots/domain dir yet"
    else
        chunks=$(ls "$PARENT_DATADIR/snapshots/domain/"v*-commitment.*.kv 2>/dev/null | wc -l)
        head_hex=$(curl -s --max-time 5 -X POST -H "Content-Type: application/json" \
            --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' "$PARENT_RPC" | \
            jq -r '.result // "null"' 2>/dev/null || echo "null")
        head_dec=$( [[ "$head_hex" != "null" && -n "$head_hex" ]] && printf '%d\n' "$head_hex" || echo 0 )
        elapsed=$(( $(date +%s) - start ))
        echo "  t=${elapsed}s: commitment_chunks=$chunks head=$head_dec"
        if [[ "$chunks" -ge "$MIN_RETIRED_CHUNKS" && "$head_dec" -gt 100000 ]]; then
            echo "  parent ready: $chunks chunks retired, head=$head_dec"
            break
        fi
    fi
    if [[ $elapsed -ge $HARDCAP_SEC ]]; then
        echo "FAIL: parent sync did not reach $MIN_RETIRED_CHUNKS chunks within ${HARDCAP_SEC}s"
        exit 1
    fi
    sleep "$POLL_SEC"
done

# Phase 2: pick cut block.
stage "Phase 2: choose cut block"
head_hex=$(curl -s --max-time 10 -X POST -H "Content-Type: application/json" \
    --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' "$PARENT_RPC" | \
    jq -r '.result')
HEAD=$(printf '%d\n' "$head_hex")
CUT_BLOCK=$(( HEAD - CUT_BUFFER ))
echo "  head=$HEAD → cut_block=$CUT_BLOCK (buffer=$CUT_BUFFER)"
if [[ $CUT_BLOCK -le 0 ]]; then
    echo "FAIL: cut block computed as $CUT_BLOCK (head=$HEAD buffer=$CUT_BUFFER)"
    exit 1
fi

# Phase 3: run snapshots fork-from.
stage "Phase 3: snapshots fork-from --parent-rpc"
rm -rf "$FORK_DATADIR"
mkdir -p "$(dirname "$FORK_DATADIR")"

FORK_FROM_LOG="/tmp/fork-from-$(date -u +%Y-%m-%dT%H%M%S).log"
if ! "$SNAPSHOTS_BIN" fork-from \
    --parent-rpc "$PARENT_RPC" \
    --parent-chain hoodi \
    --parent-datadir "$PARENT_DATADIR" \
    --cut-block "$CUT_BLOCK" \
    --new-chain-name "$FORK_CHAIN_NAME" \
    --new-datadir "$FORK_DATADIR" 2>&1 | tee "$FORK_FROM_LOG"; then
    echo "FAIL: snapshots fork-from returned non-zero"
    exit 1
fi
echo "  fork datadir written: $FORK_DATADIR"
echo "  fork-from log: $FORK_FROM_LOG"

# Phase 4: verify fork datadir shape.
stage "Phase 4: verify fork datadir shape"
FAIL=0

CHAIN_JSON="$FORK_DATADIR/chain.json"
if [[ ! -f "$CHAIN_JSON" ]]; then
    echo "FAIL: $CHAIN_JSON missing"
    FAIL=1
else
    got_name=$(jq -r '.chainName // "null"' "$CHAIN_JSON")
    got_parent=$(jq -r '.parent // "null"' "$CHAIN_JSON")
    got_cut=$(jq -r '.cutBlock // "null"' "$CHAIN_JSON")
    got_pmh=$(jq -r '.parentManifestHash // "null"' "$CHAIN_JSON")
    got_pgh=$(jq -r '.parentGenesisHash // "null"' "$CHAIN_JSON")
    echo "  chain.json ChainName=$got_name Parent=$got_parent CutBlock=$got_cut"
    echo "  ParentManifestHash=$got_pmh"
    echo "  ParentGenesisHash=$got_pgh"

    [[ "$got_name" == "$FORK_CHAIN_NAME" ]] || { echo "FAIL: ChainName mismatch (want=$FORK_CHAIN_NAME got=$got_name)"; FAIL=1; }
    [[ "$got_parent" == "hoodi" ]]          || { echo "FAIL: Parent mismatch (want=hoodi got=$got_parent)"; FAIL=1; }
    [[ "$got_cut" == "$CUT_BLOCK" ]]        || { echo "FAIL: CutBlock mismatch (want=$CUT_BLOCK got=$got_cut)"; FAIL=1; }
    [[ -n "$got_pgh" && "$got_pgh" != "null" && "$got_pgh" != "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=" ]] \
        || { echo "FAIL: ParentGenesisHash empty (expected non-zero — chainspec lookup for hoodi)"; FAIL=1; }
fi

if [[ ! -d "$FORK_DATADIR/snapshots" ]]; then
    echo "FAIL: $FORK_DATADIR/snapshots missing"
    FAIL=1
else
    total=$(find "$FORK_DATADIR/snapshots" -maxdepth 3 -type f | wc -l)
    echo "  fork snapshots total files: $total"
fi

# Straddle check: no block-snapshot file may straddle CutBlock.
CUT_CHUNK=$(( CUT_BLOCK / 1000 ))
straddlers=$(find "$FORK_DATADIR/snapshots" -maxdepth 1 -name 'v*-headers.seg' 2>/dev/null | while read -r f; do
    name=$(basename "$f")
    range=$(echo "$name" | grep -oE '^v[0-9.]+-[0-9]+-[0-9]+' | sed -E 's/^v[0-9.]+-//')
    from=$(echo "$range" | cut -d- -f1)
    to=$(echo "$range" | cut -d- -f2)
    if [[ -n "$from" && -n "$to" && $((10#$from)) -le $CUT_CHUNK && $((10#$to)) -gt $CUT_CHUNK ]]; then
        echo "$name"
    fi
done)
if [[ -n "$straddlers" ]]; then
    echo "FAIL: fork snapshots contain straddle files:"
    echo "$straddlers" | sed 's/^/    /'
    FAIL=1
else
    echo "  no block-file straddlers over cut_chunk=$CUT_CHUNK"
fi

# parent-cut artifact — only if --save-parent-cut was supplied; not
# required here since we captured live.
if [[ -f "$FORK_DATADIR/parent-cut.json" ]]; then
    echo "  parent-cut.json present"
fi

if [[ $FAIL -eq 0 ]]; then
    stage "Result: PASS"
    echo "  fork datadir: $FORK_DATADIR"
    echo "  fork chain: $FORK_CHAIN_NAME"
    exit 0
else
    stage "Result: FAIL"
    exit 1
fi
