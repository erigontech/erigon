#!/usr/bin/env bash
# erigon-launch-hoodi-fork-child.sh — launches a fork erigon on a
# fork datadir produced by `snapshots fork-from`. Ports +200 offset
# from the standard soak launcher (+100 from the fork-parent
# launcher) so all three can run concurrently.
#
# Boot path: --chain=<fork-name> reads chain.json from the fork
# datadir via chainspec.ChainSpecByNameOrForkDatadir. The fork
# inherits genesis from the parent chain's built-in registry entry
# (chain.Config.Parent). Without Phase 2c-CL the fork cannot advance
# past CutBlock — head remains at the parent's snapshot tip.

set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-fork-child}"
CHAIN="${CHAIN:?fork chain name is required — set CHAIN=<name-from-chain.json>}"
LOG="${LOG:-/tmp/erigon-hoodi-fork-child.log}"
BIN="${BIN:-./build/bin/erigon}"
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"

# Trust-root key for debug_setFork UCAN authority. Auto-provisioned
# per-datadir on first boot; the fingerprint gate then locks it in.
# The fork's trust root is INDEPENDENT of the parent's — a caller
# transitioning back from this datadir must mint with the fork's key.
TRUST_ROOT_KEY="${TRUST_ROOT_KEY:-$DATADIR/fork-test-trust-root.hex}"
mkdir -p "$DATADIR"
TRUST_ROOT_PUB=$("$INTEGRATION_BIN" trust_root_pubkey --key "$TRUST_ROOT_KEY" --generate-if-missing 2>/dev/null)
[[ -n "$TRUST_ROOT_PUB" ]] || { echo "FAIL: could not provision/derive trust-root pubkey" >&2; exit 1; }
echo "[launcher] trust-root pubkey: $TRUST_ROOT_PUB (key: $TRUST_ROOT_KEY)" >&2
# SNAPSHOT_DELEGATION points at an Authority UCAN sidecar. Setting it
# marks this node as a permissioned publisher and lets backend.go's
# fork-initiator gate-skip branch fire (see the ce2c1719b2 tightening)
# so the ENR carries chain-toml at boot instead of waiting on the
# InitialValidationComplete signal that a pre-populated fork datadir
# never emits. Without SNAPSHOT_DELEGATION the launcher runs
# unpermissioned — the V2-publish gate stays ON and this node does
# not advertise chain-toml over the wire (safe default: any random
# fork-chain node is not automatically a canonical-view publisher).
SNAPSHOT_DELEGATION="${SNAPSHOT_DELEGATION:-}"

export USE_STATE_CACHE=false
export ERIGON_MERGE_MIN_AGE_STEPS="${ERIGON_MERGE_MIN_AGE_STEPS:-6}"

DELEGATION_FLAG=()
if [[ -n "$SNAPSHOT_DELEGATION" ]]; then
    DELEGATION_FLAG=(--snapshot.delegation="$SNAPSHOT_DELEGATION")
fi

exec "$BIN" \
  --datadir="$DATADIR" \
  --chain="$CHAIN" \
  --prune.mode=minimal \
  --snap.p2p-manifest \
  --snap.lifecycle-driven-by-storage \
  --snapshot.trust-roots="$TRUST_ROOT_PUB" \
  "${DELEGATION_FLAG[@]}" \
  --http.api=eth,erigon,engine,debug,net,web3,trace,txpool,admin \
  --http.port=19745 --authrpc.port=19751 --private.api.addr=127.0.0.1:11790 \
  --torrent.port=43669 --port=31803 \
  --caplin.discovery.port=4950 --caplin.discovery.tcpport=4951 \
  --sentinel.port=8690 --beacon.api.port=6460 --mcp.port=9460 \
  --log.console.verbosity=3 >"$LOG" 2>&1
