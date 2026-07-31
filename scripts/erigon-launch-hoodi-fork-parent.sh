#!/usr/bin/env bash
# erigon-launch-hoodi-fork-parent.sh — launches a FRESH hoodi erigon
# as the parent chain for fork-testing. All ports +100 offset from
# the standard soak launcher (erigon-launch-hoodi-soak.sh) so the two
# can run concurrently. Independent datadir.

set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-fork-parent}"
LOG="${LOG:-/tmp/erigon-hoodi-fork-parent.log}"
BIN="${BIN:-./build/bin/erigon}"
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"
CHECKPOINT_URL="${CHECKPOINT_URL:-https://checkpoint-sync.hoodi.ethpandaops.io}"
# CHAIN defaults to hoodi (parent bootstrap) but can be overridden so
# the fork-restart-transition test can relaunch the SAME datadir with
# --chain=<fork-name> without dragging in the fork-child launcher's
# extra flags (which would collide with the datadir's persisted config).
CHAIN="${CHAIN:-hoodi}"

# Trust-root key for debug_setFork UCAN authority. Auto-provisioned
# per-datadir so the same key can mint transition UCANs across
# sessions; deleting the datadir also invalidates the key.
# Rotating the key mid-life is prohibited by the datadir fingerprint
# gate (backend.go), so scripts must not overwrite it.
TRUST_ROOT_KEY="${TRUST_ROOT_KEY:-$DATADIR/fork-test-trust-root.hex}"
mkdir -p "$DATADIR"
TRUST_ROOT_PUB=$("$INTEGRATION_BIN" trust_root_pubkey --key "$TRUST_ROOT_KEY" --generate-if-missing 2>/dev/null)
[[ -n "$TRUST_ROOT_PUB" ]] || { echo "FAIL: could not provision/derive trust-root pubkey" >&2; exit 1; }
echo "[launcher] trust-root pubkey: $TRUST_ROOT_PUB (key: $TRUST_ROOT_KEY)" >&2

export USE_STATE_CACHE=false
export ERIGON_MERGE_MIN_AGE_STEPS="${ERIGON_MERGE_MIN_AGE_STEPS:-6}"

# ERIGON_EXEC3_PARALLEL=false disables parallel execution. Fork tests
# use this parent as a stable substrate — they exercise fork transitions,
# not the parallel-exec path. Parallel exec has a race in the initial-
# sync ProcessFrozenBlocks completeness check that intermittently halts
# fresh hoodi sync (see leak L10 in
# docs/plans/20260731-fork-test-scope-and-leaks.md). Serial exec is
# slower but not racy; the fork test infrastructure prefers reliability
# over throughput. Override via env if you need to test the parallel
# path against a fork parent.
export ERIGON_EXEC3_PARALLEL="${ERIGON_EXEC3_PARALLEL:-false}"

exec "$BIN" \
  --datadir="$DATADIR" \
  --chain="$CHAIN" --prune.mode=minimal \
  --caplin.checkpoint-sync-url="$CHECKPOINT_URL" \
  --snap.p2p-manifest \
  --snapshot.trust-roots="$TRUST_ROOT_PUB" \
  --http.api=eth,erigon,engine,debug,net,web3,trace,txpool \
  --http.port=19645 --authrpc.port=19651 --private.api.addr=127.0.0.1:11690 \
  --torrent.port=43469 --port=31603 \
  --caplin.discovery.port=4850 --caplin.discovery.tcpport=4851 \
  --sentinel.port=8590 --beacon.api.port=6360 --mcp.port=9360 \
  --log.console.verbosity=3 >"$LOG" 2>&1
