#!/usr/bin/env bash
# erigon-launch-hoodi-archive-publisher.sh — long-lived hoodi ARCHIVE
# publisher for the LOCAL test infrastructure. Sibling to the master
# (minimal) publisher; together they let the consumer test aggregation
# across two publisher classes:
#
#   master   (minimal): recent tip only, small footprint, small chain.toml
#   archive  (archive): full state history, serves deep-unwind history
#
# Consumer's chain.toml aggregation should union both peers' entries,
# so ensureHistoryForUnwindWalk finds the older .v/.ef files via the
# archive class even when the minimal peer never advertised them.
#
# Ports use +400 offset from the standard soak launcher (master is +300).
# Table:
#   soak:             19545 / 19551 / 11590 / 43369 / 31503 / 4750 / 4751 / 8490 / 6260 / 9260
#   fork-parent:      19645 / 19651 / 11690 / 43469 / 31603 / 4850 / 4851 / 8590 / 6360 / 9360
#   master-publisher: 19845 / 19851 / 11890 / 43669 / 31803 / 5050 / 5051 / 8790 / 6560 / 9560
#   archive-publisher:19945 / 19951 / 11990 / 43769 / 31903 / 5150 / 5151 / 8890 / 6660 / 9660
#
# Long-lived by convention — launch via nohup. Datadir persists across
# restarts; trust-root key persists so consumers pin the same pubkey
# between publisher restarts.

set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-archive-publisher}"
LOG="${LOG:-/tmp/erigon-hoodi-archive-publisher.log}"
BIN="${BIN:-./build/bin/erigon}"
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"
CHECKPOINT_URL="${CHECKPOINT_URL:-https://checkpoint-sync.hoodi.ethpandaops.io}"

# Trust-root key — separate from the master publisher's key. Consumers
# pin BOTH publishers' trust-roots via --snapshot.trust-roots=<pk1>,<pk2>.
TRUST_ROOT_KEY="${TRUST_ROOT_KEY:-$DATADIR/archive-publisher-trust-root.hex}"
mkdir -p "$DATADIR"
TRUST_ROOT_PUB=$("$INTEGRATION_BIN" trust_root_pubkey --key "$TRUST_ROOT_KEY" --generate-if-missing 2>/dev/null)
[[ -n "$TRUST_ROOT_PUB" ]] || { echo "FAIL: could not provision/derive archive-publisher trust-root pubkey" >&2; exit 1; }
echo "[archive-publisher] trust-root pubkey: $TRUST_ROOT_PUB (key: $TRUST_ROOT_KEY)" >&2
echo "[archive-publisher] consumers pin via: --snapshot.trust-roots=$TRUST_ROOT_PUB" >&2

export USE_STATE_CACHE=false
export ERIGON_MERGE_MIN_AGE_STEPS="${ERIGON_MERGE_MIN_AGE_STEPS:-6}"

PUB_ADVERTISE_IP="${PUB_ADVERTISE_IP:-127.0.0.1}"

exec "$BIN" \
  --datadir="$DATADIR" \
  --chain=hoodi --prune.mode=archive \
  --caplin.checkpoint-sync-url="$CHECKPOINT_URL" \
  --snap.bootstrap-from-preverified \
  --snap.p2p-manifest \
  --snapshot.trust-roots="$TRUST_ROOT_PUB" \
  --nodekey="$TRUST_ROOT_KEY" \
  --nat="extip:$PUB_ADVERTISE_IP" \
  --http.api=eth,erigon,engine,debug,net,web3,trace,txpool,admin \
  --http.port=19945 --authrpc.port=19951 --private.api.addr=127.0.0.1:11990 \
  --torrent.port=43769 --port=31903 \
  --caplin.discovery.port=5150 --caplin.discovery.tcpport=5151 \
  --sentinel.port=8890 --beacon.api.port=6660 --mcp.port=9660 \
  --log.console.verbosity=3 >"$LOG" 2>&1
