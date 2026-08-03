#!/usr/bin/env bash
# erigon-launch-hoodi-modec-verify-preverified.sh — mode-C Config B verify
# launcher pinned to the preverified-bootstrap path.
#
# Sibling of erigon-launch-hoodi-soak.sh (which pins the p2p-manifest
# path). Mode-C's v4 emit + retire supersede + restart-safety fixes
# must work identically under both bootstrap paths — a defect that
# only surfaces under one is still a defect. This launcher gives the
# preverified-path leg of the two-mode Config B verification.
#
# Differences from erigon-launch-hoodi-soak.sh:
#   - --snap.bootstrap-from-preverified (was absent)
#   - --snap.p2p-manifest dropped (isolates the bootstrap source; peer
#     manifest gossip has its own coverage under the manifest-only
#     launcher and shouldn't shadow the preverified path here)
set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-modec-verify-preverified}"
LOG="${LOG:-/tmp/erigon-hoodi-modec-verify-preverified.log}"
BIN="${BIN:-./build/bin/erigon}"
CHECKPOINT_URL="${CHECKPOINT_URL:-https://checkpoint-sync.hoodi.ethpandaops.io}"

export USE_STATE_CACHE=false
export ERIGON_MERGE_MIN_AGE_STEPS="${ERIGON_MERGE_MIN_AGE_STEPS:-6}"

exec "$BIN" \
  --datadir="$DATADIR" \
  --chain=hoodi --prune.mode=minimal \
  --caplin.checkpoint-sync-url="$CHECKPOINT_URL" \
  --snap.bootstrap-from-preverified \
  --http.api=eth,erigon,engine,debug,net,web3,trace,txpool \
  --http.port=19545 --authrpc.port=19551 --private.api.addr=127.0.0.1:11590 \
  --torrent.port=43369 --port=31503 \
  --caplin.discovery.port=4750 --caplin.discovery.tcpport=4751 \
  --sentinel.port=8490 --beacon.api.port=6260 --mcp.port=9260 \
  --log.console.verbosity=3 >"$LOG" 2>&1
