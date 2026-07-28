#!/usr/bin/env bash
# erigon-launch-hoodi-fork-parent.sh — launches a FRESH hoodi erigon
# as the parent chain for fork-testing. All ports +100 offset from
# the standard soak launcher (erigon-launch-hoodi-soak.sh) so the two
# can run concurrently. Independent datadir.

set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-fork-parent}"
LOG="${LOG:-/tmp/erigon-hoodi-fork-parent.log}"
BIN="${BIN:-./build/bin/erigon}"
CHECKPOINT_URL="${CHECKPOINT_URL:-https://checkpoint-sync.hoodi.ethpandaops.io}"
# CHAIN defaults to hoodi (parent bootstrap) but can be overridden so
# the fork-restart-transition test can relaunch the SAME datadir with
# --chain=<fork-name> without dragging in the fork-child launcher's
# extra flags (which would collide with the datadir's persisted config).
CHAIN="${CHAIN:-hoodi}"

export USE_STATE_CACHE=false
export ERIGON_MERGE_MIN_AGE_STEPS="${ERIGON_MERGE_MIN_AGE_STEPS:-6}"

exec "$BIN" \
  --datadir="$DATADIR" \
  --chain="$CHAIN" --prune.mode=minimal \
  --caplin.checkpoint-sync-url="$CHECKPOINT_URL" \
  --snap.p2p-manifest \
  --http.api=eth,erigon,engine,debug,net,web3,trace,txpool \
  --http.port=19645 --authrpc.port=19651 --private.api.addr=127.0.0.1:11690 \
  --torrent.port=43469 --port=31603 \
  --caplin.discovery.port=4850 --caplin.discovery.tcpport=4851 \
  --sentinel.port=8590 --beacon.api.port=6360 --mcp.port=9360 \
  --log.console.verbosity=3 >"$LOG" 2>&1
