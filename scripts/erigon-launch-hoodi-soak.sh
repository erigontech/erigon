#!/usr/bin/env bash
# erigon-launch-hoodi-soak.sh — wrapper that launches the soak erigon with
# the standard set of CLI flags. Used by both the manual restart path and
# the kill-mid / fresh-sync test harnesses so a single source of truth
# owns the flag set. Pass DATADIR / LOG via env to override.

set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-soak.bkzAnZ}"
LOG="${LOG:-/tmp/erigon-hoodi.log}"
BIN="${BIN:-./build/bin/erigon}"
CHECKPOINT_URL="${CHECKPOINT_URL:-https://checkpoint-sync.hoodi.ethpandaops.io}"

exec "$BIN" \
  --datadir="$DATADIR" \
  --chain=hoodi --prune.mode=minimal \
  --caplin.checkpoint-sync-url="$CHECKPOINT_URL" \
  --http.api=eth,erigon,engine,debug,net,web3,trace,txpool \
  --http.port=19545 --authrpc.port=19551 --private.api.addr=127.0.0.1:11590 \
  --torrent.port=43369 --port=31503 \
  --caplin.discovery.port=4750 --caplin.discovery.tcpport=4751 \
  --sentinel.port=8490 --beacon.api.port=6260 --mcp.port=9260 \
  --log.console.verbosity=3 >"$LOG" 2>&1
