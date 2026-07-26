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

export USE_STATE_CACHE=false
export ERIGON_MERGE_MIN_AGE_STEPS="${ERIGON_MERGE_MIN_AGE_STEPS:-6}"

exec "$BIN" \
  --datadir="$DATADIR" \
  --chain="$CHAIN" \
  --prune.mode=minimal \
  --snap.p2p-manifest \
  --snap.lifecycle-driven-by-storage \
  --http.api=eth,erigon,engine,debug,net,web3,trace,txpool \
  --http.port=19745 --authrpc.port=19751 --private.api.addr=127.0.0.1:11790 \
  --torrent.port=43669 --port=31803 \
  --caplin.discovery.port=4950 --caplin.discovery.tcpport=4951 \
  --sentinel.port=8690 --beacon.api.port=6460 --mcp.port=9460 \
  --log.console.verbosity=3 >"$LOG" 2>&1
