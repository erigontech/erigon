#!/usr/bin/env bash
# erigon-launch-hoodi-soak.sh — wrapper that launches the soak erigon with
# the standard set of CLI flags. Used by both the manual restart path and
# the kill-mid / fresh-sync test harnesses so a single source of truth
# owns the flag set. Pass DATADIR / LOG via env to override.
#
# Two modes, gated by env:
#   leg P (default): --snap.p2p-manifest with no publisher wired. On a
#   machine with no chain-toml publishers reachable, the 2-min manifest
#   discovery times out and stage_snapshots falls back to preverified.
#   This is what most of the existing soak history was run under.
#
#   leg M (PUBLISHER_ENR + PUBLISHER_TRUST_ROOT set): staticpeer the
#   local master publisher and pin its trust-root pubkey. The manifest
#   discovery MUST succeed via the publisher — no preverified fallback
#   should fire. The soak wrapper post-checks the log for the fallback
#   line and fails the leg if it appears.

set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-soak.bkzAnZ}"
LOG="${LOG:-/tmp/erigon-hoodi.log}"
BIN="${BIN:-./build/bin/erigon}"
CHECKPOINT_URL="${CHECKPOINT_URL:-https://checkpoint-sync.hoodi.ethpandaops.io}"

export USE_STATE_CACHE=false

# ERIGON_MERGE_MIN_AGE_STEPS delays merges of newly-built files until
# they're N steps behind the current frontier. This is the same knob
# chain.toml publishers use to give peers time to download per-step
# files before those files get consolidated into wider merged files.
# For the soak: N=6 gives >30 min per-step-file lifetime on hoodi, so
# Phase 3.5 reliably finds a width==1 commitment .kv for regime 3.
# See docs/plans/20260504-v2-operational-guide.md § Delayed merge for
# peer propagation.
export ERIGON_MERGE_MIN_AGE_STEPS="${ERIGON_MERGE_MIN_AGE_STEPS:-6}"

# ERIGON_TORRENT_KEEP_COMPLETED_PEERS disables anacrolix's per-torrent
# drop-after-mutual-completion. In leg-M mode a single local publisher
# serves every torrent; keeping the conn avoids the 5s-per-cycle re-dial
# churn that would otherwise stall new-torrent metadata fetches.
# Enabling in leg P is harmless (there are no other peers to keep).
export ERIGON_TORRENT_KEEP_COMPLETED_PEERS="${ERIGON_TORRENT_KEEP_COMPLETED_PEERS:-true}"

# leg-M extras: bind the consumer to the local publisher and pin its
# trust root. Empty in leg P.
EXTRA_ARGS=()
if [[ -n "${PUBLISHER_ENR:-}" ]]; then
  EXTRA_ARGS+=(--staticpeers="$PUBLISHER_ENR")
fi
if [[ -n "${PUBLISHER_TRUST_ROOT:-}" ]]; then
  EXTRA_ARGS+=(--snapshot.trust-roots="$PUBLISHER_TRUST_ROOT")
fi

exec "$BIN" \
  --datadir="$DATADIR" \
  --chain=hoodi --prune.mode=minimal \
  --caplin.checkpoint-sync-url="$CHECKPOINT_URL" \
  --snap.p2p-manifest \
  --http.api=eth,erigon,engine,debug,net,web3,trace,txpool \
  --http.port=19545 --authrpc.port=19551 --private.api.addr=127.0.0.1:11590 \
  --torrent.port=43369 --port=31503 \
  --caplin.discovery.port=4750 --caplin.discovery.tcpport=4751 \
  --sentinel.port=8490 --beacon.api.port=6260 --mcp.port=9260 \
  "${EXTRA_ARGS[@]}" \
  --log.console.verbosity=3 >"$LOG" 2>&1
