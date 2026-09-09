#!/usr/bin/env bash
# erigon-launch-hoodi-soak.sh — wrapper that launches the soak erigon with
# the standard set of CLI flags. Used by both the manual restart path and
# the kill-mid / fresh-sync test harnesses so a single source of truth
# owns the flag set. Pass DATADIR / LOG via env to override.
#
# Two modes, gated by env:
#   leg P (default): --snap.p2p-manifest with no publisher wired.
#   --snap.bootstrap-from-preverified defaults to true so the consumer
#   seeds a synthetic manifest from preverified.toml at startup and
#   proceeds without waiting for a chain-toml peer.
#
#   leg M (PUBLISHER_ENR + PUBLISHER_TRUST_ROOT set): staticpeer the
#   local master publisher and pin its trust-root pubkey. Sets
#   --snap.bootstrap-from-preverified=false so the manifest MUST come
#   from the publisher — preverified is disabled, no silent
#   degradation possible.
#
#   Optional second publisher (ARCHIVE_ENR + ARCHIVE_TRUST_ROOT set):
#   also staticpeer a full-history publisher so mode-B unwinds deeper
#   than the minimal publisher's retention can pull the needed .v/.ef
#   files on-demand via chain.toml aggregation. Trust-roots is a
#   comma-separated list; both keys get pinned.

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

# leg-M extras: bind the consumer to the local publisher(s) and pin
# trust root(s). Empty in leg P. When both master and archive
# publishers are running the consumer needs BOTH: master for tip
# freshness, archive for deep-history .v/.ef files.
STATICPEERS=""
if [[ -n "${PUBLISHER_ENR:-}" ]]; then
  STATICPEERS="$PUBLISHER_ENR"
fi
if [[ -n "${ARCHIVE_ENR:-}" ]]; then
  if [[ -n "$STATICPEERS" ]]; then
    STATICPEERS="$STATICPEERS,$ARCHIVE_ENR"
  else
    STATICPEERS="$ARCHIVE_ENR"
  fi
fi
TRUST_ROOTS=""
if [[ -n "${PUBLISHER_TRUST_ROOT:-}" ]]; then
  TRUST_ROOTS="$PUBLISHER_TRUST_ROOT"
fi
if [[ -n "${ARCHIVE_TRUST_ROOT:-}" ]]; then
  if [[ -n "$TRUST_ROOTS" ]]; then
    TRUST_ROOTS="$TRUST_ROOTS,$ARCHIVE_TRUST_ROOT"
  else
    TRUST_ROOTS="$ARCHIVE_TRUST_ROOT"
  fi
fi
EXTRA_ARGS=()
# ENABLE_PPROF exposes the pprof HTTP server for profiling a run in place.
# Off by default so the standard soak flag set is unchanged.
if [[ "${ENABLE_PPROF:-0}" == "1" ]]; then
  EXTRA_ARGS+=(--pprof --pprof.addr=127.0.0.1 --pprof.port="${PPROF_PORT:-6060}")
fi
if [[ -n "$STATICPEERS" ]]; then
  EXTRA_ARGS+=(--staticpeers="$STATICPEERS")
fi
if [[ -n "$TRUST_ROOTS" ]]; then
  EXTRA_ARGS+=(--snapshot.trust-roots="$TRUST_ROOTS")
fi
# Leg M: manifest MUST come from the publisher; preverified is disabled
# so any failure to reach the publisher surfaces as a hang, not silent
# degradation to preverified.
if [[ -n "${PUBLISHER_ENR:-}" ]]; then
  EXTRA_ARGS+=(--snap.bootstrap-from-preverified=false)
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
