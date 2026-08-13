#!/usr/bin/env bash
# refresh-publisher.sh — wipe a publisher datadir and restart. Between
# each soak cycle the publisher(s) are refreshed so every leg exercises
# the full fresh-sync-plus-publish path, not just the "reuse existing
# files" path. Persistent publisher state can hide latent bugs (a
# consumer's on-demand history download may appear to work only
# because the publisher happens to still hold files a previous consumer
# left in its torrent client) — wiping is what surfaces them.
#
# The publisher's trust-root key gets a new value on each wipe, so a
# matching consumer leg must re-read the trust-root via
# publisher-info.sh after refresh.
#
# Class argument (default master; parallel to publisher-info.sh):
#   refresh-publisher.sh            # master (legacy)
#   refresh-publisher.sh master
#   refresh-publisher.sh archive
#
# Env overrides (per class defaults are set below):
#   PUB_DATADIR      publisher datadir to wipe
#   PUB_HTTP_PORT    publisher HTTP RPC port for readiness check
#   PUB_LOG          publisher log path
#   LAUNCHER         launcher script
#   RPC_UP_TIMEOUT_S how long to wait for RPC to come back up (default: 180)

set -eu

class="${1:-master}"
case "$class" in
  master)
    PUB_DATADIR="${PUB_DATADIR:-/erigon/tmp/erigon-hoodi-master-publisher}"
    PUB_HTTP_PORT="${PUB_HTTP_PORT:-19845}"
    PUB_LOG="${PUB_LOG:-/tmp/erigon-hoodi-master-publisher.log}"
    LAUNCHER="${LAUNCHER:-scripts/erigon-launch-hoodi-master-publisher.sh}"
    ;;
  archive)
    PUB_DATADIR="${PUB_DATADIR:-/erigon/tmp/erigon-hoodi-archive-publisher}"
    PUB_HTTP_PORT="${PUB_HTTP_PORT:-19945}"
    PUB_LOG="${PUB_LOG:-/tmp/erigon-hoodi-archive-publisher.log}"
    LAUNCHER="${LAUNCHER:-scripts/erigon-launch-hoodi-archive-publisher.sh}"
    ;;
  *)
    echo "refresh-publisher: unknown class $class (expected: master|archive)" >&2
    exit 1
    ;;
esac

RPC_UP_TIMEOUT_S="${RPC_UP_TIMEOUT_S:-180}"

echo "[refresh-publisher/$class] killing any existing publisher process..."
pkill -f "$PUB_DATADIR" || true
for _ in $(seq 1 30); do
  if ! pgrep -f "$PUB_DATADIR" >/dev/null; then break; fi
  sleep 1
done
pkill -9 -f "$PUB_DATADIR" 2>/dev/null || true

echo "[refresh-publisher/$class] wiping $PUB_DATADIR..."
rm -rf "$PUB_DATADIR"
mkdir -p "$PUB_DATADIR"

echo "[refresh-publisher/$class] archiving previous log..."
if [[ -f "$PUB_LOG" ]]; then
  mv "$PUB_LOG" "$PUB_LOG.$(date +%s).prev"
fi

echo "[refresh-publisher/$class] relaunching..."
nohup "$LAUNCHER" </dev/null >>"$PUB_LOG.boot" 2>&1 &

echo "[refresh-publisher/$class] waiting for HTTP RPC on port $PUB_HTTP_PORT (timeout ${RPC_UP_TIMEOUT_S}s)..."
for i in $(seq 1 "$RPC_UP_TIMEOUT_S"); do
  if curl -sSf -m 2 -X POST -H "Content-Type: application/json" \
      --data '{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}' \
      "http://127.0.0.1:${PUB_HTTP_PORT}" >/dev/null 2>&1; then
    echo "[refresh-publisher/$class] RPC up after ${i}s"
    exit 0
  fi
  sleep 1
done

echo "[refresh-publisher/$class] FAIL: RPC not up after ${RPC_UP_TIMEOUT_S}s" >&2
exit 1
