#!/usr/bin/env bash
# refresh-publisher.sh — wipe the master publisher datadir and restart.
# Between each continuous-soak cycle the publisher is refreshed so every
# leg exercises the full fresh-sync-plus-publish path on the publisher
# side, not just the "reuse existing files" path. The publisher's
# trust-root key gets a new value on each wipe, so a matching consumer
# leg must re-read the trust-root via publisher-info.sh after refresh.
#
# Blocks until the publisher's HTTP RPC is back up (admin_nodeInfo
# returns something). Does NOT wait for the publisher to reach the
# hoodi tip — the caller does that separately.
#
# Env:
#   PUB_DATADIR      publisher datadir to wipe (default: /erigon/tmp/erigon-hoodi-master-publisher)
#   PUB_HTTP_PORT    publisher HTTP RPC port for readiness check (default: 19845)
#   PUB_LOG          publisher log path (default: /tmp/erigon-hoodi-master-publisher.log)
#   LAUNCHER         launcher script (default: scripts/erigon-launch-hoodi-master-publisher.sh)
#   RPC_UP_TIMEOUT_S how long to wait for RPC to come back up (default: 180)

set -eu

PUB_DATADIR="${PUB_DATADIR:-/erigon/tmp/erigon-hoodi-master-publisher}"
PUB_HTTP_PORT="${PUB_HTTP_PORT:-19845}"
PUB_LOG="${PUB_LOG:-/tmp/erigon-hoodi-master-publisher.log}"
LAUNCHER="${LAUNCHER:-scripts/erigon-launch-hoodi-master-publisher.sh}"
RPC_UP_TIMEOUT_S="${RPC_UP_TIMEOUT_S:-180}"

echo "[refresh-publisher] killing any existing publisher process..."
pkill -f "$PUB_DATADIR" || true
# Give it up to 30s to release ports cleanly before wiping the datadir.
for _ in $(seq 1 30); do
  if ! pgrep -f "$PUB_DATADIR" >/dev/null; then break; fi
  sleep 1
done
pkill -9 -f "$PUB_DATADIR" 2>/dev/null || true

echo "[refresh-publisher] wiping $PUB_DATADIR..."
rm -rf "$PUB_DATADIR"
mkdir -p "$PUB_DATADIR"

echo "[refresh-publisher] archiving previous log..."
if [[ -f "$PUB_LOG" ]]; then
  mv "$PUB_LOG" "$PUB_LOG.$(date +%s).prev"
fi

echo "[refresh-publisher] relaunching..."
nohup "$LAUNCHER" </dev/null >>"$PUB_LOG.boot" 2>&1 &

echo "[refresh-publisher] waiting for HTTP RPC on port $PUB_HTTP_PORT (timeout ${RPC_UP_TIMEOUT_S}s)..."
for i in $(seq 1 "$RPC_UP_TIMEOUT_S"); do
  if curl -sSf -m 2 -X POST -H "Content-Type: application/json" \
      --data '{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}' \
      "http://127.0.0.1:${PUB_HTTP_PORT}" >/dev/null 2>&1; then
    echo "[refresh-publisher] RPC up after ${i}s"
    exit 0
  fi
  sleep 1
done

echo "[refresh-publisher] FAIL: RPC not up after ${RPC_UP_TIMEOUT_S}s" >&2
exit 1
