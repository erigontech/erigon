#!/usr/bin/env bash
# erigon-launch-hoodi-master-publisher.sh — long-lived hoodi master
# publisher for the LOCAL test infrastructure.
#
# This is the ONE launcher that legitimately uses
# --snap.bootstrap-from-preverified. Its job is to be the source of
# truth for consumer tests that must exercise the full ENR / chain.toml
# / UCAN manifest-exchange stack. See memory/local-master-publisher-
# architecture-2026-08-02.md for the rationale: without a local
# publisher, every "manifest" consumer test silently falls back to
# preverified (2-min timeout in stage_snapshots.go:260) and never
# actually validates the manifest path.
#
# Consumer tests point at this publisher via --staticpeers=<publisher-enr>
# (fetch the ENR post-startup via admin_nodeInfo on this launcher's
# HTTP port). They MUST NOT pass --snap.bootstrap-from-preverified —
# that would defeat the point.
#
# Ports use a dedicated +300 offset from the standard soak launcher
# (which is +0). +100 is taken by fork-parent, +200 hit an unrelated
# collision on 43569 the first time, so we settled on +300. Table:
#   soak:            19545 / 19551 / 11590 / 43369 / 31503 / 4750 / 4751 / 8490 / 6260 / 9260
#   fork-parent:     19645 / 19651 / 11690 / 43469 / 31603 / 4850 / 4851 / 8590 / 6360 / 9360
#   master-publisher: 19845 / 19851 / 11890 / 43669 / 31803 / 5050 / 5051 / 8790 / 6560 / 9560
#
# Long-lived by convention — launch via nohup or a session manager.
# Datadir persists across restarts; trust-root key persists so
# consumers pin the same pubkey between publisher restarts.

set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-master-publisher}"
LOG="${LOG:-/tmp/erigon-hoodi-master-publisher.log}"
BIN="${BIN:-./build/bin/erigon}"
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"
CHECKPOINT_URL="${CHECKPOINT_URL:-https://checkpoint-sync.hoodi.ethpandaops.io}"

# Trust-root key for the publisher-signed chain.toml UCAN. Consumers
# pin this pubkey via --snapshot.trust-roots=<pubkey>. Persistent so
# the pubkey doesn't rotate between publisher restarts — deleting the
# datadir also invalidates the key.
TRUST_ROOT_KEY="${TRUST_ROOT_KEY:-$DATADIR/master-publisher-trust-root.hex}"
mkdir -p "$DATADIR"
TRUST_ROOT_PUB=$("$INTEGRATION_BIN" trust_root_pubkey --key "$TRUST_ROOT_KEY" --generate-if-missing 2>/dev/null)
[[ -n "$TRUST_ROOT_PUB" ]] || { echo "FAIL: could not provision/derive master-publisher trust-root pubkey" >&2; exit 1; }
echo "[master-publisher] trust-root pubkey: $TRUST_ROOT_PUB (key: $TRUST_ROOT_KEY)" >&2
echo "[master-publisher] consumers pin via: --snapshot.trust-roots=$TRUST_ROOT_PUB" >&2

export USE_STATE_CACHE=false
# ERIGON_MERGE_MIN_AGE_STEPS delays merges of newly-built files so
# consumers have time to download per-step files before they're
# consolidated into wider merged files. Same reasoning as the soak
# launcher; the publisher needs it more (it serves those files).
export ERIGON_MERGE_MIN_AGE_STEPS="${ERIGON_MERGE_MIN_AGE_STEPS:-6}"

# --nat=extip:127.0.0.1 forces the publisher to advertise loopback in
# its ENR. Without it, erigon auto-detects the machine's external IP
# (via NAT probe / interface scan), which local consumers cannot reach
# via hairpin NAT on many hosts — the BT/torrent connection to publisher
# fails and chain.toml never downloads. Loopback works because publisher
# + consumer share the host.
PUB_ADVERTISE_IP="${PUB_ADVERTISE_IP:-127.0.0.1}"

exec "$BIN" \
  --datadir="$DATADIR" \
  --chain=hoodi --prune.mode=minimal \
  --caplin.checkpoint-sync-url="$CHECKPOINT_URL" \
  --snap.bootstrap-from-preverified \
  --snap.p2p-manifest \
  --snapshot.trust-roots="$TRUST_ROOT_PUB" \
  --nat="extip:$PUB_ADVERTISE_IP" \
  --http.api=eth,erigon,engine,debug,net,web3,trace,txpool,admin \
  --http.port=19845 --authrpc.port=19851 --private.api.addr=127.0.0.1:11890 \
  --torrent.port=43669 --port=31803 \
  --caplin.discovery.port=5050 --caplin.discovery.tcpport=5051 \
  --sentinel.port=8790 --beacon.api.port=6560 --mcp.port=9560 \
  --log.console.verbosity=3 >"$LOG" 2>&1
