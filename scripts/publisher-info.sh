#!/usr/bin/env bash
# publisher-info.sh — print the local master publisher's ENR and
# trust-root pubkey in a form the consumer launcher (leg M) can eval.
#
# Exits non-zero if the publisher isn't reachable on the expected HTTP
# port, or hasn't published its trust-root file yet. Callers wire the
# output through eval:
#   eval "$(scripts/publisher-info.sh)"
#   PUBLISHER_ENR=$ENR PUBLISHER_TRUST_ROOT=$TRUST_ROOT ./scripts/erigon-launch-hoodi-soak.sh

set -eu

PUB_HTTP_PORT="${PUB_HTTP_PORT:-19845}"
PUB_DATADIR="${PUB_DATADIR:-/erigon/tmp/erigon-hoodi-master-publisher}"
PUB_TRUST_ROOT_KEY="${PUB_TRUST_ROOT_KEY:-$PUB_DATADIR/master-publisher-trust-root.hex}"
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"

resp=$(curl -sS -m 5 -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}' \
  "http://127.0.0.1:${PUB_HTTP_PORT}") || {
    echo "publisher-info: RPC unreachable on 127.0.0.1:${PUB_HTTP_PORT}" >&2
    exit 1
  }

enr=$(echo "$resp" | jq -r '.result.enr // empty')
[[ -n "$enr" ]] || { echo "publisher-info: admin_nodeInfo returned no ENR (response: $resp)" >&2; exit 1; }

trust_root=$("$INTEGRATION_BIN" trust_root_pubkey --key "$PUB_TRUST_ROOT_KEY" 2>/dev/null || true)
[[ -n "$trust_root" ]] || { echo "publisher-info: trust-root pubkey not derivable from $PUB_TRUST_ROOT_KEY" >&2; exit 1; }

printf 'ENR=%q\nTRUST_ROOT=%q\n' "$enr" "$trust_root"
