#!/usr/bin/env bash
# publisher-info.sh — print the local publisher's ENR and trust-root
# pubkey in a form the consumer launcher (leg M) can eval. Supports
# both publisher classes (master = minimal, archive = full history):
#
#   # legacy (master defaults):
#   eval "$(scripts/publisher-info.sh)"
#   PUBLISHER_ENR=$ENR PUBLISHER_TRUST_ROOT=$TRUST_ROOT ./scripts/erigon-launch-hoodi-soak.sh
#
#   # explicit class + prefixed vars:
#   eval "$(scripts/publisher-info.sh master)"    # → MASTER_ENR, MASTER_TRUST_ROOT
#   eval "$(scripts/publisher-info.sh archive)"   # → ARCHIVE_ENR, ARCHIVE_TRUST_ROOT

set -eu

class="${1:-master}"
case "$class" in
  master)
    PUB_HTTP_PORT="${PUB_HTTP_PORT:-19845}"
    PUB_DATADIR="${PUB_DATADIR:-/erigon/tmp/erigon-hoodi-master-publisher}"
    PUB_TRUST_ROOT_KEY="${PUB_TRUST_ROOT_KEY:-$PUB_DATADIR/master-publisher-trust-root.hex}"
    enr_var="ENR"
    trust_var="TRUST_ROOT"
    ;;
  archive)
    PUB_HTTP_PORT="${PUB_HTTP_PORT:-19945}"
    PUB_DATADIR="${PUB_DATADIR:-/erigon/tmp/erigon-hoodi-archive-publisher}"
    PUB_TRUST_ROOT_KEY="${PUB_TRUST_ROOT_KEY:-$PUB_DATADIR/archive-publisher-trust-root.hex}"
    enr_var="ARCHIVE_ENR"
    trust_var="ARCHIVE_TRUST_ROOT"
    ;;
  *)
    echo "publisher-info: unknown class $class (expected: master|archive)" >&2
    exit 1
    ;;
esac
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"

resp=$(curl -sS -m 5 -X POST -H "Content-Type: application/json" \
  --data '{"jsonrpc":"2.0","method":"admin_nodeInfo","params":[],"id":1}' \
  "http://127.0.0.1:${PUB_HTTP_PORT}") || {
    echo "publisher-info($class): RPC unreachable on 127.0.0.1:${PUB_HTTP_PORT}" >&2
    exit 1
  }

enr=$(echo "$resp" | jq -r '.result.enr // empty')
[[ -n "$enr" ]] || { echo "publisher-info($class): admin_nodeInfo returned no ENR (response: $resp)" >&2; exit 1; }

trust_root=$("$INTEGRATION_BIN" trust_root_pubkey --key "$PUB_TRUST_ROOT_KEY" 2>/dev/null || true)
[[ -n "$trust_root" ]] || { echo "publisher-info($class): trust-root pubkey not derivable from $PUB_TRUST_ROOT_KEY" >&2; exit 1; }

printf '%s=%q\n%s=%q\n' "$enr_var" "$enr" "$trust_var" "$trust_root"
