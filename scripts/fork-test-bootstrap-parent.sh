#!/usr/bin/env bash
# fork-test-bootstrap-parent.sh — bring up a hoodi parent from scratch
# ready for the fork test suite. Wipes DATADIR, launches erigon in
# the background, and polls until head reaches HEAD_MIN.
#
# On success writes the pid to PIDFILE and prints:
#     BOOTSTRAP_OK datadir=<...> rpc=<...> pid=<...> head=<...>
#
# On failure exits non-zero and leaves the datadir + log in place so
# the operator can inspect state.
#
# Composable — the fork-test-fresh-run.sh wrapper chains this with
# scripts/fork-test-suite.sh --with-e2e and a teardown.

set -uo pipefail

DATADIR="${DATADIR:-/erigon/tmp/erigon-fork-test-parent}"
RPC="${RPC:-http://127.0.0.1:19645}"
HEAD_MIN="${HEAD_MIN:-2000}"
BIN="${BIN:-./build/bin/erigon}"
LAUNCH_CMD="${LAUNCH_CMD:-scripts/erigon-launch-hoodi-fork-parent.sh}"
LOG="${LOG:-/tmp/erigon-fork-test-parent.log}"
PIDFILE="${PIDFILE:-/tmp/erigon-fork-test-parent.pid}"
WAIT_TIMEOUT_SEC="${WAIT_TIMEOUT_SEC:-14400}" # 4h — hoodi fresh sync can take a while
POLL_SEC="${POLL_SEC:-30}"
WIPE_ON_START="${WIPE_ON_START:-true}"

command -v jq >/dev/null || { echo "FAIL: jq required"; exit 1; }
[[ -x "$BIN" ]] || { echo "FAIL: BIN=$BIN not executable (make erigon first)"; exit 1; }
[[ -x "$LAUNCH_CMD" ]] || { echo "FAIL: LAUNCH_CMD=$LAUNCH_CMD not executable"; exit 1; }

stage() { echo "[$(date -u +%H:%M:%S)] bootstrap: $1"; }
fail() { echo "FAIL: $1" >&2; exit 1; }

# Refuse to start on top of a running erigon holding the same datadir.
if pgrep -f "erigon.*--datadir=?$DATADIR" >/dev/null 2>&1; then
    fail "an erigon is already running against $DATADIR — stop it first"
fi

# Refuse to start if RPC port is in use — likely a stale process on
# the same port with a different datadir would confuse the tests.
if curl -s --max-time 2 -X POST -H "Content-Type: application/json" \
     --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
     "$RPC" 2>/dev/null | grep -q '"result"'; then
    fail "something is already listening at $RPC — free the port or point RPC elsewhere"
fi

if [[ "$WIPE_ON_START" == "true" && -d "$DATADIR" ]]; then
    stage "wiping stale datadir $DATADIR"
    rm -rf "$DATADIR"
fi
mkdir -p "$DATADIR"

stage "launching erigon (DATADIR=$DATADIR LOG=$LOG)"
DATADIR="$DATADIR" LOG="$LOG" BIN="$BIN" nohup "$LAUNCH_CMD" >/dev/null 2>&1 &
sleep 3
pid=$(pgrep -f "erigon.*--datadir=?$DATADIR" | head -1)
[[ -n "$pid" ]] || fail "erigon process did not appear — check $LOG"
echo "$pid" > "$PIDFILE"
stage "erigon pid=$pid"

trap 'if [[ -f "$PIDFILE" ]]; then p=$(cat "$PIDFILE"); if kill -0 "$p" 2>/dev/null; then echo "  bootstrap failed — leaving erigon pid=$p running for inspection"; fi; fi' EXIT

stage "waiting for RPC at $RPC (up to ${WAIT_TIMEOUT_SEC}s)"
start=$(date +%s)
while true; do
    if ! kill -0 "$pid" 2>/dev/null; then
        fail "erigon pid=$pid exited before RPC was reachable — check $LOG"
    fi
    head_hex=$(curl -s --max-time 5 -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        "$RPC" 2>/dev/null | jq -r '.result // "null"')
    if [[ "$head_hex" != "null" && -n "$head_hex" ]]; then
        head_dec=$(printf '%d' "$head_hex")
        elapsed=$(( $(date +%s) - start ))
        if (( head_dec >= HEAD_MIN )); then
            trap - EXIT
            echo
            echo "BOOTSTRAP_OK datadir=$DATADIR rpc=$RPC pid=$pid head=$head_dec"
            exit 0
        fi
        stage "t=${elapsed}s head=$head_dec (want >= $HEAD_MIN)"
    fi
    elapsed=$(( $(date +%s) - start ))
    if (( elapsed >= WAIT_TIMEOUT_SEC )); then
        fail "head did not reach $HEAD_MIN within ${WAIT_TIMEOUT_SEC}s (last=${head_dec:-<no-rpc>})"
    fi
    sleep "$POLL_SEC"
done
