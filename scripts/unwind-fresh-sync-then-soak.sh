#!/usr/bin/env bash
# unwind-fresh-sync-then-soak.sh — wipe the datadir, sync from scratch
# to live tip, then run the standard 3-scenario soak. Catches
# first-time-bootstrap interactions the running-datadir soak misses
# (initial OtterSync + manifest exchange + first Inventory snapshot +
# first retire/merge cycle into the new setHead path).
#
# Usage:
#   scripts/unwind-fresh-sync-then-soak.sh [--datadir PATH] [--rpc URL]
#                                          [--log PATH] [--iter N]
#                                          [--depths CSV] [--launch CMD]
#                                          [--sync-timeout SEC]
#
# Phases:
#   0. Stop any running erigon owning the datadir.
#   1. Wipe the datadir (rm -rf, then recreate empty).
#   2. Launch erigon (uses the standard launcher).
#   3. Wait for sync to live tip — head growth must slow to
#      ~chain-tip cadence (delta < 5 over a 30-second poll).
#   4. Run the standard 3-scenario soak with the configured depths.
#
# Designed to be a one-shot CI-equivalent — exit non-zero on any failure
# stage so a hands-off run produces a clear pass/fail.

set -u

DATADIR="${DATADIR:-/erigon/tmp/erigon-hoodi-soak.bkzAnZ}"
RPC="${RPC:-http://127.0.0.1:19545}"
LOG="${LOG:-/tmp/erigon-hoodi.log}"
LAUNCH_CMD="${LAUNCH_CMD:-scripts/erigon-launch-hoodi-soak.sh}"
SOAK_CMD="${SOAK_CMD:-scripts/unwind-soak.sh}"
ITER="${ITER:-2}"
DEPTHS="${DEPTHS:-5000,5000}"
SYNC_TIMEOUT_SEC="${SYNC_TIMEOUT_SEC:-1800}"
SNAP_DIR_DEFAULT="$DATADIR/snapshots"
SNAP_DIR="${SNAP_DIR:-$SNAP_DIR_DEFAULT}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --datadir) DATADIR="$2"; shift 2 ;;
        --rpc) RPC="$2"; shift 2 ;;
        --log) LOG="$2"; shift 2 ;;
        --iter) ITER="$2"; shift 2 ;;
        --depths) DEPTHS="$2"; shift 2 ;;
        --launch) LAUNCH_CMD="$2"; shift 2 ;;
        --sync-timeout) SYNC_TIMEOUT_SEC="$2"; shift 2 ;;
        --snap-dir) SNAP_DIR="$2"; shift 2 ;;
        -h|--help) sed -n '2,30p' "$0"; exit 0 ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

eth_block_number() {
    curl -s --max-time 10 -X POST -H "Content-Type: application/json" \
        --data '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' \
        "$RPC" | jq -r '.result // "null"'
}
hex_to_dec() { printf '%d\n' "$1"; }

stage() {
    local msg="$1"
    echo
    echo "===== $(date -u +%H:%M:%S) :: $msg ====="
}

# Phase 0: stop existing erigon (the launcher will fail to bind if any
# instance is still up).
stage "Phase 0: stop any running erigon owning $DATADIR"
existing_pids=$(pgrep -af "build/bin/erigon.*$(basename "$DATADIR")" | awk '{print $1}' || true)
if [[ -n "$existing_pids" ]]; then
    echo "  killing pids: $existing_pids"
    echo "$existing_pids" | xargs -r kill 2>/dev/null || true
    sleep 5
    still=$(pgrep -af "build/bin/erigon.*$(basename "$DATADIR")" | awk '{print $1}' || true)
    if [[ -n "$still" ]]; then
        echo "  SIGKILL stragglers: $still"
        echo "$still" | xargs -r kill -9 2>/dev/null || true
        sleep 3
    fi
else
    echo "  no running erigon"
fi

# Phase 1: wipe.
stage "Phase 1: wipe $DATADIR"
if [[ ! -d "$DATADIR" ]]; then
    echo "  datadir does not exist; creating fresh"
fi
rm -rf "$DATADIR"
mkdir -p "$DATADIR"
echo "  wiped + recreated"

# Phase 2: launch.
stage "Phase 2: launch erigon ($LAUNCH_CMD)"
# shellcheck disable=SC2086
DATADIR="$DATADIR" LOG="$LOG" nohup $LAUNCH_CMD </dev/null >/dev/null 2>&1 &
echo "  launched pid=$!"

# Phase 3: wait for sync to live tip. Strategy:
#   a. wait for RPC alive (head > 0).
#   b. record the first non-zero head as the "bootstrap floor" — this
#      is typically the snapshot tip the EL bootstraps to BEFORE
#      Caplin starts pushing forward blocks. A stable-at-bootstrap-floor
#      head is NOT "live tip"; it's "EL wedged waiting for Caplin."
#   c. poll head every 30s; require head > bootstrap_floor + LIVE_TIP_FORWARD
#      AND delta in [0, 5] for 2 consecutive polls — i.e., EL has
#      actually executed past the snapshot tip into Caplin-delivered
#      territory and the chain is now at live cadence.
LIVE_TIP_FORWARD="${LIVE_TIP_FORWARD:-100}"
stage "Phase 3: wait for live tip (timeout ${SYNC_TIMEOUT_SEC}s; need head > bootstrap_floor + ${LIVE_TIP_FORWARD})"
sync_end=$(( $(date +%s) + SYNC_TIMEOUT_SEC ))
prev_head=0
bootstrap_floor=0
stable_count=0
while [[ $(date +%s) -lt $sync_end ]]; do
    h_hex=$(eth_block_number)
    if [[ "$h_hex" == "null" || -z "$h_hex" ]]; then
        sleep 5
        continue
    fi
    head=$(hex_to_dec "$h_hex")
    if [[ $prev_head -eq 0 ]]; then
        echo "  $(date -u +%H:%M:%S) initial head=$head"
        prev_head=$head
        if [[ $head -gt 0 ]]; then
            bootstrap_floor=$head
            echo "  bootstrap_floor=$bootstrap_floor (snapshot tip; not yet live tip)"
        fi
        sleep 30
        continue
    fi
    delta=$((head - prev_head))
    if [[ $bootstrap_floor -eq 0 && $head -gt 0 ]]; then
        bootstrap_floor=$head
        echo "  bootstrap_floor=$bootstrap_floor (snapshot tip; not yet live tip)"
    fi
    past_floor=$((head - bootstrap_floor))
    echo "  $(date -u +%H:%M:%S) head=$head delta=$delta past_floor=$past_floor"
    if [[ $delta -le 5 && $delta -ge 0 && $past_floor -ge $LIVE_TIP_FORWARD ]]; then
        stable_count=$((stable_count + 1))
        if [[ $stable_count -ge 2 ]]; then
            echo "  live tip reached: head=$head delta=$delta past_floor=$past_floor (stable for 2 polls)"
            break
        fi
    else
        stable_count=0
    fi
    prev_head=$head
    sleep 30
done

if [[ $(date +%s) -ge $sync_end ]]; then
    echo "FAIL: sync didn't reach live tip within ${SYNC_TIMEOUT_SEC}s"
    exit 1
fi

# Phase 4: soak.
stage "Phase 4: run soak (iter=$ITER depths=$DEPTHS)"
SOAK_OUT="/tmp/unwind-fresh-then-soak-$(date -u +%Y-%m-%dT%H%M%S).csv"
SOAK_DRIVER_LOG="/tmp/unwind-fresh-then-soak-driver.log"
"$SOAK_CMD" --rpc "$RPC" --log "$LOG" --iter "$ITER" \
    --depths "$DEPTHS" --snap-dir "$SNAP_DIR" --out "$SOAK_OUT" \
    2>&1 | tee "$SOAK_DRIVER_LOG"
SOAK_RC=$?

stage "Result"
echo "soak rc=$SOAK_RC csv=$SOAK_OUT driver=$SOAK_DRIVER_LOG"
exit "$SOAK_RC"
