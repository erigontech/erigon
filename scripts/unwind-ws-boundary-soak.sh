#!/usr/bin/env bash
# unwind-ws-boundary-soak.sh — deepest-legal mode-B unwind test.
#
# Drives one debug_setHead against a running erigon at depth WS_SLOTS-1
# (weak-subjectivity window minus one slot). This is the maximum
# unwind depth the CL can rewind to via forwardSync + checkpoint
# re-anchor — one block deeper would fall outside the WS window and
# require a different recovery path.
#
# WS_SLOTS = MinEpochsForBlockRequests * SlotsPerEpoch. For hoodi
# (default): 33024 * 32 = 1,056,768 slots ≈ 1.05M blocks. Boundary
# target depth = WS_SLOTS - 1 = 1,056,767.
#
# Preconditions (documented at [[ws-boundary-soak-deferred]]):
#   1. Standard 5-iter soak passes (validates shallow + medium territory).
#   2. Caplin restart-on-UnwindCompleted lands (Fix 3 —
#      node/components/caplin/CaplinService.Restart). Without this,
#      deep unwind wedges Caplin's forwardSync.startSlot.
#
# Recovery wall-clock: 1–4 hours even with snapshot-backed catchup;
# script uses a 6h RECOVERY_TIMEOUT_SEC and matching
# SETHEAD_CALL_TIMEOUT_SEC.
#
# Usage:
#   scripts/unwind-ws-boundary-soak.sh [--rpc URL] [--log PATH] [--out CSV]
#                                      [--depth N]  # override WS_SLOTS-1
#                                      [--ws-slots N]
#
# Env overrides:
#   RPC, LOG, OUT, RECOVERY_TIMEOUT_SEC, SETHEAD_CALL_TIMEOUT_SEC,
#   WS_SLOTS_OVERRIDE.
#
# Exits 0 on success (setHead returned + head recovered past target+1000
# blocks within the timeout). Exits non-zero on forbidden log pattern,
# setHead error, or recovery timeout.

set -u

RPC="${RPC:-http://127.0.0.1:19045}"
LOG="${LOG:-/tmp/hoodi-fresh.log}"
OUT_DEFAULT="/tmp/unwind-ws-boundary-$(date -u +%Y-%m-%dT%H%M%S).csv"
OUT="${OUT:-$OUT_DEFAULT}"

# hoodi defaults per chain spec.
WS_SLOTS_DEFAULT=$(( 33024 * 32 ))
WS_SLOTS="${WS_SLOTS_OVERRIDE:-$WS_SLOTS_DEFAULT}"

RECOVERY_TIMEOUT_SEC="${RECOVERY_TIMEOUT_SEC:-21600}"    # 6 hours
SETHEAD_CALL_TIMEOUT_SEC="${SETHEAD_CALL_TIMEOUT_SEC:-21600}"
RECOVERY_WINDOW_BLOCKS="${RECOVERY_WINDOW_BLOCKS:-1000}"
POLL_INTERVAL_SEC="${POLL_INTERVAL_SEC:-30}"

DEPTH_OVERRIDE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rpc) RPC="$2"; shift 2 ;;
        --log) LOG="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --depth) DEPTH_OVERRIDE="$2"; shift 2 ;;
        --ws-slots) WS_SLOTS="$2"; shift 2 ;;
        -h|--help) sed -n '2,40p' "$0"; exit 0 ;;
        *) echo "unknown arg: $1"; exit 2 ;;
    esac
done

if [[ -z "$DEPTH_OVERRIDE" ]]; then
    DEPTH=$(( WS_SLOTS - 1 ))
else
    DEPTH="$DEPTH_OVERRIDE"
fi

echo "=== ws-boundary-soak: WS_SLOTS=$WS_SLOTS depth=$DEPTH ==="
echo "  rpc=$RPC log=$LOG out=$OUT"
echo "  recovery_timeout=${RECOVERY_TIMEOUT_SEC}s sethead_call_timeout=${SETHEAD_CALL_TIMEOUT_SEC}s"

rpc_call() {
    local method=$1
    local params=$2
    curl -s -X POST -H "Content-Type: application/json" \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"$method\",\"params\":$params,\"id\":1}" \
        "$RPC"
}

get_head() {
    local resp
    resp=$(rpc_call eth_blockNumber "[]")
    local hex
    hex=$(echo "$resp" | jq -r '.result // empty' 2>/dev/null)
    if [[ -z "$hex" || "$hex" == "null" ]]; then
        echo 0
        return
    fi
    printf '%d\n' "$hex"
}

check_forbidden() {
    local since=$1
    local pat="parent's total difficulty not found|Could not start execution service|invalid block|halting process|snapshot step misalignment|salt file did not arrive"
    local matches
    matches=$(awk -v since="$since" '$0 > since' "$LOG" 2>/dev/null | grep -cE "$pat")
    echo "$matches"
}

echo "$(date -u +%FT%TZ) reading pre_head..."
pre_head=$(get_head)
if [[ "$pre_head" -le 0 ]]; then
    echo "fail: cannot read pre_head from $RPC" >&2
    exit 1
fi
echo "  pre_head=$pre_head"

if [[ "$pre_head" -le "$DEPTH" ]]; then
    echo "fail: pre_head=$pre_head <= depth=$DEPTH; chain not deep enough for WS-boundary soak" >&2
    exit 1
fi

target=$(( pre_head - DEPTH ))
target_hex=$(printf '0x%x' "$target")
echo "  target=$target ($target_hex)"

log_marker=$(date -Iseconds -u)
start_ts=$SECONDS
echo "$(date -u +%FT%TZ) calling debug_setHead($target_hex)..."
resp=$(curl -s --max-time "$SETHEAD_CALL_TIMEOUT_SEC" \
    -X POST -H "Content-Type: application/json" \
    --data "{\"jsonrpc\":\"2.0\",\"method\":\"debug_setHead\",\"params\":[\"$target_hex\"],\"id\":1}" \
    "$RPC")
sethead_dur=$(( SECONDS - start_ts ))
sethead_err=$(echo "$resp" | jq -r '.error.message // empty' 2>/dev/null || echo "")
if [[ -n "$sethead_err" ]]; then
    echo "fail: setHead returned error after ${sethead_dur}s: $sethead_err" >&2
    echo "iter,phase,target,pre_head,post_head,duration_sec,error_count,note" > "$OUT"
    echo "1,ws_boundary,$target,$pre_head,$pre_head,$sethead_dur,1,error:${sethead_err}" >> "$OUT"
    exit 1
fi
echo "  setHead ok after ${sethead_dur}s"

echo "$(date -u +%FT%TZ) waiting up to ${RECOVERY_TIMEOUT_SEC}s for head to recover past target+${RECOVERY_WINDOW_BLOCKS}..."
recovery_deadline=$(( SECONDS + RECOVERY_TIMEOUT_SEC ))
recovered=0
last_head=0
while (( SECONDS < recovery_deadline )); do
    cur=$(get_head)
    log_bytes=$(wc -c < "$LOG" 2>/dev/null || echo 0)
    forbidden=$(check_forbidden "$log_marker")
    if (( forbidden > 0 )); then
        echo "fail: $forbidden forbidden pattern(s) in log since $log_marker" >&2
        awk -v since="$log_marker" '$0 > since' "$LOG" 2>/dev/null \
            | grep -E "parent's total difficulty not found|Could not start execution service|invalid block|halting process|snapshot step misalignment|salt file did not arrive" \
            | head -5
        echo "iter,phase,target,pre_head,post_head,duration_sec,error_count,note" > "$OUT"
        echo "1,ws_boundary,$target,$pre_head,$cur,$SECONDS,$forbidden,forbidden_pattern" >> "$OUT"
        exit 1
    fi
    if (( cur > target + RECOVERY_WINDOW_BLOCKS )); then
        recovered=1
        last_head=$cur
        break
    fi
    printf '  t+%ds head=%d log_bytes=%d\n' "$SECONDS" "$cur" "$log_bytes"
    last_head=$cur
    sleep "$POLL_INTERVAL_SEC"
done

total_dur=$SECONDS
echo "iter,phase,target,pre_head,post_head,duration_sec,error_count,note" > "$OUT"
if (( recovered == 1 )); then
    echo "  RECOVERED head=$last_head after ${total_dur}s"
    echo "1,ws_boundary,$target,$pre_head,$last_head,$total_dur,0,ok" >> "$OUT"
    echo "=== ws-boundary-soak: PASS ==="
    exit 0
fi

echo "fail: recovery timeout — head=$last_head after ${total_dur}s, target+window=$(( target + RECOVERY_WINDOW_BLOCKS ))" >&2
echo "1,ws_boundary,$target,$pre_head,$last_head,$total_dur,1,recovery_timeout" >> "$OUT"
exit 1
