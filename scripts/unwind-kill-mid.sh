#!/usr/bin/env bash
# unwind-kill-mid.sh — fire debug_setHead, SIGKILL erigon mid-unwind,
# restart, verify clean recovery. Surfaces wedges that survive a hard
# crash (half-applied state, dangling locks, post-restart inventory
# drift not picked up).
#
# Usage:
#   scripts/unwind-kill-mid.sh [--rpc URL] [--log PATH] [--depth N]
#                              [--kill-after SEC] [--iter N]
#                              [--launch CMD] [--out CSV]
#
# Per-iteration:
#   1. pre_head = eth_blockNumber
#   2. target = pre_head - depth
#   3. fire debug_setHead(target) ASYNC (curl in background)
#   4. wait KILL_AFTER seconds
#   5. SIGKILL erigon (pgrep + kill -9)
#   6. wait for the process to actually die
#   7. run LAUNCH_CMD (default scripts/erigon-launch-hoodi-soak.sh
#      in background)
#   8. poll RPC alive; then poll head until > pre_head OR timeout
#   9. CSV row with the result

set -u

RPC="${RPC:-http://127.0.0.1:19545}"
LOG="${LOG:-/tmp/erigon-hoodi.log}"
DEPTH="${DEPTH:-5000}"
KILL_AFTER_SEC="${KILL_AFTER_SEC:-15}"
ITER="${ITER:-3}"
LAUNCH_CMD="${LAUNCH_CMD:-scripts/erigon-launch-hoodi-soak.sh}"
RPC_READY_TIMEOUT_SEC="${RPC_READY_TIMEOUT_SEC:-300}"
RECOVERY_TIMEOUT_SEC="${RECOVERY_TIMEOUT_SEC:-1800}"
INTER_ITER_SLEEP_SEC="${INTER_ITER_SLEEP_SEC:-120}"
OUT_DEFAULT="/tmp/unwind-kill-mid-$(date -u +%Y-%m-%dT%H%M%S).csv"
OUT="${OUT:-$OUT_DEFAULT}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rpc) RPC="$2"; shift 2 ;;
        --log) LOG="$2"; shift 2 ;;
        --depth) DEPTH="$2"; shift 2 ;;
        --kill-after) KILL_AFTER_SEC="$2"; shift 2 ;;
        --iter) ITER="$2"; shift 2 ;;
        --launch) LAUNCH_CMD="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

rpc_call() {
    curl -s --max-time 10 -X POST -H "Content-Type: application/json" \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"$1\",\"params\":$2,\"id\":1}" \
        "$RPC"
}
eth_block_number() { rpc_call eth_blockNumber '[]' | jq -r '.result // "null"'; }
hex_to_dec() { printf '%d\n' "$1"; }
dec_to_hex() { printf '0x%x\n' "$1"; }

# kill_erigon: SIGKILLs any running build/bin/erigon process whose
# datadir matches the soak path. Returns the count of processes killed.
# Does NOT wait for them to actually exit — caller polls.
kill_erigon() {
    local pids
    pids=$(pgrep -af "build/bin/erigon.*hoodi-soak" | awk '{print $1}' || true)
    if [[ -z "$pids" ]]; then echo 0; return; fi
    echo "$pids" | xargs -r kill -9 2>/dev/null || true
    echo "$(echo "$pids" | wc -l)"
}

# wait_erigon_dead: blocks until no matching process remains. Caps at
# 60s — past that something else is up.
wait_erigon_dead() {
    local end=$(( $(date +%s) + 60 ))
    while [[ $(date +%s) -lt $end ]]; do
        if ! pgrep -af "build/bin/erigon.*hoodi-soak" >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    echo "WARN: erigon still alive after 60s post-SIGKILL" >&2
    return 1
}

# wait_rpc_alive: blocks until eth_blockNumber returns a real result
# OR timeout fires. Returns the head as a decimal on success, "fail"
# on timeout.
wait_rpc_alive() {
    local end=$(( $(date +%s) + RPC_READY_TIMEOUT_SEC ))
    local hex
    while [[ $(date +%s) -lt $end ]]; do
        hex=$(eth_block_number)
        if [[ -n "$hex" && "$hex" != "null" ]]; then
            hex_to_dec "$hex"
            return 0
        fi
        sleep 5
    done
    echo "fail"
    return 1
}

if [[ ! -s "$OUT" ]]; then
    echo "iter,pre_head,target,killed_at_head,post_restart_head,final_head,duration_sec,note" > "$OUT"
fi

echo "unwind-kill-mid: rpc=$RPC log=$LOG depth=$DEPTH kill_after=${KILL_AFTER_SEC}s iter=$ITER launch=$LAUNCH_CMD out=$OUT"
echo

OVERALL_RC=0
for ((i=1; i<=ITER; i++)); do
    PRE_HEX=$(eth_block_number)
    if [[ "$PRE_HEX" == "null" || -z "$PRE_HEX" ]]; then
        echo "iter $i: ABORT — eth_blockNumber null at start"
        echo "$i,,,,,,0,abort:no-head" >> "$OUT"
        OVERALL_RC=1
        break
    fi
    PRE=$(hex_to_dec "$PRE_HEX")
    TARGET=$((PRE - DEPTH))
    TARGET_HEX=$(dec_to_hex "$TARGET")
    START_TS=$(date +%s)
    echo "iter $i: $(date +%T) pre_head=$PRE depth=$DEPTH target=$TARGET"

    # Fire setHead in background; we don't await the curl, we await
    # the wall-clock interval before SIGKILL.
    curl -s --max-time 1800 -X POST -H "Content-Type: application/json" \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"debug_setHead\",\"params\":[\"$TARGET_HEX\"],\"id\":1}" \
        "$RPC" >/dev/null 2>&1 &
    SETHEAD_PID=$!

    # Sleep until mid-unwind. Brief enough that Provider.Unwind is
    # likely still in flight; long enough that the engine has
    # started the work.
    sleep "$KILL_AFTER_SEC"

    KILLED_HEAD_HEX=$(eth_block_number)
    KILLED_HEAD=$(hex_to_dec "${KILLED_HEAD_HEX:-0x0}")
    echo "iter $i: killing erigon at $(date +%T) head=$KILLED_HEAD"

    KILLED_COUNT=$(kill_erigon)
    if [[ "$KILLED_COUNT" == "0" ]]; then
        echo "iter $i: ABORT — no erigon process to kill" >&2
        echo "$i,$PRE,$TARGET,$KILLED_HEAD,,,0,abort:no-process" >> "$OUT"
        OVERALL_RC=1
        # Reap the in-flight curl so it doesn't hang the next iter.
        wait "$SETHEAD_PID" 2>/dev/null || true
        break
    fi
    wait_erigon_dead || { OVERALL_RC=1; }
    # Reap the curl (the connection died with the server).
    wait "$SETHEAD_PID" 2>/dev/null || true

    echo "iter $i: relaunching erigon ($(date +%T))..."
    # shellcheck disable=SC2086
    nohup $LAUNCH_CMD </dev/null >/dev/null 2>&1 &
    RELAUNCH_PID=$!

    POST_HEAD=$(wait_rpc_alive)
    if [[ "$POST_HEAD" == "fail" ]]; then
        echo "iter $i: ABORT — RPC didn't come back within ${RPC_READY_TIMEOUT_SEC}s"
        echo "$i,$PRE,$TARGET,$KILLED_HEAD,,,${RPC_READY_TIMEOUT_SEC},abort:rpc-timeout" >> "$OUT"
        OVERALL_RC=1
        break
    fi
    echo "iter $i: RPC alive at $(date +%T) post_restart_head=$POST_HEAD"

    # Recovery — head must climb back past pre_head.
    FINAL_HEAD=$POST_HEAD
    RECOVERY_OK=0
    REC_DEADLINE=$(( $(date +%s) + RECOVERY_TIMEOUT_SEC ))
    while [[ $(date +%s) -lt $REC_DEADLINE ]]; do
        sleep 30
        H=$(eth_block_number)
        FINAL_HEAD=$(hex_to_dec "${H:-0x0}")
        if [[ $FINAL_HEAD -gt $PRE ]]; then
            RECOVERY_OK=1
            break
        fi
        ELAPSED=$(( $(date +%s) - START_TS ))
        echo "  iter $i t+${ELAPSED}s head=$FINAL_HEAD pre=$PRE"
    done
    DURATION=$(( $(date +%s) - START_TS ))

    NOTE="ok"
    if [[ $RECOVERY_OK -ne 1 ]]; then
        NOTE="fail:recovery-timeout"
        OVERALL_RC=1
    fi
    echo "$i,$PRE,$TARGET,$KILLED_HEAD,$POST_HEAD,$FINAL_HEAD,$DURATION,$NOTE" >> "$OUT"
    echo "iter $i: $NOTE duration=${DURATION}s final_head=$FINAL_HEAD"

    if [[ $OVERALL_RC -ne 0 ]]; then
        echo "iter $i: ABORTING further iters"
        break
    fi
    if [[ $i -lt $ITER ]]; then
        echo "iter $i: sleeping ${INTER_ITER_SLEEP_SEC}s before next iter..."
        sleep "$INTER_ITER_SLEEP_SEC"
    fi
done

echo
echo "=== unwind-kill-mid complete: rc=$OVERALL_RC csv=$OUT ==="
cat "$OUT"
exit "$OVERALL_RC"
