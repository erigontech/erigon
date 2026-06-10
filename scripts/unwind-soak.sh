#!/usr/bin/env bash
# unwind-soak.sh — drive debug_setHead repeatedly against a running erigon to
# soak the mode-B unwind + recovery cycle on a live datadir.
#
# Usage:
#   scripts/unwind-soak.sh [--rpc URL] [--log PATH] [--iter N] [--out CSV]
#                          [--depths "30000,60000,..."]
#
# Defaults match the locally-running hoodi node from the 2026-06-08 session.
#
# Per-iteration:
#   1. record pre_head = eth_blockNumber
#   2. target = pre_head - depth[i]
#   3. call debug_setHead(target) with retry-on-busy (2s × up to 600s)
#   4. poll eth_blockNumber every 30s until > target + 1000 OR 30 min elapses
#   5. grep log since iteration start for forbidden patterns
#   6. append CSV row; abort on error/timeout
#
# Forbidden log patterns abort the soak immediately:
#   - parent's total difficulty not found
#   - Could not start execution service
#   - invalid block
#   - halting process

set -u  # not -e; we want explicit handling per step

RPC="${RPC:-http://127.0.0.1:19045}"
LOG="${LOG:-/tmp/hoodi-fresh.log}"
ITER="${ITER:-5}"
DEPTHS_DEFAULT="30000,60000,90000,60000,30000"
DEPTHS_CSV="${DEPTHS:-$DEPTHS_DEFAULT}"
OUT_DEFAULT="/tmp/unwind-soak-$(date -u +%Y-%m-%dT%H%M%S).csv"
OUT="${OUT:-$OUT_DEFAULT}"
RECOVERY_WINDOW_BLOCKS=1000
RECOVERY_TIMEOUT_SEC=1800   # 30 min per iteration recovery window
SETHEAD_BUSY_TIMEOUT_SEC=1800 # 30 min upper bound on retries-while-busy
SETHEAD_CALL_TIMEOUT_SEC=1800 # 30 min per curl call (synchronous setHead)
SETHEAD_RETRY_INTERVAL_SEC=2
POLL_INTERVAL_SEC=30
INTER_ITER_SLEEP_SEC=60

while [[ $# -gt 0 ]]; do
    case "$1" in
        --rpc) RPC="$2"; shift 2 ;;
        --log) LOG="$2"; shift 2 ;;
        --iter) ITER="$2"; shift 2 ;;
        --out) OUT="$2"; shift 2 ;;
        --depths) DEPTHS_CSV="$2"; shift 2 ;;
        -h|--help)
            sed -n '2,30p' "$0"
            exit 0
            ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

IFS=',' read -r -a DEPTHS <<< "$DEPTHS_CSV"
if [[ ${#DEPTHS[@]} -lt ITER ]]; then
    echo "ERROR: --iter=$ITER but only ${#DEPTHS[@]} depths provided" >&2
    exit 2
fi

rpc_call() {
    # rpc_call METHOD PARAMS_JSON
    curl -s --max-time 10 -X POST -H "Content-Type: application/json" \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"$1\",\"params\":$2,\"id\":1}" \
        "$RPC"
}

eth_block_number() {
    rpc_call eth_blockNumber '[]' | jq -r '.result // "null"'
}

hex_to_dec() {
    printf '%d\n' "$1"
}

dec_to_hex() {
    printf '0x%x\n' "$1"
}

set_head_retry() {
    # set_head_retry TARGET_HEX  → "ok" | "rejected: ..."
    #
    # debug_setHead is synchronous and blocks until Provider.Unwind
    # completes (seconds for shallow, minutes for deep). Use a long
    # curl timeout per attempt so we don't disconnect mid-unwind —
    # disconnecting cancels the engine's ctx and the unwind aborts
    # without making progress. The "is busy" fast-reject path returns
    # in ~5s when the engine semaphore is contended; the
    # accepted-and-processing path blocks for the unwind's duration.
    # SETHEAD_CALL_TIMEOUT_SEC covers both cases.
    local target_hex="$1"
    local start_ts now elapsed resp
    start_ts=$(date +%s)
    while true; do
        resp=$(curl -s --max-time "$SETHEAD_CALL_TIMEOUT_SEC" -X POST -H "Content-Type: application/json" \
            --data "{\"jsonrpc\":\"2.0\",\"method\":\"debug_setHead\",\"params\":[\"$target_hex\"],\"id\":1}" \
            "$RPC")
        if echo "$resp" | grep -q '"result":'; then
            echo "ok"
            return 0
        fi
        if echo "$resp" | grep -q '"error":' && ! echo "$resp" | grep -q "is busy"; then
            echo "rejected: $resp"
            return 1
        fi
        # Empty response = curl hit --max-time. Treat as fatal (the
        # engine likely accepted the call but hadn't finished within
        # our timeout, or the connection dropped). Earlier "retry on
        # empty" behaviour caused 600s busy-loops where every
        # iteration's curl timed out before the engine could finish.
        if [[ -z "$resp" ]]; then
            echo "timeout: empty response after ${SETHEAD_CALL_TIMEOUT_SEC}s"
            return 1
        fi
        now=$(date +%s)
        elapsed=$((now - start_ts))
        if [[ $elapsed -gt $SETHEAD_BUSY_TIMEOUT_SEC ]]; then
            echo "timeout: still busy after ${SETHEAD_BUSY_TIMEOUT_SEC}s"
            return 1
        fi
        sleep "$SETHEAD_RETRY_INTERVAL_SEC"
    done
}

# emit CSV header if file is empty/new
if [[ ! -s "$OUT" ]]; then
    echo "iter,target,pre_head,post_head,duration_sec,error_count,note" > "$OUT"
fi

echo "soak: rpc=$RPC log=$LOG iter=$ITER out=$OUT depths=$DEPTHS_CSV"
echo

OVERALL_RC=0
for ((i=1; i<=ITER; i++)); do
    DEPTH=${DEPTHS[$((i-1))]}
    PRE_HEAD_HEX=$(eth_block_number)
    if [[ "$PRE_HEAD_HEX" == "null" || -z "$PRE_HEAD_HEX" ]]; then
        echo "iter $i: ABORT — eth_blockNumber returned null" | tee -a "$OUT.log"
        echo "$i,,,,0,0,abort:no-head" >> "$OUT"
        OVERALL_RC=1
        break
    fi
    PRE_HEAD=$(hex_to_dec "$PRE_HEAD_HEX")
    TARGET=$((PRE_HEAD - DEPTH))
    if [[ $TARGET -lt 1 ]]; then
        echo "iter $i: ABORT — target $TARGET below 1 (pre_head=$PRE_HEAD depth=$DEPTH)" | tee -a "$OUT.log"
        echo "$i,$TARGET,$PRE_HEAD,,0,0,abort:target-too-low" >> "$OUT"
        OVERALL_RC=1
        break
    fi
    TARGET_HEX=$(dec_to_hex "$TARGET")
    LOG_OFFSET=$(stat -c %s "$LOG" 2>/dev/null || echo 0)
    START_TS=$(date +%s)
    echo "iter $i: $(date +%T) pre_head=$PRE_HEAD depth=$DEPTH target=$TARGET ($TARGET_HEX)"

    SETHEAD_RESULT=$(set_head_retry "$TARGET_HEX")
    if [[ "$SETHEAD_RESULT" != "ok" ]]; then
        DURATION=$(( $(date +%s) - START_TS ))
        echo "iter $i: ABORT setHead — $SETHEAD_RESULT" | tee -a "$OUT.log"
        echo "$i,$TARGET,$PRE_HEAD,,${DURATION},0,abort:setHead-$SETHEAD_RESULT" >> "$OUT"
        OVERALL_RC=1
        break
    fi

    SETHEAD_TS=$(date +%s)
    echo "iter $i: setHead acquired in $((SETHEAD_TS - START_TS))s; polling for recovery..."

    POST_HEAD=0
    RECOVERY_OK=0
    while true; do
        POST_HEAD_HEX=$(eth_block_number)
        if [[ "$POST_HEAD_HEX" == "null" || -z "$POST_HEAD_HEX" ]]; then
            sleep "$POLL_INTERVAL_SEC"
            continue
        fi
        POST_HEAD=$(hex_to_dec "$POST_HEAD_HEX")
        if [[ $POST_HEAD -gt $((TARGET + RECOVERY_WINDOW_BLOCKS)) ]]; then
            RECOVERY_OK=1
            break
        fi
        ELAPSED=$(( $(date +%s) - SETHEAD_TS ))
        if [[ $ELAPSED -gt $RECOVERY_TIMEOUT_SEC ]]; then
            break
        fi
        echo "  t+${ELAPSED}s head=$POST_HEAD target+1000=$((TARGET + RECOVERY_WINDOW_BLOCKS))"
        sleep "$POLL_INTERVAL_SEC"
    done

    DURATION=$(( $(date +%s) - START_TS ))

    # scan log for forbidden patterns since iteration start (by byte offset)
    ERROR_COUNT=$(tail -c +"$((LOG_OFFSET + 1))" "$LOG" 2>/dev/null | \
        grep -cE "parent's total difficulty not found|Could not start execution service|invalid block|halting process" \
        || true)

    NOTE="ok"
    if [[ $RECOVERY_OK -ne 1 ]]; then
        NOTE="abort:recovery-timeout"
        OVERALL_RC=1
    fi
    if [[ $ERROR_COUNT -gt 0 ]]; then
        NOTE="${NOTE}+errors=${ERROR_COUNT}"
        OVERALL_RC=1
    fi

    echo "$i,$TARGET,$PRE_HEAD,$POST_HEAD,$DURATION,$ERROR_COUNT,$NOTE" >> "$OUT"
    echo "iter $i: $NOTE post_head=$POST_HEAD duration=${DURATION}s errors=$ERROR_COUNT"

    if [[ $OVERALL_RC -ne 0 ]]; then
        echo "iter $i: ABORTING further iterations"
        echo "--- last 200 non-noise log lines ---"
        tail -c +"$((LOG_OFFSET + 1))" "$LOG" 2>/dev/null | \
            grep -vE "Downloader.*Syncing|publishing DownloadComplete|forced bond|Handshake transport|peerSelector|stop force.bonding|sentry.*PeerEvent|chaintoml|storage-lifecycle|GossipManager|method=eth_|p2p.*GoodPeers|Forward Sync.*progress=" \
            | tail -200
        break
    fi

    if [[ $i -lt $ITER ]]; then
        echo "iter $i: sleeping ${INTER_ITER_SLEEP_SEC}s before next iteration..."
        sleep "$INTER_ITER_SLEEP_SEC"
    fi
done

echo
echo "=== soak complete: rc=$OVERALL_RC csv=$OUT ==="
cat "$OUT"
exit "$OVERALL_RC"
