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
        --snap-dir) SNAP_DIR="$2"; shift 2 ;;
        --stress) STRESS_MODE=1; shift ;;
        --scenario2-depth) SCENARIO2_DEPTH="$2"; shift 2 ;;
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

# Three unwind scenarios per iter (per user direction 2026-06-12):
#   1. Within changeset            (target >= minUnwindableBlock)  -> Mode A
#   2. Past changeset, within DB   (target above frozenBlocks tip) -> Mode B lite (no snapshot trim)
#   3. Within snapshots            (target <= frozenBlocks tip)    -> Mode B full
#
# SCENARIO1_DEPTH ceiling matches hoodi's writable-shadow extent (< 96).
# SCENARIO2_DEPTH should land past the changeset retention but above the
# snapshot tip. SCENARIO3_DEPTH (per-iter --depths flag) drops the head
# into snapshot territory.
SCENARIO1_DEPTH="${SCENARIO1_DEPTH:-${MODEA_DEPTH:-50}}"
SCENARIO2_DEPTH="${SCENARIO2_DEPTH:-300}"
SETHEAD_PREFLIGHT_TIMEOUT_SEC="${SETHEAD_PREFLIGHT_TIMEOUT_SEC:-${MODEA_SETHEAD_TIMEOUT_SEC:-120}}"
PREFLIGHT_RECOVERY_TIMEOUT_SEC="${PREFLIGHT_RECOVERY_TIMEOUT_SEC:-${MODEA_RECOVERY_TIMEOUT_SEC:-900}}"

# Forward-progress requirement: per-iter recovery must reach pre_head+N,
# not just target+1000. Set FORWARD_PROGRESS_MARGIN=0 to require
# strictly post_head > pre_head (no margin).
FORWARD_PROGRESS_MARGIN="${FORWARD_PROGRESS_MARGIN:-1}"

# Stress mode: when set, don't wait for full recovery between iters —
# fire the next iter's setHead while the prior one's recovery is still
# climbing. Verifies the system handles overlapping unwinds.
STRESS_MODE="${STRESS_MODE:-0}"
STRESS_INTER_ITER_SEC="${STRESS_INTER_ITER_SEC:-90}"

# Inventory check: after each setHead, compare chain.toml's listed
# block-snapshot files against what's actually on disk. SNAP_DIR
# resolves from --datadir; pass --snap-dir explicitly to override.
SNAP_DIR="${SNAP_DIR:-}"
INVENTORY_CHECK="${INVENTORY_CHECK:-1}"

# inventory_drift: returns "missing_on_disk on_disk_not_in_toml" pair.
# Used to compute before/after deltas around a setHead so transient
# steady-state drift (mid-retire files not yet advertised, normal in
# a busy node) doesn't fire false positives. The setHead-induced
# regression we care about is *new* drift introduced by the unwind —
# captured by the delta, not the absolute.
inventory_drift() {
    if [[ -z "$SNAP_DIR" || ! -r "$SNAP_DIR/chain.toml" ]]; then
        echo "0 0"
        return
    fi
    local toml_files disk_files missing_on_disk on_disk_not_in_toml
    toml_files=$(grep -oE '^"[^"]+\.seg"' "$SNAP_DIR/chain.toml" 2>/dev/null \
        | tr -d '"' | sort -u || true)
    disk_files=$(ls "$SNAP_DIR" 2>/dev/null | grep -E '\.seg$' | sort -u || true)
    missing_on_disk=$(comm -23 <(echo "$toml_files") <(echo "$disk_files") | wc -l)
    on_disk_not_in_toml=$(comm -13 <(echo "$toml_files") <(echo "$disk_files") | wc -l)
    echo "$missing_on_disk $on_disk_not_in_toml"
}

# scenario_test: run one setHead test and write a CSV row. Args:
#   $1 phase label (mode_a / mode_a2 / mode_b)
#   $2 iter number
#   $3 depth (blocks below current head)
#   $4 sethead curl timeout (seconds)
#   $5 recovery timeout (seconds)
#   $6 forward-progress required (1=must exceed pre_head, 0=just hit target+1000)
# Sets:
#   PHASE_RC=0 on success, 1 on failure (sets OVERALL_RC=1 too)
#   PHASE_POST_HEAD = post-test head
scenario_test() {
    local phase=$1 iter=$2 depth=$3 sethead_timeout=$4 recovery_timeout=$5 require_forward=$6
    local pre_head_hex pre_head target target_hex log_offset start_ts resp duration sethead_dur
    local post_head_hex post_head recovery_ok elapsed errors inv_drift note
    PHASE_RC=0
    PHASE_POST_HEAD=0
    pre_head_hex=$(eth_block_number)
    if [[ "$pre_head_hex" == "null" || -z "$pre_head_hex" ]]; then
        echo "iter $iter $phase: ABORT — eth_blockNumber returned null"
        echo "$iter,$phase,,,,0,0,abort:no-head" >> "$OUT"
        PHASE_RC=1
        OVERALL_RC=1
        return
    fi
    pre_head=$(hex_to_dec "$pre_head_hex")
    target=$((pre_head - depth))
    if [[ $target -lt 1 ]]; then
        echo "iter $iter $phase: ABORT — target $target below 1 (pre_head=$pre_head depth=$depth)"
        echo "$iter,$phase,$target,$pre_head,,0,0,abort:target-too-low" >> "$OUT"
        PHASE_RC=1
        OVERALL_RC=1
        return
    fi
    target_hex=$(dec_to_hex "$target")
    log_offset=$(stat -c %s "$LOG" 2>/dev/null || echo 0)
    start_ts=$(date +%s)
    # Inventory baseline BEFORE the setHead. The delta after recovery
    # tells us whether the unwind introduced new chain.toml/disk drift.
    local pre_missing pre_extras
    read -r pre_missing pre_extras <<< "$(inventory_drift)"
    echo "iter $iter $phase: $(date +%T) pre_head=$pre_head depth=$depth target=$target ($target_hex)"
    resp=$(curl -s --max-time "$sethead_timeout" -X POST -H "Content-Type: application/json" \
        --data "{\"jsonrpc\":\"2.0\",\"method\":\"debug_setHead\",\"params\":[\"$target_hex\"],\"id\":1}" \
        "$RPC")
    sethead_dur=$(( $(date +%s) - start_ts ))
    if ! echo "$resp" | grep -q '"result":'; then
        echo "iter $iter $phase: FAILED setHead — $resp"
        echo "$iter,$phase,$target,$pre_head,,${sethead_dur},0,fail:setHead-$resp" >> "$OUT"
        PHASE_RC=1
        OVERALL_RC=1
        return
    fi
    # Recovery polling. Success requires:
    #   (a) head reached at least target + RECOVERY_WINDOW_BLOCKS, AND
    #   (b) if require_forward=1, head exceeded pre_head + FORWARD_PROGRESS_MARGIN.
    recovery_ok=0
    while true; do
        sleep 5
        post_head_hex=$(eth_block_number)
        if [[ "$post_head_hex" == "null" || -z "$post_head_hex" ]]; then
            continue
        fi
        post_head=$(hex_to_dec "$post_head_hex")
        elapsed=$(( $(date +%s) - start_ts ))
        local advanced=0
        if [[ $require_forward -eq 1 ]]; then
            if [[ $post_head -gt $((pre_head + FORWARD_PROGRESS_MARGIN)) ]]; then
                advanced=1
            fi
        else
            if [[ $post_head -gt $((target + RECOVERY_WINDOW_BLOCKS)) ]]; then
                advanced=1
            fi
        fi
        if [[ $advanced -eq 1 ]]; then
            recovery_ok=1
            break
        fi
        if [[ $elapsed -gt $recovery_timeout ]]; then
            break
        fi
        if [[ $((elapsed % 30)) -lt 6 ]]; then
            echo "  t+${elapsed}s head=$post_head $( [[ $require_forward -eq 1 ]] && echo "pre_head+${FORWARD_PROGRESS_MARGIN}=$((pre_head + FORWARD_PROGRESS_MARGIN))" || echo "target+1000=$((target + RECOVERY_WINDOW_BLOCKS))")"
        fi
    done
    PHASE_POST_HEAD=$post_head
    duration=$(( $(date +%s) - start_ts ))
    errors=$(tail -c +"$((log_offset + 1))" "$LOG" 2>/dev/null \
        | grep -cE "parent's total difficulty not found|Could not start execution service|invalid block|halting process" \
        || true)
    local post_missing post_extras
    read -r post_missing post_extras <<< "$(inventory_drift)"
    local d_missing=$((post_missing - pre_missing))
    local d_extras=$((post_extras - pre_extras))
    note="ok"
    if [[ $recovery_ok -ne 1 ]]; then
        note="fail:recovery-timeout"
        PHASE_RC=1
        OVERALL_RC=1
    fi
    if [[ $errors -gt 0 ]]; then
        note="${note}+errors=${errors}"
        PHASE_RC=1
        OVERALL_RC=1
    fi
    # missing-on-disk delta is always a hard regression (toml advertises
    # files that aren't there → next setHead reads stale view).
    # extras-on-disk delta is informational only — a busy node retiring
    # / merging in the background routinely shows transient extras.
    if [[ $d_missing -gt 0 ]]; then
        note="${note}+inv_missing+=${d_missing}"
        PHASE_RC=1
        OVERALL_RC=1
    fi
    if [[ $d_extras -ne 0 ]]; then
        note="${note}+inv_extras=${d_extras}"
    fi
    echo "$iter,$phase,$target,$pre_head,$post_head,$duration,$errors,$note" >> "$OUT"
    echo "iter $iter $phase: $note post_head=$post_head duration=${duration}s errors=$errors inv_missing=$post_missing/+$d_missing inv_extras=$post_extras/$d_extras"
}

# emit CSV header if file is empty/new
if [[ ! -s "$OUT" ]]; then
    echo "iter,phase,target,pre_head,post_head,duration_sec,error_count,note" > "$OUT"
fi

echo "soak: rpc=$RPC log=$LOG iter=$ITER out=$OUT depths=$DEPTHS_CSV"
echo "       scenario1_depth=$SCENARIO1_DEPTH scenario2_depth=$SCENARIO2_DEPTH stress=$STRESS_MODE snap_dir=$SNAP_DIR"
echo

OVERALL_RC=0
for ((i=1; i<=ITER; i++)); do
    DEPTH=${DEPTHS[$((i-1))]}

    # Scenario 1: within changeset (Mode-A path). Must succeed before
    # any deeper test is meaningful — chaindata-only unwind is the
    # safety baseline.
    scenario_test mode_a "$i" "$SCENARIO1_DEPTH" \
        "$SETHEAD_PREFLIGHT_TIMEOUT_SEC" "$PREFLIGHT_RECOVERY_TIMEOUT_SEC" 1
    if [[ $OVERALL_RC -ne 0 ]]; then
        echo "iter $i: ABORTING — Mode-A (scenario 1) regression"
        break
    fi

    # Scenario 2: past changeset, above frozen-blocks tip. Currently
    # routes through setHeadModeB but the snapshot-trim subpath
    # no-ops (no files past toBlock). Exercises the "lite" Mode-B.
    scenario_test mode_a2 "$i" "$SCENARIO2_DEPTH" \
        "$SETHEAD_PREFLIGHT_TIMEOUT_SEC" "$PREFLIGHT_RECOVERY_TIMEOUT_SEC" 1
    if [[ $OVERALL_RC -ne 0 ]]; then
        echo "iter $i: ABORTING — scenario 2 (past-changeset, within-DB) regression"
        break
    fi

    # Scenario 3: within snapshots (Mode-B with full trim). Stress
    # mode uses a shorter recovery polling — the *eventual* recovery
    # gets verified at the end of the loop, after the next iter's
    # setHead has already fired.
    if [[ "$STRESS_MODE" == "1" ]]; then
        scenario_test mode_b "$i" "$DEPTH" \
            "$SETHEAD_CALL_TIMEOUT_SEC" "$STRESS_INTER_ITER_SEC" 0
    else
        scenario_test mode_b "$i" "$DEPTH" \
            "$SETHEAD_CALL_TIMEOUT_SEC" "$RECOVERY_TIMEOUT_SEC" 1
    fi

    if [[ $OVERALL_RC -ne 0 ]]; then
        echo "iter $i: ABORTING — scenario 3 (within-snapshots) regression"
        LOG_OFFSET=0  # show full recent tail on abort
        echo "--- last 200 non-noise log lines ---"
        tail -c +"$((LOG_OFFSET + 1))" "$LOG" 2>/dev/null | \
            grep -vE "Downloader.*Syncing|publishing DownloadComplete|forced bond|Handshake transport|peerSelector|stop force.bonding|sentry.*PeerEvent|chaintoml|storage-lifecycle|GossipManager|method=eth_|p2p.*GoodPeers|Forward Sync.*progress=" \
            | tail -200
        break
    fi

    if [[ $i -lt $ITER ]]; then
        if [[ "$STRESS_MODE" == "1" ]]; then
            echo "iter $i: stress mode — proceeding to iter $((i+1)) without waiting for full recovery"
        else
            echo "iter $i: sleeping ${INTER_ITER_SLEEP_SEC}s before next iteration..."
            sleep "$INTER_ITER_SLEEP_SEC"
        fi
    fi
done

# In stress mode the per-iter scenario-3 only polled briefly. The full
# recovery still needs to land before we call the soak passed: head
# must reach AT LEAST the last iter's pre_head + FORWARD_PROGRESS_MARGIN.
# Use a generous timeout because the EL is recovering from multiple
# overlapping setHeads.
if [[ "$STRESS_MODE" == "1" && $OVERALL_RC -eq 0 ]]; then
    echo
    echo "stress mode: final-recovery wait (must exceed iter-$ITER pre_head)"
    FINAL_DEADLINE=$(( $(date +%s) + RECOVERY_TIMEOUT_SEC ))
    FINAL_OK=0
    while [[ $(date +%s) -lt $FINAL_DEADLINE ]]; do
        sleep 30
        FINAL_HEAD_HEX=$(eth_block_number)
        FINAL_HEAD=$(hex_to_dec "${FINAL_HEAD_HEX:-0x0}")
        printf "  final-recovery head=%d\n" "$FINAL_HEAD"
        # Reuse last PRE_HEAD captured by scenario_test via CSV.
        LAST_PRE=$(tail -1 "$OUT" | cut -d, -f4)
        if [[ -n "$LAST_PRE" && $FINAL_HEAD -gt $((LAST_PRE + FORWARD_PROGRESS_MARGIN)) ]]; then
            FINAL_OK=1
            break
        fi
    done
    if [[ $FINAL_OK -ne 1 ]]; then
        echo "stress mode: FINAL-RECOVERY TIMEOUT"
        echo "$((ITER + 1)),final_recovery,,${LAST_PRE},$FINAL_HEAD,${RECOVERY_TIMEOUT_SEC},0,fail:final-recovery-timeout" >> "$OUT"
        OVERALL_RC=1
    else
        echo "stress mode: final-recovery OK head=$FINAL_HEAD"
        echo "$((ITER + 1)),final_recovery,,${LAST_PRE},$FINAL_HEAD,0,0,ok" >> "$OUT"
    fi
fi

echo
echo "=== soak complete: rc=$OVERALL_RC csv=$OUT ==="
cat "$OUT"
exit "$OVERALL_RC"
