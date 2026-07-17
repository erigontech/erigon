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
ITER="${ITER:-4}"
# DEPTHS: when unset (the common case), post-sync `integration regime-depths`
# computes them from the live commitment .kv file layout so each iter
# hits a distinct regime:
#   1. in changeset  — mode-A window (target above CanUnwindToBlockNum)
#   2. in mdbx       — mode-B, target step has no .kv file yet
#   3. per-step file — mode-B, target step in a commitment .kv width==1
#   4. multi-step    — mode-B, target step in a commitment .kv width>1
# The subcommand fails loudly if any regime is unreachable — never
# silently downgrades coverage. Set DEPTHS to override (CSV).
DEPTHS="${DEPTHS:-}"
# integration binary path — used by Phase 3.5 to compute regime targets.
INTEGRATION_BIN="${INTEGRATION_BIN:-./build/bin/integration}"
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

# Phase 0: stop any running erigon on OUR datadir AND anything hogging
# the ports we need. Without the port check, a leftover erigon from an
# earlier run on a DIFFERENT datadir goes undetected here — the new
# launcher then fails to bind (address already in use), silently dies,
# and the driver ends up talking to the leftover erigon, producing
# results attributed to the wrong code/state.
stage "Phase 0: stop any running erigon owning $DATADIR + free required ports"
existing_pids=$(pgrep -af "build/bin/erigon.*$(basename "$DATADIR")" | awk '{print $1}' || true)
if [[ -n "$existing_pids" ]]; then
    echo "  killing datadir-owning pids: $existing_pids"
    echo "$existing_pids" | xargs -r kill 2>/dev/null || true
    sleep 5
    still=$(pgrep -af "build/bin/erigon.*$(basename "$DATADIR")" | awk '{print $1}' || true)
    if [[ -n "$still" ]]; then
        echo "  SIGKILL stragglers: $still"
        echo "$still" | xargs -r kill -9 2>/dev/null || true
        sleep 3
    fi
else
    echo "  no datadir-owning erigon"
fi

# Ports used by scripts/erigon-launch-hoodi-soak.sh. If any port is held
# by an erigon (from another datadir) or anything else, kill it — abort
# if we can't.
REQUIRED_PORTS=(19545 19551 11590 43369 31503 4750 4751 8490 6260 9260)
for port in "${REQUIRED_PORTS[@]}"; do
    holder_pid=$(ss -ltnp 2>/dev/null | awk -v p=":$port" '$4 ~ p {n=split($6,a,","); for(i=1;i<=n;i++) if(a[i] ~ /pid=/){split(a[i],b,"="); split(b[2],c,","); print c[1]; exit}}')
    [[ -z "$holder_pid" ]] && holder_pid=$(ss -lunp 2>/dev/null | awk -v p=":$port" '$5 ~ p {n=split($7,a,","); for(i=1;i<=n;i++) if(a[i] ~ /pid=/){split(a[i],b,"="); split(b[2],c,","); print c[1]; exit}}')
    if [[ -n "$holder_pid" ]]; then
        cmd=$(ps -p "$holder_pid" -o comm= 2>/dev/null || echo "unknown")
        echo "  port $port held by pid=$holder_pid ($cmd) — killing"
        kill -9 "$holder_pid" 2>/dev/null || true
        sleep 2
    fi
done
# Final sanity: 19545 must be free before Phase 2.
if ss -ltn 2>/dev/null | awk '{print $4}' | grep -qE ':19545$'; then
    echo "FAIL: port 19545 still held after Phase 0 cleanup — aborting"
    exit 1
fi
echo "  ports clear"

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
#
# LIVE_TIP_FORWARD default = 10000: must exceed the deepest mode_a
# unwind depth AND give Caplin's BlockCollector cache enough forward
# window to bridge the recovery. Live-caught 2026-06-14: with
# LIVE_TIP_FORWARD=100, mode_a depth=50 unwind succeeded but recovery
# wedged because Caplin's BlockCollector cache started ABOVE the
# post-unwind head and the gap blocks weren't in snapshots. See
# memory pin 2026-06-14-gate1-soak-architectural-fixes (bug 6).
stage "Phase 3: wait for live tip (chain-cadence gate: delta in [1,5] for 2 consecutive polls, past_floor>0; stagnation stall=${SYNC_STAGNATION_POLL_LIMIT:-10} polls)"
prev_head=0
bootstrap_floor=0
stable_count=0
# Liveness: head must advance between polls. Stagnation = no
# advancement for SYNC_STAGNATION_POLL_LIMIT consecutive polls (default
# 10 * 30s = 5 min of zero progress). The fail signal is stagnation,
# NOT a wall-clock timeout — slow-but-progressing sync (e.g. 1k blk/min
# catching up from snapshot tip to live) should be tolerated, not
# false-failed. The SYNC_TIMEOUT_SEC env var is retained as a hard
# upper bound (default 4h) for a genuinely-wedged process that the
# stagnation gate somehow misses.
SYNC_STAGNATION_POLL_LIMIT="${SYNC_STAGNATION_POLL_LIMIT:-10}"
SYNC_HARD_DEADLINE_SEC="${SYNC_HARD_DEADLINE_SEC:-14400}"
sync_end=$(( $(date +%s) + SYNC_HARD_DEADLINE_SEC ))
stagnation_polls=0
prev_head_for_progress=0
prev_dl_progress=0
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
        prev_head_for_progress=$head
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
    # Liveness: head OR Caplin DownloadHistoricalBlocks must show
    # progress. During fresh sync, Caplin can run for several minutes
    # filling the block buffer before EL head advances; gating only on
    # head would false-fail during that window. The DownloadHistory
    # progress line "progress=N/M" advances monotonically inside that
    # window and is the right complementary signal.
    dl_progress_now=0
    if [[ -n "$LOG" && -r "$LOG" ]]; then
        dl_progress_now=$(grep -oE 'Downloading Execution History +progress=[0-9]+/' "$LOG" 2>/dev/null \
            | tail -1 | grep -oE '[0-9]+/' | tr -d '/' || echo 0)
        dl_progress_now=${dl_progress_now:-0}
    fi
    if [[ $head -gt $prev_head_for_progress || $dl_progress_now -gt $prev_dl_progress ]]; then
        stagnation_polls=0
    else
        stagnation_polls=$((stagnation_polls + 1))
    fi
    prev_dl_progress=$dl_progress_now
    echo "  $(date -u +%H:%M:%S) head=$head delta=$delta past_floor=$past_floor stagnation=${stagnation_polls}/${SYNC_STAGNATION_POLL_LIMIT}"
    # Live tip = block advancing at chain cadence (delta in [1,5]) for
    # two consecutive polls. Historic gate was `past_floor >=
    # LIVE_TIP_FORWARD` (default 10k) which is unreachable when the
    # preverified snapshot tip is close to live tip — as it is between
    # preverified rolls (twice/week). Anchor on cadence, not absolute
    # advance-past-bootstrap.
    if [[ $delta -ge 1 && $delta -le 5 && $past_floor -gt 0 ]]; then
        stable_count=$((stable_count + 1))
        if [[ $stable_count -ge 2 ]]; then
            echo "  live tip reached: head=$head delta=$delta past_floor=$past_floor (chain cadence for 2 polls)"
            break
        fi
    else
        stable_count=0
    fi
    if [[ $stagnation_polls -ge $SYNC_STAGNATION_POLL_LIMIT ]]; then
        echo "FAIL: sync stagnated — head has not advanced for $((SYNC_STAGNATION_POLL_LIMIT * 30))s (last head=$head); process likely wedged"
        exit 1
    fi
    prev_head=$head
    prev_head_for_progress=$head
    sleep 30
done

if [[ $(date +%s) -ge $sync_end ]]; then
    echo "FAIL: sync did not reach live tip within hard deadline ${SYNC_HARD_DEADLINE_SEC}s (system was making progress but extremely slow)"
    exit 1
fi

# Phase 3.5: compute DEPTHS from live file layout via `integration
# regime-depths`. Requires all 4 regimes to be reachable — a per-step
# commitment .kv file must exist for regime 3. Retire+merge is chain-
# cadence-driven and may have consolidated the per-step file already;
# in that case, wait for the next retire boundary to produce a fresh
# per-step file. Hard cap 90 min (~1 hoodi step at cadence).
if [[ -z "$DEPTHS" ]]; then
    stage "Phase 3.5: compute DEPTHS from live commitment .kv file layout"
    if [[ ! -x "$INTEGRATION_BIN" ]]; then
        echo "FAIL: integration binary not found at $INTEGRATION_BIN"
        echo "  build with: make integration"
        exit 1
    fi
    RD_OUT="$(mktemp)"
    RD_START=$(date +%s)
    RD_HARDCAP_SEC=${RD_HARDCAP_SEC:-5400}
    RD_POLL_SEC=${RD_POLL_SEC:-30}
    RD_ATTEMPT=0
    while true; do
        RD_ATTEMPT=$((RD_ATTEMPT + 1))
        if "$INTEGRATION_BIN" regime-depths --datadir="$DATADIR" --chain=hoodi >"$RD_OUT" 2>&1; then
            break
        fi
        # Read reason — regime-depths emits '[EROR] regime N unreachable: ...'
        REASON=$(grep -E 'EROR.*regime .* unreachable' "$RD_OUT" | head -1 | sed 's/.*EROR[^]]*] //')
        if ! echo "$REASON" | grep -q 'regime 3 unreachable'; then
            echo "FAIL: integration regime-depths returned nonzero (non-regime-3 reason)"
            cat "$RD_OUT"
            rm -f "$RD_OUT"
            exit 1
        fi
        ELAPSED=$(( $(date +%s) - RD_START ))
        if [[ $ELAPSED -ge $RD_HARDCAP_SEC ]]; then
            echo "FAIL: regime-depths timed out after ${RD_HARDCAP_SEC}s waiting for a per-step commitment .kv file (regime 3)"
            cat "$RD_OUT"
            rm -f "$RD_OUT"
            exit 1
        fi
        echo "  attempt $RD_ATTEMPT: $REASON — waiting ${RD_POLL_SEC}s for next retire (elapsed ${ELAPSED}s / cap ${RD_HARDCAP_SEC}s)"
        sleep "$RD_POLL_SEC"
    done
    echo "  regime-depths output (attempt $RD_ATTEMPT):"
    grep -E '^regime=|^DEPTHS=' "$RD_OUT" | sed 's/^/    /'
    DEPTHS="$(grep -E '^DEPTHS=' "$RD_OUT" | tail -1 | sed 's/^DEPTHS=//')"
    rm -f "$RD_OUT"
    if [[ -z "$DEPTHS" ]]; then
        echo "FAIL: regime-depths did not emit DEPTHS= line"
        exit 1
    fi
    # If ITER exceeds the 4-regime depth count, cycle the pattern so
    # every iter still targets a distinct regime (r1,r2,r3,r4,r1,...).
    BASE_DEPTHS="$DEPTHS"
    BASE_COUNT=$(echo "$BASE_DEPTHS" | tr ',' '\n' | wc -l)
    if [[ $ITER -gt $BASE_COUNT ]]; then
        DEPTHS=""
        for ((_i=0; _i<ITER; _i++)); do
            _idx=$((_i % BASE_COUNT + 1))
            _d=$(echo "$BASE_DEPTHS" | cut -d, -f$_idx)
            DEPTHS="${DEPTHS:+$DEPTHS,}$_d"
        done
        echo "  cycled $BASE_COUNT depths to fill ITER=$ITER"
    fi
    echo "  DEPTHS=$DEPTHS"
fi

# Phase 4: soak.
stage "Phase 4: run soak (iter=$ITER depths=$DEPTHS)"
SOAK_OUT="/tmp/unwind-fresh-then-soak-$(date -u +%Y-%m-%dT%H%M%S).csv"
SOAK_DRIVER_LOG="/tmp/unwind-fresh-then-soak-driver.log"
set -o pipefail
"$SOAK_CMD" --rpc "$RPC" --log "$LOG" --iter "$ITER" \
    --depths "$DEPTHS" --snap-dir "$SNAP_DIR" --out "$SOAK_OUT" \
    2>&1 | tee "$SOAK_DRIVER_LOG"
SOAK_RC=${PIPESTATUS[0]}
set +o pipefail

# Phase 5: disk-clean assertion. After all iters + forward-recovery,
# domain .kv files for each domain must form a partition (sorted by
# fromStep, each file's fromStep = previous file's toStep — no gaps,
# no overlaps). Every sidecar (.torrent) and accessor (.kvi, .bt,
# .kvei) must have a matching .kv. Failures indicate Provider.Unwind
# cleanup didn't fully run.
stage "Phase 5: disk-clean assertion"
if [[ "$SOAK_RC" -eq 0 ]]; then
    OVERLAP_COUNT=0
    GAP_COUNT=0
    ORPHAN_COUNT=0
    for DOMAIN in accounts storage code commitment receipt rcache; do
        # Collect (fromStep, toStep, name) triples for this domain, sorted by fromStep.
        LAYOUT=$(ls "$SNAP_DIR"/domain/v*-"$DOMAIN".*.kv 2>/dev/null \
            | while read -r f; do
                name=$(basename "$f")
                # Filename shape: v<ver>-<domain>.<from>-<to>.kv
                range=$(echo "$name" | grep -oE '\.[0-9]+-[0-9]+\.kv$' | sed 's/^\.\(.*\)\.kv$/\1/')
                from=$(echo "$range" | cut -d- -f1)
                to=$(echo "$range" | cut -d- -f2)
                if [[ -n "$from" && -n "$to" ]]; then
                    echo "$from $to $name"
                fi
            done | sort -n)
        if [[ -z "$LAYOUT" ]]; then
            continue
        fi
        # Check consecutive files: prev.toStep must equal cur.fromStep.
        prev_to=""
        while read -r from to name; do
            if [[ -n "$prev_to" && "$prev_to" != "$from" ]]; then
                if [[ "$prev_to" -gt "$from" ]]; then
                    echo "  OVERLAP: $DOMAIN — prev.toStep=$prev_to > cur.fromStep=$from ($name)"
                    OVERLAP_COUNT=$((OVERLAP_COUNT + 1))
                else
                    echo "  GAP: $DOMAIN — prev.toStep=$prev_to < cur.fromStep=$from ($name)"
                    GAP_COUNT=$((GAP_COUNT + 1))
                fi
            fi
            prev_to=$to
        done <<< "$LAYOUT"
    done

    # Orphan-sidecar check: every .torrent, .kvi, .bt, .kvei must map
    # to an existing .kv by <domain>.<from>-<to>. Version prefixes may
    # differ between primary and accessor (e.g. accessor .bt uses v1.1
    # while its .kv primary uses v2.0) — matching keys on
    # <domain>.<range> ignores the version prefix.
    declare -A PRIMARIES
    for f in "$SNAP_DIR"/domain/v*.kv; do
        [[ -e "$f" ]] || continue
        name=$(basename "$f")
        key=$(echo "$name" | sed -E 's/^v[0-9.]+-([a-z]+\.[0-9]+-[0-9]+)\.kv$/\1/')
        PRIMARIES[$key]=1
    done
    for f in "$SNAP_DIR"/domain/*.torrent "$SNAP_DIR"/domain/*.kvi "$SNAP_DIR"/domain/*.bt "$SNAP_DIR"/domain/*.kvei; do
        [[ -e "$f" ]] || continue
        name=$(basename "$f")
        key=$(echo "$name" | sed -E 's/^v[0-9.]+-([a-z]+\.[0-9]+-[0-9]+)\.(kv|kvi|bt|kvei)(\.torrent)?$/\1/')
        if [[ -z "${PRIMARIES[$key]:-}" ]]; then
            echo "  ORPHAN: $name (no .kv matching key $key)"
            ORPHAN_COUNT=$((ORPHAN_COUNT + 1))
        fi
    done

    TOTAL=$((OVERLAP_COUNT + GAP_COUNT + ORPHAN_COUNT))
    if [[ $TOTAL -gt 0 ]]; then
        echo "FAIL: disk-clean assertion — overlaps=$OVERLAP_COUNT gaps=$GAP_COUNT orphans=$ORPHAN_COUNT"
        SOAK_RC=2
    else
        echo "  disk-clean: OK (partitions clean, no orphan sidecars)"
    fi
fi

stage "Result"
echo "soak rc=$SOAK_RC csv=$SOAK_OUT driver=$SOAK_DRIVER_LOG"
exit "$SOAK_RC"
