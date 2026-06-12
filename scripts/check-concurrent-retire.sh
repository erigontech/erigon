#!/usr/bin/env bash
# check-concurrent-retire.sh — post-hoc check on a soak log: did any
# retire/merge fire concurrently with a setHead's Provider.Unwind
# window? Surfaces races between BlockRetire's NotifyOnFilesChange
# path and the in-flight mode-B unwind.
#
# Usage:
#   scripts/check-concurrent-retire.sh [--log PATH] [--csv PATH]
#
# Logic:
#   1. Walk the erigon log for setHead windows:
#      [Provider.Unwind: commitment-anchor applied] markers AND the
#      paired Provider.FinalizeUnwind markers (or recovery-window
#      tail-bound for setHeads that didn't finalize cleanly).
#   2. Walk the same log for retire firings ([snapshots] Retire Blocks).
#   3. Cross-check: for each retire timestamp, was it within an
#      open setHead window?
#   4. Report:
#        - # retire firings total
#        - # retire firings DURING a setHead window (the value we
#          actually care about — if > 0, concurrent retire happened
#          and the soak still passed → win)
#        - per-overlap snippet (setHead toBlock + retire range)
#
# Designed as a passive checker — run after any soak to see whether
# the concurrent-retire scenario was exercised naturally.

set -u

LOG="${LOG:-/tmp/erigon-hoodi.log}"
CSV="${CSV:-}"

while [[ $# -gt 0 ]]; do
    case "$1" in
        --log) LOG="$2"; shift 2 ;;
        --csv) CSV="$2"; shift 2 ;;
        -h|--help) sed -n '2,25p' "$0"; exit 0 ;;
        *) echo "unknown flag: $1" >&2; exit 2 ;;
    esac
done

if [[ ! -r "$LOG" ]]; then
    echo "log not readable: $LOG" >&2
    exit 2
fi

# Pull setHead windows. Each window starts when Provider.Unwind
# logs the commitment-anchor (mode-B engaged AND past the snapshot
# trim) and ends when Provider.FinalizeUnwind logs the deferred
# trim execution (post-commit). For aborted setHeads
# FinalizeUnwind never fires; we treat those as 60-second windows.
#
# Extract: "HH:MM:SS|toBlock"  pairs.
SETHEAD_STARTS=$(grep -E "Provider\.Unwind: commitment-anchor applied" "$LOG" \
    | awk -F '[][]| ' '{for (i=1; i<=NF; i++) if ($i ~ /^[0-9]+:[0-9]+:[0-9]+\./) ts=$i; for (i=1; i<=NF; i++) if ($i ~ /^toBlock=/) blk=$i; print ts "|" blk}')
SETHEAD_ENDS=$(grep -E "Provider\.FinalizeUnwind: deferred snapshot-trim ops executed" "$LOG" \
    | awk -F '[][]| ' '{for (i=1; i<=NF; i++) if ($i ~ /^[0-9]+:[0-9]+:[0-9]+\./) ts=$i; print ts}')

ts_to_sec() {
    # HH:MM:SS.ms -> seconds since midnight (integer). Doesn't handle
    # day rollovers — soaks longer than 24h need date-aware parsing.
    local hms=${1%%.*}
    IFS=: read -r h m s <<< "$hms"
    echo $((10#$h * 3600 + 10#$m * 60 + 10#$s))
}

# Build start-list and end-list as parallel arrays. Pair each start
# with the next end after it (or +60s if none).
mapfile -t STARTS <<< "$SETHEAD_STARTS"
mapfile -t ENDS <<< "$SETHEAD_ENDS"

declare -a WIN_START_SEC WIN_END_SEC WIN_TOBLOCK
end_idx=0
for line in "${STARTS[@]}"; do
    [[ -z "$line" ]] && continue
    ts=${line%%|*}
    blk=${line##*|}
    ss=$(ts_to_sec "$ts")
    # find first end ts >= ss
    es=$((ss + 60))
    for ((i=end_idx; i<${#ENDS[@]}; i++)); do
        [[ -z "${ENDS[i]}" ]] && continue
        ee=$(ts_to_sec "${ENDS[i]}")
        if [[ $ee -ge $ss ]]; then
            es=$ee
            end_idx=$((i + 1))
            break
        fi
    done
    WIN_START_SEC+=("$ss")
    WIN_END_SEC+=("$es")
    WIN_TOBLOCK+=("$blk")
done

WINDOWS=${#WIN_START_SEC[@]}
echo "found $WINDOWS setHead/Provider.Unwind window(s)"

# Retire firings.
RETIRES=$(grep -E "\[snapshots\] Retire Blocks" "$LOG" \
    | awk -F '[][]| ' '{for (i=1; i<=NF; i++) if ($i ~ /^[0-9]+:[0-9]+:[0-9]+\./) ts=$i; for (i=1; i<=NF; i++) if ($i ~ /range=/) r=$i; print ts "|" r}')
mapfile -t RETIRES_ARR <<< "$RETIRES"
TOTAL_RETIRES=0
OVERLAP=0
OVERLAP_LINES=""
for line in "${RETIRES_ARR[@]}"; do
    [[ -z "$line" ]] && continue
    TOTAL_RETIRES=$((TOTAL_RETIRES + 1))
    ts=${line%%|*}
    range=${line##*|}
    rs=$(ts_to_sec "$ts")
    for ((i=0; i<WINDOWS; i++)); do
        if [[ $rs -ge ${WIN_START_SEC[i]} && $rs -le ${WIN_END_SEC[i]} ]]; then
            OVERLAP=$((OVERLAP + 1))
            OVERLAP_LINES+="  $ts retire $range overlapped setHead ${WIN_TOBLOCK[i]}\n"
            break
        fi
    done
done

echo "total retire firings: $TOTAL_RETIRES"
echo "concurrent with setHead: $OVERLAP"
if [[ $OVERLAP -gt 0 ]]; then
    printf "%b\n" "$OVERLAP_LINES"
fi

if [[ -n "$CSV" ]]; then
    {
        echo "metric,value"
        echo "setHead_windows,$WINDOWS"
        echo "retire_firings_total,$TOTAL_RETIRES"
        echo "retire_concurrent_with_setHead,$OVERLAP"
    } > "$CSV"
    echo "csv: $CSV"
fi

# Exit 0 always — this is a passive analyzer, not a pass/fail gate.
# Caller decides what to do with the count.
exit 0
