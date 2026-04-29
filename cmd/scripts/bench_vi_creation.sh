#!/bin/bash
set -euo pipefail

# Benchmark VI creation: EF-based vs V-based path
# Usage: bench_vi_creation.sh <source_dir> <unchanged_dir> <possiblefast_dir>

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ERIGON_BIN="${SCRIPT_DIR}/../../build/bin/erigon"
MIRROR_SCRIPT="${SCRIPT_DIR}/mirror-datadir.sh"

if [ $# -ne 3 ]; then
    echo "Usage: $0 <source_dir> <unchanged_dir> <possiblefast_dir>"
    echo ""
    echo "Arguments:"
    echo "  source_dir       - Original datadir with all snapshots"
    echo "  unchanged_dir    - Destination for EF-based (standard) run"
    echo "  possiblefast_dir - Destination for V-based (new) run"
    exit 1
fi

SOURCE_DIR="$1"
UNCHANGED_DIR="$2"
POSSIBLEFAST_DIR="$3"

if [ ! -f "$ERIGON_BIN" ]; then
    echo "Error: erigon binary not found at $ERIGON_BIN"
    echo "Run 'make erigon' first."
    exit 1
fi

if [ ! -f "$MIRROR_SCRIPT" ]; then
    echo "Error: mirror-datadir.sh not found at $MIRROR_SCRIPT"
    exit 1
fi

if [ ! -d "$SOURCE_DIR" ]; then
    echo "Error: source directory '$SOURCE_DIR' does not exist"
    exit 1
fi

echo "=== Step 1: Ensure all indexes exist in source ==="
"$ERIGON_BIN" seg index --datadir="$SOURCE_DIR"

echo ""
echo "=== Step 2: Mirror source → unchanged_dir ==="
"$MIRROR_SCRIPT" "$SOURCE_DIR" "$UNCHANGED_DIR"

echo ""
echo "=== Step 3: Mirror source → possiblefast_dir ==="
"$MIRROR_SCRIPT" "$SOURCE_DIR" "$POSSIBLEFAST_DIR"

echo ""
echo "=== Step 4: Delete .vi files for page-compressed histories ==="
# Page-compressed histories: accounts, commitment, rcache
for dir in "$UNCHANGED_DIR" "$POSSIBLEFAST_DIR"; do
    echo "Deleting .vi files in $dir/snapshots/accessor/"
    find "$dir/snapshots/accessor/" -name "*-accounts.*.vi" -delete -print 2>/dev/null || true
    find "$dir/snapshots/accessor/" -name "*-commitment.*.vi" -delete -print 2>/dev/null || true
    find "$dir/snapshots/accessor/" -name "*-rcache.*.vi" -delete -print 2>/dev/null || true
done

echo ""
echo "=== Step 5: Run EF-based (standard) path ==="
echo "Evicting page cache for unchanged_dir..."
vmtouch -e "$UNCHANGED_DIR/snapshots/accessor/" "$UNCHANGED_DIR/snapshots/history/" 2>/dev/null || echo "Warning: vmtouch not available, skipping cache eviction"

UNCHANGED_LOG=$(mktemp /tmp/vi_bench_unchanged_XXXXXX.log)
echo "Logging to $UNCHANGED_LOG"
TIMEFORMAT='%R'
EF_TIME=$( { time "$ERIGON_BIN" seg index --datadir="$UNCHANGED_DIR" 2>&1 | tee "$UNCHANGED_LOG"; } 2>&1 | tail -1 )

echo ""
echo "=== Step 6: Run V-based (new) path ==="
echo "Evicting page cache for possiblefast_dir..."
vmtouch -e "$POSSIBLEFAST_DIR/snapshots/accessor/" "$POSSIBLEFAST_DIR/snapshots/history/" 2>/dev/null || echo "Warning: vmtouch not available, skipping cache eviction"

POSSIBLEFAST_LOG=$(mktemp /tmp/vi_bench_possiblefast_XXXXXX.log)
echo "Logging to $POSSIBLEFAST_LOG"
V_TIME=$( { time ERIGON_VI_FROM_V=1 "$ERIGON_BIN" seg index --datadir="$POSSIBLEFAST_DIR" 2>&1 | tee "$POSSIBLEFAST_LOG"; } 2>&1 | tail -1 )

echo ""
echo "=== Step 7: Parse allocation metrics ==="

# Sum metrics for a given method label. Uses awk to parse `key=value` pairs
# robustly — earlier versions used `grep -oP 'alloc_bytes=\K[-0-9]+'` which
# also matched `total_alloc_bytes=` (substring), producing multi-line output
# and breaking the arithmetic.
parse_metrics() {
    local logfile="$1"
    local method="$2"
    awk -v lbl="$method" '
        $0 ~ "method=" lbl {
            for (i = 1; i <= NF; i++) {
                n = split($i, kv, "=")
                if (n != 2) continue
                if (kv[1] == "alloc_bytes")          sa += kv[2]
                else if (kv[1] == "total_alloc_bytes") st += kv[2]
                else if (kv[1] == "mallocs")           sm += kv[2]
            }
            count++
        }
        END { printf "%d %d %d %d\n", sa+0, st+0, sm+0, count+0 }
    ' "$logfile"
}

read EF_ALLOC EF_TALLOC EF_MALLOCS EF_COUNT <<< "$(parse_metrics "$UNCHANGED_LOG" "ef_based")"
read V_ALLOC V_TALLOC V_MALLOCS V_COUNT <<< "$(parse_metrics "$POSSIBLEFAST_LOG" "v_based")"

# In the V-based run, files that are not page-compressed (V0 format or
# detected_compression_type=none) fall back to the ef_based path. Account
# for those so the comparison is apples-to-apples.
read V_RUN_EF_ALLOC V_RUN_EF_TALLOC V_RUN_EF_MALLOCS V_RUN_EF_COUNT <<< "$(parse_metrics "$POSSIBLEFAST_LOG" "ef_based")"

fmt_bytes() {
    local bytes=${1:-0}
    if [ "$bytes" -lt 0 ]; then
        echo "${bytes} B"
    elif [ "$bytes" -gt 1073741824 ]; then
        echo "$(echo "scale=2; $bytes / 1073741824" | bc) GB"
    elif [ "$bytes" -gt 1048576 ]; then
        echo "$(echo "scale=2; $bytes / 1048576" | bc) MB"
    elif [ "$bytes" -gt 1024 ]; then
        echo "$(echo "scale=2; $bytes / 1024" | bc) KB"
    else
        echo "${bytes} B"
    fi
}

echo ""
echo "=== Results ==="
echo ""
printf "%-22s | %-10s | %-18s | %-18s | %-14s | %-5s\n" "Run / method" "Time" "HeapAlloc Delta" "TotalAlloc Delta" "Mallocs" "Files"
printf "%-22s-+-%-10s-+-%-18s-+-%-18s-+-%-14s-+-%-5s\n" "----------------------" "----------" "------------------" "------------------" "--------------" "-----"
printf "%-22s | %-10s | %-18s | %-18s | %-14s | %-5s\n" "EF run / ef_based"  "${EF_TIME}s" "$(fmt_bytes $EF_ALLOC)" "$(fmt_bytes $EF_TALLOC)" "$EF_MALLOCS" "$EF_COUNT"
printf "%-22s | %-10s | %-18s | %-18s | %-14s | %-5s\n" "V run / v_based"    "${V_TIME}s"  "$(fmt_bytes $V_ALLOC)"  "$(fmt_bytes $V_TALLOC)"  "$V_MALLOCS"  "$V_COUNT"
printf "%-22s | %-10s | %-18s | %-18s | %-14s | %-5s\n" "V run / ef_fallback" "-"          "$(fmt_bytes $V_RUN_EF_ALLOC)" "$(fmt_bytes $V_RUN_EF_TALLOC)" "$V_RUN_EF_MALLOCS" "$V_RUN_EF_COUNT"

echo ""
echo "Notes:"
echo "  - Wall-clock 'Time' is total ./erigon seg index time including parallel work."
echo "  - 'V run / ef_fallback' counts files in the V run that fell back to ef_based"
echo "    (non-page-compressed .v files). For an apples-to-apples comparison,"
echo "    compare 'EF run / ef_based' restricted to the same fileset vs 'V run / v_based'."
echo ""
echo "Detailed logs:"
echo "  EF-based run: $UNCHANGED_LOG"
echo "  V-based run:  $POSSIBLEFAST_LOG"
