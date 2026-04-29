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

# Extract allocation metrics from logs
# Log format: [INFO] [vi] build complete method=ef_based file=... alloc_bytes=... total_alloc_bytes=... mallocs=...
parse_metrics() {
    local logfile="$1"
    local method="$2"
    local total_alloc=0
    local total_total_alloc=0
    local total_mallocs=0
    local count=0

    while IFS= read -r line; do
        alloc=$(echo "$line" | grep -oP 'alloc_bytes=\K[-0-9]+' || echo "0")
        talloc=$(echo "$line" | grep -oP 'total_alloc_bytes=\K[0-9]+' || echo "0")
        mallocs=$(echo "$line" | grep -oP 'mallocs=\K[0-9]+' || echo "0")
        total_alloc=$((total_alloc + alloc))
        total_total_alloc=$((total_total_alloc + talloc))
        total_mallocs=$((total_mallocs + mallocs))
        count=$((count + 1))
    done < <(grep "method=$method" "$logfile" || true)

    echo "$total_alloc $total_total_alloc $total_mallocs $count"
}

read EF_ALLOC EF_TALLOC EF_MALLOCS EF_COUNT <<< "$(parse_metrics "$UNCHANGED_LOG" "ef_based")"
read V_ALLOC V_TALLOC V_MALLOCS V_COUNT <<< "$(parse_metrics "$POSSIBLEFAST_LOG" "v_based")"

# Format bytes to human-readable
fmt_bytes() {
    local bytes=$1
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
printf "%-16s | %-10s | %-20s | %-20s | %-12s | %-5s\n" "Method" "Time" "HeapAlloc Delta" "TotalAlloc Delta" "Mallocs" "Files"
printf "%-16s-+-%-10s-+-%-20s-+-%-20s-+-%-12s-+-%-5s\n" "----------------" "----------" "--------------------" "--------------------" "------------" "-----"
printf "%-16s | %-10s | %-20s | %-20s | %-12s | %-5s\n" "ef_based" "${EF_TIME}s" "$(fmt_bytes $EF_ALLOC)" "$(fmt_bytes $EF_TALLOC)" "$EF_MALLOCS" "$EF_COUNT"
printf "%-16s | %-10s | %-20s | %-20s | %-12s | %-5s\n" "v_based" "${V_TIME}s" "$(fmt_bytes $V_ALLOC)" "$(fmt_bytes $V_TALLOC)" "$V_MALLOCS" "$V_COUNT"

echo ""
echo "Detailed logs:"
echo "  EF-based: $UNCHANGED_LOG"
echo "  V-based:  $POSSIBLEFAST_LOG"
