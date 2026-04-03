#!/bin/bash

## Iteratively execute + check history to find the window where corruption is introduced.
##
## Each iteration:
##   1. Mirror the source datadir (hardlinks snapshots, copies mutable files)
##   2. Run stage_exec on the mirror for EXEC_MINUTES (default 20)
##   3. Read the block number reached via print_stages
##   4. Run check-commitment-hist-at-blk-range from 0 to that block
##   5. If check fails → stop (corruption window found)
##   6. If check passes → mirror becomes the new source, repeat
##
## Usage: ./cmd/scripts/find-corruption-window.sh [options]
##   --source DIR        Initial source datadir (default: /erigon-data/sepolia)
##   --workdir DIR       Where to create mirrors (default: /erigon-data/sepolia-mirrors)
##   --minutes N         Minutes to run stage_exec per iteration (default: 20)
##   --start-iter N      Starting iteration number (default: 1)
##   --sample FRAC       Fraction of blocks to sample in check (default: 1.0)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ERIGON_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
INTEGRATION="$ERIGON_ROOT/build/bin/integration"
ERIGON="$ERIGON_ROOT/build/bin/erigon"
MIRROR_SCRIPT="$SCRIPT_DIR/mirror-datadir.sh"

# Defaults
SOURCE="/erigon-data/sepolia"
WORKDIR="/erigon-data/sepolia-mirrors"
EXEC_MINUTES=20
START_ITER=1
SAMPLE="1"
CHAIN="sepolia"

# Parse args
while [[ $# -gt 0 ]]; do
    case "$1" in
        --source)    SOURCE="$2"; shift 2 ;;
        --workdir)   WORKDIR="$2"; shift 2 ;;
        --minutes)   EXEC_MINUTES="$2"; shift 2 ;;
        --start-iter) START_ITER="$2"; shift 2 ;;
        --sample)    SAMPLE="$2"; shift 2 ;;
        --chain)     CHAIN="$2"; shift 2 ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

# Validate
for bin in "$INTEGRATION" "$ERIGON" "$MIRROR_SCRIPT"; do
    if [ ! -x "$bin" ]; then
        echo "ERROR: $bin not found or not executable"
        exit 1
    fi
done

if [ ! -d "$SOURCE" ]; then
    echo "ERROR: source datadir '$SOURCE' does not exist"
    exit 1
fi

mkdir -p "$WORKDIR"

echo "=== find-corruption-window ==="
echo "  source:    $SOURCE"
echo "  workdir:   $WORKDIR"
echo "  chain:     $CHAIN"
echo "  exec time: ${EXEC_MINUTES}m per iteration"
echo "  sample:    $SAMPLE"
echo ""

get_exec_block() {
    local datadir="$1"
    # Parse: "Execution                0               0"
    # Extract the stage_at column (first number after "Execution")
    "$INTEGRATION" print_stages --datadir="$datadir" 2>&1 \
        | awk '/^Execution[[:space:]]/ { print $2; exit }'
}

iter=$START_ITER
current_source="$SOURCE"

while true; do
    mirror_dir="$WORKDIR/mirror-$iter"
    echo "========================================"
    echo "  Iteration $iter"
    echo "  Source: $current_source"
    echo "  Mirror: $mirror_dir"
    echo "========================================"

    # Record start block
    start_block=$(get_exec_block "$current_source")
    echo "[iter $iter] Starting execution block: ${start_block:-unknown}"

    # Step 1: Mirror
    echo "[iter $iter] Creating mirror..."
    if [ -d "$mirror_dir" ]; then
        echo "  Mirror already exists, reusing"
    else
        bash "$MIRROR_SCRIPT" "$current_source" "$mirror_dir"
    fi

    # Step 2: Run stage_exec for N minutes, then send SIGINT for graceful commit
    echo "[iter $iter] Running stage_exec for ${EXEC_MINUTES} minutes..."
    timeout --signal=INT --kill-after=60s "${EXEC_MINUTES}m" "$INTEGRATION" stage_exec \
        --datadir="$mirror_dir" \
        --chain="$CHAIN" \
        2>&1 | tee "$mirror_dir/stage_exec.log" || true
    # timeout returns 124 on timeout (SIGINT sent), which is expected
    # --kill-after=60s sends SIGKILL if process doesn't exit within 60s of SIGINT

    # Step 3: Get block number reached
    end_block=$(get_exec_block "$mirror_dir")
    if [ -z "$end_block" ]; then
        echo "[iter $iter] ERROR: Could not determine execution block from print_stages"
        exit 1
    fi
    if [ "$end_block" = "$start_block" ]; then
        echo "[iter $iter] ERROR: Execution made no progress (still at block $end_block)"
        exit 1
    fi
    echo "[iter $iter] Execution reached block: $end_block (started at ${start_block:-unknown})"

    # Step 4: Run check-commitment-hist-at-blk-range
    echo "[iter $iter] Checking commitment history from 0 to $end_block..."
    check_log="$mirror_dir/check-commitment.log"
    if "$ERIGON" seg check-commitment-hist-at-blk-range \
        --datadir="$mirror_dir" \
        --from=0 \
        --to="$end_block" \
        --sample="$SAMPLE" \
        2>&1 | tee "$check_log"; then

        echo "[iter $iter] CHECK PASSED — history is clean through block $end_block"
        echo ""

        # Step 6: Mirror becomes the new source
        current_source="$mirror_dir"
        iter=$((iter + 1))
    else
        echo ""
        echo "========================================"
        echo "  CORRUPTION FOUND at iteration $iter"
        echo "  Blocks: ${start_block:-0} → $end_block"
        echo "  Mirror preserved at: $mirror_dir"
        echo "  Check log: $check_log"
        echo "  Exec log:  $mirror_dir/stage_exec.log"
        echo "========================================"
        exit 0
    fi
done
