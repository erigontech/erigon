#!/usr/bin/env bash
# bench_stress_getlogs.sh — stress-test runner for eth_getLogs via rpc_perf.
#
# Usage:
#   ./cmd/scripts/bench_stress_getlogs.sh [OPTIONS]
#
# Run and save results:
#   ./cmd/scripts/bench_stress_getlogs.sh --label before
#   ./cmd/scripts/bench_stress_getlogs.sh --label after
#
# Compare two saved runs:
#   ./cmd/scripts/bench_stress_getlogs.sh --compare before after
#
# Show a saved run:
#   ./cmd/scripts/bench_stress_getlogs.sh --show before
#
# Options:
#   --label   NAME     Label for this run; output saved to /tmp/bench_stress_getlogs_NAME.txt
#   --seq     SEQ      Vegeta sequence qps:duration,... (default: 1:1,30000:10)
#   --reps    N        Repetitions per sequence step (default: 5)
#   --pattern PATH     Path to .tar pattern file (default: auto-detected)
#   --rpc-perf PATH    Path to rpc_perf binary (default: auto-detected)
#   --compare A B      Compare two previously saved label files and exit
#   --show    NAME     Print a previously saved result file and exit
#   --build            Run 'make erigon' before the test
#
# Note: the RPC port is embedded in the pattern file, not a separate flag.

set -euo pipefail

# ── defaults ──────────────────────────────────────────────────────────────────

LABEL=""
SEQ="1:1,30000:10"
REPS=5
PATTERN=""
RPC_PERF=""
COMPARE_A=""
COMPARE_B=""
SHOW_LABEL=""
BUILD=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"

# ── argument parsing ──────────────────────────────────────────────────────────

while [[ $# -gt 0 ]]; do
    case "$1" in
        --label)   LABEL="$2";     shift 2 ;;
        --seq)     SEQ="$2";       shift 2 ;;
        --reps)    REPS="$2";      shift 2 ;;
        --pattern) PATTERN="$2";   shift 2 ;;
        --rpc-perf) RPC_PERF="$2"; shift 2 ;;
        --compare) COMPARE_A="$2"; COMPARE_B="$3"; shift 3 ;;
        --show)    SHOW_LABEL="$2"; shift 2 ;;
        --build)   BUILD=1;        shift ;;
        -h|--help)
            sed -n '2,30p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
            exit 0 ;;
        *) echo "Unknown option: $1" >&2; exit 1 ;;
    esac
done

# ── helpers ───────────────────────────────────────────────────────────────────

result_file() { echo "/tmp/bench_stress_getlogs_${1}.txt"; }

find_binary() {
    # Search common locations for rpc_perf
    local candidates=(
        "${REPO_ROOT}/build/bin/rpc_perf"
        "${HOME}/silkworm/tests/rpc-tests3/build/bin/rpc_perf"
        "${HOME}/silkworm/tests/rpc-tests/build/bin/rpc_perf"
    )
    for p in "${candidates[@]}"; do
        [[ -x "$p" ]] && echo "$p" && return
    done
    command -v rpc_perf 2>/dev/null && return
    echo "" && return
}

find_pattern() {
    local candidates=(
        "${HOME}/silkworm/tests/rpc-tests3/perf/pattern/mainnet/stress_test_eth_getLogs_15M.tar"
        "${HOME}/silkworm/tests/rpc-tests/perf/pattern/mainnet/stress_test_eth_getLogs_15M.tar"
        "${REPO_ROOT}/perf/pattern/mainnet/stress_test_eth_getLogs_15M.tar"
    )
    for p in "${candidates[@]}"; do
        [[ -f "$p" ]] && echo "$p" && return
    done
    echo "" && return
}

# Extract the result lines from a saved file (the [N.M] test lines)
result_lines() { grep -E '^\[' "$1" 2>/dev/null || true; }

# ── --show ────────────────────────────────────────────────────────────────────

if [[ -n "$SHOW_LABEL" ]]; then
    f="$(result_file "$SHOW_LABEL")"
    if [[ ! -f "$f" ]]; then
        echo "No saved results for label '${SHOW_LABEL}' (expected: ${f})" >&2
        exit 1
    fi
    cat "$f"
    exit 0
fi

# ── --compare ─────────────────────────────────────────────────────────────────

if [[ -n "$COMPARE_A" ]]; then
    fa="$(result_file "$COMPARE_A")"
    fb="$(result_file "$COMPARE_B")"
    for f in "$fa" "$fb"; do
        if [[ ! -f "$f" ]]; then
            echo "File not found: $f" >&2
            exit 1
        fi
    done

    echo ""
    echo "  Comparison: ${COMPARE_A} → ${COMPARE_B}"
    echo ""

    # Header
    printf "  %-6s  %-8s  %-52s  %-52s\n" "step" "qps" "$COMPARE_A" "$COMPARE_B"
    printf "  %-6s  %-8s  %-52s  %-52s\n" "------" "--------" \
        "$(printf '%.0s─' {1..52})" "$(printf '%.0s─' {1..52})"

    # Print side-by-side for matching step labels
    while IFS= read -r line_a; do
        step=$(echo "$line_a" | grep -oP '^\[\K[^\]]+')
        qps=$(echo "$line_a"  | grep -oP 'qps:\s*\K[0-9]+')
        line_b=$(result_lines "$fb" | grep -F "[$step]" || echo "(missing)")
        # Strip leading step label for display
        body_a=$(echo "$line_a" | sed 's/^\[[^]]*\] rpcdaemon: executes test //')
        body_b=$(echo "$line_b" | sed 's/^\[[^]]*\] rpcdaemon: executes test //')
        printf "  %-6s  %-8s  %-52s  %-52s\n" "[$step]" "$qps" "$body_a" "$body_b"
    done < <(result_lines "$fa")
    echo ""
    exit 0
fi

# ── run ───────────────────────────────────────────────────────────────────────

if [[ -z "$LABEL" ]]; then
    echo "Error: --label is required (e.g. --label before)" >&2
    exit 1
fi

# Resolve binaries
[[ -z "$RPC_PERF" ]] && RPC_PERF="$(find_binary)"
if [[ -z "$RPC_PERF" ]]; then
    echo "Error: rpc_perf binary not found. Use --rpc-perf PATH to specify it." >&2
    exit 1
fi

[[ -z "$PATTERN" ]] && PATTERN="$(find_pattern)"
if [[ -z "$PATTERN" ]]; then
    echo "Error: pattern file not found. Use --pattern PATH to specify it." >&2
    exit 1
fi

# Optional build
if [[ "$BUILD" -eq 1 ]]; then
    echo "Building erigon..."
    (cd "$REPO_ROOT" && make erigon)
fi

OUT="$(result_file "$LABEL")"
echo ""
echo "  label   : $LABEL"
echo "  sequence: $SEQ  (${REPS} reps)"
echo "  pattern : $PATTERN"
echo "  output  : $OUT"
echo "  binary  : $RPC_PERF"
echo ""

# Run rpc_perf, capture output and display simultaneously
"$RPC_PERF" \
    -p "$PATTERN" \
    -t "$SEQ" \
    -r "$REPS" \
    -y eth_getLogs \
    -P \
    2>&1 | tee "$OUT"

echo ""
echo "Results saved to: $OUT"
echo "Compare with:  $0 --compare <other_label> $LABEL"
