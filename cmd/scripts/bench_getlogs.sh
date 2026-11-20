#!/usr/bin/env bash
# bench_getlogs.sh — runner for bench_eth_getlogs.py (direct HTTP requests).
#
# Usage:
#   ./cmd/scripts/bench_getlogs.sh before          # measure and save as "before"
#   ./cmd/scripts/bench_getlogs.sh after           # measure and save as "after"
#   ./cmd/scripts/bench_getlogs.sh compare         # compare before vs after
#   ./cmd/scripts/bench_getlogs.sh show before     # print saved results
#
# Options (appended after the subcommand):
#   --url    URL     RPC endpoint (default: http://localhost:8546)
#   --reps   N       warm repeats per scenario (default: 5)
#   --ranges LIST    comma-separated block ranges (default: 1,10,100,1000)
#   --warmup N       pre-warmup passes before measuring (default: 0)
#
# Results saved to: /tmp/bench_getlogs_<label>.csv

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PY="${SCRIPT_DIR}/bench_eth_getlogs.py"

if [[ ! -f "$PY" ]]; then
    echo "Error: bench_eth_getlogs.py not found at $PY" >&2
    exit 1
fi

subcmd="${1:-}"
shift || true

case "$subcmd" in
    before|after)
        out="/tmp/bench_getlogs_${subcmd}.csv"
        echo "Running benchmark: label=${subcmd}  output=${out}"
        python3 -u "$PY" --label "$subcmd" --out "$out" "$@"
        echo ""
        echo "Saved to: $out"
        echo "Next: $0 compare"
        ;;
    compare)
        fa="/tmp/bench_getlogs_before.csv"
        fb="/tmp/bench_getlogs_after.csv"
        for f in "$fa" "$fb"; do
            if [[ ! -f "$f" ]]; then
                echo "Missing: $f  (run '$0 before' and '$0 after' first)" >&2
                exit 1
            fi
        done
        python3 -u "$PY" --compare "$fa" "$fb" "$@"
        ;;
    show)
        label="${1:-}"
        if [[ -z "$label" ]]; then
            echo "Usage: $0 show <label>" >&2
            exit 1
        fi
        shift
        f="/tmp/bench_getlogs_${label}.csv"
        if [[ ! -f "$f" ]]; then
            echo "No saved results for '${label}' (expected: $f)" >&2
            exit 1
        fi
        python3 -u "$PY" --show "$f" "$@"
        ;;
    -h|--help|"")
        sed -n '2,18p' "${BASH_SOURCE[0]}" | sed 's/^# \?//'
        ;;
    *)
        echo "Unknown subcommand: $subcmd" >&2
        echo "Use: before | after | compare | show" >&2
        exit 1
        ;;
esac
