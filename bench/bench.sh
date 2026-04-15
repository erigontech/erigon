#!/usr/bin/env bash
set -euo pipefail

# --- Constants ---
ERIGON_A=~/erigon_A
ERIGON_B=~/erigon_B
DATADIR_BASE=/erigon-data
RUNS_BASE=~/bench-runs

# --- Inputs ---
if [[ $# -ne 3 ]]; then
  echo "usage: bench.sh <branchA> <branchB> <chain>"
  exit 1
fi
BRANCH_A=$1
BRANCH_B=$2
CHAIN=$3

# --- Precondition validation ---
for d in "$ERIGON_A" "$ERIGON_B"; do
  [[ -d "$d/.git" ]] || { echo "ERROR: $d is not a git clone"; exit 1; }
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for f in "$SCRIPT_DIR/config_A.toml" "$SCRIPT_DIR/config_B.toml"; do
  [[ -f "$f" ]] || { echo "ERROR: missing $f"; exit 1; }
done

command -v tmux >/dev/null || { echo "ERROR: tmux not in PATH"; exit 1; }

echo "All preconditions passed."
