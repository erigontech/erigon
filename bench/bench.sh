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

# --- Worktree cleanliness (no silent stash) ---
check_clean() {
  local dir=$1
  git -C "$dir" diff --quiet && git -C "$dir" diff --cached --quiet \
    || { echo "ERROR: $dir has uncommitted changes — refusing to checkout"; exit 1; }
}
check_clean "$ERIGON_A"
check_clean "$ERIGON_B"

# --- Fetch / checkout / pull (sequential) ---
prepare_branch() {
  local dir=$1 branch=$2
  echo "Preparing $dir on branch $branch..."
  git -C "$dir" fetch origin "$branch"
  git -C "$dir" checkout "$branch"
  git -C "$dir" pull --ff-only origin "$branch"
}
prepare_branch "$ERIGON_A" "$BRANCH_A"
prepare_branch "$ERIGON_B" "$BRANCH_B"

echo "All preconditions passed. Branches checked out and up to date."

# --- Build (sequential; make uses -jN internally) ---
echo "Building A ($BRANCH_A)..."
( cd "$ERIGON_A" && make erigon integration )
echo "Building B ($BRANCH_B)..."
( cd "$ERIGON_B" && make erigon integration )

# --- Verify binaries exist ---
for d in "$ERIGON_A" "$ERIGON_B"; do
  for b in erigon integration; do
    [[ -x "$d/build/bin/$b" ]] || { echo "ERROR: missing $d/build/bin/$b"; exit 1; }
  done
done
echo "Build complete. All four binaries verified."
