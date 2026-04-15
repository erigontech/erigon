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

# --- SHAs ---
sanitize() { printf '%s' "$1" | sed 's/[`$\\]/\\&/g'; }
sha_a=$(git -C "$ERIGON_A" rev-parse --short HEAD)
subj_a=$(sanitize "$(git -C "$ERIGON_A" log -1 --pretty=%s)")
sha_b=$(git -C "$ERIGON_B" rev-parse --short HEAD)
subj_b=$(sanitize "$(git -C "$ERIGON_B" log -1 --pretty=%s)")

# --- Paths ---
TS=$(date -u +%Y%m%d-%H%M%S)
slug() { printf '%s' "$1" | tr '/' '-'; }
RUNDIR="$RUNS_BASE/$TS-$(slug "$BRANCH_A")-vs-$(slug "$BRANCH_B")"
DATADIR_A="$DATADIR_BASE/A_$CHAIN"
DATADIR_B="$DATADIR_BASE/B_$CHAIN"

mkdir -p "$RUNDIR" "$DATADIR_A" "$DATADIR_B"
cp "$SCRIPT_DIR/config_A.toml" "$RUNDIR/config_A.toml"
cp "$SCRIPT_DIR/config_B.toml" "$RUNDIR/config_B.toml"

# --- Journal (metadata.md) ---
cat > "$RUNDIR/metadata.md" <<EOF
# Erigon A/B bench — $TS UTC

## A
- branch: \`$BRANCH_A\`
- commit: \`$sha_a\` ($subj_a)
- datadir: \`$DATADIR_A\`
- config: ./config_A.toml (frozen copy in this dir)
- metrics: http://localhost:6061/debug/metrics/prometheus

## B
- branch: \`$BRANCH_B\`
- commit: \`$sha_b\` ($subj_b)
- datadir: \`$DATADIR_B\`
- config: ./config_B.toml (frozen copy in this dir)
- metrics: http://localhost:6062/debug/metrics/prometheus

## Chain
$CHAIN

## Prep (runs in each pane before erigon)
- exec-stage: reset via \`integration stage_exec --reset\`

## Launch
- tmux session: erigon-bench-$TS
- logs: A.log, B.log (tee'd live)
EOF

# --- Echo summary ---
cat "$RUNDIR/metadata.md"
echo
echo "Starting tmux session erigon-bench-$TS..."
