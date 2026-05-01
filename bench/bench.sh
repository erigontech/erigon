#!/usr/bin/env bash
set -euo pipefail

# --- Constants ---
ERIGON_A=~/erigon_A
ERIGON_B=~/erigon_B
DATADIR_BASE=/erigon-data
RUNS_BASE=~/bench-runs

usage() {
  cat <<EOF
usage: bench.sh <branchA> <branchB> <chain> [datadirA] [datadirB] [flags]

  branchA, branchB     local branch, remote branch, tag, or SHA
  chain                e.g. mainnet, holesky, dev
  datadirA, datadirB   optional explicit datadirs (default: $DATADIR_BASE/A_<chain>, $DATADIR_BASE/B_<chain>)

  --reset              run 'integration stage_exec --reset' before erigon
  --mem-limit-32g      wrap each erigon launch in 'systemd-run --user -p MemoryMax=32G'
  --no-deep-merge      export ERIGON_NO_DEEP_MERGE_HISTORY=true (skips deep history merges)
  --block-limit-1      run 'erigon snapshots rm-state --latest' as pre-flight, then start
                       erigon with --sync.loop.block.limit=1 (approximates chain-tip workload
                       for measuring big-merge impact)

The launcher exports ERIGON_KV_READ_METRICS=true so kv_get_{count,sum} and
trie_state_levelled_{load,skip}_rate series are populated on /debug/metrics/prometheus.
EOF
}

# --- Argv parsing ---
RESET=false
MEM_LIMIT_32G=false
NO_DEEP_MERGE=false
BLOCK_LIMIT_1=false
POS=()
for arg in "$@"; do
  case "$arg" in
    --reset)            RESET=true ;;
    --mem-limit-32g)    MEM_LIMIT_32G=true ;;
    --no-deep-merge)    NO_DEEP_MERGE=true ;;
    --block-limit-1)    BLOCK_LIMIT_1=true ;;
    -h|--help)          usage; exit 0 ;;
    *)                  POS+=("$arg") ;;
  esac
done

if [[ ${#POS[@]} -lt 3 || ${#POS[@]} -gt 5 ]]; then
  usage; exit 1
fi
BRANCH_A=${POS[0]}
BRANCH_B=${POS[1]}
CHAIN=${POS[2]}
DATADIR_A_OVERRIDE=${POS[3]:-}
DATADIR_B_OVERRIDE=${POS[4]:-}

for arg in "$BRANCH_A" "$BRANCH_B"; do
  [[ "$arg" != -* ]] || { echo "ERROR: ref must not start with a dash: $arg"; exit 1; }
  [[ "$arg" != +* ]] || { echo "ERROR: ref must not start with '+' (refspec syntax): $arg"; exit 1; }
  [[ "$arg" != HEAD && "$arg" != "@" ]] || { echo "ERROR: '$arg' is a special git ref"; exit 1; }
  git check-ref-format "refs/heads/$arg" 2>/dev/null || { echo "ERROR: invalid ref: $arg"; exit 1; }
  [[ "$arg" != *'$'* && "$arg" != *'`'* && "$arg" != *'"'* ]] || { echo "ERROR: ref contains shell-unsafe characters: $arg"; exit 1; }
done
[[ "$CHAIN" =~ ^[a-zA-Z0-9_.-]+$ ]] || { echo "ERROR: invalid chain name (no slashes): $CHAIN"; exit 1; }

# --- Precondition validation ---
for d in "$ERIGON_A" "$ERIGON_B"; do
  [[ -d "$d/.git" ]] || { echo "ERROR: $d is not a git clone"; exit 1; }
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for f in "$SCRIPT_DIR/config_A.toml" "$SCRIPT_DIR/config_B.toml"; do
  [[ -f "$f" ]] || { echo "ERROR: missing $f"; exit 1; }
done

command -v tmux >/dev/null || { echo "ERROR: tmux not in PATH"; exit 1; }

[[ -d "$DATADIR_BASE" && -w "$DATADIR_BASE" ]] || {
  echo "ERROR: $DATADIR_BASE does not exist or is not writable"
  echo "  Fix: sudo mkdir -p $DATADIR_BASE && sudo chown \$USER $DATADIR_BASE"
  exit 1
}

# --- Worktree cleanliness (no silent stash) ---
check_clean() {
  local dir=$1
  git -C "$dir" diff --quiet && git -C "$dir" diff --cached --quiet \
    || { echo "ERROR: $dir has uncommitted changes — refusing to checkout"; exit 1; }
}
check_clean "$ERIGON_A"
check_clean "$ERIGON_B"

# --- Fetch / checkout / pull (sequential) ---
# Treats branches as fetched & fast-forwarded; tags/SHAs as detached checkouts (no pull).
prepare_ref() {
  local dir=$1 ref=$2
  echo "Preparing $dir on ref $ref..."
  git -C "$dir" fetch --tags origin "$ref" 2>/dev/null || git -C "$dir" fetch --tags origin
  if git -C "$dir" show-ref --verify --quiet "refs/heads/$ref"; then
    git -C "$dir" switch -- "$ref"
    git -C "$dir" pull --ff-only origin "$ref"
  elif git -C "$dir" show-ref --verify --quiet "refs/remotes/origin/$ref"; then
    # remote-only branch: create a tracking local branch
    git -C "$dir" switch -c "$ref" --track "origin/$ref"
  elif git -C "$dir" show-ref --verify --quiet "refs/tags/$ref"; then
    git -C "$dir" switch --detach "refs/tags/$ref"
  else
    # treat as SHA / arbitrary commit-ish
    git -C "$dir" switch --detach "$ref"
  fi
}
prepare_ref "$ERIGON_A" "$BRANCH_A"
prepare_ref "$ERIGON_B" "$BRANCH_B"

echo "All preconditions passed. Refs checked out."

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
sha_a=$(git -C "$ERIGON_A" rev-parse --short HEAD)
subj_a=$(git -C "$ERIGON_A" log -1 --pretty=format:%s)
sha_b=$(git -C "$ERIGON_B" rev-parse --short HEAD)
subj_b=$(git -C "$ERIGON_B" log -1 --pretty=format:%s)

# --- Paths ---
TS=$(date -u +%Y%m%d-%H%M%S)
slug() { printf '%s' "$1" | tr '/' '-'; }
RUNDIR="$RUNS_BASE/$TS-$(slug "$BRANCH_A")-vs-$(slug "$BRANCH_B")"
DATADIR_A=${DATADIR_A_OVERRIDE:-$DATADIR_BASE/A_$CHAIN}
DATADIR_B=${DATADIR_B_OVERRIDE:-$DATADIR_BASE/B_$CHAIN}

mkdir -p "$RUNDIR" "$DATADIR_A" "$DATADIR_B"
cp "$SCRIPT_DIR/config_A.toml" "$RUNDIR/config_A.toml"
cp "$SCRIPT_DIR/config_B.toml" "$RUNDIR/config_B.toml"

# --- Journal (metadata.md) ---
if $RESET; then
  reset_note='enabled — \`integration stage_exec --reset\` runs before erigon'
else
  reset_note='disabled — datadirs preserved as-is (pass --reset to opt in)'
fi

cat > "$RUNDIR/metadata.md" <<EOF
# Erigon A/B bench — $TS UTC

## A
- ref: \`$BRANCH_A\`
- commit: \`$sha_a\` ($subj_a)
- datadir: \`$DATADIR_A\`
- config: ./config_A.toml (frozen copy in this dir)
- metrics: http://localhost:6061/debug/metrics/prometheus

## B
- ref: \`$BRANCH_B\`
- commit: \`$sha_b\` ($subj_b)
- datadir: \`$DATADIR_B\`
- config: ./config_B.toml (frozen copy in this dir)
- metrics: http://localhost:6062/debug/metrics/prometheus

## Chain
$CHAIN

## Environment (exported in run_*.sh)
- ERIGON_KV_READ_METRICS=true (enables kv_get_{count,sum} and trie_state_levelled_{load,skip}_rate)
$([ "$NO_DEEP_MERGE" = true ] && echo "- ERIGON_NO_DEEP_MERGE_HISTORY=true (skip deep history merges)")

## Wrappers / erigon args
- mem-limit-32g: $MEM_LIMIT_32G $([ "$MEM_LIMIT_32G" = true ] && echo " — systemd-run --user -p MemoryMax=32G wraps erigon")
- block-limit-1: $BLOCK_LIMIT_1 $([ "$BLOCK_LIMIT_1" = true ] && echo " — rm-state --latest pre-flight + --sync.loop.block.limit=1 erigon arg")

## Prep (runs in each pane before erigon)
- exec-stage reset: $reset_note

## Launch
- tmux session: erigon-bench-$TS
- logs: A.log, B.log (tee'd live)
EOF

# --- Echo summary ---
cat "$RUNDIR/metadata.md"
echo
echo "Starting tmux session erigon-bench-$TS..."

# --- Tmux launch ---
# Unset to allow launching a fresh session even when bench.sh itself is invoked
# from inside an existing tmux session.
unset TMUX TMUX_PANE
SESSION="erigon-bench-$TS"
write_pane_script() {
  local side=$1 dir=$2 datadir=$3

  # Build env-var exports
  local exports='export ERIGON_KV_READ_METRICS=true'
  if $NO_DEEP_MERGE; then
    exports="$exports"$'\n'"export ERIGON_NO_DEEP_MERGE_HISTORY=true"
  fi

  # Build optional pre-flight steps
  local preflight=""
  if $RESET; then
    preflight+=$'\n'"  echo \"=== stage_exec --reset ===\" &&"
    preflight+=$'\n'"  ./build/bin/integration stage_exec --datadir=\"$datadir\" --chain=\"$CHAIN\" --reset &&"
  fi
  if $BLOCK_LIMIT_1; then
    preflight+=$'\n'"  echo \"=== erigon snapshots rm-state --latest ===\" &&"
    preflight+=$'\n'"  echo 1 | ./build/bin/erigon snapshots rm-state --latest --datadir=\"$datadir\" &&"
  fi

  # Build erigon command (with optional systemd-run wrap and block-limit arg)
  local erigon_args="--config=\"$RUNDIR/config_$side.toml\" --datadir=\"$datadir\" --chain=\"$CHAIN\""
  if $BLOCK_LIMIT_1; then
    erigon_args="$erigon_args --sync.loop.block.limit=1"
  fi
  local erigon_cmd
  if $MEM_LIMIT_32G; then
    erigon_cmd="systemd-run --user -P -t -G --wait -p MemoryMax=32G ./build/bin/erigon $erigon_args"
  else
    erigon_cmd="./build/bin/erigon $erigon_args"
  fi

  cat > "$RUNDIR/run_$side.sh" <<EOS
#!/usr/bin/env bash
set -o pipefail
$exports
cd "$dir" && {$preflight
  echo "=== erigon ===" &&
  $erigon_cmd
} 2>&1 | tee "$RUNDIR/$side.log"
EOS
  chmod +x "$RUNDIR/run_$side.sh"
}
write_pane_script A "$ERIGON_A" "$DATADIR_A"
write_pane_script B "$ERIGON_B" "$DATADIR_B"

tmux new-session     -d -s "$SESSION" -n bench "$RUNDIR/run_A.sh"
tmux set-window-option -t "$SESSION:bench" pane-base-index 0
tmux set-window-option -t "$SESSION:bench" pane-border-status top
tmux split-window    -v -t "$SESSION:bench" "$RUNDIR/run_B.sh"
tmux select-pane     -t "$SESSION:bench.0" -T "A: $BRANCH_A"
tmux select-pane     -t "$SESSION:bench.1" -T "B: $BRANCH_B"
tmux select-pane     -t "$SESSION:bench.0"

if [[ -t 0 ]]; then
  tmux attach-session -t "$SESSION"
else
  echo "Detached run; reattach with: tmux attach -t $SESSION"
fi
