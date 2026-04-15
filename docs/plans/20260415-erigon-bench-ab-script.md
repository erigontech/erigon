# Erigon A/B Benchmarking Script

## Overview

A single-host side-by-side benchmarking harness that runs two Erigon nodes — one per git
branch — against the same chain, exposing their metrics on adjacent ports (6061 and 6062)
so Prometheus/Grafana can compare them directly.

- **Problem:** comparing two Erigon branches under the same workload currently requires
  manual directory management, manual flag juggling to avoid port conflicts, and ad-hoc
  output capture. Easy to get wrong, hard to reproduce.
- **Benefit:** one command (`./bench.sh <branchA> <branchB> <chain>`) does the full cycle —
  checkout, build, reset execution stage, launch both in a split tmux session — and
  writes a per-run directory with all metadata frozen for later analysis.
- **Integration:** deliverable is three standalone files (script + two configs) placed in
  a user-chosen tooling directory — **not** committed to the erigon repo itself. Operates
  on two pre-existing local clones at `~/erigon_A/` and `~/erigon_B/`.

## Context

- **Project:** Erigon — Ethereum execution client (Go). `make erigon` and `make integration`
  produce `./build/bin/erigon` and `./build/bin/integration` respectively.
- **Relevant existing skills referenced during brainstorm:**
  - `erigon-network-ports` — authoritative list of all flags binding ports.
  - `erigon-exec-from-0` — authoritative form of `integration stage_exec --reset`.
- **Chain handling:** `--chain=<name>` is passed through to both erigon and integration.
  Caplin runs embedded in erigon for chains that need a CL; no separate CL process needed.
- **Port map convention:** A uses erigon defaults; B uses `+100` on every port flag
  (metrics uses `+1` for adjacency in Grafana).

## Development Approach

- **Testing approach:** Regular (code first, manual end-to-end verification). This is a
  bash orchestrator — unit testing bash is low-value; the testing strategy is explicit
  manual verification scenarios run against a real datadir after each task.
- Complete each task fully before moving to the next.
- Make small, focused changes.
- **Each task ends with a verification checklist item that must pass before next task.**
- Maintain `set -euo pipefail` discipline throughout.

## Testing Strategy

- **No unit tests** — target is a ~150-line bash script; behavior is trivially verified
  by running it.
- **Manual verification scenarios** (each task lists the scenarios it enables):
  - (a) **Missing-dir failure:** with `~/erigon_A` absent, script aborts before any git/build
    action.
  - (b) **Dirty-worktree failure:** with uncommitted changes in either clone, script aborts
    and names the dirty dir.
  - (c) **Successful run:** after a clean run, the run dir contains exactly four artifacts:
    `metadata.md`, `config_A.toml`, `config_B.toml`, and (once erigons have started writing)
    `A.log` + `B.log`.
  - (d) **Repeat run:** second invocation creates a new run dir without touching the first;
    datadirs are NOT wiped; only exec stage is reset.
- **Dry-run flag:** none. Out of scope per brainstorm.

## Progress Tracking

- Mark completed items with `[x]` immediately when done.
- Add newly discovered tasks with ➕ prefix.
- Document issues/blockers with ⚠️ prefix.
- Update plan if implementation deviates from original scope.

## Solution Overview

The deliverable is three files shipped together in a user-chosen directory (not checked
into erigon):

```
<tooling-dir>/
  bench.sh           # orchestrator
  config_A.toml      # A-side port map + metrics=true + pprof=true
  config_B.toml      # B-side port map (+100 offset, metrics.port=6062)
```

The script's CLI composition is intentionally minimal — it only passes three flags per
instance (`--config`, `--datadir`, `--chain`). All port, metrics, and pprof configuration
lives in the TOML files, which the user owns and edits freely.

**Fixed on-disk conventions:**

| Path | Purpose |
|------|---------|
| `~/erigon_A/` | Clone for branch A (must pre-exist, real clone — not git worktree) |
| `~/erigon_B/` | Clone for branch B (must pre-exist) |
| `/erigon-data/A_<chain>` | A's datadir (created if missing; NOT wiped) |
| `/erigon-data/B_<chain>` | B's datadir (created if missing; NOT wiped) |
| `~/bench-runs/<ts>-<A-slug>-vs-<B-slug>/` | Per-run frozen artifacts |

**Phase ordering (strict):**

1. **Prep phase** (script foreground, sequential, fails loudly):
   validate → worktree-clean check → fetch/checkout/pull → build → SHA resolution →
   mkdir datadirs + rundir → copy configs → write metadata.md → echo summary.
2. **Tmux phase:**
   create detached session → split horizontally (A top, B bottom) → each pane runs
   `integration stage_exec --reset && erigon --config ...` under `tee` → attach.

Both resets run **concurrently** (one per pane), then each pane's erigon launches as soon
as that side's reset succeeds. `tee` captures both reset and erigon output to the run dir.

## Technical Details

### Port map

Shipped in the TOML configs (user-editable; script never overrides):

| CLI flag | A value | B value |
|----------|---------|---------|
| `port` | 30303 | 30403 |
| `p2p.allowed-ports` | `30303,30304,30305,30306,30307` | `30403,30404,30405,30406,30407` |
| `torrent.port` | 42069 | 42169 |
| `private.api.addr` | `127.0.0.1:9090` | `127.0.0.1:9190` |
| `http.port` | 8545 | 8645 |
| `ws.port` | 8546 | 8646 |
| `authrpc.port` | 8551 | 8651 |
| `mcp.port` | 8553 | 8653 |
| `caplin.discovery.port` | 4000 | 4100 |
| `caplin.discovery.tcpport` | 4001 | 4101 |
| `sentinel.port` | 7777 | 7877 |
| `beacon.api.port` | 5555 | 5655 |
| `metrics` + `metrics.port` | `true` / 6061 | `true` / 6062 |
| `pprof` + `pprof.port` | `true` / 6060 | `true` / 6160 |

### config_A.toml (full contents)

```toml
# Erigon A-side benchmarking config (defaults).
# Metrics exposed on :6061 — paired with B on :6062 for Grafana.

metrics = true
"metrics.addr" = "0.0.0.0"
"metrics.port" = 6061

pprof = true
"pprof.port" = 6060

port = 30303
"p2p.allowed-ports" = "30303,30304,30305,30306,30307"
"torrent.port" = 42069

"private.api.addr" = "127.0.0.1:9090"
"http.port" = 8545
"ws.port" = 8546
"authrpc.port" = 8551
"mcp.port" = 8553

"caplin.discovery.port" = 4000
"caplin.discovery.tcpport" = 4001
"sentinel.port" = 7777
"beacon.api.port" = 5555
```

### config_B.toml (full contents)

```toml
# Erigon B-side benchmarking config (+100 offset on all ports except metrics, which is +1).
# Metrics exposed on :6062 — paired with A on :6061 for Grafana.

metrics = true
"metrics.addr" = "0.0.0.0"
"metrics.port" = 6062

pprof = true
"pprof.port" = 6160

port = 30403
"p2p.allowed-ports" = "30403,30404,30405,30406,30407"
"torrent.port" = 42169

"private.api.addr" = "127.0.0.1:9190"
"http.port" = 8645
"ws.port" = 8646
"authrpc.port" = 8651
"mcp.port" = 8653

"caplin.discovery.port" = 4100
"caplin.discovery.tcpport" = 4101
"sentinel.port" = 7877
"beacon.api.port" = 5655
```

**TOML syntax note:** dotted keys are quoted to force them to be treated as literal
single keys (not nested tables). Erigon's urfave/cli v2 config loader accepts this form.
If erigon rejects a quoted dotted key on load, fall back to the unquoted nested-table
form (e.g. `[torrent]\nport = 42069`).

### bench.sh structure (pseudocode)

```bash
#!/usr/bin/env bash
set -euo pipefail

# --- Constants ---
ERIGON_A=~/erigon_A
ERIGON_B=~/erigon_B
DATADIR_BASE=/erigon-data
RUNS_BASE=~/bench-runs

# --- Inputs ---
[[ $# -eq 3 ]] || { echo "usage: bench.sh <branchA> <branchB> <chain>"; exit 1; }
BRANCH_A=$1
BRANCH_B=$2
CHAIN=$3

# --- Precondition validation ---
for d in "$ERIGON_A" "$ERIGON_B"; do
  [[ -d "$d/.git" ]] || { echo "ERROR: $d is not a git clone"; exit 1; }
done
for f in ./config_A.toml ./config_B.toml; do
  [[ -f "$f" ]] || { echo "ERROR: missing $f in CWD"; exit 1; }
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
  git -C "$dir" fetch origin "$branch"
  git -C "$dir" checkout "$branch"
  git -C "$dir" pull --ff-only origin "$branch"
}
prepare_branch "$ERIGON_A" "$BRANCH_A"
prepare_branch "$ERIGON_B" "$BRANCH_B"

# --- Build (sequential; make uses -jN internally) ---
( cd "$ERIGON_A" && make erigon integration )
( cd "$ERIGON_B" && make erigon integration )

# --- Verify binaries exist ---
for d in "$ERIGON_A" "$ERIGON_B"; do
  for b in erigon integration; do
    [[ -x "$d/build/bin/$b" ]] || { echo "ERROR: missing $d/build/bin/$b"; exit 1; }
  done
done

# --- SHAs ---
# Sanitize `, $, \ so commit subjects containing them don't break the unquoted heredoc.
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
cp ./config_A.toml "$RUNDIR/config_A.toml"
cp ./config_B.toml "$RUNDIR/config_B.toml"

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

# --- Tmux launch ---
SESSION="erigon-bench-$TS"
make_cmd() {
  local side=$1 dir=$2 datadir=$3
  # Quote vars inside the generated command so spaces/metachars in paths don't break the shell.
  cat <<EOS
cd "$dir" && { echo "=== stage_exec --reset ==="; \
  ./build/bin/integration stage_exec --datadir="$datadir" --chain="$CHAIN" --reset && \
  echo "=== erigon ===" && \
  ./build/bin/erigon --config="$RUNDIR/config_$side.toml" --datadir="$datadir" --chain="$CHAIN"; \
} 2>&1 | tee "$RUNDIR/$side.log"
EOS
}
CMD_A=$(make_cmd A "$ERIGON_A" "$DATADIR_A")
CMD_B=$(make_cmd B "$ERIGON_B" "$DATADIR_B")

tmux new-session     -d -s "$SESSION" -n bench "$CMD_A"
# Force pane-base-index=0 at session scope so user's ~/.tmux.conf can't shift pane IDs.
tmux set-option      -t "$SESSION" pane-base-index 0
# pane-border-status is a window option; set it BEFORE attach (attach blocks until detach).
tmux set-window-option -t "$SESSION:bench" pane-border-status top
tmux split-window    -v -t "$SESSION:bench" "$CMD_B"
tmux select-pane     -t "$SESSION:bench.0" -T "A: $BRANCH_A"
tmux select-pane     -t "$SESSION:bench.1" -T "B: $BRANCH_B"
tmux select-pane     -t "$SESSION:bench.0"
tmux attach-session  -t "$SESSION"
```

### metadata.md template (literal)

```
# Erigon A/B bench — <UTC timestamp>

## A
- branch: `<branchA>`
- commit: `<short sha>` (<commit subject>)
- datadir: `/erigon-data/A_<chain>`
- config: ./config_A.toml (frozen copy in this dir)
- metrics: http://localhost:6061/debug/metrics/prometheus

## B
- branch: `<branchB>`
- commit: `<short sha>` (<commit subject>)
- datadir: `/erigon-data/B_<chain>`
- config: ./config_B.toml (frozen copy in this dir)
- metrics: http://localhost:6062/debug/metrics/prometheus

## Chain
<chainName>

## Prep (runs in each pane before erigon)
- exec-stage: reset via `integration stage_exec --reset`

## Launch
- tmux session: erigon-bench-<ts>
- logs: A.log, B.log (tee'd live)
```

### Out of scope (explicitly excluded)

- No `rm-all-state-snapshots` (reset only via `stage_exec --reset`).
- No datadir wiping between runs.
- No parallel git fetch or build (sequential for readable output).
- No port-availability precheck (erigon fails loudly if port is bound).
- No SIGINT trap in `bench.sh` (lifecycle managed via tmux after attach).
- No separate rpcdaemon or caplin process (caplin runs embedded).
- No optional flags (`--wipe-state`, `--build`, `--dry-run`, etc.).
- No auto-clone of missing `~/erigon_A` / `~/erigon_B` (fail fast instead).

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): files to create, manual verification
  scenarios runnable on the dev host.
- **Post-Completion** (no checkboxes): Grafana dashboard setup, deciding where the final
  deliverable directory lives, any team distribution steps.

## Implementation Steps

### Task 1: Create config_A.toml with A-side port map

**Files:**
- Create: `<tooling-dir>/config_A.toml`

- [ ] create `config_A.toml` with full A-side port map per the "Technical Details / config_A.toml" section above
- [ ] verify every port flag from the port map table is present (visual diff against the table)
- [ ] real TOML parse is deferred to scenario (c) in Task 7 — erigon itself will reject bad config on startup, which is the only parser that matters

### Task 2: Create config_B.toml with B-side port map

**Files:**
- Create: `<tooling-dir>/config_B.toml`

- [ ] create `config_B.toml` with full B-side port map (+100 on every port, metrics uses 6062)
- [ ] diff A vs B: every numeric port value should differ; no accidental sharing (quick `diff config_A.toml config_B.toml` sanity check)

### Task 3: Create bench.sh skeleton with arg validation + precondition checks

**Files:**
- Create: `<tooling-dir>/bench.sh`

- [ ] add shebang `#!/usr/bin/env bash` and `set -euo pipefail`
- [ ] parse three positional args (branchA, branchB, chain); print usage and exit 1 on wrong count
- [ ] validate `~/erigon_A/.git`, `~/erigon_B/.git`, `./config_A.toml`, `./config_B.toml`, and `tmux` binary
- [ ] make executable: `chmod +x bench.sh`
- [ ] **verify scenario (a):** move `~/erigon_A` aside temporarily and run the script — must abort with a clear error pointing at `~/erigon_A` before touching anything else

### Task 4: Add worktree-clean check + git fetch/checkout/pull

**Files:**
- Modify: `<tooling-dir>/bench.sh`

- [ ] add `check_clean` function using `git diff --quiet && git diff --cached --quiet`
- [ ] run `check_clean` on both clones before any git-mutating command
- [ ] add `prepare_branch` function: `git fetch origin <branch> && git checkout <branch> && git pull --ff-only origin <branch>`
- [ ] call `prepare_branch` for A then B (sequential)
- [ ] **verify scenario (b):** create a dummy uncommitted change in `~/erigon_A` (`touch x && git add x`), run the script, confirm it aborts naming `~/erigon_A`; clean up the dummy change

### Task 5: Add build phase + binary verification

**Files:**
- Modify: `<tooling-dir>/bench.sh`

- [ ] run `( cd ~/erigon_A && make erigon integration )` then same for B (sequential subshells)
- [ ] verify `./build/bin/erigon` and `./build/bin/integration` exist and are executable in both dirs; abort with named missing binary otherwise
- [ ] **verify:** run the script to this point (comment out tasks 6–7 temporarily); confirm four freshly-built binaries exist: `ls -la ~/erigon_{A,B}/build/bin/{erigon,integration}`

### Task 6: Add SHA resolution + rundir creation + metadata.md + config snapshot

**Files:**
- Modify: `<tooling-dir>/bench.sh`

- [ ] resolve `sha_a`/`subj_a`/`sha_b`/`subj_b` via `git -C <dir> rev-parse --short HEAD` and `git log -1 --pretty=%s`
- [ ] compute `TS` as `date -u +%Y%m%d-%H%M%S`; compute branch slugs (`tr '/' '-'`)
- [ ] `mkdir -p` the rundir (`~/bench-runs/$TS-<A-slug>-vs-<B-slug>`), `$DATADIR_A`, `$DATADIR_B`
- [ ] copy `./config_A.toml` and `./config_B.toml` into rundir (plain `cp`, not symlinks)
- [ ] write `$RUNDIR/metadata.md` via heredoc matching the template verbatim
- [ ] echo `metadata.md` contents to stdout, then "Starting tmux session $SESSION..."
- [ ] **verify:** run script up to this point (comment out tmux launch temporarily); inspect the rundir for the four expected artifacts (`metadata.md`, `config_A.toml`, `config_B.toml`, no logs yet)

### Task 7: Add tmux session launch (split, pane commands, attach)

**Files:**
- Modify: `<tooling-dir>/bench.sh`

- [ ] build `CMD_A` and `CMD_B` via a `make_cmd` helper — each wraps `integration stage_exec --reset && erigon --config=...` inside `{ ...; } 2>&1 | tee "$RUNDIR/X.log"`; all path vars inside the heredoc must be double-quoted
- [ ] `tmux new-session -d -s "$SESSION" -n bench "$CMD_A"`
- [ ] force `tmux set-option -t "$SESSION" pane-base-index 0` (defends against user's `~/.tmux.conf` setting it to 1)
- [ ] enable pane titles via `tmux set-window-option -t "$SESSION:bench" pane-border-status top` — **must run before** `attach-session` (attach blocks)
- [ ] `tmux split-window -v -t "$SESSION:bench" "$CMD_B"` (horizontal split: A top, B bottom)
- [ ] set pane titles via `tmux select-pane -t "$SESSION:bench.0" -T "A: $BRANCH_A"` / `.1 -T "B: $BRANCH_B"`
- [ ] select pane 0, then `tmux attach-session -t "$SESSION"` (last command in script)
- [ ] **verify scenario (c):** run the script with real inputs on a test chain (`chiado` recommended — small, fast reset); detach tmux after both panes show erigon heartbeat logs; confirm rundir contains all four artifacts with `A.log` and `B.log` non-empty

### Task 8: Verify repeat-run semantics (scenario d)

- [ ] after task 7 completes once, invoke `./bench.sh <same args>` a second time
- [ ] confirm the first rundir is untouched (same mtime, same content)
- [ ] confirm a new rundir with a later timestamp is created
- [ ] confirm datadirs are **not** wiped (inspect `/erigon-data/A_<chain>` — all MDBX files and snapshots still present; only exec stage progress inside the DB was reset, mdbx.dat may reclaim pages but is not deleted)
- [ ] confirm metrics endpoints are reachable: `curl -s http://localhost:6061/debug/metrics/prometheus | head` and `:6062`

### Task 9: Verify acceptance criteria
- [ ] all four manual verification scenarios (a–d) pass
- [ ] metadata.md content matches the template verbatim for a real run
- [ ] both metrics endpoints (6061 and 6062) respond during a run
- [ ] `tmux kill-session -t erigon-bench-<ts>` cleanly terminates both erigons (check `ps` afterwards)

### Task 10: [Final] Update documentation
- [ ] decide and record the final tooling-dir location (e.g. `~/org/wrk/erigon-bench/`)
- [ ] add a short `README.md` alongside the three files describing invocation, preconditions, and where artifacts land
- [ ] move this plan to `docs/plans/completed/` (create dir if needed)

## Post-Completion

**Manual verification:**
- Run a real benchmark on a non-trivial chain (`mainnet` or `sepolia`) with two meaningfully
  different branches, confirm Grafana sees both metrics endpoints as distinct targets.
- Stress: let both erigons run for ≥1 hour and verify no port conflicts or shared-resource
  issues emerge (file locks on snapshots, MDBX contention with shared `/erigon-data` parent).

**External system updates:**
- Grafana: add `localhost:6061` and `localhost:6062` as two Prometheus scrape targets,
  labeled `side=A` / `side=B` so existing dashboards can be cloned and filtered.
- Team distribution: decide whether the three-file deliverable lives in a personal
  tooling repo, a shared dotfiles repo, or gets proposed as a contribution to
  `erigon-infra`. This plan does not commit it into the erigon repo.
