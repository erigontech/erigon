# Erigon A/B Benchmark Harness

Side-by-side benchmarking of two Erigon branches on a single host. Runs both
nodes in a split tmux session with separate datadirs and port maps, exposing
metrics on adjacent ports (6061 / 6062) for Prometheus/Grafana comparison.

## Preconditions

- Two independent Erigon clones at `~/erigon_A/` and `~/erigon_B/` (real
  clones, not git worktrees).
- Both clones must have clean worktrees (no uncommitted changes).
- `tmux` must be installed and on `PATH`.
- Go 1.25+, GCC 10+ or Clang (for `make erigon integration`).

## Files

| File | Purpose |
|------|---------|
| `bench.sh` | Orchestrator script |
| `config_A.toml` | A-side port/metrics/pprof config (erigon defaults) |
| `config_B.toml` | B-side port/metrics/pprof config (+100 offset, metrics on 6062) |

## Usage

```bash
./bench.sh <branchA> <branchB> <chain>
```

Example:

```bash
./bench.sh main feature/new-trie mainnet
```

The script will:

1. Validate preconditions (clones exist, worktrees clean, configs present, tmux available).
2. Fetch, checkout, and pull each branch in its respective clone.
3. Build `erigon` and `integration` binaries in both clones.
4. Create a timestamped run directory under `~/bench-runs/` with frozen config
   copies and a `metadata.md` journal.
5. Create datadirs at `/erigon-data/A_<chain>` and `/erigon-data/B_<chain>` if
   they don't already exist (never wiped between runs).
6. Launch a tmux session with two panes: each resets the execution stage via
   `integration stage_exec --reset`, then starts `erigon`. Output is tee'd to
   `A.log` / `B.log` in the run directory.

## Artifacts

Each run creates:

```
~/bench-runs/<timestamp>-<branchA-slug>-vs-<branchB-slug>/
  metadata.md      # Branch SHAs, datadirs, config paths, metrics URLs
  config_A.toml    # Frozen copy of A-side config
  config_B.toml    # Frozen copy of B-side config
  A.log            # Combined stdout+stderr from A's reset + erigon
  B.log            # Combined stdout+stderr from B's reset + erigon
```

## Port Map

A uses erigon defaults; B uses +100 on every port (metrics uses +1 for Grafana adjacency).

| Port | A | B |
|------|---|---|
| metrics | 6061 | 6062 |
| pprof | 6060 | 6160 |
| p2p | 30303 | 30403 |
| torrent | 42069 | 42169 |
| private API | 9090 | 9190 |
| HTTP RPC | 8545 | 8645 |
| WebSocket | 8546 | 8646 |
| Auth RPC | 8551 | 8651 |
| MCP | 8553 | 8653 |
| Caplin discovery | 4000 | 4100 |
| Caplin TCP | 4001 | 4101 |
| Sentinel | 7777 | 7877 |
| Beacon API | 5555 | 5655 |

Edit `config_A.toml` / `config_B.toml` to change ports. The script passes only
`--config`, `--datadir`, and `--chain` to erigon.

## Stopping

Detach from the tmux session with `Ctrl-b d`. Kill both nodes:

```bash
tmux kill-session -t erigon-bench-<timestamp>
```
