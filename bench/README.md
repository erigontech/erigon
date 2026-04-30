# Erigon A/B Benchmark Harness

Side-by-side benchmarking of two Erigon refs (branch / tag / SHA) on a single
host. Runs both nodes in a split tmux session with separate datadirs and port
maps, exposing metrics on adjacent ports (6061 / 6062) for Prometheus/Grafana
comparison.

## Preconditions

- Two independent Erigon clones at `~/erigon_A/` and `~/erigon_B/` (real
  clones, not git worktrees). The script switches each clone to the
  requested ref before building.
- Both clones must have clean worktrees (no uncommitted changes — refused).
- `/erigon-data/` must exist and be writable (create with
  `sudo mkdir -p /erigon-data && sudo chown $USER /erigon-data`, or adjust
  `DATADIR_BASE` in `bench.sh`).
- `tmux` must be installed and on `PATH`.
- Go 1.25+, GCC 10+ or Clang (for `make erigon integration`). On hosts where
  the system Go is older, prepend a user-local install to `PATH`:
  `PATH=$HOME/go-1.25/bin:$PATH ./bench/bench.sh ...`

## Files

| File | Purpose |
|------|---------|
| `bench.sh` | Orchestrator script |
| `config_A.toml` | A-side port/metrics/pprof config (erigon defaults) |
| `config_B.toml` | B-side port/metrics/pprof config (+100 offset, metrics on 6062) |

## Usage

```
./bench.sh <refA> <refB> <chain> [datadirA] [datadirB] [--reset]
```

- `refA` / `refB`: local branch, remote-only branch, tag, or SHA. The script
  detects which form and checks out accordingly (tags / SHAs go to detached
  HEAD; remote-only branches get a tracking branch created).
- `chain`: e.g. `mainnet`, `holesky`, `dev`.
- `datadirA` / `datadirB` *(optional)*: explicit datadirs. When omitted,
  defaults to `/erigon-data/A_<chain>` and `/erigon-data/B_<chain>`.
- `--reset` *(opt-in)*: runs `integration stage_exec --reset` on each datadir
  before erigon starts. Off by default — i.e. the harness will resume from
  whatever exec state the datadir already has.

### Examples

Same datadirs every time, defaults:

```bash
./bench.sh main feature/new-trie mainnet
```

Custom datadirs (typical for the per-host bench setup):

```bash
./bench.sh awskii/inc-shard-def-size v3.4.0 mainnet \
  /erigon-data/mainnet-main /erigon-data/mainnet-3.4
```

Same again but force a clean exec-stage reset on both sides (use after a
restart or whenever you want a fully clean execution baseline):

```bash
./bench.sh awskii/inc-shard-def-size v3.4.0 mainnet \
  /erigon-data/mainnet-main /erigon-data/mainnet-3.4 --reset
```

The script will:

1. Validate preconditions (clones exist, worktrees clean, configs present,
   tmux available, datadir base writable).
2. Fetch and check out each ref in its respective clone (branch fast-forward,
   or tag/SHA detached checkout — no pull on detached refs).
3. Build `erigon` and `integration` binaries in both clones.
4. Create a timestamped run directory under `~/bench-runs/` with frozen
   config copies and a `metadata.md` journal.
5. Create the supplied datadirs (`mkdir -p` — never wipes existing data).
6. Launch a tmux session with two panes: optionally reset, then start
   `erigon` with `ERIGON_KV_READ_METRICS=true` exported. Output is `tee`'d to
   `A.log` and `B.log` in the run directory.

The script supports being invoked from inside an existing tmux session (it
unsets `$TMUX` before creating its own). When stdin is not a TTY (e.g. ssh
non-interactive), it prints the reattach hint and exits cleanly instead of
trying to attach.

## Required env var

`ERIGON_KV_READ_METRICS=true` is exported by the launcher scripts. Without
it, the per-level commitment counters (`kv_get_count{level,domain}`,
`trie_state_levelled_{load,skip}_rate{level,key}`) report zero — that's the
whole point of the A/B harness. If you launch erigon outside of `bench.sh`
(e.g. by hand to repro something), don't forget to set it.

## Artifacts

Each run creates:

```
~/bench-runs/<timestamp>-<refA-slug>-vs-<refB-slug>/
  metadata.md      # Refs + SHAs, datadirs, config paths, env, metrics URLs
  config_A.toml    # Frozen copy of A-side config
  config_B.toml    # Frozen copy of B-side config
  run_A.sh         # Launcher script for A (env + erigon, optional reset)
  run_B.sh         # Launcher script for B
  A.log            # Combined stdout+stderr from A's run_A.sh
  B.log            # Combined stdout+stderr from B's run_B.sh
```

## Port Map

A uses erigon defaults; B uses +100 on every port (metrics uses +1 for
Grafana adjacency). Edit `config_A.toml` / `config_B.toml` to change ports.

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

The script passes only `--config`, `--datadir`, and `--chain` to erigon. All
other CLI options live in the per-side TOML config so a re-run is exactly
reproducible from the frozen copies in the run directory.

### Hosts already running another erigon on `:6061`

If `:6061` is already taken on the target host, either stop that other
erigon, or shift `config_A.toml` / `config_B.toml` to a free range
(e.g. `+200`/`+300` offsets) and update the alloy scrape blocks
correspondingly. See the alloy section below.

## Pre-launch checks

Before invoking the script for the first time on a host:

1. Both target datadirs are at the same head block (so the comparison
   starts from a common baseline). Eyeball with:

   ```bash
   for d in /erigon-data/mainnet-main /erigon-data/mainnet-3.4; do
     echo "$d:"
     ls -la "$d/chaindata/mdbx.dat" "$d/snapshots/" 2>/dev/null | head -5
   done
   ```

2. The datadirs were initialized with the same `--prune.mode`. Erigon
   refuses to start if `--prune.*` flags change between runs. If the existing
   state is `minimal`, both `config_A.toml` and `config_B.toml` already pin
   `"prune.mode" = "minimal"` — adjust if you used a different mode at init.

3. None of the bench ports listed above are already in use (`ss -tlnp |
   grep -E ":(6061|6062|30303|30403|8545|8645)\b"`).

4. After launch, hit each metrics endpoint to verify both nodes are alive and
   the env-gated counters are populating:

   ```bash
   curl -s http://127.0.0.1:6061/debug/metrics/prometheus | grep '^kv_get_count{domain="account"' | head -6
   curl -s http://127.0.0.1:6062/debug/metrics/prometheus | grep '^kv_get_count{domain="account"' | head -6
   ```

   Non-zero series across the L0–L4 / `recent` levels means
   `ERIGON_KV_READ_METRICS=true` was honored.

## Grafana / alloy integration

The bench is wired into the org alloy template at
`erigontech/erigon-infrastructure-configurations` →
`roles/infra_config/templates/alloy-configs.j2`. For hosts in the
`arbitrum_bench_hosts` Jinja list, the rendered alloy config emits:

- 6061 erigon scrape with `instance="<host>:6061"` and `bench_side="A"`
- 6062 erigon scrape with `instance="<host>:6062"` and `bench_side="B"`
- two phantom node_exporter scrapes that re-emit `node_uname_info` (and
  every other node-exporter metric) with the per-process `instance` labels,
  so dashboards whose `$instance` template variable is sourced from
  `label_values(node_uname_info, instance)` get one dropdown entry per
  erigon process

To add a new bench host, append it to `arbitrum_bench_hosts` in
`alloy-configs.j2` and re-run `ansible-playbook grafana-alloy.yml -e
target_host=<new-host>`.

## Stopping

Detach from the tmux session with `Ctrl-b d`. Kill both nodes:

```bash
tmux kill-session -t erigon-bench-<timestamp>
```

To stop and restart with a clean exec stage:

```bash
tmux kill-session -t erigon-bench-<timestamp>
./bench.sh ... --reset
```

## Recovery from a failed experiment

If a run goes wrong and you need both datadirs back at a common baseline:

1. Stop the bench: `tmux kill-session -t erigon-bench-<timestamp>`.
2. Restore each datadir's snapshots/chaindata to the equal baseline you keep
   elsewhere (manual — the harness doesn't manage backups).
3. Re-run with `--reset` to wipe the exec stage and start clean:

   ```bash
   ./bench.sh <refA> <refB> <chain> <datadirA> <datadirB> --reset
   ```

Verify the two datadirs are at the same head block (see *Pre-launch
checks*) before relaunching, otherwise the comparison starts skewed.
