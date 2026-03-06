---
name: qmtree-bench
description: Build and benchmark qmtree over Ethereum state history. Measures tree creation time and storage size against a synced datadir.
allowed-tools: Bash, Read, Write, Edit, Glob, Grep
user-invocable: true
---

# QMTree Benchmark

Build a qmtree (QMDB Twig Merkle Tree) over real Ethereum state history and measure
performance: tree creation throughput (blocks/s, leaves/s), total runtime, and on-disk
storage size (entry file + twig file).

## Arguments

The user specifies a chain and block range. If omitted, defaults apply.

| Argument | Default | Description |
|----------|---------|-------------|
| `chain` | `hoodi` | Chain name. Determines which datadir to use. `hoodi` or `mainnet`. |
| `datadir` | (auto) | Override datadir path. Auto-detected from chain if omitted. |
| `from` | `1` | Start block number |
| `to` | `0` (latest) | End block number. `0` = latest available. |
| `tree-dir` | (auto) | Where to persist tree files. Defaults to `<datadir>/qmtree-bench/`. |
| `output-dir` | (auto) | Where to write CSV results. Defaults to `<datadir>/qmtree-bench-results/`. |

### Examples
- `/qmtree-bench` — Run on hoodi, full chain, default dirs
- `/qmtree-bench hoodi from=1 to=100000` — First 100k blocks on hoodi
- `/qmtree-bench mainnet from=1 to=1000000` — First 1M blocks on mainnet
- `/qmtree-bench datadir=/data/erigon-mainnet from=18000000 to=18100000` — Custom datadir, specific range

## Procedure

### Phase 0: Pre-flight Checks

1. **Build erigon** if not already built:
   ```bash
   make erigon
   ```

2. **Resolve datadir.** If `datadir` is specified, use it directly. Otherwise, look for
   standard locations based on chain:
   ```bash
   # Check common locations in order of preference:
   # 1. ~/.local/share/erigon-<chain>
   # 2. ~/erigon-<chain>
   # 3. /data/erigon-<chain>
   # For hoodi: also check erigon-hoodi, erigon/hoodi
   # For mainnet: also check erigon, erigon-mainnet
   ```
   If no datadir is found, report an error with instructions.

3. **Check disk space.** The qmtree adds roughly 32 bytes per leaf (entry file) plus
   ~65KB per twig (~2048 leaves). For mainnet (~3B txnums), expect ~96GB entry + ~100GB
   twig. For hoodi (much smaller), expect well under 1GB.
   ```bash
   df -h <tree-dir-parent>
   ```
   If available space is less than estimated need, warn the user and suggest a partial
   range or a different disk.

4. **Check data availability.** The datadir must have state history for accounts and
   storage domains covering the requested block range. Commitment domain is NOT required.
   If the erigon command exits with a "history not available" error, suggest narrowing
   the range.

### Phase 1: Run QMTree Benchmark

```bash
./build/bin/erigon qmtree-bench \
  --datadir <datadir> \
  --from <from> \
  --to <to> \
  --tree-dir <tree-dir> \
  --output-dir <output-dir>
```

The command logs progress every 1000 blocks with:
- Current block number and root hash
- Total leaves appended so far
- Per-block elapsed time
- Cumulative tree file sizes

At completion it logs:
- Total blocks processed
- Total leaves
- Entry file size, twig file size, total size
- Path to persisted tree data

### Phase 2: Collect and Report Results

After the benchmark completes:

1. **Read the CSV output:**
   ```bash
   head -20 <output-dir>/qmtree_roots.csv
   wc -l <output-dir>/qmtree_roots.csv
   ```

2. **Measure final tree size on disk:**
   ```bash
   du -sh <tree-dir>/entries/ <tree-dir>/twigs/ <tree-dir>/
   ```

3. **Compute aggregate stats from the CSV** (blocks/sec, avg time per block,
   total changes, etc.):
   ```bash
   # Total runtime from first to last block elapsed_ns
   # Average blocks/sec
   # Total state changes processed
   ```

4. **Report summary** to the user including:
   - Chain and block range
   - Total runtime (wall clock)
   - Blocks processed and blocks/sec
   - Total leaves (txnums) and leaves/sec
   - Total state changes
   - Entry file size, twig file size, combined size
   - Size per leaf (bytes/leaf)
   - Tree data directory (for re-use or inspection)

### Phase 3: Re-run Support

The benchmark is designed to be re-runnable:

- **Resume from existing tree:** If `--tree-dir` points to an existing tree directory
  with data, the current implementation starts fresh (overwrites). To benchmark
  incrementally, use different `--from` values on successive runs.

- **Compare runs:** Save CSV outputs from different runs and compare them. The CSV
  contains per-block timing so you can identify slow blocks.

- **Clean up:** To start fresh, remove the tree directory:
  ```bash
  rm -rf <tree-dir>/entries/ <tree-dir>/twigs/
  ```

## Troubleshooting

### "history not available for given start/end"
The datadir doesn't have state history covering the requested blocks. Either:
- Use a narrower `--from`/`--to` range within available history
- Use a fully synced archive datadir

### Out of disk space
The tree files grow linearly with the number of leaves. For a partial test:
- Start with a small range (e.g., `--to 100000`) to estimate growth rate
- Extrapolate total size needed for the full range
- Switch to a machine with more disk or use an external volume

### Slow progress / low throughput
- Check if the datadir is on SSD/NVMe (HDD will be very slow for random reads)
- The bottleneck is typically `HistoryRange` + `GetAsOf` reads from state history
- Consider running with `--pprof` to profile:
  ```bash
  ./build/bin/erigon qmtree-bench --pprof --datadir ... --from ... --to ... --output-dir ...
  # In another terminal:
  go tool pprof http://localhost:6060/debug/pprof/profile?seconds=30
  ```
