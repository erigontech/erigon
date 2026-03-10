---
name: erigon-seg-integrity
description: Run integrity checks on Erigon datadirs using the 'erigon seg integrity' command. Use this when the user wants to verify snapshot/segment file integrity or during the snapshot publishing process.
allowed-tools: Bash, Read
---

# Erigon Segment Integrity Check

The `erigon seg integrity` command verifies that snapshot and segment files are internally consistent. This skill knows how to run it efficiently, profile where it spends time, find bottlenecks, tune parallelism and sampling, and interpret results.

## Prerequisites

1. **Erigon must be stopped** — the command requires exclusive MDBX access
2. **Binary must be built** — run `/erigon-build` first if needed
3. **Datadir must exist** — a synced Erigon datadir with snapshot files

---

## Command Syntax

```bash
# Full integrity run (fast checks only, default)
./build/bin/erigon seg integrity --datadir=<path>

# Single-block commitment check (targeted)
./build/bin/erigon seg check-commitment-hist-at-blk --datadir=<path> --block=<N>

# Block-range commitment check (parallel, highly I/O intensive)
./build/bin/erigon seg check-commitment-hist-at-blk-range --datadir=<path> --from=<N> [--to=<M>]
```

---

## Check Taxonomy

The checks are classified into tiers. Always discover the current list dynamically:

```bash
./build/bin/erigon seg integrity --help
```

**Fast checks** (default run): `Blocks`, `HeaderNoGaps`, `BlocksTxnID`, `InvertedIndex`,
`StateProgress`, `HistoryNoSystemTxs`, `CommitmentKvi`, `ReceiptsNoDups`, `RCacheNoDups`,
`CommitmentRoot`, `CommitmentHistVal`, `StateRootVerifyByHistory`, `Publishable`

**Slow checks** (opt-in, can take hours): `StateVerify`

**Deprecated / off by default**: `CommitmentKvDeref`

**Separate subcommands** (not part of `integrity`):
- `check-commitment-hist-at-blk` — recomputes and verifies commitment at a single block
- `check-commitment-hist-at-blk-range` — parallel check across a block range

---

## All Flags

### `erigon seg integrity`

| Flag | Default | Purpose |
|------|---------|---------|
| `--check` | (all fast) | Comma-separated check names to run |
| `--skip-check` | — | Comma-separated check names to skip |
| `--failFast` | `true` | Stop on first error vs collect all warnings |
| `--fromStep` | `0` | Skip files whose step is below this value |
| `--sample` | `0.01` | Fraction of items to sample (0.0–1.0) |
| `--seed` | (random) | Fix seed for reproducible sampling across runs |
| `--file-integrity-cache` | — | Path to cache file; skips re-checking unchanged files |
| `--skip-torrent-verify` | `false` | Skip torrent piece hash verification when using cache |

### `erigon seg check-commitment-hist-at-blk-range`

| Flag | Default | Purpose |
|------|---------|---------|
| `--from` | (required) | Start block number |
| `--to` | (latest) | End block number (exclusive) |
| `--sample` | `1.0` | Fraction of blocks to check (use `<1.0` to sample) |
| `--seed` | (random) | Fix seed for reproducible sampling |
| `--from-step` | `0` | Skip files before given step |
| `--failFast` | `true` | Stop vs continue on first error |

### `erigon seg check-commitment-hist-at-blk`

| Flag | Default | Purpose |
|------|---------|---------|
| `--block` | (required) | Block number to verify |

---

## Performance Characteristics & Bottlenecks

### Slow checks (cost order, slowest first)

1. **`StateVerify`** — full forward+reverse correspondence check between commitment files and domain files. Can run for many hours on mainnet. Controlled by `CHECK_VERIFY_STATE_WORKERS` env var (defaults to ~AlmostAllCPUs). Over-parallelism hurts here due to MDBX contention — try `CHECK_VERIFY_STATE_WORKERS=4` if the node is also doing other work.

2. **`CommitmentHistVal`** — samples commitment history values and recomputes block state roots. The `doIntegrity` dispatcher divides `--sample` by 100 for this check internally (so `--sample=0.01` → effective `0.0001`). Very disk-intensive.

3. **`StateRootVerifyByHistory`** (`check-commitment-hist-at-blk-range`) — uses `madvise(MADV_RANDOM)` internally because it issues highly parallel random I/O. Benefits from many cores. Use `--sample` to check a fraction of the range.

4. **`CommitmentKvDeref`** (deprecated) — spawns one goroutine per commitment file; very fast per file but total can be high on many files.

5. **`CommitmentKvi`** — runs via `CheckKvis` with sampling. Fast at low sample ratios.

### Fast checks (seconds to minutes on mainnet)

`Blocks`, `HeaderNoGaps`, `BlocksTxnID`, `InvertedIndex`, `ReceiptsNoDups`, `RCacheNoDups`,
`CommitmentRoot`, `StateProgress`, `HistoryNoSystemTxs`, `Publishable`

---

## Optimization Strategies

### Make repeated runs fast — use the file-integrity cache

The cache keys files by torrent InfoHash (not just filename), so it correctly invalidates when file content changes:

```bash
./build/bin/erigon seg integrity \
  --datadir=/data/erigon \
  --file-integrity-cache=/data/erigon/integrity.cache
```

On second run unchanged files are skipped instantly. Use `--skip-torrent-verify` to skip the piece-hash step when you trust the file hasn't changed on disk.

### Adjust sampling to trade speed vs coverage

```bash
# Very fast smoke test (1% sample)
--sample=0.01

# Full coverage (100%)
--sample=1.0

# Reproducible across runs (same blocks/items checked)
--seed=12345 --sample=0.05
```

### Run only the checks you care about

```bash
# Only commitment checks
--check=CommitmentKvi,CommitmentRoot,CommitmentHistVal

# Skip the slow StateVerify during CI
--skip-check=StateVerify

# Only fast checks (skip all slow ones)
# (omit --check entirely — default is FastChecks)
```

### Isolate a suspicious block range

When a specific block range is suspect, use the targeted subcommand instead of running full integrity:

```bash
# Check a single block
./build/bin/erigon seg check-commitment-hist-at-blk \
  --datadir=/data/erigon --block=18000000

# Check a range with 10% sampling (fast but representative)
./build/bin/erigon seg check-commitment-hist-at-blk-range \
  --datadir=/data/erigon --from=17000000 --to=18000000 \
  --sample=0.1 --seed=42
```

### Tune StateVerify parallelism

```bash
# Reduce workers if MDBX contention is high (watch for lock waits in logs)
CHECK_VERIFY_STATE_WORKERS=4 ./build/bin/erigon seg integrity \
  --datadir=/data/erigon --check=StateVerify

# Max workers for dedicated integrity machine
CHECK_VERIFY_STATE_WORKERS=32 ./build/bin/erigon seg integrity \
  --datadir=/data/erigon --check=StateVerify
```

---

## Profiling & Finding Bottlenecks

To find where time is actually going:

```bash
# Enable pprof (check for port conflicts first — use /erigon-network-ports)
./build/bin/erigon seg integrity \
  --datadir=/data/erigon \
  --pprof --pprof.port=6061 \
  --check=CommitmentHistVal

# In another terminal: capture CPU profile
go tool pprof http://localhost:6061/debug/pprof/profile?seconds=30
```

Watch the logs for per-check timing — each check logs `[integrity] starting` and completion with elapsed time. If a check is slow:
- High MDBX read time → reduce workers or use `madv_random` hint (already applied to `check-commitment-hist-at-blk-range`)
- High CPU on hash → sampling is too high, reduce `--sample`
- Many files being re-checked → enable `--file-integrity-cache`

---

## Corner Cases & Bug Detection

### Empty blocks
`check-commitment-hist-at-blk-range` correctly handles empty blocks (no state changes) — it verifies the gap is truly empty and that all blocks in the gap share the same state root.

### Partial blocks at file boundaries
`CommitmentRoot` and `CommitmentHistVal` skip partial blocks at file boundaries (where txNum doesn't align with a complete block). This is expected — watch for `skipping partial block` in logs.

### Step boundary effects in StateVerify
Non-base files use a reverse check that includes the next file's commitment refs to handle accounts written near step boundaries. If you see unexpected mismatches only in the last file, this is the cause.

### rcache files
`RCacheNoDups` only runs when rcache files are actually being generated (`--persist.receipts`). On nodes without rcache files the check is silently skipped — this is correct.

### Torrent infohash mismatch on restart
If the integrity cache reports unexpected misses after a restart, the snapshot files may have been re-downloaded or re-built (infohash changed). Clear the cache and re-run.

---

## Workflow: Full Integrity Audit

```bash
# Step 1: Fast pass (minutes) — catch obvious problems
./build/bin/erigon seg integrity \
  --datadir=/data/erigon \
  --failFast=false \
  --file-integrity-cache=/data/erigon/integrity.cache

# Step 2: If Step 1 passes, targeted slow checks
./build/bin/erigon seg integrity \
  --datadir=/data/erigon \
  --check=StateVerify \
  --failFast=false

# Step 3: Commitment history spot-check on recent blocks
./build/bin/erigon seg check-commitment-hist-at-blk-range \
  --datadir=/data/erigon \
  --from=<tip-100000> \
  --sample=0.2 \
  --seed=1
```

## Workflow: Snapshot Publishing Pre-check

```bash
# 1. Run seg retire first
./build/bin/erigon seg retire --datadir=/data/erigon

# 2. Full integrity with cache (fast on second run)
./build/bin/erigon seg integrity \
  --datadir=/data/erigon \
  --failFast=false \
  --file-integrity-cache=/data/erigon/integrity.cache

# 3. Publishable check
./build/bin/erigon seg integrity \
  --datadir=/data/erigon \
  --check=Publishable
```

---

## Catching New Slow Checks Before They Degrade Speed

Developers periodically add new integrity checks. A new check may accidentally land in `FastChecks` while being slow in practice, causing sudden regressions in routine integrity run time.

### How to detect a newly added check

After any PR that touches `db/integrity/` or `integrity_action_type.go`, compare the check list against the last known baseline:

```bash
# Dump current check list
./build/bin/erigon seg integrity --help 2>&1 | grep -A5 "\-\-check"
```

Any check that appears in `FastChecks` (the default run) but wasn't there before is a candidate for timing analysis.

### How to time an individual check in isolation

Run each new check alone on a real datadir and measure wall time:

```bash
time ./build/bin/erigon seg integrity \
  --datadir=/data/erigon \
  --check=<NewCheckName> \
  --sample=1.0 \
  --failFast=false
```

**Rule of thumb for FastChecks**: a check that takes more than ~5 minutes on a full mainnet datadir should either:
- Be moved to `SlowChecks` (opt-in only), or
- Have `--sample` support added with a low default (≤0.01), or
- Support `--file-integrity-cache` so it can be skipped on unchanged files, or
- Be parallelised to bring it under the threshold

### How to identify the bottleneck inside a slow new check

1. Run with pprof enabled and capture a CPU profile:

```bash
./build/bin/erigon seg integrity \
  --datadir=/data/erigon \
  --check=<NewCheckName> \
  --pprof --pprof.port=6061 &

sleep 5 && go tool pprof -http=:8080 http://localhost:6061/debug/pprof/profile?seconds=60
```

2. Look at the flamegraph for:
   - MDBX cursor scans (`mdbx_cursor_get`, `BeginTemporalRo`) → add sampling or limit concurrency
   - Decompressor reads (`seg.Reader`, `decompress`) → add file-integrity-cache or reduce sample ratio
   - Hashing / recompute loops → verify whether full recompute is needed or a hash-comparison shortcut exists

3. Check whether the check opens one MDBX tx per file (correct) or one global tx (bad — serialises all workers). Each goroutine should call `db.BeginTemporalRo(ctx)` independently.

4. Check whether the check respects `ctx.Done()` promptly — a check that ignores context cancellation will block graceful shutdown.

### Watch for checks that overlap each other

Some checks cover partially the same ground. Overlap isn't always bad — two checks may share a domain but each catch a unique class of bug the other misses. But unintentional full overlap wastes time on large datadirs.

**Known overlapping pairs** (as of 3.4):

| Check A | Check B | Shared ground | Unique to A | Unique to B |
|---------|---------|---------------|-------------|-------------|
| `CommitmentKvi` | `CommitmentKvDeref` | commitment `.kv` files | KVI index offset correctness | branch key → domain key resolution |
| `CommitmentHistVal` | `StateRootVerifyByHistory` | commitment history values | samples random history buckets | recomputes full state root per block |
| `ReceiptsNoDups` | `RCacheNoDups` | receipt monotonicity | raw receipt snapshots | receipt cache layer |
| `CommitmentRoot` | `StateRootVerifyByHistory` | state root correctness | reads stored root from file | recomputes root from history |

**How to audit for new overlaps when a check is added:**

1. Read the new check's `Check*` function in `db/integrity/` — note which files it opens and which keys it iterates.
2. Compare against existing checks that open the same domain or file type.
3. For each pair, ask: "Can check A find a bug that B would miss, or vice versa?" If the answer is no for both directions, one of them is redundant and should be removed or merged.
4. If there is partial overlap (each catches something unique), document it in the `integrity_action_type.go` comment for that check so future developers don't remove it thinking it's a pure duplicate.

**Practical heuristic:** if two checks scan the same `.kv` or `.v` file with the same iteration order and no different validation logic, they are likely duplicates. Run them individually on the same datadir and compare wall times — a suspiciously similar runtime is a hint.

### Checklist for a new integrity check PR

- [ ] Check is placed in the correct tier (`FastChecks` vs `SlowChecks`) based on measured runtime
- [ ] Check supports `--sample` / `SamplerCfg` if it scans many items
- [ ] Check respects `--failFast` flag
- [ ] Check respects `--fromStep` / `--from-step` to skip old files
- [ ] Check is compatible with `--file-integrity-cache` if it reads file content (fingerprint files and call `cache.has` / `cache.add`)
- [ ] Check logs `[integrity] <CheckName> starting` and per-file progress at `Info` level
- [ ] Check uses a separate MDBX tx per goroutine (no shared tx across workers)
- [ ] Check returns promptly when `ctx` is cancelled
- [ ] Measured wall time on mainnet documented in the PR

---

## Interpreting Output

- `[integrity] skipping (cache hit)` — file was already verified and is unchanged; safe to trust
- `[integrity] CommitmentRoot skipping partial block` — expected at file boundaries
- `[integrity] CommitmentHistVal skipping partial block` — expected at file boundaries
- `[integrity] CommitmentKvDeref skipping plain storage/account` — expected for keys not referenced by commitment branches
- `WRN [integrity]` with `err=...` when `--failFast=false` — non-fatal issues; collect and investigate
- `ERR [integrity]` — fatal; check stopped

When a check fails, increase verbosity with `--log.lvl=debug` and narrow the scope with `--check=<failing_check>` and `--failFast=false` to collect all instances of the problem.
