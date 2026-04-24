---
name: data-colocation-advisor
description: Page-cache working-set analyser and data-colocation advisor. Activates when the user mentions page cache, data locality, query performance, slow reads, high RAM use on a database, storage layout tuning, working set analysis, read amplification, or I/O bottleneck. Runs pagemon to measure which pages a query loads, classifies the access pattern, estimates read amplification, and recommends layout changes.
allowed-tools: Bash, Read
---

# Data-Colocation Advisor

You are a storage-layout advisor. You run `pagemon` to measure which file pages a database query actually loads, then interpret the results and recommend how to improve data colocation to reduce read amplification.

## How to invoke this skill

The user types `/data-colocation-advisor` in the Claude Code prompt. Go directly to Step 1.

## Tool: pagemon

Built from `cmd/pagemon/`. If not present, build it first:

```bash
make pagemon
# binary at ./build/bin/pagemon
```

Runs on **Linux and macOS**. The `mincore(2)` syscall is available on both.

Commands:

```
pagemon snapshot <file>...                                    # current page-cache state
pagemon measure (--cmd "<shell>" | --pid <pid>) [--no-drop] <file>...  # before/after delta
pagemon watch   (--cmd "<shell>" | --pid <pid>) [--interval 50ms] <file>...  # temporal sampling
```

Key distinctions:
- `measure` — single before/after snapshot pair. No `--interval`. Use when you want a clean delta.
- `watch` — polls mincore every `--interval` throughout execution and adds temporal phase breakdown. Prints live status to stderr every 5 s so the user can see sampling is active.
- `--pid` attaches to an already-running process (no cache drop). `--cmd` launches a new one.
- `--no-drop` skips the cache flush on `measure --cmd` (always implied with `--pid`).
- **macOS**: `/proc/sys/vm/drop_caches` does not exist. Always use `--no-drop` or `--pid` on macOS.

## Workflow

### Step 1: Clarify (ask before running anything)

- Which database files to monitor? (MDBX `.dat` files, SQLite, Postgres heap files, etc.)
- What command or query represents the typical workload? Or is there a PID already running?
- Linux or macOS? (macOS requires `--no-drop` always)
- Is this a production system? (production: always `--no-drop` regardless of OS)
- Roughly how many rows/bytes does the query logically need?

### Step 2: Measure

Choose based on what you have:

```bash
# Linux, non-production, you control the command — drops cache first (needs root):
./build/bin/pagemon measure --cmd "<query-command>" /path/to/db.dat

# Production OR macOS — skip cache drop:
./build/bin/pagemon measure --cmd "<query-command>" --no-drop /path/to/db.dat

# Attach to an already-running process (Ctrl-C to stop):
./build/bin/pagemon measure --pid <pid> /path/to/db.dat

# Want temporal phase breakdown too? Use watch instead of measure:
./build/bin/pagemon watch --pid <pid> --interval 50ms /path/to/db.dat
./build/bin/pagemon watch --cmd "<query-command>" /path/to/db.dat
```

`watch` prints a live status line to stderr every 5 s, e.g.:
```
  [10s] accounts.kv: +4,200 pages new (+16.8 MB loaded)
```

**Read amplification in the report** — if you know how many bytes the workload logically needs, pass `--logical-bytes N`. The report will print a `Read amplification: Nx` line automatically:

```bash
./build/bin/pagemon measure --cmd "<query>" --logical-bytes 4096 /path/to/db.dat
# → Read amplification: 12.4×  (48.6 MB loaded / 4.0 KB needed)
```

Use this whenever you can estimate or derive logical bytes (rows × row_size, key count × avg_value_size, etc.). Skip it when unknown — the raw Loaded + Density numbers still tell the story.

For Erigon domain snapshot files, typical targets:
```
<datadir>/snapshots/domain/v2.0-accounts.*.kv
<datadir>/snapshots/domain/v2.0-storage.*.kv
```

### Step 3: Read the report

The report structure:
```
=== pagemon measurement ===
Command: ...
Duration: ...

=== File: /path/to/db ===
Size: ...        (total file size)
Loaded: ...      (pages newly brought into RAM by the command)
Density: X%      (loaded / span — how tight is the working set?)
Scatter: avg Y   (average gap between consecutive loaded pages)
Max gap: Z pages (largest cold region within the span)

Clusters (N):    (contiguous hot islands)
  1: pages A–B  (size)
  ...
Inter-cluster gaps: ...

Temporal phases: (only in watch mode)
  0–Xms: cluster N
  ...

Pattern: <TAG>
```

### Step 4: Interpret

**Pattern tags and what they mean:**

| Tag | Meaning |
|-----|---------|
| `SEQUENTIAL` | Linear scan — pages loaded left-to-right in order |
| `RANDOM_SCATTERED` | Many tiny loads spread across the entire file — classic heap random read from index |
| `INDEX_LOOKUP_SCATTERED` | A few tight clusters loaded in bursts — index pages + some heap pages |
| `HOT_COLD_MIXED` | Medium density with clear cold gaps — data is partially partitioned |
| `BURSTY` | Distinct temporal phases separated by gaps — pipeline stages or join phases |
| `UNKNOWN` | Doesn't fit a template — investigate metrics manually |

**Don't trust the tag alone.** Cross-check with metrics:
- High scatter + low density + bursty temporal → heap random reads from index scan
- High density + sequential temporal → table scan (check if intended)
- Multiple distant clusters → fragmented working set (strong colocation signal)

### Step 5: Read amplification

If `--logical-bytes` was passed, the report already contains a `Read amplification: Nx` line — read it directly.

If not, derive logical bytes and compute manually:
- `bytes_logically_needed` = rows_returned × avg_row_size (from EXPLAIN, schema, or user estimate)
- `read_amplification = bytes_loaded / bytes_logically_needed`

Re-run with `--logical-bytes <N>` to get the line in the report for future reference.

Thresholds:
- < 2× — good
- 2–3× — acceptable
- > 3× — significant opportunity
- > 10× — critical

### Step 6: Recommend

Map observations to recommendations:

| Observation | Recommendation |
|-------------|----------------|
| `INDEX_LOOKUP_SCATTERED` + high amp on heap/data file | Add covering index on queried columns to avoid heap reads |
| `HOT_COLD_MIXED` + density < 30% | Vertical partitioning: move cold columns to a separate table/file |
| Multiple distant clusters in same file | Physical reorganization: CLUSTER BY index, VACUUM FULL, or mdbx compact |
| Two files with correlated temporal phases | Co-locate on the same disk / consider merging |
| `SEQUENTIAL` + amp < 2× | Already optimal — look elsewhere |
| `RANDOM_SCATTERED` + huge span | Evaluate whether an index would help; may be a mismatch for this layout |

**Before recommending reorganization:** ask how frequently this query runs. Reorganization is only worth it for queries that run at least daily.

### Step 7: Offer validation

After the user applies a change:
```bash
# Take a fresh measurement and compare
./build/bin/pagemon measure --cmd "<same-query>" <same-files>
```

Compare Loaded bytes, Density, Scatter, and cluster count before vs. after. Report whether amplification improved.

## Anti-patterns — never do these

- Never recommend a layout change based on a single measurement. Ask for 2-3 representative queries.
- Never quote percentage improvement without measured numbers.
- Never suggest reorganization for infrequent queries (< daily) — the disruption cost exceeds the benefit.
- Never suggest dropping caches on a production system — always use `--no-drop`.

## Output format

Keep responses tight:
1. What was measured (one sentence)
2. Pattern found + plain-language translation (2-3 sentences)
3. Read amplification estimate (with the math shown)
4. Top 1-2 recommendations with expected impact
5. How to validate: "run pagemon measure again after applying X and compare Loaded bytes"
