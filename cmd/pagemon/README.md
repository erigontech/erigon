# pagemon

Page-cache working-set analyser for database files.

Uses `mincore(2)` to measure which pages of a file are resident in the OS page
cache before and after a query runs. Clusters the hot regions, classifies the
access pattern, and prints a plain-text report you can feed to the
`/data-colocation-advisor` Claude Code skill.

**Platforms:** Linux, macOS (Darwin). Requires no root for observational mode.
Root is needed only for `--drop` cache (Linux only).

## Build

```bash
make pagemon          # output: ./build/bin/pagemon
# or
go run ./cmd/pagemon
```

## Commands

### `snapshot` — current page-cache state

```bash
pagemon snapshot <file>...
```

Shows which pages are resident right now. Useful to check how warm a file is
before running a query.

### `measure` — before/after delta

Single before/after snapshot pair. No `--interval` flag.

```bash
# Launch a command — drops page cache first for a clean baseline (Linux, root):
pagemon measure --cmd "erigon-tool dump-state --block 21000000" /data/state.kv

# Skip cache drop (production safe, macOS, or no root):
pagemon measure --cmd "..." --no-drop /data/state.kv

# Attach to an already-running process (cache drop skipped automatically):
pagemon measure --pid 12345 /data/state.kv

# Show read amplification inline — pass the bytes the query logically needs:
pagemon measure --cmd "..." --logical-bytes 204800 /data/state.kv
```

### `watch` — temporal sampling

Same as `measure` but polls mincore every `--interval` and adds a temporal
phase breakdown showing which clusters lit up at which point in time.
`--interval` is a `watch`-only flag — `measure` does not have it.

```bash
# Launch a command, sample every 50ms (default):
pagemon watch --cmd "..." /data/state.kv
pagemon watch --cmd "..." --interval 100ms /data/state.kv

# Attach to a running process (Ctrl-C to stop):
pagemon watch --pid $(pgrep erigon) /data/state.kv
pagemon watch --pid $(pgrep erigon) --interval 100ms /data/a.kv /data/b.kv

# Show read amplification inline:
pagemon watch --pid $(pgrep erigon) --logical-bytes 204800 /data/state.kv
```

Prints a live status line to stderr every 5 s while sampling:

```
  [10s] state.kv: +4,200 pages new (+16.8 MB loaded)
```

### `diff` — compare two snapshots *(Phase 2, not yet implemented)*

```bash
pagemon snapshot --out before.snap /data/state.kv
# ... apply a change ...
pagemon snapshot --out after.snap /data/state.kv
pagemon diff before.snap after.snap
```

## Output format

```
=== pagemon measurement ===
Command:  <cmd or pid N>
Duration: 2847ms

=== File: /path/to/db ===
Size:    4.2GB (1,048,576 pages)
Loaded:  48.6MB (12,450 pages)
Read amplification: 12.4×  (48.6 MB loaded / 3.9 MB needed)   ← only with --logical-bytes
Density: 1.3%   Scatter: avg 78 pages   Max gap: 45,000 pages

Clusters (4):
  1: pages 1,024–1,156     (528.0KB)
  2: pages 204,800–205,120 (1.3MB)
  3: pages 512,000–512,450 (1.8MB)
  4: pages 998,000–998,102 (408.0KB)
Inter-cluster gaps: 200.0MB, 1.2GB, 1.9GB

Temporal phases (4):
  0s–200ms:    cluster 1
  200ms–900ms: cluster 2
  ...

Pattern: INDEX_LOOKUP_SCATTERED
```

## Pattern tags

| Tag | Meaning |
|-----|---------|
| `SEQUENTIAL` | Monotonically increasing load, high density — likely a table/file scan |
| `INDEX_LOOKUP_SCATTERED` | A few tight clusters loaded in bursts — index + heap random reads |
| `HOT_COLD_MIXED` | Medium density with clear cold gaps — partially partitioned data |
| `RANDOM_SCATTERED` | Many tiny loads spread across the full file span |
| `BURSTY` | Distinct temporal phases separated by idle gaps |
| `UNKNOWN` | Doesn't match a template — inspect metrics directly |

## Read amplification

Pass `--logical-bytes N` (bytes the query logically needs) to `measure` or `watch` and the
report will compute it inline:

```
Read amplification: 12.4×  (48.6 MB loaded / 3.9 MB needed)
```

Derive `N` from: rows returned × avg row size, key count × avg value size, or an `EXPLAIN` estimate.

Thresholds:

- < 2× — good
- 2–3× — acceptable
- \> 3× — significant layout opportunity
- \> 10× — critical

## Claude Code skill

The `/data-colocation-advisor` skill runs `pagemon measure` for you, reads the
report, estimates read amplification, and recommends layout changes (covering
indexes, vertical partitioning, physical reorganisation, co-location).

---

## Feature log

### Shipped

| Feature | Origin |
|---------|--------|
| `snapshot` subcommand — point-in-time mincore residency | design |
| `measure --cmd` — drop caches, run command, report delta | design |
| `measure --no-drop` — skip drop_caches for production / no-root use | design |
| `watch --cmd` — temporal mincore sampling every `--interval` | design |
| Cluster detection — groups loaded pages separated by < 32-page gaps | design |
| Inter-cluster gap reporting | design |
| Temporal phase breakdown in `watch` output | design |
| Pattern classification: SEQUENTIAL / INDEX_LOOKUP_SCATTERED / HOT_COLD_MIXED / RANDOM_SCATTERED / BURSTY / UNKNOWN | design |
| Huge-file sampling mode — strides every 8 pages for files > 50GB | design |
| Non-Linux build stub — binary compiles on any platform, fails clearly at runtime | implementation |
| `--pid` on `watch` — attach to a running process instead of launching a command | requested |
| `--pid` on `measure` — before/after delta against a running process | requested |
| macOS (Darwin) support — `SYS_MINCORE=78` available on darwin/amd64 and arm64 | requested |
| Live status line every 5s during `watch` — shows new pages loaded so far | requested |
| `diff` subcommand stub (Phase 2 placeholder) | implementation |
| `/data-colocation-advisor` Claude Code skill | design |
| `--logical-bytes` on `measure`/`watch` — inline read-amplification line in report | requested |

### Planned (not yet implemented)

| Feature | Notes |
|---------|-------|
| Terminal heatmap (`--heatmap` flag) | Unicode block chars, ANSI colours; auto-on when stdout is a tty |
| `diff <before> <after>` | Requires `--out <file>` on snapshot/measure to save residency state; shows side-by-side heatmap and delta metrics |
| Temporal heatmap replay in `watch` | One heatmap frame per phase, top-to-bottom scan shows evolution |
| eBPF access-frequency layer | mincore shows residency, not frequency; eBPF would add per-page heat counts |
| O_DIRECT / io_uring bypass detection | Files accessed with O_DIRECT bypass the page cache and are invisible to mincore |
| Windows support | mincore has no equivalent; would need `QueryWorkingSetEx` |
