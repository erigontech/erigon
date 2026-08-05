# PrefixIndex: MatchCmp Integration + Lint Fixes + Benchmarks

## Overview
- PrefixIndex on branch `awskii/prefix-index-standalone` doesn't compile — it references an undefined `keyCmpFunc` type and nonexistent `BtIndex.keyCmp` method
- Upstream commit `d8471e0655` replaced the old keyCmp callback pattern with direct `g.MatchCmp(key)` calls (zero-copy, 0 allocs, 24ns miss vs 58ns+1alloc before)
- This plan integrates PrefixIndex with the MatchCmp API, fixes all lint errors, and runs benchmarks

## Context (from discovery)
- Files involved:
  - `db/datastruct/btindex/prefix_index.go` — main implementation (broken `keyCmpFunc` references)
  - `db/datastruct/btindex/btree_index.go` — integration point (`idx.keyCmp` calls on lines 513, 524)
  - `db/datastruct/btindex/prefix_index_test.go` — tests passing nonexistent `ir.keyCmp`
  - `db/datastruct/btindex/bpstree_bench_test.go` — benchmarks with stale constructor signatures
  - `db/datastruct/btindex/bps_tree.go` — reference implementation of `compareKey` using MatchCmp
- BpsTree.compareKey pattern (the target): `g.Reset(b.offt.Get(di)); return g.MatchCmp(key)`
- MatchCmp semantics: returns 0 on match (advances g past key), <0 or >0 on mismatch (resets g)
- Current compile errors: 5 (3 undefined keyCmpFunc, 2 undefined idx.keyCmp)

## Development Approach
- **Testing approach**: Regular (code first, then verify existing tests pass)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: all tests must pass before starting next task**
- **CRITICAL: update this plan file when scope changes during implementation**
- Run `make lint` multiple times (non-deterministic) until clean

## Testing Strategy
- **Unit tests**: Existing tests in `prefix_index_test.go` cover Seek, Get, node distribution, and BpsTree-vs-PrefixIndex comparison — these must all pass after changes
- **Benchmarks**: Run comparison benchmarks to validate zero-copy MatchCmp integration delivers expected performance

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix

## Solution Overview
- **Drop `keyCmpFunc` indirection entirely** — follow BpsTree's pattern of calling `g.MatchCmp()` directly
- Add `compareKey` method to PrefixIndex mirroring `BpsTree.compareKey`
- Replace 3 call sites in Seek/Get with direct `compareKey` calls
- Remove error-handling blocks (compareKey panics on bounds, matching BpsTree convention)
- Remove `g.Buf`/`cur.key` updates from comparison calls (MatchCmp is zero-copy)
- Net effect: delete code, add one 5-line method

## Technical Details
- `compareKey(g *seg.Reader, key []byte, di uint64) int` — resets reader to offset of item `di`, calls `g.MatchCmp(key)`
- On match (cmp==0): g is advanced past key, ready to read value with `g.Next(nil)`
- On mismatch: g position is reset by MatchCmp internally
- Panics if `di >= p.offt.Count()` (same as BpsTree — bounds are programming errors)
- Sequential scan paths (`r-l <= DefaultBtreeStartSkip`) keep using `g.Next()` — no change needed

## Implementation Steps

### Task 1: Add compareKey method and remove keyCmpFunc

**Files:**
- Modify: `db/datastruct/btindex/prefix_index.go`

- [x] Remove `keyCmpFunc` field from `PrefixIndex` struct (line 49)
- [x] Remove `keyCmp keyCmpFunc` parameter from `NewPrefixIndex` constructor (line 201) and its assignment (line 205)
- [x] Remove `keyCmp keyCmpFunc` parameter from `NewPrefixIndexWithNodes` constructor (line 281) and its assignment (line 285)
- [x] Add `compareKey(g *seg.Reader, key []byte, di uint64) int` method mirroring BpsTree.compareKey
- [x] Rewrite Seek binary search: replace `keyCmpFunc` with `compareKey`, fix comparison direction (MatchCmp returns Compare(key, fileKey) — `cmp < 0` → `r = m`)
- [x] Rewrite Get binary search loop: same fix
- [x] Rewrite Get post-loop check: same fix
- [x] `errors` import still needed (Seek uses errors.Is)

### Task 2: Fix BtIndex integration

**Files:**
- Modify: `db/datastruct/btindex/btree_index.go`

- [x] Fix line 513: remove `idx.keyCmp` argument from `NewPrefixIndex(kvGetter, idx.ef, idx.dataLookup, idx.keyCmp)` → `NewPrefixIndex(kvGetter, idx.ef, idx.dataLookup)`
- [x] Fix line 524: remove `idx.keyCmp` argument from `NewPrefixIndexWithNodes(...)` → `NewPrefixIndexWithNodes(kvGetter, idx.ef, idx.dataLookup, nodes)`

### Task 3: Fix test and benchmark files

**Files:**
- Modify: `db/datastruct/btindex/prefix_index_test.go`
- Modify: `db/datastruct/btindex/bpstree_bench_test.go`

- [x] Fix all `NewPrefixIndex(g, efi, ir.dataLookup, ir.keyCmp)` calls → `NewPrefixIndex(g, efi, ir.dataLookup)` in prefix_index_test.go
- [x] Fix all `NewPrefixIndexWithNodes(..., ir.keyCmp, ...)` calls → remove `ir.keyCmp` parameter in prefix_index_test.go
- [x] Fix `NewBpsTree(g2, efi, DefaultBtreeM, ir2.dataLookup, ir2.keyCmp)` → `NewBpsTree(g2, efi, DefaultBtreeM, ir2.dataLookup)` in prefix_index_test.go line 1949
- [x] No stale calls in bpstree_bench_test.go
- [x] Verify package compiles: `go build ./db/datastruct/btindex/`
- [x] Run unit tests: `go test ./db/datastruct/btindex/ -short -count=1` — all pass

### Task 4: Lint until clean

**Files:**
- Modify: any files flagged by linter

- [x] Run `make lint` — 0 issues
- [x] Run `make lint` again — 0 issues
- [x] Run `make lint` a third time — 0 issues
- [x] Verify build: `make erigon integration` — success

### Task 5: Run benchmarks and report

- [x] Run BenchmarkSeekComparison
- [x] Run BenchmarkGetComparison
- [x] Run BenchmarkPrefixIndex (Seek + Get)
- [x] Results reported below

#### Benchmark Results (1M keys, CompressKeys, DO-Premium-AMD 4-core)

| Benchmark | BpsTree (ns/op) | PrefixIndex (ns/op) | Speedup | Allocs |
|-----------|-----------------|---------------------|---------|--------|
| Seek      | 2278            | 1324                | **1.72x** | 0/0  |
| Get       | 2144            | 1218                | **1.76x** | 0/0  |

Standalone PrefixIndex benchmarks:
- PrefixIndexSeek: ~1277 ns/op, 0 allocs
- PrefixIndexGet:  ~1292 ns/op, 0 allocs

## Post-Completion

**Manual verification:**
- Run full test suite with `make test-short` to catch any cross-package regressions
- Consider running with real .kv data files if available for production-scale validation
