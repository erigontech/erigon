# Spec: Standalone PrefixIndex

## Overview

Extract the prefix index from `bps_tree.go` into a standalone `PrefixIndex` type in `prefix_index.go`. PrefixIndex replaces `BpsTree` as the search engine inside `BtIndex`. It builds per-prefix evenly-distributed cached nodes at open time from the `.kv` file, providing consistent narrowing across all key prefixes.

**Key difference from BpsTree**: No two-path architecture. PrefixIndex always uses prefix buckets + per-bucket node binary search, regardless of file size. For files below the prefix threshold, it degrades gracefully (fewer buckets, more nodes per bucket).

## Architecture

```
Before:
  BtIndex
    └── BpsTree
          ├── prefix *prefixIndex  (large files, >=60k keys)
          │     ├── buckets[65536]  (DI ranges)
          │     └── nodes in buckets (from global M-th sampling)
          └── mx []Node            (small files, <60k keys)
              └── bs() binary search

After:
  BtIndex
    └── PrefixIndex
          ├── buckets [65536]prefixBucket  (DI ranges + up to 8 even-spaced nodes each)
          ├── l1First [256]uint64          (aggregated from buckets)
          ├── l1End   [256]uint64          (aggregated from buckets)
          └── Seek() / Get()              (single code path)
```

## Data Structures

### prefixBucket (unchanged from current)

```go
type prefixBucket struct {
    firstDI uint64   // first key DI with this 2-byte prefix (MaxUint64 = empty)
    endDI   uint64   // DI past last key (exclusive upper bound)
    nodes   []Node   // up to 8 cached keys for binary search within bucket
}
```

### PrefixIndex (new standalone type)

```go
type PrefixIndex struct {
    buckets  [65536]prefixBucket
    l1First  [256]uint64    // min(firstDI) across buckets[b0<<8..b0<<8|0xFF]
    l1End    [256]uint64    // max(endDI) across buckets[b0<<8..b0<<8|0xFF]

    offt     *eliasfano32.EliasFano  // offset index (from .bt file)

    dataLookupFunc dataLookupFunc
    keyCmpFunc     keyCmpFunc
    cursorGetter   cursorGetter

    trace bool
}
```

### Node (reuse existing)

```go
type Node struct {
    key []byte
    off uint64  // offset in kv file
    di  uint64  // key ordinal in kv
}
```

## Building Strategy

### Phase 1: Sequential Scan (one pass over .kv)

Scan all keys sequentially, recording per-prefix metadata:

```go
type bucketBuilder struct {
    firstDI uint64
    endDI   uint64
    count   uint64  // total keys in this prefix (for computing even spacing)
}
```

For each key at position `di`:
1. Compute 2-byte prefix
2. Update `firstDI` (min), `endDI` (di+1), increment `count`

Cost: one sequential pass over `.kv`. Same as current WarmUp for large files.

### Phase 2: Node Selection (random access from .kv)

For each non-empty prefix bucket:
1. Compute 8 evenly-spaced DI positions within `[firstDI, endDI)`:
   - If `count <= 8`: select ALL positions (every key is a node)
   - If `count > 8`: select at positions `firstDI + i*(count-1)/7` for `i=0..7`
2. Read keys at those DI positions using EliasFano offsets + `seg.Reader`
3. Store as `nodes` in the bucket

```
Example: prefix 0x02ab has keys at DI 531250..532049 (800 keys)
  Spacing: 800/8 = 100
  Selected DIs: 531250, 531364, 531478, 531592, 531706, 531821, 531935, 532049
  Each node.key read from .kv via ef.Get(di) → offset → reader.Reset(offset) → reader.Next()
```

Cost: up to `8 * active_prefixes` random reads. For Ethereum mainnet with ~50k active prefixes, that's ~400k reads — fast since the file is memory-mapped.

### Phase 3: L1 Aggregation

Same as current `computeL1()`: for each first byte `b0`, aggregate `min(firstDI)` and `max(endDI)` across `buckets[b0<<8..b0<<8|0xFF]`.

### Alternative: Pre-built Nodes from .bt File

When opening a `.bt` file that already has serialized nodes (current format), PrefixIndex can optionally use those nodes instead of scanning `.kv`:
1. Read nodes from `.bt` file
2. Distribute into prefix buckets (same as current `NewBpsTreeWithNodes`)
3. But still scan `.kv` for bucket ranges (firstDI, endDI)

This provides backward compatibility and avoids the Phase 2 random reads. The scan for bucket ranges (Phase 1) is still needed.

## Lookup Algorithm

### lookup(key) → (l, r uint64)

Same as current `prefixIndex.lookup`:
1. `len(key) < 1`: return `(0, totalCount)` — full range
2. `len(key) == 1`: use L1 tables
3. `len(key) >= 2`: use exact bucket, fallback to L1

### narrowWithNodes(key) → (l, r, exactDI, found)

Same as current `prefixIndex.narrowWithNodes`:
1. Get bucket for key's 2-byte prefix
2. Binary search over bucket's cached nodes
3. Exact match → return `(0, 0, exactDI, true)`
4. No match → return narrowed `[l, r)` from node boundaries

### Seek(g *seg.Reader, seekKey []byte) → (*Cursor, error)

```
1. Empty key → return first key
2. lookup(seekKey) → [l, r)
   - If (0, 0) → key prefix doesn't exist, scan forward? TBD
3. narrowWithNodes(seekKey) → refine [l, r), check exact match
4. Disk binary search in [l, r):
   - While r - l > DefaultBtreeStartSkip:
     - m = (l+r)/2, compare key at m with seekKey
     - Narrow l or r
   - When r - l <= DefaultBtreeStartSkip: sequential scan
5. Return cursor at found position
```

### Get(g *seg.Reader, key []byte) → (v []byte, ok bool, offset uint64, err error)

Same flow as Seek but returns value on exact match, nil on miss.

## Context (key code locations)

| What | Where | Lines |
|------|-------|-------|
| Current `prefixIndex` + `prefixBucket` | `bps_tree.go` | 38-181 |
| `BpsTree.Seek()` with prefix path | `bps_tree.go` | 419-517 |
| `BpsTree.Get()` with prefix path | `bps_tree.go` | 522-628 |
| `BpsTree.WarmUp()` (prefix build) | `bps_tree.go` | 317-384 |
| `NewBpsTreeWithNodes()` (prefix build from nodes) | `bps_tree.go` | 198-240 |
| `BtIndex` struct (file wrapper) | `btree_index.go` | 346-356 |
| `BtIndex.bplus` field (BpsTree reference) | `btree_index.go` | 351 |
| `OpenBtreeIndexWithDecompressor` | `btree_index.go` | 457-524 |
| `BtIndexWriter.Build()` (node serialization) | `btree_index.go` | 260-316 |
| `Node` type + encode/decode | `bps_tree.go` | 261-315 |
| `Cursor` type | `btree_index.go` | 59-170 |
| `FilesItem.bindex` (domain integration) | `dirty_files.go` | 58 |
| `DomainRoTx.getLatestFromFile` (caller) | `domain.go` | 558-565 |
| Test helper `generateKV` | `testhelpers_test.go` | 64-126 |
| Existing prefix index test | `btree_index_test.go` | 746-845 |
| Benchmarks | `bpstree_bench_test.go` | 1-156 |

## Development Approach

- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: all tests must pass before starting next task**
- Run `make lint` after changes (run repeatedly — linter is non-deterministic)

## Implementation Tasks

### Task 1: Create PrefixIndex type and builder

**Files:**
- Create: `db/datastruct/btindex/prefix_index.go`

Move and adapt from `bps_tree.go`:
- [ ] Move `prefixBucket` struct (lines 42-46) — keep unchanged
- [ ] Move `maxNodesPerBucket` constant (line 39) — keep as 8
- [ ] Create `PrefixIndex` struct with fields: `buckets [65536]prefixBucket`, `l1First [256]uint64`, `l1End [256]uint64`, `offt *eliasfano32.EliasFano`, `dataLookupFunc`, `keyCmpFunc`, `cursorGetter`, `trace bool`
- [ ] Move `record()` method — keep unchanged
- [ ] Move `computeL1()` method — keep unchanged
- [ ] Move `lookup()` method — keep unchanged
- [ ] Move `narrowWithNodes()` method — keep unchanged
- [ ] Add `addNode()` method — keep unchanged
- [ ] Add constructor `NewPrefixIndex(kv *seg.Reader, offt *eliasfano32.EliasFano, dataLookup dataLookupFunc, keyCmp keyCmpFunc) *PrefixIndex` that:
  1. Allocates PrefixIndex with sentinel values
  2. Sequential scan: iterate all keys via `kv`, call `record(key, di)` for each
  3. Node selection: for each non-empty bucket, compute 8 evenly-spaced DI positions within `[firstDI, endDI)`, read keys at those positions via `offt.Get(di)` + `kv.Reset(off)` + `kv.Next()`, store as nodes
  4. Call `computeL1()`
- [ ] Add constructor `NewPrefixIndexWithNodes(kv *seg.Reader, offt *eliasfano32.EliasFano, dataLookup dataLookupFunc, keyCmp keyCmpFunc, nodes []Node) *PrefixIndex` that:
  1. Allocates PrefixIndex with sentinel values
  2. Sequential scan: iterate all keys via `kv`, call `record(key, di)` for each
  3. Distribute pre-built nodes into prefix buckets (same as current `NewBpsTreeWithNodes` logic)
  4. Call `computeL1()`
  - This provides backward compatibility with existing `.bt` files that have serialized nodes
- [ ] Add `Close()` method — nil out buckets, offt
- [ ] Verify builds: `go build ./db/datastruct/btindex/...`

**Design note on node selection pass:**

After the sequential scan, we know each bucket's `[firstDI, endDI)` range and could compute the count as `endDI - firstDI`. However, this is only correct if all DIs in that range belong to this prefix — which they do NOT (DIs are globally sequential, not per-prefix). So we need to count keys per prefix during the scan.

Add a temporary `counts [65536]uint32` array during construction (256KB, stack-friendly). During sequential scan, increment `counts[prefix]++` for each key. After scan, use `counts[prefix]` for even spacing calculation. The counts array is discarded after construction.

To select the actual DI positions for evenly-spaced nodes, we need to know which DIs belong to each prefix. Options:

**Option A (two-pass):** First pass counts per prefix. Second pass, knowing counts, selects the i-th key within each prefix at the right spacing. This avoids storing all DIs per prefix but requires two sequential scans.

**Option B (collect DIs):** During single pass, collect DI values per prefix into temporary slices. After scan, pick evenly-spaced DIs from each slice. Memory: for 12M keys, average ~183 DIs per active prefix — manageable if we cap collection or use a pool.

**Option C (approximate):** Use `(endDI - firstDI)` as approximate count and compute approximate spacing. Some buckets share DI ranges with other prefixes, so spacing won't be perfectly even, but it's close enough and requires only one pass + no extra memory.

Recommend **Option A** (two sequential passes) for correctness without extra memory. Sequential `.kv` reads are fast (memory-mapped, read-ahead friendly). The second pass only needs to read keys for nodes (~8 per active prefix), skipping the rest.

### Task 2: Add Seek and Get methods to PrefixIndex

**Files:**
- Modify: `db/datastruct/btindex/prefix_index.go`

Port search logic from `BpsTree.Seek()` and `BpsTree.Get()`, simplified to single path:

- [ ] Implement `(p *PrefixIndex) Seek(g *seg.Reader, seekKey []byte) (*Cursor, error)`:
  1. Empty key + count > 0 → cursor at position 0
  2. `lookup(seekKey)` → `[l, r)` — if `(0, 0)` fall through to r = count (key might still exist past any prefix)
  3. `narrowWithNodes(seekKey)` → refine `[l, r)`, check exact match shortcut
  4. Disk binary search (same logic as BpsTree lines 470-506):
     - While `r - l > DefaultBtreeStartSkip`: binary search with `keyCmpFunc`
     - When small range: sequential scan
  5. Return cursor

- [ ] Implement `(p *PrefixIndex) Get(g *seg.Reader, key []byte) (v []byte, ok bool, offset uint64, err error)`:
  1. Empty key + count > 0 → return first value
  2. `lookup(key)` → `[l, r)` — if `(0, 0)` return not found
  3. `narrowWithNodes(key)` → refine, check exact match
  4. Disk binary search (same logic as BpsTree lines 577-617):
     - While `r - l > DefaultBtreeStartSkip`: binary search
     - When small range: sequential scan for exact match
  5. Final check at position l

- [ ] Add `(p *PrefixIndex) Offsets() *eliasfano32.EliasFano`
- [ ] Add `(p *PrefixIndex) Distances() (map[int]int, error)`
- [ ] Verify builds: `go build ./db/datastruct/btindex/...`

### Task 3: Wire PrefixIndex into BtIndex

**Files:**
- Modify: `db/datastruct/btindex/btree_index.go`

- [ ] Add `search *PrefixIndex` field to `BtIndex` struct (alongside `bplus` initially)
- [ ] In `OpenBtreeIndexWithDecompressor`: after reading EliasFano and nodes, create `PrefixIndex` via `NewPrefixIndexWithNodes()` instead of (or alongside) `NewBpsTreeWithNodes()`. Set `idx.search = prefixIndex`, set `cursorGetter`
- [ ] Update `BtIndex.Get()` to delegate to `b.search.Get()` when `b.search != nil` (fall back to `b.bplus` otherwise)
- [ ] Update `BtIndex.Seek()` to delegate to `b.search.Seek()` when `b.search != nil`
- [ ] Update `BtIndex.Offsets()` and `BtIndex.Distances()` to use `b.search` when available
- [ ] Update `BtIndex.Close()` to close `b.search`
- [ ] Verify: `go test ./db/datastruct/btindex/... -count=1`
- [ ] Verify: `make lint`

### Task 4: Unit tests for PrefixIndex

**Files:**
- Create: `db/datastruct/btindex/prefix_index_test.go`

- [ ] `TestPrefixIndexBuild`: Create .kv file with `generateKV`, build PrefixIndex from it, verify:
  - Non-empty buckets have `firstDI < endDI`
  - Nodes are within their bucket's DI range
  - Nodes are sorted by key within each bucket
  - L1 entries aggregate correctly
  - Active prefixes have <=8 nodes each
- [ ] `TestPrefixIndexNodeDistribution`: Verify even distribution — create skewed .kv (many keys with prefix 0x0000, few with 0xff00), verify both prefixes get nodes proportional to their count
- [ ] `TestPrefixIndexSeek`: Full seek correctness — seek every key, verify cursor returns correct key
- [ ] `TestPrefixIndexSeekBeyond`: Seek with key larger than all entries → nil cursor
- [ ] `TestPrefixIndexGet`: Full get correctness — get every key, verify value
- [ ] `TestPrefixIndexGetMissing`: Get non-existent key → not found
- [ ] `TestPrefixIndexSmallFile`: File with <60k keys — verify PrefixIndex still works (single code path)
- [ ] `TestPrefixIndexBackwardCompat`: Build with `NewPrefixIndexWithNodes` (pre-built nodes from .bt) — verify same results as BpsTree for Seek/Get on same data
- [ ] Verify: `go test ./db/datastruct/btindex/... -count=1 -v`

### Task 5: Benchmarks

**Files:**
- Modify: `db/datastruct/btindex/bpstree_bench_test.go`

- [ ] Add `BenchmarkPrefixIndexSeek` — same setup as `BenchmarkBpsTreeSeek` (12M keys, compressed), uses PrefixIndex path
- [ ] Add `BenchmarkPrefixIndexGet` — same setup, tests Get
- [ ] Add comparison sub-benchmarks: `PrefixIndex` vs `BpsTree` on same data
- [ ] Run: `go test ./db/datastruct/btindex/ -bench=. -benchmem -count=3`
- [ ] Document results

### Task 6: Clean up and finalize

- [ ] Remove `BpsTree.prefix` field and embedded `prefixIndex` type from `bps_tree.go` (code moved to `prefix_index.go`)
- [ ] Keep `BpsTree` itself intact for now (small-file fallback, can be removed in follow-up)
- [ ] Update `BtIndex` to use PrefixIndex as primary, BpsTree as fallback only when PrefixIndex is nil
- [ ] Run full tests: `go test ./db/datastruct/btindex/... -count=1`
- [ ] Run: `make lint` (repeat until clean)
- [ ] Run: `make erigon integration`
- [ ] Move plan to `docs/plans/completed/`

## Edge Cases

| Case | Behavior |
|------|----------|
| Key shorter than 2 bytes | L1-only narrowing, then disk bsearch |
| Key shorter than 1 byte | No prefix narrowing, full disk bsearch |
| No keys with prefix | `lookup` returns `(0, 0)`, Get returns not-found |
| All keys share first 2 bytes | Single bucket with 8 nodes, good narrowing |
| File with 0 keys | `PrefixIndex.offt.Count() == 0`, early return |
| File with 1-7 keys | All keys become nodes in their bucket, exact match likely |
| Prefix has exactly 8 keys | All 8 become nodes — complete coverage |
| Prefix has >8 keys | 8 evenly spaced — worst case gap is `count/8` |
| `.bt` file has no nodes section | Use `NewPrefixIndex` (build nodes from .kv scan) |
| `.bt` file has nodes section | Use `NewPrefixIndexWithNodes` (distribute existing nodes) |

## Memory Budget

| Component | Size | Notes |
|-----------|------|-------|
| `buckets[65536]` (DI ranges only) | 65536 * 16 = 1 MB | `firstDI` + `endDI` per bucket |
| `l1First[256]` + `l1End[256]` | 256 * 8 * 2 = 4 KB | L1 aggregation arrays |
| Nodes (8 per active prefix) | ~50k * 8 * 80B = ~32 MB worst case | 80B avg per node (32B key + 16B di/off + slice header) |
| Nodes (realistic) | ~5-10 MB | Most files have 10-20k active prefixes |
| Temporary counts array | 65536 * 4 = 256 KB | Discarded after construction |
| **Total per file** | **~3-12 MB** | Comparable to current BpsTree+prefixIndex |

Note: current BpsTree allocates `N/M` nodes globally (~46k for 12M keys) at ~80B each = ~3.7MB. PrefixIndex allocates up to `8 * active_prefixes` nodes. For files with many active prefixes, PrefixIndex may use slightly more memory but with much better distribution.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Two-pass .kv scan slower than single pass | Sequential reads are fast on mmap'd files; measure with benchmark |
| More nodes total = more memory | Cap at 8 per bucket; for 50k active prefixes = 400k nodes max |
| Node random reads slower than sequential | Reads are from mmap'd file; only 8 per active prefix |
| Breaking BtIndex API | Keep BtIndex wrapper unchanged; PrefixIndex is internal |
| Regression for small files | Test with files below 60k threshold |
