# CachingPatriciaContext + TrieReader-Based Warmup

## Overview

Replace the current commitment warmup implementation (`Warmuper` + `WarmupCache`) with a
two-layer design:

1. **CachingPatriciaContext** — a read-through caching wrapper around `PatriciaContext` with
   a shared concurrent cache (`maphash.Map`) and per-worker underlying DB contexts.
2. **TrieReader-based warmup** — 16 `TrieReader` instances calling `Lookup()` with a visitor
   callback, replacing the hand-rolled trie traversal in `warmupKey()`.

**Problems solved:**
- Removes ~70 lines of duplicated trie navigation logic (`warmupKey` reimplements `TrieReader.Lookup`)
- Provides precise "key warmed" signal (TrieReader returns found/not-found)
- Non-blocking RO warmup — TrieReader is stateless, no grid mutation
- Transparent caching — `startDepth` optimization becomes unnecessary (shallow prefix reads are cache hits)
- Single caching layer used by both warmup and `Process()`, replacing the split `WarmupCache` + `branchFromCacheOrDB` pattern

## Context (from discovery)

**Files/components involved:**
- `execution/commitment/warmuper.go` — current warmup orchestrator (to be replaced)
- `execution/commitment/warmup_cache.go` — current evict-on-read cache (to be replaced)
- `execution/commitment/trie_reader.go` — RO trie navigator (to add visitor callback)
- `execution/commitment/commitment.go` — `PatriciaContext` interface, `HashSort()`, `BranchEncoder`
- `execution/commitment/hex_patricia_hashed.go` — `Process()`, `branchFromCacheOrDB/accountFromCacheOrDB/storageFromCacheOrDB`
- `execution/commitment/commitmentdb/commitment_context.go` — `ComputeCommitment()`, `trieContextFactory`

**Related patterns:**
- `maphash.Map[V]` (concurrent, lock-free) — `common/maphash/maphash.go`
- `TrieContextFactory func() (PatriciaContext, func())` — per-worker context + cleanup
- `extractBranchCellAddresses()` — current account/storage prefetch during warmup

**Dependencies:**
- `xsync.Map` (via `maphash.Map`) for concurrent cache
- `errgroup.Group` for worker coordination (existing pattern)

## Development Approach

- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests**
- **CRITICAL: all tests must pass before starting next task**
- **CRITICAL: update this plan file when scope changes during implementation**
- Run `make lint` before committing

## Testing Strategy

- **Unit tests**: CachingPatriciaContext cache semantics, TrieReader visitor callback, new Warmuper integration
- **Benchmarks**: Before/after comparison using existing `Benchmark_HexPatriciaHashed_Process` and new targeted benchmarks for cache hit rates, per-key warmup latency, memory usage
- **Integration**: Existing `trie_reader_integration_test.go` extended for cached context
- **Regression**: Ensure state root computation produces identical results

## Progress Tracking

- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix
- Update plan if implementation deviates from original scope

## Implementation Steps

### Task 1: CachingPatriciaContext — shared cache layer

**Files:**
- Create: `execution/commitment/caching_patricia_context.go`
- Create: `execution/commitment/caching_patricia_context_test.go`

- [x] Define `CachingPatriciaContext` struct with three `maphash.Map` fields (branches, accounts, storage)
- [x] Define `branchCacheEntry` struct holding `(data []byte, step kv.Step)`
- [x] Implement `NewCachingPatriciaContext()` constructor
- [x] Implement `Reset()` — calls `Clear()` on all three maps
- [x] Implement `Wrap(underlying PatriciaContext) PatriciaContext` — returns a `cachedView`
- [x] Define `cachedView` struct with `cache *CachingPatriciaContext` and `underlying PatriciaContext`
- [x] Implement `cachedView.Branch(prefix)` — cache check via `Get`, miss → `underlying.Branch()` → `Set` → return
- [x] Implement `cachedView.Account(plainKey)` — same read-through pattern
- [x] Implement `cachedView.Storage(plainKey)` — same read-through pattern
- [x] Implement `cachedView.PutBranch(prefix, data, prevData)` — pass-through to underlying + invalidate cache entry for prefix
- [x] Write tests: cache miss → populates cache → subsequent read is cache hit
- [x] Write tests: `PutBranch` invalidates cached branch entry
- [x] Write tests: `Reset()` clears all entries
- [x] Write tests: concurrent reads from multiple goroutines (race detector)
- [x] Run tests — must pass before task 2

### Task 2: TrieReader visitor callback

**Files:**
- Modify: `execution/commitment/trie_reader.go`
- Modify: `execution/commitment/trie_reader_test.go`

- [ ] Define `BranchVisitor func(depth int, cell *cell) error` callback type
- [ ] Add `LookupWithVisitor(hashedKey []byte, visitor BranchVisitor) (cell, bool, error)` method
- [ ] At each branch node after `parseCellAt()`, invoke `visitor(depth, &parsedCell)` if visitor is non-nil
- [ ] Visitor receives the parsed cell which contains `accountAddr`/`storageAddr` — caller can issue prefetch reads
- [ ] Ensure existing `Lookup()` still works unchanged (calls `LookupWithVisitor` with nil visitor, or stays separate)
- [ ] Write tests: visitor is called at each depth during traversal
- [ ] Write tests: visitor receives correct cell data (account/storage addresses match expected)
- [ ] Write tests: visitor error propagates as Lookup error
- [ ] Write tests: nil visitor behaves identically to plain Lookup
- [ ] Run tests — must pass before task 3

### Task 3: TrieReader-based Warmuper replacement

**Files:**
- Modify: `execution/commitment/warmuper.go`
- Modify: `execution/commitment/warmuper_test.go` (if exists)

- [ ] Replace `warmupKey()` body with `TrieReader.LookupWithVisitor()` call
- [ ] Implement warmup visitor: for each cell with `accountAddrLen > 0`, call `ctx.Account()`; for `storageAddrLen > 0`, call `ctx.Storage()` — reads go through `cachedView` so results are cached
- [ ] Remove `branchFromCacheOrDB`, `accountFromCacheOrDB`, `storageFromCacheOrDB` methods from Warmuper
- [ ] Remove `extractBranchCellAddresses` usage from warmuper (TrieReader's `parseCellAt` replaces it)
- [ ] Each worker creates a `TrieReader` with `cache.Wrap(workerCtx)` instead of raw `PatriciaContext`
- [ ] Remove `startDepth` from `WarmKey()` signature — no longer needed (cache handles repeated prefix reads)
- [ ] Update `WarmupConfig`: remove `MaxDepth` (TrieReader traverses to leaf), remove `EnableWarmupCache` (always cached)
- [ ] Update `HashSort()` call sites that pass `startDepth` — simplify to just `warmuper.WarmKey(hk)`
- [ ] Write tests: warmup with TrieReader produces same cache state as old implementation
- [ ] Write tests: 16 concurrent warmup workers with shared cache (race detector)
- [ ] Run tests — must pass before task 4

### Task 4: Integrate CachingPatriciaContext into Process()

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Modify: `execution/commitment/commitmentdb/commitment_context.go`

- [ ] In `ComputeCommitment()`: create `CachingPatriciaContext`, pass to `Process()` via `WarmupConfig` or new field
- [ ] In `Process()`: wrap the main `PatriciaContext` with `cache.Wrap(hph.ctx)` so Process reads also hit the shared cache
- [ ] Remove `branchFromCacheOrDB`, `accountFromCacheOrDB`, `storageFromCacheOrDB` from `HexPatriciaHashed` — direct `hph.ctx.Branch/Account/Storage` calls now go through cached view
- [ ] Remove `hph.cache *WarmupCache` field and all `SetCache`/`hph.cache` references
- [ ] Remove `BranchEncoder.SetCache()` and its cache-or-DB pattern in `CollectUpdate()`
- [ ] Call `cache.Reset()` after `Process()` returns (in `ComputeCommitment` defer)
- [ ] Handle recording mode: when `recorder != nil`, skip cache (or wrap recorder's context without caching) to preserve trace completeness
- [ ] Write tests: `Process()` with CachingPatriciaContext produces identical root hash as before
- [ ] Run existing `Benchmark_HexPatriciaHashed_Process` to verify no regression
- [ ] Run tests — must pass before task 5

### Task 5: Remove old WarmupCache

**Files:**
- Delete: `execution/commitment/warmup_cache.go`
- Delete: `execution/commitment/warmup_cache_test.go`
- Modify: `execution/commitment/hex_patricia_hashed.go` (cleanup remaining references)
- Modify: `execution/commitment/commitment.go` (cleanup `extractBranchCellAddresses` if no longer used)

- [ ] Delete `warmup_cache.go` and `warmup_cache_test.go`
- [ ] Remove `extractBranchCellAddresses()` from `hex_patricia_hashed.go` (replaced by TrieReader cell parsing)
- [ ] Remove `enableWarmupCache` field from `HexPatriciaHashed`
- [ ] Remove `EnableWarmupCache()` method from `HexPatriciaHashed`
- [ ] Clean up any remaining imports or references to deleted types
- [ ] Run `make lint` — fix all issues
- [ ] Run `go test ./execution/commitment/...` — must pass

### Task 6: Benchmarks and observability

**Files:**
- Create: `execution/commitment/caching_patricia_context_bench_test.go`
- Modify: `execution/commitment/warmuper.go` (metrics/logging)

- [ ] Add cache hit/miss counters to `CachingPatriciaContext` (atomic uint64 per map: branchHits, branchMisses, accountHits, accountMisses, storageHits, storageMisses)
- [ ] Add `Stats()` method returning hit rates — logged after `Process()` completes
- [ ] Add per-key warmup completion logging: when `LookupWithVisitor` returns, the key is warmed — log timing if debug-level
- [ ] Write `BenchmarkCachingPatriciaContext_ReadThrough` — measure read-through overhead vs direct
- [ ] Write `BenchmarkCachingPatriciaContext_CacheHit` — measure cache hit latency
- [ ] Write `BenchmarkWarmuper_TrieReader` — compare new warmup vs baseline (no warmup) on synthetic trie
- [ ] Run before/after `Benchmark_HexPatriciaHashed_Process` comparison and document results
- [ ] Run tests — must pass

### Task 7: Verify acceptance criteria

- [ ] Verify identical state root for all test cases (unit + integration)
- [ ] Verify warmup cache hit rates are reported in logs
- [ ] Verify per-key "warmed" signal works (LookupWithVisitor returns found=true for existing keys)
- [ ] Verify no data races under `-race` flag with 16 workers
- [ ] Run full test suite: `go test ./execution/commitment/...`
- [ ] Run `make lint`
- [ ] Run `make erigon` — binary builds cleanly

### Task 8: [Final] Update documentation

- [ ] Update `execution/commitment/agents.md` if it exists (or CLAUDE.md) with new caching architecture
- [ ] Move this plan to `docs/plans/completed/`

## Technical Details

### CachingPatriciaContext data structures

```go
type CachingPatriciaContext struct {
    branches *maphash.Map[branchCacheEntry]
    accounts *maphash.Map[*Update]
    storage  *maphash.Map[*Update]
}

type branchCacheEntry struct {
    data []byte
    step kv.Step
}

type cachedView struct {
    cache      *CachingPatriciaContext
    underlying PatriciaContext
}
```

### Cache flow during commitment

```
ComputeCommitment():
  cache := NewCachingPatriciaContext()
  defer cache.Reset()

  // Warmup phase (parallel, non-blocking):
  for i := 0; i < 16; i++ {
      ctx, cleanup := ctxFactory()
      view := cache.Wrap(ctx)
      reader := NewTrieReader(view, length.Addr)
      // worker goroutine: reader.LookupWithVisitor(key, prefetchVisitor)
  }

  // Process phase (sequential):
  processView := cache.Wrap(mainCtx)
  hph.ctx = processView  // Process reads hit warmed cache
  rootHash = hph.Process(...)
```

### Visitor callback for account/storage prefetch

```go
func warmupVisitor(ctx PatriciaContext) BranchVisitor {
    return func(depth int, c *cell) error {
        if c.accountAddrLen > 0 {
            _, _ = ctx.Account(c.GetAccountAddr()) // read-through caches it
        }
        if c.storageAddrLen > 0 {
            _, _ = ctx.Storage(c.GetStorageAddr()) // read-through caches it
        }
        return nil
    }
}
```

### Key behavioral changes from old design

| Aspect | Old (WarmupCache) | New (CachingPatriciaContext) |
|--------|-------------------|----------------------------|
| Cache eviction | Get-and-evict (single use) | No eviction during cycle |
| startDepth | Manual tracking in HashSort | Unnecessary (cache hits) |
| Trie traversal | Hand-rolled in warmupKey() | TrieReader.LookupWithVisitor() |
| Cache scope | Warmup only, copied to Process | Shared across warmup + Process |
| PutBranch | Not cached | Pass-through + invalidate |
| Lifetime | Warmuper owns cache | ComputeCommitment owns, Reset after |

## Post-Completion

**Manual verification:**
- Run full sync on testnet with warmup enabled — compare state roots against known-good values
- Compare commitment computation wall time (before/after) on a representative block range
- Check memory profile — CachingPatriciaContext should use similar or less memory than old WarmupCache since entries are not duplicated between warmup and process caches

**Performance validation:**
- If cache hit rates are below ~60% for branches, investigate whether key ordering in HashSort provides sufficient locality
- If memory usage increases significantly, consider adding a size limit to the cache maps with LRU eviction
