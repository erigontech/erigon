# HashSort Performance: Grow-only KeyUpdate Slab + Byte Arena

## Overview

Reduce allocation pressure in `Updates.HashSort` which accounts for 74.6% of allocations in `Benchmark_HexPatriciaHashed_Process` (93KB/op, 128 allocs/op). 44% of CPU is GC driven by these allocations.

The fix replaces per-key heap allocations with a grow-only value slab (`[]KeyUpdate`) and a contiguous byte arena (`[]byte`) on the `Updates` struct. Both are reset to `[:0]` between calls but retain capacity, adapting to workload size (chain tip ~50 keys, historical batch ~50K+ keys).

## Context

**Files involved:**
- `execution/commitment/commitment.go` — `Updates` struct (line 1495), `HashSort` (line 1706), `KeyUpdate` struct (line 1864)
- `execution/commitment/hex_patricia_hashed_bench_test.go` — `Benchmark_HexPatriciaHashed_Process`

**Current allocation pattern (per key, 3 heap allocs):**
```go
batch := make([]*KeyUpdate, 0, hashSortBatchSize)  // pointer slice
hk := common.Copy(k)                                // alloc #1
pk := common.Copy(v)                                 // alloc #2
batch = append(batch, &KeyUpdate{...})               // alloc #3 (pointer escape)
```

**Safety verified:** Both `hashedKey` and `plainKey` are consumed inline by `followAndUpdate` — keys are copied into fixed-size cell arrays (`accountAddr[20]`, `storageAddr[52]`, `hashedExtension[128]`). No downstream path retains slice references. Arena reset between batches is safe.

**Two HashSort modes:**
- `ModeDirect` (production): ETL-based, keys come as `[]byte` from ETL. Currently converts to `string(pk)` and back via `toBytesZeroCopy`. Roundtrip is unnecessary.
- `ModeUpdate` (used in tests/some paths): btree-based, needs `plainKey` as `string` for `<` comparison. Arena still helps for `hashedKey` copies.

## Development Approach
- **Testing approach**: Regular (code first, benchmark to validate)
- Complete each task fully before moving to the next
- Benchmark before and after each task to measure impact
- Run existing tests after each change to catch regressions

## Implementation Steps

### Task 1: Capture baseline benchmarks

**Files:**
- Create: `docs/plans/20260304-hashsort-arena-slab-baseline.txt`

- [x] Run `Benchmark_HexPatriciaHashed_Process` with `-count=5 -benchmem` and save output
- [x] Run with `-cpuprofile` and `-memprofile`, save `pprof -top` output
- [x] Document baseline numbers in this plan (update Technical Details section)

### Task 2: Add slab and arena fields to `Updates` struct

**Files:**
- Modify: `execution/commitment/commitment.go`

- [x] Add `batchSlab []KeyUpdate` field to `Updates` struct (value slice, not pointer slice)
- [x] Add `byteArena []byte` field to `Updates` struct
- [x] Add `arenaAlloc` helper method: appends bytes to arena, returns sub-slice
  ```go
  func (t *Updates) arenaAlloc(b []byte) []byte {
      off := len(t.byteArena)
      t.byteArena = append(t.byteArena, b...)
      return t.byteArena[off : off+len(b)]
  }
  ```
- [x] Reset both in `Reset()` method: `t.batchSlab = t.batchSlab[:0]` and `t.byteArena = t.byteArena[:0]`
- [x] Run `go test ./execution/commitment/...` — must pass

### Task 3: Convert `ModeDirect` path to use slab + arena

**Files:**
- Modify: `execution/commitment/commitment.go`

- [x] Replace `batch := make([]*KeyUpdate, 0, hashSortBatchSize)` with `t.batchSlab = t.batchSlab[:0]`
- [x] Replace `common.Copy(k)` with `t.arenaAlloc(k)` for hashed key
- [x] Replace `common.Copy(v)` with `t.arenaAlloc(v)` for plain key
- [x] Replace `batch = append(batch, &KeyUpdate{...})` with `t.batchSlab = append(t.batchSlab, KeyUpdate{...})`
- [x] Update batch processing loop to iterate `t.batchSlab` by index (not pointer deref)
- [x] Reset arena between batches: `t.byteArena = t.byteArena[:0]` after batch processing completes
- [x] Replace `string(pk)` with direct `[]byte` storage in `KeyUpdate.plainKey` for ModeDirect — this requires changing how the callback passes `pk` (use the arena slice directly instead of `toBytesZeroCopy(p.plainKey)`)
- [x] Run `go test ./execution/commitment/...` — must pass
- [x] Run `Benchmark_HexPatriciaHashed_Process` with `-count=5 -benchmem`, compare to baseline

### Task 4: Convert `ModeUpdate` path to use slab + arena for `hashedKey`

**Files:**
- Modify: `execution/commitment/commitment.go`

- [x] Replace `batch := make([]*KeyUpdate, 0, hashSortBatchSize)` with `t.batchSlab = t.batchSlab[:0]`
- [x] Replace `hk := make([]byte, len(item.hashedKey)); copy(hk, item.hashedKey)` with `hk := t.arenaAlloc(item.hashedKey)`
- [x] Update batch processing loop to iterate `t.batchSlab` by index
- [x] Note: `plainKey` stays as `string` in ModeUpdate (needed for btree `<` ordering) — but the batch copy can reference `item.plainKey` directly since tree is cleared only after all processing (line 1858)
- [x] Reset arena between batches
- [x] Run `go test ./execution/commitment/...` — must pass

### Task 5: Add dedicated HashSort benchmarks

**Files:**
- Modify: `execution/commitment/commitment_bench_test.go`

- [x] Add `BenchmarkHashSort_ModeDirect` — inserts N keys via `Put`, then calls `HashSort` with noop callback. Sub-benchmarks for N=50 (chain tip), N=5000, N=50000 (historical)
- [x] Add `BenchmarkHashSort_ModeUpdate` — same pattern with `ModeUpdate`
- [x] Run new benchmarks, document results
- [x] Run full benchmark suite to verify no regressions

### Task 6: Verify and document results

**Files:**
- Modify: `docs/plans/20260304-hashsort-arena-slab.md`

- [x] Run `Benchmark_HexPatriciaHashed_Process` with `-count=5 -benchmem`, compare to Task 1 baseline
- [x] Run with `-cpuprofile`, compare GC % to baseline
- [x] Document before/after comparison in Technical Details
- [x] Run `go test ./execution/commitment/...` — full pass
- [x] Move this plan to `docs/plans/completed/`

## Technical Details

### KeyUpdate struct change

Current:
```go
type KeyUpdate struct {
    plainKey  string   // string for btree ordering
    hashedKey []byte   // independent heap alloc
    update    *Update  // pointer to update (only set in ModeUpdate)
}
```

After (no struct change needed — the change is in how instances are allocated):
- `ModeDirect`: `plainKey` stored as `string` still (needed for btree compat in shared struct), but the underlying bytes come from arena. The `toBytesZeroCopy` conversion becomes a direct pass of the arena slice to the callback.
- `ModeUpdate`: `plainKey` references tree item directly (no copy needed since tree is cleared after processing).

### Arena lifecycle

```
HashSort() called
  ├── batchSlab = batchSlab[:0]     // reset, keep capacity
  ├── byteArena = byteArena[:0]     // reset, keep capacity
  │
  ├── ETL/Tree iteration
  │   ├── key bytes → arenaAlloc()  // append to arena
  │   ├── KeyUpdate → batchSlab     // append value (not pointer)
  │   │
  │   └── batch full (10K)?
  │       ├── process batch via fn() callback
  │       │   └── followAndUpdate copies keys into cell fixed arrays
  │       ├── batchSlab = batchSlab[:0]   // reset for next batch
  │       └── byteArena = byteArena[:0]   // safe: keys already copied into cells
  │
  └── process final batch
      └── initCollector() / tree.Clear()
```

### Expected impact

- **Allocations**: ~128/op → ~2-5/op (remaining: ETL internals, Update struct)
- **Alloc bytes**: ~93KB/op → ~5-10KB/op
- **CPU**: ~44% GC → <10% GC
- **Memory**: Slab + arena grow to peak and stay. For chain tip (~50 keys), ~10KB retained. For batch (~50K), ~5MB retained. Both acceptable.

## Baseline (captured Task 1, 2026-03-04)

```
Benchmark_HexPatriciaHashed_Process-12    58540    20837 ns/op    93612 B/op    128 allocs/op
Benchmark_HexPatriciaHashed_Process-12    58990    20722 ns/op    93646 B/op    128 allocs/op
Benchmark_HexPatriciaHashed_Process-12    57842    20694 ns/op    93747 B/op    128 allocs/op
Benchmark_HexPatriciaHashed_Process-12    54991    21069 ns/op    93725 B/op    128 allocs/op
Benchmark_HexPatriciaHashed_Process-12    55731    21123 ns/op    93756 B/op    128 allocs/op

Average: ~20889 ns/op, ~93697 B/op, 128 allocs/op
CPU: GC-related functions ~32.5% of CPU time
Memory: HashSort accounts for 63.27% of all allocations (4354.60MB / 6883.08MB)
```

## Final Results (Task 6, 2026-03-04)

```
Benchmark_HexPatriciaHashed_Process-12    71618    17442 ns/op    10601 B/op    106 allocs/op
Benchmark_HexPatriciaHashed_Process-12    71840    18145 ns/op    10586 B/op    106 allocs/op
Benchmark_HexPatriciaHashed_Process-12    71552    18431 ns/op    10571 B/op    106 allocs/op
Benchmark_HexPatriciaHashed_Process-12    69348    17831 ns/op    10638 B/op    106 allocs/op
Benchmark_HexPatriciaHashed_Process-12    67329    18014 ns/op    10562 B/op    106 allocs/op

Average: ~17973 ns/op, ~10592 B/op, 106 allocs/op
```

### Before/After Comparison

| Metric        | Before (Task 1)  | After (Task 6)   | Improvement     |
|---------------|-------------------|-------------------|-----------------|
| ns/op         | 20,889            | 17,973            | -14.0%          |
| B/op          | 93,697            | 10,592            | -88.7% (8.8x)   |
| allocs/op     | 128               | 106               | -17.2%          |
| GC CPU %      | ~32.5%            | ~24%              | -8.5pp          |
| Iterations    | ~57K              | ~70K              | +23%            |

The slab+arena changes achieved a 8.8x reduction in allocation bytes per operation,
14% improvement in throughput, and meaningful GC pressure reduction.

## Post-Completion

- Monitor production memory usage after deploy — the grow-only arena retains peak capacity
- Consider adding `Updates.TrimArena()` method if memory retention becomes a concern for long-running nodes
- Future: apply same pattern to `WarmupCache.PutBranch` (3 allocs/put) if profiling shows it as next bottleneck
