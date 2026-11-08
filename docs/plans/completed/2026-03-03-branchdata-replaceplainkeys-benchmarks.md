# BranchData ReplacePlainKeys — Benchmarks & Allocation Profiling

## Goal

Build focused benchmarks for `BranchData.ReplacePlainKeys`, profile allocations, and identify strategies to reduce them on both the key-shortening (write) and key-expansion (read) paths.

## Key Files

| File | What | Lines |
|------|------|-------|
| `execution/commitment/commitment.go` | `ReplacePlainKeys` method | 875–1005 |
| `execution/commitment/commitment_bench_test.go` | Existing benchmark (to rewrite) | 66–118 |
| `db/state/domain_committed.go` | `replaceShortenedKeysInBranch` (expansion) | 52–124 |
| `db/state/domain_committed.go` | `valTransform` (shortening, reuses `dt.comBuf`) | 340–436 |

## Problems with Existing Benchmark

- `make([]byte, 0, len(enc))` and `make([][]byte, 0)` allocate **inside** the hot loop
- `require.*` assertions inside the loop (heavy reflection, `fmt.Sprintf`)
- Measures round-trip (shorten + expand) in a single benchmark — conflates two different costs
- No `b.ReportAllocs()` call
- No sub-benchmarks to isolate scenarios

---

## Implementation Steps

### Step 1: Rewrite benchmarks in `commitment_bench_test.go`

- [x] Add helper `benchReplacePlainKeys` that takes `BranchData`, a pre-allocated buffer, and the callback — runs `b.Loop()` with `b.ReportAllocs()`, no assertions in the loop
- [x] Add sub-benchmarks via `b.Run()` under a single top-level `BenchmarkBranchData_ReplacePlainKeys`:

| Sub-benchmark | Data | Callback | Notes |
|---------------|------|----------|-------|
| `Shorten/Dense` | 16-cell synthetic via `generateCellRow` + `EncodeBranch` | Truncate account key to 4 bytes, storage to 8 bytes | Measures shortening path |
| `Shorten/Sparse` | 2-cell synthetic | Same | Realistic sparse update |
| `Expand/Dense` | Pre-shortened 16-cell | Expand back to original keys (looked up from pre-built map) | Measures expansion path |
| `Expand/Sparse` | Pre-shortened 2-cell | Same | |
| `NoChange/Dense` | 16-cell synthetic | Return `nil` (keep original) | Measures pure copy overhead |
| `BufferReuse/FreshAlloc` | 16-cell synthetic | Shorten | `make` inside loop (current behavior, for comparison) |
| `RealData/NoChange` | Hex from production (`86e586e5...`) | Return `nil` (keep original) | Production data has already-shortened 4-byte storage keys; measures parsing overhead on real format |

- [x] ~~Keep the old `BenchmarkBranchData_ReplacePlainKeys` renamed to `BenchmarkBranchData_ReplacePlainKeys_RoundTrip`~~ Removed — redundant with `TestBranchData_ReplacePlainKeys` in `commitment_test.go`
- [x] Verify all benchmarks compile and run: `go test -bench=BenchmarkBranchData_ReplacePlainKeys -benchtime=1x ./execution/commitment/`

### Step 2: Run baseline benchmarks

```bash
cd execution/commitment
go test -bench=BenchmarkBranchData_ReplacePlainKeys -benchmem -count=6 -timeout=5m . | tee bench_baseline.txt
```

- [x] Capture baseline numbers (ns/op, B/op, allocs/op)
- [x] Note which sub-benchmarks have non-zero allocs

### Step 3: CPU profiling

```bash
go test -bench=BenchmarkBranchData_ReplacePlainKeys/Shorten/Dense -cpuprofile=cpu_shorten.out -benchtime=3s .
go test -bench=BenchmarkBranchData_ReplacePlainKeys/Expand/Dense -cpuprofile=cpu_expand.out -benchtime=3s .
```

- [x] Run `go tool pprof -top cpu_shorten.out` and `go tool pprof -top cpu_expand.out`
- [x] Identify top CPU consumers (likely: `runtime.memmove` from append, `binary.Uvarint`, callback overhead)

### Step 4: Memory profiling

```bash
go test -bench=BenchmarkBranchData_ReplacePlainKeys/Shorten/Dense -memprofile=mem_shorten.out -benchtime=3s .
go test -bench=BenchmarkBranchData_ReplacePlainKeys/Expand/Dense -memprofile=mem_expand.out -benchtime=3s .
```

- [x] Run `go tool pprof -alloc_space mem_shorten.out` and `go tool pprof -alloc_space mem_expand.out`
- [x] Identify top allocation sources

### Step 5: Escape analysis

```bash
go build -gcflags='-m -m' ./execution/commitment/ 2>&1 | grep -i -A2 'ReplacePlainKeys'
```

- [x] Check if `newData`, `numBuf`, or callback closures escape to heap
- [x] Document findings

Escape analysis findings:
- `fn` (callback parameter): does NOT escape to heap (line 875:63: "fn does not escape")
- `numBuf` ([MaxVarintLen64]byte array): stays on stack. The `append(newData, numBuf[:n]...)` copies bytes into newData but numBuf itself is stack-allocated
- `newData` (parameter): leaks to result ~r0 (expected - it's the return value). Each `append(newData, ...)` is marked "escapes to heap" because the result slice is returned and may trigger backing array growth
- `branchData` (receiver): leaks to heap because slices of it are passed to the `fn` callback (e.g., `fn(branchData[pos-int(l):pos], false)` at line 924)
- Error paths: `errors.New()` and `fmt.Errorf()` allocate on error paths (10 instances), but these don't affect hot-path performance
- Function cannot be inlined (cost 1426 vs budget 80) due to complexity
- Benchmark callback closures: "func literal does not escape" for all benchReplacePlainKeys callbacks
- Key insight: the only hot-path heap allocation is newData growth from append. When caller pre-sizes the buffer (as benchmarks do), there are 0 allocations

### Step 6: Analyze and propose optimizations

Based on profiling data, evaluate these candidates:

| Candidate | When it helps | Expected impact |
|-----------|--------------|-----------------|
| Pre-size `newData` to `len(branchData)` | When caller passes `nil` or undersized buffer | Eliminates growth-related allocs |
| Early return when `fn` always returns nil | `NoChange` path — no keys actually replaced | Zero allocs, zero copies |
| Chunked copy (memcpy unchanged spans) | Branches with many non-key fields | Fewer `append` calls |
| Caller buffer pooling (`sync.Pool`) | High-frequency call sites | Amortize allocation across calls |
| In-place mutation for same-size keys | When replacement key length == original | Skip newData entirely |

- [x] Rank candidates by expected impact based on profile data
- [x] Implement top 1-2 optimizations
- [x] Re-run benchmarks, compare with baseline

Ranking (by expected impact based on profile data):
1. Pre-size newData + Chunked span copy + Early return (combined as "span-based lazy copy") - HIGH
2. In-place mutation for same-size keys - LOW (rare case in practice)
3. sync.Pool - LOW (callers already reuse buffers via dt.comBuf)

Implemented: Span-based lazy copy combining candidates 1, 2, and 3 from the table above.
Instead of copying every field individually to newData, track unchanged spans and flush them
in bulk only when a key actually changes. When no keys change, return branchData as-is (zero
copies). Auto-sizes newData to len(branchData) on first key change when cap is insufficient.

Results (median ns/op, baseline -> optimized):
- Shorten/Dense: 717 -> 524 (27% faster)
- Shorten/Sparse: 94 -> 56 (40% faster)
- Expand/Dense: 927 -> 836 (10% faster)
- Expand/Sparse: 114 -> 78 (32% faster)
- NoChange/Dense: 559 -> 228 (59% faster)
- RealData/NoChange: measures parsing overhead on production-format data (already-shortened keys)

---

## Success Criteria

- All sub-benchmarks pass and produce stable numbers
- Profile data clearly identifies allocation sources
- At least one optimization reduces allocs/op on the shorten and/or expand paths
- No behavioral changes to `ReplacePlainKeys` (existing tests still pass)
