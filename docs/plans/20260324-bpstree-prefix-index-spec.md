# Spec: 2-Level Prefix Index for BpsTree

## Overview

Add a 2-level prefix index to `BpsTree` that maps the first 2 bytes of keys to `di` (decompressor index) boundaries. During `Seek`/`Get`, the prefix index provides O(1) left/right bounds before binary search, dramatically reducing the search range for large files.

**Activation**: Only for files with >60,000 keys (below this threshold, the cached node binary search is already fast enough).

**Integration point**: Built during `WarmUp()`, used in `Seek()`/`Get()` before and alongside existing `bs()`.

## Data Structure

```go
// prefixIndex maps first 2 bytes of keys to minimum di where that prefix starts.
// Level 1: byte[0] → minDi (256 entries)
// Level 2: byte[0][byte[1]] → minDi (256×256 = 65,536 entries)
//
// For a lookup key starting with [0x02, 0xab]:
//   left  = prefixL2[0x02][0xab]     (first di with prefix 02ab)
//   right = prefixL2[0x02][0xac]     (first di with prefix 02ac, or next non-empty)
//   binary search [left, right) instead of [0, N)
type prefixIndex struct {
    l1 [256]uint64       // byte[0] → min di
    l2 [256][256]uint64  // byte[0][byte[1]] → min di
}
```

Memory: `256×8 + 256×256×8 = 2KB + 512KB = 514KB` per file. Acceptable.

### Sentinel Values

- Unused entries hold `math.MaxUint64` (no keys with that prefix exist)
- This naturally sorts empty prefixes to the end, making "find next non-empty" straightforward

## Context (key code locations)

| What | Where | Lines |
|------|-------|-------|
| `BpsTree` struct | `db/datastruct/btindex/bps_tree.go` | 90-99 |
| `WarmUp()` | same file | 193-228 |
| `bs()` (cached node binary search) | same file | 230-257 |
| `Seek()` | same file | 259-335 |
| `Get()` | same file | 340-411 |
| `DefaultBtreeM` | `db/datastruct/btindex/btree_index.go` | 53 |
| `DefaultBtreeStartSkip` | same file | 55 |
| `BtIndex.bplus` field | same file | 351 |
| Existing `b0 [256]bool` (first-byte tracking during index build) | same file | 421 |
| Benchmark | `db/datastruct/btindex/bpstree_bench_test.go` | 13-53 |

## Development Approach
- **Testing approach**: Regular (code first, then tests)
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: all tests must pass before starting next task**
- Run `make lint` after changes (run repeatedly per CLAUDE.md — linter is non-deterministic)

## Implementation Steps

### Task 1: Define prefixIndex type and construction

**Files:**
- Modify: `db/datastruct/btindex/bps_tree.go`

- [ ] Add `prefixIndex` struct with `l1 [256]uint64` and `l2 [256][256]uint64` fields
- [ ] Add `prefix *prefixIndex` field to `BpsTree` struct (nil when not active)
- [ ] Add constant `minKeysForPrefixIndex = 60_000`
- [ ] Implement `newPrefixIndex() *prefixIndex` — allocates and fills all entries with `math.MaxUint64`
- [ ] Implement `(p *prefixIndex) record(key []byte, di uint64)` — if `len(key) >= 1` and `di < p.l1[key[0]]`, set `p.l1[key[0]] = di`; same for `l2[key[0]][key[1]]` when `len(key) >= 2`
- [ ] Verify builds: `go build ./db/datastruct/btindex/...`

### Task 2: Build prefix index during WarmUp

**Files:**
- Modify: `db/datastruct/btindex/bps_tree.go`

- [ ] In `WarmUp()`, after computing `N`: if `N >= minKeysForPrefixIndex`, call `newPrefixIndex()` and store in `b.prefix`
- [ ] The existing WarmUp loop iterates every `step`-th key. This is NOT sufficient for the prefix index — it needs every key's first 2 bytes. Modify WarmUp to iterate ALL keys when prefix index is active, calling `p.record(key, di)` for each. The existing node caching (every `step`-th key) continues as before — inside the same loop, the `if di == nextCachePoint` logic picks which keys to cache as nodes.
- [ ] Log prefix index allocation in the existing WarmUp log line (add "prefixIdx" field when active)
- [ ] Verify builds: `go build ./db/datastruct/btindex/...`
- [ ] Run existing tests: `go test ./db/datastruct/btindex/... -count=1`

**Design note on WarmUp loop restructuring:**

Current WarmUp iterates with `step` jumps (only reads every M-th key). With the prefix index, we need to read every key's first 2 bytes anyway. The restructured loop should:
1. Iterate ALL keys sequentially (di = 0, 1, 2, ...)
2. For each key: call `p.record(key, di)`
3. Every `step`-th key (at di = step-1, 2*step-1, ...): cache the full key as a Node (existing behavior)

This means WarmUp scans the entire file even for large datasets, but this is acceptable because:
- WarmUp already reads from the compressed segment reader sequentially
- Sequential reads are fast (no random I/O)
- This is a one-time startup cost
- The file is already memory-mapped

The `keyCmpFunc` call used today for reading keys during WarmUp positions the reader at arbitrary offsets. For the full scan, we should instead iterate sequentially using the segment reader's `Next()` method to avoid O(N) random seeks. This requires reading key bytes directly rather than using `keyCmpFunc`.

### Task 3: Add prefix index lookup method

**Files:**
- Modify: `db/datastruct/btindex/bps_tree.go`

- [ ] Implement `(p *prefixIndex) lookup(key []byte, totalCount uint64) (l, r uint64)` that returns the narrowed `[l, r)` range:
  - If `len(key) < 1`: return `(0, totalCount)` — no narrowing possible
  - L1 narrowing: `l = p.l1[key[0]]`; for right bound, find next occupied L1 entry after `key[0]`
  - If `len(key) >= 2` — L2 narrowing: `l = p.l2[key[0]][key[1]]` (tighter left); for right bound, scan `p.l2[key[0]][key[1]+1..]` then continue to `p.l2[key[0]+1..][0..]` for next occupied entry
  - If `l == math.MaxUint64` (no keys with this prefix): return `(0, 0)` — signals "not found"
  - Clamp `r` to `totalCount` if it exceeds it
- [ ] Add unit test `TestPrefixIndexLookup` — construct a prefixIndex, record known keys, verify `lookup()` returns correct bounds
- [ ] Verify: `go test ./db/datastruct/btindex/... -run TestPrefixIndex -v`

### Task 4: Integrate prefix index into Seek and Get

**Files:**
- Modify: `db/datastruct/btindex/bps_tree.go`

- [ ] In `Seek()`, before `b.bs(seekKey)`: if `b.prefix != nil`, call `b.prefix.lookup(seekKey, b.offt.Count())` to get `(pl, pr)`. Then call `b.bs(seekKey)` to get `(_, bl, br)`. Use the intersection: `l = max(pl, bl)`, `r = min(pr, br)`. This ensures we always get the tighter bounds from whichever method narrows more.
- [ ] In `Get()`, same integration pattern before the binary search loop.
- [ ] Handle edge case: if prefix index returns `(0, 0)` (no keys with this prefix), return immediately — key cannot exist. For `Get`, return `nil, false`. For `Seek`, fall through to find next key via cached nodes.
- [ ] Verify: `go test ./db/datastruct/btindex/... -count=1`

### Task 5: Add benchmarks

**Files:**
- Modify: `db/datastruct/btindex/bpstree_bench_test.go`

- [ ] Add `BenchmarkBpsTreeGet` — same setup as existing `BenchmarkBpsTreeSeek` but calls `bt.Get()` instead
- [ ] Add `BenchmarkBpsTreeSeekLargeFile` — uses 1M keys (enough to trigger prefix index at 60k threshold) with shorter key sizes (20 bytes — typical Ethereum hash length) to benchmark the prefix index path specifically
- [ ] Add a sub-benchmark or separate benchmark that disables the prefix index (set `b.prefix = nil` after warmup) to measure the delta
- [ ] Run benchmarks: `go test ./db/datastruct/btindex/ -bench=BpsTree -benchmem -count=3`
- [ ] Document results in commit message

### Task 6: Verify and finalize

- [ ] Run full package tests: `go test ./db/datastruct/btindex/... -count=1`
- [ ] Run lint: `make lint` (repeat until clean)
- [ ] Run build: `make erigon`
- [ ] Move plan to `docs/plans/completed/`

## Technical Details

### Lookup algorithm walkthrough

For key `0x02ab...` in a file with 12M keys:

```
Step 1: L1 lookup
  l1[0x02] = 500,000    (first key starting with 0x02)
  l1[0x03] = 700,000    (first key starting with 0x03)
  → L1 range: [500000, 700000)  — 200k keys

Step 2: L2 lookup
  l2[0x02][0xab] = 531,250   (first key starting with 0x02ab)
  l2[0x02][0xac] = 532,050   (first key starting with 0x02ac)
  → L2 range: [531250, 532050)  — 800 keys

Step 3: bs() cached nodes
  bs(key) returns dl=531,400, dr=531,650
  → bs range: [531400, 531650) — 250 keys

Step 4: Intersection
  l = max(531250, 531400) = 531400
  r = min(532050, 531650) = 531650
  → Final range: [531400, 531650) — 250 keys
  → Binary search: log2(250) ≈ 8 comparisons

Without prefix index: bs() alone gives [531400, 531650), then 8 comparisons.
With prefix index: same result here, BUT bs() itself was faster because
the prefix index can skip the bs() entirely when it gives tighter bounds.
```

For skewed distributions (e.g., many keys starting with 0x00 in Ethereum):
- L1 gives wide range (0x00 prefix covers many keys)
- L2 narrows significantly (0x00xx is more selective)
- `bs()` may still give tighter bounds — intersection ensures best of both

### WarmUp loop restructuring

```
Before (current):
  for i := step; i < N; i += step {
      di := i - 1
      _, key, err = b.keyCmpFunc(nil, di, kv, key[:0])  // random access
      b.mx = append(b.mx, Node{...})
  }
  // Reads N/M keys via random access

After (with prefix index):
  var di uint64
  kv.Reset(0)
  nextCache := step - 1
  for di = 0; di < N; di++ {
      key, _ = kv.Next(key[:0])   // sequential read (fast)
      kv.Skip()                    // skip value

      if b.prefix != nil {
          b.prefix.record(key, di)  // record first 2 bytes
      }
      if di == nextCache {
          off := b.offt.Get(di)
          b.mx = append(b.mx, Node{off: off, key: common.Copy(key), di: di})
          nextCache += step
      }
  }
  // Reads ALL keys sequentially — faster than N/M random accesses
  // for large files, and builds prefix index as a side effect
```

### Edge cases

| Case | Behavior |
|------|----------|
| Key shorter than 2 bytes | L1 only (no L2 narrowing), falls through to bs() |
| Key shorter than 1 byte | No prefix narrowing, pure bs() path |
| No keys with prefix | lookup returns (0, 0), Get returns not-found, Seek falls through |
| All keys share first byte | L1 gives full range, L2 still narrows, bs() available as fallback |
| File with <60k keys | `b.prefix` is nil, zero overhead, existing behavior unchanged |
| Single key in file | `N < M`, step=1, all keys cached, prefix index not built |

### Interaction with NewBpsTreeWithNodes

`NewBpsTreeWithNodes` accepts pre-built nodes (from serialized index) and skips WarmUp. The prefix index must be built separately in this path. Options:
1. Add a separate `BuildPrefixIndex(kv *seg.Reader)` method that scans keys to build the prefix index
2. Call it from `NewBpsTreeWithNodes` when `len(nodes)*M >= minKeysForPrefixIndex`

This is handled in Task 2 — the prefix index scan can be factored into a helper called from both WarmUp and NewBpsTreeWithNodes.
