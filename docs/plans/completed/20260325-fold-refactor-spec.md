# Spec: Refactor HexPatriciaHashed.fold() (Zabaniya)

## Overview

Restructure `fold()` from a 258-line monolith into a dispatcher that delegates to focused helper methods. The three update paths (delete, propagate, branch) become separate methods. Within the branch path, the shared cell-preparation and keccak2-feeding logic is unified so that deferred and non-deferred encoding diverge only at the final collect call.

**Guiding principle**: Every extraction must be a pure structural move — no behavior changes, no "while we're here" cleanups. The diff for each task should be mechanically verifiable: old code removed from fold(), identical code appears in the new method.

## Context (key code locations)

| What | Where | Lines |
|------|-------|-------|
| `HexPatriciaHashed` struct | `execution/commitment/hex_patricia_hashed.go` | 59–110 |
| `cell` struct | same file | 219–234 |
| `cell.fillFromLowerCell` | same file | 403–450 |
| `computeCellHashLen` | same file | 916–930 |
| `computeCellHash` | same file | 1126–1200 |
| `createCellGetter` | same file | 1931–1994 |
| `updateKind` constants | same file | 2001–2031 |
| **`fold()`** | same file | **2037–2293** |
| `loadStateIfNeeded` | same file | 2295–2321 |
| `followAndUpdate` (caller) | same file | 2422 |
| `Process` (caller) | same file | 2632, 2745 |
| `CollectUpdate` | `execution/commitment/commitment.go` | 556–610 |
| `CollectDeferredUpdate` | same file | 619–666 |
| `EncodeBranch` | same file | 680–756 |
| Tests | `execution/commitment/hex_patricia_hashed_test.go` | — |
| Benchmarks | `execution/commitment/commitment_bench_test.go` | — |

## Development Approach

- **Testing approach**: Tests first — add a fold-specific benchmark before refactoring, run after every task
- Complete each task fully before moving to the next
- Make small, focused changes — each task is one commit
- **CRITICAL: all tests must pass before starting next task**
- Run `make lint` after changes (run repeatedly — linter is non-deterministic)
- Use `benchstat` to compare before/after for each task

## Analysis of fold() Structure

### Current control flow

```
fold()
├── Setup (lines 2037-2072): row/upRow/upCell/depth/updateKey, trace logging
├── afterMapUpdateKind() → updateKind (line 2074)
├── switch updateKind:
│   ├── updateKindDelete (2076-2108)
│   │   ├── propagate touch/after maps upward (3 subcases: root/depth64/normal)
│   │   ├── upCell.reset()
│   │   ├── if branchBefore: CollectUpdate(delete) + cache evict
│   │   └── activeRows--, currentKeyLen adjustment
│   ├── updateKindPropagate (2109-2148)
│   │   ├── propagate touchMap upward (2 subcases: root/normal)
│   │   ├── loadStateIfNeeded
│   │   ├── upCell.fillFromLowerCell
│   │   ├── if branchBefore: CollectUpdate(delete)
│   │   └── activeRows--, currentKeyLen adjustment
│   └── updateKindBranch (2149-2290)
│       ├── propagate touchMap upward + set rootPresent (2 subcases)
│       ├── compute bitmap, handle !branchBefore
│       ├── iterate afterMap: memoization check + loadStateIfNeeded + computeCellHashLen
│       ├── keccak2.Reset + write RLP prefix
│       ├── if deferred:
│       │   ├── iterate afterMap: computeCellHash → keccak2.Write
│       │   └── CollectDeferredUpdate
│       ├── else (non-deferred):
│       │   ├── createCellGetter → CollectUpdate (keccak2 fed inside cellGetter)
│       │   └── fill remaining empty cells into keccak2
│       ├── set upCell extension/hash fields
│       ├── keccak2.Read → upCell.hash
│       └── activeRows--, currentKeyLen adjustment
└── return nil
```

### Repeated patterns

| Pattern | Occurrences | Lines |
|---------|-------------|-------|
| `activeRows--` + `currentKeyLen` adjustment | 3 (all cases) | 2103-2108, 2144-2145, 2285-2290 |
| `touchMap[row-1] \|= bit` (propagate touch upward) | 3 (all cases) | 2085-2089, 2114-2117, 2151-2158 |
| `branchBefore[row]` → `CollectUpdate(delete)` | 2 (delete + propagate) | 2094-2102, 2137-2143 |
| `loadStateIfNeeded` + metrics bookkeeping | 2 (propagate + branch) | 2124-2133, 2177-2201 |
| `hph.trace` guard → `fmt.Printf` | 8 scattered | throughout |

### What changes per case vs what's shared

**Shared (all cases):** setup, `activeRows--`, `currentKeyLen` adjustment, `depthsToTxNum` cleanup

**Delete-specific:** `upCell.reset()`, `afterMap &^= bit`, cache eviction

**Propagate-specific:** `fillFromLowerCell`, single-child state loading

**Branch-specific:** bitmap computation, cell iteration, hash computation, keccak2 accumulation, branch encoding (deferred/non-deferred), upCell extension/hash finalization

## Implementation Steps

### Task 0: Add fold-specific benchmark

**Files:**
- Modify: `execution/commitment/commitment_bench_test.go`

- [ ] Add `BenchmarkHexPatriciaHashedFold` that constructs a small trie (accounts + storage), calls `Process()`, and measures fold performance. This gives a baseline before refactoring.
- [ ] Capture baseline: `go test ./execution/commitment/ -bench=BenchmarkHexPatriciaHashedFold -benchmem -count=5 > bench-baseline.txt`
- [ ] Verify existing tests: `go test ./execution/commitment/... -count=1 -short`

### Task 1: Extract common epilogue from switch cases

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

The `activeRows--` and `currentKeyLen` adjustment is duplicated in all three cases with minor variations. Move it after the switch:

```go
// Currently in each case:
hph.activeRows--
if upDepth > 0 {
    hph.currentKeyLen = upDepth - 1
} else {
    hph.currentKeyLen = 0
}

// After: single instance after switch
```

- [ ] Verify that all three cases compute the same epilogue (they do — the delete case has the identical `if upDepth > 0` check, and propagate uses `max(upDepth-1, 0)` which is equivalent)
- [ ] Move the epilogue after the switch statement
- [ ] Remove the per-case duplicates
- [ ] Run tests: `go test ./execution/commitment/... -count=1 -short`
- [ ] Run benchmark, compare with benchstat

**Risk**: Low. Pure code motion.

### Task 2: Extract `collectDeleteUpdate` helper

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

The `branchBefore → CollectUpdate(delete) + cache evict` pattern appears in both delete and propagate cases. Extract:

```go
func (hph *HexPatriciaHashed) collectDeleteUpdate(updateKey []byte, row int) error {
    if hph.branchBefore[row] {
        _, err := hph.branchEncoder.CollectUpdate(hph.ctx, updateKey, 0, hph.touchMap[row], 0, RetrieveCellNoop)
        if err != nil {
            return fmt.Errorf("failed to encode leaf node update: %w", err)
        }
        if hph.cache != nil {
            hph.cache.EvictBranch(updateKey)
        }
    }
    return nil
}
```

- [ ] Extract the helper
- [ ] Replace both call sites (delete case line ~2094, propagate case line ~2137)
- [ ] Note: the delete case has the cache eviction, propagate case does not — verify whether propagate should also evict (check git blame for when cache eviction was added). If not, add a `evictCache bool` parameter or keep them separate.
- [ ] Run tests: `go test ./execution/commitment/... -count=1 -short`

**Risk**: Low. The two call sites use identical arguments except possibly the cache eviction.

### Task 3: Extract `foldDelete` method

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

```go
func (hph *HexPatriciaHashed) foldDelete(row int, nibble, upDepth int16, upCell *cell, updateKey []byte) error
```

- [ ] Move the `updateKindDelete` case body into this method
- [ ] Replace the case body with a single call: `if err := hph.foldDelete(row, nibble, upDepth, upCell, updateKey); err != nil { return err }`
- [ ] Run tests: `go test ./execution/commitment/... -count=1 -short`
- [ ] Run benchmark, compare with benchstat

**Risk**: Low. Pure extraction — no logic changes.

**Performance note**: Go will likely inline this since the method body is straightforward. Even if not inlined, one function call per fold() invocation is negligible.

### Task 4: Extract `foldPropagate` method

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

```go
func (hph *HexPatriciaHashed) foldPropagate(row int, nibble, upDepth, depth int16, upCell *cell, updateKey []byte) error
```

- [ ] Move the `updateKindPropagate` case body into this method
- [ ] Replace the case body with a single call
- [ ] Run tests: `go test ./execution/commitment/... -count=1 -short`
- [ ] Run benchmark, compare with benchstat

**Risk**: Low. Pure extraction.

### Task 5: Extract `prepareBranchCells` — cell iteration + state loading

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

The loop at lines 2166–2206 iterates `afterMap`, handles memoization, loads state, and computes `totalBranchLen`. This is a self-contained preparation step before keccak2 hashing:

```go
// prepareBranchCells iterates afterMap cells, drops stale memoized hashes,
// loads state from DB where needed, and returns the total RLP-encoded branch length.
func (hph *HexPatriciaHashed) prepareBranchCells(row int, depth int16) (totalBranchLen int16, err error)
```

- [ ] Extract the loop (lines 2166–2206) into this method
- [ ] The `nibblesLeftAfterUpdate` value is needed for the initial `totalBranchLen` calculation — pass it as a parameter or compute it inside from `afterMap[row]`
- [ ] Replace the loop in the branch case with a single call
- [ ] Run tests: `go test ./execution/commitment/... -count=1 -short`
- [ ] Run benchmark — this is the most performance-sensitive extraction since it touches the inner cell iteration loop. Verify zero regression.

**Risk**: Medium. This loop accesses many fields (`afterMap`, `touchMap`, `grid`, `depthsToTxNum`, `hadToLoadL`). Ensure no subtle aliasing or ordering issues.

### Task 6: Unify keccak2 feeding between deferred and non-deferred paths

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

Both paths iterate `afterMap` cells and feed hashes into `keccak2`. Extract:

```go
// feedBranchHashesToKeccak iterates afterMap cells, computes cell hashes via
// computeCellHash, writes them (and 0x80 for empty slots) into keccak2.
func (hph *HexPatriciaHashed) feedBranchHashesToKeccak(row int, depth int16) error
```

Then the deferred/non-deferred fork becomes:
```go
hph.feedBranchHashesToKeccak(row, depth)
if hph.branchEncoder.DeferUpdatesEnabled() {
    hph.branchEncoder.CollectDeferredUpdate(...)
} else {
    cellGetter := hph.createCellGetter(...)
    hph.branchEncoder.CollectUpdate(...)
}
```

- [ ] Verify that both paths produce identical keccak2 state — the deferred path calls `computeCellHash` directly; the non-deferred path calls it via `createCellGetter`. Both write the same bytes to keccak2.
- [ ] **Critical check**: in the non-deferred path, `createCellGetter` also writes trailing empty cells inside `CollectUpdate/EncodeBranch` (for nibbles after `lastNibble`). The deferred path writes all 17 slots explicitly. After unification, the trailing empty cells must still be written. The non-deferred path's post-`CollectUpdate` loop (lines 2259-2266) handles this — it must be preserved.
- [ ] If the keccak2 writes are NOT identical between paths (timing differs — non-deferred interleaves encoding with hashing), document why and keep them separate. Do NOT force unification if it changes keccak2 input ordering.
- [ ] Run tests: `go test ./execution/commitment/... -count=1 -short`
- [ ] Run benchmark, compare with benchstat

**Risk**: High. The keccak2 feeding order MUST be identical — any reordering produces different hashes and breaks consensus. Carefully verify by adding a temporary assertion that compares the keccak2 output between old and new paths before committing.

**If unification is not safe**: Skip this task. The deferred and non-deferred paths may intentionally feed keccak2 at different times (during vs before encoding). In that case, keep them as separate code paths within `foldBranch` and accept the duplication.

### Task 7: Extract `foldBranch` method

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

```go
func (hph *HexPatriciaHashed) foldBranch(row int, nibble, upDepth, depth int16, upCell *cell, updateKey []byte) error
```

This is the largest extraction. After Tasks 5 and 6, the branch case body should already be smaller. This task moves whatever remains.

- [ ] Move the `updateKindBranch` case body (including calls to `prepareBranchCells` and the deferred/non-deferred fork) into this method
- [ ] The upCell extension/hash finalization (lines 2268-2284) goes inside this method
- [ ] Replace the case body with a single call
- [ ] Run tests: `go test ./execution/commitment/... -count=1 -short`
- [ ] Run benchmark, compare with benchstat

**Risk**: Medium. Large code motion, but by this point the individual pieces have already been validated.

### Task 8: Final verification

- [ ] Verify fold() is ≤60 lines
- [ ] Run full test suite: `go test ./execution/commitment/... -count=1`
- [ ] Run benchmarks: `go test ./execution/commitment/ -bench=. -benchmem -count=5 > bench-after.txt`
- [ ] Compare: `benchstat bench-baseline.txt bench-after.txt` — verify no regression
- [ ] Run lint: `make lint` (repeat until clean)
- [ ] Run build: `make erigon integration`
- [ ] Move plan to `docs/plans/completed/`

## Technical Details

### Expected fold() after refactoring

```go
func (hph *HexPatriciaHashed) fold() (err error) {
    if hph.activeRows == 0 {
        return errors.New("cannot fold - no active rows")
    }

    row := hph.activeRows - 1
    upRow := row - 1
    depth := hph.depths[row]
    updateKeyLen := hph.currentKeyLen
    updateKey := nibbles.HexToCompact(hph.currentKey[:updateKeyLen])
    defer func() { hph.depthsToTxNum[depth] = 0 }()

    var upCell *cell
    var nibble, upDepth int16
    if row == 0 {
        upCell = &hph.root
    } else {
        upDepth = hph.depths[upRow]
        nibble = int16(hph.currentKey[upDepth-1])
        upCell = &hph.grid[upRow][nibble]
    }

    if hph.trace {
        // setup trace logging
    }

    switch afterMapUpdateKind(hph.afterMap[row]) {
    case updateKindDelete:
        err = hph.foldDelete(row, nibble, upDepth, upCell, updateKey)
    case updateKindPropagate:
        err = hph.foldPropagate(row, nibble, upDepth, depth, upCell, updateKey)
    default: // updateKindBranch
        err = hph.foldBranch(row, nibble, upDepth, depth, upCell, updateKey)
    }
    if err != nil {
        return err
    }

    // Common epilogue
    hph.activeRows--
    hph.currentKeyLen = max(upDepth-1, 0)
    return nil
}
```

### Performance-sensitive areas

| Area | Concern | Mitigation |
|------|---------|------------|
| `prepareBranchCells` loop | Inner loop over all cells in a row; called millions of times | Ensure no extra allocations; pass `afterMap` directly, don't copy grid row |
| `feedBranchHashesToKeccak` | Keccak state accumulation; ordering is consensus-critical | Verify byte-identical keccak2 input; add temporary assertion in testing |
| Method call overhead | `foldDelete`/`foldPropagate`/`foldBranch` called once per fold | Negligible — one call per fold, each does substantial work (DB reads, hashing) |
| Parameter passing | 6 parameters to each fold* method | All value types or pointers already on stack; no heap escape |
| `updateKey` slice | Allocated once per fold via `nibbles.HexToCompact` | Already exists; no change |

### Method parameter design

The extracted methods receive only what they need from fold()'s local variables:

```
foldDelete(row int, nibble int16, upDepth int16, upCell *cell, updateKey []byte)
foldPropagate(row int, nibble int16, upDepth int16, depth int16, upCell *cell, updateKey []byte)
foldBranch(row int, nibble int16, upDepth int16, depth int16, upCell *cell, updateKey []byte)
prepareBranchCells(row int, depth int16, nibblesLeft int) (int16, error)
feedBranchHashesToKeccak(row int, depth int16) error  // if unification is safe
```

**Why not a foldContext struct?** A struct would require either heap allocation (escape analysis may not prove it stays on stack if it's passed to sub-methods) or careful coding to keep it stack-allocated. The parameter lists are short enough (5–6 args) that direct passing is simpler and guaranteed zero-alloc. If a future refactoring adds more parameters, revisit.

### Keccak2 ordering verification strategy

Before Task 6, add a temporary test that:
1. Processes identical updates with `DeferUpdatesEnabled() = true` and `= false`
2. After each fold(), captures `keccak2.Sum(nil)` (or the resulting `upCell.hash`)
3. Asserts the hashes are identical

If this test fails, the deferred and non-deferred paths produce different keccak2 inputs, and Task 6 (unification) should be skipped.

### What this refactoring does NOT touch

- `createCellGetter` — stays as-is (separate initiative)
- `CollectUpdate` / `CollectDeferredUpdate` / `EncodeBranch` — no interface changes
- `loadStateIfNeeded` — already extracted, stays as-is
- `computeCellHash` / `computeCellHashLen` — stays as-is
- `followAndUpdate` / `Process` — callers unchanged
- Trace logging content — moved to new methods, but messages unchanged
