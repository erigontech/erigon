# Eliminate merge from fold→encode→write hot path

## Overview
Encode complete BranchData (all `afterMap` cells) during trie fold instead of encoding only touched cells and merging with previous data. This removes the `BranchMerger.Merge()` call and its allocations from the commitment hot path.

**Key insight:** `hashRow()` already iterates ALL `afterMap` cells and extracts `cellEncodeData` for every one. Currently only touched cells (`bitmap = touchMap & afterMap`) are encoded, producing incomplete BranchData that must be merged with prev. By encoding all cells (`bitmap = afterMap`), we produce complete BranchData that replaces prev directly.

## Context (from discovery)

**Files involved:**
- `execution/commitment/commitment.go` — `BranchEncoder`, `CollectUpdate` (line 550), `encodeDeferredUpdate` (line 389), `ApplyDeferredBranchUpdates` (line 439), worker pools (line 433-434)
- `execution/commitment/hex_patricia_hashed.go` — `foldBranch` (line 1912), `hashRow` (line 2018)
- `execution/commitment/commitment_bench_test.go` — existing benchmarks (no direct `EncodeBranch` benchmark exists)
- `execution/commitment/hex_patricia_hashed_test.go` — multi-step UniqueRepresentation tests
- `execution/commitment/hex_concurrent_patricia_hashed_test.go` — multi-batch comparison tests

**What stays unchanged:**
- `BranchMerger` and `MergeHexBranches` methods — used by snapshot merging, domain compaction
- `branchBefore` flag — still used for touchMap upward propagation (lines 1913-1921)
- prev fetch in `CollectUpdate` — needed for equality check + `PutBranch` prevData parameter

**Merger field usage (confirmed):**
- `be.merger` is only used at line 582 in `CollectUpdate`
- `encodeDeferredUpdate` receives merger as parameter from `workerMergerPool`
- No external callers of `encodeDeferredUpdate` outside commitment.go

## Development Approach
- **testing approach**: code changes first, then verify with existing multi-step tests + add new benchmarks
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: all existing multi-step commitment tests must pass before proceeding** — these tests exercise the prev→encode→write→read-as-prev→encode cycle that this change affects
- run tests after each change

## Testing Strategy
- **unit tests**: existing multi-step tests are the primary correctness validation — they compare sequential single-update roots with batch roots
- **benchmarks**: new benchmarks to quantify the improvement (encode-complete vs encode-partial+merge)
- **critical multi-step tests** (exercise the full prev→encode→write→read-as-prev→encode cycle):
  - `Test_HexPatriciaHashed_BrokenUniqueReprParallel` (6 phases, sequential vs batch)
  - `Test_HexPatriciaHashed_ProcessUpdates_UniqueRepresentationInTheMiddle` (31 updates, checkpoint at step 6)
  - `Test_HexPatriciaHashed_ProcessUpdates_UniqueRepresentation_AfterStateRestore` (restore at midpoint)
  - `Test_HexPatriciaHashed_DeferredBranchUpdates` (deferred vs normal path)
  - `Test_HexPatriciaHashed_UniqueRepresentation` / `UniqueRepresentation2`
  - `TestCompareRoots_MultiBatch_ThreePhases` (create/update/delete)
  - `TestCompareRoots_MultiBatch_RepeatedSingleNibble` (4 batches, concentrated nibble)
  - `TestCompareRoots_MultiBatch_AlternatingConcentration` (4 batches, varying density)
  - `TestCompareRoots_MultiBatch_CreateThenDeleteAll`
  - `Test_HexPatriciaHashed_RestoreAndContinue`
  - `Test_HexPatriciaHashed_Sepolia`

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix

## Implementation Steps

### Task 1: Add baseline benchmarks before any code changes

**Files:**
- Modify: `execution/commitment/commitment_bench_test.go`

- [x] add `BenchmarkEncodeBranch` — encode with bitmap=1 cell vs bitmap=afterMap (2, 8, 16 cells). Use `cellEncodeData` fixtures with realistic field sizes (hash=32, accountAddr=20, extension=0-4)
- [x] add `BenchmarkCollectUpdate_EncodeMerge` — end-to-end benchmark: encode partial + merge with prev. Setup: create a prev BranchData with N cells, then benchmark encoding 1 touched cell + merging. Vary N=2,8,16
- [x] run benchmarks, record baseline numbers as comments in test file
- [x] run `go test -run TestHexPatriciaHashed -count=1 ./execution/commitment/...` to confirm tests pass before changes

### Task 2: Change foldBranch to always encode complete BranchData

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [x] in `foldBranch` (line 1922): change `bitmap := hph.touchMap[row] & hph.afterMap[row]` to `bitmap := hph.afterMap[row]`
- [x] remove the `branchBefore` bitmap adjustment block (lines 1923-1927) — no longer needed since bitmap is always afterMap. Keep the touchMap adjustment (`hph.touchMap[row] |= hph.afterMap[row]`) only if it's needed for upward propagation correctness; if not, remove entirely
- [x] pass `(bitmap, hph.afterMap[row], hph.afterMap[row], &cellData)` to `CollectUpdate`/`CollectDeferredUpdate` — touchMap=afterMap so the stored BranchData is marked complete
- [x] run multi-step tests: `go test -run "UniqueRepresentation|BrokenUniqueRepr|RestoreAndContinue|DeferredBranch|Sepolia" -count=1 ./execution/commitment/...`
- [x] run concurrent multi-batch tests: `go test -run "CompareRoots_MultiBatch" -count=1 ./execution/commitment/...`

### Task 3: Remove merge from CollectUpdate (immediate path)

**Files:**
- Modify: `execution/commitment/commitment.go`

- [x] in `CollectUpdate` (line 578-586): remove the `merger.Merge(prev, update)` call and surrounding merge block. Keep the equality check (`bytes.Equal(prev, update)` → skip write)
- [x] remove `merger` field from `BranchEncoder` struct (line 332)
- [x] remove merger initialization from `NewBranchEncoder` (line 345)
- [x] run multi-step tests: `go test -run "UniqueRepresentation|BrokenUniqueRepr|RestoreAndContinue|DeferredBranch|Sepolia" -count=1 ./execution/commitment/...`

### Task 4: Remove merge from encodeDeferredUpdate (deferred path)

**Files:**
- Modify: `execution/commitment/commitment.go`

- [x] in `encodeDeferredUpdate` (line 389): remove `merger *BranchMerger` parameter and the `merger.Merge(upd.prev, update)` call (line 406). Keep equality check
- [x] remove `workerMergerPool` (line 434)
- [x] update all callers in `ApplyDeferredBranchUpdates` (lines 454, 460, 496) — remove merger acquisition and passing
- [x] run deferred-specific test: `go test -run "DeferredBranch" -count=1 ./execution/commitment/...`
- [x] run full commitment test suite: `go test -count=1 ./execution/commitment/...`

### Task 5: Add post-change benchmarks and compare

**Files:**
- Modify: `execution/commitment/commitment_bench_test.go`

- [x] add `BenchmarkCollectUpdate_EncodeDirect` — same setup as Task 1's encode+merge benchmark but exercising the new code path (encode complete, no merge)
- [x] run both old-baseline and new benchmarks: `go test -bench "BenchmarkEncodeBranch|BenchmarkCollectUpdate" -benchmem -count=5 ./execution/commitment/...`
- [x] compare results with `benchstat` if available, document improvement in allocs/op and ns/op

### Task 6: Verify acceptance criteria

- [x] all existing commitment tests pass: `go test -count=1 ./execution/commitment/...`
- [x] `make lint` passes
- [x] `make erigon integration` builds successfully
- [x] benchmark shows reduction in allocations on the encode+write path
- [x] move this plan to `docs/plans/completed/`

## Technical Details

**Current hot path (CollectUpdate):**
```
foldBranch → bitmap = touchMap & afterMap (partial)
  → EncodeBranch(bitmap, touchMap, afterMap, cells) → incomplete BranchData
  → fetch prev from cache/DB
  → merger.Merge(prev, update) → complete BranchData  ← ELIMINATED
  → PutBranch(prefix, merged, prev)
```

**New hot path:**
```
foldBranch → bitmap = afterMap (complete)
  → EncodeBranch(afterMap, afterMap, afterMap, cells) → complete BranchData
  → fetch prev from cache/DB
  → if bytes.Equal(prev, update) → skip
  → PutBranch(prefix, update, prev)
```

**Why this is safe:** `hashRow()` iterates all `afterMap` cells and calls `cellEncodeDataFromCell()` for each. `prepareBranchCells()` loads state from DB for all `afterMap` cells before `hashRow()` runs. So all cell data is fully populated.

**touchMap semantics in stored BranchData:** Setting touchMap=afterMap means `IsComplete()` returns true. Future merges (by other callers, not this hot path) reading this as prev will see `bitmap1 = touchMap & afterMap = afterMap`, correctly finding data for all cells.
