# Parallel Hex Patricia Hashed Commitment

## Overview

Parallelize `HexPatriciaHashed.Process` so each account's storage subtrie (and groups of accounts) can be computed concurrently. Build a new `ParallelPatriciaHashed` sibling type that pushes split-points to arbitrary depths in the trie — not just depth 1 as the existing `ConcurrentPatriciaHashed` PoC does.

**Problem**: Commitment computation is single-threaded above depth 1. On bloatnet-style workloads (~250 accounts updating ~9M storage slots), one nibble bucket can dominate runtime and leave 15 CPU cores idle for most of the computation.

**Approach**: During `TouchPlainKey`, build a path-compressed prefix trie of touched hashed-keys. After all touches arrive, walk the trie to identify split-points (forks with subtree size ≥ 32 touched keys) and emit a flat queue of leaf-tasks. Workers process leaves concurrently. At fold time, each worker checks the current prefix against a split-point map; on hit, it deposits its produced cell into a shared per-split-point slot and either exits or — if it's the last sibling to arrive — continues folding upward through the shared state.

**Out of scope for v1**: `GenerateWitness` stays sequential.

## Context (from brainstorm + discovery)

**Files involved (untouched)**:
- `execution/commitment/hex_patricia_hashed.go` — existing `HexPatriciaHashed`, `Process`, `fold`, `foldMounted`, `mountTo`. Leave alone; we mount onto and re-use its `fold` machinery via a fresh instance per worker.
- `execution/commitment/hex_concurrent_patricia_hashed.go` — existing depth-1 PoC (`ConcurrentPatriciaHashed`). Keep alive for compatibility.

**Files modified**:
- `execution/commitment/commitment.go` — add `ModeParallel`, add `Updates.parallel *parallelUpdate` field, new `TouchPlainKey` case, `Close` plumbing.
- `execution/commitment/verify_test.go` — add `ModeParallel` arm to fuzz harness; add deletion + bloatnet edge-case tests.

**New files**:
- `execution/commitment/prefix_trie.go` — `prefixNode`, arena, insert.
- `execution/commitment/parallel_update.go` — `parallelUpdate`, `splitPoint`, `leafTask`, `Prepare`.
- `execution/commitment/parallel_patricia_hashed.go` — `ParallelPatriciaHashed`, worker pool, barrier protocol.

**Patterns we reuse from existing code**:
- `maphash.NonConcurrentMap[V]` — same generic map type used by `BranchEncoder.pendingPrefixes`. Freeze-once / read-many fits our split-point map.
- `DeferredBranchUpdate.cells [16]cellEncodeData` layout (commitment.go:225-239) — identical shape to what we need for split-point cell deposits. We borrow the layout (memcpy compatibility) but allocate `splitPoint`s separately, since their lifetime differs.
- `sync.Pool` pattern from `deferredUpdatePool` (commitment.go:242-246) — apply the same pattern to a `splitPointPool` if profiling shows allocation pressure (optional optimization).
- `BranchEncoder.SetDeferUpdates(true)` + `TakeDeferredUpdates()` + `ApplyDeferredBranchUpdates` (commitment.go:355-450) — each worker uses deferred mode; orchestrator merges at end.
- Existing `Warmuper` (warmuper.go) — pre-warms DB branch reads along split-point ancestor paths.
- ETL collectors per nibble (`Updates.nibbles[16]`) from the PoC — keep this routing in TouchPlainKey for ModeParallel.

**Dependencies**: no new external deps. All in-tree.

## Development Approach

- **testing approach**: Regular (code first, then tests in same task). The data structures (prefix trie, splitPoint) lend themselves to direct unit tests right after implementation. Fuzz testing comes once the orchestration is end-to-end.
- **CARDINAL CORRECTNESS RULE**: every end-to-end test of `ParallelPatriciaHashed` must drive the same update set through sequential `HexPatriciaHashed` (ModeDirect) and assert byte-equal root hashes. Same-root-as-sequential is the only definition of correctness. Use the `assertEquivalentRoot` helper introduced in Task 6 — every subsequent end-to-end test calls it. Tests that produce a root in one mode alone (without comparing) do not count.
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
  - tests are not optional — they are a required part of the checklist
  - cover both success and error scenarios
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run `make lint` after each task; the linter is non-deterministic, run until clean
- maintain backward compatibility — existing `ModeDirect` and `ModeUpdate` codepaths must remain bit-identical
- Copyright year 2026 on new files

## Testing Strategy

**Cardinal correctness rule** (applies to every test that exercises end-to-end commitment in ModeParallel):

> Every test that drives a non-trivial update set through `ParallelPatriciaHashed` MUST also drive the same update set through sequential `HexPatriciaHashed` (ModeDirect) and assert byte-equal root hashes. This includes unit tests, edge-case tests, fuzz tests, and integration tests — not just the dedicated equivalence harness. Same-root-as-sequential is the only definition of correctness for this work.

This eliminates the risk of bugs that only manifest in specific update shapes (e.g. a particular split-point fanout, a deletion pattern, a depth combination). A test that produces *some* root without comparing to sequential is worthless for our purposes.

**Concrete implementation**: introduce a small helper in test code:

```go
// assertEquivalentRoot drives `updates` through both modes and asserts roots match.
// Returns the (shared) root hash so callers can do additional assertions.
func assertEquivalentRoot(t *testing.T, ctx PatriciaContext, updates []touch) []byte
```

Every ModeParallel test calls this helper instead of computing a root via one mode alone.

**Test categories:**

- **structure tests** (no end-to-end commitment, no equivalence assertion needed): prefix-trie insertion mechanics, arena reuse, splitPoint counter init, Prepare DFS output shape. These verify data-structure invariants in isolation.
- **end-to-end tests** (every one must call `assertEquivalentRoot`): worker pool, barrier protocol, deletion patterns, bloatnet shape, empty updates, single-account-many-storage, integration via feature flag.
- **fuzz**: `verify_test.go` `ModeParallel` arm — random update sets driven through both modes, equal-root assertion. This is automated coverage of update shapes we didn't anticipate.
- **race detector**: `go test -race` on every barrier-protocol test — the splitPoint cell deposit + atomic arrival is the highest-risk concurrency surface.
- **bench**: optional — comparison bench between sequential and parallel modes on bloatnet-shape workload. Evidence only, not a CI assertion.
- no e2e tests apply (library code, no UI surface).

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

**Three-layer architecture:**

1. **Build layer** (during `TouchPlainKey`): incrementally inserts each hashed key into a path-compressed prefix trie. Maintains `subtreeCount` along each visited node so freeze can read it in O(1). Memory: ~80B/node × ~2N nodes worst case — accepted up to ~1.5GB for bloatnet.

2. **Prepare layer** (sequential, runs at start of `ParallelPatriciaHashed.Process`): DFS over the prefix trie. At every internal node with `popcount(bitmap) >= 2 && subtreeCount >= 32`, emit a `splitPoint`; otherwise collapse the subtree into a single `leafTask` and append to `leafQueue`. For each emitted splitPoint, pre-load DB cells via `ctx.Branch()` (already warmed by Warmuper) so untouched-nibble slots are populated. Result: `splitMap maphash.NonConcurrentMap[*splitPoint]` + `leafQueue []leafTask`.

3. **Execute layer** (parallel): worker pool drains `leafQueue`. Each worker uses a fresh `HexPatriciaHashed` (from a sync.Pool) with deferred branch updates enabled. After processing its leaf-range, the worker folds upward. Each fold step consults `splitMap`:
   - **Miss**: ordinary `hph.fold()`.
   - **Hit**: produce the cell, write it into `sp.cells[nib]`, set bit. `sp.arrived.Add(-1)`. If result > 0 → exit (append deferred updates to shared slice). If result == 0 → this worker is last-finisher: copy all 16 `sp.cells` into its grid, restore `currentKey/depths/touchMap/afterMap`, then continue fold loop.

The last-finisher of the topmost split-point computes the root hash. Orchestrator waits via `errgroup`. Deadlock is impossible because every queued leaf-task either deposits exactly once or signals an error.

## Technical Details

### Data structures

**`prefixNode`** (in `prefix_trie.go`):
```go
type prefixNode struct {
    ext          []byte         // compressed nibble path from parent (each byte = one nibble 0x00..0x0F)
    bitmap       uint16         // which children present
    children     []*prefixNode  // dense, len = popcount(bitmap)
    subtreeCount uint32         // total touched keys in this subtree (incremented on each insert along path)
}
```

Allocated from a bump arena (slabs of 16K nodes) to amortize allocation cost. The arena is reset on each `Updates.Reset()`.

**Insert algorithm** (`(t *prefixTrie) Insert(hashedKey []byte)`):
1. Start at root, current depth = 0.
2. Walk: at current node, compare `hashedKey[depth:depth+len(ext)]` with `ext`.
3. **Full match + more key remains**: pick child by `hashedKey[depth+len(ext)]`. If bit not set in bitmap, append new leaf child. Else descend.
4. **Partial match at offset k**: split current node — create intermediate node with shared prefix `ext[:k]`, current node keeps `ext[k+1:]`, sibling leaf gets `hashedKey[...]`. Re-link parent.
5. **Full match, no key remains**: increment `subtreeCount` at the terminal node.
6. On every visited node, bump `subtreeCount`.

**`splitPoint`** (in `parallel_update.go`):
```go
type splitPoint struct {
    prefix        []byte               // hashed-key prefix in nibble form
    touchedBitmap uint16               // workers will arrive for these nibbles
    dbBitmap      uint16               // nibbles existing in DB at this prefix (from ctx.Branch)
    cells         [16]cellEncodeData   // pre-populated for non-touched dbBitmap slots; workers overwrite their slots
    arrived       atomic.Int32         // init = popcount(touchedBitmap) - 1
    branchBefore  bool                 // true iff dbBitmap != 0
}
```

Cell layout matches `DeferredBranchUpdate.cells` for memcpy-compatibility.

**`leafTask`** (in `parallel_update.go`):
```go
type leafTask struct {
    prefix   []byte  // hashed-key prefix shared by all keys in this task
    keyCount uint32  // for scheduling — bigger tasks first
}
```

The ancestor chain of split-points is discovered implicitly by the worker's fold loop via `splitMap.Get(foldedPrefix)` at each fold step — no `enclosingSP` field is needed. Earlier drafts had one; it's removed to keep `splitMap` the single source of truth.

**`parallelUpdate`** (in `parallel_update.go`):
```go
type parallelUpdate struct {
    trieRoot   *prefixNode
    arena      []prefixSlab           // slabs of [16384]prefixNode each
    arenaNext  int                    // bump index within current slab

    splitMap   *maphash.NonConcurrentMap[*splitPoint]   // populated by Prepare
    leafQueue  []leafTask                               // populated by Prepare

    // Shared during Process for deferred-branch merging
    deferredMu       sync.Mutex
    deferredCombined []*DeferredBranchUpdate
}
```

### Worker fold-time barrier protocol

Inside the worker after its leaf-range processing finishes:

```
for hph.activeRows > 0 {
    if err := ctx.Err(); err != nil { return err }

    // Determine the prefix the next fold step will produce-into
    foldedPrefix := hph.currentKey[:hph.depths[hph.activeRows-1]]

    if sp, hit := splitMap.Get(string(foldedPrefix)); hit {
        // Produce the cell via existing fold machinery (mounted-style fold)
        myCell, nib, err := hph.produceCellForBarrier()
        if err != nil { return err }

        sp.cells[nib] = myCell                    // memcpy via cellEncodeData layout
        // bitmap-set under release semantics happens implicitly via atomic.Add below

        remaining := sp.arrived.Add(-1)
        if remaining > 0 {
            // Sibling still working — exit cleanly
            pu.appendDeferred(hph.branchEncoder.TakeDeferredUpdates())
            hph.Reset()
            pool.Put(hph)
            return nil
        }
        // Last-finisher — load all siblings into our grid, continue
        loadSiblingsIntoGrid(hph, sp)
        continue
    }
    // Not a split-point — normal fold
    if err := hph.fold(); err != nil { return err }
}
```

`loadSiblingsIntoGrid(hph, sp)` reconstructs the worker's state from scratch — the worker's existing `hph` has folded past this depth from one branch only, so we overwrite the relevant row entirely:

```
copy(hph.currentKey[:], sp.prefix)
hph.currentKeyLen = int16(len(sp.prefix))
row := <fixed row index — see note below>
hph.depths[row]       = int16(len(sp.prefix))
hph.depthsToTxNum[row] = 0       // reset; will be re-filled by fold metrics path
hph.activeRows         = row + 1

// Zero the row first, then populate
for n := 0; n < 16; n++ { hph.grid[row][n].reset() }
for bit := sp.dbBitmap | sp.touchedBitmap; bit != 0; {
    nib := bits.TrailingZeros16(bit)
    hph.grid[row][nib] = sp.cells[nib]   // memcpy (cellEncodeData layout)
    bit &^= 1 << nib
}
hph.touchMap[row] = sp.touchedBitmap
hph.afterMap[row] = 0
for n := 0; n < 16; n++ {
    if !hph.grid[row][n].IsEmpty() { hph.afterMap[row] |= 1 << n }
}
hph.branchBefore[row] = sp.branchBefore
```

**Note on row indexing**: rows in `hph.grid` are indexed by their stack-depth in `activeRows`, not by trie-depth. After a sequential `fold` chain, `row` for a split-point at trie-depth D is the position in the active-rows stack — typically `0` when D is the topmost (root) split-point, or the depth-step count from root otherwise. Task 7 must determine this index precisely by inspecting the existing `fold`/`unfold` indexing convention; the freeze pass can pre-compute it per splitPoint if needed. ⚠️ This is the single most subtle indexing concern in the implementation — call it out in code review.

**Memory model — why the last-finisher sees all sibling writes**:

`sync/atomic` operations on a single memory location form a single total order (Go memory model, "atomic operations" section). For our split-point's `arrived atomic.Int32`:

1. Each writer goroutine performs `sp.cells[nib] = myCell` *before* `sp.arrived.Add(-1)`. This program order, combined with `Add`'s atomic semantics, means the N-th (last) `Add` synchronizes-with all prior `Add`s on the same variable.
2. The last-finisher's `Add` returns `0`. Because of (1), every prior writer's `sp.cells[nib] = ...` store happens-before this final `Add`, and therefore happens-before the last-finisher's subsequent reads of `sp.cells[*]`.
3. Non-last writers exit without reading `sp.cells`, so no cross-writer synchronization is required.

This is sufficient under Go ≥1.19's strengthened atomic semantics. No explicit fences or mutex needed.

### Untouched-nibble pre-population (the critical subtlety)

The prefix trie only knows about *touched* keys, but the on-disk branch at a split-point's prefix may have additional nibbles populated by previously committed (untouched) keys. The last-finisher's fold must see those untouched cells, or the resulting branch hash will be wrong.

In `Prepare`, for each emitted splitPoint:
```go
branch, _, err := ctx.Branch(nibbles.HexToCompact(sp.prefix))
if err != nil { return err }
if len(branch) > 0 {
    touchMap, afterMap, row, err := branch.decodeCells()
    sp.dbBitmap = afterMap
    sp.branchBefore = true
    for bit := afterMap; bit != 0; {
        nib := bits.TrailingZeros16(bit)
        sp.cells[nib] = *row[nib]    // pre-populate
        bit &^= 1 << nib
    }
}
sp.arrived.Store(int32(bits.OnesCount16(sp.touchedBitmap) - 1))
```

Workers overwrite only their own `sp.cells[nib]` slots; untouched slots survive into the last-finisher's grid.

### Leaf-task → key-range delivery

Each `leafTask.prefix` defines a contiguous span in `Updates.nibbles[leafTask.prefix[0]]`. ETL collectors stream sequentially — they don't support seek-by-prefix. Two practical options:

- **First cut (used in this plan)**: group leafTasks by `prefix[0]` and stream the corresponding `nibbles[i]` collector exactly *once* per nibble bucket, dispatching each key to the matching leafTask (a small in-memory lookup over the group's prefixes). One scan per nibble bucket regardless of how many leafTasks share that nibble. Acceptable overhead for v1.
- **Avoid**: per-leaf full scans of `nibbles[prefix[0]]`. With N leaves sharing a nibble bucket that's N full passes — quadratic.

Follow-up optimization (out of scope): per-leaf chunk files persisted during Prepare, removing the dispatch step entirely.

### Resilience guarantees

- Every queued `leafTask` runs its fold-drain loop until it either (a) deposits at the first splitPoint it encounters on its ancestor chain (and either exits or transitions to last-finisher continuing further deposits), or (b) returns an error. Deposit-or-error is exhaustive — no "skip deposit" path.
- The ancestor chain is traversed implicitly: each successful fold step that crosses a split-point triggers another `splitMap.Get` hit. A worker may deposit at multiple split-points during its lifetime (the last-finisher of split-point P may continue folding upward, become a writer at P's ancestor split-point P', etc.).
- Workers run inside an `errgroup`. On error, `ctx.Cancel()` propagates; surviving workers check `ctx.Err()` at the top of each fold step and bail. They do NOT deposit on error path — but this is safe because the orchestrator waits on `errgroup.Wait()`, not on `arrived` counters.
- No goroutine ever blocks waiting on `arrived`. The counter is a routing signal only.
- Deadlock is impossible by construction.

### Memory budget

- Prefix-trie nodes for 9M-key bloatnet: ~18M nodes × ~80B = ~1.5GB peak. Acceptable per brainstorm.
- splitMap for bloatnet: ~30K split-points × ~3.2KB worst (cells[16] dense) = ~100MB. Optimizable to ~10MB with sparse cell storage if needed.
- Per-worker `HexPatriciaHashed` from sync.Pool: existing size × NumCPU workers.
- ETL collectors: disk-backed via existing `SortAndFlushInBackground`.

### Concurrency hot paths

- `splitMap.Get`: read-only after Prepare, no synchronization needed.
- `sp.cells[nib] = ...`: each `nib` written by exactly one worker; no race.
- `sp.arrived.Add(-1)`: atomic, provides happens-before for last-finisher.
- `pu.deferredCombined`: short mutex held during slice append.
- `branchEncoder.deferred`: per-worker; merged centrally on exit.

## What Goes Where

- **Implementation Steps** (checkboxes below): all in-tree code, tests, fuzz, lint.
- **Post-Completion** (no checkboxes): integration wiring once feature-flag default flips, perf benchmark report, follow-up optimization PRs.

## Implementation Steps

### Task 1: Prefix trie data structure + arena

**Files:**
- Create: `execution/commitment/prefix_trie.go`
- Create: `execution/commitment/prefix_trie_test.go`

- [x] define `prefixNode` struct (ext, bitmap, children, subtreeCount) in `prefix_trie.go`
- [x] define `prefixSlab` type (`[16384]prefixNode`) and arena helpers (`newPrefixArena`, `allocNode`, `resetArena`)
- [x] implement `prefixTrie` wrapper with `Insert(hashedKey []byte)` — handles full-match descent, partial-extension split, fresh child append
- [x] implement `Walk(fn func(prefix []byte, node *prefixNode))` DFS used later by Prepare
- [x] add helpers: `popcount(node) int`, `childIndex(node, nib) (idx int, ok bool)`
- [x] write unit tests: empty trie, single insert, two inserts diverging at depth 0, divergence inside extension (partial split), divergence at end of extension (descend into existing child), duplicate insert (no growth), deep insert (~128 nibbles)
- [x] write tests for `subtreeCount` accumulation: after N inserts on shared prefix, root.subtreeCount == N; mixed-prefix counts propagate correctly
- [x] write test for arena reuse: insert, reset, insert again, no leaks (validate node count)
- [x] `go test ./execution/commitment/ -run TestPrefixTrie` passes
- [x] `make lint` clean

### Task 2: splitPoint + leafTask + parallelUpdate skeleton

**Files:**
- Create: `execution/commitment/parallel_update.go`
- Create: `execution/commitment/parallel_update_test.go`

- [x] define `splitPoint` struct (prefix, touchedBitmap, dbBitmap, cells, arrived, branchBefore) in `parallel_update.go`
- [x] define `leafTask` struct (prefix, keyCount) — no `enclosingSP`; ancestor chain is discovered via `splitMap.Get` during fold
- [x] define `parallelUpdate` struct (trieRoot, arena, splitMap, leafQueue, deferredMu, deferredCombined)
- [x] implement `newParallelUpdate()` constructor wiring the maphash and arena
- [x] implement `(pu *parallelUpdate) Insert(hashedKey []byte)` — thin wrapper over prefix-trie Insert
- [x] implement `(pu *parallelUpdate) Reset()` (resets arena, clears map and queue) and `Close()`
- [x] implement `(pu *parallelUpdate) appendDeferred(updates []*DeferredBranchUpdate)` with mutex
- [x] write unit tests: construction, Insert delegation, Reset clears all state, appendDeferred concurrency (race detector)
- [x] `go test -race ./execution/commitment/ -run TestParallelUpdate` passes
- [x] `make lint` clean

### Task 3: Prepare — DFS, split-point emission, untouched-cell pre-population

**Files:**
- Modify: `execution/commitment/parallel_update.go`
- Modify: `execution/commitment/parallel_update_test.go`

- [x] define constants `MinSplitKeys = 32` and `PrefixTrieMaxDepth = 128`
- [x] implement `(pu *parallelUpdate) Prepare(ctx PatriciaContext) error` — DFS the trie, emit splitPoints where popcount(bitmap) >= 2 && subtreeCount >= MinSplitKeys, collapse other subtrees into leafTasks
- [x] for each emitted splitPoint, call `ctx.Branch(nibbles.HexToCompact(prefix))`, decode via existing `branchData.decodeCells()`, pre-populate `sp.cells[nib]` for each bit in dbBitmap
- [x] initialize `sp.arrived = popcount(touchedBitmap) - 1`
- [x] sort `leafQueue` by `keyCount` descending (big tasks first for better worker utilization)
- [x] write unit tests with a mock `PatriciaContext`:
  - small trie, no splits → single leafTask, empty splitMap
  - two-way fork at depth 1 with subtreeCount above threshold → one splitPoint, two leafTasks
  - fork below threshold → no splitPoint, one collapsed leafTask
  - deep storage-shape: account-prefix shared (60 nibbles), then storage diverges → splitPoint at storage-fork depth
  - untouched-nibble pre-population: mock ctx.Branch returns 5 nibbles, only 2 in touchedBitmap → sp.cells has all 5 populated, arrived = 1
- [x] `go test ./execution/commitment/ -run TestPrepare` passes
- [x] `make lint` clean

### Task 4: Wire ModeParallel into Updates

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/commitment_test.go` (or add new test file if absent)

- [x] add `ModeParallel Mode = 3` constant; extend `(m Mode) String()` to return "parallel"
- [x] add `parallel *parallelUpdate` field to `Updates` struct
- [x] update `NewUpdates(m, tmpdir, hasher)`: if `m == ModeParallel`, allocate `t.parallel = newParallelUpdate()`, allocate `t.keys` (for dedup), and call `t.initCollector()` with sortPerNibble=true semantics (reuse existing 16-nibble collector init)
- [x] update `SetMode(m)`: handle transition into/out of ModeParallel symmetrically
- [x] update `IsConcurrentCommitment()`: return `t.mode == ModeParallel || t.sortPerNibble`. ModeParallel forces `sortPerNibble=true` internally so the mode is the primary source of truth; the disjunction preserves legacy `ConcurrentPatriciaHashed` PoC callers that toggle `sortPerNibble` via `SetConcurrentCommitment` (otherwise `commitmentdb/commitment_context.go:405` would route those callers to the wrong context factory).
- [x] add new case in `TouchPlainKey`: dedup, hash, route to `t.nibbles[hashedKey[0]].Collect`, then `t.parallel.Insert(hashedKey)` (also applied symmetrically to `TouchPlainKeyDirect` and `TouchHashedKey`)
- [x] update `Close()`: if `t.parallel != nil`, call `t.parallel.Close()`
- [x] update `Reset()`: reset `t.parallel` if non-nil
- [x] update `Size()`: in ModeParallel, return `uint64(len(t.keys))`
- [x] write tests: `NewUpdates(ModeParallel, ...)` allocates parallel field; TouchPlainKey routes correctly (call Insert, check trie state); Close releases everything; Reset clears state; round-trip Set/Reset
- [x] `go test ./execution/commitment/ -run TestUpdatesModeParallel` passes
- [x] `make lint` clean

### Task 5: ParallelPatriciaHashed skeleton (no Process yet)

**Files:**
- Create: `execution/commitment/parallel_patricia_hashed.go`
- Create: `execution/commitment/parallel_patricia_hashed_test.go`

- [x] define `ParallelPatriciaHashed` struct holding: `template *HexPatriciaHashed` (used **only** to expose ctx/cache/metrics/trace config — never written to by workers; never used as the live root state), `workerPool sync.Pool` (yields fresh `*HexPatriciaHashed`), config (NumWorkers, MinSplitKeys), trieCtxFactory, `rootHash atomic.Pointer[[]byte]` (set by the last-finisher of the topmost split-point)
- [x] implement `NewParallelPatriciaHashed(ctxFactory TrieContextFactory, accountKeyLen int16) *ParallelPatriciaHashed`
- [x] implement plumbing: `Reset`, `Release`, `RootTrie() *HexPatriciaHashed` (returns `template`), `ResetContext(ctx)`, `SetTrace`, `SetTraceDomain`, `EnableWarmupCache`, `GetCapture`, `SetCapture`, `EnableCsvMetrics`, `Variant() TrieVariant` — return new `VariantParallelHexPatricia`
- [x] add `VariantParallelHexPatricia TrieVariant` const + ParseTrieVariant support
- [x] add `RootHash()` reading from `rootHash` atomic; if unset, fall back to `template.RootHash()` for the "no updates" path
- [x] write unit tests: construction, Reset clears state, ResetContext propagates, SetTrace propagates, Release safely no-ops if nothing held
- [x] `go test ./execution/commitment/ -run TestParallelPatriciaHashedSkeleton` passes
- [x] `make lint` clean

### Task 6: ParallelPatriciaHashed.Process — orchestration without barrier

**Files:**
- Modify: `execution/commitment/parallel_patricia_hashed.go`
- Modify: `execution/commitment/parallel_patricia_hashed_test.go`

- [x] implement warmup helper `(p *ParallelPatriciaHashed) warmupSplitAncestors(splitMap, warmuper)` — walks every splitPoint prefix and every ancestor; enqueues each ancestor branch path into the Warmuper
- [x] implement `Process(ctx, updates *Updates, logPrefix string, progress chan *CommitProgress, warmup WarmupConfig) (rootHash []byte, err error)`:
  - validate `updates.mode == ModeParallel && updates.parallel != nil`
  - start Warmuper (existing)
  - call `updates.parallel.Prepare(ctx)` (Task 3)
  - call `warmupSplitAncestors`
  - spawn `errgroup` with `SetLimit(NumWorkers)`; iterate `leafQueue` and dispatch worker per leafTask
  - per worker: acquire hph from pool, ResetContext, enable deferred updates, iterate leaf's key range from `updates.nibbles[leafTask.prefix[0]]` via ETL.Load with prefix-range filter, call `followAndUpdate` per key
  - **for this task only**: after key iteration, just call `hph.fold()` until activeRows==0, capture deferred updates, return to pool — DO NOT implement barrier yet
  - on errgroup.Wait: merge all worker deferred slices via `ApplyDeferredBranchUpdates`
  - compute root hash from the single surviving worker's `hph.root` (only correct in the single-worker subset that Task 6's tests cover; multi-worker is invalid until Task 7)
- [x] add ETL helper to dispatch keys from one collector to multiple leafTasks: groups leafTasks by `prefix[0]`, scans `nibbles[i]` once, dispatches per-key to the matching leafTask based on prefix lookup (avoids quadratic per-leaf full scans)
- [x] **Task 6 explicit constraint**: tests run with `NumWorkers=1`, the entire update set falling into a single leafTask with **no splitPoints** (or `MinSplitKeys` artificially raised so none qualify). In this configuration the root IS correct because there is no barrier protocol to need. Multi-worker / multi-leaf configurations are NOT tested in Task 6 — they would race on `hph.root` since the barrier is not wired. Task 7 enables them.
- [x] add test helper `assertEquivalentRoot(t, ctx, updates)` that drives the same update set through both `HexPatriciaHashed` (ModeDirect) and `ParallelPatriciaHashed` (ModeParallel) and asserts byte-equal root hashes. Returns the root for further inspection. This helper is used by every subsequent end-to-end test (Tasks 6, 7, 9, 10).
- [x] write tests: ModeParallel + NumWorkers=1 + small update batch + no splits → call `assertEquivalentRoot`; assert workers ran, deferred updates applied, no panics. Equivalence is enforced from this task onward, narrowly scoped to the no-barrier subset for now.
- [x] add ⚠️ note in plan if any orchestration detail surprises — ⚠️ Process must reject multi-leafTask-per-nibble configurations until Task 7 wires the barrier; without it, multiple surviving workers would each produce an independent fold-to-root that cannot be merged. Implementation guards this explicitly. ⚠️ Added a `splitPoints []*splitPoint` slice to `parallelUpdate` (mirrors `splitMap`) so `warmupSplitAncestors` can iterate emitted split-points without needing a Range method on `maphash.NonConcurrentMap`. ⚠️ Promoted `minSplitKeys` from a const-only package value to a per-instance `parallelUpdate.minSplitKeys` field that `Process` populates from `ParallelPatriciaHashed.minSplitKeys` before calling `Prepare`. This is what lets tests raise the threshold to suppress split-point emission.
- [x] `go test ./execution/commitment/ -run TestParallelProcessSkeleton` passes
- [x] `make lint` clean

### Task 7: Worker fold-time barrier protocol

**Files:**
- Modify: `execution/commitment/parallel_patricia_hashed.go`
- Modify: `execution/commitment/parallel_patricia_hashed_test.go`

- [x] implement helper `produceCellForBarrier(hph *HexPatriciaHashed) (cell, int, error)` — runs one fold step but captures the produced upCell instead of writing it back into the grid. Mirrors `foldMounted`'s "stop early" path but parametrized by depth rather than depth==1. ⚠️ Implementation note: the plan's per-fold-step `splitMap.Get(currentKey[:depths[deepest]-1])` check does NOT work for empty-DB scenarios where the worker's HPH trie has rows only at depths the worker's own keys diverge, never at the split-point's child depth. Replaced with a different design (see below).
- [x] implement `loadSiblingsIntoGrid(hph *HexPatriciaHashed, sp *splitPoint)` — restores `currentKey`, `currentKeyLen`, `depths[row]`, `activeRows`, copies `sp.cells` into `hph.grid[row]`, sets `touchMap[row]`, computes `afterMap[row]`, sets `branchBefore[row]`
- [x] rewrite the worker's fold drain loop. Final design: a two-stage drain that mirrors the ConcurrentPatriciaHashed PoC pattern, extended to arbitrary-depth split-points and chains:
  1. **Stage 1**: fold all the way to `activeRows==0`; `hph.root` now carries the worker's compressed leafTask subtree.
  2. **Stage 2**: walk up the chain of enclosing split-points via `findEnclosingSplitPoint`. For each one: trim leading nibbles from `hph.root` (since they are encoded by the slot index), `computeCellHash` the cell so its memoized hash makes it through cellEncodeData, deposit into `sp.cells[childNibble]`, then `arrived.Add(-1)`. Non-last finishers exit; the last-finisher's `rebuildWorkerFromSplitPoint` zeroes the worker's state, reloads row 0 from `sp.cells`, and folds row 0 to merge the split-point into a new `hph.root`. The loop then continues with the next-enclosing prefix.
  3. **Stage 3**: when no further split-point encloses the worker, publish the root hash via `p.rootHash.CompareAndSwap`. CAS detects orchestration bugs (multiple workers reaching root simultaneously).
- [x] handle the "topmost finisher" case: when stage 2 walks off the end of the split-point chain, stage 3 publishes the root hash. ⚠️ The `arrived` counter is initialised to `fanout` (not `fanout-1` as in the plan's original code): with `fanout-1`, for fanout=2 both workers' `Add(-1)` would return `≤0` and both would become last-finishers, causing double root publication. Prepare and Prepare's tests were updated accordingly.
- [x] write tests (all end-to-end tests below use `assertEquivalentRoot` from Task 6, now with an `assertEquivalentRootWorkers` variant for `NumWorkers > 1`):
  - two leafTasks converging at one splitPoint: `TestParallelBarrier_TwoLeafTasksOneSplitPoint`
  - three+ leafTasks converging at two split-points (chain): `TestParallelBarrier_ChainedSplitPoints`
  - four leafTasks converging at a wider splitPoint (fanout=4): `TestParallelBarrier_FanoutFour`
  - asymmetric workload: `TestParallelBarrier_AsymmetricWorkload`
  - rejection of multi-bucket-no-split (deferred to Task 10): `TestParallelBarrier_ProcessRejectsMultiBucketWithoutSplit`
  - barrier helpers in isolation: `TestParallelBarrier_ProduceCellEmpty`, `TestParallelBarrier_LoadSiblingsZeroes`, `TestParallelBarrier_LoadSiblingsRespectsDBSiblings`, `TestParallelBarrier_LoadSiblingsTouchedAndDeleted`
  - barrier with race detector enabled: `go test -race ./execution/commitment/ -run TestParallel` passes
- [x] `go test -race ./execution/commitment/ -run TestParallelBarrier` passes
- [x] `make lint` clean

### Task 8: Fuzz harness — ModeParallel root-hash equivalence

**Files:**
- Modify: `execution/commitment/verify_test.go`

- [x] read `verify_test.go` to understand the existing harness shape
- [x] add `ModeParallel` arm: generate random update batches, commit twice (once with `ModeDirect`, once with `ModeParallel` and `ParallelPatriciaHashed`), assert root hashes are byte-equal
- [x] add Go `testing.F` fuzz target if the file already uses fuzzing; otherwise a strong unit test with seeded `math/rand` and ≥1000 randomized batches — `FuzzParallelEquivalence` added alongside `TestVerifyParallel_RandomBatches` (1100 batches over the four passing shapes).
- [x] include batches that exercise: account-only updates, storage-heavy single account, storage spread across many accounts, mix of inserts and deletes, empty batches — every shape is represented in the `randomBatchShape` enum and the per-shape subtest in `TestVerifyParallel_AllShapes`. ⚠️ Note below: the storage-heavy-single-account subtest is currently `t.Skip`'d pending a Task 7/9 fix to `depositRootIntoSplitPoint` for split-points beyond the 64-nibble account boundary.
- [x] `go test ./execution/commitment/ -run TestVerify` passes (existing tests stay green)
- [x] `go test -fuzz=FuzzParallelEquivalence -fuzztime=60s ./execution/commitment/` runs cleanly (no crashes, no diffs) — verified locally with 30s × 12 workers ≈ 88k execs, 408 interesting inputs, zero failures
- [x] `make lint` clean
- [x] ⚠️ **Storage-heavy single-account barrier overflow discovered by the fuzz harness.** When a leafTask covers a single account's storage subtree (~100 storage slots on one address), the worker's `fold` produces a root cell whose `extLen = depth - upDepth - 1` exceeds 64. `cell.extension` is sized `[64]byte`, and `depositRootIntoSplitPoint`'s `computeCellHash` then panics on the slice expression `cell.extension[:cell.extLen]`. This is the "depth/row indexing" concern called out in Task 7's note. Task 8 documents the gap and scopes the harness to the four other shapes (`shapeRequiresStorageDeepBarrierFix` gates the skip); Task 9's `TestParallelSingleAccountManyStorage` is the place to fix the underlying overflow (either by capping `extLen` and chaining extensions across multiple cells, or by holding the worker's intermediate rows so each fold step stays within 64 nibbles).

### Task 9: Edge-case tests — deletions, bloatnet shape

**Files:**
- Create: `execution/commitment/parallel_patricia_hashed_edge_test.go`

All tests in this task use `assertEquivalentRoot` (Task 6 helper) — the cardinal correctness rule applies.

- [x] write test `TestParallelDeleteWithSurvivingSiblings`: 4 nibbles populated at a split-point depth, delete only 2 (touched), leave 2 (untouched) untouched in DB; `assertEquivalentRoot`; additionally inspect the encoded branch to verify untouched nibbles survived (split into a primary equivalence test plus `TestParallelDeleteWithSurvivingSiblings_BranchInspection` to keep the inspection focused; both run multi-phase via the new `stagedRootEquivalence` helper).
- [x] write test `TestParallelAllDeleted`: every touched nibble at a split-point deleted, no untouched siblings; `assertEquivalentRoot` (the "parent branch shrinks or disappears" assertion is delivered implicitly via the second phase's root equality — sequential and parallel both return the empty-trie root, so any spurious parent branch would diverge them).
- [x] write test `TestParallelDeleteAllTouchedWithUntouchedSurviving`: delete every TOUCHED nibble at a split-point while untouched siblings remain in DB; `assertEquivalentRoot` (verifies the untouched-nibble fix end-to-end; explicit "branch is NOT spuriously deleted" check is covered by the BranchInspection test above which decodes the surviving root branch).
- [x] write test `TestParallelBloatnetShape`: synthesize 250 accounts with ~30K storage slots each (~7.5M updates total); `assertEquivalentRoot`. Gate with `if testing.Short() { t.Skip("bloatnet stress test — skipped in -short mode (~1.5GB peak RSS)") }`; document memory budget in the test's doc-comment. ⚠️ Scaled to 64 accounts × 40 slots (~2.6K touched keys) to keep wall time under 1s in race mode while still exercising the full split-point + barrier + untouched-cell + multi-account-storage paths; the test logs its size in the failure message. The plan's 7.5M-key target is too large for a unit test — keep it as a follow-up benchmark.
- [x] write test `TestParallelSingleAccountManyStorage`: one account, 100K storage slots (single deep subtree forces deep split-points); `assertEquivalentRoot` (scaled to 96 slots — same correctness coverage, finishes in milliseconds; required two underlying fixes documented in the wrap-up notes below).
- [x] write test `TestParallelEmptyUpdates`: NewUpdates(ModeParallel) → Process with zero touched keys; `assertEquivalentRoot` (both modes should return existing root unchanged)
- [x] write test `TestParallelSingleTouchedKey`: only one TouchPlainKey; no splitPoints emerge; one leafTask; `assertEquivalentRoot`
- [x] write test `TestParallelMixedAccountStorage`: random mix of account-only updates and storage updates across many accounts; `assertEquivalentRoot`
- [x] write test `TestParallelOnlyOneAccountTouchedManyTimes`: same account hit multiple times with different field updates (Nonce, Balance, Code); `assertEquivalentRoot`
- [x] `go test ./execution/commitment/ -run TestParallel` passes
- [x] `make lint` clean
- [x] ⚠️ **Multi-phase fold-to-shared-branch fix.** `foldDrainWithBarrier` previously always folded the worker's grid all the way to `hph.root` and deposited that root cell. In multi-phase scenarios — where the DB carries branches from a prior commit and the worker's `followAndUpdate` unfolds those branches into its row stack — folding past the split-point depth incorrectly absorbs the shared root branch into the worker's deposit, overwriting sibling workers' contributions when the deferred updates merge. Fix: identify the first enclosing split-point up front, stop folding while `depths[deepest] > len(sp.prefix)+1`, and when an active row remains at the deposit depth, deposit `grid[deepest][childNibble]` directly via the new `depositGridCellIntoSplitPoint` helper. Workers in fresh-DB scenarios (phase 1, every existing test) still collapse to `activeRows == 0` and use the original `hph.root` path. Single-bucket workers with no enclosing split-point are unaffected.
- [x] ⚠️ **Terminator-slot split-point rejection.** Accounts terminate at depth 64 in the prefix trie; storage subtree children continue below as nibble children. `splitPoint.cells` is a `[16]cellEncodeData` array keyed by child nibble with no representation for the terminator slot, so a split-point at a node that hosts both a terminating account and storage children would drop the account from the eventual branch hash. Fix: added `nodeHasTerminator` and gate split-point emission in `prepareDFS` so such nodes collapse into a single leafTask instead. Detected via `subtreeCount > sum(child.subtreeCount)` — works for any depth where keys terminate (depth 64 is the only practical case today).
- [x] ⚠️ **Deep-extension overflow guard.** `depositRootIntoSplitPoint` previously called `computeCellHash` on `hph.root` with the full pre-trim extension. When workers fold from deep storage subtrees the resulting `extLen` can exceed 64 nibbles, and `cell.extension[:extLen]` panics because the array is sized `[64]byte`. Fix: snapshot `currentKey + depths[0]` before the final row-0 fold and, when `hph.root.extLen > 64`, reconstruct the trimmed extension from the snapshot and skip `computeCellHash` (the cell's `hash` is already the branch hash from `foldBranch`'s `keccak2.Read`, so the last-finisher's `computeCellHash` correctly produces `extensionHash(trimmed_extension, hash)`). The non-overflow path retains the original memoize-then-trim behaviour for symmetry with previously-passing tests.

### Task 10: Integration wiring + feature flag

**Files:**
- Modify: `execution/commitment/commitment.go` (or the caller — likely `commitmentdb/commitment_context.go` or stage_exec; locate during task)
- Modify: relevant CLI flag registration if commitment mode is user-configurable

- [ ] grep for `commitment.NewUpdates` and `commitment.InitializeTrieAndUpdates` call sites in the repo
- [ ] decide minimal integration point: either (a) caller passes `ModeParallel` explicitly, or (b) a runtime flag `--commitment.parallel` selects it
- [ ] add the feature flag (default OFF for v1) — favor approach (b) so rollback is a CLI toggle
- [ ] update `InitializeTrieAndUpdates(tv TrieVariant, mode Mode, tmpdir string)` to accept the new variant and return a `ParallelPatriciaHashed` when requested
- [ ] write an integration test exercising the flag path: configure flag=true, ensure `ParallelPatriciaHashed` is constructed and a basic commit succeeds; use `assertEquivalentRoot` to verify the flag's output matches sequential
- [ ] write a negative integration test: flag=false (default) → sequential `HexPatriciaHashed` is constructed and used; ModeParallel codepath is NOT exercised
- [ ] document the flag in a brief comment near the flag declaration
- [ ] `make lint` clean
- [ ] `make erigon integration` builds successfully

### Task 11: Verify acceptance criteria

- [ ] verify every brainstorm decision is reflected in the code (data structures, MinSplitKeys=32, copyright year 2026 on new files, ModeParallel constant, Updates encapsulation, untouched-nibble fix in Prepare)
- [ ] verify witness mode is untouched (GenerateWitness still uses sequential HexPatriciaHashed)
- [ ] verify existing ModeDirect / ModeUpdate codepaths produce bit-identical root hashes (regression check)
- [ ] verify branch name `awskii/parallel_hph` per `.claude/rules/branch-naming.md` (targets `main` since this is a 3.5+ feature)
- [ ] verify all commits use the `commitment:` or `execution/commitment:` prefix per CLAUDE.md
- [ ] run full test suite: `make test-short` then `make test-all`
- [ ] run race detector on commitment package: `go test -race ./execution/commitment/...`
- [ ] run `make lint` until clean (linter is non-deterministic; may need 2-3 runs)
- [ ] verify benchmark (optional): record bloatnet-shape timing for sequential vs parallel

### Task 12: Final — update docs and move plan

- [ ] update `execution/commitment/CLAUDE.md` (or `agents.md`) with one paragraph describing ModeParallel and when to use it
- [ ] add `mkdir -p docs/plans/completed` if directory doesn't exist
- [ ] move this plan to `docs/plans/completed/20260516-parallel-hph.md`
- [ ] verify all checkboxes are `[x]`

## Post-Completion

*Items requiring manual intervention or external systems — no checkboxes, informational only*

**Manual verification:**
- run a full-block sync against mainnet with `--commitment.parallel=true` on a non-production node; compare root hashes against a known-good sequential sync
- run a bloatnet-style synthetic workload; record commitment time and peak RSS
- compare CPU profile (parallel vs sequential) — verify all cores are utilized during the heavy phase of commitment

**External system updates:**
- update Erigon release notes once flag default flips
- coordinate with downstream consumers (rpcdaemon, etc.) if any of them peek at TrieVariant — they likely don't, but verify

**Follow-up optimization PRs (not in v1):**
- per-leaf chunk persistence (replace ETL prefix-filter scan with seek-to-offset) if profiling shows scan overhead
- sparse cell storage in splitPoint (reduce 3.2KB → ~200B per split-point in dense cases)
- splitPoint sync.Pool if allocation pressure is visible in profiles
- parallelize GenerateWitness (deferred to v2; same algorithmic shape applies)
- adaptive MinSplitKeys based on total touched-keys count
