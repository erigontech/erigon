# Streaming-mode multi-depth concurrent fold + remove cross-block storage cell cache

## Overview

Replace `StreamingCommitter`'s fold so the concurrent-fold property already proven at
row 0 (a branch's 16 disjoint child subtrees fold independently, then stitch into the
parent branch row) is applied **recursively at every branch row** — folding independent
subtrees at all depths concurrently. Remove the cross-block `accountStorageCache`.

**Problem it solves.** Today streaming only materializes split nodes at the top 16
nibbles (`foldPresentSplits`/`foldSplit`); everything below a top nibble folds
*sequentially* in one worker, except a big-storage account which gets `storageRootLocal`'s
flat 16-way fan-out. A single deep subtree (whale account storage) is the measured
bottleneck (serial streaming-fold ≈10× slower than concurrent subtree processing).

The previously-built `accountStorageCache` tried to win this by caching folded storage
cells across blocks. That is unsound and is being removed: a cell's extension depends on
its depth, so a cached cell cannot be substituted if its row moves; and it bought only a
narrow warm-whale win while the parallel fold already ties/beats ModeParallel without it.
A latent concurrency flake was also found in that path (see Task 2).

**Benefit.** One uniform, parallel, cache-free fold engine for streaming mode: independent
subtrees at any depth fold simultaneously, parity-clean under `-race`, ties-or-beats
ModeParallel, no cross-block cache to keep coherent.

## Context (from discovery)

Package `execution/commitment` (worktree `/Users/awskii/org/wrk/erigon-prepare-fold`,
branch `awskii/parallel_prepare_fold`).

Files/components involved:
- `streaming_commitment.go` — `StreamingCommitter`, `dfsDeepLocal`, `foldPresentSplits`,
  `foldSplit`, `stitchSplitCells`, `storageRootLocal`, `newStorageWorker`,
  `collectStorageNibbleKeys`, and all cache wiring.
- `streaming_storage_cache.go` — the cache type + shared deep-fold helpers
  `foldStorageChildCell`, `aggregateStorageRoot`, `assembleAccountRoot`,
  `storageWorkerFactory`.
- `prefix_trie.go` — `prefixNode` (path-compressed hash-prefix trie, `bitmap`,
  `children`, `subtreeCount`).
- `parallel_update.go` — `Prepare`, split-point detection. The full predicate is
  `subtrees >= 2 && node.subtreeCount >= MinSplitKeys && !nodeHasTerminator(node) && nodeDepth < 64`
  (`MinSplitKeys = 64`). **The `nodeDepth < 64` cap is load-bearing for parallel mode**
  (parallel_update.go:281-283, "keep each account's storage whole within one leafTask") and
  is exactly why streaming cannot reuse `Prepare`'s split set unchanged for whale storage —
  see Solution Overview and Task 3. Also `appendDeferred` (mutex-guarded) lives here.
- `streaming_commitment.go` — `mergeDeferredByPrefix` (≈819) and `applyDeferredGuarded`
  (≈1413) live here, NOT in `parallel_update.go`.
- **Streaming does not call `Prepare` today** — its scaffolding is `sc.splits map[byte]*splitState`
  (top nibble only), indexed by single `byte` at ~8 sites. Task 3 must introduce a
  prefix-keyed, depth-agnostic split source; this is net-new wiring, not a tweak.
- `hex_concurrent_patricia_hashed.go` — `mountTo(root, nibble)` (row-0 only),
  `ConcurrentPatriciaHashed`.
- `hex_patricia_hashed.go` — `foldMounted` (row-0 stop condition
  `activeRows==1 && depths[activeRows-1]==1`), `resetForReuse` (does NOT clear `grid`),
  `hphPool`.
- `parallel_mount.go` — `deepStorageThreshold`, `setAccountStorageRoot`,
  `concurrentStorageRoot`. **Must not be modified** (streaming-local boundary).

Related patterns found:
- The non-cache deep fold (`storageRootLocal`) already folds to depth 64/65 by setting
  `currentKey`/`depths` by hand — the template for an arbitrary-depth fold primitive.
- Deferred branch updates + per-fold `overlayContext` already linearize writes for the
  top-16 splits; this plan lifts that machinery to every row.

Dependencies identified: `errgroup` (already used), `Prepare`'s split-point set.

## Development Approach

- **Testing approach: TDD** (Red→Green→Refactor), per repo `CLAUDE.md`. Reproduce bugs as
  failing tests before fixing. Task 1 is a pure refactor (no behavior change for the
  non-cache path) — existing non-cache parity tests are its safety net; state this in the
  task rather than inventing redundant tests.
- Complete each task fully before the next; small focused changes.
- **Every task must build + pass `make lint` and `make erigon integration` before the next.**
- `make lint` is non-deterministic — run repeatedly until clean.
- Maintain parity with sequential ModeDirect and ModeParallel at every step.

## Testing Strategy

- **Unit/parity tests**: required per task. Parity = identical state root AND identical
  stored branch set vs sequential ModeDirect and vs ModeParallel.
- **Race**: concurrency-sensitive tests run under `go test -race -count>=20` — the flake
  class found in this work does NOT surface as a `DATA RACE` report, only as a value
  mismatch, so high `-count` under `-race` is the detection method.
- No e2e/UI tests in this package.
- **No `t.Skip`** / build-tag exclusions to silence failures (repo rule for agents).

## Progress Tracking

- Mark completed items `[x]` immediately.
- New tasks: `➕` prefix. Blockers: `⚠️` prefix.
- Keep this file in sync with actual work.

## Solution Overview

The hash-prefix trie (`prefixNode`) is the scaffold. A **split node** is in-block-only fold
scaffolding holding the authoritative folded cell ("golden truth") for one branch row at a
known prefix — keyed by prefix, so it is structural and depth-safe (unlike a detached cached
cell).

**Split source (decided).** Streaming uses its OWN inline split predicate during the
recursive walk — it does NOT call `Prepare` and does NOT modify `parallel_update.go`
(parallel mode keeps both its guards byte-identical). The predicate on a `prefixNode`:

    popcount(bitmap) >= 2  &&  subtreeCount >= MinSplitKeys  &&  plainKey == nil

Two guards are load-bearing:
- **`plainKey == nil` (terminator guard — correctness, not optimization).** A node hosting a
  terminating key has no terminator slot in a branch-indexed split cell, so splitting there
  would drop that key from the branch hash and diverge from the sequential root
  (`parallel_update.go:269-276`). This naturally excludes the **account@depth-64** node (it
  terminates the account above its storage) and any storage-slot terminator.
- **No depth cap.** `Prepare`'s `nodeDepth >= 64` guard (parallel_update.go:281-283) exists
  for parallel mode's leafTask-orphan model; streaming never inherits it, so storage-interior
  forks (depth > 64) — the actual bottleneck — become split-eligible.

**The account/storage boundary stays on `storageRootLocal`.** Because account@64 carries a
terminator it is never a generic split-point; `storageRootLocal` folds its storage children
and injects the storageRoot into the account leaf (the existing, correct boundary handler).
Storage-interior recursion happens BELOW that boundary, where nodes carry no account
terminator. The account trie (depth 1..63) splits via the same predicate where forks qualify.

Fold = parallel **post-order** over the split-point tree: split-tree leaves fold first
(concurrent), each parent folds once all its child split nodes are folded, capped at
`numWorkers`. A split-aware `dfsDeepLocal` stops at child split-points and drops in their
already-folded cells; the parent stitches them into its row.

**Write linearization (the core problem).** Concurrent phase is read-only on shared ctx;
all branch writes are deferred (`branchEncoder` + `leaveDeferredForCaller`); each fold runs
against its own `overlayContext`, so a collapse-driven mid-fold self-flush
(`readBranchAndCheckForFlushing`) reads its own isolated writes — never another fold's
pending writes or the not-yet-applied deferred batch. Deferred updates merge by prefix
(disjoint subtrees ⇒ disjoint prefixes ⇒ commute) and apply once at end via
`applyDeferredGuarded`. The only ordering is post-order stitch + a single end-of-block apply.

## Technical Details

- **Arbitrary-depth fold primitive**: "fold the subtree rooted at prefix `P` to a single
  cell at depth `len(P)`". Generalize the deep fold's own hand-rolled mount
  (`foldStorageChildCell`/`aggregateStorageRoot` set `currentKey`/`depths` directly) to any
  prefix — `foldMounted`/`mountTo` (the row-0 PoC) are not involved. The mounted worker MUST
  be fully initialized for every cell/row it reads (see Task 2 bug).
- **Split detection**: an inline predicate on a `prefixNode` during the recursive walk —
  `popcount(bitmap) >= 2 && subtreeCount >= MinSplitKeys && plainKey == nil` — NOT `Prepare`.
  The `plainKey == nil` terminator guard is correctness (excludes account@64 + slot
  terminators); no depth cap. The account/storage boundary is handled by `storageRootLocal`;
  storage-interior recursion happens below it. Deep split nodes are transient (recursion-local);
  `sc.splits map[byte]` (top-16 + background-fold reuse) is unchanged.
- **Stitch**: generalize `stitchSplitCells` to drop a child split cell into its parent's
  column at any depth, stripping the leading extension nibble as today.
- **Collapse up past a split-point**: a delete that empties a child split node returns an
  empty cell; the parent clears that branch bit at stitch (structural — no re-fold, no
  flushed signal, because each fold is overlay-isolated and read-only on base).

## What Goes Where

- **Implementation Steps** (`[ ]`): all code/tests in this package.
- **Post-Completion** (no checkboxes): benchmark comparison vs ModeParallel, working-tree
  hygiene for ralphex.

## Implementation Steps

### Task 1: Remove the cross-block accountStorageCache

**Files:**
- Create: `execution/commitment/streaming_deep_fold.go` (home for moved shared helpers)
- Modify: `execution/commitment/streaming_commitment.go`
- Delete: `execution/commitment/streaming_storage_cache.go`
- Delete: `execution/commitment/streaming_storage_cache_test.go`
- Delete: `execution/commitment/nested_cache_bench_test.go`
- Delete: `execution/commitment/nested_storage_prototype_test.go` (orphaned cache prototype/benchmark)

- [x] grep-confirm callers first: `foldStorageChildCell` + `aggregateStorageRoot` are used by
      the kept `storageRootLocal` (streaming_commitment.go:1286,1306) → **move** these two into
      `streaming_deep_fold.go`. `assembleAccountRoot` (only caller is the deleted cache test)
      and the `storageWorkerFactory` type (only used by the removed `foldStorageRootCached`)
      become dead → **delete** them, not move (re-grep to confirm zero surviving callers)
- [x] delete `streaming_storage_cache.go`, `streaming_storage_cache_test.go`,
      `nested_cache_bench_test.go`, and `nested_storage_prototype_test.go` (the last benchmarks
      the removed cache concept; confirm it doesn't supply a helper any kept test needs —
      `whaleByNibble`/`storKV`/`foldChildAt` live in the kept `deep_storage_concurrent_bench_test.go`)
- [x] strip `StreamingCommitter` cache fields: `caches`, `accTouch`, `nestedCacheOn`,
      `nestedCap`, `nibbleFolds`, `bgDeepFolds` (and their init in `NewStreamingCommitter`,
      cleanup in `endBlock`/`Reset`/`InvalidateCaches`/`Release`)
- [x] remove cache methods: `SetNestedCache`, `routeCachedStorage`, `splitHasCache`,
      `foldSplitBgCached`, `cacheFor`, `storageRootCached`, `storageRootCachedNibble`,
      `foldCachedStorageRoot`, `newIsolatedStorageWorker`, `NibbleFolds`, `BgDeepFolds`
      (also removed the now-dead `mergeDeferredByPrefix`, `accountKeyOf`, `foldKeysDeep`,
      and the `InvalidateCaches`/`InvalidateStreamingCaches` chain + its commitmentdb caller —
      streaming holds no cross-block state to invalidate; Task 4 re-adds `mergeDeferredByPrefix`)
- [x] remove the `TouchKey` nested-cache branch and the two `dfsDeepLocal` cache branches
      (the `len(path) > accountKeyNibbles` cacheFor route and the `len(path) == 64` cacheFor
      route); KEEP the `storageRootLocal` branch, `storageRootLocal`, `newStorageWorker`,
      `collectStorageNibbleKeys`, `deepStorageThreshold`, `setAccountStorageRoot`,
      `deepLocalFolds`/`DeepLocalFolds`
- [x] grep the whole package for any remaining cache references (`accountStorageCache`,
      `cacheFor`, `nestedCache`, `NibbleFolds`, …) including test files; fix fallout
- [x] (pure refactor — no new tests; existing non-cache parity tests are the safety net)
      run `make lint` (until clean) + `make erigon integration`; run the streaming parity
      suite — must pass before Task 2

### Task 2: Arbitrary-depth fold primitive + fix the stale-grid bug

**Files:**
- Modify: `execution/commitment/streaming_deep_fold.go` (generalize the deep-fold mount; fix `aggregateStorageRoot` grid init)
- Modify: `execution/commitment/hex_patricia_hashed.go` (ONLY if the chosen grid fix is `resetForReuse` clearing `grid`)
- Create: `execution/commitment/streaming_deep_fold_test.go` (regression + primitive tests)

Note: `foldMounted`/`mountTo` (the row-0 concurrent-trie PoC path) are NOT touched. The deep
fold (`storageRootLocal` → `foldStorageChildCell` + `aggregateStorageRoot`) already mounts at
depth 64 by hand; the arbitrary-depth primitive generalizes THAT, leaving the PoC path and
`parallel_mount.go` untouched.

- [ ] write a FAILING regression test first: spawn many concurrent per-nibble folds (to
      pollute `hphPool`), then call the aggregation path, asserting the storageRoot equals
      the sequential oracle; run under `-race -count>=20`; confirm it fails red for the
      right reason (residual stale-`grid` state, NOT a setup error)
- [ ] fix root cause: ensure the aggregation/fold-to-depth helper fully initializes every
      worker cell/state it reads — `aggregateStorageRoot` must set `grid[0][accNib]` (today it
      does not) and/or `resetForReuse` must clear `grid` on pool return; choose the minimal
      correct fix and document the invariant in one line
- [ ] generalize the deep-fold mount (the hand-rolled `currentKey`/`depths` setup in
      `foldStorageChildCell`/`aggregateStorageRoot`) to mount at an arbitrary prefix `P` and
      fold to a cell at depth `len(P)`. Do NOT modify `foldMounted`/`mountTo`/`parallel_mount.go`.
- [ ] verify `parallel_mount.go`, `hex_concurrent_patricia_hashed.go`, and
      `prepare_on_touch_test.go` compile with NO edits (the PoC mount path is untouched)
- [ ] write tests for the primitive at depths mid-account, 64/65, and a mid-extension
      (path-compressed) prefix (parity vs the existing `storageRootLocal` path)
- [ ] run `-race -count>=20` regression + unit tests, `make lint`, `make erigon integration` — must pass before Task 3

### Task 3: Multi-depth split-points + parallel post-order schedule

**Files:**
- Create: `execution/commitment/streaming_split_fold.go` (inline predicate + recursive split fold + schedule)
- Modify: `execution/commitment/streaming_commitment.go` (`dfsDeepLocal`, `storageRootLocal`, `stitchSplitCells`)
- Modify: `execution/commitment/streaming_commitment_test.go` (or a new test file)

Do NOT touch `parallel_update.go`/`Prepare` or re-key `sc.splits` — deep split nodes are
transient (in-block recursion-local); the top-16 `sc.splits map[byte]` layer (with its
background-fold reuse) stays unchanged.

- [ ] add the inline split predicate `isSplitPoint(node)` =
      `popcount(node.bitmap) >= 2 && node.subtreeCount >= MinSplitKeys && node.plainKey == nil`.
      The `plainKey == nil` (terminator) guard is load-bearing for correctness (excludes the
      account@64 node and slot terminators). NO depth cap.
- [ ] implement the recursive, split-aware fold: walk a `prefixNode` subtree; at a child where
      `isSplitPoint` holds, fold that child subtree CONCURRENTLY in its own worker (a transient
      split node holding its folded cell) instead of recursing inline; cap concurrency at
      `sc.numWorkers` via `errgroup`; post-order — the parent stitches child cells after they complete
- [ ] keep `storageRootLocal` as the account/storage boundary handler (account@64 terminator is
      never a split-point), but make it RECURSIVE: instead of a flat 16-way fan-out of first-storage
      nibbles, each storage-nibble subtree is itself split at deeper qualifying forks via the same
      recursive fold — this is where storage-interior concurrency (depth > 64) comes from
- [ ] generalize `stitchSplitCells` to stitch a child split cell into its parent's column at any
      depth (strip the leading extension nibble as today)
- [ ] write tests: split-points at 2–3 depths fold to the same root as sequential; **assert a
      split fires at depth > 64 inside a whale storage subtree** (seam/counter — parity alone
      won't catch a regression to account-trie-only parallelism); assert account@64 still routes
      through `storageRootLocal` (terminator node is never split)
- [ ] run `-race -count>=20`, `make lint`, `make erigon integration` — must pass before Task 4

### Task 4: Write linearization at every split node

**Files:**
- Modify: `execution/commitment/streaming_commitment.go` (per-fold worker setup, deferred collection, end-of-block apply)
- Modify: `execution/commitment/streaming_commitment_test.go`

- [ ] each split fold: wrap base ctx in `overlayContext`; `branchEncoder.setDeferUpdates(true)`
      + `SetLeaveDeferredForCaller(true)`; collect deferred via `TakeDeferredUpdates` into
      the per-walk `parallelUpdate` (`appendDeferred`, mutex-guarded)
- [ ] end of block: merge deferred by prefix (`mergeDeferredByPrefix`), apply once via
      `applyDeferredGuarded(sc.numWorkers)`
- [ ] handle collapse propagating up past a split-point: emptied child split node returns an
      empty cell; parent clears that branch bit at stitch (no re-fold, no flushed signal)
- [ ] write tests: a delete-driven collapse that crosses a split boundary yields the same
      root + branch set as sequential; verify no fold observes another's pending writes
      (overlay isolation), under `-race -count>=20`
- [ ] run `-race -count>=20`, `make lint`, `make erigon integration` — must pass before Task 5

### Task 5: Multi-depth-split parity + race tests

**Files:**
- Create: `execution/commitment/streaming_multidepth_parity_test.go`

- [ ] build a corpus whose touched keys create split-points at SEVERAL depths (account-trie
      forks + a whale storage subtree with deep forks); reuse `whaleByNibble` + existing
      parity harnesses
- [ ] assert the streaming concurrent-fold root equals sequential ModeDirect AND ModeParallel
- [ ] assert every stored branch equals the sequential/ModeParallel branch set
- [ ] assert (via the Task 3 seam/counter) that the whale storage subtree actually folded at
      split-points below depth 64 — the headline goal, which parity alone does not verify
- [ ] run the new test under `-race -count>=20` (success + collapse/delete edge cases)
- [ ] run `make lint`, `make erigon integration`, and the full `execution/commitment` test
      suite — must pass before verify task

### Task 6: Verify acceptance criteria

- [ ] verify all Overview requirements are implemented (multi-depth concurrent fold, cache
      removed, write linearization)
- [ ] verify edge cases: empty/collapsing subtrees, single-nibble accounts, whale storage
- [ ] run full package test suite under `-race`: `go test -race ./execution/commitment/...`
- [ ] confirm parity vs ModeDirect and ModeParallel holds across the suite
- [ ] confirm `make lint` clean and `make erigon integration` builds

### Task 7: [Final] Documentation + plan move

- [ ] update `execution/commitment` agents.md/CLAUDE.md notes only if a new durable pattern
      was introduced (split-point tree / arbitrary-depth fold)
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Items requiring manual intervention or external systems — informational only*

**Manual verification:**
- Benchmark the new fold vs `ModeParallel-fullfold` on a 750k whale (M-series): success bar
  is ties-or-beats ModeParallel with no cross-block cache. The deleted
  `Benchmark_NestedCacheWhaleRefold` was the prior harness; re-create an equivalent
  ModeParallel-vs-streaming comparison if a number is wanted.

**Working-tree hygiene (ralphex):**
- ralphex runs from a clean git state. Before running, ensure the worktree has no leftover
  experimental edits from the brainstorm session (the now-moot parallelization of the
  cache's dirty fold, the `sc.numWorkers` call site) and no unrelated pre-existing WIP that
  would taint a fresh run. Resolve/commit/stash these deliberately — do not let a task
  depend on them.
