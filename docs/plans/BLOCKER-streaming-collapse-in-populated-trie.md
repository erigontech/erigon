# BLOCKER: streaming deep-collapse diverges when 2+ top-nibble splits exist

Found while writing Task 5 (multi-depth parity + race tests) of
`20260608-streaming-multidepth-fold.md`.

## Verdict: in-scope streaming-mode bug (NOT a core/ModeDirect bug)

An earlier version of this doc concluded the divergence was a "pre-existing
core-engine incremental-collapse divergence" surfaced (not introduced) by the
streaming work, and declared it out of scope. **That conclusion was wrong.**
Verified directly:

- Embedded mix-collapse corpus, workers=1, deterministic:
  `ModeDirect == ModeParallel` (two independent engines agree); `streaming` and
  `streaming-scheduled` are the lone outliers.
- Whale-only collapse: `ModeDirect == ModeParallel == streaming` all agree
  (so ModeDirect's whale-alone incremental is NOT off — the earlier "fresh vs
  incremental solo oracle" argument was a degenerate single-account-root
  artifact, exactly the unreliable minimal-test shape this doc earlier flagged).

So streaming is wrong; seq/parallel are right. The fix is in scope.

## Trigger (minimal)

Whale (30k slots, block-2 deletes 1/3 + updates 1/3) PLUS **at least one other
account in a DIFFERENT top nibble**. The divergence requires **2+ top-nibble
splits to exist on disk** (block 1). A sibling that *shares* the whale's top
nibble does NOT trigger it; a sibling in a different top nibble does. Block 2
touches only the whale.

- Block 1 is correct in all configs (whale storage byte-identical to seq).
- Block 2's whale fold is in-memory-identical regardless of the sibling
  (`DeepLocalFolds=1`, `StorageSplits=268`, storage root `2e87f806…` — correct).
- The bug appears only when a root-level branch exists (2+ top nibbles).

## Two distinct defects, both in streaming's mount-based synchronous fold

Verified by isolating each (streaming-vs-parallel, both concurrent commitment, so
the comparison is Merkle-clean — raw `cm` bytes also differ by step metadata,
which is noise; compare decoded afterMaps / the returned root, not bytes):

1. **Storage leaf fold drops/!collapses on-disk siblings.**
   `foldSubtreeAtPrefix` (execution/commitment/streaming_split_fold.go) pre-mounts
   the pooled worker (`activeRows=1`, `grid[0][col].reset()`, `depths[0]=pd`).
   Parallel's `concurrentStorageRoot` per-child uses a **fresh** worker
   (`activeRows=0`) that bootstraps via `followAndUpdate` and unfolds the on-disk
   branch from the root. Replacing the pre-mount with the parallel-style fresh
   fold makes **all** whale storage branches match parallel (0 Merkle-divergent),
   confirming this defect. (Disabling `isSplitPoint` does NOT fix it — it is the
   flat leaf path, not the split recursion.)

2. **A sole-account top-nibble subtree is mis-hashed at the root branch.**
   When the whale is the only account under its top nibble and a root branch
   exists, `foldSplit`/`foldMounted` returns the whale's split cell with
   `hash = storageRoot` (set by `setAccountStorageRoot`) and the early stop
   `activeRows==1 && depths[0]==1` (hex_patricia_hashed.go:2588, the existing
   `// todo potential bug`). When `base` folds the root branch, the whale child
   cell is hashed as if at depth 1 with `hash=storageRoot` instead of the account
   RLP hash. Decoded root branch, `ni=0` (whale): parallel `hash=c542e8d6…` (the
   account hash); streaming `hash=2e87f806…` (the storage root). This makes the
   **root hash wrong even after defect 1 is fixed**.

Both reduce to the same root cause: streaming's mount-based fold
(`mountTo`/`foldMounted` for the account trie; the hand-rolled pre-mount in
`foldSubtreeAtPrefix` for storage) does not reproduce the proven row-0
concurrent-fold property at depth, for an incremental collapse embedded in a
populated trie. The fix direction is the one in the plan's design intent:
fold each independent subtree on a fresh worker that bootstraps from the root
(as `concurrentStorageRoot`/`concurrentAccountRoot` do), rather than mounting,
and linearize the writes. This is consensus-critical; it needs the foldMounted
early-stop semantics handled, not guessed.

## Minimal repro (deterministic, workers=1)

```go
// whaleByNibble uses a fixed seed (424242): the whale is deterministic.
wk1, wu1, wk2, wu2 := whaleCollapseCorpus()
other := <address whose KeyToHexNibbleHash[0] != whale top nibble>
k1 := append([][]byte{other}, wk1...)
u1 := append([]Update{{Flags: BalanceUpdate /*bal=5*/}}, wu1...)
seq, _ := runIncremental(t, modeSeq,      0, k1, u1, wk2, wu2)
par, _ := runIncremental(t, modeParallel, 1, k1, u1, wk2, wu2)
str, _ := runIncremental(t, modeStreaming,1, k1, u1, wk2, wu2)
// seq == par ; str != seq  (and str-scheduled != seq)
```

## Status of Task 5

- `TestStreaming_MultiDepthSplitParity` (single-block multi-depth, parity vs
  ModeDirect AND ModeParallel, branch parity, seams) PASSES — committed-pending in
  `streaming_multidepth_parity_test.go` (untracked). It covers the headline goal.
- The multi-depth COLLAPSE/DELETE parity variant cannot pass until both defects
  above are fixed. Per repo policy a failing/weakened test is not committed.
