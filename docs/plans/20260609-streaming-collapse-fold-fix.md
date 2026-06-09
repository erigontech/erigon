# Streaming-mode deep-collapse fold fix

Fix the confirmed, consensus-critical streaming-mode divergence documented in
`docs/plans/BLOCKER-streaming-collapse-in-populated-trie.md`: a two-block
incremental where a whale account's deep storage collapses (block 2 deletes 1/3
+ updates 1/3) while embedded among other accounts produces a wrong root and
wrong stored branches in `modeStreaming`/`modeStreamingScheduled`, while
`ModeDirect == ModeParallel` agree.

Package: `execution/commitment`. Branch: `awskii/parallel_prepare_fold`.

## Success criterion (the oracle)

`TestStreaming_MultiDepthCollapseParity` (already written, uncommitted, currently
RED) must go green at workers 1/4/8: `requireIncrementalEquiv` asserts
`seq == parallel == streaming == streaming-scheduled` root AND stored-branch
parity. The whale-only `TestStreaming_StorageCollapseAcrossSplit` and the
single-block `TestStreaming_MultiDepthSplitParity` (incl. `StorageSplits>0`,
`DeepLocalFolds>0`) must stay green. Validate the fix against `ModeParallel`
(both use concurrent commitment, so the comparison is Merkle-clean; raw `cm`
bytes also differ by step metadata, which is noise — compare decoded afterMaps
and the returned root, not bytes).

## Root cause (verified)

Streaming's mount-based synchronous fold does not reproduce the row-0
concurrent-fold property for an incremental collapse embedded in a populated
trie. Three sibling-preservation gaps, all in `streaming_split_fold.go` /
`streaming_deep_fold.go` / `streaming_commitment.go`:

1. **Flat first-nibble leaf** — `foldSubtreeAtPrefix` pre-mounts the worker
   (`activeRows=1`, `grid[0][col].reset()`), so `needUnfolding` sees an empty
   cell and never unfolds the on-disk storage branch → untouched on-disk
   siblings dropped on collapse. Parallel's `concurrentStorageRoot` per-child
   uses a fresh worker (`activeRows=0`) that bootstraps via `followAndUpdate`
   and unfolds from the root. **(Partially fixed in working tree:** the
   `len(parentPrefix)==64` leaf now routes through the proven `foldChildSubtree`;
   existing tests pass, but the split path below still diverges.)**

2. **Deep-split leaf** — under a storage-interior split (`childPrefix > 64`),
   `foldStorageChild`'s non-split leaf still calls `foldSubtreeAtPrefix` at the
   deep prefix, with the same pre-mount-drops-siblings defect.

3. **Deep-split aggregation** — `aggregateSubtreeRoot` builds the branch row with
   `afterMap = present` (touched children only); a grandchild subtree that is
   wholly untouched on disk is not in `present` and is dropped. Parallel avoids
   this because `concurrentStorageRoot` is flat (each first-nibble worker reads
   its whole on-disk subtree via unfold; the asm aggregation only ever needs the
   touched first-nibbles, and the collapse corpus touches all 16).

4. **Sole-account top-nibble at the root branch (re-validate)** — with the
   buggy storage, the whale (sole account under its top nibble, with a root
   branch above) is stored in the root branch with `hash=storageRoot`
   (`2e87f806…`) instead of the account RLP hash (`c542e8d6…`). Parallel uses the
   same `foldMounted`/stitch path and gets it right, so this is likely a
   consequence of the storage divergence feeding `setAccountStorageRoot`; **after
   fixing 1–3, re-run the oracle and confirm whether it persists.** If it does,
   diff streaming `foldSplit`/`dfsDeepLocal` against parallel
   `processMounted`/`dfsSubtreeDeep` step by step (`foldMounted` early-stop
   `activeRows==1 && depths[0]==1`, hex_patricia_hashed.go:2588 `// todo`).

## The fix: generalize the row-0 unfold-then-mount to depth

The proven pattern (parallel `processMounted`): unfold a `base` worker one level
at the on-disk root so its `grid[0]` carries every on-disk sibling, then each
child mounts onto `base.grid[0][nib]` (a cell with the on-disk hash, so its
first `needUnfolding` reads the on-disk subtree). The storage-interior split must
do the same at the split prefix: unfold the on-disk storage branch at the split
prefix so the touched grandchildren mount onto cells that carry on-disk hashes,
and untouched grandchildren are preserved in the afterMap. The deep leaf fold
must likewise mount onto an unfolded on-disk cell rather than a reset-empty one.

This is the design intent already stated in the parent plan ("reuse the row-0
concurrent-fold property recursively at every branch row"). The bug is that the
arbitrary-depth mount skipped the on-disk unfold that the row-0 mount gets for
free from `base`.

## Implementation Steps

### Task 1: Write the failing oracle (TDD red)

**Files:**
- Modify: `execution/commitment/streaming_multidepth_parity_test.go`

- [ ] Add `TestStreaming_MultiDepthCollapseParity`:

```go
func TestStreaming_MultiDepthCollapseParity(t *testing.T) {
	t.Parallel()
	wk1, wu1, wk2, wu2 := whaleCollapseCorpus()
	mk, mu := buildMixedCorpus(0xC0FFEE, 4000)
	k1 := append(append([][]byte{}, mk...), wk1...)
	u1 := append(append([]Update{}, mu...), wu1...)
	for _, w := range []int{1, 4, 8} {
		requireIncrementalEquiv(t, k1, u1, wk2, wu2, w)
	}
}
```

- [ ] Confirm it is RED for the right reason: `requireIncrementalEquiv` shows
      `parallel == seq` passes but `streaming`/`streaming-scheduled` diverge
      (the assertion message is "streaming(workers=N) vs sequential root
      mismatch"; `branchDiff` dumps whale-storage afterMap diffs).
- [ ] The flat-leaf fix in `streaming_split_fold.go` (`len(parentPrefix)==64 →
      foldChildSubtree`) is already committed; confirm
      `TestStreaming_StorageCollapseAcrossSplit` and
      `TestStreaming_MultiDepthSplitParity` stay green.

### Task 2: Unfold on-disk siblings at the storage-interior split prefix

**Files:**
- Modify: `execution/commitment/streaming_split_fold.go`
- Modify: `execution/commitment/streaming_deep_fold.go`

- [ ] In `aggregateSubtreeRoot` (and/or `foldStorageChild`'s split branch), read
      the on-disk branch at the split prefix and merge untouched grandchildren
      into the folded branch (afterMap = touched ∪ untouched-on-disk), mirroring
      how a normal `unfold` + `fold` preserves untouched children. Drop only
      children that collapsed to empty.
- [ ] Make the deep-split leaf (`foldStorageChild`, `len(parentPrefix) > 64`)
      mount onto an unfolded on-disk cell (carry the on-disk hash so the first
      `needUnfolding` reads the subtree) instead of `grid[0][col].reset()`.
- [ ] Run the oracle at workers=1; iterate on the decoded-afterMap diff vs
      parallel until storage branches match.

### Task 3: Re-validate the root/account hash; fix if it persists

**Files:**
- Modify: `execution/commitment/streaming_commitment.go` (only if needed)

- [ ] Re-run the oracle after Task 2. If only the root branch's sole-account cell
      differs (`hash=storageRoot` vs account hash), diff streaming `foldSplit` vs
      parallel `processMounted` and fix the `foldMounted` early-stop / stitch
      handling for a sole deep account under a top nibble.

### Task 4: Full validation

- [ ] `TestStreaming_MultiDepthCollapseParity` green at workers 1/4/8.
- [ ] `go test -run 'TestStreaming|TestDeepFold|TestVerifyParallel|TestAggregate' -race -count=20` green.
- [ ] `make lint` clean (run repeatedly), `make erigon integration` builds.
- [ ] Commit the now-green collapse parity test.

## Post-Completion
- Confirm the whale-bottleneck benchmark still shows the multi-depth concurrency
  win (the fix must preserve storage-interior splits, not disable them).
