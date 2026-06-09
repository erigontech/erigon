# Streaming-mode deep-collapse fold fix

Fix the confirmed, consensus-critical streaming-mode divergence: a two-block
incremental where a whale account's deep storage collapses (block 2 deletes 1/3
+ updates 1/3) while embedded among other accounts produces a wrong root in
`modeStreaming`/`modeStreamingScheduled`, while `ModeDirect == ModeParallel`
agree. See `docs/plans/BLOCKER-streaming-collapse-in-populated-trie.md` and
`docs/plans/FINDINGS-streaming-collapse-task2.md` (note: that FINDINGS doc's
"context-dependent / unfixable" conclusion is REFUTED below — do not act on it).

Package: `execution/commitment`. Branch: `awskii/parallel_prepare_fold`.

## Success criterion (the oracle)

`TestStreaming_MultiDepthCollapseParity` (committed, currently RED) green at
workers 1/4/8: `requireIncrementalEquiv` asserts `seq == parallel == streaming ==
streaming-scheduled`. `TestStreaming_StorageCollapseAcrossSplit` and
`TestStreaming_MultiDepthSplitParity` (incl. `StorageSplits`/`DeepLocalFolds`
seams) must stay green. Validate against `ModeParallel` (both concurrent → diff
decoded afterMaps + child hashes, not raw `cm` bytes which carry step noise).

## REFUTED: the divergence is NOT context-dependent — the target is well-defined

The prior attempt "proved" the correct whale storage root is context-dependent
(differs embedded vs whale-only) and therefore no embedding-insensitive fold can
match it. That proof is **wrong** — it used the *whale-only* incremental result
as ground truth, but that result is itself a degenerate single-account-root
artifact. Measured directly (workers=1, deterministic), `seq` incremental vs a
fresh single-block build of the same surviving state:

- **EMBEDDED: incremental == fresh** (`d6c4a67e…`). The embedded root is
  path-independent and well-defined; `parallel` also produces it.
- **WHALE-ONLY: incremental (`11732ba1…`) ≠ fresh (`94741c75…`)**. The whale-only
  incremental is the inconsistent one (degenerate single-account trie).

So the embedded correct answer is unique and an embedding-insensitive fold *can*
reach it. `ModeParallel` does, with a flat per-first-storage-nibble fold.

## Root cause (verified) — the per-first-nibble fold reads the wrong scope

`storageRootLocal` decomposes the whale storage into 16 first-nibble subtrees and
folds each independently, then `aggregateStorageRoot` stitches them. The
per-first-nibble fold reads the wrong amount of on-disk state on an incremental
collapse:

- `foldSubtreeAtPrefix` **pre-mounts at the account prefix (depth 64)** with
  `grid[0][col].reset()` (empty), so `needUnfolding` skips the on-disk branch →
  untouched on-disk siblings **dropped**. (Current baseline; wrong.)
- A **fresh worker bootstrapping from the trie root** (`foldChildSubtree`)
  unfolds the *entire* on-disk storage (all 16 first-nibbles) and returns the
  **whole storage root** for every first-nibble → the storage-root branch gets
  all 16 children equal. (Tried; wrong — reverted.)
- **CORRECT: mount each first-nibble at its own prefix `accountHash+nibble`
  (depth 65)** and unfold the on-disk branch *there*, confining the unfold to
  that child's own interior siblings, then fold this child's touched keys and
  return the depth-65 cell. This is exactly what `ParallelPatriciaHashed`'s inline
  fold achieves and what the prior attempt's `foldStorageLeaf` (mounted at
  `childPrefix`) was approaching — it closed most of the divergence before
  regressing on issues below.

With the deep split disabled and the leaf folded correctly, only the storage-root
branch + a few propagated account branches remain — i.e. the bug is entirely this
per-child scope, not anything context-dependent.

## The fix

Mount each independent storage subtree at its OWN prefix and unfold the on-disk
branch there (read-only), so the fold is correct for incremental collapse and
preserves exactly that subtree's untouched on-disk siblings — no more, no less.
The prior attempt got the structure largely right; reuse its working pieces and
resolve the three issues that blocked it (all recorded in FINDINGS):

1. **Mount at `childPrefix` (depth 65), not `parentPrefix` (64).** Mounting at the
   parent pulls in sibling subtrees owned by other concurrent workers → conflicting
   writes. `childPrefix` confines the unfold to this child. Use `readDiskBranch`
   (read-only `unfoldBranchNode` half) to seed the on-disk cell.
2. **Merge-order / `forceFullBranch`.** Once a storage fold unfolds on-disk data,
   incremental (touch⊂after) deferred branches merge out of fold order in
   `applyDeferred` and `BranchMerger.Merge` drops cells (panics). Force
   self-contained (touch|=after) branch encoding for unfolded storage folds;
   this cannot change hashes, only stored byte form.
3. **Cross-worker collapse (the wall).** When block-2 deletes collapse an interior
   branch that ALSO has untouched on-disk children handled by a different worker,
   the per-worker fold cannot see the cross-worker collapse → phantom child or
   re-added branch. The deep storage split (depth>64) is what introduces the
   cross-worker boundary; the flat per-first-nibble decomposition (depth-65 mounts,
   each first-nibble folded WHOLE in one worker) does NOT have this problem and
   already gives the storage concurrency win. **Strongly prefer** making the flat
   depth-65 decomposition correct first (it is the proven `ModeParallel` shape).
   If the `StorageSplits>0` seam then fails because no depth>64 split fires, that
   seam is asserting an unsound optimization on the collapse path — update it
   (keep asserting deep splits only on the single-block insert test, where they
   are correct). Re-introducing correct depth>64 splits is a follow-up, not a
   blocker for this fix.

## Implementation Steps

### Task 2: Mount each first-nibble storage subtree at its depth-65 prefix

**Files:**
- Modify: `execution/commitment/streaming_split_fold.go`,
  `execution/commitment/streaming_deep_fold.go`,
  `execution/commitment/hex_patricia_hashed.go` (add read-only `readDiskBranch`)

- [ ] Replace the per-child leaf fold so each first-nibble subtree mounts at
      `accountHash+nibble` (depth 65), unfolds the on-disk branch there, folds the
      child's touched keys, and returns the depth-65 cell (stripping the leading
      nibble the parent column carries). Mirror `ModeParallel`'s inline result.
- [ ] Add `forceFullBranch` (self-contained branch encoding) for storage folds
      that unfolded on-disk data, to avoid the `BranchMerger.Merge` out-of-order
      drop. Verify it changes no root hash.
- [ ] Run the oracle at workers=1; iterate on the decoded-afterMap+child-hash diff
      vs `ModeParallel` until storage branches match (target: 0 diff).

### Task 3: Resolve the deep-split / seam tension

- [ ] If the flat depth-65 decomposition passes the oracle but the deep storage
      split (depth>64) still diverges on collapse, gate the deep split to the
      non-collapse path or remove it from the collapse path, and adjust the
      `StorageSplits>0` seam: keep it on `TestStreaming_MultiDepthSplitParity`
      (single-block insert, where deep splits are correct), drop/relax it where the
      collapse path no longer deep-splits. Document the decision in the plan.

### Task 4: Full validation

- [ ] `TestStreaming_MultiDepthCollapseParity` green at workers 1/4/8.
- [ ] `go test -run 'TestStreaming|TestDeepFold|TestVerifyParallel|TestAggregate' -race -count=20` green.
- [ ] `make lint` clean (run repeatedly), `make erigon integration` builds.
- [ ] The collapse parity test (already committed) is now green; ensure no test
      was muted/skipped to get there.

## Post-Completion
- Confirm the whale-bottleneck benchmark still shows the storage concurrency win
  (the depth-65 per-first-nibble fan-out, ~16-way, is the proven win; deeper
  splits are a follow-up).
- User will test on a real mainnet chain after this lands.
