# Task 2 attempt findings — streaming deep-collapse fold

> **CORRECTION (supersedes the "context-dependent / unfixable" conclusion below).**
> The "DECISIVE DIAGNOSIS" point 5 — that the correct whale storage root is
> context-dependent (embedded ≠ whale-only) and therefore unfixable by an
> embedding-insensitive fold — is **WRONG**. It used the *whale-only* incremental
> result as ground truth, but that result is a degenerate single-account-root
> artifact: measured directly, `seq` whale-only incremental (`11732ba1`) ≠ a fresh
> single-block build of the same surviving state (`94741c75`), whereas `seq`
> EMBEDDED incremental == fresh (`d6c4a67e`). The embedded target is well-defined
> and `ModeParallel` reaches it with a flat per-first-nibble fold. The fix IS
> achievable — see `20260609-streaming-collapse-fold-fix.md`. The genuinely useful
> parts of this doc are the implementation pieces ("What was implemented", the
> `childPrefix` mount, `forceFullBranch`, the merge-order hazard) — reuse those.


Status: **not solved**. A full implementation attempt was reverted (it regressed
`TestStreaming_StorageCollapseAcrossSplit`, which must stay green). This records
what was tried, what worked, and the exact wall hit, so the next attempt does not
repeat the dead ends.

## DECISIVE DIAGNOSIS (2026-06-09) — Task 2 as written is provably insufficient

Instrumented the embedded-collapse oracle at workers=1 (deterministic). Findings,
each measured directly:

1. **Block 1 is correct everywhere.** Embedded streaming == seq for all 10839
   branches after block 1. Whale-only block 1+2 streaming == seq (0 diff). The
   divergence is entirely in block 2 of the *embedded* run.

2. **The bug is in the whale storage fold, not the account trie.** After block 2,
   embedded streaming vs seq: 9514 of 9514 whale **storage** branches differ
   (7570 with different afterMaps); only **3** account-trie branches differ, all
   with identical afterMaps (propagated-hash-only). The 809 mixed-corpus storage
   branches (small, flat path) are correct.

3. **Parallel (flat `concurrentStorageRoot`) is the exact oracle.** par-embedded
   vs seq-embedded storage: same=10323, diff=0 (byte-for-byte). Streaming-embedded
   vs par-embedded: 9514 differ. Parallel does **no** depth>64 split — it folds
   each first-storage-nibble from the on-disk root and is correct.

4. **The streaming deep-split fold is embedding-INSENSITIVE.** Streaming-embedded
   whale-storage branches are **byte-identical** to streaming-whale-only
   (same=9514, diff=0). The isolation pre-mount (`foldSubtreeAtPrefix`,
   `aggregateSubtreeRoot`) folds the storage subtree the same way regardless of
   what sits above the account.

5. **The CORRECT whale storage genuinely differs embedded vs whale-only.** Whale
   account leaf hash (= account RLP, embeds storageRoot, position-independent):
   - seq-embedded   = `c542e8d6…`  (correct; wraps the embedded storageRoot)
   - parallel-embedded = `c542e8d6…`  (correct)
   - streaming-embedded = `2e87f806…`  (WRONG — this is the *whale-only* storageRoot
     placed RAW as the leaf, i.e. BLOCKER defect 2, and it is the whale-only value
     not the embedded one)
   - seq-whale-only root = `11732ba1…` (≠ `c542e8d6…`)

   A deep storage branch hash (`…f09`, after=ffff in all) is `bdc7e8bf…` in
   seq-embedded but `5c75e48b…` in both seq-whale-only and streaming-embedded.
   The whale storage **slots are byte-identical** in both runs (sm: same=20000,
   diff=0) — yet the engine's incremental collapse yields a different storage
   structure depending on context (it is path/history-dependent; seq's 2-block
   incremental result also differs from a fresh single-block build of the same
   surviving state). seq==parallel pins this context-dependent result as ground
   truth.

**Proof Task 2 cannot work as specified.** The correct storageRoot is
context-dependent (point 5): embedded ≠ whale-only. Any embedding-insensitive fold
produces ONE result for both and therefore cannot equal both correct answers.
Whale-only already passes (streaming==seq there), so the single result the deep
split produces is the whale-only one — wrong for embedded. Task 2's prescription
(read the on-disk branch at the split prefix, merge untouched siblings) does NOT
add embedding-sensitivity: the on-disk storage is byte-identical embedded vs
whale-only (point 1), so merging it leaves the fold embedding-insensitive. Hence
it cannot fix the embedded case. This also explains the earlier whale-only
regression: the disk-merge perturbed a fold that was already producing the
(correct-for-whale-only) answer.

**What the real fix requires.** Reproduce parallel's proven behaviour: fold each
independent subtree **from the real on-disk trie root**, through the account-trie
context, so the fold is embedding-sensitive and matches seq/parallel — i.e. the
"generalize `foldMounted`'s early-stop to storage depth" direction the BLOCKER
named, NOT the isolation pre-mount. The flat `concurrentStorageRoot` path is the
reference. The open hard part is returning a folded cell at an arbitrary storage
**depth** (the split prefix) from a from-root fold, which today only `foldMounted`
does and only at depth 1. Note this is in direct tension with the
`StorageSplits>0` seam asserted by `TestStreaming_MultiDepthSplitParity` /
`…StorageCollapseAcrossSplit` / `…StorageInteriorSplits`: the proven-correct
oracle (parallel) does flat depth-64 folding with zero depth>64 splits.

(Reproduce with a throwaway test that runs seq/parallel/streaming over
`whaleCollapseCorpus()` embedded in `buildMixedCorpus(0xC0FFEE, 4000)` at
workers=1 and diffs `MockState.cm` storage branches + the whale account leaf hash.)

---


## What was implemented (then reverted)

1. `HexPatriciaHashed.readDiskBranch(prefix)` — read-only half of
   `unfoldBranchNode`: decode the pre-block on-disk branch at `prefix` into a
   `[16]cell` (fillFromFields + deriveHashedKeys at depth len(prefix)+1), `ok=false`
   when no branch exists there. This helper is correct and reusable.

2. `aggregateSubtreeRoot` disk-merge: seed `grid[1]` with on-disk siblings for
   nibbles not in `present`, compute `after = diskBitmap` then add touched-nonempty /
   remove collapsed, so untouched on-disk grandchildren survive the fold.

3. `forceFullBranch` worker flag (set in `newStorageWorker`): make `foldBranch`
   force `touchMap |= afterMap` (full, self-contained branch encoding) even when
   `branchBefore` is true. **Required** once a storage fold unfolds on-disk data,
   because incremental (touch⊂after) deferred storage branches merge OUT OF FOLD
   ORDER in `applyDeferred`, and `BranchMerger.Merge` then drops cells → a stored
   branch with `afterMap=ffff` but 15 cells → `index out of range` panic in
   `Merge` (commitment.go:1242) on the next block that reads it as `prev`.
   `forceFullBranch` cannot change root hashes (hashes come from afterMap cells),
   only the stored byte form.

4. Leaf rewrite: replaced `foldSubtreeAtPrefix(parentPrefix,…)` with
   `foldStorageLeaf(childPrefix,…)` mounted at **childPrefix** (not parentPrefix),
   unfolding the on-disk branch there. Rationale: mounting at parentPrefix and
   unfolding pulls in the parent branch's SIBLING subtrees, which are owned by other
   concurrent workers → conflicting writes (verified: streaming resurrected a child
   seq deleted). Mounting at childPrefix confines the unfold to this child's own
   subtree, mirroring the split aggregate's cell/`child.ext` convention. Also added
   `prependChildExt` (prepend, not overwrite, to survive collapse-to-single tails).

## What the attempt achieved

- Non-collapse parity stayed green: `TestStreaming_MultiDepthSplitParity`,
  `TestFoldStorageLeaf_MatchesDepth64` (renamed unit test), `TestDeepConcurrent_WhaleParity`.
- The oracle's structural divergence largely closed: most divergent branches went
  from dropped-sibling afterMaps to **identical afterMaps** (only cosmetic full-vs-
  incremental byte differences) — i.e. sibling preservation worked.

## The wall (unsolved)

- `TestStreaming_StorageCollapseAcrossSplit` (whale-only collapse, previously green)
  **regressed** to a wrong root. The disk-merge changes the computed root even where
  it should be a no-op. `forceFullBranch` is ruled out (can't affect hashes), so the
  remaining ~21 oracle divergences and the whale regression are the same class:
  **cross-worker collapse propagation**. Block 2 deletes 1/3 + updates 1/3, leaving
  1/3 untouched. At an interior storage branch some children are touched (folded by
  one worker) and some are untouched-on-disk; when block-2 deletes collapse a branch
  that ALSO has untouched on-disk children handled implicitly, the per-worker fold
  cannot see the cross-worker collapse, so streaming either keeps a phantom child or
  recomputes a branch seq removed.

- This matches the repo's own note that "two-pass split-reader collapse detection is
  intrinsic" (memory: witness-builder-redesign) and the BLOCKER's explicit warning
  that this is consensus-critical and "needs the foldMounted early-stop semantics
  handled, not guessed."

## Concrete next steps to try

1. Understand why the OLD code passes whale-only collapse while (apparently)
   dropping untouched siblings — instrument `whaleCollapseCorpus` to print, per
   interior split branch, `present` (touched) vs the on-disk child bitmap. The
   hypothesis "the whale is dense so present==surviving" must be confirmed or
   refuted; if refuted, the OLD code has a different sibling-preservation mechanism
   that the new code must not bypass.
2. Decide sibling preservation at the RIGHT layer. The split aggregate's `present`
   is the touched prefix-trie bitmap; a fully-untouched on-disk child subtree is not
   in the prefix trie at all, so no worker folds it and the aggregate never sees it.
   Preserving it requires reading the on-disk branch (done) AND keeping its cell hash
   AND propagating collapse when the surrounding touched children all delete. The
   merge-order hazard (point 3 above) means this must be self-contained per branch.
3. Validate every step against `ModeParallel` (which matches seq) by decoded-afterMap
   diff at workers=1, not raw `cm` bytes.

## Debug aids used (remove before commit)

- `validateBranchWellFormed(BranchData)` + DBG prints around `encodeDeferredUpdate`'s
  `Merge` in commitment.go pinpointed the malformed-branch panic source.
