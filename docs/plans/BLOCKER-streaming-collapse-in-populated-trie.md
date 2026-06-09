# BLOCKER: streaming concurrent deep-collapse diverges when the whale shares the trie with other accounts

Found while writing Task 5 (multi-depth parity + race tests) of
`20260608-streaming-multidepth-fold.md`.

## Symptom

Two-block incremental:
- Block 1: a whale account (30k storage slots, deep-folded via `storageRootLocal`)
  **plus at least one other account**.
- Block 2: touch only the whale — delete 1/3 of its slots, update 1/3, leave 1/3
  on disk (the existing `whaleCollapseCorpus`).

Result (workers=1, fully deterministic):
- `ModeDirect`  == `ModeParallel`  ✓
- `modeStreaming`          != sequential  ✗
- `modeStreamingScheduled` != sequential  ✗

The divergent stored branches are all under the **whale account hash** (verified by
decoding the compact branch keys): for whale storage-interior branches streaming
encodes the WRONG present-children set (afterMap) — it leaves stale extra child bits
that the sequential/parallel engines collapse/drop on the block-2 delete. So the
defect is a wrong folded afterMap on collapse, not a missing PutBranch call.

## What is and isn't the cause

- NOT an invalid corpus: `ModeParallel` folds it to the sequential root and branch
  set. Only the streaming engine diverges.
- NOT stale pooled-grid state: a full `grid` clear in `resetForReuse` (every pooled
  worker starts zeroed, identical to a fresh worker) does **not** fix the
  mixed-account case. (A grid clear *did* fix a storage-less single extra account,
  so there may be two interacting issues; the dominant one is logic, not grid.)
- Single-block multi-depth (`TestStreaming_MultiDepthSplitParity`, 6000 mixed
  accounts WITH storage + whale) is CORRECT, including branch parity — so block-1
  whale storage is written correctly even with other accounts present. The defect
  is in the **block-2 incremental collapse** path.
- The existing Task-4 test `TestStreaming_StorageCollapseAcrossSplit` passes because
  its whale is the SOLE account — the account trie has no branch above the whale, so
  the failing interaction never arises.

## Minimal repro

```go
wk1, wu1, wk2, wu2 := whaleCollapseCorpus()
mk, mu := buildMixedCorpus(0xC0FFEE, 1) // ONE extra account is enough
k1 := append(append([][]byte{}, mk...), wk1...)
u1 := append(append([]Update{}, mu...), wu1...)
seq, _ := runIncremental(t, modeSeq, 0, k1, u1, wk2, wu2)
str, _ := runIncremental(t, modeStreaming, 1, k1, u1, wk2, wu2)
// bytes.Equal(seq, str) == false
```

## Suspected mechanism (not yet proven)

In `foldSplit` the main worker `w` uses the REAL trie ctx (not the discard-overlay
that `foldKeys` uses), and the deep storage workers from `newStorageWorker` also
write to the same shared store. When the whale shares its top-nibble subtree with
another account, `w` must fold the whale account leaf up THROUGH an account-trie
branch (reading the sibling account from DB and preserving it), while
`storageRootLocal`'s workers concurrently self-flush whale-storage-branch DELETIONS
to the same store and stage deferred updates. The end-of-block
`mergeDeferredByPrefix` + `applyDeferredGuarded` apparently loses/over-rides some of
those storage-branch deletions in this configuration. The whale-only case never
exercises the account-trie-sibling fold above the whale, which is why it passes.

This is the Task-4 "write linearization at every split node" path; the linearization
is correct for an isolated whale but not for a whale embedded in a populated trie.

## Why this is serious

It is a correctness (consensus-affecting) divergence in streaming mode, not a test
artifact. It needs a real fix in the streaming deep-collapse linearization, with a
careful eye on regressing the Task 1-4 work.

## Status of Task 5

- `TestStreaming_MultiDepthSplitParity` (single-block, multi-depth, parity vs
  ModeDirect AND ModeParallel, branch parity, StorageSplits/DeepLocalFolds seams) is
  written and PASSES — it covers the headline Task-5 goal.
- The multi-depth COLLAPSE/DELETE parity test cannot pass until this bug is fixed.
  Not committed as a failing/weakened test (repo rule: agents must not skip or mute).

## Deeper investigation (2026-06-09)

Localized the divergence precisely. The repro is `whaleCollapseCorpus` (30k-slot whale,
block-2 deletes 1/3 + updates 1/3) plus `buildMixedCorpus(0xC0FFEE, 4000)`; the whale's
account hash is `000b0008...`. Findings, all from the account-touched corpus (the
account IS touched in block 2, so these are not the "untouched-account" artifact):

- **The divergence is the whale storage root, and it is correct in seq/parallel, wrong
  in streaming.** The depth-64 storage-root branch (prefix = 64-nibble account hash) has
  child0 hash `7380…` in ModeDirect-mix, ModeParallel-mix, AND a fresh single-batch
  commit of the final state (both whale-alone and whale+mixed). Streaming produces
  `ffc0…`. Three independent oracles agree on `7380…`, so **`7380…` is the true value and
  streaming's `ffc0…` is wrong.**
- **It is systematic, not a single collapsed branch.** ALL 16 first-storage-nibble
  subtree roots differ between streaming and seq/parallel (child0 `ffc0` vs `7380`,
  child1 `7a27` vs `6d90`, …). So this is a whole-storage-fold difference, not one
  mis-collapsed afterMap. The earlier "stale extra child bits" symptom is downstream of
  this.
- **Underlying state is byte-identical.** Final `sm` whale storage: 20000 slots, 0 value
  diffs between solo and mix. Block-1 whale storage `cm` is byte-identical (diff=0). So
  this is a pure commitment-fold divergence, not a state/input difference.
- **Streaming faithfully reproduces ModeDirect's *whale-alone* incremental result.**
  ModeDirect-solo incremental gives `ffc0` (== streaming), but a fresh single-batch
  commit of the same final solo state gives `7380`. So ModeDirect's OWN incremental
  result for a whale-alone deep-storage collapse is inconsistent with a fresh commit;
  embedding the whale with other accounts is what makes seq/parallel produce the correct
  `7380`. This is why `TestStreaming_StorageCollapseAcrossSplit` (whale-only) passes — it
  compares streaming against a ModeDirect-solo reference that is itself off, so both agree
  on the wrong value. The bug surfaces only when the whale is embedded, where the
  reference becomes correct.
- **Ruled out as the cause:** (a) the Task-3 recursive multi-depth split — forcing
  `isSplitPoint` to return false (flat per-nibble fold only) does NOT fix it; (b) the
  Task-3 mount-at-prefix mechanism — swapping the flat fold back to the Task-2
  unfold-from-root `foldChildSubtree` does NOT fix it. The divergence is deep inside each
  first-nibble subtree's incremental-collapse fold, independent of the split scheduler and
  the mount primitive.

**Assessment.** This is a deep, pre-existing core-engine incremental-collapse divergence
(ModeDirect's whale-alone incremental fold disagreeing with a fresh commit) that the
streaming multi-depth work *surfaced* rather than introduced. A correct fix touches the
shared fold/collapse semantics (consensus-critical) and is out of scope for this plan's
streaming-only tasks; per repo `CLAUDE.md` it should not be guessed at. Needs a focused,
separately-scoped investigation of the core fold collapse path with the oracle being a
fresh single-batch commit (not the ModeDirect-solo incremental reference).
