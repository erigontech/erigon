# Plan — mode-D emit split: aligned wide file + stub v4

**Date:** 2026-08-15 (title corrected 2026-08-17 to mode-D)
**Branch:** `merge/main-into-feat-snapshot-flow-20260731`
**Supersedes:** the `subsumedV4ItemsFromMergeLocked` extension direction.

**Mode terminology.** The doc originally called this a "mode-C" fix. Under the refined mode-A/B/C/D taxonomy (2026-08-17), the split-emit fires specifically for **mode-D**: unwind target lands inside a merged/multi-step retired `.kv` file. **Mode-C** — target lands in a single per-step `.kv` file — needs only a stub v4 emit; there's no wider range to preserve so the aligned peer would collapse to a 0-width file. The code gates the split on `fileEntry.FromStep < targetStep` for exactly this reason.

## Problem

Mode-D emits a single v4 boundary file spanning `[fromStep*stepSize .. lastTxN+1]` — a range that typically covers MANY full steps AND a partial head step (the step containing the unwind target).

Post-emit, retire runs for the head step (once forward-exec crosses the next step boundary) and produces a step-aligned per-step file whose range `[stepBoundary*stepSize .. (stepBoundary+1)*stepSize)` overlaps the v4's tail `[stepBoundary*stepSize .. lastTxN+1]`.

The two files land in the aggregator's visible set together. `getLatestFromFile`'s (data, accessor) pairing in the overlap window mixes an offset from one file's `.kvi` into another file's `.kv` mmap, and reads land past valid data — **SIGBUS in `eliasfano16.get2`** (iter 4 mode_a in the current soak).

The merge-side v4 retirement (`subsumedV4ItemsFromMergeLocked`, added in `0738a47986`) doesn't help — merge won't produce a wider file that contains a v4 boundary (`2be971b96b`'s merge guard), so the merge that WOULD retire v4 never fires.

## Fix

Instead of one mid-step v4 emit, produce TWO files whenever the unwind target is mid-step:

1. **Aligned wide file** — `v2.2-commitment.<startStep>-<targetStep>.kv` etc. Step-aligned endTxN = `targetStep * stepSize`. Behaves as a normal wide step file (like a merged 288-301). Content: state as-of `targetStep*stepSize - 1` (last txN of the step BEFORE the target's step). Subject to normal merge; no v4 semantics.

2. **Stub v4** — `v4.0-commitment.<targetStep*stepSize>-<lastTxN+1>.kv`. Mid-step endTxN. Content: state as-of `lastTxN` for keys touched in `(targetStep*stepSize .. lastTxN]`. `startTxN == targetStep*stepSize` → `v4FilesForStep(targetStep)` picks it up when retire produces the target-step per-step file; wholly contained in target-step's retire range → `retireSubsumedV4ItemsInRange` retires it atomically with that retire's `IntegrateDirtyFiles`.

**Invariant restored**: v4 files always start at a step boundary AND end within one step of the boundary. All existing v4 machinery keeps working with the stub-only shape.

## Emit sequencing

Two computes in series, second reuses first's on-disk aligned file as its baseline:

1. Compute #1: `RecomputeAtTxNumWithoutSD(lastTxN = stepBoundaryTxN)` where `stepBoundaryTxN = targetStep*stepSize - 1`. Full walk from prior baseline.
2. Emit aligned file to disk.
3. Notify aggregator (`NotifyOnFilesChange([aligned])`) + `ForceReopenUnderlyingFilesTx()` on the outer tx so compute #2's tx sees the aligned file as its file-side baseline.
4. Compute #2: `RecomputeAtTxNumWithoutSD(lastTxN = originalLastTxN)`. Uses aligned as baseline (walks only the head-step delta).
5. Emit stub v4 to disk.

Cold-start / step-aligned target: the split gate is `lastTxN+1 % stepSize != 0`. When target IS step-aligned, `stepBoundaryTxN == lastTxN` and there is no head-step content — emit a single aligned file only, skip the stub. This preserves the existing step-aligned unwind path.

## Data structures

Extend `commitmentRecomputeResult` (in `provider_unwind_commitment.go`):
```go
type commitmentRecomputeResult struct {
    // Existing (target-txN compute):
    lastTxNum        uint64
    encodedTrieState []byte
    branches         *etl.Collector // consumed by Apply
    regenBranches    *etl.Collector // consumed by v4 emit (=> stub emit)
    // New (step-boundary compute):
    alignedTxNum        uint64        // stepBoundaryTxN
    alignedEncodedState []byte        // anchor for aligned file
    alignedRegenBranches *etl.Collector // consumed by aligned emit
}
```

`Close` releases all three collectors.

## Emit switch in `regenerateBoundaryStepFiles`

Replace the single `WriteCommitmentBoundaryFileV4` / `WriteStateBoundaryFileV4` call in the `actionRegenTruncate` branch with two calls:

```go
case action == actionRegenTruncate && !boundaryAligned:
    // Split emit: aligned wide + stub v4.
    alignedPath := agg.DomainKVFilePath(kvDomain, fromStep, targetStep)   // step-aligned name
    stubPath    := agg.DomainKVFilePathV4(kvDomain, targetStep*stepSize, lastTxN+1)  // stub v4 name

    // Emit aligned
    if kvDomain == kv.CommitmentDomain {
        WriteCommitmentBoundaryFileV4(ctx, recompute.alignedRegenBranches,
            recompute.alignedEncodedState-derived-anchor, alignedPath+".regen", ...)
    } else {
        walker := historyKeyWalker(tx, kvDomain, fromTxN, stepBoundaryTxN)
        WriteStateBoundaryFileV4(ctx, kvDomain, walker, lookup, stepBoundaryTxN,
            alignedPath+".regen", ...)
    }
    // Register aligned + refresh tx view (see "aggregator mid-unwind mutation" below).

    // Emit stub
    if kvDomain == kv.CommitmentDomain {
        WriteCommitmentBoundaryFileV4(ctx, recompute.regenBranches, anchor,
            stubPath+".regen", ...)
    } else {
        walker := historyKeyWalker(tx, kvDomain, stepBoundaryTxN+1, lastTxN)
        WriteStateBoundaryFileV4(ctx, kvDomain, walker, lookup, lastTxN,
            stubPath+".regen", ...)
    }

    pairs = append(pairs, regenPair{aligned}, regenPair{stub})
```

## Aggregator mid-unwind mutation

Between compute #1 and compute #2, aligned file must become visible to the outer tx's baseline lookups.

Concretely:
1. After all per-domain aligned files are emitted (all five domains: accounts, storage, code, commitment, receipt), rename `<path>.regen` → `<path>` for each. Standard rename dance already exists in `provider_unwind_finalize.go`.
2. `p.Aggregator.NotifyOnFilesChange(alignedNames)` — puts them into `dirtyFiles` and triggers a `recalcVisibleFiles` on the aggregator's live view.
3. `opts.Tx.(kv.CanReopenUnderlyingFilesTx).ForceReopenUnderlyingFilesTx()` — re-pins the outer tx's aggregator view to the fresh generation that includes the aligned files. Same pattern `execution/stagedsync/stage_snapshots.go:427` uses post-download.
4. Run compute #2. `GetLatestFromFilesUpToStep(kv.CommitmentDomain, KeyCommitmentState, maxStep=targetStep-1)` now returns the aligned file's anchor as baseline.

## Finalize path

`pendingRegenState` currently tracks one regen pair per domain. Extend to two:
- Aligned pair (regenPath, finalPath, oldPath, domain, isAligned=true)
- Stub pair   (regenPath, finalPath, oldPath, domain, isAligned=false)

`FinalizeUnwind` walks both, does the rename dance for each, publishes to Inventory, republishes chain.toml. Same mechanism, twice per domain.

`AbortUnwind` unwinds both — deletes any `<path>.regen` staged files.

## Test surface

Reuse existing test scaffolds — same `WriteCommitmentBoundaryFileV4`/`WriteStateBoundaryFileV4` are called, just twice with different (lastTxN, path). Two new unit tests:

- `TestRegenerateBoundaryStepFiles_SplitEmitsAlignedPlusStub`: mid-step target → assert two files land per domain, aligned has step-aligned endTxN, stub has raw-txN endTxN.
- `TestRegenerateBoundaryStepFiles_AlignedTargetSkipsStub`: step-aligned target → assert one aligned file, no stub.

End-to-end via soak: iter 4 `mode_a` after iters 1-3 mode_b (matches the current SIGBUS repro seed).

## Files touched

- `node/components/storage/provider_unwind_commitment.go` — `commitmentRecomputeResult` extension + `ensureCommitmentAtBlockCompute` runs two computes.
- `node/components/storage/provider_unwind_state_regen_wire.go` — split emit switch; aggregator mid-unwind refresh helper.
- `node/components/storage/provider_unwind_finalize.go` — walk both pairs per domain.
- `node/components/storage/provider_unwind_state_plan.go` — `pendingRegenState` shape (or a parallel `alignedPairs` field).
- `db/state/aggregator.go` — expose `DomainKVFilePath` (step-aligned) if not already public; verify `NotifyOnFilesChange` is safe to call mid-unwind.

## Non-goals

- Not touching `rangeContainsV4Item` (merge guard), `subsumedV4ItemsFromMergeLocked`, `retireSubsumedV4ItemsInRange`, `v4FilesForStep`, `isRawTxNItem`. All existing v4 machinery is correct for the stub-only shape.
- Not changing the target semantic (mode-B setHead still targets a specific block; unwind target still lands mid-step in general).

## Verification

1. `make lint && make erigon integration` clean.
2. Unit tests in `node/components/storage/` all green.
3. Fresh soak with seed `43dcf1e6382e4efca72d1966aa98ddeb`. Expected: iters 1-3 mode_b still pass, iter 4 mode_a no longer SIGBUS, soak continues.
4. Post-soak disk state: `find snapshots/domain -name 'v4.0-*-*.kv'` shows only stub v4s whose range is confined to one step; no wide-spanning v4s.
