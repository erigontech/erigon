# Plan: Universal "two v4 files then background merge" file lifecycle

**Date:** 2026-08-25
**Branch:** `merge/main-into-feat-snapshot-flow-20260731`
**Design owner:** user (via 2026-08-25 session)

## Contract

Applies to EVERY retire-produced file — state (`.kv`, `.v`, `.ef`, `.efi`, `.vi`, `.bt`, `.kvei`, `.kvi`) AND block snapshots (`.seg` for headers/bodies/transactions plus `.idx`/`-to-block.idx`):

- **v4 #1** = a raw-boundary file with `endTxN` (state) or `to` (block) that is non-aligned to the class's natural chunk. Written by mode-C/D unwind emission.
- **v4 #2** = a raw-boundary file with `startTxN` / `from` that is non-aligned to the class's natural chunk (and equal to v4 #1's non-aligned edge). Written by the ordinary retire buildFiles / dumpBlocksRange path when it finds an existing v4 #1 already occupying the aligned chunk.
- **Merge** = the existing background merge machinery (state: `Aggregator.mergeLoop`, block: `BlockRetire.MergeBlocks`) recognises the (v4 #1, v4 #2) pair as a mergeable range whose union is the aligned chunk. Output is the standard step-aligned/1000-aligned file; two v4 inputs go through the ordinary retire-visible-swap.

**Absolute invariants**:
- No file class EVER seeds its tail into MDBX. Frozen data belongs in files.
- No inline merge during collate (state side) or during trim (block side). Merge is always background.
- `chunkAlignedToBlock` (block-side forcing function) and `mergeV4IntoStepFile` (state-side inline compose) both go away.
- Read-side pre-merge consults both v4 files; overlap-dedup from commit `84eabbdd61` applies universally.

Maps onto the mode taxonomy (memory/mode-abcd-taxonomy-2026-08-17):
- mode-B = shadow-only unwind, never touches files
- mode-C/D = file-touching unwind, emits v4 #1 files, never seeds file data into MDBX
- retire + merge = only path that writes standard aligned files, always background

## 1. Naming scheme — two-edge v4, one convention shared by state and block

Today's `IsRawTxN` predicate (`db/state/dirty_files.go:207`) only checks `endTxNum % stepSize != 0`. That catches v4 #1 but silently misclassifies v4 #2 (whose `endTxNum` IS aligned, only `startTxNum` is not) as a legacy step-form file. Fix: extend to either-edge.

### State side (already partially wired, extend)

Current v4 #1 (unwind emits) — unchanged shape:
- `v4.0-accounts.{alignedStartTxN}-{lastTxN+1}.kv` / `.v` / `.ef` / `.vi` / `.efi` / `.bt` / `.kvi` / `.kvei`

Example (stepSize=1024, unwind target lastTxN=3097): v4 #1 covers `[3072, 3098)`:
- `v4.0-accounts.3072-3098.kv`, `.v`, `.ef`, plus accessor sextet

New v4 #2 (retire tail emits) — same v4.0 prefix, non-aligned `from`:
- `v4.0-accounts.{lastTxN+1}-{alignedEndTxN}.kv` / `.v` / `.ef` / ...

Same example (stepEnd = 4096): v4 #2 covers `[3098, 4096)`:
- `v4.0-accounts.3098-4096.kv`, `.v`, `.ef`

Post-merge: standard `v2.0-accounts.3-4.kv`, `v1.0-accounts.3-4.v`, etc.

Predicate change (only extend `IsRawTxN`):
- Any FilesItem where `startTxNum%stepSize != 0 || endTxNum%stepSize != 0` is a v4.
- `filterDirtyFiles` at `db/state/dirty_files.go:504-509` already dispatches on `fileVer.GreaterOrEqual(version.TxNumNamingPivot)` — untouched.

### Block side (new, no version bump needed)

The block-file parser `snaptype.ParseFileName` at `db/snaptype/files.go:198-234` has a "dual-mode" branch: if either of the two range strings is > 6 chars, both are treated as raw block numbers instead of "step * 1000". Force v4 filenames into the raw branch by ALWAYS zero-padding both endpoints to 7 digits.

Example (block target = 3491690, aligned chunk = [3491000, 3492000)):
- v4 #1 (unwind tail) covers `[3491000, 3491691)`:
  - `v1.1-3491000-3491691-headers.seg`, `-bodies.seg`, `-transactions.seg` + `.idx` and `-to-block.idx`
- v4 #2 (retire tail) covers `[3491691, 3492000)`:
  - `v1.1-3491691-3492000-headers.seg` + siblings
- After merge: standard `v1.1-003491-003492-headers.seg` (legacy 6-char zero-pad form).

Rationale for staying on the length-heuristic branch instead of a v2.0 block pivot:
1. `snaptype.ParseFileName` already handles both branches correctly; no changes to schema-versioned parsers or downloader manifests.
2. `ParseRange` (`db/snaptype/files.go:256`) already returns raw block coordinates from either branch — no consumer needs updating.
3. Every consumer that assumes "/1000" is safely bypassed because both endpoints are literally 7 digits.
4. Devnet-scale (blocks < 1M) already needs zero-padding under this rule.

Sentinel predicate for block v4:
- Add `snaptype.FileInfo.IsRawBlock()` returning `f.From%uint64(snaptype.Erigon2MinSegmentSize) != 0 || f.To%uint64(snaptype.Erigon2MinSegmentSize) != 0`. Placed in `db/snaptype/files.go` near `IsCorrectFileName`.

## 2. State-side changes (file-by-file)

### 2A. Remove inline merge from retire buildFiles
- `db/state/history.go` — `History.buildFiles` (827-928): delete the block at 879-892 (`h.mergeV4IntoStepFile(...)` + `[dbg-buildfiles-v4]` warn).
- `db/state/history.go` — `History.collate` at 573-574: new helper `historyRetireDestPaths(step)` chooses v4 #2 paths when a v4 #1 exists for the step, else the standard step-aligned paths.
- Same treatment for `Domain.collate` and standalone `InvertedIndex.collate`.
- `db/state/history_v4_compose.go`: delete `mergeV4IntoStepFile`. Keep `historyEFVCursor` if the merger's adapter uses it; otherwise delete along with `mergeV4AndMDBXHistoryFiles`.

### 2B. Retire tail emission uses v4-#2 naming when v4 #1 is present
- New helpers `historyRetireDestPaths`, `domainRetireDestPaths`, `iiRetireDestPaths` — read `dirtyFiles.V4FilesForStep(stepSize, step)` and return either (v4 #2 pair) or (standard step-aligned pair).
- `IntegrateDirtyFiles` at `db/state/aggregator.go:1608+` already handles arbitrary (from, to) — no change.

### 2C. Extend the merge scheduler to see v4 pairs
- `db/state/dirty_files.go`:
  - `FilesItem.IsRawTxN` at line 207: broaden to `i.startTxNum%stepSize != 0 || i.endTxNum%stepSize != 0`.
  - Add `DirtyFiles.V4PairForStep(stepSize, step) (v41, v42 *FilesItem, ok bool)`.
- `db/state/merge.go`:
  - Replace the `rangeContainsV4Item` guard at line 210 with `v4PairMergeRange` positive-detection.
  - New helper scans for the (v41, v42) topology and returns the step-aligned range.
- `db/state/aggregator.go`:
  - `mergeLoopStep` at line 1501: `findMergeRange` picks up new v4-pair candidates. `mergeFiles` sees both v4 items as inputs — v4 files are ordinary EF+.v containers with raw txN coordinates, no v4-specific decode path.

### 2D. Read-side updates
- `db/state/dirty_files.go` — `FileNameMaskForItem` at 278 already uses raw endpoints when `IsRawTxN`; broadening auto-routes v4 #2.
- `db/state/history_key_txnum_range.go` — `iterateKeyTxNumFrozen` at 30: walks `ht.iit.files` in visibleFiles order; v4 pieces appear as visible files with disjoint startTxN. No change.
- `db/state/dirty_files.go` — `calcVisibleFiles` at 810 → CullPlan: verify both v4 items survive visibility. Add `TestCalcVisibleFiles_V4Pair_BothVisible`.

### 2E. Delete now-obsolete code
- `db/state/history_v4_compose.go`: delete `mergeV4IntoStepFile`; conditionally delete `mergeV4AndMDBXHistoryFiles`, `openHistoryEFVCursor`.
- `db/state/history.go` 879-892: inline-merge block.
- `db/state/merge.go` — `rangeContainsV4Item` at 210: delete.
- `db/state/aggregator.go` — `subsumedV4ItemsForStepLocked`: audit + remove obsolete callers.

## 3. Block-side changes

### 3A. Stop forcing 1000-alignment during rebuild
- `node/components/storage/provider_unwind_snapshot_rebuild.go`:
  - `chunkAlignedToBlock` at 54: DELETE.
  - `rebuildBlockStraddles` at 192: `newToBlock = toBlock + 1` unconditionally.
  - Delete the 1000-alignment guards in `sliceStraddleSeg` (680-682) and `rebuildTransactionsStraddleFile` (832-834).
  - Delete `seedLeftoverBlocks` (413) and `computeTDAnchor` (333) — block data belongs in files.
- `node/components/storage/provider_unwind_snapshot_trim.go`:
  - `unwindSnapshotsPastBlock`: `newTo := toBlock + 1` at line 73.
- `db/snaptype/files.go`:
  - Add `Type.FileInfoV4(dir, from, to)` returning a FileInfo whose Name/Path uses `%07d-%07d`.
  - Alternatively extend `Type.FileInfo` to auto-select zero-pad width based on whether either endpoint is 1000-aligned.

### 3B. Retire tail emission
- `db/snapshotsync/freezeblocks/block_snapshots.go`:
  - New helper `chooseRetireTailRange(snapshots, snapDir, blockFrom, defaultBlockTo) (from, to, isV4Tail)`.
  - Scan for a v4 #1 whose `From == blockFrom` and `To < alignedChunkEnd`; if found return `(v41.To, alignedChunkEnd, true)`.
  - `dumpBlocks` calls it before `dumpBlocksRange`; the FileInfo generation switches to `FileInfoV4` when isV4Tail.

### 3C. Extend the block merger to recognise v4 pairs
- `db/snapshotsync/merger.go`:
  - `FindMergeRanges` at 41: extend to recognise `[from, midTo) + [midTo, alignedEnd)` pair whose union is 1000-aligned as a mergeable range.
  - `filesByRangeOfType` at 77: v4 pair members fall inside the aligned range by construction, selected naturally.
  - `merge` at 332: concatenates entries in from-order — v4 #1 provides prefix, v4 #2 provides suffix; merged output tiles the aligned chunk exactly.

### 3D. Read-side updates
- `db/snapshotsync/freezeblocks/block_reader.go`: `HeaderByNumber` etc. resolve via `View.Segments(t)`. As long as visibility includes v4 pieces (§3E), no change.
- `db/snapshotsync/snapshots.go`:
  - `NoGaps` at 89: verify `[from1, to1), [to1, to2)` reports no gap.
  - `recalcVisibleFiles`: v4 pair members are overlap-free (they abut), both should survive.

### 3E. Inventory + segment scan
- `node/components/storage/snapshot/inventory.go`: `snaptype.ParseFileName` returns correct From/To via the 7-char literal branch.
- `db/snapshotsync/snapshots.go` — `openSegments`: scans via `ParseFileName`, gets raw block coordinates. Add test.
- Accessor names for v4-shaped `.seg` must use the same 7-char format. Update `snaptype2.Type.IdxFileNames` or the FileInfo → IdxFileName path.

### 3F. Delete now-obsolete code
- `provider_unwind_snapshot_rebuild.go`: `chunkAlignedToBlock`, `seedLeftoverBlocks`, `computeTDAnchor`, `dumpStraddleDiagnostic`.
- `provider_unwind_snapshot_trim.go`: the "Non-1000-aligned toBlock ... returns an explicit error" comment at 68-72.
- Tests referencing the deleted helpers.

## 4. Sequencing / commit stack

Ten commits, each atomic (build + `make test-short` clean per commit):

### Commit 1: `db/state: broaden IsRawTxN to either-edge, add V4PairForStep`
- Extend `FilesItem.IsRawTxN` in `db/state/dirty_files.go:207`.
- Add `DirtyFiles.V4PairForStep`.
- Tests: `TestFilesItem_IsRawTxN_TwoEdges`, `TestDirtyFiles_V4PairForStep_TilesStep`.
- No caller changes semantics; v4 #2 doesn't exist on disk yet.

### Commit 2: `db/state: extend findMergeRangeInFiles to accept v4 pairs positively`
- Replace `rangeContainsV4Item` guard with `v4PairMergeRange` positive-detection.
- Test: `TestFindMergeRangeInFiles_V4PairFiresAsAlignedRange`.
- Deps: commit 1.

### Commit 3: `db/state: retire dest path selects v4 #2 when v4 #1 present`
- Add `historyRetireDestPaths`, `domainRetireDestPaths`, `iiRetireDestPaths`.
- Wire into `.collate` for each. Defensive short-circuit at `history.go:890` so inline path no-ops when v4 #2 fires.
- Test: `TestHistoryRetireDestPaths_ChoosesV4TailWhenV4OneExists`.
- Deps: commits 1, 2.

### Commit 4: `db/state: delete mergeV4IntoStepFile inline path — background merge takes over`
- Delete inline block at `history.go:879-892`.
- Delete `mergeV4IntoStepFile` from `history_v4_compose.go`.
- Test: `TestAggregator_V4PairMergesToStepAlignedFile_Background`.
- Deps: commit 3.

### Commit 5: `db/snaptype: FileInfoV4 helper + 7-char literal encoding for block v4`
- Add `Type.FileInfoV4` + `FileInfo.IsRawBlock()`.
- Test: `TestParseFileName_V4Literal_RoundTrip`.
- Purely additive; parallel with 1-4.

### Commit 6: `db/snapshotsync: extend FindMergeRanges + visibility for block v4 pairs`
- `merger.FindMergeRanges` recognises v4-pair aligned range.
- Verify `NoGaps` treats the pair as no-gap.
- Tests: `TestFindMergeRanges_V4Pair_ProducesAlignedRange`, `TestNoGaps_V4Pair`.
- Deps: commit 5.

### Commit 7: `node/components/storage: drop chunkAlignedToBlock, emit block v4 #1 at raw target`
- Delete `chunkAlignedToBlock`, `seedLeftoverBlocks`, `computeTDAnchor`, `dumpStraddleDiagnostic`.
- `unwindSnapshotsPastBlock` and `rebuildBlockStraddles`: `newTo = toBlock + 1`.
- `sliceStraddleSeg` / per-type rebuild: use `FileInfoV4` for non-aligned range.
- No MDBX writes.
- Tests: `TestRebuildBlockStraddles_MidChunkTargetProducesV4Headers/Bodies/Transactions`.
- Deps: commits 5, 6.

### Commit 8: `db/snapshotsync/freezeblocks: retire tail emission uses v4 #2 naming when v4 #1 exists`
- `chooseRetireTailRange` helper.
- `dumpBlocks` / `dumpBlocksRange` invoke it before choosing `blockTo`.
- Test: `TestBlockRetire_TailEmitAfterV4One_MergesToAligned`.
- Deps: commit 7.

### Commit 9: `node/components/storage,db/snapshotsync: read-path + inventory updates for block v4`
- Verify `AllTypedSegments`, `View.Segments`, block reader resolvers all work.
- Update any assertion sites expecting 1000-aligned (grep `%1000 == 0`, `Erigon2MinSegmentSize` guards outside merge boundary).
- Test: `TestBlockReader_MidChunkUnwindThenReExecute_ReadsResolveViaV4Pair`.
- Deps: commit 8.

### Commit 10: `docs: universal v4 lifecycle spec + memo landing`
- Update this doc with landed-commit SHAs.
- Update `mode-abcd-taxonomy-2026-08-17` and `v4-retire-cost-audit-followup-2026-08-25` memos.
- Deps: 1-9.

### Parallelizable groups
- 1, 5 land first, in parallel.
- 2 depends on 1; 6 depends on 5. Parallel.
- 3, 4 chain on state side.
- 7, 8, 9 chain on block side.
- State chain (3, 4) and block chain (7, 8, 9) independent — different files, different tests. Parallel.
- 10 requires all.

## 5. Verification strategy

### Per-commit unit tests
Listed under each commit above. All fast, run under `make test-short`.

### Integration verification against frozen datadirs

**`/erigon/tmp/erigon-hoodi-modec-verify.cycle-010.frozen-iter4-fail`** (state-side setHead timeout under inline-merge cost):
1. Clone via hardlink to a temp datadir.
2. Rebuild erigon with commit 4 (state-side background merge landed).
3. Replay the setHead sequence that timed out at iter 4; assert setHead completes < 10s.
4. Assert on-disk state has step-aligned files, no `.mdbx-only` stragglers, no v4 pairs post-merge-loop drain.

**`/erigon/tmp/erigon-hoodi-modec-verify.cycle-011.frozen-iter6b-fail`** (block-side "no header for block 3491690"):
1. Clone via hardlink to a temp datadir.
2. Rebuild erigon with commit 8 (block-side v4 pair emit + merge landed).
3. Retry mode-C at block 3491690.
4. Assert `HeaderByNumber(3491690)` returns the correct header via `v1.1-3491000-3491691-headers.seg`.
5. Continue forward-exec past the block; assert v4 #2 emits and merger consolidates.
6. Invariant: no MDBX writes to `kv.Headers/HeaderTD/BlockBody/EthTx/Senders` for blocks ≥ alignedStart during mode-C unwind.

### Cycle-12 clean run (post-commit-9)
Full unwind-soak.sh. Success criteria:
- Zero "no header for block N" errors.
- Zero setHead preflight timeouts.
- On-disk: no `.mdbx-only` intermediates, no un-merged v4 pairs older than one merge-loop tick.
- MDBX size stays flat cycle-over-cycle.
- `Timings: Forkchoice` metric: FCU cycles stay < 700ms.

## 6. Risk assessment

### Invariants to guard
1. **No MDBX seeding of frozen data.** Assert (under INTEG_TEST=1) that Provider.Unwind never writes to `kv.Headers/HeaderTD/BlockBody/EthTx/Senders` for blocks ≥ smallest v4 #1 `From`.
2. **Merge is always background.** Grep for `mergeV4IntoStepFile` remaining anywhere should fail CI after commit 4.
3. **v4 pair completeness before merge scheduling.** `v4PairMergeRange` must not propose merging when only v4 #1 exists. Test: emit v4 #1 alone, run MergeLoop, assert no merge fires.
4. **v4 pair range tiling.** `TestV4PairForStep_RejectsMismatchedBoundary` — a v4 #1 ending at txN=100 and v4 #2 starting at txN=101 (gap of 1) MUST be rejected.
5. **Cross-family alignment.** In `Aggregator.mergeLoop`, when Accounts/Storage/Commitment all have a v4 pair for the same step, all three must merge in the same tick.

### What could break during migration
- **Race after commit 3, before commit 4**: retire emits v4 #2 files but `mergeV4IntoStepFile` still runs. The short-circuit in commit 3 protects; verify with a targeted test.
- **`filesCoverBackwardTo` gap detection**: broadened `IsRawTxN` includes v4 #2 — the bridge branch should still work because v4 #2's endTxN IS aligned. Add `TestFilesCoverBackwardTo_V4Two_UsesStrictAdjacency`.
- **Cross-cadence retire on block side**: `chooseRetireTailRange` may need to consult per-type retire cadences (headers merged at 10k while tx still at 1k).
- **Downloader / inventory publish**: v4 files should be marked non-advertisable (transient); only merged aligned files published.

### Instrumentation to add
1. Log lines with `[v4-pair]` prefix in `chooseRetireTailRange`, `V4PairForStep`, `findMergeRangeInFiles` v4 branch, `merger.FindMergeRanges` v4 branch.
2. Metrics: `v4_files_on_disk{class="state|block", role="one|two"}` gauges. Assert both trend to zero within one merge-loop tick.
3. Metric: `v4_merge_latency_seconds` — time from v4 #2 landing to merged aligned file publishing.

## 7. Explicit dependencies (DAG)

```
1 (IsRawTxN)  → 2 (findMergeRange v4 pair) → 3 (retire dest v4-#2) → 4 (delete inline merge)
                                                                                    │
5 (FileInfoV4) → 6 (block merger v4 pair) → 7 (rebuild v4 #1) → 8 (retire v4 #2) → 9 (read path)
                                                                                                │
                                                              all of 4, 9 ──────────→ 10 (docs)
```

- Commits 1, 5 land first, in parallel.
- Commits 2, 6 land next, in parallel.
- 3, 4 chain (state).
- 7, 8, 9 chain (block).
- State chain and block chain independent — can proceed in parallel.
- 10 requires full stack.

Minimum critical-path length: 5 commits per side (1→2→3→4 or 5→6→7→8→9). With sequential landing on one worker: 10 commits total.

## Critical files

- `/erigon/mark/hive/clients/erigon/erigon/db/state/history.go`
- `/erigon/mark/hive/clients/erigon/erigon/db/state/merge.go`
- `/erigon/mark/hive/clients/erigon/erigon/db/state/dirty_files.go`
- `/erigon/mark/hive/clients/erigon/erigon/db/state/aggregator.go`
- `/erigon/mark/hive/clients/erigon/erigon/db/state/history_v4_compose.go`
- `/erigon/mark/hive/clients/erigon/erigon/db/snapshotsync/merger.go`
- `/erigon/mark/hive/clients/erigon/erigon/db/snaptype/files.go`
- `/erigon/mark/hive/clients/erigon/erigon/db/snapshotsync/freezeblocks/block_snapshots.go`
- `/erigon/mark/hive/clients/erigon/erigon/node/components/storage/provider_unwind_snapshot_rebuild.go`
- `/erigon/mark/hive/clients/erigon/erigon/node/components/storage/provider_unwind_snapshot_trim.go`
