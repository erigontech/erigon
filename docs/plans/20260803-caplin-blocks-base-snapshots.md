# CaplinSnapshots rides BaseRoSnapshots (beacon blocks + blob sidecars)

## Overview

`freezeblocks.CaplinSnapshots` reimplements the dirty/visible segment
lifecycle that `snapshotsync.BaseRoSnapshots` already provides — its own
`dirty`/`visible` fields and locks, `recalcVisibleFiles`, `OpenList`/
`OpenFolder`/`closeWhatNotInList`, `idxAvailability`, an unpinned `CaplinView`,
and its own `BuildMissingIndices`. Embedding the base deletes that copy and
gives caplin the refcounted generation model: pinned reader views, drain-gated
reclaim with `db/mvcc` retire reasons, `TryAcquireRange` build claims, and
`RetireFilesBelow/Above`.

This is PR-1 of the caplin-EL snapshot parity program (umbrella doc
`20260729-caplin-el-snapshot-parity.md`, branch `awskii/caplin-snapshot-parity`
— NOT on this branch; do not open it during execution). PR-0 landed as #22878
(merged) and #22944 (open); this branch is stacked on #22944's branch.

The value is not cosmetic: caplin views today pin nothing
(`VisibleSegments.BeginRo` returns `RoTx` with `release == nil`) and
`closeWhatNotInList` closes decompressors inline, so a reader iterating an old
visible slice can hit a closed mmap the moment anything removes a file. That
is latent only because nothing deletes caplin segments yet — PR-3a's merge
tier changes that, and it is blocked on this.

## Context (from discovery)

Verified in THIS checkout (branch base predates the #22878 merge — do NOT cite
the new enum ranges; here `MinCoreEnum=1, MinCaplinEnum=9, MinBorEnum=11,
MaxEnum=15` and `RegisterType` has no duplicate/range panic):

- `db/snapshotsync/snapshots.go:569` —
  `NewBaseRoSnapshots(cfg ethconfig.BlocksFreezing, snapDir string, types []snaptype.Type, baseSegType snaptype.Type, alignMin bool, logger log.Logger) *BaseRoSnapshots`.
  Adopters: `db/snapshotsync/blocksnapshots/block_snapshots.go:37`,
  `polygon/heimdall/snapshots.go:43` (both embed by value).
- `snaptype.CaplinSnapshotTypes = []Type{BeaconBlocks, BlobSidecars}` — already
  the order base's `idxAvailability()` needs (it keys off `s.enums[0]`,
  snapshots.go:986), so `baseSegType = snaptype.BeaconBlocks`.
- **Watermark conventions differ and must be preserved** —
  `CaplinSnapshots` exposes three conventions today:
  - `SegmentsMax()` (caplin_snapshots.go:97, set :223-230) = last **dirty**
    BeaconBlocks `To-1`, populated as soon as the `.seg` opens, no index
    required. Base `SegmentsMax()` (snapshots.go:619) counts **visible** only.
    Consumer `cl/phase1/stages/stage_history_download.go:281` is the archive
    backfill termination clause `(!ArchiveBlocks || slot <= SegmentsMax())` —
    a visible-based value can collapse it. Replacement:
    `DirtyBlocksAvailable(snaptype.CaplinEnums.BeaconBlocks)`
    (snapshots.go:645, returns `seg.to-1` over the dirty btree).
  - `IndicesMax()` (caplin_snapshots.go:250-257) = last visible `To`
    (exclusive). Only two consumers (a capcli log line, `LogStat`); base's
    `to-1` is a cosmetic shift that actually fixes `LogStat`'s off-by-one at
    caplin_snapshots.go:101.
  - `FrozenBlobs()` (caplin_snapshots.go:693-705) = max visible BlobSidecars
    `To` — **exclusive**. `VisibleBlocksAvailable` (snapshots.go:649) returns
    `to-1`; substituting it breaks `cl/beacon/handler/blobs.go:118`
    (`slot < FrozenBlobs()` would drop the last frozen slot) and
    `cl/antiquary/antiquary.go:466` (re-dump). MUST stay exclusive.
  - `BlocksAvailable()` = `min(SegmentsMax, IndicesMax)` = `To-1` healthy;
    base `BlocksAvailable` = visible `To-1`. Same value — convention-compatible.
- **Base `BuildMissedIndices` nil-panics for caplin types.**
  `BeaconBlocks`/`BlobSidecars` are plain `SnapType` literals
  (`db/snaptype/caplin_types.go:22-33`) with `indexBuilder == nil`, never
  `RegisterType`d. Base resolves the builder via `s.IndexBuilder(t.Type())`
  (snapshots.go:1654) from `s.operators`, but base exposes only
  `SetRangeExtractor` (snapshots.go:678) — there is **no `SetIndexBuilder`**,
  so `operators[BeaconBlocks]` is never populated and
  `t.BuildIndexes(ctx, info, nil, …)` (snapshots.go:1662) falls through to a
  nil builder.
- **Base has no exported `OpenList`.** `openSegments(fileNames, open,
  optimistic)` (snapshots.go:1080-1159) is unexported; both exported wrappers
  re-scan the directory (`OpenFolder` :1168, `OpenSegments` :1206 — note the
  latter takes `alignMin` as a parameter rather than using `s.alignMin`, a
  trap). Caplin's `OpenList(fileNames []string, optimistic bool) error`
  (:165-236) is what `TestOpenListDirtyLockRace` exercises.
- **`.tmp` sweep** — `RemoveOverlaps` (snapshots.go:1356-1415) enumerates the
  whole dir and unlinks every `*.tmp` (:1405-1413, in-tree TODO "may remove
  Caplin's useful .tmp files"). `seg.Compressor.Compress` writes its output
  temp as `<dirs.Snap>/<name>.*.tmp` (`db/seg/compress.go:313` →
  `common/dir/rw_dir.go:231-243`), and caplin's dir IS `dirs.Snap`
  (caplin_snapshots.go:86, 339, 410). **Concurrency, verified:** all three
  `RemoveOverlaps` call sites are gated — `MergeBlocks`
  (block_snapshots.go:331) runs inside `buildFiles`' single goroutine after
  `DumpBlocks` returns, single-flighted by the `working` CAS
  (:382-445); `BlockRetire.RemoveOverlaps` (:546) and the caplin-state one are
  reached only from the offline `snapshots retire` CLI
  (snapshots_cmd.go:3511,3515). So EL can never sweep its own in-flight temp.
  The one live window is a caplin antiquary dump running while EL's post-merge
  sweep fires — possible only because the caplin build semaphore is disabled
  (below). Both are fixed here.
- **Build semaphore is commented out AND buggy**: `cl/antiquary/antiquary.go`
  ~403-408 and ~448-453 use `defer a.snBuildSema.TryAcquire(...)` where
  `Release` is meant — a verbatim re-enable leaks the permit and permanently
  wedges snapshot building. Source: `segmentsBuildLimiter`
  (`node/eth/backend.go:410`, handed to caplin at :1026).
- `CaplinSnapshots.Salt` is never assigned anywhere (node-built indices use
  salt 0); capcli uses `snaptype.GetIndexSalt` (cmd/capcli/cli.go:1382).
- Commit convention: package-prefixed (`db/snapshotsync: …`), not `feat: …`.

## Development Approach

- **testing approach**: TDD (red → green). Every task names its red test and
  the condition it pins.
- one task = one commit, package-prefixed, each independently green
  (build + tests) before the next.
- **Behavior-equivalence is the acceptance bar**: this is a refactor; the
  numbers caplin reports must not move except where a shift is explicitly
  declared (`IndicesMax` `To`→`To-1`).
- `make lint` is non-deterministic — repeat until clean before finishing.
- update this plan file when scope changes.

## Testing Strategy

- **unit tests**: `db/snapshotsync/freezeblocks` (lifecycle + watermarks),
  `db/snapshotsync` (new base APIs), `cl/antiquary` (semaphore).
- **watermark equivalence fixture** (Task 2) is the safety net for the whole
  refactor — it must exist and pass before the embed lands.
- **race**: the reader-vs-removal test must run under `-race`.
- no e2e.

## Progress Tracking

- mark completed items `[x]` immediately when done
- add newly discovered tasks with ➕ prefix, blockers with ⚠️
- keep the plan in sync with actual work

## Solution Overview

Order matters: grow the base API and pin current behavior FIRST, then embed,
then delete the duplicates.

1. Base gains `SetIndexBuilder` and an exported `OpenList` (both mirror
   existing shapes).
2. A characterization fixture pins today's four watermark numbers.
3. `CaplinSnapshots` embeds `BaseRoSnapshots`; the three
   convention-divergent accessors stay caplin-owned and are re-expressed on
   base primitives (`DirtyBlocksAvailable`, a pinned `View`).
4. Duplicated lifecycle code is deleted; the read path moves onto pinned views.
5. The `.tmp` sweep is scoped to owned types; the antiquary semaphore is
   re-enabled and fixed.

`alignMin = false`: blobs start at Deneb and trail blocks, and alignMin clamps
every type to `slices.Min` of visible tips (snapshots.go:844-864) — with it
true, block availability would be pinned to the blob tip.

## Technical Details

- Embed by value like the two existing adopters:
  `type CaplinSnapshots struct { snapshotsync.BaseRoSnapshots; beaconCfg
  *clparams.BeaconChainConfig }`. Construct with
  `NewBaseRoSnapshots(cfg, dirs.Snap, snaptype.CaplinSnapshotTypes,
  snaptype.BeaconBlocks, false /* alignMin */, logger)`.
- Keep caplin-owned, do NOT inherit:
  - `SegmentsMax()` → `s.DirtyBlocksAvailable(snaptype.CaplinEnums.BeaconBlocks)`
  - `FrozenBlobs()` → Deneb short-circuit, then a pinned `View()`, last visible
    BlobSidecars segment's `To()` (exclusive), 0 when empty. Never
    `VisibleBlocksAvailable(...)+1`.
  - `BuildMissingIndices` — rewritten to walk base's dirty btrees on
    `!seg.IsIndexed()` with `TryAcquireRange`/`ReleaseRange`
    (snapshots.go:541-548), still dispatching `snapshotsync.BeaconSimpleIdx`.
    Registering an `IndexBuilder` (Task 1) additionally makes the inherited
    `BuildMissedIndices` safe rather than nil-panicking.
- `SetIndexBuilder(t snaptype.Type, b snaptype.IndexBuilder)` mirrors
  `SetRangeExtractor` (snapshots.go:678-687). Register for both caplin types in
  `NewCaplinSnapshots` via an `IndexBuilderFunc` adapter delegating to
  `BeaconSimpleIdx`.
- Exported base `OpenList(fileNames []string, optimistic bool) error` mirrors
  `OpenFolder`'s lock/retire/openSegments/recalc shape minus the directory
  scan (~15 lines), so `TestOpenListDirtyLockRace` keeps testing the same
  invariant.
- `.tmp` scoping: in `RemoveOverlaps`, skip temps whose parsed name is not one
  of the instance's types — `if info, _, ok := snaptype.ParseFileName(s.dir,
  strings.TrimSuffix(filepath.Base(f), ".tmp")); ok && !s.HasType(info.Type)
  { continue }`. Unparseable names keep today's behavior (deleted) so orphan
  cleanup does not regress.
- Salt: wire `snaptype.GetIndexSalt(dirs.Snap, logger)` at construction. If it
  errors on a fresh datadir, fall back to the current zero value rather than
  failing construction — index salt is stored inside each `.idx`, so mixed
  salts stay readable; this only aligns the node with capcli.

## What Goes Where

- **Implementation Steps**: code + tests on `awskii/caplin-blocks-base-snapshots`,
  stacked on `awskii/caplin-snapshot-prefixes`.
- **Post-Completion**: PR body, rebase note, follow-ups.

## Implementation Steps

### Task 1: base gains SetIndexBuilder and exported OpenList

**Files:**
- Modify: `db/snapshotsync/snapshots.go`
- Modify: `db/snapshotsync/snapshots_test.go`

- [ ] write red test: registering an index builder via `SetIndexBuilder` makes
  `IndexBuilder(t)` return it (today there is no setter — the test does not
  compile / returns nil)
- [ ] add `SetIndexBuilder(t snaptype.Type, b snaptype.IndexBuilder)` mirroring
  `SetRangeExtractor` (snapshots.go:678-687)
- [ ] write red test: `OpenList` on a temp dir opens exactly the named files
  and leaves an unnamed on-disk segment unopened (no exported `OpenList` today)
- [ ] add exported `OpenList(fileNames []string, optimistic bool) error`
  mirroring `OpenFolder` minus the directory scan
- [ ] run `go test ./db/snapshotsync/...` — green before task 2

### Task 2: characterization fixture for caplin watermarks

**Files:**
- Modify: `db/snapshotsync/freezeblocks/caplin_snapshots_test.go`

- [ ] build a fixture with several beaconblocks segments and blob segments
  covering a SHORTER range (mirrors Deneb trailing), at least one segment
  present-but-unindexed
- [ ] assert today's exact numbers for `SegmentsMax`, `IndicesMax`,
  `BlocksAvailable`, `FrozenBlobs` — including that `SegmentsMax` counts the
  unindexed segment (dirty) and `FrozenBlobs` is the exclusive `To`
- [ ] assert `FrozenBlobs() == 0` when the chain config has no Deneb
- [ ] run `go test ./db/snapshotsync/freezeblocks/...` — GREEN (this pins
  current behavior; it is the equivalence net for tasks 3-5 and must keep
  passing unchanged except the declared `IndicesMax` shift)

### Task 3: embed BaseRoSnapshots, keep the divergent accessors caplin-owned

**Files:**
- Modify: `db/snapshotsync/freezeblocks/caplin_snapshots.go`
- Modify: `db/snapshotsync/freezeblocks/caplin_snapshots_test.go`

- [ ] embed `snapshotsync.BaseRoSnapshots` by value; construct via
  `NewBaseRoSnapshots(cfg, dirs.Snap, snaptype.CaplinSnapshotTypes,
  snaptype.BeaconBlocks, false, logger)`; keep `beaconCfg`
- [ ] register `BeaconSimpleIdx` as the `IndexBuilder` for both caplin types
- [ ] re-express `SegmentsMax()` on `DirtyBlocksAvailable`, `FrozenBlobs()` on a
  pinned `View()` with the Deneb short-circuit and empty guard (both per
  Technical Details); let `BlocksAvailable`/`IndicesMax` come from the base
- [ ] update the Task 2 fixture ONLY for the declared `IndicesMax` `To`→`To-1`
  shift; every other assertion must pass untouched
- [ ] wire `GetIndexSalt(dirs.Snap, logger)` with zero-value fallback
- [ ] run `go test ./db/snapshotsync/freezeblocks/... ./cl/...` — green

### Task 4: delete the duplicated lifecycle, move reads onto pinned views

**Files:**
- Modify: `db/snapshotsync/freezeblocks/caplin_snapshots.go`
- Modify: `db/snapshotsync/freezeblocks/beacon_block_reader.go`
- Modify: `db/snapshotsync/freezeblocks/caplin_snapshots_test.go`

- [ ] write red `-race` test: a reader holding a caplin view while a
  concurrent `OpenFolder` drops a file must never touch a closed mmap
  (today `closeWhatNotInList` closes inline and views pin nothing)
- [ ] delete the duplicates: own `dirty`/`visible` fields and locks,
  `recalcVisibleFiles`, `closeWhatNotInList`, own `OpenFolder`,
  `idxAvailability`, unpinned `CaplinView`; route `OpenList` to the base
- [ ] move `ReadHeader`/`ReadBlobSidecars`/segment lookups onto base
  `View`/`ViewType` pinned `RoTx`; keep the zstd pooling and per-slot lookup
  helpers as caplin-specific
- [ ] rewrite `BuildMissingIndices` to walk base dirty btrees on
  `!IsIndexed()` with `TryAcquireRange`/`ReleaseRange`, dispatching
  `BeaconSimpleIdx`
- [ ] run `go test -race ./db/snapshotsync/... ./cl/antiquary/...` — green

### Task 5: scope the .tmp sweep; re-enable and fix the antiquary semaphore

**Files:**
- Modify: `db/snapshotsync/snapshots.go`
- Modify: `db/snapshotsync/snapshots_test.go`
- Modify: `cl/antiquary/antiquary.go`

- [ ] write red test: `RemoveOverlaps` on an instance owning only EL types
  leaves a `beaconblocks`-named `.tmp` in the same dir intact, and still
  removes an unparseable orphan `.tmp` (today every `.tmp` is deleted)
- [ ] scope the sweep per Technical Details (skip temps whose parsed type the
  instance does not own; unparseable names keep current behavior)
- [ ] re-enable the antiquary build semaphore in BOTH blocks, fixing the
  `defer TryAcquire` → `defer Release` bug; skip the cycle when the permit
  cannot be acquired
- [ ] write test: the antiquary releases the permit after a dump cycle (a
  second acquire succeeds) — pins the leak the commented-out code would have
  shipped
- [ ] run `go test ./db/snapshotsync/... ./cl/antiquary/...` — green

### Task 6: Verify acceptance criteria

- [ ] watermark fixture passes with only the declared `IndicesMax` shift
- [ ] no caplin-owned dirty/visible lifecycle remains (grep the deleted
  symbols); views pin generations
- [ ] `make lint` — repeat until clean
- [ ] `make erigon integration` — both build
- [ ] `go test -race ./db/snapshotsync/... ./cl/antiquary/... ./cl/phase1/...
  ./polygon/heimdall/...`

### Task 7: [Final] Update documentation

- [ ] update `db/agents.md` if the caplin snapshot description drifts
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

**PR** (opened manually — no push task here):
- Title: `db/snapshotsync, cl: CaplinSnapshots rides BaseRoSnapshots`
- Body: what the duplicate lifecycle cost (unpinned views + inline close =
  latent use-after-close, blocking the merge tier), what is inherited, and the
  three accessors deliberately kept caplin-owned with their conventions. Note
  the `IndicesMax` `To`→`To-1` shift. No Summary heading, no Testing section.
- **Stacked on #22944** — base the PR on `awskii/caplin-snapshot-prefixes`, or
  rebase onto main once that merges.

**Follow-ups** (not this PR): PR-3a still needs `Merger`'s `snaptype.Unknown`
hardcode parameterized, a `chain.Config` for `NewMerger`, and a real caplin
`MergeLimit` in snapcfg; PR-2 does the same adoption for caplin state.
