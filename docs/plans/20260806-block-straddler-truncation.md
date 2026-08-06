# Plan — block .seg straddler truncation at unwind

**Date:** 2026-08-06
**Branch:** `merge/main-into-feat-snapshot-flow-20260731`
**Status:** design; implementation deferred

## Context

The 2026-08-06 state-v4 accessor fix (`e272828445`) removed the leg-M v1 iter 1 mode_b gas-mismatch class. Once forward re-exec started working correctly, a **block-side** issue surfaced: the retire's later widening consolidation for a range that STRADDLES the unwind target hits `TxsAmountBasedOnBodiesSnapshots` at `db/snaptype2/block_types.go:428` with a `negative txs count` error because the newly-built bodies.seg has a phantom empty body (`BaseTxnID=0`) for a block that wasn't yet in DB when retire fired.

### Concrete repro

Post-fix soak run 1 (2026-08-06 10:37→12:38, datadir `/erigon/tmp/erigon-hoodi-soak.postfix-run1`):

- Unwind target: block 3,233,964 (iter 1 mode_b, depth 129,843).
- Straddler `.seg` files at that boundary: `v1.1-003233-003234-{bodies,headers,transactions}.seg` (blocks 3,233,000 → 3,233,999).
- Pre-existing wider file: `v1.1-003200-003233-*.seg` (32k-block merged, ends at 3,232,999 — entirely below target, not a straddler).
- Unwind's `collectFilesPastBlock` at [`node/components/storage/provider_unwind_snapshot_trim.go:153`](../../node/components/storage/provider_unwind_snapshot_trim.go#L153-L163) uses `FromBlock > toBlock` — keeps the straddler (FromBlock=3,233,000 ≤ 3,233,964).
- Forward re-exec re-inserts blocks 3,233,965 → tip into MDBX. At 11:24 head was at 3,239,964.
- 11:24:04 retire fired for range 003230-003240 (blocks 3,230,000 → 3,239,999). Head was 35 blocks short of range end.
- 11:24:55 `[EROR] retire blocks err="buildIdx: transactions: negative txs count v1.1-003230-003240-bodies.seg: lastBody.BaseTxId=0 < firstBody.BaseTxId=114883250"`.
- On-disk artifact: `v1.1-003230-003240-{bodies,headers}.seg` exist (partial, phantom-tail) but no matching `-transactions.seg` (index build refused).

Retire self-recovered on the next cycle (once forward re-exec caught up). The orphan bodies+headers .seg pair remained on disk.

### Why the current design produces this

The straddler `v1.1-003233-003234-*.seg` (pre-unwind) is retained by unwind. Its content covers blocks 3,233,000 → 3,233,999 — but post-unwind, only blocks 3,233,000 → 3,233,964 are canonical (chain-DB canonical hash truncation gates the rest). The .seg's post-target portion (blocks 3,233,965 → 3,233,999) is **stale but physically present**.

When retire later fires for a wider range including the straddler (003230-003240), its input source is a mix of:

1. The straddler .seg (has blocks 3,233,000 → 3,233,999, but post-target portion is stale).
2. Newly-re-executed DB blocks (3,233,965 onwards).
3. Blocks whose only source is a pre-existing wider file (`003200-003233`).

The consolidation's tail-block reader hits blocks where DB is authoritative but hasn't yet caught up — inserts empty body → indexer refuses.

Under `snapshots are immutable` (per CLAUDE.md), we cannot modify the straddler in place. Two design paths follow.

## Design options

### Option A — v4-shape block .seg emit (analog of the state-v4 fix)

Mirror what `WriteStateBoundaryFileV4` did for state domains. At unwind time, emit truncated `.seg` files covering only blocks [FromBlock, toBlock] and remove the original straddler.

**File naming**: block-file naming is 1000-block-aligned in the current schema (`003233-003234` = blocks 3,233,000 → 3,233,999). A truncation at block 3,233,964 needs a name that isn't 1000-aligned — either raw-block-number naming (analog of state v4's raw-txN), or accept 1000-block-boundary rounding down (losing blocks 3,233,001 → 3,233,964).

Straightforward-but-invasive; touches:

- **New naming**: `v4.0-<from-block>-<to-block>-bodies.seg` etc. Schema, parser, visibility filter.
- **New emit primitive**: `WriteBoundaryBodiesFileV4` / `WriteBoundaryHeadersFileV4` / `WriteBoundaryTransactionsFileV4` — reads pre-unwind straddler, writes truncated .seg for each block-file kind.
- **Accessor builds**: block .seg files carry `.tx` (transactions) index, `.idx` (headers). Same "no-accessors → invisible" failure mode as the state side would apply; must build inline.
- **Retire bridge**: analog of commit `906f8f3de1 filesCoverBackwardTo — bridge mode-C v4 files in the walk` for block files, so later merges can supersede v4-shape block files.
- **Provider.Unwind integration**: call the new emit primitive alongside `regenerateBoundaryStepFiles` for state.

Estimated: ~800-1200 lines net.

### Option B — retire barrier + orphan cleanup

Teach retire's dumpBlocksRange (`db/snapshotsync/freezeblocks/block_snapshots.go:942`) to REFUSE building a .seg whose `blockTo > current head`. Retire retries later once forward re-exec has fully populated the range. On restart, sweep any orphan `-bodies.seg` / `-headers.seg` pair without a matching `-transactions.seg` (the phantom-tail case).

Less invasive; touches:

- **DumpBlocks head-bound guard**: read chainDB max block before starting the dump; error out with a distinct error type if blockTo > maxBlock.
- **Retire caller retry**: `BuildFilesInBackground` re-schedules on the guard error rather than logging EROR.
- **Boot-time orphan sweep**: at Aggregator OpenFolder time, detect `.seg` triples with a missing member and remove the orphaned members (analog of the receipt-side sweep already in `commitment_validator.go`).

Estimated: ~150-300 lines. Doesn't require immutable-invariant carve-outs.

**Downside**: retire delayed. Under bursty forward-exec after a deep unwind, a retire round might be skipped several cycles until head catches up. Steady-state disk allocation grows temporarily. Doesn't grow unboundedly — retire fires as soon as head catches up.

### Option C — accept + document

The retire error is one-off in each run (retire moves on after failing, retries once head advances). The orphan segments are limited (a bodies+headers pair per deep-unwind boundary hit; the transactions.seg never lands so the trio is incomplete). Under Phase 5 disk-clean these orphans DO fail the assertion — we'd need to broaden the orphan sweep to include block .seg triples.

Not really a fix, more a "known limitation" posture. Rejected as an option per [[feedback-never-avoid-problem]].

## Recommendation

**Option B first** (retire barrier + orphan cleanup) — small, targeted, prevents the failure class without immutability carve-outs. Ship it. If subsequent soaks still surface the retire-fires-on-straddler-range-mid-catchup pattern (they might, under different timings), escalate to Option A.

Option A remains available as a follow-up if:

- Repeated deep unwinds compound the disk waste that Option B leaves during the retry-until-head-caught-up window.
- Consumer nodes downloading the straddler face the same phantom-tail confusion (they wouldn't, since consumers read what's on disk, but downstream federated-history use might).
- The mode-C track's endgame ([[shadow-fork-single-command-vision-2026-08-02]] gap 6) needs "trim-below-block" semantics that Option B can't provide.

## Critical files

**Option B (recommended first)**:

- `db/snapshotsync/freezeblocks/block_snapshots.go` — `DumpBlocks`, `dumpBlocksRange`, `BuildFilesInBackground` — head-bound guard + retry.
- `node/components/storage/provider.go` or new file — orphan `.seg` triple sweep at boot.
- `db/snapshotsync/freezeblocks/block_snapshots_test.go` — regression test: DumpBlocks refuses `blockTo > head`.

**Option A (bigger)**:

- `db/snaptype2/block_types.go` — v4-shape schema parsing.
- `db/snapshotsync/freezeblocks/block_snapshots.go` — v4-shape emit primitives.
- `db/state/merge.go` — `filesCoverBackwardTo` analog for block files.
- `node/components/storage/provider_unwind_snapshot_trim.go` — call the new emit at unwind time, remove the straddler post-emit.

## Verification

**Option B**:

- Unit test: DumpBlocks called with blockTo > maxDBBlock returns the guard error.
- Level 4 test: seed a datadir with an artificial "unwound and forward-exec mid-catchup" shape; verify retire skips the partial range and retries once head catches up.
- Level 5 test: run the same soak profile that surfaced the run-1 retire error (iter 1 mode_b depth 129k against fresh datadir); expect zero `negative txs count` errors, expect zero orphan bodies+headers .seg pairs post Phase 5.

**Option A** (if we escalate):

- Everything above, plus: after unwind, verify a truncated v4 block .seg pair (bodies+headers+transactions) exists with correct accessors; verify the pre-existing straddler is removed; verify forward re-exec + retire produces a valid wider merged .seg that supersedes the v4 pair; verify Phase 5 sees no v4 block files remaining post-quiescence.

## Cross-links

- [[mode-c-v4-emit-nondeterministic-2026-08-06]] — the state-v4 fix this issue was masked behind.
- [[mode-c-checkpoint-2026-08-04]] — mode-C v4 emit direction context.
- Commit `906f8f3de1 db/state: filesCoverBackwardTo` — precedent for merge-side v4 bridging (state).
- Commit `e272828445 db/state, node/components/storage: build .bt/.kvei/.kvi inline on mode-C v4 emit` — precedent for inline accessor build.
- [[HARD INVARIANTS]] — "snapshots are immutable" + "Data flow: db → snapshots" constraints.
