# v3: fold a window block from its access list while it executes

## Problem

A block that carries a block access list (BAL) and owns a changeset (a "window" block: every block at the tip)
folds its commitment only after its last transaction: `maybeComputeAhead` returns for `ownsChangeset(n)`
(`execution/stagedsync/committer.go:590`). Blocks before the window already fold from the BAL while they execute.

A 6-line prototype that lets window blocks do the same, measured on 150M-gas BAL fixtures (16-thread Linux box,
load 0.0-0.3, 24 interleaved pairs per cell, block stage time):

| cell         | base     | prototype | change | mgas/s        |
|--------------|----------|-----------|--------|---------------|
| selfdestruct | 151.7 ms | 118.9 ms  | -20.7% | 989 -> 1,262  |
| diff_to_diff | 78.02 ms | 72.25 ms  | -8.0%  | 1,923 -> 2,076 |
| ttp_0d       | 77.62 ms | 69.14 ms  | -10.3% | 1,932 -> 2,170 |

The prototype is not safe to ship: it has no rule for a rejected block, for two window blocks in one batch, for
shadow mode or for the head-capture handoff, and no tests.

## Design

- The fold of window block `n` from its BAL may start as soon as all of these hold:
  - `pending[n]` is BAL-driven and not yet computed ahead (as today);
  - the v3 feed path is active (`cc.branchPrefetch != nil`);
  - the block's changeset is saved by hash (`cc.doms.GetChangesetByHash(n, hash) != nil`): the exec loop saves it
    before it publishes the block's first `txResult` (`exec3_parallel.go:1586`), sometimes earlier
    (`:1216`), never guaranteed at `blockRequest` time;
  - block `n-1` is folded: `n == cc.firstBlockNum`, or `cc.hasComputed && cc.lastComputedBlock == n-1`. In the
    window every block is folded at its own boundary, by either path, so this replaces the pre-window rule
    (`lastComputedAheadBlock == n-1`), which stays as it is for pre-window blocks;
  - the existing gate (`computeAheadGateOpen`) and stop-cause cap;
  - `dbg.BALShadowCompute` is off and `commitmentdb.ParentReadsWanted()` is false. Shadow mode recomputes the
    block and would fold it twice into one changeset; the head-capture handoff (`noteVerified`) needs the header's
    parent hash and the block's parent state, which exist only at `blockResult`. Under either switch a window
    block keeps today's fold at its `blockResult`.
- The fold is tried at `blockRequest` (as today), at the end of `blockResult(n-1)` (as today) and at every
  `txResult` of block `n`, before that result is accumulated. The first try that passes runs it.
- It goes through the code pre-window blocks use (`computeBlockFromBAL` -> `computeRootFromBAL` ->
  `computeRootFromUpdates`), which routes a changeset-owning block to `computeWithBlockAccumulator`: the branch
  deltas land in the block's own changeset and the pending update is stamped with the block's hash.
- Success is silent for a window block, as the per-block fold is (`compute` publishes only a mismatch). A root
  mismatch resets the pending update and publishes `ErrWrongTrieRoot`, as today.
- A rejected block (`blockResult.Err != nil`) that was folded ahead: the calculator resets the pending update and
  forgets the block (`pending`, `computedAhead`, `balRoots`). Nothing else runs in that calculator: the apply loop
  cancels the run; at the tip the validation's `SharedDomains` is closed (`fork_validator.go:294-298`); in a stage
  run the unwind calls `ResetPendingUpdates` and `SeekCommitment` (`stage_execute.go:555`, `:600`).
- A window block that straddles an unfrozen step edge checkpoints from the BAL (`checkpointStepsFromBAL`), into
  its own changeset.

## Number to beat, first measurement, stop line

- Base (`24de510f37f`): the table above. Measured by the supervisor after Task 1 on the same bed, base against
  the branch head, 24 interleaved pairs per cell.
- Stop line: the head must win block stage time on at least two of the three cells and lose none; otherwise the
  branch stops after Task 1 with the numbers.

## Rules for every task

- No behaviour change for blocks without a BAL, for pre-window blocks, or with `BAL_DRIVEN_COMMITMENT=false`.
- Tests live next to the existing compute-ahead tests (`committer_step_boundary_test.go`, `committer_test.go`)
  and reuse their setup; no new harness.
- No new switch, no new exported name.

### Task 1: window blocks fold ahead

Files: `execution/stagedsync/committer.go`, tests.

- [x] Tests first, each red for the stated reason before the change:
  - a window BAL block: after its `blockRequest` and first `txResult` it is folded (`computedAhead`), the root
    equals the header's, nothing was published, the pending update carries the block's number and hash; its
    `blockResult` runs no second fold; after the pending update is flushed, the commitment diff in the block's
    changeset equals the diff the same block produces with `BAL_DRIVEN_COMMITMENT` off.
  - two window blocks in one batch, the second with a BAL: no fold of the second before `blockResult` of the
    first was handled; folded after it; both roots right.
  - a window BAL block stays on the `blockResult` fold when `BAL_SHADOW_COMPUTE` is on, and when
    `commitmentdb.ParentReadsWanted()` is true.
  - a window BAL block whose BAL root differs from the header's: `ErrWrongTrieRoot` is published and no pending
    update remains.
- [x] `maybeComputeAhead`: the window rule of the design; the pre-window rule untouched.
- [x] `handleMessage`: try the fold at each `txResult`, before the result is accumulated.
- [x] `computeBlockFromBAL`: no publish on success for a changeset-owning block.
- [x] One line under `logNpPhases`: `[np-phase] ahead` with `blk` and `window`.
- [x] Guards: the first test fails when the window rule is removed and when the success publish is restored; the
      second fails when the `n-1` condition is dropped; the third fails when either switch is ignored.

### Task 2: a rejected block that was folded ahead

- [x] Test first: a window BAL block folded ahead, then its `blockResult` with `Err`: no pending update remains
      (`PeekPendingUpdate() == nil`), the block is gone from `pending`, `computedAhead` and `balRoots`, and the
      calculator published nothing for it.
- [x] `handleMessage`, `blockResult` with `Err`: the reset above when the block was folded ahead.
- [x] If the package has a test that drives a whole invalid block through the executor, add the BAL case: the
      invalid block is reported invalid and a valid block at the same height, executed afterwards, gets its
      header root. If there is none, say so in the turn's notes; the unit test stands.

### Task 3: a window block across a step edge

- [ ] Test, mirroring `TestComputeAhead_StepBoundaryCheckpointMidBlock` for a changeset-owning block: the
      checkpoint at the edge is written into the block's own changeset, the block-end root equals the header's,
      and the `txResult` at the edge runs no second checkpoint.
- [ ] Fix what the test shows; if it passes as written, the task is the test.

### Task 4: remove this plan

- [ ] Delete `docs/plans/20261112-v3-bal-ahead-fold.md` in the last commit.

## Out of scope

- Starting the fold before the changeset exists (a private diff installed at `blockResult`).
- The head-capture handoff for a block folded ahead; shadow mode in the window.
- Worker count of the fold while the block executes; when the fold should wait for the BAL prefetch.
- Blocks without a BAL (the early-fold branch).
