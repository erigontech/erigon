# Item 1 — apply-loop resource authority (batch-cut / fold-orphan fix)

Status: **planned, not implemented.** Deferred behind #22154. Design chosen with Mark:
**full apply-loop authority** (not the lighter exec-loop catch-up variant). Flush-boundary
crux **resolved** (2026-07-03): cap the fold-ahead distance rather than partition the
flush — see "Crux resolved" below. Ready to implement once #22154 lands.

## Problem

PR #21416 runs the commitment calculator concurrently with execution, folding each
block from its BAL ahead of the per-tx result stream. The fold is at most one block
ahead of execution (gated by `blockResult(n-1)` — see `foldGateOpen` in committer.go).

The batch size cut currently lives in the **exec loop** (`processResults`,
exec3_parallel.go ~1104, via `execLoopShouldExit`). When it fires at block K:

- exec has advanced the **account/storage** domains through K,
- the calculator may already have folded K+1, which advances **only the commitment
  domain** (branches) — a fold never writes account/storage values (those come from
  exec).

Committing then persists commitment@K+1 alongside state@K. On restart the state root
recomputed from state@K will not match the persisted commitment@K+1 → wrong root /
corruption. This is yperbasis's item-1 review finding.

The exec loop can only see execution progress; it cannot see how far the calculator
folded, so it cannot pick a consistent cut point. Two loops independently estimating
resources / deciding the cut is the ambiguity Mark wants removed.

## Design — full apply-loop authority

The apply loop is the only place that sees **both** streams: execution progress
(`lastBlockResult`) and commitment progress (calculator `rootResults` /
`pe.txExecutor.lastCommittedBlockNum`). Make it the single resource + shutdown
authority.

1. **Move the size/resource check out of the exec loop into the apply loop.** Remove
   `execLoopShouldExit`'s size arm from `processResults`; the exec loop no longer
   estimates `rs.SizeEstimate*`. The exec loop keeps only the natural-end signals it
   already carries (maxBlockNum reached, dispatch `exhausted`).

2. **Single cut decision, at the furthest-ahead block.** When the apply loop observes
   `sizeEst > batchLimit`, it computes `M = max(lastExecutedBlock, lastFoldedBlock)`
   and lets the lagging loop catch up to `M` before committing — "stop at the furthest
   ahead, let the lagging catch up." The fold-ahead cap `C` (see below) bounds the
   catch-up to ≤ `C` blocks in either direction, so the whole of `sd.mem` can be
   flushed at `M` with state and commitment both consistent at `M` — no txNum-scoped
   flush, no rollback.

   **Fold-ahead cap `C`.** The calculator must not fold more than `C` blocks past
   exec's applied position (`committer.go` `maybeFoldAhead`/`foldGateOpen`). Today this
   is implicitly `+1` (the fold gate needs `blockResult(n-1)`); the fully-BAL-driven
   mode must carry an explicit `C` so the catch-up-at-cut stays bounded and the excess
   held folds stay bounded.

3. **The cut cancels both exec and commitment, with a clean-exit cause.** Reuse the
   cancellation channel the error path already uses, but distinguish the cause:
   - `errDeliberateStop` — an actual failure (wrong root, etc.); already exists.
   - `errCleanBatchExit` — NEW: a clean resource-driven batch boundary, not an error.
   The teardown path maps `errCleanBatchExit` to the existing `ErrLoopExhausted`
   stage-loop signal ("more work pending, resume next cycle"); `errDeliberateStop`
   keeps its current failure semantics. The calculator already only *publishes*
   (items 3/4) — it does not self-cancel — so the apply loop cancelling the shared
   `executorContext` cleanly stops both the fold and the workers.

4. **Invert the shutdown-ownership invariant deliberately.** Today exec3_parallel.go
   ~450-454 documents: "Apply loop exits ONLY when applyResults is closed by the exec
   loop … the exec loop owns shutdown sequencing." This design moves that authority to
   the apply loop. Rewrite that invariant comment to match, and re-audit every drain /
   close-ordering site that relied on the exec loop closing `applyResults` first
   (`closeApplyChannels`, the `rootResults` drain on `applyResults` close, the
   completeness check `appliedBlocks` vs `txResultBlocks`). The close ordering
   (commitResults before applyResults) must still hold so the calculator drains and
   closes `rootResults` before the apply loop stops reading it.

## Requirement: calculator more than one block ahead

Do NOT assume the fold is at most one block ahead. In a fully BAL-driven batch the
calculator can compute commitment straight from the BALs without waiting on execution,
so at a resource cut the commitment progress `F` may be many blocks ahead of the exec
progress `E` (feasible, if unlikely, within a single batch). The cut logic must be
correct for arbitrary `F - E`, not just `+1`.

Consistency constraint at the commit boundary `M`:

- A block's **state** (account/storage values) is only in `sd.mem` if exec executed it
  — a fold never writes state values. So `M ≤ E`.
- A block's **commitment** must also be present — folds are monotonic, so commitment
  through `E` is present whenever `F ≥ E`. So the furthest consistent boundary is
  `M = E` (when `F ≥ E`), or `M = F` (when commitment lags, then let the fold catch up
  to `E` — cheap, it's just more folding from BALs).

The open sub-problem this exposes: when `F > E` at cut time, the folds for `E+1 … F`
are already in `sd.mem` (commitment ahead of state) and must not be flushed to disk
without matching state.

**Crux resolved (2026-07-03): cap the fold-ahead distance; do NOT partition the flush.**
`SharedDomains.Flush` → `sd.mem.Flush` → `TemporalMemBatch.flushWriters` flushes *all*
domain writers with no txNum ceiling (verified: domain_shared.go:873 → temporal_mem_batch.go
`flushWriters`; it has `if w == nil` nil-guards but no per-txNum filter). So a
flush-at-boundary would need new txNum-partition machinery, and truncate-excess would
need a commitment-only in-memory rollback — both are heavy. Avoid both by keeping the
fold from ever running far ahead:

- **Bound fold-ahead by a small constant `C`.** The calculator folds at most `C` blocks
  past exec's applied position. Then at a resource cut the lagging loop catches up at
  most `C` blocks (exec runs ≤ `C` more, or the fold does), so committing at
  `M = max(E, F)` and flushing *all* of `sd.mem` is always consistent — no partition,
  no rollback. `C` also bounds the held-fold memory.
- The parallelism win is unaffected: any `C ≥ 2` keeps exec from ever idling on
  commitment; `C` just caps how far the fold may outrun exec.
- The current code already bounds fold-ahead to `+1` implicitly — `foldGateOpen`
  requires `blockResult(n-1)`, so `F ≤ E+1`. The future fully-BAL-driven mode (fold
  without waiting on exec results) must carry an **explicit** `C` cap so this invariant
  survives; that is the one line the >1-ahead requirement adds.

So the flush stays "all of sd.mem"; correctness comes from the fold-ahead cap making
`max(E,F)` always cheaply reachable by both loops.

## Interaction with the step-straddle fold-gate (item 2) — future refinement

The item-2 fold-gate (a block crossing a step boundary drops to the incremental path)
is a correct stopgap, not the complete BAL-driven behaviour. Two refinements fall out
of the more-than-one-block-ahead mode and should be folded into this work:

- **Handle the step straddle inside the BAL process, don't punt the whole block.** The
  BAL has per-tx granularity (index + block-start txNum), so the fold can compute up to
  the interior step boundary from the BAL, emit the mid-block checkpoint the storage
  model needs, then continue folding the remainder — instead of abandoning the fold and
  re-running the block incrementally. This keeps far-ahead blocks on the fast path even
  when they straddle a step edge.
- **Decide BAL-vs-incremental lazily, not eagerly at request time.** When the
  calculator is many blocks ahead, the mode choice for a block can be deferred until the
  commit boundary is known: a block only needs to be made incremental if it will
  actually be the straddling block at a checkpoint. Eager classification in
  `handleBlockRequest` (current code) over-commits far-ahead blocks to incremental
  before it is known whether the straddle matters for the chosen boundary `M`.

Both depend on the mid-block step checkpoint being expressible from a fold, which is the
same storage-model concern as the flush-boundary crux above. Until then the eager
fold-gate stands.

## Why not the lighter variant

The low-risk alternative (keep the cut in the exec loop, but defer honoring it while
`pe.txExecutor.lastCommittedBlockNum > blockResult.BlockNum` so exec catches up to the
fold) fixes the orphan with ~5 lines and preserves the current ownership invariant. It
was rejected in favour of the full inversion because it leaves resource estimation
split across loops (the ambiguity Mark wants gone) and only patches the symptom. If the
full design proves too risky to land in one step, this remains a valid intermediate.

## Tests required

The change is consensus-critical (batch boundary). It must not merge without:

1. **Unit — cut point selection.** Pure-function test (mirroring
   `TestExecLoopShouldExitPriority`) over the new apply-loop decision: given
   (lastExecuted, lastFolded, sizeEst, batchLimit, maxBlockNum, exhausted), assert the
   chosen commit block `M` and the cause (`errCleanBatchExit` vs `errDeliberateStop`
   vs continue). Cover: fold ahead by 1, **fold ahead by many (F ≫ E)**, exec ahead,
   equal, size-limit-and-maxBlock overlap, size-limit-and-exhausted overlap. Assert
   `M = E` when `F ≥ E` and `M = F` (fold catches up to E) when commitment lags.

2. **Unit — clean vs error cause mapping.** Assert `errCleanBatchExit` maps to
   `ErrLoopExhausted` at the stage boundary (resume, not fail) and `errDeliberateStop`
   maps to the failure path (no phantom "more work" on a genuine stop). Guards the
   single-block fork-validation batch that must NOT return `ErrLoopExhausted`.

3. **Unit — orphan regression (the bug).** Drive the calculator + apply loop with a
   scripted stream where the fold reaches K+1 while the size cut fires at K; assert the
   committed boundary is a single consistent block `M` (state-domain txNum and
   commitment-domain txNum agree), never commitment@K+1 with state@K. Red before the
   fix, green after. Add a variant with the fold at the cap `C` ahead (F = E+C) and
   assert exec catches up to `M = F` and the flushed boundary is consistent (state and
   commitment both at `M`).

3a. **Unit — fold-ahead cap.** Assert the calculator never folds more than `C` blocks
   past exec's applied position (drive results slowly, feed many BALs, check
   `lastFoldedBlock - lastAppliedBlock ≤ C`). Guards the invariant the flush-all-at-M
   correctness rests on.

4. **Concurrency / drain ordering.** Extend the existing apply-loop close-ordering
   coverage: with the authority inverted, assert no result is processed on a closed
   channel and the completeness check (`applyLoopMissingBlocks`) still fires for a
   silently-missed block.

5. **Integration gate (required before merge).**
   - `hive` engine-api (`/hive-test api`) — 0 failures.
   - `hive` eest-devnet BAL (`/hive-test eest-devnet`) — 0 failures (parallel path,
     `--experimental.bal`).
   - Mainnet-tip A/B: run a batch that actually hits the size cut mid-batch (small
     `--batchSize`) with `dbg.BALDrivenCommitment` on, confirm zero wrong-root across a
     restart at the cut boundary.

## Touch points

- `execution/stagedsync/exec3_parallel.go` — `processResults` (remove size arm),
  the apply-loop goroutine (add resource authority + cut-at-M + catch-up), `execLoop`
  shutdown sequencing, `triggerBatchCommitment`, `closeApplyChannels`, the 450-454
  invariant comment, `errDeliberateStop` companion `errCleanBatchExit`.
- `execution/stagedsync/exec3.go` — `executeBlocks` / dispatch: keep `exhausted`
  (blockLimit/step natural end) but ensure it no longer double-decides the resource
  cut.
- `execution/stagedsync/exec3_metrics.go` — `SizeEstimate*` logging moves with the
  authority.
- `execution/stagedsync/committer.go` — `maybeFoldAhead`/`foldGateOpen`: enforce the
  explicit fold-ahead cap `C` (today implicit `+1`); expose `lastFoldedBlock` to the
  apply loop for the `M = max(E, F)` decision if not already visible via
  `pe.txExecutor.lastCommittedBlockNum`.
- New/updated tests per the list above.

## Open design questions (for the implementation pass)

- **Value of `C`.** `+1` (today) is safe and simplest; a small `C` (e.g. 2–8) buys more
  exec/commitment overlap slack at the cost of ≤ `C` blocks of catch-up memory at a
  cut. Pick when the fully-BAL-driven mode lands; `+1` is fine for the current PR.
- **Where the apply loop reads `F`.** `pe.txExecutor.lastCommittedBlockNum` is updated
  from `rootResults` via `handleCommitResult`; confirm it reflects *folded* (not just
  incrementally-computed) blocks so `M = max(E, F)` is correct, else thread
  `lastFoldedBlock` explicitly.
