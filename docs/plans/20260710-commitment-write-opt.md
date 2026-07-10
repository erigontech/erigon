# Parallel-engine commitment write optimizations: kill deferred-update clones + merge-under-fold

## Overview

Profiling the whole-fresh fork (flag-on / w18, 275 ms) showed the fold is essentially optimal; the
remaining allocation and critical-path costs are in the **deferred branch-update path**, not the fold.
A subsystem investigation established that the "externalize the write" architecture is **already built**
for the parallel engine: `ctx.PutBranch` writes only to `sd.mem` (never MDBX; durability is at
`SharedDomains.Commit`→`tx.Commit()` at the FCU seal), encode already runs under the fold
(`DeferBranchUpdates=true`), and the parallel engine applying blocks **already defers**
(`exec3.go:206` — `parallel && isApplyingBlocks → SetDeferCommitmentUpdates(true)`), so the
end-of-`Process` apply **barrier never runs on it**. The barrier (`hex_patricia_hashed.go:2612`,
`!leaveDeferredForCaller`) runs **only on the serial (default) path**.

**Constraints (hard):**
- **The parallel trie is not the default.** Do not switch the default engine.
- **The serial path must not be affected** — no behavior change, byte-identical roots and branch bytes.
  The serial barrier stays; its dup-prefix flush-then-read stays. Nothing here touches the serial engine's
  correctness.

So this plan is a **tight, parallel-only perf change** — two wins the investigation isolated:
1. **Kill `bytes.Clone` in the deferred path** — `getDeferredUpdate` does 3 copies/branch
   (`commitment.go:276-278`) + `mergeDeferredUpdate` a 4th (`:392`); it was ~8% of allocs in the profile.
   Replace with a per-block arena owned by `BranchEncoder`. Pure allocation change, **byte-identical**, so
   it does not affect the serial path's behavior even though `BranchEncoder` is shared.
2. **Merge-under-fold (parallel engine)** — the parallel engine's returned deferred list is still
   **pre-merge** (`raw`+`prev`); the per-branch merge (`raw`⊕`prev`→`encoded`) runs later at *flush*
   (`flushPendingUpdates`→`ApplyDeferredBranchUpdates` merge pass, `commitment.go:455-482`), on the next
   block's `computeCommitment` critical path. Run the merge on the fold workers so the returned list
   carries `encoded` and the flush is a pure `sd.mem` memcpy — pipelining block N's merge with N+1's exec.

No new architecture, no FCU/`Commit` rework, no reader-visibility layer (`sd.mem` is already the
coherence layer, one block deep; builder/`getProof` read committed DB only). The "external apply" is the
existing `Commit`.

In short: **materialize the branches concurrently on the fold workers, fold the parents safely** (the
eager bottom-up wavefold, already present via the pending-counter dispatch), **and write only on the
`Commit` hook** (already the parallel engine's behavior). The two deltas here are (a) moving the
materialization/merge onto the fold and (b) killing the per-branch clones — nothing else.

Target: reduced allocs/op on the parallel engine (kill the clone) + merge off the flush critical path;
**serial path byte- and behavior-identical**; root parity == `header.Root` unchanged.

## Context (verified against `awskii/fresh-build-fork` @ afafd273ec, package `execution/commitment`)

- **Defer condition:** `exec3.go:206` `if isForkValidation || (parallel && isApplyingBlocks) { SetDeferCommitmentUpdates(true) }`. Parallel-block-apply already defers.
- **The serial-only barrier (do NOT touch):** `hex_patricia_hashed.go:2612` `if DeferUpdatesEnabled() && !leaveDeferredForCaller { ApplyDeferredUpdates(NumCPU, ctx.PutBranch) }`.
- **The clones to kill:** `getDeferredUpdate` (`commitment.go:272-282`, 3× `common.Copy` at `:276-278`),
  `mergeDeferredUpdate` (`commitment.go:392`, `common.Copy(merged)`). `BranchEncoder.ClearDeferred`
  (`commitment.go:369-378`) is the per-block reset point (arena reset hook).
- **The merge pass to move:** `ApplyDeferredBranchUpdates` (`commitment.go:419`) splits a parallel **merge**
  half (`:455-482`, fills `upd.encoded`) + a sequential **write** half (`:484-493`). Only the merge half moves.
- **Parallel deferred return:** `parallel_patricia_hashed.go:351` (`p.deferredForCaller = pu.deferredCombined`), take at `:101`; per-fork encode via `CollectDeferredUpdate` (`parallel_patricia_hashed.go:340-347`).
- **Flush (where merge currently runs):** `domain_shared.go:1760-1773` (`computeCommitment` flushes prev
  block's `pendingUpdate` into `sd.mem` before folding current) → `FlushPendingUpdates` → `ApplyDeferredBranchUpdates`.
- **`DeferredBranchUpdate`** (`commitment.go:237-246`): carries `prefix`, `raw`, `prev`, `encoded` — the
  contract change is "return with `encoded` set, `raw`/`prev` cleared."
- **Oracle:** root == `header.Root` over a real block range (`computeAndCheckCommitmentV3`, `exec3.go:766`);
  plus `runEngineBatchesParity` (root + stored-branch bytes) for the parallel engine.

## Development Approach

- TDD **red → green → refactor**. Order: **correctness > fail-closed > performance > simplicity.**
- **Serial-path invariant (load-bearing):** the serial engine's behavior is byte-identical after every
  task. The arena change must produce identical encoded bytes; the merge-move touches only the parallel
  engine's deferred return + `flushPendingUpdates` memcpy path. A `git diff` of `hex_patricia_hashed.go`'s
  serial `Process`/barrier path shows no behavior change; a serial-engine root-parity test stays green.
- **No default/CLI change.** Parallel stays experimental; no flag flips.
- Benchmark every perf claim (parallel-engine microbench + the fresh-build `Benchmark_FreshBuildFork`);
  no estimated numbers.
- `make lint && make test-short` after each task; full parity + serial-byte-identity at milestone boundaries.

## Testing Strategy

- Parity: parallel-engine root + stored-branch parity == sequential, N≥3, on the whale + mixed corpora
  (unchanged by this work).
- **Serial-identity guard:** a serial-engine (`ModeDirect`) root+branch test proving byte-identical output
  before/after the arena change.
- Alloc: parallel-engine deferred path allocs/op down after the clone kill; arena lifetime bounded to the block.
- Perf: `flushPendingUpdates` merge time → ~0 (memcpy) after merge-under-fold; `Benchmark_FreshBuildFork`
  not regressed (ideally improved).
- `make lint && make test-short` after each task.

## Milestone 0 — Baseline (no code)

### Task 1: Characterize the deferred path

- [x] bench the parallel engine's deferred path: allocs/op in `getDeferredUpdate`/`mergeDeferredUpdate`
      (memprofile), and `flushPendingUpdates` merge-pass wall time per block on the whale corpus. Record the
      numbers this plan must beat. No code change.

#### Baseline (Task 1, recorded 2026-07-10 @ e2e3235191, Apple M5 Max 18-core, go1.25.7)

Repro: `go test -run '^$' -bench <name> -benchmem -cpuprofile -memprofile ./execution/commitment/`;
pprof numbers via `-show/-focus 'getDeferredUpdate|mergeDeferredUpdate|ApplyDeferredBranchUpdates'`.
The benches apply deferred updates inline in `Process` (same `ApplyDeferredBranchUpdates` halves that
`flushPendingUpdates` runs with `runtime.NumCPU()` workers in production), so the merge/write splits
below are the flush-pass costs Task 3 targets.

**Per-branch clone cost (microbench, 3 allocs = the 3 `common.Copy` at `commitment.go:276-278`):**
- `BenchmarkGetDeferredUpdate` (full 16-cell branch): 139 ns/op, 912 B/op, 3 allocs/op
- `BenchmarkGetDeferredUpdate_FewCells` (2-cell): 43 ns/op, 144 B/op, 3 allocs/op

**Whole-fresh whale block (`Benchmark_FreshBuildFork/1MWhales/flag-on/w18`, 5x, ~1.05M keys):**
- 263.2 ms/op, 387.7 MB/op, 8.454M allocs/op (timed window)
- `getDeferredUpdate`: 133.5 MB + 872k heap allocs per block (7.5% of run alloc_space, ~10% of the
  timed window's allocs/op — the plan's "~8% of allocs"); ≈436k deferred branches/block (2 allocs each:
  prefix+raw; empty `prev` copy doesn't allocate). CPU incl. copy children: 84 ms/block across workers.
- merge pass: **0 CPU samples** — whole-fresh `prev` is empty, `mergeDeferredUpdate` is a no-op path.
- apply pass (`ApplyDeferredBranchUpdates`): 36 ms CPU/block, of which the sequential `PutBranch`
  write loop = 34 ms (wall≈CPU, sequential; MockState map — production writes `sd.mem`), merge chunk ≈2 ms.

**Seeded whale block (`Benchmark_FreshBuildFork/incremental-whale120k/flag-on`, 40x, 120k-slot retouch):**
- 24.5 ms/op, 97.1 MB/op, 1.116M allocs/op (timed = batch2 only; profile spans batch1+batch2)
- `getDeferredUpdate`: 42.5 MB + 233.5k allocs per block-pair (10.9% of run alloc_space)
- `mergeDeferredUpdate` (the 4th clone, `commitment.go:392`): 12.2 MB + 44.6k allocs per seeded block
  (≈44.6k branches actually merged against a non-empty `prev`)
- **merge-pass baseline Task 3 must beat: 4.25 ms CPU per seeded block** (0.17s worker-goroutine cum /
  40; ~0.3–1 ms wall at 18 workers, ~4 ms sequential-equivalent) + the 12.2 MB/44.6k-alloc merged
  copies. Write half (sequential `PutBranch` loop): ≈1.5 ms/block. Post-Task-3 target: flush merge ≈0,
  flush = pure write.

**Targets:** Task 2 kills the 2–3 collection allocs/branch (872k allocs/133.5 MB per fresh 1M block;
912 B/op microbench) and the merge copy (44.6k/12.2 MB per seeded block). Task 3 moves the 4.25 ms
CPU merge pass off the flush critical path.

## Milestone 1 — Kill the deferred-update clones (byte-identical, serial-safe)

### Task 2: Per-block arena in `BranchEncoder`

**Files:**
- Modify: `execution/commitment/commitment.go`
- Modify: `execution/commitment/commitment_test.go` (or a focused bench/test file)

- [ ] red: a bench asserting reduced allocs/op for the deferred-update collection over an N-branch block
      (fails at baseline).
- [ ] green: add a per-block bump arena owned by `BranchEncoder`, reset in `ClearDeferred` (`:369-378`);
      `getDeferredUpdate` (`:272-282`) and `mergeDeferredUpdate` (`:392`) write `prefix`/`raw`/`prev`/`merged`
      into arena-backed bytes instead of `common.Copy`. Lifetime bounded to the block (freed at
      `ClearDeferred`/flush).
- [ ] **arena-lifetime safety (load-bearing — verify before writing code):** does `sd.mem` /
      `TemporalMemBatch.DomainPut` **copy** the branch bytes on write, or **retain** the caller's slice? If
      it retains, a per-block arena that resets at `ClearDeferred` recycles bytes `sd.mem` still references →
      use-after-reset. Then the arena must survive until after flush/`Commit` (not per-block), or the write
      must copy. Also confirm the serial barrier path (`:2612`) does not read arena bytes after a reset.
- [ ] **serial-identity test:** serial (`ModeDirect`) engine over a block range produces byte-identical
      roots + stored branches before/after (the arena must not change any encoded byte).
- [ ] parity: parallel-engine root + branch parity unchanged; allocs/op down.
- [ ] `make lint && make test-short`. Before Task 3.

## Milestone 2 — Merge-under-fold (parallel engine only)

### Task 3: Run the per-branch merge on the fold workers; flush becomes memcpy

**Files:**
- Modify: `execution/commitment/commitment.go` (extract `MergeDeferredBranchUpdates` = merge half of `:455-482`)
- Modify: `execution/commitment/parallel_patricia_hashed.go`

- [ ] red: a unit test asserting the parallel engine's returned deferred list has `encoded` populated and
      `raw`/`prev` cleared (fails today — returns pre-merge), AND that `flushPendingUpdates` performs no merge
      (pure memcpy) for such a list.
- [ ] green: extract `MergeDeferredBranchUpdates(deferred, numWorkers)` (merge half only, no `PutBranch`);
      run it on the parallel path before `p.deferredForCaller = pu.deferredCombined` (`:351`) — ideally per
      fork as subtrees complete (`CollectDeferredUpdate` site) to overlap the fold; make
      `ApplyDeferredBranchUpdates`/flush skip the merge when `encoded` is already set.
- [ ] **merge feasibility + unwind-changeset safety (load-bearing — verify first):** determine what
      `mergeDeferredUpdate` combines. If `prev` is the on-disk previous branch, confirm it is available on the
      fold worker (empty for whole-fresh; from the seed for seedable) — no fresh ctx read the worker can't do.
      AND: `flushPendingUpdates`'s changeset routing (`domain_shared.go:379-401`) writes the unwind undo
      record from `prev` — moving merge under the fold must NOT drop `prev` before the changeset is built.
      Keep `prev` alongside `encoded`, or build the changeset during the under-fold merge. Reorg test must
      reproduce the correct pre-block root.
- [ ] **serial path untouched:** the serial engine's `ApplyDeferredUpdates` barrier path is unchanged
      (still merges+writes inline) — `git diff` shows no serial behavior change.
- [ ] parity: parallel root + branch parity unchanged; `flushPendingUpdates` merge time → ~0.
- [ ] `make lint && make test-short`. Before Task 4.

## Milestone 3 — Verify + measure

### Task 4: Benchmark + acceptance

- [ ] parallel-engine allocs/op down vs Task 1 baseline (clone kill); `flushPendingUpdates` merge ~eliminated.
- [ ] `Benchmark_FreshBuildFork` not regressed (ideally improved at w18).
- [ ] serial engine byte-identical (root + branch) over a block range; no default/CLI change.
- [ ] full parity suite + `make lint && make test-short` clean.

### Task 5: [Final] Docs

**Files:**
- Modify: package doc / design doc

- [ ] document the arena + merge-under-fold on the parallel deferred path; note serial path untouched and
      the "externalize the write" reality (already deferred to `Commit`; this only moves merge off the flush
      critical path and kills the clones).
- [ ] move this plan to `docs/plans/completed/`.

## Post-Completion

*No checkboxes — external / manual.*

- **Branch:** `awskii/commitment-write-opt` off `awskii/fresh-build-fork` @ `afafd273ec`. Independent of the
  fresh-build fork; both live on the parallel engine.
- **Honest scope:** small, low-risk. The parallel engine already externalizes the DB write (`sd.mem` +
  `Commit`) and is already barrier-free; this only (1) removes the ~8%-of-allocs `bytes.Clone` and (2) moves
  the per-branch merge off the next-block flush critical path. Serial (default) path is untouched. Not an
  architecture change — the architecture is already there.
- **Deferred / not in scope:** making the serial engine write-free (would affect the default path — forbidden);
  the reorg-recompute-before-read hot path (`domain-epoch-unwind.md` §8); the bigger fold/build headroom
  (materialized-mount / unfold-on-touch) is a separate track.
