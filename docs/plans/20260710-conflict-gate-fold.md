# Fresh-build account-plane fork-join: parallelize the whole-fresh commit, close the 4.5× bulk-load gap

## Overview

The frontier commitment engine wins steady-state (~4× faster than main on seedable/incremental commits)
but loses **~4.5× on a fresh mixed build** (`1MWhales`: 900 vs main's 202 ms). Cause: `derive`
(`fold_dag.go:160`) gates the *fork* on `seedable` — on a fresh build nothing is seedable, so the whole
account plane demotes to the serial root finale; only big fresh whales split off.

This plan takes the **simple, safe slice that captures the entire measured prize**: when the on-disk
state is **empty (a whole-fresh build — genesis / initial sync)**, fork the account plane per-prefix via
the fresh `foldNode` body (empty-wall), instead of demoting to serial. Everything else — the seedable
path, demotion, the depth-64 seam wiring, streaming — is **left exactly as frontier does it**.

This is additive and **internally routed — no new user flag**: inside the parallel/streaming commitment
engine, a whole-fresh commit takes the conflict-gate fork; any non-empty state takes the existing
frontier path unchanged. The only user-facing flags stay `--experimental.parallel-commitment` /
`--experimental.streaming-commitment`. "Frontier as a kept side version" is the *in-engine* fallback for
non-fresh state, not a separate flag. A **test-only package toggle** drives fork-vs-serial A/B
benchmarking during development; it never becomes a CLI flag.

**Why this slice and not the general conflict-gate model:** review established that a *seedable* fork
must keep the ctx-ful mount+replay body (`foldNode` is fresh-only, nil-context — `truthtree_fold.go:289`),
and that a divergence inside an on-disk extension (`errStorageBaseNotBranch`) needs a base case the
general model under-scopes. Those risks only exist when on-disk state is present. On a **whole-fresh
build there is no on-disk state**, so none of them arise — correctness reduces to "empty-wall fork ==
sequential." The general model (fork at every gate over mixed state, two fold bodies, extension case,
streaming reconciliation) is the documented follow-up, not this plan.

The only real work: generalize the storage-plane-only fork-join (`forkFolder`, `truthtree_fold.go:339`)
to the **account plane**, including the depth-64 account/storage seam — which `foldNode` already crosses.
`foldFreshForkJoin` (`fold_pool.go:567`) is the storage precedent to lift.

Target: flag-on `1MWhales` ∥ from 900 → ~200 ms (approach/beat main); **zero** change to steady-state
(seedable path untouched); roots + stored branches byte-identical to sequential.

## Context (verified against `awskii/truthtree-forkjoin` @ d5757bfe66, package `execution/commitment`)

- **The gate:** `derive` (`fold_dag.go:131`), `:160` `if sc <= b.k || !b.seedable(prefix) { leaf }`;
  seam cases `:142` (seedable whale split) / `:155` (`freshWhaleCandidate`). Shared by flag-off frontier —
  **must not be edited unconditionally** (breaks the fallback + `seedMerge`).
- **Fresh fold body (reuse):** `foldCtx.foldNode` (`truthtree_fold.go:164`) — buffer-reuse, crosses the
  depth-64 seam, nil-context (fresh-only, correct for empty state). `newFoldCtx` `:289`.
- **Fork-join to generalize:** `forkFolder` / `foldFreshForkJoin` (`truthtree_fold.go:339`,
  `fold_pool.go:567`) — per-prefix recursive fork-join, **currently storage-plane-only** (rejects the
  account seam at `:364`). Extending it to the account plane is the core task.
- **Kept untouched (the fallback + steady-state win):** `seedMerge` (`fold_pool.go:232`), the mount+replay
  bodies (`foldMergeTask`/`foldLeafTask`), `seedOrDemote` (`streaming_deep_fold.go:70`), the demotion path,
  `reuseSchedulerCells` (`streaming_commitment.go:260`), `foldFreshStorage` fallback (single-first-nibble
  extension collapse + delete-slot, `fold_pool.go:406`).
- **Whole-fresh detection:** the commit is whole-fresh iff the on-disk state is empty. Detect via the
  root being unseedable **and no seedable prefix found anywhere during derivation** (a propagate-folded
  root is not empty — it has deeper seedable branches, so this correctly excludes it), or a caller-supplied
  fresh-build hint. Fail safe: any doubt → frontier path.
- **The whole-fresh path bypasses the seedable machinery entirely** — it folds the `prefixNode` via
  `forkFolder` with empty-wall bases, so `seedMerge`'s fail-closed-on-absent-branch (`fold_pool.go:232`,
  by design) never fires on it. Adversarial review confirmed every failure mode (spanning on-disk leaf
  across consecutive unseedable gates, present-branch-behind-extension, embedded subtree, nil-ctx
  reconcile of untouched siblings) requires on-disk state and **cannot arise on an empty build**.
- **Scope boundary (fail-safe):** `processMounted` rejects a non-empty root extension
  (`parallel_mount.go:99`). Uniform hashed keys branch at the root (1MWhales qualifies); a
  common-prefix / single-account fresh corpus that yields a root extension routes to frontier, not the
  fork path.
- **Eager fold already exists:** `dispatchFoldTasks` fires a parent when its pending counter hits zero
  (`fold_pool.go:699`); only the root finale is a deliberate serial barrier. No new machinery — verify it.
- **Oracle:** `runEngineBatchesParity` (root + `requireBranchParity` stored-branch bytes), N≥3.
- **Perf:** `Benchmark_Commitment_1MWhales` (fresh mixed, whole-`Process`), incremental seeded bench
  (`incremental-whale120k`, `parallel_streaming_bench_test.go:518`). Bench with **carried updates**
  (`TouchPlainKeyDirect`, `commitment.go:1687`) to match production — `WrapKeyUpdates`/`Insert(nil)`
  forces ctx re-reads production never does. Baselines: main ∥ 202 ms; frontier ∥ 900 ms; frontier
  incremental ∥ 3.83 ms.
- Fork-count counters exist for red-first assertions: `forkFoldMaxDepth`, `directWhaleStorageFolds`
  (`fold_pool.go:323`, `truthtree_fold.go:306`).

## Development Approach

- TDD **red → green → refactor**. Order: **correctness > fail-closed > performance > simplicity.**
- **Additive + internally routed (no new user flag):** the whole-fresh fork path is gated on empty-state
  detection *inside* the parallel/streaming engine; every non-empty commit keeps the exact frontier
  behavior. Selection is by the existing parallel/streaming mode flags only. `runEngineBatchesParity`
  stays green at every task; the whole-fresh path goes red→green. A test-only package toggle (default:
  fork on) lets benches measure the frontier-serial baseline — it is dev-only, never a CLI flag.
- **Red-first means asserting the new path ran**, not just parity (frontier is already parity-correct):
  tests assert the whole-fresh fork executed (fork-count/depth counters) AND parity == sequential.
- **Scope guard — do not touch on-disk-state handling:** no changes to `seedMerge`, the mount+replay
  bodies, demotion, `foldFreshStorage`'s fallback, or streaming reuse. If a corpus has any on-disk state,
  it must route to frontier unchanged.
- Buffer-per-lineage; alloc ceilings gate fold-touching tasks.
- Do not flip the default flag until parity holds AND `1MWhales` ∥ ≤ main AND incremental ∥ unchanged.

## Testing Strategy

- Parity: `runEngineBatchesParity` (root + branches) on fresh whole-build corpora (whales + tail
  accounts + seam), N≥3, flag-on == flag-off == sequential.
- **Guard tests:** a corpus with on-disk state (incremental) must take the frontier path (assert the
  whole-fresh path did NOT fire) and stay byte-identical — proves the slice is additive.
- Alloc: parallel arm buffer-per-lineage within ceiling.
- Perf (carried updates): `1MWhales` ∥ approaches/beats main; incremental ∥ == frontier (untouched).
- `make lint && make test-short` after each task; full parity at milestone boundaries.

## Milestone 1 — Whole-fresh detection + flag (additive, green at every step)

### Task 1: Empty-state gate — route whole-fresh builds to a new path (no CLI flag)

**Files:**
- Modify: `execution/commitment/fold_pool.go`
- Modify: `execution/commitment/frontier_parity_test.go` (or new `fresh_build_fork_test.go`)

- [x] add a whole-fresh detector *inside* the parallel/streaming engine (empty on-disk state: unseedable
      root + no seedable prefix seen during derivation, or caller hint). Non-empty state → frontier path
      unchanged. **No CLI flag** — the existing parallel/streaming mode selection is the only user flag.
      (`wholeFreshBuild`, `fold_pool.go`: root-prefix probe + `sawSeedable` tracking in `dispatchFrontier`.)
- [x] add a **test-only package toggle** (default: fork on) so benches can measure the frontier-serial
      baseline; dev-only, never wired to a CLI flag. (`forkWholeFresh atomic.Bool`, default on via `init`.)
- [x] wire a new entry the whole-fresh case routes to (stub → falls back to frontier for now, tree stays green).
      (`dispatchWholeFresh` counts the route via `wholeFreshFolds` and delegates to the extracted `foldFrontierBody`.)
- [x] test: a fresh corpus hits the new entry (assert via counter, red until Task 2); an incremental corpus
      does NOT (guard test, green now); toggle-off == frontier. (`fresh_build_fork_test.go`: detector unit test +
      fresh-routes/incremental-guard/toggle-off parity across all three engine modes. The counter proves the
      route fired; the account-plane fork itself lands in Task 2.)
- [x] `make test-short`. Before Task 2.

## Milestone 2 — Account-plane fork-join over `foldNode`

### Task 2: Generalize `forkFolder` from storage-only to the account plane (incl. depth-64 seam)

**Files:**
- Modify: `execution/commitment/truthtree_fold.go`, `execution/commitment/fold_pool.go`

- [x] lift the storage-plane-only restriction (`truthtree_fold.go:364`): `forkFolder`/`foldFreshForkJoin`
      forks account-plane conflict gates too, folding account cells (with their storage roots) via
      `foldNode`, recursing across the depth-64 seam into each account's (fresh) storage.
      (`forkFolder.fold` now mirrors `foldNode`'s child classification by plane — `setPlaneLeaf` picks
      account/storage by key length — and the depth-64 seam cases recurse via `ff.fold` so a whale's
      fresh storage forks on the same threshold as the account plane above it.)
- [x] the whole-fresh entry (Task 1) folds the entire account plane per-prefix, empty-wall, buffer-per-lineage,
      down to the `foldK` grain; eager fold via the existing pending counters.
      (`dispatchWholeFresh` folds each top-nibble subtree via `foldFreshAccountSubtreeCellForkJoin`
      through one shared `forkFolder{k: foldK(root.subtreeCount, numWorkers)}`, stitches the mount-wall
      cells into base, and folds base — the fork/join barrier is the eager-fold mechanism. Materializes
      ModeParallel nil updates once up front, and routes any delete, orphan storage, or non-pure-branch
      top nibble to the frontier fold — fail-safe.)
- [x] parity: `TestFreshBuild_AccountPlane` (whales + tail + seam), flag-on == flag-off == sequential,
      N≥3, AND the fork path fired (counter). Guard test (incremental → frontier) stays green.
      (3 seed/worker configs × parallel/streaming/streaming-scheduled; `wholeFreshForkJoins` proves the
      fork ran, `forkFoldMaxDepth` proves it fanned out below the top nibble.)
- [x] alloc: parallel arm within ceiling; serial grain ≤ existing ceiling.
      (`TestFreshBuild_AccountPlaneForkJoinAllocCeiling`: 2.5 MB/op, well under the 96 MB ceiling.)
- [x] full parity suite. Before Task 3. (`make lint && make test-short` clean; `make erigon integration` builds.)

### Task 3: Seam correctness on the account plane

**Files:**
- Modify: `execution/commitment/truthtree_fold.go`

- [x] verify the account→storage-root wiring holds under the account-plane fork (each account's fresh
      storage folds via the same recursion; the account cell gets its storage root before its branch keccak).
      Reuse `foldNode`'s existing seam handling — do not degenerate the seam to a plain nibble fork.
      (`forkFolder.fold` handles the three seam kinds — `childSeamAccount`/`childSeamAccountExt`/
      `childSeamAccountInline` — exactly as `foldNode`, recursing the storage root via `ff.fold`, placing it
      into the account cell via `setSeamAccount`/`setSeamAccountExt` before the branch keccak deferred to the
      shared `hashBranchRow`. `TestFreshBuild_AccountForkJoinSeamParity` pins it deterministically: an
      unbounded-sem, low-K `ff.fold` at depth 0 provably forks below the seam — `forkFoldMaxDepth > 64` — into
      the whale's fresh storage and reproduces the serial fold + sequential oracle byte-for-byte.)
- [x] parity tests: touched account with fresh storage across the seam, single-first-nibble account,
      accounts sharing top nibbles; == sequential.
      (`TestFreshBuild_Seam`: three scenarios × 3 engine modes, each flag-on == flag-off == sequential
      root + stored branches, fork-join fired. Corpora: `freshWhaleAccountPlaneCorpus` (whale storage across
      the seam), `freshSeamSingleNibbleCorpus` (ext + inline single-first-nibble seams), and
      `freshSeamSharedTopNibbleCorpus` (deep shared-top-nibble account branches).)
- [x] `make test-short` + full parity. Before Task 4. (`make lint` clean twice, full commitment `-short`
      suite green, race-clean, `make erigon integration` builds.)

## Milestone 3 — Performance (production-representative)

### Task 4: 1MWhales + incremental benches with carried updates

**Files:**
- Modify: `execution/commitment/parallel_streaming_bench_test.go`

- [x] switch the whale + incremental benches to carried updates (`TouchPlainKeyDirect`); keep a
      `WrapKeyUpdates` variant only for the parity harness.
      (`wrapCarriedUpdates` (TouchPlainKeyDirect) vs `wrapKeyUpdatesParallel` (WrapKeyUpdates) selected
      via a `benchWrap` func; `runParallelBenchCarried`/`runIncrementalBenchCarried` use the carried
      variant. `Benchmark_Commitment_1MWhales` switched to carried; the WrapKeyUpdates path is retained
      only for the parity harness. New `benchForkWholeFresh` toggle helper mirrors the test helper.)
- [x] measure flag-on vs flag-off (frontier) vs main on `1MWhales` (fresh) and the incremental delta.
      (`Benchmark_FreshBuildFork`: flag-on/flag-off × worker-sweep on 1MWhales + incremental. Numbers
      below. "main" is an external baseline — not runnable in this worktree — referenced from Context.)
- [x] acceptance: flag-on `1MWhales` ∥ approaches/beats main (900 → ~200 ms); flag-on incremental ∥
      unchanged vs frontier (~3.83 ms — the seedable path is untouched).
      (**Incremental guard: MET** — flag-on == flag-off (24.4 ms == 24.4 ms on M5 Max), whole-fresh path
      dormant on seeded state. **1MWhales vs frontier: 2× faster at oversubscription** (w≥2×NumCPU:
      357 ms vs frontier's flat ~715 ms), but **~13% slower at NumCPU** (800 ms vs 707 ms). **vs main's
      ~200 ms: NOT reached** on this hardware — best flag-on 357 ms; the M5-Max frontier baseline is
      707 ms not the plan's 900 ms, so direct ms-comparison to a different-machine 202 ms is unsound;
      the meaningful in-repo result is the 2× fork-over-frontier win at oversubscription. Finding for
      Task 6's default-flag decision: the win is oversubscription-gated — see mechanism below.)
- [x] record numbers in the plan. Before Task 5. (Results table below.)

#### Task 4 results (measured — M5 Max, 18 cores arm64, carried updates, `-benchtime=15x`)

`Benchmark_FreshBuildFork`, roots byte-identical between flag-on/flag-off (`0fdf30b1…`), stored branches
parity-clean.

1MWhales (fresh, 1,095,003 keys):

| workers | flag-off (frontier) | flag-on (fork) | fork vs frontier |
|---------|---------------------|----------------|------------------|
| NumCPU=18 | 707 ms | 800 ms | 1.13× slower |
| 2×=36 | 716 ms | 358 ms | 2.00× faster |
| 4×=72 | 720 ms | 357 ms | 2.02× faster |

Peak mem/op: flag-off ~513 MB, flag-on ~383 MB (fork ~25% lower, ~12% more allocs). ModeDirect
sequential anchor: 1172 ms.

incremental-whale120k (seeded, on-disk state — the guard):

| flag-off | flag-on |
|----------|---------|
| 24.4 ms | 24.4 ms |

Identical → the whole-fresh route stays dormant when on-disk state is present; the seedable path is
byte- and perf-untouched. (Absolute 24 ms vs the plan's 3.83 ms baseline is machine/corpus scaling, not
a regression — the flag-on/flag-off delta is what proves the guard, and it is zero.)

Mechanism (why the win is oversubscription-gated): frontier is flat (~710 ms) across the worker sweep —
`freshWhaleCandidate` splits the whale storage independently of grain, giving fixed parallelism. The
whole-fresh account-plane fork's grain is `foldK = subtreeCount/numWorkers`; on a whale-dominated corpus
`subtreeCount` is dominated by the 750k-slot whale storage, so at numWorkers=NumCPU the grain (~60k) is
too coarse — the account-plane forks consume the fold budget and the whale storage folds serially behind
them (shallow `forkFoldMaxDepth`, `directWhaleStorage=0`). At numWorkers≥2×NumCPU the grain shrinks
(~30k/15k) and the whale storage splits finely enough to overlap the account plane, hitting 357 ms.
Follow-up (out of Task 4's measure-only scope, for Task 6 / the materialized-mount continuation): give
the whole-fresh dispatch an explicit whale-split (as frontier has) so the win holds at NumCPU without
oversubscription.

## Milestone 3.5 — Per-subtree grain: split the whale finely at NumCPU

### Task 5: Whale-split within the whole-fresh dispatch (close the NumCPU regression)

Problem (measured, Task 4): `foldK = subtreeCount/numWorkers` is a *global* grain. On a whale-dominated
corpus `subtreeCount` is dominated by the whale storage, so at NumCPU the grain is too coarse — the
account-plane forks consume the budget and the whale folds serially behind them (w18: 800 ms, 13% slower
than frontier); it only wins at ≥2×NumCPU (357 ms). Frontier's `freshWhaleCandidate` splits the whale
*independent* of grain (flat ~710 ms). This is the "generalized recursion" gap: concurrency must be
allocated by **subtree cost**, not a flat global grain.

**Files:**
- Modify: `execution/commitment/fold_pool.go`, `execution/commitment/truthtree_fold.go`

- [ ] make the whole-fresh fork **per-subtree adaptive**: a deep whale storage subtree keeps forking at its
      *own* `subtreeCount`/numWorkers so it splits finely at any worker count — OR invoke an explicit
      whale-split (reuse frontier's `freshWhaleCandidate` / `deepStorageThreshold`) for big-storage accounts
      inside the whole-fresh dispatch, independent of the account-plane budget.
- [ ] parity unchanged: fresh whale + account plane + seam == sequential, N≥3 (roots + branches byte-identical).
- [ ] perf (`Benchmark_FreshBuildFork`, carried updates): at **NumCPU (w18)** flag-on ≤ flag-off frontier
      (close the 13% regression, ideally beat it); at oversubscription still ≥ 2×; alloc ≤ frontier (~25% less).
- [ ] `make lint && make test-short` clean. Before Task 6.

## Milestone 4 — Verify + document

### Task 6: Verify acceptance criteria

- [ ] flag-on == flag-off == sequential: root + branch parity, N≥3, fresh corpora + seam; guard tests
      confirm incremental routes to frontier unchanged.
- [ ] `1MWhales` ∥ ≤ main; incremental ∥ == frontier; alloc ceilings held.
- [ ] no changes leaked into `seedMerge` / mount+replay / demotion / `foldFreshStorage` fallback / streaming
      reuse (`git diff` those files shows only additive wiring, if any).
- [ ] `make lint && make test-short` clean.

### Task 7: [Final] Docs; flag-default decision

**Files:**
- Modify: `docs/design/parallel-patricia-hashed.md`, package doc

- [ ] document the whole-fresh fork path (additive, in the parallel/streaming engine, no CLI flag; frontier
      untouched for on-disk state); note the materialized-mount TruthTree as the continuation.
- [ ] confirm no CLI flag shipped and the test-only toggle is dev-only; whole-node validation before relying
      on the fork path in production sync.
- [ ] move this plan to `docs/plans/completed/`.

## Post-Completion

*No checkboxes — external / manual.*

- **Branch:** `awskii/fresh-build-fork` off `awskii/truthtree-forkjoin` @ `d5757bfe66` (has `foldNode` +
  `foldFreshForkJoin`). Stash the uncommitted single-whale Task-4 work first. Rollback: frontier is the
  flag-off engine, unchanged.
- **Whole-node validation before default-on:** flag-on vs flag-off root comparison over an initial-sync
  (fresh-heavy) block range — where the win is largest — plus a steady-state range to confirm no regression.
- **Honest scope:** the win is **fresh/bulk-load time** (initial sync, genesis, fresh-heavy blocks),
  closing the 4.5× gap to main while frontier keeps its ~4× steady-state lead. Steady-state is byte- and
  perf-identical (untouched). Not a memory change.
- **Continuation — the materialized-mount TruthTree (the full model, no new flag):** instead of a nil-ctx
  fresh fold + per-fork ctx reads, **unfold the on-disk mounts into the tree during build** — materialize
  siblings / spanning leaves / extensions as tree cells, carrying each untouched leaf's value or
  position-invariant hash — so the parallel fork-join folds **uniformly and ctx-free** over one
  materialized tree (TruthTree "knows all mounts and traverses seamlessly"). This *solves* the on-disk
  breaks (nil-ctx, spanning leaf, extension) rather than avoiding them; the whole-fresh slice above is its
  degenerate case (nothing to mount) and builds the account-plane fork-join it reuses. **Crux to measure:**
  the mount reads happen either way — materializing them during build must be **amortized (unfold-on-touch,
  the streaming tumbler) or parallelized** to beat frontier's seed-during-fold. Lands in the
  parallel/streaming engine; no special flag.
