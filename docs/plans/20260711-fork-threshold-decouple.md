# Fork-threshold decouple: parallelize the fresh fork's serial top-nibble dispatch

## Overview

The whole-fresh account-plane fork (`dispatchWholeFresh`) folds its 16 top-nibble subtrees in a **serial
`for` loop** — each subtree's fork-join fully drains before the next starts. Measured on a fresh 1M-whale
build (M5 Max 18c): the 16 folds run back-to-back and their wall times **sum to 190 ms = 74 % of the
258 ms `Process`**, while the engine averages only **5.3 of 18 cores busy**. The 13 small uniform top
nibbles (~12k subtree each) cost 7 ms apiece — **91 ms serial**, *more than the 750k whale's 70 ms* —
because each folds nearly serially internally (its children fall below the fork threshold) and they never
overlap.

The top level is the one place the fold **doesn't** fork. Every level *inside* a subtree fork-joins via
`forkFolder`; the outermost 16-way split is a plain loop. The 16 top nibbles are disjoint hashed-key
prefixes — structurally independent, always safe to fold concurrently. Route them through the same
`foldSem`-bounded fork mechanism the interior already uses, and the serial region collapses toward the
whale's critical path.

**Target:** fresh 1M `Process` 258 ms → ~140–160 ms (past main-parallel #21945's 197 ms), avg-cores up
from 5.3, with **incremental unregressed** (different code path) and **memory bounded**.

Root computation stays **byte-identical** — this reorders *when* independent subtrees fold, never *what*
they fold to.

## Context (from discovery)

- **Base:** branch `awskii/fork-threshold-decouple` off `2ae26b58d5` (write-opt complete: fresh-build
  fork + deferred-update arena + merge-under-fold; codex-reviewed clean). Rollback: `git reset --hard
  2ae26b58d5`.
- **`forkFolder` is fresh-path-only** — constructed at `fold_pool.go:287` (`dispatchWholeFresh`),
  `fold_pool.go:727` (`foldFreshForkJoin`, whale storage), `truthtree_fold.go:493` (`storageFold`,
  nested). The incremental/frontier path uses `deriveFoldFrontier` → `dispatchFoldTasks` and **never
  touches `forkFolder`**. Keep the change inside `forkFolder`/`dispatchWholeFresh` and do **not** alter
  shared `foldK` (`fold_dag.go:36`) → incremental cannot regress.
- **The serial loop:** `fold_pool.go:279-289` — `for _, top := range rootTask.children { c, d, err :=
  foldFreshAccountSubtreeCellForkJoin(ctx, ff, top.node, ...) }`. Each call creates its own `foldCtx`
  (`newFoldCtx`) and returns a `mountWallCell` + deferred; they are already independent. Only the loop
  serializes them.
- **The fork mechanism to reuse:** `forkFolder.fold` (`truthtree_fold.go:352`) — `jobs` loop with
  `ff.sem.TryAcquire(1)` → `errgroup` fork, inline `foldNode` fallback under saturation. `TryAcquire`
  (non-blocking) is load-bearing for deadlock freedom: a forked parent holds its slot while awaiting its
  own forked children, so a blocking acquire would wedge. Any new top-level dispatch **must** preserve
  the TryAcquire-then-inline-fallback invariant.
- **Threshold conflation (the root cause):** `forkFolder.k = foldK(subtreeCount, numWorkers)` serves as
  *both* the serial-leaf boundary (`pn.subtreeCount <= k → foldNode`) *and* the fork gate
  (`child.subtreeCount > k → fork`). At the root `k ≈ 58k` (1.05M/18), so every 12k top nibble is
  `<= k` and folds as a serial leaf. The load-balance grain (`total/numWorkers`, "one chunk per worker")
  is the wrong gate for the top-level fan-out.

## Development Approach

- **TDD / red-first** (repo CLAUDE.md). Optimization order: **correctness > fail-closed > performance >
  simplicity**. No `t.Skip`, ever.
- **Measure-first**: Task 1 ports the measurement tooling and records the baselines this plan must beat
  *before* any production change. Perf claims are backed by the ported benches, not estimates.
- **Parallel-only**: the golden serial `HexPatriciaHashed` (`hex_patricia_hashed.go`) must stay
  byte-identical and untouched — verify with `git diff` (empty for that file). The incremental/frontier
  parallel path must stay unregressed (measured gate), guaranteed structurally by keeping the change
  inside `forkFolder`.
- Complete each task fully (tests green, `make lint` clean, `-race` where concurrency changes) before the
  next. Update this file when scope shifts.

## Testing Strategy

- **Correctness (green throughout):** root byte-identity vs the pre-change fork and vs the serial oracle,
  on multi-nibble fresh corpora incl. whales and the depth-64 seam. These are refactor-safety nets.
- **Behavioral red-first:** a top-level peak-concurrency counter — with a multi-nibble fresh corpus and
  ≥2 `foldSem` slots, the number of top-nibble subtrees folding concurrently must exceed 1. Fails today
  (serial loop → peak 1), passes after. This is the driver, not a perf assertion.
- **Deadlock-freedom under saturation:** run the new dispatch with `foldSem` size 1 and 2 over a
  fork-heavy corpus; must complete (inline fallback), `-race` clean.
- **Perf gates (acceptance, Task 5):** ported `Benchmark_FreshNibTimeline` (folds now overlap),
  `Benchmark_ProcessCPUUtil` (avg-cores up), `Benchmark_SerialVsForkCPU`, and existing
  `Benchmark_FreshBuildFork` (fresh improved, incremental flat, memory bounded).

## Solution Overview

1. **Route the top-level 16-nibble dispatch through the shared fork mechanism** — replace the serial loop
   in `dispatchWholeFresh` with a `TryAcquire`+`errgroup` dispatch over `rootTask.children`, sharing
   `foldSem`: fork a nibble's `foldSubtreeCellForkJoin` onto its own goroutine when a slot is free, fold
   inline when saturated. Each nibble already produces an independent `mountWallCell`; collect them into
   `cells`/`present`, then `stitchAndFoldRoot` as today. This alone delivers the measured win: the 13
   small nibbles fold concurrently (each serial-within, parallel-across → ~7 ms wall instead of 91 ms),
   the whale forks internally and overlaps them.
2. **Unify the fork-dispatch pattern** — the top-level dispatch and `forkFolder.fold`'s `jobs` loop are
   the same "for each independent child: fork if a slot is free, else inline" shape. Extract a shared
   helper so the top level is not a special case (the "remove duplicates / generalize" goal). Keep the
   deadlock-safe TryAcquire-inline-fallback contract in the one place.
3. **(Measure-gated) internal threshold decouple** — evaluate whether also gating `forkFolder.fold`'s
   interior fork decision on slot-availability + a fixed goroutine-worthiness floor (`foldKMin`), rather
   than `child.subtreeCount > k`, buys additional parallelism beyond the top-level fix. Adopt only if it
   improves fresh without regressing memory or incremental. Not assumed — measured.

## Technical Details

- **Concurrency bound:** total in-flight fold goroutines stay capped by `foldSem` (numWorkers slots).
  The top-level dispatch acquires from the *same* semaphore as interior forks, so routing 16 nibbles
  through it does not oversubscribe — it fills idle slots the serial loop left empty.
- **Deferred collection:** each forked nibble owns its `foldCtx.deferred`; results merge after join
  exactly as the serial loop appended them. Order of `append` into the combined slice is not
  root-affecting (branch records are keyed by prefix), but preserve deterministic collection for the
  duplicate-prefix flush path — verify against the existing deferred-vs-eager differential tests.
- **Fail-closed:** any nibble error recycles all collected deferred (`putDeferredUpdates`) and returns
  nothing, matching the current loop's error arm.
- **Seam/fallback unchanged:** the `allTopNibblesPureBranch` / `accountPlaneForkable` / `hasEmpty`
  guards that route non-forkable corpora to the frontier fold stay exactly as-is; only the forkable
  branch's dispatch changes.

## What Goes Where

- **Implementation Steps** (`[ ]`): benches, the dispatch rewrite, the shared helper, threshold sweep,
  acceptance, docs — all in this repo.
- **Post-Completion** (no checkbox): none external; this is a self-contained parallel-engine change.

## Implementation Steps

### Task 1: Port measurement tooling and record baselines (no production change)

**Files:**
- Create: `execution/commitment/cpu_util_bench_test.go` (port from branch `awskii/cpu-util-bench`)
- Modify: `execution/commitment/fold_pool.go` (add the `onNibFold` measurement hook near the whole-fresh
  counters + the two-line call in the `dispatchWholeFresh` loop)

- [ ] port `Benchmark_ProcessCPUUtil`, `Benchmark_SerialVsForkCPU`, `Benchmark_FreshNibTimeline`, the
      `runProcessCPUUtilBench`/`runSerialCPUUtilBench`/`rusageCPU` helpers, and the `onNibFold` hook
      (`git show awskii/cpu-util-bench:execution/commitment/cpu_util_bench_test.go` and the `fold_pool.go`
      hook diff)
- [ ] run and record in this plan the numbers to beat: `Benchmark_SerialVsForkCPU` (serial ~1160 ms /
      1.09 cores; fork w18 ~255 ms / 5.3 cores), `Benchmark_FreshNibTimeline` (sum≈190 ms, ratio≈0.74,
      whale nib ~70 ms, 13 small ~7 ms), `Benchmark_FreshBuildFork` fresh + incremental-whale120k
      (~24.5 ms) + B/op
- [ ] confirm compile + benches run; no production behavior change (hook is nil in prod)
- [ ] run tests - package green before Task 2

### Task 2: Red — top-level peak-concurrency behavioral test

**Files:**
- Modify: `execution/commitment/fold_pool.go` (test-readable peak-concurrency counter for the top-nibble
  dispatch, gated like the existing whole-fresh atomics)
- Create/Modify: `execution/commitment/fresh_build_fork_test.go` (the red test)

- [ ] add a test-observable counter recording the peak number of top-nibble subtrees folding
      concurrently in `dispatchWholeFresh`
- [ ] write a failing test: a fresh corpus with ≥4 populated top nibbles and `foldSem` ≥ 2 must reach
      peak top-level concurrency > 1 (fails today: serial loop → peak 1). Confirm it fails for the right
      reason (serial dispatch), not a setup error
- [ ] write the root-parity green test alongside: same corpus, root byte-identical to the current fork
      and to the serial oracle (refactor safety net)
- [ ] run tests - the concurrency test is RED, the parity test is GREEN

### Task 3: Green — route the top-nibble dispatch through the shared fork mechanism

**Files:**
- Modify: `execution/commitment/fold_pool.go` (`dispatchWholeFresh` — replace the serial loop with the
  concurrent `foldSem`-bounded dispatch)
- Modify: `execution/commitment/truthtree_fold.go` (if extracting the shared fork-dispatch helper)

- [ ] replace `dispatchWholeFresh`'s serial `for` loop with a `TryAcquire`+`errgroup` dispatch over
      `rootTask.children` sharing `fp.foldSem`: fork `foldFreshAccountSubtreeCellForkJoin` when a slot is
      free, fold inline under saturation; collect `cells`/`present`/`deferred` as before; preserve the
      fail-closed error arm (`putDeferredUpdates`)
- [ ] preserve the deadlock-safe TryAcquire-then-inline-fallback contract (no blocking acquire)
- [ ] make the Task 2 concurrency test pass; keep the parity test green
- [ ] update `onNibFold` capture if the loop shape changed so `Benchmark_FreshNibTimeline` still reports
      per-nibble folds (now overlapping)
- [ ] write a deadlock-freedom test: same dispatch with `foldSem` size 1 and 2 completes and is `-race`
      clean
- [ ] run tests + `-race` on the concurrency-touched tests - all green before Task 4

### Task 4: Unify the dispatch pattern and sweep the threshold

**Files:**
- Modify: `execution/commitment/truthtree_fold.go` (extract the shared fork-dispatch helper used by both
  `forkFolder.fold` and the top-level dispatch)
- Modify: `execution/commitment/fold_pool.go` / `execution/commitment/fold_dag.go` (only if the measured
  internal-threshold decouple is adopted — a `forkFolder` fork-floor field, not a `foldK` change)

- [ ] extract the "for each independent child: fork if slot free, else inline" pattern into one helper;
      route both `forkFolder.fold` and the top-level dispatch through it (remove the duplication); keep
      the deadlock contract in the single helper
- [ ] measure whether gating the interior fork on slot-availability + a fixed `foldKMin` floor (instead
      of `child.subtreeCount > k`) improves fresh; sweep the floor. **Adopt only if** fresh improves AND
      incremental (`Benchmark_FreshBuildFork/incremental-whale120k`) stays ≤ baseline AND B/op does not
      balloon. Record the decision + numbers in this plan
- [ ] if not adopted, `log`/note it explicitly (no silent scope drop) and keep only the top-level fix
- [ ] write tests for the shared helper (fork-under-slots, inline-under-saturation, error propagation)
- [ ] run tests + `-race` - green before Task 5

### Task 5: Verify acceptance criteria

- [ ] `Benchmark_SerialVsForkCPU` / `Benchmark_ProcessCPUUtil`: fresh w18 improved toward ~140–160 ms,
      avg-cores up from 5.3; record the numbers
- [ ] `Benchmark_FreshNibTimeline`: top-nibble folds overlap (no longer sum to ~74% of wall)
- [ ] `Benchmark_FreshBuildFork`: fresh improved, `incremental-whale120k` unregressed (≤ ~24.5 ms),
      B/op bounded (note any regression vs the 25% fresh-fork memory win)
- [ ] root byte-identity across the parity + deferred-vs-eager differential suites; whale + depth-64 seam
      corpora covered
- [ ] serial engine untouched: `git diff 2ae26b58d5 -- execution/commitment/hex_patricia_hashed.go` empty
- [ ] full `execution/commitment` suite green, `-race` green, `make lint` clean (run repeatedly), `make
      test-short` exit 0

### Task 6: [Final] Documentation and cleanup

- [ ] update `execution/commitment/doc.go` (or the fresh-fork design doc) to describe the unified
      slot-bounded top-level dispatch replacing the serial loop
- [ ] decide the fate of the `onNibFold` hook + measurement benches: keep the benches as regression gates;
      keep or remove the hook per whether the nib-timeline bench still needs it
- [ ] update CLAUDE.md / memory only if a new invariant emerged
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*None external.* Parallel-engine-only change; no consuming-project or deployment updates. The fork route
is not the default engine, so no release-gating beyond the parallel-commitment flag.
