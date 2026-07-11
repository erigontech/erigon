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
- **The serial loop:** `fold_pool.go:294-304` (forkFolder constructed at `:287`) — `for _, top := range
  rootTask.children { c, d, err := foldFreshAccountSubtreeCellForkJoin(ctx, ff, top.node, ...) }`. Each
  call creates its **own** `foldCtx` (`newFoldCtx`, `fold_pool.go:365`) and returns an independent
  `mountWallCell` + deferred. Only the loop serializes them. Note this per-nibble-own-`foldCtx` ownership
  differs from `forkFolder.fold`'s interior inline arm, which reuses the *parent* `fc` and merges child
  hashes into one branch row (`truthtree_fold.go:457`) — the two dispatch loops are **not** the same
  shape (see Task 4).
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
- **Behavioral red-first (deterministic, not timing-based):** a rendezvous barrier at each top-nibble
  fold's execution start that releases only after ≥2 entrants arrive, with a timeout that **fails**.
  Serial loop → 1 entrant → deterministic timeout (RED); concurrent → 2 arrive → release (GREEN). Works
  at `foldSem`=1 (one forks, one inline in the dispatcher = 2 entrants). Also asserts
  `wholeFreshForkJoins` fired so a frontier-fallback corpus can't produce a false red. This is the
  driver, not a perf assertion — and it must never `t.Skip`.
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
- **Deferred collection (no shared append):** each forked nibble writes into its **own** result slot
  (per-nibble `cells[nib]`/`present[nib]`/a `results[i] []*DeferredBranchUpdate`); the combined `deferred`
  slice is assembled **after `g.Wait()`**, never appended from goroutines (a shared `append` races —
  `-race` would catch it, but do not write it). Order is not root-affecting here and provably so: the
  route folds only the complementary set — `reuseSchedulerCells` (`streaming_commitment.go:267-288`)
  **prunes reused nibbles from `rootTask.children`** — so the top nibbles are disjoint prefixes among
  themselves and disjoint from reused, `reusedDeferred` stays appended **last**, and the combined slice
  has **no duplicate prefixes** (`hasDuplicatePrefix` false → order-independent apply). This is why the
  reorder cannot change bytes; it is not a "preserve ordering for the duplicate-prefix path" concern
  (there are no duplicate prefixes on this route).
- **Fail-closed under concurrency:** the error arm must recycle deferred from **all** nibbles — completed
  forked `results[i]`, the inline nibble, and `reusedDeferred` — matching `forkFolder.fold`'s `results[i]`
  recycling (`truthtree_fold.go:464-473`). A serial-style single `putDeferredUpdates(deferred)` leaks
  in-flight forked results.
- **Inline arm holds no slot:** the saturation fallback (fold a nibble inline in the dispatch goroutine)
  must hold **no** `foldSem` slot while folding, mirroring `forkFolder.fold`'s inline arm
  (`truthtree_fold.go:457`). This no-slot property is the load-bearing bit of the deadlock-free contract:
  slotted goroutines only ever await children that hold their own slot or fold inline, so concurrency
  stays `numWorkers` slotted + 1 un-slotted dispatcher — identical to today.
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

- [x] port `Benchmark_ProcessCPUUtil`, `Benchmark_SerialVsForkCPU`, `Benchmark_FreshNibTimeline`, the
      `runProcessCPUUtilBench`/`runSerialCPUUtilBench`/`rusageCPU` helpers, and the `onNibFold` hook
      (`git show awskii/cpu-util-bench:execution/commitment/cpu_util_bench_test.go` and the `fold_pool.go`
      hook diff)
- [x] run and record in this plan the numbers to beat: `Benchmark_SerialVsForkCPU` (serial ~1160 ms /
      1.09 cores; fork w18 ~255 ms / 5.3 cores), `Benchmark_FreshNibTimeline` (sum≈190 ms, ratio≈0.74,
      whale nib ~70 ms, 13 small ~7 ms), `Benchmark_FreshBuildFork` fresh + incremental-whale120k
      (~24.5 ms) + B/op
- [x] confirm compile + benches run; no production behavior change (hook is nil in prod)
- [x] run tests - package green before Task 2

**Baselines recorded (M5 Max 18c, darwin/arm64, `-benchtime 3x`, sequential runs, 2026-07-11):**

| Bench | wall ms/op | avg-cores | B/op | allocs/op |
|---|---|---|---|---|
| `SerialVsForkCPU/1MWhales/serial-direct` | 1167 | 1.06 | 389,152,746 | 7,788,249 |
| `SerialVsForkCPU/1MWhales/fork-w18` | 264.0 | 5.22 | 384,631,192 | 7,797,520 |
| `FreshBuildFork/1MWhales/flag-off/w18` | 693.6 | — | 511,144,674 | 6,845,889 |
| `FreshBuildFork/1MWhales/flag-on/w18` | 273.0 | — | 378,019,794 | 7,747,429 |
| `FreshBuildFork/incremental-whale120k/flag-off` | 24.3 | — | 105,262,146 | 933,415 |
| `FreshBuildFork/incremental-whale120k/flag-on` | 25.0 | — | 102,461,549 | 942,212 |

`FreshNibTimeline` (single fresh 1M-whale Process, w18): Process wall **271 ms**, sum(nib folds)
**188 ms**, **ratio 0.69**, 16 nibs — whale nib=a (762k keys) **67 ms**, nib=4 (162k) 20 ms,
nib=2 (17k) 11 ms, remaining 13 small (~12k each) **~7 ms apiece ≈ 91 ms serial**. Confirms the
plan's premise: top-nibble folds are serial and sum to ~70 % of Process wall.

**Numbers to beat (Task 5/6 gates):** fresh fork w18 wall 264–273 ms → target ~140–160 ms;
avg-cores 5.22 → up; nib-fold sum/wall ratio 0.69 → folds overlap (ratio well below wall);
incremental-whale120k flag-on ≤ ~25 ms (observation, machine-dependent); fresh flag-on B/op
~378 MB bounded (vs 511 MB flag-off).

### Task 2: Red — deterministic top-level concurrency test

**Files:**
- Modify: `execution/commitment/fold_pool.go` (a test-only rendezvous hook straddling each top-nibble
  fold's *execution*, gated like the existing whole-fresh atomics; nil in prod)
- Create/Modify: `execution/commitment/fresh_build_fork_test.go` (the red test)

- [x] **deterministic barrier, NOT a peak counter** (a timing-dependent peak counter can flake, and the
      no-`t.Skip` rule would then wedge the executor). Add a test-only rendezvous the fold entry calls at
      the *start* of each top-nibble execution (inside both the forked goroutine and the inline arm — not
      at enqueue): the test's rendezvous releases only after ≥2 entrants arrive, with a timeout that
      **fails** (never skips). Serial dispatch → only 1 entrant ever → deterministic timeout = RED;
      concurrent → 2 arrive, release = GREEN. Works even at `foldSem` size 1 (one nibble forks, one folds
      inline in the dispatcher = 2 concurrent entrants)
      *(hook `onNibFoldStart` fires inside `foldFreshAccountSubtreeCellForkJoin` — the per-nibble
      execution entry — so any future dispatch arm calls it from its executing goroutine)*
- [x] pin red-for-the-right-reason: the test must also assert `wholeFreshForkJoins` incremented (matches
      `fresh_build_fork_test.go:172`), so a corpus that silently slips to the frontier fallback (fails
      `allTopNibblesPureBranch`/`accountPlaneForkable`/`hasEmpty`) is a test-setup failure, not a false red
      *(also guards `arrived >= 2` so a <2-nibble corpus is a setup failure too)*
- [x] write the root-parity green test alongside: same corpus, root **and the persisted branch-record
      set** byte-identical to the current fork and to the serial oracle (refactor safety net)
      *(`TestFreshBuild_TopNibbleDispatchParity`: fork-on + fork-off vs sequential, root +
      `requireBranchParity`, all parityModes × w1/w4)*
- [x] run tests - the barrier test is RED (times out under the serial loop), the parity test is GREEN
      *(RED on the overlap assertion after the 5s strand — setup guards passed, so red for the right
      reason; parity green; rest of package green via `-skip`; `make lint` clean ×2)*

### Task 3: Green — route the top-nibble dispatch through the shared fork mechanism

**Files:**
- Modify: `execution/commitment/fold_pool.go` (`dispatchWholeFresh` — replace the serial loop with the
  concurrent `foldSem`-bounded dispatch)
- Modify: `execution/commitment/truthtree_fold.go` (if extracting the shared fork-dispatch helper)

- [ ] replace `dispatchWholeFresh`'s serial `for` loop with a `TryAcquire`+`errgroup` dispatch over
      `rootTask.children` sharing `fp.foldSem`: fork `foldFreshAccountSubtreeCellForkJoin` when a slot is
      free, fold inline (holding **no** slot) under saturation
- [ ] **no shared append from goroutines**: each nibble writes its own `cells[nib]`/`present[nib]` and a
      per-index `results[i]` deferred slot; assemble the combined `deferred` (then `reusedDeferred` last)
      **after `g.Wait()`**
- [ ] fail-closed error arm recycles **all** nibbles' deferred — forked `results[i]`, the inline nibble,
      and `reusedDeferred` — per `forkFolder.fold`'s pattern (`truthtree_fold.go:464-473`); not a single
      serial-style `putDeferredUpdates(deferred)`
- [ ] preserve the deadlock-safe TryAcquire-then-inline-fallback contract (no blocking acquire; inline
      holds no slot)
- [ ] make the Task 2 concurrency test pass; keep the parity test green
- [ ] update `onNibFold` capture if the loop shape changed so `Benchmark_FreshNibTimeline` still reports
      per-nibble folds (now overlapping)
- [ ] write a deadlock-freedom test: same dispatch with `foldSem` size 1 and 2 completes and is `-race`
      clean
- [ ] run tests + `-race` on the concurrency-touched tests - all green before Task 4

### Task 4: Sweep the interior fork threshold (measure-gated, byte-identical)

**Files:**
- Modify: `execution/commitment/fold_pool.go` (a `forkFolder` fork-floor — **not** a `foldK` change, so
  the frontier path is untouched)

- [ ] measure whether gating the interior fork on slot-availability + a fixed `foldKMin` floor (instead
      of `child.subtreeCount > k`) buys parallelism *beyond* Task 3's top-level fix; sweep the floor.
      Fork-vs-inline is byte-identical, so this only moves concurrency/`B/op`, never bytes
- [ ] **adopt only if** fresh improves AND `Benchmark_FreshBuildFork/incremental-whale120k` stays ≤
      baseline AND B/op does not balloon; record the decision + numbers here. The entire *measured* win
      is already in Task 3 — treat this as opportunistic
- [ ] if not adopted, note it explicitly here (no silent scope drop) and keep only the top-level fix
- [ ] run tests + `-race` - green before Task 5

### Task 5: (Optional) Unify the fork-dispatch pattern — cleanup only

**Files:**
- Modify: `execution/commitment/truthtree_fold.go`, `execution/commitment/fold_pool.go`

- [ ] **caveat first:** the two loops are NOT the same shape — `forkFolder.fold`'s inline arm reuses the
      *parent* `fc` and merges child hashes into one branch row; the top level gives each nibble its own
      `newFoldCtx` producing independent mount-wall cells. A shared helper that imposes the interior
      "inline reuses parent fc" contract on the top level would be **wrong** (interleaves independent
      nibbles' deferred/scratch → byte corruption). The measured win is entirely in Task 3
- [ ] extract a shared helper **only** if it preserves per-nibble `foldCtx` ownership at the top level;
      it must not touch the byte-identity-critical path merely to de-duplicate. If the abstraction can't
      stay clean, **skip this task** and note why (duplication is acceptable here per repo policy)
- [ ] if done: write helper tests (fork-under-slots, inline-under-saturation, error propagation);
      root + branch-record byte-identity unchanged; `-race` green

### Task 6: Verify acceptance criteria

- [ ] **byte-identity (primary gate):** root **and the persisted branch-record set** identical to the
      pre-change fork and the serial oracle — assert the deferred/stored-branch set directly, not just the
      32-byte root (the existing test compares roots only; extend it), across the parity +
      deferred-vs-eager differential suites; whale + depth-64 seam corpora covered
- [ ] **incremental non-regression via diff-scope, not absolute ms:** confirm no incremental/frontier-path
      files changed — `git diff 2ae26b58d5 --stat` touches only `forkFolder`/`dispatchWholeFresh` (+ tests
      + `foldKMin` floor if Task 4 adopted), never `deriveFoldFrontier`/`dispatchFoldTasks`/`foldK`. Run
      `Benchmark_FreshBuildFork/incremental-whale120k` and record it as an observation (machine-dependent),
      not a hard-fail number
- [ ] serial engine untouched: `git diff 2ae26b58d5 -- execution/commitment/hex_patricia_hashed.go` empty
- [ ] **perf observations (recorded, not hard-fail):** `Benchmark_SerialVsForkCPU`/`ProcessCPUUtil` fresh
      w18 improved (target ~140–160 ms), avg-cores up from 5.3; `Benchmark_FreshNibTimeline` top-nibble
      folds overlap (no longer sum to ~74% of wall); fresh B/op bounded (note any regression vs the 25%
      fresh-fork memory win)
- [ ] deadlock-freedom: dispatch at `foldSem` size 1 and 2 over a fork-heavy corpus completes, `-race` green
- [ ] full `execution/commitment` suite green, `-race` green, `make lint` clean (run repeatedly), `make
      test-short` exit 0

### Task 7: [Final] Documentation and cleanup

- [ ] update `execution/commitment/doc.go` (or the fresh-fork design doc) to describe the slot-bounded
      top-level dispatch replacing the serial loop
- [ ] decide the fate of the `onNibFold` hook + measurement benches: keep the benches as regression gates;
      keep or remove the hook per whether the nib-timeline bench still needs it
- [ ] update CLAUDE.md / memory only if a new invariant emerged
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*None external.* Parallel-engine-only change; no consuming-project or deployment updates. The fork route
is not the default engine, so no release-gating beyond the parallel-commitment flag.
