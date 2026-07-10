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

- [ ] add a whole-fresh detector *inside* the parallel/streaming engine (empty on-disk state: unseedable
      root + no seedable prefix seen during derivation, or caller hint). Non-empty state → frontier path
      unchanged. **No CLI flag** — the existing parallel/streaming mode selection is the only user flag.
- [ ] add a **test-only package toggle** (default: fork on) so benches can measure the frontier-serial
      baseline; dev-only, never wired to a CLI flag.
- [ ] wire a new entry the whole-fresh case routes to (stub → falls back to frontier for now, tree stays green).
- [ ] test: a fresh corpus hits the new entry (assert via counter, red until Task 2); an incremental corpus
      does NOT (guard test, green now); toggle-off == frontier.
- [ ] `make test-short`. Before Task 2.

## Milestone 2 — Account-plane fork-join over `foldNode`

### Task 2: Generalize `forkFolder` from storage-only to the account plane (incl. depth-64 seam)

**Files:**
- Modify: `execution/commitment/truthtree_fold.go`, `execution/commitment/fold_pool.go`

- [ ] lift the storage-plane-only restriction (`truthtree_fold.go:364`): `forkFolder`/`foldFreshForkJoin`
      forks account-plane conflict gates too, folding account cells (with their storage roots) via
      `foldNode`, recursing across the depth-64 seam into each account's (fresh) storage.
- [ ] the whole-fresh entry (Task 1) folds the entire account plane per-prefix, empty-wall, buffer-per-lineage,
      down to the `foldK` grain; eager fold via the existing pending counters.
- [ ] parity: `TestFreshBuild_AccountPlane` (whales + tail + seam), flag-on == flag-off == sequential,
      N≥3, AND the fork path fired (counter). Guard test (incremental → frontier) stays green.
- [ ] alloc: parallel arm within ceiling; serial grain ≤ existing ceiling.
- [ ] full parity suite. Before Task 3.

### Task 3: Seam correctness on the account plane

**Files:**
- Modify: `execution/commitment/truthtree_fold.go`

- [ ] verify the account→storage-root wiring holds under the account-plane fork (each account's fresh
      storage folds via the same recursion; the account cell gets its storage root before its branch keccak).
      Reuse `foldNode`'s existing seam handling — do not degenerate the seam to a plain nibble fork.
- [ ] parity tests: touched account with fresh storage across the seam, single-first-nibble account,
      accounts sharing top nibbles; == sequential.
- [ ] `make test-short` + full parity. Before Task 4.

## Milestone 3 — Performance (production-representative)

### Task 4: 1MWhales + incremental benches with carried updates

**Files:**
- Modify: `execution/commitment/parallel_streaming_bench_test.go`

- [ ] switch the whale + incremental benches to carried updates (`TouchPlainKeyDirect`); keep a
      `WrapKeyUpdates` variant only for the parity harness.
- [ ] measure flag-on vs flag-off (frontier) vs main on `1MWhales` (fresh) and the incremental delta.
- [ ] acceptance: flag-on `1MWhales` ∥ approaches/beats main (900 → ~200 ms); flag-on incremental ∥
      unchanged vs frontier (~3.83 ms — the seedable path is untouched).
- [ ] record numbers in the plan. Before Task 5.

## Milestone 4 — Verify + document

### Task 5: Verify acceptance criteria

- [ ] flag-on == flag-off == sequential: root + branch parity, N≥3, fresh corpora + seam; guard tests
      confirm incremental routes to frontier unchanged.
- [ ] `1MWhales` ∥ ≤ main; incremental ∥ == frontier; alloc ceilings held.
- [ ] no changes leaked into `seedMerge` / mount+replay / demotion / `foldFreshStorage` fallback / streaming
      reuse (`git diff` those files shows only additive wiring, if any).
- [ ] `make lint && make test-short` clean.

### Task 6: [Final] Docs; flag-default decision

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
