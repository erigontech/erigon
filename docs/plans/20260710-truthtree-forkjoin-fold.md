# TruthTree fresh-whale fork-join: swap the fold body to `foldNode`, restore parallelism

> **Status (2026-07-11):** Tasks 1–3 done @ `d5757bfe66`. The Context section below predates three
> plans that have since landed on this branch — `completed/20260710-conflict-gate-fold.md` (whole-fresh
> account-plane fork-join: `forkFolder` is now production via `dispatchWholeFresh`, and
> `foldFreshForkJoin` from Task 3 is currently test-only), `completed/20260710-commitment-write-opt.md`
> (`getDeferredUpdate` now takes the `BranchEncoder` arena — Task 4's deletion of
> `foldFreshStorageRootDeferred` must account for the changed signature), and the flag-on routing now
> lives in `foldFreshWhaleStorage` (`fold_pool.go`), not at the line pinned below. Re-verify every
> anchor in Context before executing Tasks 4–6. See also item 4 in
> `20260711-deferred-write-api-simplification-followup.md` (thread `accountKeyLen`/`TrieConfig` into
> the same constructors Task 4 touches).

## Overview

The `--experimental.truthtree-fold` flag regresses the fresh-whale fold **4.56×** (623 vs 137 ms/op,
whole-`Process`, 750k slots). Root cause is pinned at `fold_pool.go:406`: a flag-on fresh whale
(≥2 first nibbles) routes to `foldFreshStorageRootDeferred` — a **serial** `foldNode` recursion —
dropping the per-first-nibble parallel fan-out that `foldFreshStorage` runs under `foldSem`. The
buffer-reuse win (44 MB isolated) is real but swamped by the lost 8.5× parallelism.

**TruthTree *is* the `foldTask` DAG** — the DAG is the divergence structure the fold walks, not a thing
to remove. The only improvement is the fold **body**: `foldNode` (buffer-reuse direct fold) replaces
mount+replay, removing the replay allocations, task by task, inside the existing DAG.

The regression fix restores the fresh-whale task's fan-out, but as **recursive per-prefix fork-join**,
not a per-nibble split. The fan-out unit is the **split point** (`subtreeCount > K`, `foldK`-granular):
`foldNode` forks children onto `foldSem` at *every* divergence and recurses, serial below K — the same
split-point decomposition the DAG already applies to **seedable** subtrees (`derive`/`addChildren`),
now applied at fold time to the fresh subtree. A fixed 16-way per-first-nibble split (what
`foldFreshStorage` does today) is explicitly rejected: per-nibble on both account and storage planes is
exactly the current-parallel "1 job per nibble" fan-out this work exists to move past. This must be the
leaf's fold body, not a DAG derive-time split, because freshness is only provable at fold time — the
derive-time `Branch` probe cannot distinguish "fresh" from "single on-disk leaf that becomes a branch"
(the demotion case), so the DAG correctly demotes the fresh whale to a leaf and the per-prefix fan-out
lives inside it.

**No DAG removal. No streaming change.** `deriveFoldFrontier`, `foldTask`, the seedable-demotion rule,
the sequential root finale, the storage-only seam, and streaming's `reuseSchedulerCells` are all
untouched. Delete **only** the serial `foldFreshStorageRootDeferred`.

Endpoint: flag-on fresh whale folds via a parallel `foldNode` fan-out; flag-on ≤ flag-off on the whale;
roots + stored branches byte-match sequential HexPatricia.

## Context (verified against branch `awskii/truthtree-fold` @ 48ab330d23, package `execution/commitment`)

- **Fold body:** `foldCtx.foldNode(node, prefix, branchDepth) (common.Hash, error)` — `truthtree_fold.go:154`.
  Buffer-reuse (one `fc.cell`, one `fc.buf` per `foldCtx`); two-phase (record child kinds, then re-derive
  each cell from `(node, childHash)` + 17-slot keccak). NOT cell-per-node.
- **The regression (delete this):** `foldFreshStorageRootDeferred(node, accPrefix)` — `truthtree_fold.go:364`.
  Serial `fc.foldNode(node, accPrefix, 64)` + per-branch-prefix `DeferredBranchUpdate` emission.
- **Flag-off path + primitives to reuse (NOT the fan-out shape to copy):** `foldPool.foldFreshStorage(ctx, node, accPrefix)`
  — `fold_pool.go:428`. It does a flat per-first-nibble errgroup split under `foldSem` (GOMAXPROCS) with a
  mount+replay body. Reuse its `foldSem` bound, empty-wall base (`seedBaseAtPrefix`, `errStorageBaseNotBranch`
  expected), and aggregation primitive — but **not** its per-nibble split shape. The new path forks
  recursively at split points (`subtreeCount > K`), not at top nibbles.
- **The split-point recursion to mirror instead:** `deriveFoldFrontier`→`foldDAGBuilder.derive`/`addChildren`
  (`fold_pool.go:67`, `fold_dag.go`) — how the DAG decomposes a *seedable* subtree per-prefix, `foldK`-granular.
  The fresh fork-join applies the same `subtreeCount > K` fork decision inside `foldNode`.
- **The routing to fix:** `fold_pool.go:406` — `if fp.truthtreeFold && bits.OnesCount16(node.bitmap) >= 2 { foldFreshStorageRootDeferred } else { foldFreshStorage }`.
- **The arity-1 guard (preserve + test, do NOT touch `foldNode`):** the `>= 2` gate keeps single-first-nibble
  whales on `foldFreshStorage`, whose `aggregateMountedStorageRoot` routes the lone survivor to
  `storageRootFromSingleChild` (`streaming_deep_fold.go:166`) — correct extension root + delete record.
  `foldNode` (17-slot branch keccak) is wrong for arity-1 and must never be handed one; the gate ensures it.
- **Correctness oracle:** `runEngineBatchesParity` (`parallel_testkit_test.go:319`) — asserts **root** parity
  AND calls `requireBranchParity` (`:374`, stored-branch bytes), N≥3 batches. Use this (not `runEngineBatches`,
  which returns roots without asserting).
- **Gates:** `TestTruthtreeFold_AllocCeiling` (44 MB serial ceiling; `truthtree_fold_test.go:516`),
  `Benchmark_TruthtreeFold_FreshWhaleAlloc` (`:509`), `Benchmark_TruthtreeFold_Gate` (whole-`Process`
  flag-on vs flag-off; `parallel_streaming_bench_test.go:500`), `Benchmark_DeepStorageWhale`
  (Sequential 815 ms / ModeParallel 145 ms base, 750k).
- **UNTOUCHED (explicitly out of scope):** `deriveFoldFrontier` (`fold_pool.go:67`), `dispatchFrontier`,
  `foldTask`, `foldDAGBuilder`, `freshWhaleCandidate`, `foldStorageSeam`, the demote-to-ancestor-replay
  path, the sequential root finale, `streaming_commitment.go` / `reuseSchedulerCells`.

## Development Approach

- TDD **red → green → refactor** per repo CLAUDE.md. Order: **correctness > fail-closed > perf > simplicity.**
- Load-bearing claim (M1 proves it, zero parallelism risk): a **provably-fresh** whale's storage folds via
  `foldNode` against an empty-wall base and byte-matches sequential at every interior prefix. This holds
  *because* freshness means no on-disk siblings anywhere below — `foldNode`'s diskless recursion equals the
  sequential fold. (Not a general `foldNode` property: an unseedable-but-not-fresh node would need the DAG's
  demotion — which is why this plan touches only the provably-fresh path and leaves the DAG intact.)
- Every task ends green before the next. `runEngineBatchesParity` stays green throughout.
- **Buffer-per-lineage:** each forked goroutine owns its own `foldCtx` (own scratch + output buffer). The
  44 MB serial ceiling holds per lineage; the parallel arm lands ~49 MB (proto `V3LeanParallel`), never 575 MB.
- **Do not flip the default flag** until parity is green AND flag-on ≤ flag-off on the whale.
- KISS: mirror `foldFreshStorage`'s existing errgroup/`foldSem` structure; do not invent task machinery.

## Testing Strategy

- Parity: `runEngineBatchesParity` (root + stored branches) for every fold shape touched.
- Alloc: serial arm ≤ 44 MB; add a parallel-arm assertion (~49 MB, never 575 MB).
- Perf: `Benchmark_TruthtreeFold_Gate` + isolated fresh-whale bench; success = flag-on ≤ flag-off, 750k whale.
- `make lint && make test-short` in `execution/commitment` after each task; full parity suite before Milestone 2.

## What Goes Where

- **Implementation Steps** (`[ ]`): the parallel `foldNode` fresh path, routing, the serial-path deletion, tests, benches.
- **Post-Completion** (no checkboxes): flip the default flag; whole-node validation; optionally retire
  `foldFreshStorage` once flag-on is validated on a real node.

## Milestone 1 — Fresh-fold parity, serial (de-risk the empty-wall claim + the arity gate)

### Task 1: Interior-prefix parity for the serial `foldNode` fresh fold + arity-gate routing

**Files:**
- Modify: `execution/commitment/truthtree_fold_test.go`

- [x] `TestTruthtreeFold_FreshInteriorParity`: a **multi-level** fresh whale (storage branches nested ≥2
      deep below the depth-64 root) folded via the serial `foldFreshStorageRootDeferred` path; assert
      `runEngineBatchesParity` (root + every stored branch) == sequential, N≥3 batches. No `foldK` hook —
      `foldNode` recurses every interior branch and emits a `DeferredBranchUpdate` at each, so a multi-level
      whale already exercises interior prefixes.
- [x] fresh **non-whale** case (small fresh storage, still storage-less on disk) — the shape the whale test
      never exercises. (`TestTruthtreeFold_FreshNonWhaleParity`: small-storage account-plane subtrees fold
      through the direct account-plane recursion, root + branch parity, N≥3.)
- [x] **arity-gate** test: a single-first-nibble fresh whale (`popcount==1`) must route to `foldFreshStorage`
      /`storageRootFromSingleChild` (extension root + delete record), NOT `foldNode`; assert the root and the
      absence of a bogus branch record at the depth-64 prefix, == sequential. (`TestTruthtreeFold_ArityGateSingleNibble`;
      full stored-branch parity is intentionally not asserted — a single-survivor collapse has a benign
      resolved-root-vs-stored-leaf encoding difference that flag-off parallel shares.)
- [x] depth-64 seam case: touched account + fresh storage; assert the injected storage root and seam branch
      bytes == sequential. (`TestTruthtreeFold_Depth64SeamParity`: touched whale account over fresh
      multi-nibble storage, seam + interior branches byte-match, serial fold fires.)
- [x] run `make test-short` in `execution/commitment` — must pass before Task 2. (Full `go test -short ./...`
      under `execution/commitment` green.)

### Task 2: Fix any interior empty-wall divergence (conditional — only if Task 1 reds)

**Files:**
- Modify: `execution/commitment/truthtree_fold.go`

- [x] Task 1 was fully green — no interior empty-wall divergence was localized. No speculative edit to
      `foldNode` / `truthtree_fold.go`; the serial fresh fold already byte-matches sequential at every
      interior prefix (four Task 1 parity tests confirm).
- [x] no fixed case to pin — Task 1's `FreshInteriorParity` / `FreshNonWhaleParity` / `ArityGateSingleNibble`
      / `Depth64SeamParity` already cover interior, non-whale, arity-gate, and seam prefixes red-clean.
- [x] parity suite green before Milestone 2 — the four Task 1 parity tests pass (0.34s) and full
      `go test -short ./execution/commitment/` is green (14.8s).

## Milestone 2 — Parallel `foldNode` fresh fan-out (recover the regression)

### Task 3: `foldFreshForkJoin` — recursive per-prefix fork-join over `foldNode`, buffer-per-lineage

**Files:**
- Modify: `execution/commitment/fold_pool.go`
- Modify: `execution/commitment/truthtree_fold.go`

- [x] `foldPool.foldFreshForkJoin(ctx, node, accPrefix) (common.Hash, []*DeferredBranchUpdate, error)` added
      (`fold_pool.go`): a recursive fork-join (`forkFolder.fold`, `truthtree_fold.go`) over the touched
      subtree. At a branch with `subtreeCount > K` (`K = foldK(node.subtreeCount, numWorkers)` — the same
      `foldK` policy the DAG uses) each child branch forks with its **own** `foldCtx` and recurses; at
      `subtreeCount <= K` it folds serially via `foldCtx.foldNode` (buffer reuse). The fork unit is the
      **split point**, never the top nibble.
- [x] empty-wall base (provably fresh); forked child roots stitch via the shared `hashBranchRow` — the
      exact 17-slot branch keccak extracted from `foldNode`, so serial and fork-join are byte-identical;
      per-branch `DeferredBranchUpdate`s emitted; child deferred merged at each join after the barrier.
      Parity gate confirms the store is dedup-safe (unique fresh prefixes, order-independent).
- [x] concurrency bounded by the existing `foldSem` (no new semaphore); only children `> K` fork, small
      subtrees fold inline via `foldNode`. **TryAcquire + inline-fallback** replaces a blocking acquire:
      recursive forks would deadlock a blocking acquire (a forked goroutine holds its slot while awaiting
      its own children); the elastic fallback always makes progress and never over-spawns.
- [x] `TestTruthtreeFold_ForkJoinParity` (non-whale + 3 whales, N≥3) and
      `TestTruthtreeFold_ForkJoinMultiDepthParity` (K=64 over an abundant sem, `forkFoldMaxDepth > 64`
      pins forks **below** the depth-64 storage root): fork-join == serial `foldFreshStorageRootDeferred`
      == sequential oracle, root + every stored branch, all green under `-race`.
- [x] `TestTruthtreeFold_ForkJoinAllocCeiling`: parallel arm 43 MB/op on the 750k whale (< 96 MB ceiling,
      never ~575 MB); serial `TestTruthtreeFold_AllocCeiling` still 42.6 MB.
- [x] parity + alloc gates pass; `make lint` and full `go test -short ./execution/commitment/` green.

### Task 4: Route flag-on fresh whale to the parallel path; delete the serial path

**Files:**
- Modify: `execution/commitment/fold_pool.go`
- Modify: `execution/commitment/truthtree_fold.go`

- [ ] rewire `fold_pool.go:406`: flag-on fresh whale (`>= 2` nibbles) → `foldFreshForkJoin` (not
      `foldFreshStorageRootDeferred`); the `< 2` branch stays on `foldFreshStorage` (arity-1 guard preserved).
- [ ] delete `foldFreshStorageRootDeferred`; delete `foldFreshStorageRoot` only if no remaining test refs it.
- [ ] `Benchmark_TruthtreeFold_Gate` + fresh-whale bench: assert flag-on ≤ flag-off on 750k whale (regression erased).
- [ ] run parity suite — must pass before Task 5.

### Task 5: Verify acceptance criteria

- [ ] flag-on == flag-off == sequential: root + stored-branch parity, N≥3 batches, across fresh whale, fresh
      non-whale, single-first-nibble whale (arity gate), depth-64 seam.
- [ ] `Benchmark_TruthtreeFold_Gate`: flag-on ≤ flag-off on 750k whale.
- [ ] alloc: serial ≤ 44 MB, parallel ~49 MB, never 575 MB.
- [ ] DAG untouched: `rg 'deriveFoldFrontier|foldTask|freshWhaleCandidate|reuseSchedulerCells'` shows no
      diffs vs the branch base in non-test code.
- [ ] `make lint && make test-short` clean in `execution/commitment`.

### Task 6: [Final] Docs; flip the default

**Files:**
- Modify: package doc / parallel-patricia design doc

- [ ] document the parallel `foldNode` fresh fold (TruthTree = direct-fold body inside the existing DAG; no DAG change).
- [ ] flip `--experimental.truthtree-fold` default on (only after Task 5 fully green).
- [ ] move this plan to `docs/plans/completed/`.

## Post-Completion

*No checkboxes — external / manual.*

- **Branch:** `awskii/truthtree-forkjoin` off `awskii/truthtree-fold` (48ab330d23). Rollback: the serial-but-correct
  `truthtree-fold` tip; `truthtree-rollback-base` (46ffcc1b8c) is the pre-TruthTree frontier engine.
- **Validation before default-on:** whole-node flag-on vs flag-off root comparison over a live block range
  (the unit parity oracle is not a field check).
- **Optional follow-up:** once flag-on is validated, `foldFreshStorage` (mount+replay) can be retired so the
  fresh path has a single body. Separate change.
- **Honest-scope note (PR body, not code):** engine-level allocs are ~flat (the isolated 44 MB fold win is a
  slice of whole-`Process` ~350 MB). Wins are **time** (parallel `foldNode`, regression erased) and a smaller
  **friction** delta (the serial fallback deleted, replay dropped on the fresh path). Not a memory win — do not claim one.
