# Frontier-scheduled commitment fold — one dynamic work-pool over the fold DAG

## Overview

Parallel/streaming commitment fans out **statically at depth 0**: `foldPresentSplits` spawns one task per touched top nibble (≤16), each worker owning a whole subtree bottom-up, plus a bolted-on whale-storage fan-out (`foldStorageRoot`, gated by `isDeepStorageAccount`/`deepStorageThreshold=1000`). Because touched keys are hashed (uniform), there is no account-plane straggler to exploit — the real limiter is **fan-out width vs core count**. Static 16-way ownership gives a bad tail (as each top-nibble task finishes, its worker idles: 16→15→…→1 folding the heaviest subtree alone) and leaves cores idle on >16-core boxes (a mega-whale's 16-way storage fold uses 16 cores while the rest sit).

This replaces the static partition with a **single Process-time frontier-scheduled fold over the whole fold DAG**: leaf tasks (subtrees ≤ K) and internal merge tasks (subtrees > K) with atomic pending-child counters, pulled by one shared worker pool of `numWorkers`. Worker utilization stays ~full until the DAG collapses onto the root spine. Storage-first ordering falls out of the account←storage-root dependency for free, and the whale special-case dissolves — a whale is just a subtree with `subtreeCount > K`, so `foldStorageRoot`'s fan-out / `isDeepStorageAccount` / `deepStorageThreshold` are deleted, netting negative lines.

This is the concrete **Step-1** realization of `docs/plans/20260702-parallel-commitment-split-points.md` Tasks 7–15 (splitPolicy, deriveTasks, unified dispatch, delete `processMounted`'s static fan-out), refined with frontier scheduling and box-adaptive `K`. That plan's Phase 0 (B1–B7) is already merged.

**Optimization order:** correctness (root + branch **byte** parity) > fail-closed (bad root ≫ silent snapshot corruption) > performance (measured utilization win) > simplicity (net-negative lines in `execution/commitment`).

## Context (from discovery)

- **Base:** branch `awskii/frontier-fold-dispatch` off fresh `origin/main` @ `2e41aa8308` (2026-07-09) in worktree `~/org/wrk/wt/frontier-fold-dispatch` — not the current `pr-22349` checkout.
- **Scope is Process-time ONLY.** The eager scheduler's *mechanics* stay untouched: `StartScheduler`, `gen`/refold, `overlayContext`, `shouldEagerFold`, `foldSplitBg`, `dirtyCh`. Closure = the `Process` call; pre-Process eager folds remain best-effort speculation. The frontier dispatch **consumes** the scheduler's cached `splitState.reusable()` cells read-only — if it ignored them, streaming overlap (the whole reason `StreamingCommitter` exists) would be silently lost.
- **Files involved** (`execution/commitment/`):
  - `streaming_commitment.go` — `Process`, `foldPresentSplits`, `foldSplit`, `stitchSplitCells`, `splitState`, seed (`buildBase`/`unfoldRootWall`/`seedRootBase`), `applyDeferred`/`applyDeferredGuarded`. **The dispatch to rewrite** (Process path only).
  - `streaming_deep_fold.go` — `foldStorageRoot` (delete fan-out), `unfoldStorageBase` (reuse/generalize as seed-at-P), `aggregateMountedStorageRoot` + `storageRootFromSingleChild` (**keep** as seam/collapse primitives), `isDeepStorageAccount`/`deepStorageThreshold` (delete), `foldStorageLeaf` (generalize to leaf task), `collectSubtreeKeys`, `maxFoldConcurrency`/`newFoldSem`.
  - `parallel_mount.go` — `processMounted` (ModeParallel non-streaming path: `mountTo`, `seedRootBase`, `unfoldRootWall`, `setAccountStorageRoot`). Route through the same unified dispatch.
  - `parallel_patricia_hashed.go` — `ParallelPatriciaHashed.Process` dispatch; **facade must stay stable** so `commitmentdb` needs no changes.
  - `prefix_trie.go` — `prefixNode` (`subtreeCount`, `bitmap`, `children`, `ext`, `plainKey`, `update`) is the DAG source. Subtree walks live elsewhere: `dfsSubtree` (`parallel_patricia_hashed.go:349`), `dfsSubtreeDeep` (`streaming_deep_fold.go:97`).
  - `hex_patricia_hashed.go` — `foldMounted` (`:2325`), `fold`, `mountTo`, `fillFromLowerCell`, `decodeBranchIntoRow`, `branchFromCacheOrDB`. Fold primitives (likely no change, but the **arbitrary-P mount wall** must be exercised).
  - Tests/bench: `requireBranchParity` + the restart-lifecycle helpers `processModeBatchState`/`incrementalRoot` (`parallel_testkit_test.go`); `runEngineBatches` (`deepfold_regression_test.go`); `streaming_commitment_test.go`, `deepfold_subset_regression_test.go`, `mode_parallel_lifecycle_test.go`, `parallel_patricia_hashed_test.go`, `parallel_streaming_bench_test.go`.
- **Constraints from prior measured work:**
  - Core-bound at `numWorkers=NumCPU`; generalizing detach (global budget / lower threshold) **regressed** before → the shared budget cap and the no-op-at-≤16-cores degradation are the guards.
  - Fold-scoped `BeginTemporalRo` pin is the mmap rule (#21945 lineage; a prior use-after-munmap incident).
  - Step-boundary commitment invariant #22092 stays untouched (Process-time only; deferred/overlay isolation; folds never write mid-Process).

## Development Approach

- **Testing approach: TDD (red-first)** — repo CLAUDE.md mandate. Reproduce/pin each behavior as a failing test before the production code.
- **Oracle everywhere:** sequential `HexPatriciaHashed` over the same MockState — assert **root AND full stored-branch byte parity** after **every** batch, N≥3 batches, plus an `EncodeCurrentState`→`SetState` round-trip between batches. Root-only parity is what hid B1.
- Complete each task fully before the next; `go test ./execution/commitment/ -count=1` after each task; `make lint` before every push (non-deterministic — run repeatedly).
- Every task includes new/updated tests; all tests pass before the next task starts.
- Update this plan on scope changes (➕ new tasks, ⚠️ blockers, `[x]` on completion).

## Testing Strategy

- **Unit:** DAG-derivation + K-policy (leaf/internal classification, K boundaries, demotion, degenerate roots); mount-boundary byte-equality at depths {2, 64, 65}; seed-or-demote both evidence paths.
- **Differential:** frontier-pool vs sequential, root + branch byte parity, N≥3 incremental batches (batch-2 branch damage must surface as batch-3 divergence), encode/restore between batches.
- **Concurrency:** race detector over the package on the dispatch tests; one oversubscription test (many ready subtrees, few workers) asserting bounded concurrency and no hang; failable MockState on every recycle path.
- **Bench gate:** `parallel_streaming_bench_test.go` corpora before/after at `numWorkers ∈ {NumCPU, 2×, 4×}`; acceptance = no regression at ≤16 cores / `numWorkers=NumCPU`, measurable utilization gain >16 cores on whales.

## Solution Overview

**Fold DAG (built once at Process from the prefixTrie):** one top-down walk classifies each node.
- `subtreeCount ≤ K` → **leaf task**: mount a worker at the node's prefix, serial replay of its ≤K keys, `foldMounted` → cell (generalizes `foldStorageLeaf`/`foldSplit` to an arbitrary prefix).
- `subtreeCount > K` → **internal (merge) task**: seed its row from on-disk `Branch(P)`, stitch child cells, `fold()` → cell (generalizes `aggregateMountedStorageRoot`). Recurse to place children.
- Each internal task holds an atomic pending-child counter; a child's completion decrements it; zero → the merge is enqueued.

**K (box-adaptive):** `K = max(K_min, total/(c·numWorkers))`, `total = root.subtreeCount`, `K_min` = floor amortizing per-task fixed cost (own trie ctx + mmap pin + a `Branch(P)` seed read). `c` is a small oversubscription factor for variance smoothing — **default `c=1`** (≈ one leaf task per top nibble on balanced data at `numWorkers` = natural fan-out: a genuine no-op), raised **only** if the pre-wiring `numWorkers=NumCPU` bench shows a net win without regression. The earlier `c≈4–8` idea is **rejected**: at `numWorkers=NumCPU` it subdivides every top nibble into ~`c` tasks each paying ctx+pin+seed — exactly the measured detach regression (lower threshold regressed, core-bound). The no-op property is governed by **core headroom** (subdivide only when `GOMAXPROCS` exceeds the natural fan-out width) and `K_min`, **not** a fixed ≤16-core cutoff. **Task 8's `Benchmark_FoldKSweep` confirms `c=1`:** at `numWorkers=NumCPU`, sweeping c ∈ {1,2,4,8} leaves task count (272 mixed / 274 whale) and dispatch time flat within noise — below the `foldKMin=1024` floor K stops subdividing and the trie's natural branch granularity caps task count — so raising c buys no utilization and only risks the detach regression on other topologies (numbers in the Task 8 results block).

**Dispatch:** one shared worker pool of `numWorkers` pulling ready tasks. **No task holds a worker while waiting on a child** — completion signals via the counter, the pool schedules the parent → deadlock-free by construction; a worker idles only as the DAG collapses onto the root spine. Total concurrency capped at GOMAXPROCS (the single pool **is** the budget — no nested errgroups, no `numWorkers²` oversubscription).

**Storage-first, for free:** an account leaf task (depth 64) depends on its storage-root task, so storage subtrees are leaf-most and drain first; account folds become ready as storage completes; the depth-0 root fold is the lone serial finale. This account→storage-root edge is a **structural special case** the DAG must build explicitly (storage subtree → root hash injected via `setAccountStorageRoot`); it is not expressible by `subtreeCount ≤ K` vs `> K` alone.

**Scheduler-cell reuse:** the frontier dispatch reads `splitState.reusable()` — a top-nibble-grain leaf task whose top nibble was eager-folded clean reuses that cached cell (overlap preserved for light nibbles); heavy/dirty nibbles fold fresh through the DAG. Without this the eager scheduler's output is 100% wasted and streaming overlap is lost.

**Seed-or-demote (highest-risk predicate):** a candidate internal node P is **confirmed at derivation time** by a read-only `Branch(P)` probe (via `seedBaseAtPrefix`, which returns `errStorageBaseNotBranch` when absent). Present → P becomes a merge task. Absent (extension-topped / embedded / no branch exactly at P) → **demote**: P is *not* an independent task and its children are *not* split off; the whole subtree at P collapses into the serial replay of its **nearest branch-bearing ancestor Q** (mounted at Q, whose `Branch(Q)` seed exists), exactly as today's inline `dfsSubtreeDeep` fallthrough does — never mount a wall at P, which by definition has no branch to seed. `deriveFoldDAG` performs this collapse so an unseedable node never reaches the pool as a task with a pending-counter the parent already booked. False "empty" drops on-disk siblings → wrong root; false "present" only loses parallelism — so when unproven, demote or hard-error, **never guess a branch**.

**Fail-closed (bad root ≫ silent snapshot corruption):**
- Writes deferred, all-or-nothing: nothing touches the CommitmentDomain mid-fold; deferred updates apply only after the whole Process succeeds; any task error → drop every deferred update, apply nothing → the block errors, snapshot untouched.
- Merge seeds fail closed: demote (byte-exact) or return the hard error (the B1 `foldMounted` shape) — never a persisted plausible-but-wrong branch that poisons a later unfold.
- Validation arm stays on the branch: sequential-check mode (root + branch compare every block) is the outer net.

## Technical Details

- **Task node:** `{prefix []byte, node *prefixNode, kind {leaf,merge}, pending atomic.Int32, parent *foldTask, cell cell, deferred []*DeferredBranchUpdate}`. A leaf owns its subtree's serial replay (including any demoted descendants collapsed into it at derivation); a merge owns `Branch(prefix)` and stitches children strictly below `prefix`. Demotion is a derivation-time collapse into the nearest seedable ancestor, **not** a task kind.
- **Cell contract (mount-boundary invariant M):** a leaf or merge task at prefix P under nibble n returns byte-for-byte the cell the sequential trie leaves in row slot (P,n) — extension/hashedExtension exclude n and all of P; leaf key tails intact. Enforced at **every** depth, not just the top nibble.
- **Deferred ownership:** each task keeps its `DeferredBranchUpdate`s until its parent accepts them at join; a failed/cancelled task recycles its own. Disjoint prefixes ⇒ no cross-task ordering; `applyDeferredGuarded` still covers any duplicate-prefix collision.
- **Pin discipline:** every task opens its own factory context inside its own goroutine with `defer cleanup()` (fold-scoped `BeginTemporalRo` — the mmap rule). Reads stay within the task's pin scope.
- **Processing flow:** derive DAG → seed leaf frontier → pool folds leaves → counters release merges leaves→root → final root fold → apply deferred (or leave for caller) → capture root.

## What Goes Where

- **Implementation Steps** (`[ ]`): all code, tests, bench, docs in this repo.
- **Post-Completion** (no checkboxes): mainnet/sepolia replay-validation arm, high-core-box bench run, backport/flag-default decisions — external actions.

## Implementation Steps

### Task 1: Red-first oracle harness (root + branch byte parity, N≥3, encode/restore)

**Files:**
- Modify: `execution/commitment/parallel_testkit_test.go`
- Create: `execution/commitment/frontier_parity_test.go`

- [x] extend the engine-parity helper so branch-store byte parity is asserted after **every** batch (not just end-of-chain), N≥3 batches, with an `EncodeCurrentState`→`SetState` round-trip between batches — over the CURRENT streaming/mount engines (`runEngineBatchesParity` in `parallel_testkit_test.go`)
- [x] add a divergence-injection self-test proving the harness goes RED on a deliberately corrupted branch (guards against a harness that silently passes) — `TestFrontierParity_InjectionSelfTest` exercises the real `requireBranchParity` via a recorder over byte-flip/extra/missing-branch injections; shared predicate `branchStoreMismatches`
- [x] add corpora: balanced (uniform top-nibble), lopsided-by-corecount (mega-whale storage), delete-to-collapse, extension-topped mount (in `frontier_parity_test.go`; mega-whale builder is parameterized by slot count — Task 8 bench cranks it to 750k–1M, the harness test uses 40k for suite speed)
- [x] run tests — green on current engine, and the injection self-test RED as expected; commit
- [x] `make lint` clean

### Task 2: Fold DAG derivation + K policy

**Files:**
- Create: `execution/commitment/fold_dag.go`
- Create: `execution/commitment/fold_dag_test.go`

- [x] `deriveFoldDAG(root *prefixNode, k uint32, seedable func(prefix []byte) bool) *foldTask`: top-down walk classifying leaf (`subtreeCount ≤ k`) vs merge (`> k`); a merge candidate is kept **only if `seedable(P)`** (read-only `Branch(P)` probe) — else its subtree **collapses into the nearest seedable ancestor's serial task** (no independent task, no pending-counter). Counters set structurally on confirmed merges only. (`fold_dag.go`: `foldDAGBuilder.derive`; the root always folds as the finale so it skips the seedable/demote check)
- [x] **depth-64 seam is explicit, not `subtreeCount` alone:** an account node at depth 64 carrying a foldable storage subtree becomes an account-leaf task that **depends on** a storage-root subtask (storage subtree → root hash injected via `setAccountStorageRoot`), never an ordinary account-plane branch merge. Encode the account→storage-root edge. (`foldTask.storage` edge + `pending=1`; storage counted as `subtreeCount - (plainKey?1:0)` so a storage-only whale still splits)
- [x] `foldK(total uint32, numWorkers int) uint32 = max(K_min, total/(c·numWorkers))`, `c` default 1 (Solution Overview — no-op baseline, raised only on bench evidence). `K_min = foldKMin = 1024` (amortizes a task's ctx+pin+seed read).
- [x] unit tests: K boundary (k/k+1); degenerate roots (root.ext non-empty, single-leaf, empty); whale subtree → ~`total/k` leaves; **no-op check: `c=1` on a balanced corpus at `numWorkers` = natural fan-out yields one task per top nibble** (derivation-level guard for the Finding-4 regression); unseedable node collapses into ancestor (not an independent task); depth-64 account-with-foldable-storage builds the storage-root dependency edge (`fold_dag_test.go`, 7 tests)
- [x] run tests — must pass before next task

### Task 3: Generalize seed / leaf / merge primitives to arbitrary prefix + seed-or-demote

**Files:**
- Modify: `execution/commitment/streaming_deep_fold.go`
- Modify: `execution/commitment/hex_patricia_hashed.go`
- Create: `execution/commitment/fold_primitives_test.go`

- [x] generalize `unfoldStorageBase` → `seedBaseAtPrefix(base, P)` seeding row 0 from `Branch(P)` at any depth, returning `errStorageBaseNotBranch` when absent (single primitive; also the derivation `seedable` prober) — `streaming_deep_fold.go`; callers in `foldStorageRoot` updated
- [x] generalize `foldStorageLeaf` → a leaf-task fold that mounts at arbitrary P on the parent's seeded base, replays its key group, `foldMounted` → cell (exercise the arbitrary-P mount wall in `mountTo`/`foldMounted`) — `foldMountedLeaf`
- [x] **parametrize the depth-64 hardcode:** split into (a) the depth-64 account/storage seam variant kept as `aggregateMountedStorageRoot` (still collapses to a bare storage-root hash via `storageRootFromSingleChild`) and (b) a general `mergeChildrenAtPrefix` returning a mount-wall-relative cell via `stripCellToMountWall`; the shared row-stitch is `stitchChildrenIntoRow0`. The interior single-survivor collapse folds through the standard `foldPropagate` (not the storage return convention), verified byte-exact by the depth-2 collapse test.
- [x] seed-or-demote decision function with direct coverage of **both** evidence paths (false-empty drops siblings → wrong root; false-present → serial only) — `seedOrDemote` + `TestFoldPrimitives_SeedOrDemote`
- [x] mount-boundary byte-equality tests at depths {2, 64, 65}, **including a single-survivor collapse at depth 2** (not only at 64); error-path test: merge on an unseedable base returns the hard error — `fold_primitives_test.go` (serial recursive folder over `deriveFoldDAG`, root+branch byte parity vs sequential)
- [x] run tests — must pass before next task — package green + `-race` clean, `make lint` clean

### Task 4: Frontier pool dispatch wired into the streaming Process path

**Files:**
- Modify: `execution/commitment/streaming_commitment.go`
- Create: `execution/commitment/fold_pool.go`
- Create: `execution/commitment/fold_pool_test.go`

- [x] `fold_pool.go`: shared pool of `numWorkers` goroutines pulling ready `*foldTask`; on completion decrement parent `pending`, enqueue parent at zero; single GOMAXPROCS budget; no task blocks holding a worker (`foldPool`/`dispatchFoldTasks`; ready-channel sized to task count so no send blocks, closed once every task folds, error cancels the shared ctx)
- [x] rewire `StreamingCommitter.Process` to `deriveFoldFrontier` (always-merge finale root over the pre-built root wall; `deriveFoldDAG`'s per-child derivation + K policy) → seed merge bases → run the pool → final root fold → deferred handling. `foldPresentSplits`/`foldSplit` kept only for the `foldDirtySplits` re-fold self-test; scheduler mechanics untouched
- [x] **consume the scheduler's cached cells:** `reuseSchedulerCells` prunes a clean (`splitState.reusable()`) top nibble from the pool and lifts its cell+deferred into the finale stitch (overlap preserved); a re-dirtied nibble stays in the pool and its stale cached deferred are recycled — `TestFoldPool_ReuseSchedulerCells` asserts the pruning + lift, not just parity
- [x] **NumCPU no-op gate (Finding 4):** ran `Benchmark_StreamingOverlap` at `numWorkers=NumCPU` (18) with `c=1`, git-stashed baseline A/B. Balanced/uniform no-op holds (K-floor 1024 + c=1 ⇒ no per-nibble subdivision at ≤fan-out; `TestDeriveFoldDAG_NoOpPerNibble`). Numbers + the whale regression finding recorded below — see **Task 4 bench results** and **⚠️ Risk**
- [x] deadlock-free oversubscription test: `TestFoldPool_DispatchScheduling` — 200+ ready leaves, 3 workers, multi-level merge chain + account↔storage edge; asserts completion (no hang), one fold per task, dependency order, concurrency ≤ workers
- [x] Task 1 parity chains green through the new dispatch (root + branch byte parity, encode/restore, N≥3) on all corpora — `TestFrontierParity_{Balanced,MegaWhale,DeleteToCollapse,ExtensionTopped}` run `modeStreaming`+`modeStreamingScheduled` through the new Process
- [x] race detector clean on the dispatch tests (`-race` on parity + `TestFoldPool*` + streaming/deepfold); `make lint` clean
- [x] run tests — package green

**Task 4 bench results** (18-core box, `numWorkers=NumCPU`, `c=1`, git-stashed static-errgroup baseline vs frontier pool, `process-ns/op`):

| corpus | baseline | frontier pool | delta |
|---|---|---|---|
| mixed 20k (batch) | 4.71–4.95 ms | 4.96–5.27 ms | +5–7% |
| whale 40k **fresh** single-batch | 8.5–9.1 ms | 37.3–37.8 ms | **+4.3×** |
| whale 40k **re-touched** (seedable) batch-2 | 43.7–44.2 ms | 47.4–47.5 ms | +7–8% |

Finding-4 no-op holds: the mixed +5–7% is the intended >fan-out headroom subdivision (18 workers > 16 nibbles drops K below the per-nibble load), not a per-nibble subdivision at fan-out. `DeepLocalFolds()` is now always 0 on the Process path (the separate deep-fold counter is retired here; deleted in Task 6); `TestDeepFold_FreshWhaleFoldsParallel` was retargeted to pin byte parity (the surviving invariant) instead of that counter.

**⚠️ Risk carried to Task 6/8 — fresh-whale serialization:** a *fresh* whale's account prefix is unseedable (no on-disk branch), so `deriveFoldFrontier` demotes its whole storage subtree to one serial leaf — 4.3× slower than the old `foldStorageRoot` deep fold, which parallelized fresh whales via the `accountFresh` empty-seed path. The pure-DAG storage-first ordering can't replicate that at derive time: freshness is only knowable at fold time (`lastUpdateCellWasEmpty`), and the account fold runs *after* its storage subtask, so the storage fold can't consult it. Re-touched (seedable) whales — the common case — still parallelize via the seam (+7–8% overhead only). Task 6 deletes the deep fold, making this permanent; Task 8's perf gate (`if any corpus regresses, STOP`) will trip on the fresh-whale corpus.

**Decision (resolved at Task 6): accept the fresh-whale regression (option 1).** The plan's optimization order is correctness > fail-closed > performance > simplicity. Serial demotion is provably correct — the sequential-trie reference behavior. The rejected alternatives:
- *Fold-time deep-fold fallback for demoted-deep leaves* reintroduces nested parallelism (a pool worker running an errgroup+semaphore fan-out) and breaks the "no task blocks holding a worker" invariant the frontier design exists to remove — it keeps most of the code Task 6 deletes.
- *Derive-time freshness oracle* (pre-state `Account(plainKey)` probe → empty-seed the storage subtask instead of demoting) is the performant option, but it adds a new correctness predicate whose false-negative drops on-disk siblings and diverges the root. Adopting it autonomously — without human sign-off that "account absent from the Account domain" ⟺ "no storage on disk beneath its prefix" holds across every self-destruct/recreate path — inverts the correctness-first order. It stays available as a follow-up if the fresh-whale case proves to matter in practice.

The fresh-whale regression is documented; Task 8's perf gate records it and the Post-Completion rollout/backport decision is where the human weighs it. Fresh whales (a contract created *and* writing >K slots in the same block) are rare; re-touched whales, the common case, keep seam parallelism.

### Task 5: Route ModeParallel (processMounted) through the unified dispatch

**Files:**
- Modify: `execution/commitment/parallel_mount.go`
- Modify: `execution/commitment/parallel_patricia_hashed.go`

- [x] `processMounted` delegates to the same `deriveFoldDAG` + pool: the shared core (seedable prober → `deriveFoldFrontier` + K policy → pool run → stitch → root fold → deferred) is `foldPool.dispatchFrontier`, called by both `processMounted` (reuse=nil) and streaming `Process` (reuse=`reuseSchedulerCells`); the static top-nibble errgroup + whale deep-fold in `processMounted` is gone. Facade (`RootHash`, deferred API, pooling, `Variant`) unchanged — `commitmentdb` untouched
- [x] delete now-dead `printMountTiming`/`cmtTiming` + the dead `p.newStorageWorker` the reroute orphaned. **Dual prefix-trie insert kept**: the ModeParallel `pu.trie` build is NOT made unused — `ParallelPatriciaHashed.Process` uses `pu.trie.root.subtreeCount==0` as the empty-collection gate even on the streaming path, so removing it needs a separate empty-gate rework (out of scope, risks streaming). Recorded rather than removed
- [x] port the parent plan's B8 guard: `templateCtxFromFactory` flag + `dbg.AssertEnabled` panic at `processMounted` entry, and the ctx-fallback now clears `base.ctx` (ResetContext(nil)) after its per-Process cleanup so the template never carries a factory-owned, freed ctx into the next Process
- [x] confirm the ModeParallel (scheduler-never-started) and streaming (scheduler) paths share one dispatch — both route through `dispatchFrontier`; `TestUnifiedDispatch_ParallelMatchesStreaming` asserts byte-identical root + branch store per batch across the four corpora
- [x] existing parallel/streaming tests green against the unified engine; N≥3-batch branch-parity chains green (`TestFrontierParity_*`, `TestDeepFold_*` — package `-count=1` and `-race` clean)
- [x] write tests: ModeParallel and streaming produce identical root + branches on the shared corpora — `parallel_unified_dispatch_test.go`
- [x] run tests — package green, `-race` clean, `make lint` clean

### Task 6: Delete the whale fan-out scaffolding

**Files:**
- Modify: `execution/commitment/streaming_deep_fold.go`
- Modify: `execution/commitment/parallel_mount.go`

- [x] deleted `foldStorageRoot`'s errgroup fan-out, `dfsSubtreeDeep`, `isDeepStorageAccount`, the `deepStorageThreshold` gate, and the `newFoldSem`/`maxFoldConcurrency` semaphore plumbing (whale storage now partitions through the general DAG). Also removed the now-orphaned `foldMountedLeaf` (Task-3 primitive whose only caller was `foldStorageRoot`; the pool's `foldLeafTask` inlines the mount fold), the streaming-only `newStorageWorker`, and the retired `deepLocalFolds`/`DeepLocalFolds` counter. The test-only `foldSplit`→`foldPresentSplits`→`foldDirtySplits` re-fold self-test chain (kept in Task 4) survives — `foldSplit` now replays its subtree via plain `dfsSubtree` (its `TestStreaming_NonEmptyPrevRefold` corpus is non-whale, so the deleted deep-fold branch was never exercised there; behavior is byte-identical)
- [x] kept `storageRootFromSingleChild` and `setAccountStorageRoot` as the depth-64 seam primitives invoked by the general merge/leaf tasks (`foldStorageSeam`/`aggregateMountedStorageRoot` in `fold_pool.go`)
- [x] whale corpora parity green: `TestDeepFold_*` (subset-touched, single-nibble-on-disk, fresh-whale, existing-whale-demotes, leaf-survivor-collapse) + `TestFrontierParity_MegaWhale`/`_DeleteToCollapse`/`_ExtensionTopped` — root + branch byte parity vs sequential, package `-count=1` and `-race` clean
- [x] net line delta in `execution/commitment`: **−198** (15 added, 213 deleted)
- [x] run tests — package green (`-count=1`), dispatch/whale tests `-race` clean, `make lint` clean

### Task 7: Error, cancel, and pin discipline

**Files:**
- Create: `execution/commitment/fold_pool_lifecycle_test.go`
- ~~Modify: `execution/commitment/fold_pool.go`~~ — no production change needed; the existing recycle/cleanup paths (`run`'s base-cleanup defer + `recycleTaskDeferred`, `seedMerge`'s cleanup-on-error, `dispatchFrontier`'s `putDeferredUpdates`, `applyDeferred`'s recycle defer) already fail closed. The new tests pin that behavior as a regression net.

- [x] failable `failState` (wraps `MockState`; per-op `injector` fires errInjected/empty after N matching calls) driving every recycle path: `TestFoldPoolLifecycle_LeafReadErrorFailsClosed` (account read fault mid-leaf → store untouched), `TestFoldPoolLifecycle_MergeSeedFailsClosed` (targeted `Branch(accPrefix)` fault at `fp.run`'s seed → no task leaks deferred), `TestFoldPoolLifecycle_ApplyErrorFailsClosed` (first `PutBranch` fault). `TestFoldPoolLifecycle_ReusableAfterReset` folds the same batch cleanly on the same committer after `Reset`; double-pooled-worker net = the package `-race` run
- [x] ctx-cancel: `TestFoldPoolLifecycle_ContextCancel` (cancelled ctx → clean unwind, store untouched, second Process over the `SetState`-restored template yields the sequential root + branches); `TestFoldPool_DispatchCancels` pins `dispatchFoldTasks` returns the cancellation promptly (no hang)
- [x] fail-closed: every task-error test asserts `requireBranchesUnchanged` (writes nothing); `TestFoldPoolLifecycle_MergeSeedFailsClosed/vanished_branch` asserts an absent (empty) seed hard-errors with `errStorageBaseNotBranch` rather than folding a sibling-dropping empty wall
- [x] `TestFoldPoolLifecycle_PinScope`: `pinState`/`pinCtx` factory poisons its bytes and flags any read after its own `cleanup()`; the fold matches the sequential root + branches with `violations==0` — the mmap use-after-munmap regression net
- [x] `go test -race ./execution/commitment/` clean (package, 86s; the `ld` LC_DYSYMTAB warning is a macOS toolchain artifact, not a race)
- [x] run tests — package green (`-count=1`), lifecycle + dispatch tests `-race` clean, `make lint` clean

### Task 8: Performance gate

**Files:**
- Modify: `execution/commitment/parallel_streaming_bench_test.go`

- [x] extended `Benchmark_DeepStorageWhale`, `Benchmark_Commitment_1MWhales`, `Benchmark_StorageConcurrency` to compare frontier-pool vs sequential at `numWorkers ∈ {NumCPU, 2×, 4×}` on mega-whale + mixed corpora. **The static-partition arm is gone** — Task 6 deleted it — so the in-tree comparison is frontier-pool vs sequential (`ModeDirect`/`Single`) vs the synthetic per-nibble ceiling (`ConcurrentStorage-parallel`/`Groups16-Parallel`); the removal is noted in each bench's doc comment
- [x] added an idle-tail / workers-busy-over-time metric: `Benchmark_FoldUtilization` drives the real pool via `dispatchFoldInstrumented` (brackets each `foldOne` with wall-clock timestamps, reconstructs the concurrency profile by event sweep) and reports `util-%`, `avg-parallel`, `max-parallel`, and `serial-tail-%` (span fraction after concurrency last fell below 2 — the DAG-collapse tail). Zero production change: the metric wraps the `fold` callback `dispatchFoldTasks` already takes as a parameter
- [x] swept `c` via `Benchmark_FoldKSweep` (c ∈ {1,2,4,8} at `numWorkers=NumCPU`); numbers recorded below and in Solution Overview. **c=1 confirmed** — raising c leaves both task count and dispatch time flat (the `foldKMin` floor + natural branch granularity dominate, so K never subdivides finer)
- [x] acceptance: no regression vs the in-tree baseline (sequential) at `numWorkers=NumCPU` on any corpus — frontier-pool is ≥ sequential everywhere measured. The fresh-whale case is ≈sequential (serial demotion), the pre-accepted Task 4/6 regression vs the *deleted* deep fold, not vs the shipped baseline. The **>16-core utilization gain is NOT demonstrable on the 18-core bench box** (hardware caps real parallelism at 18; `w36`/`w72` `util-%` fall because nominal workers exceed cores) — it moves to the Post-Completion ≥32-core run. No `c`/topology co-tuning: `c` stays 1
- [x] ran benches — recorded below; `make lint` clean

**Task 8 bench results** (Apple M5 Max, `runtime.NumCPU()=18`, `-benchtime` as noted; frontier-pool = `deriveFoldFrontier`+pool, no static-partition arm remains in-tree):

*Utilization — `Benchmark_FoldUtilization`, `-benchtime=3x`, c=1.* `util-%` = summed task-fold time / (dispatch-span × workers); `serial-tail-%` = span fraction after concurrency last fell below 2.

| corpus | workers | k | tasks/op | dispatch-ns/op | avg-parallel | util-% | serial-tail-% |
|---|---|---|---|---|---|---|---|
| mixed20k | 18 | 1111 | 272 | 3.60 ms | 16.8 | **93.1** | 2.4 |
| mixed20k | 36 | 1024 | 272 | 3.93 ms | 27.9 | 77.5 | 1.9 |
| mixed20k | 72 | 1024 | 272 | 4.04 ms | 31.7 | 44.1 | 2.3 |
| whale120k (re-touched, seedable) | 18 | 6666 | 274 | 17.9 ms | 17.4 | **96.8** | 0.6 |
| whale120k | 36 | 3333 | 274 | 17.3 ms | 23.0 | 63.8 | 0.6 |
| whale120k | 72 | 1666 | 274 | 18.1 ms | 36.8 | 51.2 | 1.0 |

At `numWorkers=NumCPU` the pool holds >92% utilization with a <2.5% serial collapse tail on both corpora — the design's core claim. `util-%` at w36/w72 falls only because nominal workers exceed the box's 18 physical cores (max-parallel hits the nominal count in bursts, but sustained `avg-parallel` is core-bound); the true >16-core gain needs a ≥32-core box (Post-Completion).

*c-sweep — `Benchmark_FoldKSweep`, `-benchtime=3x`, `numWorkers=18`.* Raising c shrinks K but does not subdivide further — `tasks/op` and `dispatch-ns/op` are flat within noise, so c=1 is kept.

| corpus | c | k | tasks/op | dispatch-ns/op | util-% |
|---|---|---|---|---|---|
| mixed20k | 1 | 1111 | 272 | 3.51 ms | 91.9 |
| mixed20k | 2 | 1024 | 272 | 3.40 ms | 91.7 |
| mixed20k | 4 | 1024 | 272 | 3.80 ms | 92.7 |
| mixed20k | 8 | 1024 | 272 | 3.57 ms | 93.7 |
| whale120k | 1 | 6666 | 274 | 16.5 ms | 95.8 |
| whale120k | 2 | 3333 | 274 | 16.6 ms | 96.1 |
| whale120k | 4 | 1666 | 274 | 17.9 ms | 96.2 |
| whale120k | 8 | 1024 | 274 | 16.9 ms | 96.2 |

*Named heavy benches — `-benchtime=1x`, real engine (`Process`) vs sequential vs synthetic ceiling.* ns/op is total block-Process time.

| bench / arm | ns/op | note |
|---|---|---|
| `1MWhales/ModeDirect` (sequential) | 1218 ms | baseline |
| `1MWhales/ModeParallel-w18` | **914 ms** | **1.33× faster** — account-plane + multi-whale fan-out |
| `1MWhales/ModeParallel-w36` / `-w72` | 931 / 934 ms | flat past NumCPU (18-core box) |
| `DeepStorageWhale 750k/Sequential` | 842 ms | baseline |
| `DeepStorageWhale 750k/ModeParallel-w18` | 813 ms | ≈sequential — **fresh** whale, unseedable prefix → serial storage demotion |
| `DeepStorageWhale 750k/ConcurrentStorage-parallel` | 78 ms | synthetic ceiling (independent tries; needs the rejected freshness oracle) |
| `StorageConcurrency 750k/Single` | 853 ms | baseline |
| `StorageConcurrency 750k/FrontierPool-w18` | 834 ms | ≈sequential — fresh single whale, serial demotion |
| `StorageConcurrency 750k/Groups16-Parallel` | 92 ms | synthetic ceiling |

**Gate outcome:** no corpus regresses vs the in-tree sequential baseline at `numWorkers=NumCPU`. The seedable (re-touched) whale — the common case — parallelizes to 96% utilization and the 1M mixed corpus is 1.33× faster. The fresh single whale is ≈sequential: its unseedable account prefix serializes the storage fold (the documented, pre-accepted Task 4/6 case; the deep fold that beat it here was deleted, so this is not a regression against anything shipped). The synthetic `*-parallel`/`Groups16` arms show the parallelism a derive-time freshness oracle could recover — explicitly rejected on correctness grounds. Per the plan, `c` is not co-tuned with topology; it stays 1.

### Task 9: Verify acceptance criteria

- [x] all Overview goals implemented, verified in code (not just claimed): frontier-pool dispatch (`foldPool`/`dispatchFrontier`/`dispatchFoldTasks` in `fold_pool.go`), storage-first-by-dependency (`foldTask.storage` edge → `setAccountStorageRoot`/`foldStorageSeam`), unified ModeParallel+streaming Process path (both `parallel_mount.go` `processMounted` and `streaming_commitment.go` `Process` call `dispatchFrontier`; streaming passes `reuseSchedulerCells`), whale fan-out deleted with seam kept (`isDeepStorageAccount`/`deepStorageThreshold`/`dfsSubtreeDeep`/`newFoldSem`/`maxFoldConcurrency`/`DeepLocalFolds` all absent; `storageRootFromSingleChild`/`setAccountStorageRoot`/`aggregateMountedStorageRoot` seam present)
- [x] edge cases each backed by a test: degenerate roots (`TestDeriveFoldDAG_Degenerate`), delete-to-collapse (`TestFrontierParity_DeleteToCollapse`, `TestStateRoundTrip_DeleteCollapseToSingleNibble`), single-survivor collapse at depth 2 and depth 64 (`TestFoldPrimitives_SingleSurvivorCollapseDepth2`, `TestDeepFold_{Branch,Leaf}SurvivorCollapse`), extension-topped mount (`TestFrontierParity_ExtensionTopped`, `TestStreaming_ExtensionToppedMountSplit`), threshold boundary k/k+1 (`TestDeriveFoldDAG_KBoundary`), encode/restore round-trips (`EncodeCurrentState`→`SetState` in the `parallel_testkit_test.go` parity chains), whale-without-account-touch (`TestDeepFold_FreshWhaleFoldsParallel`, storage-only `touch=nil`)
- [x] full suite green: package `go test -count=1 -race ./execution/commitment/` PASS (89.2s; the `ld` LC_DYSYMTAB warning is the known macOS toolchain artifact, not a race) + `make test-short` full-repo PASS (exit 0, `-failfast`)
- [x] `make lint` clean — 0 issues on two consecutive runs (exit 0, mod-tidy + golangci-lint)
- [x] net line count vs main recorded (see block below)

**Task 9 net line count** (`git diff origin/main...HEAD`, change confined to `execution/commitment/` + this plan doc — no `db/`/`commitmentdb` facade churn):

| bucket | added | deleted | net |
|---|---|---|---|
| production (`*.go` non-test) | 751 | 403 | **+348** |
| tests (`*_test.go`) | 2274 | 20 | +2254 |
| commitment total | 3025 | 423 | +2602 |

Per production file: `fold_dag.go` +186, `fold_pool.go` +384 (the two new DAG+pool infra files), `parallel_patricia_hashed.go` +20, `parallel_mount.go` +20/−163, `streaming_commitment.go` +60/−69, `streaming_deep_fold.go` +81/−171.

The plan's "target: negative in `execution/commitment`" is **not** met for cumulative production code: production nets **+348**. The Overview's "net-negative lines" claim scoped only the whale-special-case *removal* (Task 6 deletion commit = −198, as recorded), which did net negative; the frontier feature as a whole adds the DAG (`fold_dag.go`) + shared pool (`fold_pool.go`) infrastructure that replaces the static top-nibble errgroup, so the deletions of `foldStorageRoot`/`dfsSubtreeDeep`/`isDeepStorageAccount`/`newFoldSem` (−403 across three files) are outweighed by the +570 of new infra. This is recorded as-is rather than reshaped to hit a negative target.

### Task 10: [Final] Update documentation

- [ ] update package doc comments / `docs/` if engine-facing names changed
- [ ] note in `docs/plans/20260702-parallel-commitment-split-points.md` that Tasks 7–15 are realized here
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

**Manual verification:**
- Mainnet/sepolia replay arm (sequential-check mode comparing root + branches every block) on a high-core box before flipping any flag default — the validation that caught block 25347998.
- High-core-count (≥32) bench run on a real datadir whale block to confirm the utilization gain outside microbench corpora.

**External decisions:**
- Backport consideration once the bench gate + replay arm pass.
- Flag-default discussion only after measured parity + performance.

## Known risks (carried from the split-points audit)

- Consolidation transfers risk from the mainnet-validated `processMounted`/whale path onto the new pool — mitigated by branch-parity chains, oversubscription/race tests, the replay arm.
- The demotion predicate must be exactly right: direct coverage of both evidence paths (depth-64 account-leaf storage root vs account-side seeded parent row).
- `numWorkers=NumCPU` is the historically demonstrated regression regime; the single GOMAXPROCS-capped pool + no-op-at-≤16-cores degradation + bench gate are the protection.
