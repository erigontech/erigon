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

**K (box-adaptive):** `K = max(K_min, total/(c·numWorkers))`, `total = root.subtreeCount`, `K_min` = floor amortizing per-task fixed cost (own trie ctx + mmap pin + a `Branch(P)` seed read). `c` is a small oversubscription factor for variance smoothing — **default `c=1`** (≈ one leaf task per top nibble on balanced data at `numWorkers` = natural fan-out: a genuine no-op), raised **only** if the pre-wiring `numWorkers=NumCPU` bench shows a net win without regression. The earlier `c≈4–8` idea is **rejected**: at `numWorkers=NumCPU` it subdivides every top nibble into ~`c` tasks each paying ctx+pin+seed — exactly the measured detach regression (lower threshold regressed, core-bound). The no-op property is governed by **core headroom** (subdivide only when `GOMAXPROCS` exceeds the natural fan-out width) and `K_min`, **not** a fixed ≤16-core cutoff.

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

- [ ] generalize `unfoldStorageBase` → `seedBaseAtPrefix(base, P)` seeding row 0 from `Branch(P)` at any depth, returning `errStorageBaseNotBranch` when absent (single primitive; also the derivation `seedable` prober)
- [ ] generalize `foldStorageLeaf` → a leaf-task fold that mounts at arbitrary P on the parent's seeded base, replays its key group, `foldMounted` → cell (exercise the arbitrary-P mount wall in `mountTo`/`foldMounted`)
- [ ] **parametrize the depth-64 hardcode:** `storageRootFromSingleChild`'s `computeCellHash(&root, 64, nil)` and `aggregateMountedStorageRoot`'s `base.root` return are storage-seam-specific — split into (a) the depth-64 account/storage seam variant (storage-root injection, kept) and (b) a general account-plane merge returning a **mount-wall-relative cell excluding P** (invariant M) at `depth = len(P)`. An interior single-survivor collapse must not use the storage return convention.
- [ ] seed-or-demote decision function with direct coverage of **both** evidence paths (false-empty drops siblings → wrong root; false-present → serial only)
- [ ] mount-boundary byte-equality tests at depths {2, 64, 65}, **including a single-survivor collapse at depth 2** (not only at 64); error-path test: merge on an unseedable base returns the hard error
- [ ] run tests — must pass before next task

### Task 4: Frontier pool dispatch wired into the streaming Process path

**Files:**
- Modify: `execution/commitment/streaming_commitment.go`
- Create: `execution/commitment/fold_pool.go`
- Create: `execution/commitment/fold_pool_test.go`

- [ ] `fold_pool.go`: shared pool of `numWorkers` goroutines pulling ready `*foldTask`; on completion decrement parent `pending`, enqueue parent at zero; single GOMAXPROCS budget; no task blocks holding a worker
- [ ] rewire `StreamingCommitter.Process` to `deriveFoldDAG` → run the pool → final root fold → deferred handling (replace `foldPresentSplits`/`foldSplit`/`stitchSplitCells` static errgroup on the Process path; leave the scheduler *mechanics* untouched)
- [ ] **consume the scheduler's cached cells:** a top-nibble-grain leaf task whose `splitState.reusable()` holds a clean pre-folded cell reuses it (overlap preserved for light nibbles); heavy/dirty nibbles fold fresh. Test asserts reuse is actually observed (not silently orphaned).
- [ ] **NumCPU no-op gate (Finding 4):** before declaring the task done, run the `numWorkers=NumCPU` balanced-corpus bench with `c=1` and confirm no wall-clock regression vs the pre-change engine; if it regresses, STOP — do not proceed to Task 5
- [ ] deadlock-free oversubscription test: many ready subtrees, few workers → completes, concurrency bounded by GOMAXPROCS
- [ ] Task 1 parity chains green through the new dispatch (root + branch byte parity, encode/restore, N≥3) on all corpora
- [ ] race detector clean on the dispatch tests; `make lint` clean
- [ ] run tests — must pass before next task

### Task 5: Route ModeParallel (processMounted) through the unified dispatch

**Files:**
- Modify: `execution/commitment/parallel_mount.go`
- Modify: `execution/commitment/parallel_patricia_hashed.go`

- [ ] `processMounted` delegates to the same `deriveFoldDAG` + pool (its static top-nibble errgroup replaced); facade (`RootHash`, deferred API, pooling, `Variant`) unchanged so `commitmentdb` needs no changes
- [ ] delete now-dead `printMountTiming`/`cmtTiming` (`parallel_mount.go:217-249`) and any ModeParallel dual prefix-trie insert the shared dispatch makes unused (funds the net-negative target beyond the whale deletion)
- [ ] port the parent plan's B8 guard: debug-assert that the template never carries a factory-owned ctx across Process calls (the `processMounted` ctx-fallback site survives here)
- [ ] confirm the ModeParallel (scheduler-never-started) and streaming (scheduler) paths share one dispatch
- [ ] existing parallel/streaming tests green against the unified engine; N≥3-batch branch-parity chains green
- [ ] write tests: ModeParallel and streaming produce identical root + branches on the shared corpora
- [ ] run tests — must pass before next task

### Task 6: Delete the whale fan-out scaffolding

**Files:**
- Modify: `execution/commitment/streaming_deep_fold.go`
- Modify: `execution/commitment/parallel_mount.go`

- [ ] delete `foldStorageRoot`'s errgroup fan-out, `isDeepStorageAccount`, the `deepStorageThreshold` gate, and now-dead `foldSem`/`newFoldSem`/`maxFoldConcurrency` plumbing if unused (whale storage now partitions through the general DAG)
- [ ] keep `storageRootFromSingleChild` and `setAccountStorageRoot` as the depth-64 seam primitives invoked by the general merge/leaf tasks
- [ ] whale corpora (750k–1M slots, embedded-slot, single-nibble-on-disk, collapse-to-survivor, delete-all) parity green: root + branch byte parity vs sequential
- [ ] record net line delta in `execution/commitment` (target: negative)
- [ ] run tests — must pass before next task

### Task 7: Error, cancel, and pin discipline

**Files:**
- Create: `execution/commitment/fold_pool_lifecycle_test.go`
- Modify: `execution/commitment/fold_pool.go`

- [ ] failable MockState (Branch/PutBranch error after N calls) driving every recycle path: worker error mid-leaf, merge-seed error, deferred-apply error → no double-pooled workers, no leaked deferred updates, committer reusable after `Reset`
- [ ] ctx-cancel mid-Process: clean unwind, trie restorable via `SetState`, second Process after cancel yields the sequential root
- [ ] fail-closed assertions: any task error drops ALL deferred and writes nothing to the store (snapshot untouched); unseedable merge hard-errors rather than emitting a branch
- [ ] poisoning ctx factory that invalidates buffers after `cleanup()` — regression net for the mmap use-after-munmap class; every task's reads stay in its own pin scope
- [ ] `go test -race ./execution/commitment/` clean
- [ ] run tests — must pass before next task

### Task 8: Performance gate

**Files:**
- Modify: `execution/commitment/parallel_streaming_bench_test.go`

- [ ] extend `Benchmark_DeepStorageWhale`, `Benchmark_Commitment_1MWhales`, `Benchmark_StorageConcurrency` to compare frontier-pool vs (git-stashed) static partition vs sequential at `numWorkers ∈ {NumCPU, 2×, 4×}` on mega-whale + mixed corpora
- [ ] add an idle-tail / workers-busy-over-time metric (not just aggregate ns/op)
- [ ] sweep `c` to fix the constant; record ALL numbers in this plan (Solution Overview + a results block)
- [ ] acceptance: no regression at ≤16 cores / `numWorkers=NumCPU`; measurable utilization gain >16 cores on whales — if any corpus regresses, STOP and record numbers, do not co-tune `c` and topology in the same change
- [ ] run benches — record; `make lint` clean

### Task 9: Verify acceptance criteria

- [ ] all Overview goals implemented: frontier-pool dispatch, storage-first-by-dependency, unified ModeParallel+streaming Process path, whale fan-out deleted with seam kept
- [ ] edge cases: degenerate roots, delete-to-empty, single-survivor collapse, extension-topped mount, threshold boundary, encode/restore round-trips, whale-without-account-touch
- [ ] full suite: `GOGC=80 make test-all` (or `make test-short` + package `-count=1 -race`)
- [ ] `make lint` clean (run repeatedly — non-deterministic)
- [ ] net line count vs main recorded (target: negative in `execution/commitment`)

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
