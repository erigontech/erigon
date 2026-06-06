# Parallel Patricia Hashed — Commitment Trie Parallelization

Status: Experimental (`--experimental.parallel-commitment`, default off)
Component: `execution/commitment`
Audience: contributors working on commitment / state-root computation

## Abstract

`ParallelPatriciaHashed` computes the state-commitment root across a pool of
worker goroutines while producing a root hash that is **byte-for-byte identical**
to the sequential `HexPatriciaHashed`. It partitions the touched-key set into
independent subtrees that workers fold concurrently, and reconciles the shared
upper trie at a small number of *split-points* via a fold-time barrier. It is one
of three implementations of the `commitment.Trie` interface; nothing in the
on-disk format, branch encoding, or root definition changes.

## Motivation

Commitment is a large fraction of block-execution time and is sequential in
`HexPatriciaHashed`: keys are processed in sorted order through a single 128-row
grid and folded bottom-up. On wide update sets the work below the first few
nibble levels is embarrassingly parallel — distinct subtrees share no state until
they meet near the root.

`ConcurrentPatriciaHashed` already exploits this, but only as a fixed 16-way
split at the top nibble and only when the trie is already wide there. This design
generalizes parallelism to **adaptive, multi-level, chained** split-points and a
worker pool sized to the host, working on any batch shape.

## The three trie variants

All three implement `commitment.Trie` and produce the same root; they differ only
in how the computation is scheduled.

| | `HexPatriciaHashed` | `ConcurrentPatriciaHashed` | `ParallelPatriciaHashed` |
| --- | --- | --- | --- |
| flag | (default) | `--experimental.concurrent-commitment` | `--experimental.parallel-commitment` |
| `Updates` mode | `ModeDirect` / `ModeUpdate` | `ModeDirect` + `sortPerNibble` | `ModeParallel` |
| parallel unit | none | one goroutine per top nibble (≤16 mounts) | worker pool (`NumCPU`) over leaf tasks |
| split granularity | none | fixed: 16-way at **depth 1** only | adaptive: any node with `subtrees≥2` and `subtreeCount≥MinSplitKeys`, at **any depth**, chained |
| merge mechanism | single bottom-up fold | each mount folds, takes `rootMu`, writes `root.grid[0][nib]`; root folded last | fold-time barrier at each split-point; last finisher rebuilds the row and folds up the chain |
| root-update sync | — | `rootMu` mutex | atomic `arrived` counter + staged `pendingRoot` |
| branch writes | inline `PutBranch` | inline (per mount) | deferred per worker, merged + applied once (or handed to the caller under parallel-apply/fork-validation) |
| applicability | always | only when the top of the trie is a wide branch (`CanDoConcurrentNext` heuristic, re-evaluated each block; first block always sequential) | any shape, except multiple-leaf-tasks-with-no-split (rejected) |
| depth of parallelism | — | shallow (depth 1) | deep (splits inside storage subtrees) |
| extra inputs | — | unfolded root + 16 mounted sub-tries | prefix trie (carries keys + `plainKey`s) + split-point DB pre-population |

### How `ConcurrentPatriciaHashed` works (for contrast)

`ParallelHashSort` unfolds the root to depth 1 and mounts 16 sub-tries (one per
top nibble) onto it. An errgroup (limit 16) then, per nibble, scans that nibble's
ETL collector, applies its keys into the mount via `followAndUpdate`, and
`foldNibble` folds the mount and — under `rootMu` — writes the folded cell into
`root.grid[0][nib]`. After all nibbles finish, the root is folded once. Because
the only split is the depth-1 row, it parallelizes exactly when the top node is a
16-way branch; `CanDoConcurrentNext` checks this after each block and toggles the
mode for the next one (so the first block of a session runs sequentially).

`ParallelPatriciaHashed` removes both restrictions: it discovers split-points
adaptively from the touched keys (so it parallelizes on the first batch and deep
inside storage subtrees, not just at depth 1), and it scales to a worker pool
rather than a hard cap of 16.

## Background: the sequential fold

`HexPatriciaHashed` keeps a `grid[128][16]cell` (one row per nibble depth) and a
`currentKey`. Per sorted batch it **unfolds** down to the next key (loading
branches from the `PatriciaContext`), **applies** the update, and **folds**
completed rows back up — hashing each branch and writing it via
`PatriciaContext.PutBranch`. The final fold to row 0 yields the root. Critically,
a branch hash mixes **every present nibble** of a branch, not only the touched
ones — the property the parallel design must preserve.

## Pipeline

| phase | when | what |
| --- | --- | --- |
| 1. Touch | `Updates.TouchPlainKey` | insert each hashed key into an in-memory path-compressed prefix trie, carrying the un-hashed `plainKey` on the terminating node (no ETL collectors) |
| 2. Prepare | start of `Process` (sequential) | DFS the prefix trie → emit split-points + leaf tasks; pre-populate each split-point's cells from its on-disk branch |
| 3. Dispatch | `Process` (concurrent) | each leaf task is handed to a pooled worker that DFS-walks its own trie subtree, reading `plainKey`s off terminating nodes — no per-nibble bucketing |
| 4. Fold + barrier | `Process` (concurrent) | each worker folds its subtree to its enclosing split-point, deposits a cell, and exits; the last sibling rebuilds the merged row and folds upward through the split-point chain; the topmost finisher publishes the root |
| 5. Commit | end of `Process` | merge every worker's deferred branch updates and apply once; promote the staged root only after the apply succeeds |

### Data structures

- **`prefixTrie`** (`prefix_trie.go`) — path-compressed nibble trie of the touched
  keys, bump-allocated from a slab `prefixArena`. Each `prefixNode` holds a
  compressed `ext`, a child `bitmap`, a dense `children` slice, `subtreeCount`
  (incremented on every traversal *including* the terminating node — so it doubles
  as a terminator detector), and a `plainKey` set only on terminating nodes. The
  `plainKey` bytes live once in a per-batch `plainKeyArena`; `Insert` carries a
  terminator's `plainKey` to the correct child through path-compression splits.
- **`splitPoint`** (`parallel_update.go`) — a barrier at a prefix. Holds
  `cells [16]cellEncodeData`, an atomic `arrived`, `touchedBitmap`, `dbBitmap`,
  and `branchBefore`. `cells` matches `DeferredBranchUpdate.cells` so DB-loaded
  cells copy in by assignment.
- **`leafTask`** — a touched-key subtree rooted at `node` (a `*prefixNode`) under
  `prefix`, with `keyCount` for scheduling. The worker DFS-walks `node` to
  enumerate its keys. The enclosing split-point chain is **not** stored; a worker
  discovers it by walking `splitMap` longest-prefix-first during the fold.
- **`parallelUpdate`** — the per-batch state: prefix trie, freeze-time `splitMap`
  (+ a `splitPoints` slice for iteration), `leafQueue`, and a mutex-guarded
  `deferredCombined`.

### Phase 2 — Prepare (`parallelUpdate.Prepare`)

DFS classifies each node:

- **Split-point** iff `subtrees ≥ 2` **and** `subtreeCount ≥ MinSplitKeys` **and**
  the node hosts no terminator key. For each, Prepare loads the on-disk branch via
  `ctx.Branch`, seeds `sp.cells` for every nibble present on disk, sets
  `dbBitmap`/`branchBefore`, and stores `arrived = subtrees`.
- **Leaf task** otherwise: the node's whole subtree collapses into one task.

The root is special-cased: even when it is not a split-point, Prepare descends one
level so each leaf task roots a distinct top-nibble subtree (rather than one task
covering the whole trie). `leafQueue` is finally sorted by descending `keyCount`
(heaviest tasks dispatch first).

**Adaptive coarsening.** Before the DFS, `Process` raises the effective threshold
for large batches to `max(MinSplitKeys, touchedKeys / (numWorkers×4))`. A fixed
`MinSplitKeys` shatters a wide, uniform trie into tens of thousands of leaf tasks,
each acquiring a ~1MB worker grid; at that volume the pool stops amortizing (a
1M-flat batch otherwise allocates 16–32 GB and runs slower than sequential). The
threshold only ever rises, so small and clustered batches keep `MinSplitKeys`.

### Phase 3 — Dispatch (`runLeafTask`)

Each leaf task owns a disjoint trie subtree, so dispatch is a flat errgroup over
`leafQueue` capped at `numWorkers` (heaviest first). A pooled `*HexPatriciaHashed`
worker `dfsSubtree`-walks its `leafTask.node` in nibble order, reconstructing each
hashed key from the path and reading the `plainKey` off the terminating node, and
calls `followAndUpdate` — then folds back through the barrier on the same
goroutine. There is no per-nibble bucketing, no routing scan, and no separate
fold semaphore: build and fold for a task run on one worker, so the errgroup limit
alone bounds both concurrency and MDBX reader slots to `numWorkers`. Each worker
takes its own context for the whole build+fold. A node emits its own key *before*
descending, so an account at depth 64 precedes its storage keys (sorted order),
which the fold state machine requires.

### Phase 4 — Fold with barrier (`foldDrainWithBarrier`)

1. **Fold to the split-point depth.** Find the deepest enclosing split-point and
   fold until the deepest active row settles at its child depth
   (`len(sp.prefix)+1`); with no enclosing split-point, fold to `activeRows==0`.
   Folding past the child depth would absorb the shared parent branch into the
   worker's root and clobber siblings' deposits.
2. **Deposit** the worker's contribution into `sp.cells[childNibble]`, stripping
   the leading nibbles implied by the slot's position.
3. **Barrier.** `sp.arrived.Add(-1)`; if `>0` the worker exits. The worker whose
   decrement returns `0` is the unique **last finisher**.
4. **Rebuild + climb.** The last finisher rebuilds the split-point's row from all
   deposits plus DB pre-population, folds it once into the merged cell, and
   repeats from step 2 against the next enclosing split-point. The topmost
   finisher publishes the root.

The atomic decrement is the happens-before edge: the last finisher observes every
sibling deposit, and arrival order cannot deadlock.

### Phase 5 — Commit

Workers accumulate `DeferredBranchUpdate`s instead of writing branches. After all
workers finish, `applyDeferredUpdates` merges every list and applies it through a
single context (`ApplyDeferredBranchUpdates`), so no two goroutines ever call
`PutBranch` concurrently. The published root is **staged** on `pendingRoot` and
promoted to `rootHash` (and mirrored into the template's root cell + root flags)
only after the deferred apply succeeds — a failed apply never surfaces an
unpersisted root. Under `SetLeaveDeferredForCaller` (parallel apply / fork
validation) the inline apply is skipped: the merged list is handed to the caller
via `TakeDeferredUpdates` and the root is promoted directly, since the root hash
comes from the in-memory fold and does not depend on when the branches land.

## Correctness

The cardinal rule, asserted by every end-to-end test via
`assertEquivalentRoot[Workers]`: the parallel root must equal the sequential root
byte-for-byte. Supporting invariants:

- **Untouched-nibble pre-population.** A branch hash mixes all present nibbles;
  workers write only touched slots, so Prepare seeds the rest from disk.
- **`arrived = subtrees`, not `subtrees − 1`.** With `subtrees − 1`, both workers of a
  2-way fork pass the barrier and two roots get published.
- **No split-point at a terminator node.** `splitPoint.cells` is indexed by child
  nibble with no terminator slot; splitting at a node where a key ends (e.g. an
  account at depth 64 above its storage) would drop that key from the branch
  hash. Detected as `subtreeCount > Σ child.subtreeCount`.
- **Root leaf tasks carry a non-empty prefix** so each roots a distinct top-nibble subtree.
- **`plainKey` follows the split.** `prefixTrie.Insert` must move a terminator's
  `plainKey` to the correct child through path-compression splits; a misroute is a
  wrong DB read and a diverged root.
- **Single deferred apply** — all `PutBranch` writes happen from one goroutine.

`go test -race ./execution/commitment/...` exercises the concurrency;
`TestVerifyParallel_RandomBatches` runs ~1100 randomised batches and
`FuzzParallelEquivalence` fuzzes parallel-vs-sequential equality.

## Concurrency & resource model

- A single errgroup over `leafQueue`, limit = `numWorkers`. Each task runs
  build (subtree DFS + `followAndUpdate`) and fold on one goroutine, so the limit
  alone caps concurrency — there is no per-nibble bucketing and no `foldSem`.
- Each worker holds one context for its whole build+fold, so simultaneous
  contexts — and thus MDBX reader slots — track `numWorkers`, not the task count.
- The frozen post-`Prepare` trie is read-only and workers walk disjoint subtrees,
  so concurrent `plainKey`/structure reads are race-free (the split-point ban on
  terminator nodes keeps shared ancestors off the fold path).
- `deferredCombined` is the only shared-mutable slice during the parallel phase,
  guarded by `deferredMu`.

## Configuration

- `--experimental.parallel-commitment` selects `VariantParallelHexPatricia` and
  takes precedence over `--experimental.concurrent-commitment`
  (`execctx.PickTrieVariant`).
- `MinSplitKeys` (default 64) — minimum subtree size for a fork to become a
  split-point; smaller forks collapse to a leaf task because barrier coordination
  would dominate the saved work. `Process` coarsens it adaptively for large
  batches (see Phase 2) to bound the leaf-task count.
- `numWorkers` (default `runtime.NumCPU()`, override via `SetNumWorkers`).

## Performance

`Benchmark_Commitment_DirectVsParallel`, Apple M5 Max (18 cores), medians of
`-benchtime=10x -count=3`:

| workload | direct | parallel @8w | speedup |
| --- | ---: | ---: | ---: |
| flat 100K accounts | 122 ms | 73 ms | 1.67× |
| storage-heavy 500K (1K acct × 499 slots) | 714 ms | 362 ms | 1.97× |

Throughput peaks near 8 workers and regresses beyond it (1.45×/1.76× at 18
workers) — bounded by memory bandwidth, not lock contention. On high-core hosts a
tuned worker count beats raw `NumCPU()`; worker-count autotuning is future work.
These numbers are for hand inspection, not a CI gate (too noisy for a "must be
faster" assertion).

The per-leafTask DFS dispatch decoupled build parallelism from the top-nibble
count (the previous per-nibble scan capped build at ≤16 and serialized each
bucket). On the 18-core M5 Max this is roughly perf-neutral (geomean ~-2%, up to
-6% at w8 on bucket-imbalanced batches, memory parity) because the workload
saturates bandwidth at ~8 workers; the benchmark's shared-lock `MockState` also
under-reports the parallel win versus production's independent MDBX readers. The
larger gains are expected on higher-bandwidth / >16-core hosts.

On heavier loads the adaptive threshold is load-bearing: without it a 1M flat
batch over-splits and allocates 16–32 GB (parallel slower than sequential, and
worse with more workers); with it the same batch runs ~1.6× at sub-GB memory.
Storage-heavy is insensitive to the threshold — its keys already cluster under a
bounded set of account prefixes.

## Limitations & future work

- **Multiple leaf tasks, no split-point.** When Prepare emits more than one leaf
  task but zero split-points, independent workers would fold to root with only
  their own subtree touched and disagree; `Process` returns an error rather than a
  wrong root. A synthetic root barrier (or auto-fallback to sequential) would lift
  this.
- **Witness generation** still uses sequential `HexPatriciaHashed`; a
  hashed-only `TouchHashedKey` leaves a `plainKey`-less terminator the parallel
  fold rejects, so this path is not wired for the parallel trie.
- **Worker-count autotuning** (see Performance).

### Parallel block apply / fork validation (supported)

`deferCommitmentUpdates` now works with the parallel trie. Two pieces:

- **ModeParallel buffer.** The parallel-exec commitment calculator
  (`stagedsync/committer.go`) keeps its `Updates` buffer in `ModeParallel` instead
  of forcing `ModeUpdate`, so `Process` accepts it.
- **Caller-deferred branches.** `Process` honors `SetLeaveDeferredForCaller`:
  it stages the merged `deferredCombined` for the caller (`TakeDeferredUpdates`)
  rather than applying inline, so branch writes land in the correct block's
  changeset via the pending-update flush.

Leaf **values** come from the calculator's as-of state reader
(`sd.GetAsOf(plainKey, lastTxNum+1)`, which sees `sd.mem`) rather than the
`ModeUpdate` btree, since `ModeParallel` carries only keys. Correctness of that
substitution is asserted at runtime by the block-root check (`ErrWrongTrieRoot`).

## Source map

| file | contents |
| --- | --- |
| `execution/commitment/parallel_patricia_hashed.go` | `ParallelPatriciaHashed`, `Process`, `runLeafTask`/`dfsSubtree` dispatch, fold/barrier, deposit, deferred apply/hand-off |
| `execution/commitment/parallel_update.go` | `parallelUpdate`, `plainKeyArena`, `Prepare` DFS, split-point emission, DB pre-population |
| `execution/commitment/prefix_trie.go` | path-compressed prefix trie + slab arena; `Insert` carries `plainKey` on terminating nodes |
| `execution/commitment/commitment.go` | `Updates` (`ModeParallel` carries keys in the prefix trie), `InitializeTrieAndUpdates` |
| `execution/commitment/commitmentdb/commitment_context.go` | wires `ModeParallel` + caller-deferred updates into `ComputeCommitment` |
| `execution/stagedsync/committer.go` | parallel-exec commitment calculator; keeps the `ModeParallel` buffer, feeds values via the as-of reader |
| `execution/commitment/hex_concurrent_patricia_hashed.go` | `ConcurrentPatriciaHashed` (the depth-1 variant compared above) |
