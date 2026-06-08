# ParallelPatriciaHashed — Specification

| | |
| --- | --- |
| Component | `execution/commitment` |
| Stability | Experimental — `--experimental.parallel-commitment`, default off |
| Implements | `commitment.Trie` |
| Audience | Contributors to commitment / state-root computation |

The key words MUST, MUST NOT, SHOULD, and MAY are used as defined in RFC 2119.
Sections marked *(informative)* are non-normative.

## 1. Scope and cardinal requirement

`ParallelPatriciaHashed` computes the state-commitment root across a pool of
worker goroutines. It is one of three `commitment.Trie` implementations and
changes nothing in the on-disk format, branch encoding, or root definition.

**R1 (cardinal).** For every input, the root hash produced by
`ParallelPatriciaHashed.Process` MUST be byte-for-byte identical to the root
`HexPatriciaHashed.Process` produces for the same input. Every other requirement
in this document exists to uphold R1; any optimization that violates R1 is a
defect, not a trade-off.

It partitions the touched-key set into disjoint subtrees folded concurrently by
workers, and reconciles the shared upper trie at a bounded set of *split-points*
through a fold-time barrier.

## 2. Preliminaries *(informative)*

`HexPatriciaHashed` keeps a `grid[128][16]cell` (one row per nibble depth) and a
`currentKey`. Per sorted batch it unfolds down to the next key (loading branches
from the `PatriciaContext`), applies the update, and folds completed rows upward,
hashing each branch and writing it via `PatriciaContext.PutBranch`; the final fold
to row 0 yields the root. A branch hash mixes **every present nibble** of the
branch, not only the touched ones — the property §4 must preserve under
partitioning.

## 3. Data structures

Field lists are normative: the named fields and their stated invariants define the
contract. Files are given for reference.

### 3.1 `prefixNode`, `prefixTrie`, `plainKeyArena` (`prefix_trie.go`, `parallel_update.go`)

A path-compressed nibble trie of the touched hashed keys, bump-allocated from a
slab arena.

`prefixNode`:

| field | type | meaning / invariant |
| --- | --- | --- |
| `ext` | `[]byte` | compressed nibble path from the parent (each byte ∈ 0x00..0x0F) |
| `children` | `[]*prefixNode` | dense; `len == popcount(bitmap)`; ordered by nibble ascending |
| `bitmap` | `uint16` | present child nibbles |
| `subtreeCount` | `uint32` | incremented on every traversal **including** the terminating node; `subtreeCount > Σ children.subtreeCount` ⟺ a key terminates at this node |
| `plainKey` | `[]byte` | the un-hashed key terminating here; non-nil **iff** a key terminates at this node and was supplied with a plain key; nil otherwise |

- `plainKey` bytes are owned by a per-batch `plainKeyArena` (chunked; a full chunk
  is replaced, not grown, so issued sub-slices remain stable for the trie's life).
- **`Insert(hashedKey, plainKey)`** MUST place `plainKey` on the node where
  `hashedKey` terminates, including across the two path-compression *split* cases:
  when a node is split, a `plainKey` already present on it belongs to a key that
  now terminates strictly below the split point and MUST be moved to the retained
  child (`oldChild`); the split parent retains a `plainKey` only if the incoming
  key terminates exactly at it. (Invariant I5.)

### 3.2 `splitPoint` (`parallel_update.go`)

A fold-time barrier at a trie prefix.

| field | type | meaning / invariant |
| --- | --- | --- |
| `prefix` | `[]byte` | the nibble prefix at which workers converge |
| `cells` | `[16]cellEncodeData` | child-cell slots; layout matches `DeferredBranchUpdate.cells` so DB-loaded cells assign in directly |
| `arrived` | `atomic.Int32` | initialised to `popcount(touchedBitmap)`; each depositor decrements; the decrement returning 0 identifies the unique last finisher |
| `touchedBitmap` | `uint16` | child nibbles touched in this batch |
| `dbBitmap` | `uint16` | child nibbles present on disk (from `ctx.Branch`) |
| `branchBefore` | `bool` | whether an on-disk branch existed at `prefix` |

### 3.3 `leafTask` (`parallel_update.go`)

One unit of parallel work: the touched-key subtree rooted at `node`.

| field | type | meaning |
| --- | --- | --- |
| `prefix` | `[]byte` | the from-root nibble path to `node` (non-empty — I7) |
| `node` | `*prefixNode` | subtree root the worker DFS-walks to enumerate its keys |
| `keyCount` | `uint32` | `node.subtreeCount`, used to schedule heaviest tasks first |

The enclosing split-point chain is not stored; a worker discovers it by walking
`splitMap` longest-prefix-first during the fold.

### 3.4 `parallelUpdate` (`parallel_update.go`)

Per-batch state: the `prefixTrie`, the `plainKeyArena`, the freeze-time `splitMap`
(plus a `splitPoints` slice for iteration), the `leafQueue`, and a
mutex-guarded `deferredCombined`. `Insert` calls MUST be serialized by the caller;
`splitMap`/`leafQueue` are written only by `Prepare` and read concurrently
afterward; `deferredCombined` is the sole shared-mutable slice during the parallel
phase and is guarded by `deferredMu`.

### 3.5 `ParallelPatriciaHashed` (`parallel_patricia_hashed.go`)

Holds a configuration `template *HexPatriciaHashed` (exposes ctx/cache/metrics/
trace only — never live root state during `Process`), a `sync.Pool` of worker
tries, a `TrieContextFactory`, `numWorkers`, `minSplitKeys`, the published
`rootHash`, a staged `pendingRoot`, and — for the deferred path — a
`leaveDeferredForCaller` flag with a `deferredForCaller` hand-off slice.

## 4. Pipeline

| phase | site | action |
| --- | --- | --- |
| 1. Touch | `Updates.TouchPlainKey` (ModeParallel) | insert each hashed key into the prefix trie, carrying its `plainKey` on the terminating node; no ETL collectors are used |
| 2. Prepare | `Process`, sequential | DFS the prefix trie → emit split-points and leaf tasks; pre-populate each split-point's cells from its on-disk branch |
| 3. Dispatch + fold | `Process`, concurrent | a flat errgroup over leaf tasks; each worker DFS-walks its subtree, applies keys, then folds back through the barrier |
| 4. Barrier | within fold | at each split-point the last finisher rebuilds the merged row and folds upward; the topmost finisher publishes the root |
| 5. Commit | `Process` end | apply (or hand off) the merged deferred branch updates; promote the staged root |

### 4.1 Phase 2 — Prepare (`parallelUpdate.Prepare`)

The DFS classifies each node:

- **Split-point** — iff `popcount(bitmap) ≥ 2` AND `subtreeCount ≥ threshold` AND
  the node hosts no terminator key (I4). For each, Prepare loads the on-disk branch
  via `ctx.Branch`, seeds `cells`/`dbBitmap`/`branchBefore`, and sets
  `arrived = popcount(touchedBitmap)` (I3).
- **Leaf task** — otherwise: the node's whole subtree collapses into one task.

The root is special-cased: when it does not qualify as a split-point, Prepare still
descends one level so each emitted leaf task roots a distinct top-nibble subtree
(I7) rather than one task covering the whole trie. `leafQueue` is then sorted by
descending `keyCount`.

**Adaptive threshold.** Before the DFS, `Process` sets
`threshold = max(MinSplitKeys, touchedKeys / (numWorkers × 4))`; it only ever
raises `MinSplitKeys`. A fixed `MinSplitKeys` shatters a wide uniform trie into
tens of thousands of leaf tasks, each acquiring a worker grid, until the pool stops
amortizing (an unbounded batch otherwise allocates tens of GB). Clustered batches,
whose keys already group under a bounded set of prefixes, keep `MinSplitKeys`.

### 4.2 Phase 3 — Dispatch and fold (`runLeafTask`, `dfsSubtree`)

Dispatch is a single errgroup over `leafQueue`, limit `numWorkers`, heaviest task
first. For each task a pooled `*HexPatriciaHashed` worker, bound to one
factory-provided `PatriciaContext` and with deferred branch writes enabled:

1. **Build.** `dfsSubtree(node, prefix)` walks the subtree in nibble-ascending
   order. At each terminating node it reconstructs the full hashed key from the
   accumulated path, reads the `plainKey` off the node, and calls
   `followAndUpdate(hashedKey, plainKey, nil)`; the value is read from the
   `PatriciaContext`. A node MUST emit its own key **before** descending to its
   children, so an account at depth 64 precedes its storage keys — the sorted order
   the fold state machine requires (I6). A terminating node with a nil `plainKey`
   and no children is unsupported and MUST raise an error rather than be skipped.
2. **Fold.** The same goroutine then folds the worker through the barrier (§4.3).

Build and fold for a task run on one goroutine; there is no per-nibble bucketing,
no routing scan, and no separate fold semaphore (contrast §10).

### 4.3 Phase 4 — Fold with barrier (`foldDrainWithBarrier`)

1. Fold the worker's grid down to the deepest enclosing split-point's child depth
   (`len(sp.prefix)+1`), or to `activeRows == 0` if none encloses it. Folding past
   the child depth would absorb the shared parent branch into the worker's root and
   clobber siblings' deposits.
2. Deposit the worker's contribution into `sp.cells[childNibble]`, stripping the
   leading nibbles implied by the slot's position.
3. `sp.arrived.Add(-1)`. A worker whose decrement returns `> 0` exits. The worker
   whose decrement returns `0` is the unique last finisher.
4. The last finisher rebuilds the split-point's row from all deposits plus DB
   pre-population, folds it once into the merged cell, and repeats from step 2
   against the next enclosing split-point. The topmost finisher publishes the root.

The atomic decrement is the sole happens-before edge: it guarantees the last
finisher observes every sibling deposit, and arrival order cannot deadlock.

### 4.4 Phase 5 — Commit and root publication

Workers accumulate `DeferredBranchUpdate`s rather than writing branches. After all
workers finish:

- **Default (inline).** `applyDeferredUpdates` merges every list and applies it
  through a single context (`ApplyDeferredBranchUpdates`), so no two goroutines call
  `PutBranch` concurrently. The staged `pendingRoot` is promoted to `rootHash`
  (and mirrored into the template's root cell and root flags) **only after** the
  apply succeeds — a failed apply MUST NOT surface an unpersisted root.
- **Caller-deferred.** Under `SetLeaveDeferredForCaller(true)` the inline apply is
  skipped; the merged list is handed to the caller via `TakeDeferredUpdates` and
  the root is promoted directly. This is sound because the root hash is determined
  by the in-memory fold and is independent of when the branches are persisted
  (§6.3).

## 5. Invariants

Each is normative and individually testable; the equivalence harness (§8) is the
primary enforcement of I1.

- **I1 — Equal root.** The published root equals the sequential root for every
  input (R1).
- **I2 — Untouched-nibble pre-population.** Because a branch hash mixes all present
  nibbles, Prepare MUST seed each split-point's untouched, on-disk child cells from
  `ctx.Branch`; workers write only touched slots.
- **I3 — `arrived = popcount(touchedBitmap)`**, not `−1`. With `−1`, both workers of
  a 2-way fork pass the barrier and two roots are published.
- **I4 — No split-point at a terminator node.** `splitPoint.cells` has no terminator
  slot; splitting where a key ends (e.g. an account above its storage) would drop
  that key from the branch hash. Detected via `subtreeCount > Σ child.subtreeCount`.
- **I5 — `plainKey` follows the split.** `prefixTrie.Insert` MUST route a
  terminator's `plainKey` to the correct node across path-compression splits (§3.1).
  A misroute is a wrong DB read and a diverged root.
- **I6 — Sorted emission.** Within a leaf task, keys MUST be presented to
  `followAndUpdate` in ascending hashed-key order, a terminating node before its
  descendants; the DFS yields this because children are nibble-ordered.
- **I7 — Non-empty leaf-task prefix.** Every leaf task carries a non-empty prefix
  rooting a distinct subtree, so enclosing-split-point lookup is well-defined.
- **I8 — Single branch writer.** All `PutBranch` calls issue from one goroutine
  (inline apply) or are handed to the caller; workers own disjoint subtrees hence
  disjoint branch prefixes, and each split-point has a single producer.
- **I9 — Frozen-trie read safety.** After `Prepare` returns, the prefix trie and
  `plainKeyArena` are immutable; workers walk disjoint subtrees, so concurrent
  structure/`plainKey` reads are race-free. I4 keeps shared ancestor terminators off
  the fold path.

## 6. Integration contract

### 6.1 Updates mode

`Process` MUST be called with `updates.mode == ModeParallel` and a populated
`updates.parallel`; it rejects any other mode. `InitializeTrieAndUpdates` forces
`ModeParallel` for `VariantParallelHexPatricia`.

### 6.2 Value source

ModeParallel carries keys only; per-key account/storage values are read from the
`PatriciaContext` during `followAndUpdate`, not from the `Updates` buffer. Each
worker takes its own context from the `TrieContextFactory`, so DB reads run
concurrently.

### 6.3 Parallel block apply / fork validation

The parallel-exec commitment calculator (`stagedsync/committer.go`) drives this
path. Two conditions make it compatible with the parallel trie:

1. **ModeParallel buffer.** The calculator MUST keep its `Updates` buffer in
   `ModeParallel` (it does not downgrade to `ModeUpdate`), so `Process` accepts it.
2. **Caller-deferred branches.** With `deferCommitmentUpdates` set, `Process` runs
   under `SetLeaveDeferredForCaller(true)`; the merged branch updates are stashed in
   the pending update and flushed into the correct block's changeset, not applied at
   the current txNum.

Because the ModeParallel buffer holds no values, leaf values are served by the
calculator's as-of state reader (`sd.GetAsOf(plainKey, lastTxNum+1)`, which
consults `sd.mem`). The reader MUST therefore be installed before `Process`. The
substitution of the as-of reader for the `ModeUpdate` value btree is validated at
runtime by the block-root check (`ErrWrongTrieRoot`).

## 7. Configuration

| parameter | default | effect |
| --- | --- | --- |
| `--experimental.parallel-commitment` | off | selects `VariantParallelHexPatricia`; takes precedence over `--experimental.concurrent-commitment` (`execctx.PickTrieVariant`) |
| `MinSplitKeys` | 64 | minimum subtree size for a fork to become a split-point; raised adaptively per batch (§4.1) |
| `numWorkers` | `runtime.NumCPU()` | worker-pool size and errgroup limit; override via `SetNumWorkers` |

## 8. Failure modes

| condition | behaviour |
| --- | --- |
| empty update set | return the template's existing root (matches the sequential no-op) |
| ≥ 2 leaf tasks, 0 split-points | return an error; independent workers would each fold to root with only their subtree touched and disagree |
| terminating node with nil `plainKey` and no children | return an error (only reachable via a hashed-only `TouchHashedKey`; that path is not wired for the parallel trie) |
| deferred apply failure (inline path) | discard the staged root; never surface an unpersisted root |
| worker error mid-fold | cancel the group; return pooled deferred entries and clear `pendingRoot` |

## 9. Validation

- `go test -race ./execution/commitment/...` — exercises the concurrency.
- `TestVerifyParallel_RandomBatches` — ~1100 randomized batches, parallel vs
  sequential root equality.
- `FuzzParallelEquivalence` — fuzzes parallel-vs-sequential equality.
- `ErrWrongTrieRoot` — at block-apply time, the computed root is compared to the
  block header; the value-source substitution of §6.3 is proven here, not by the
  unit harness.

## 10. Relationship to the other variants *(informative)*

All three implement `commitment.Trie` and produce the same root; they differ only
in scheduling.

| | `HexPatriciaHashed` | `ConcurrentPatriciaHashed` | `ParallelPatriciaHashed` |
| --- | --- | --- | --- |
| flag | (default) | `--experimental.concurrent-commitment` | `--experimental.parallel-commitment` |
| `Updates` mode | `ModeDirect` / `ModeUpdate` | `ModeDirect` + `sortPerNibble` | `ModeParallel` |
| parallel unit | none | one goroutine per top nibble (≤16) | worker pool over leaf tasks |
| split granularity | none | fixed 16-way at depth 1 | adaptive: any node with `subtrees ≥ 2`, `subtreeCount ≥ threshold`, any depth, chained |
| merge | single bottom-up fold | per-mount fold under `rootMu` | fold-time barrier per split-point |
| branch writes | inline | inline (per mount) | deferred, applied once or handed to the caller |
| key delivery | one sorted stream | 16 per-nibble ETL collectors | prefix trie carrying `plainKey`s |
| applicability | always | only when the top node is a wide branch (`CanDoConcurrentNext`) | any shape except ≥2-tasks-with-no-split |

`ConcurrentPatriciaHashed` is the only consumer of the 16 per-nibble ETL
collectors; ModeParallel does not allocate them.

A fourth variant, `StreamingCommitter` (`--experimental.streaming-commitment` →
`VariantStreamingHexPatricia`), layers on this one rather than replacing it: it
reuses the same prefix trie, mount/fold engine, and `concurrentStorageRoot`, and
upholds R1 identically. It differs only in *when* the fold runs — touched keys are
re-folded per top-nibble split in a background worker pool overlapping execution,
so `Process` collapses to a merge of already-folded split cells. Folds are
stateless (re-folded from the prefix-trie key set, never a persistent per-split hph
mutated by touches — that would break the monotonic `followAndUpdate` contract). It
uses the `streaming` flag on `Updates` (not a new `Mode`) and is not layered with
the `ERIGON_CMT_MOUNT`/`ERIGON_CMT_DEEP` env gates. Its only real win is on a live
node (hiding unfold DB-read latency under execution); the in-memory benchmark cannot
show it. See `execution/commitment/streaming_commitment.go`.

## 11. Performance characteristics *(informative)*

Throughput on the benchmark corpus peaks near 8 workers and is bounded by memory
bandwidth, not lock contention; worker-count autotuning is future work. The
per-leaf-task DFS dispatch decouples build-phase parallelism from the top-nibble
count (the prior per-nibble scan capped build at ≤16 and serialized each bucket),
which helps most on bucket-imbalanced batches and on hosts with more cores or
bandwidth than the 18-core reference. The benchmark `MockState` serializes reads on
a shared lock and therefore under-reports the parallel speedup relative to
production's independent per-worker MDBX readers. These figures are for inspection,
not a CI gate.

## 12. Source map

| file | contents |
| --- | --- |
| `execution/commitment/parallel_patricia_hashed.go` | `ParallelPatriciaHashed`, `Process`, `runLeafTask`/`dfsSubtree`, fold/barrier, deposit, deferred apply and hand-off |
| `execution/commitment/parallel_update.go` | `parallelUpdate`, `plainKeyArena`, `Prepare` DFS, split-point emission, DB pre-population |
| `execution/commitment/prefix_trie.go` | path-compressed prefix trie + slab arena; `Insert` `plainKey` placement |
| `execution/commitment/commitment.go` | `Updates` (ModeParallel carries keys in the prefix trie), `InitializeTrieAndUpdates` |
| `execution/commitment/commitmentdb/commitment_context.go` | wires ModeParallel and caller-deferred updates into `ComputeCommitment` |
| `execution/stagedsync/committer.go` | parallel-exec commitment calculator; keeps the ModeParallel buffer, serves values via the as-of reader |
| `execution/commitment/hex_concurrent_patricia_hashed.go` | `ConcurrentPatriciaHashed`, for contrast |
| `execution/commitment/streaming_commitment.go` | `StreamingCommitter`, the prepare-on-touch variant layered on this one |
