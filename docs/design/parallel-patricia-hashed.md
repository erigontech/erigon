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

It walks the touched-key prefix trie once into a **fold DAG**, then folds that DAG
with a **single frontier-scheduled worker pool** and re-folds the root row on the
main goroutine. A subtree of at most `K` keys is a **leaf task** — one worker
mounts at its prefix and replays its keys serially; a larger subtree is a **merge
task** — a worker seeds its row from the on-disk `Branch(prefix)` and stitches its
already-folded child cells. `K` is box-adaptive (§7). A worker never blocks holding
a slot while waiting on a child: each child's completion decrements its parent's
pending counter and the pool schedules the parent at zero, so utilization stays
near-full until the DAG collapses onto the root spine.

There is no separate whale/big-storage special case: an account with a large
storage subtree is just a subtree with more than `K` keys, and its storage folds in
parallel through ordinary merge tasks. The one structural special case is the
depth-64 account↔storage seam (§4.1.1): an account leaf **depends on** a storage-root
subtask, so storage subtrees are leaf-most in the DAG and drain first —
storage-first ordering falls out of the dependency for free.

## 2. Preliminaries *(informative)*

`HexPatriciaHashed` keeps a `grid[128][16]cell` (one row per nibble depth) and a
`currentKey`. Account keys occupy depths 0–64 (the leaf carries the storage root);
storage keys continue to depths 64–128. The same unfold/apply/fold codepath drives
both — there is no separate storage trie. Per sorted batch it unfolds down to the
next key (loading branches
from the `PatriciaContext`), applies the update, and folds completed rows upward,
hashing each branch and writing it via `PatriciaContext.PutBranch`; the final fold
to row 0 yields the root. A branch hash mixes **every present nibble** of the
branch, not only the touched ones — the property §4 must preserve under
partitioning, by seeding any row a fold collapses from the DB first: the shared
root row before dispatch, and every merge task's `Branch(prefix)` row before it
stitches its children (§4.1, including the depth-64 storage-root seam of §4.1.1).

## 3. Data structures

Field lists are normative: the named fields and their stated invariants define the
contract. Files are given for reference.

### 3.1 `prefixNode`, `prefixTrie`, `plainKeyArena` (`prefix_trie.go`, `parallel_update.go`)

A path-compressed nibble trie of the touched hashed keys, bump-allocated from a
slab arena. It is the sole carrier of the touched-key set and their plain keys;
the mount fold DFS-walks it directly.

`prefixNode`:

| field | type | meaning / invariant |
| --- | --- | --- |
| `ext` | `[]byte` | compressed nibble path from the parent (each byte ∈ 0x00..0x0F) |
| `children` | `[]*prefixNode` | dense; `len == popcount(bitmap)`; ordered by nibble ascending |
| `bitmap` | `uint16` | present child nibbles |
| `subtreeCount` | `uint32` | incremented on every traversal **including** the terminating node; `subtreeCount > Σ children.subtreeCount` ⟺ a key terminates at this node |
| `plainKey` | `[]byte` | the un-hashed key terminating here; non-nil **iff** a key terminates at this node and was supplied with a plain key; nil otherwise |
| `update` | `*Update` | the carried per-key value; nil ⟹ the fold re-reads from `ctx` |

- `plainKey` bytes are owned by a per-batch `plainKeyArena` (chunked; a full chunk
  is replaced, not grown, so issued sub-slices remain stable for the trie's life).
- **`Insert(hashedKey, plainKey)`** MUST place `plainKey` on the node where
  `hashedKey` terminates, including across the two path-compression *split* cases:
  when a node is split, a `plainKey` already present on it belongs to a key that
  now terminates strictly below the split point and MUST be moved to the retained
  child (`oldChild`); the split parent retains a `plainKey` only if the incoming
  key terminates exactly at it. (Invariant I3.)

### 3.2 `parallelUpdate` (`parallel_update.go`)

Per-batch state: the `prefixTrie`, the `plainKeyArena`, and a mutex-guarded
`deferredCombined` slice. `Insert` calls MUST be serialized by the caller;
`deferredCombined` is the sole shared-mutable slice during the parallel phase and
is guarded by `deferredMu` (`appendDeferred`).

### 3.3 `ParallelPatriciaHashed` (`parallel_patricia_hashed.go`)

Holds a configuration/base `template *HexPatriciaHashed`, a `sync.Pool` of worker
tries, a `TrieContextFactory`, the `cfg TrieConfig` and `accountKeyLen` used to mint
pooled workers, `numWorkers`, the published `rootHash`, and — for the deferred path
— a `leaveDeferredForCaller` flag with a `deferredForCaller` hand-off slice. An
optional `streaming *StreamingCommitter`: when set, `Process` delegates to it
(`processStreaming`, §10) and the mount path below is not used. The `template`
doubles as the **finale base** during `Process`: its row 0 is seeded from the root
branch, the top-nibble tasks' folded cells are stitched into it, and it folds the
merged root. (Outside `Process` it exposes ctx/cache/metrics/trace configuration only.)

## 4. Pipeline

| phase | site | action |
| --- | --- | --- |
| 1. Touch | `Updates.TouchPlainKey` (ModeParallel) | insert each hashed key into the prefix trie, carrying its `plainKey`/`update` on the terminating node; no ETL collectors are used |
| 2. Derive + fold DAG | `dispatchFrontier`, concurrent | seed the base root row; walk the prefix trie into a fold DAG (leaf/merge tasks + storage-root seams); fold the DAG with one frontier-scheduled worker pool; stitch the top-nibble cells into the base row and fold the merged root |
| 3. Commit | `Process` end | apply (or hand off) the merged deferred branch updates; publish the root |

### 4.1 Phase 2 — Derive and fold the DAG (`dispatchFrontier`, `deriveFoldFrontier`, `foldPool`)

Both the mount path (`processMounted`) and the streaming path (`StreamingCommitter.Process`)
call `foldPool.dispatchFrontier`; the streaming path additionally passes a
`reuseSchedulerCells` consumer (§10). The steps below are that shared core.

1. **Seed the base root row.** `unfoldRootWall` / `seedRootBase` seed `template`'s
   `grid[0]` from the on-disk root branch, populating **every present child nibble,
   touched or not** (I2), so untouched siblings survive the final fold and the branch
   hash mixes all nibbles. The base MUST have `root.ext == 0` (§8).
2. **Derive the DAG.** `deriveFoldFrontier(root, K, seedable)` walks the frozen prefix
   trie top-down once (`fold_dag.go`). Each node is classified:
   - `subtreeCount ≤ K` → **leaf task**: a serial replay of its ≤ `K` keys mounted on
     the parent merge's seeded base.
   - `subtreeCount > K` **and** `seedable(prefix)` → **merge task**: seed `Branch(prefix)`,
     stitch its child cells, fold to the mount wall. Its children are derived recursively.
   - `subtreeCount > K` but **not** `seedable(prefix)` → **demote**: the subtree is *not*
     an independent task; it collapses into the serial replay of its nearest
     branch-bearing ancestor (the seed-or-demote predicate, §5 I7 / §8).
   - depth-64 account/storage seam → an **account-leaf task depending on a storage-root
     subtask** (§4.1.1), never an ordinary account-plane merge.
   The root itself is always the serial finale (never demoted): it folds against the
   pre-seeded root wall. `K` is the box-adaptive boundary (§7). A merge (and an
   account leaf awaiting its storage root) starts with a pending counter equal to its
   child count.
3. **Seed merge bases.** Before dispatch, `foldPool.run` seeds each merge task's own
   pooled `*HexPatriciaHashed` from `Branch(prefix)` (`seedBaseAtPrefix`) under its own
   factory `PatriciaContext` with deferred branch writes enabled — the mount wall its
   children copy from and the row it stitches and folds.
4. **Fold with the frontier pool.** `dispatchFoldTasks` runs `numWorkers` goroutines
   pulling ready tasks off a channel. A task is ready when its pending counter is zero.
   - A **leaf task** mounts a pooled worker on the parent merge's base (`mountTo`),
     replays its key group in nibble-ascending order via `followAndUpdate` (a node emits
     its own key before descending, so an account at depth 64 precedes its storage keys —
     I4), and `foldMounted` returns the mount-wall cell.
   - A **merge task** stitches its already-folded child cells into its seeded base
     (`mergeChildrenAtPrefix`) and folds to the mount wall.
   Folding a task decrements its parent's pending counter; at zero the parent is enqueued.
   No task ever blocks holding a worker while waiting on a child, so the single pool is
   the whole concurrency budget and the dispatch is deadlock-free by construction. A
   fold error cancels the shared context and every worker drains out.
5. **Merge and fold root.** On the main goroutine, each top-nibble task's cell is
   stitched into `base.grid[0][nibble]` (`stitchSplitCells`, stripping the leading
   nibble a hash-only sub-branch carries in its extension) and the base folds row 0 to
   the root; `rootPresent` is set from `!root.IsEmpty()` so the encode/restore round-trip
   (§6.3) stays correct.

Each task owns its own seeded base (merge) or pooled worker (leaf) and its own factory
context; only the main goroutine mutates `template` after the pool drains. There is no
fold-time barrier and no cross-worker synchronisation — tasks own disjoint prefixes, so
their reads and deferred-branch accumulation never overlap.

### 4.1.1 Depth-64 account/storage seam (`fold_dag.go`, `foldStorageSeam`)

An account at depth 64 whose storage subtree exceeds `K` is **not** an ordinary
account-plane merge: its children are storage nibbles, not account siblings. The
derivation encodes this as an account-leaf task with a `storage` dependency edge:

1. **Split.** The account node becomes a **leaf task** (`planeAccount`) plus a
   **storage-root subtask** (`planeMerge`, `planeStorage`) rooted at the same depth-64
   prefix; the leaf's pending counter is 1 (the storage edge). The storage subtask's
   children are derived from the touched first-storage nibbles under ordinary K policy,
   so a whale's storage folds across many merge/leaf tasks, not one worker.
2. **Fold the storage root.** The storage subtask is a merge seeded from the account's
   on-disk storage-root branch (I2 at depth 64: untouched on-disk storage siblings MUST
   be present, or the storage root — hence the state root — diverges). `foldStorageSeam`
   aggregates the folded child cells (`aggregateMountedStorageRoot`) into the account's
   storage-root cell, collapsing a single survivor via `storageRootFromSingleChild`.
3. **Inject.** The collapsed root hash is stored on the parent, releasing its pending
   edge; when the account leaf folds it applies the account update and calls
   `setAccountStorageRoot`, which writes the hash into the account leaf (`cell.hash`,
   `hashLen = 32`). `computeCellHash` uses it as the storageRoot for an account whose
   storage cell was not streamed, so the leaf hashes identically to the serial path.

Because the account leaf depends on its storage-root subtask, storage subtrees are
leaf-most in the DAG and drain first — storage-first ordering is a property of the
dependency, not a scheduled phase.

### 4.2 Phase 3 — Commit and root publication

Tasks accumulate `DeferredBranchUpdate`s rather than writing branches. After the
fold:

- **Default (inline).** `applyDeferredUpdates` merges every list and applies it
  through a single context (`ApplyDeferredBranchUpdates`), so no two goroutines call
  `PutBranch` concurrently (I5). The root hash returned by the base fold is then
  published to `rootHash`.
- **Caller-deferred.** Under `SetLeaveDeferredForCaller(true)` the inline apply is
  skipped; the merged list is handed to the caller via `TakeDeferredUpdates` and
  the root is published directly. This is sound because the root hash is determined
  by the in-memory fold and is independent of when the branches are persisted
  (§6.3).

## 5. Invariants

Each is normative and individually testable; the equivalence harness (§9) is the
primary enforcement of I1.

- **I1 — Equal root.** The published root equals the sequential root for every
  input (R1).
- **I2 — Untouched-nibble preservation.** Because a branch hash mixes all present
  nibbles, every branch row a fold collapses MUST first be seeded from `ctx.Branch`
  so untouched on-disk siblings are present. This holds for every merge task
  (`seedBaseAtPrefix(Branch(prefix))` before it stitches its children), the shared
  root row before dispatch (`seedRootBase`), and each depth-64 storage-root seam
  (§4.1.1). Dropping any seed drops untouched siblings and diverges the root.
- **I3 — `plainKey` follows the split.** `prefixTrie.Insert` MUST route a
  terminator's `plainKey` to the correct node across path-compression splits (§3.1).
  A misroute is a wrong DB read and a diverged root.
- **I4 — Sorted emission.** Within a mount, keys MUST be presented to
  `followAndUpdate` in ascending hashed-key order, a terminating node before its
  descendants; the DFS yields this because children are nibble-ordered.
- **I5 — Single branch writer.** All `PutBranch` calls issue from one goroutine
  (inline apply) or are handed to the caller; tasks own disjoint subtree prefixes
  hence disjoint branch prefixes, and each prefix has a single producer.
- **I6 — Read safety.** Each task folds on its own pooled worker (leaf) or seeded
  base (merge) under its own factory context, and the tasks own disjoint prefixes of
  the frozen prefix trie; the merged base row is folded only on the main goroutine
  after the pool drains, so concurrent structure/`plainKey` reads and the final fold
  are race-free. The dispatch is deadlock-free by construction: no task holds a worker
  while waiting on a child (§4.1 step 4).
- **I7 — Seed-or-demote is exact.** A merge task is created for prefix P **only** when
  a read-only `Branch(P)` probe confirms an on-disk branch exactly at P; otherwise the
  subtree demotes into its nearest seedable ancestor's serial replay. A false "empty"
  probe would drop on-disk siblings and diverge the root, so an unproven probe MUST
  demote or hard-error — never fold a synthesized empty wall (§8). For the depth-64
  seam, the storage root from `foldStorageSeam` injected via `setAccountStorageRoot`
  MUST equal the root the serial inline stream would produce.

## 6. Integration contract

### 6.1 Updates mode

`Process` MUST be called with `updates.mode == ModeParallel` and a populated
`updates.parallel`; it rejects any other mode. `InitializeTrieAndUpdates` forces
`ModeParallel` for `VariantParallelHexPatricia`.

### 6.2 Value source

ModeParallel carries keys (and, when supplied, their `update`) in the prefix trie;
where a node's `update` is nil the value is read from the `PatriciaContext` during
`followAndUpdate`, not from a separate `Updates` value buffer. Each worker takes
its own context from the `TrieContextFactory`, so DB reads run concurrently.

### 6.3 Parallel block apply / fork validation

The parallel-exec commitment calculator (`stagedsync/committer.go`) drives this
path. Two conditions make it compatible with the parallel trie:

1. **ModeParallel buffer.** The calculator MUST keep its `Updates` buffer in
   `ModeParallel` (it does not downgrade to `ModeUpdate`), so `Process` accepts it.
2. **Caller-deferred branches.** With `deferCommitmentUpdates` set, `Process` runs
   under `SetLeaveDeferredForCaller(true)`; the merged branch updates are stashed in
   the pending update and flushed into the correct block's changeset, not applied at
   the current txNum.

Because the ModeParallel buffer may hold no values, leaf values are served by the
calculator's as-of state reader (`sd.GetAsOf(plainKey, lastTxNum+1)`, which
consults `sd.mem`). The reader MUST therefore be installed before `Process`. The
substitution of the as-of reader is validated at runtime by the block-root check
(`ErrWrongTrieRoot`).

## 7. Configuration

| parameter | default | effect |
| --- | --- | --- |
| `--experimental.parallel-commitment` | off | selects `VariantParallelHexPatricia` (`execctx.PickTrieVariant`) |
| `--experimental.streaming-commitment` | off | selects `VariantStreamingHexPatricia` (`StreamingCommitter`); takes precedence over `--experimental.parallel-commitment` |
| `K` | `max(foldKMin, total/(c·numWorkers))` | leaf/merge boundary (`foldK`): subtrees ≤ `K` fold as one serial leaf, larger ones subdivide into merges; `total` = root `subtreeCount` |
| `foldKMin` | 1024 | compile-time const: floors `K` so a merge task always folds enough keys to amortize its fixed cost (own trie context, mmap pin, a `Branch(prefix)` seed read) |
| `c` | 1 | compile-time oversubscription factor in `foldK`; `c=1` keeps `K` a no-op at the natural fan-out (≈ one task per worker on balanced data). Raised only on bench evidence — a higher `c` subdivides every top nibble into ~`c` tasks each paying ctx+pin+seed, the measured detach regression |
| `numWorkers` | `runtime.NumCPU()` | shared pool size (the single GOMAXPROCS-capped concurrency budget); override via `SetNumWorkers` |

## 8. Failure modes

| condition | behaviour |
| --- | --- |
| empty update set | return the template's existing root (matches the sequential no-op) |
| base root carries an extension (`root.ext != 0`) | return an error — not yet supported by the dispatch path |
| terminating node with nil `plainKey` and no children | return an error (only reachable via a hashed-only `TouchHashedKey`; that path is not wired for the parallel trie) |
| merge seed absent at `Branch(prefix)` when the DAG expected it | hard error `errStorageBaseNotBranch` (a store change mid-Process) — never fold a sibling-dropping empty wall |
| deferred apply failure (inline path) | discard the staged root; never surface an unpersisted root |
| task error mid-fold | cancel the shared context; recycle every collected task's deferred entries to the pool, apply nothing (fail-closed) |

## 9. Validation

- `go test -race ./execution/commitment/...` — exercises the concurrency.
- `TestVerifyParallel_RandomBatches` / `TestVerifyParallel_AllShapes` — randomized
  and shaped batches, parallel vs sequential root equality.
- `FuzzParallelEquivalence` — fuzzes parallel-vs-sequential equality.
- `TestFrontierParity_*` (`frontier_parity_test.go`) — root **and** stored-branch
  byte parity vs the sequential trie after **every** batch (N ≥ 3), with an
  `EncodeCurrentState`→`SetState` round-trip between batches, over balanced,
  mega-whale, delete-to-collapse, and extension-topped corpora; an injection self-test
  proves the harness goes red on a corrupted branch.
- `TestUnifiedDispatch_ParallelMatchesStreaming` — the mount and streaming paths
  produce byte-identical root + branch store per batch through the shared dispatch.
- `ErrWrongTrieRoot` — at block-apply time, the computed root is compared to the
  block header; the value-source substitution of §6.3 is proven here, not by the
  unit harness.

## 10. Relationship to the other variants *(informative)*

Both implement `commitment.Trie` and produce the same root; they differ only
in scheduling.

| | `HexPatriciaHashed` | `ParallelPatriciaHashed` |
| --- | --- | --- |
| flag | (default) | `--experimental.parallel-commitment` |
| `Updates` mode | `ModeDirect` / `ModeUpdate` | `ModeParallel` |
| parallel unit | none | one fold task per DAG node (leaf ≤ `K` keys / merge > `K`), pulled by a shared pool of `numWorkers` |
| split granularity | none | frontier-scheduled over the whole fold DAG at any depth; a whale's storage subtree subdivides into ordinary merge/leaf tasks (§4.1.1) |
| merge | single bottom-up fold | per-task cells stitched into the parent's seeded row, folded up the DAG to a single root fold |
| branch writes | inline | deferred, applied once or handed to the caller |
| key delivery | one sorted stream | prefix trie carrying `plainKey`/`update` |
| applicability | always | any shape with `root.ext == 0` |

A third variant, `StreamingCommitter` (`--experimental.streaming-commitment` →
`VariantStreamingHexPatricia`), layers on this one rather than replacing it: it
reuses the same prefix trie and the **same** `foldPool.dispatchFrontier` core, and
upholds R1 identically. It differs only in *when* the fold runs — as touches arrive,
an eager background scheduler pre-folds clean top-nibble splits overlapping execution;
at `Process` the frontier dispatch **consumes** those cached cells via
`reuseSchedulerCells` (a clean split is pruned from the DAG and its pre-folded
cell/deferred stitched directly), while re-dirtied or heavy nibbles fold fresh through
the pool. Folds are stateless (re-folded from the prefix-trie key set, never a
persistent per-split hph mutated by touches — that would break the monotonic
`followAndUpdate` contract). It uses the `streaming` flag on `Updates` (not a new `Mode`).

Whale storage is **not** a special case in either variant: it partitions through the
same fold DAG as any other subtree (§4.1.1), and the mount path (`processMounted`) and
streaming path (`Process`) share one `dispatchFrontier` — the mount path simply passes
`reuse=nil`. See `execution/commitment/streaming_commitment.go`.

## 11. Performance characteristics *(informative)*

Parallelism is no longer bounded by the ≤ 16 touched root nibbles: the frontier pool
subdivides any subtree over `K` into merge/leaf tasks at any depth, so a single "whale"
account with hundreds of thousands of storage slots folds across the whole pool, and
utilization stays near-full until the DAG collapses onto the root spine. At
`numWorkers = NumCPU` on a seedable (re-touched) whale the pool holds > 92 %
utilization with a < 2.5 % serial-collapse tail; the > 16-core utilization gain is only
demonstrable on a > 16-core box.

At `numWorkers = NumCPU` the commitment is core-bound: worker budget beyond NumCPU
buys little, and the `c = 1` / `foldKMin` policy (§7) is a deliberate no-op at the
natural fan-out — a smaller `K` would subdivide every top nibble into tasks each paying
ctx+pin+seed, the measured detach regression.

One case is serial by construction: a **fresh** whale (a contract created *and* writing
> `K` slots in the same block) has no on-disk account branch, so its prefix is
unseedable and its storage subtree demotes (§5 I7) to a single serial leaf. Freshness
is only knowable at fold time, after derivation, so the pure-DAG storage-first ordering
cannot parallelize it without a derive-time freshness oracle — deliberately not adopted
(a false negative would drop on-disk siblings and diverge the root; correctness
outranks the speedup). Re-touched whales, the common case, keep full seam parallelism.

The benchmark `MockState` serializes reads on a shared lock and therefore
under-reports the parallel speedup relative to production's independent per-worker
MDBX readers; figures are for inspection, not a CI gate.

## 12. Source map

| file | contents |
| --- | --- |
| `execution/commitment/fold_dag.go` | the fold DAG: `foldTask`, `deriveFoldDAG` (leaf/merge classification, seed-or-demote, the depth-64 storage-root seam edge), `foldK`/`foldKMin` |
| `execution/commitment/fold_pool.go` | the frontier pool: `foldPool`, `dispatchFrontier`, `deriveFoldFrontier`, `dispatchFoldTasks` (ready-counter scheduling), `foldLeafTask`/`foldMergeTask`/`foldStorageSeam`, `seedMerge`, fail-closed recycling |
| `execution/commitment/parallel_patricia_hashed.go` | `ParallelPatriciaHashed`, `Process` (routes to `processStreaming` when a committer is set), `dfsSubtree`, `newFoldPool`, deferred apply and hand-off |
| `execution/commitment/parallel_mount.go` | `processMounted` — seed the root wall, then `dispatchFrontier` (reuse=nil); `unfoldRootWall`; `seedRootBase`; `mountTo`; `setAccountStorageRoot` |
| `execution/commitment/streaming_deep_fold.go` | the fold seam primitives the pool folds with: `seedBaseAtPrefix` (the seedable prober), `seedOrDemote`, `mergeChildrenAtPrefix`/`stripCellToMountWall`, `aggregateMountedStorageRoot`/`storageRootFromSingleChild`, `stitchChildrenIntoRow0`, `collectSubtreeKeys`, `newDeferredStorageWorker` |
| `execution/commitment/hex_patricia_hashed.go` | sequential engine; `foldMounted` and the `mountWall` stop used by every fold task |
| `execution/commitment/parallel_update.go` | `parallelUpdate`, `plainKeyArena`, `Insert`/deferred accumulation |
| `execution/commitment/prefix_trie.go` | path-compressed prefix trie + slab arena; `Insert` `plainKey` placement; `subtreeCount` (the DAG's leaf/merge signal) |
| `execution/commitment/commitment.go` | `Updates` (ModeParallel carries keys in the prefix trie), `InitializeTrieAndUpdates` |
| `execution/commitment/commitmentdb/commitment_context.go` | wires ModeParallel and caller-deferred updates into `ComputeCommitment` |
| `execution/stagedsync/committer.go` | parallel-exec commitment calculator; keeps the ModeParallel buffer, serves values via the as-of reader |
| `execution/commitment/streaming_commitment.go` | `StreamingCommitter`, the prepare-on-touch variant layered on this one; `Process`, `reuseSchedulerCells`, the eager scheduler |
