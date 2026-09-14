# ParallelPatriciaHashed — Specification

| | |
| --- | --- |
| Component | `execution/commitment` |
| Stability | Default on — `--experimental.parallel-commitment=false` selects the sequential trie |
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

One walker traverses the touched-key prefix trie on a single trie. Wherever that
trie branches into two or more children over a subtree large enough to pay for it,
the walker positions itself on the row below that prefix, mounts a clone per child
nibble, waits, and stitches the folded cells back into the row it holds — a
*fork/stitch* model driven from the prefix trie, at any depth and in either plane.

The prefix trie is already the task tree: a node with two or more touched child
nibbles is a split point, its `ext` is the bridge to the next split point below,
`subtreeCount` is the grain, and the depth-64 node is the account/storage seam. The
work concentrating inside one subtree — a hot contract's storage — needs no separate
mechanism: that storage node is a split point like any other.

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
partitioning, by unfolding any row a fold collapses from the DB first. The walker
reaches every fork row through the engine's own `unfold`, which does exactly that
(§4.1).

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
doubles as the **mount base** during `Process`: the fork walk positions it at each
eligible split row, stitches the workers' folded cells into that row, and folds the
completed walk to the root. (Outside `Process` it exposes ctx/cache/metrics/trace
configuration only.)

## 4. Pipeline

| phase | site | action |
| --- | --- | --- |
| 1. Touch | `Updates.TouchPlainKey` (ModeParallel) | insert each hashed key into the prefix trie, carrying its `plainKey`/`update` on the terminating node; no ETL collectors are used |
| 2. Walk + fork | `processMounted`, `forkWalk` | walk the prefix trie on the base trie; at every split point whose subtree reaches the round's grain, position the base on the row below the split prefix, mount one clone per child nibble, stitch the folded cells back into that row and carry on; fold the base to the root at the end |
| 3. Commit | `Process` end | apply (or hand off) the merged deferred branch updates; publish the root |

### 4.1 Phase 2 — Fork the walk at every split point (`processMounted`, `fork_walk.go`)

`forkWalk.walk` traverses the prefix trie on one trie (`template`) in nibble-ascending
order, applying each terminating node's `plainKey`/`update` through `followAndUpdate`
before descending, so an account at depth 64 precedes its storage keys (I4). A
terminating node with a nil `plainKey` and no children is an error.

A node is a **fork point** when it has two or more children, the work below its
non-largest children — `subtreeCount − max(child.subtreeCount)` — reaches the round's
grain (§4.1.1), and a waiter permit is free. At a fork point with prefix `P` the walker:

1. **Positions itself on the row at depth `len(P)+1`.** `unfoldToRow` folds while
   `needFolding(P·0)`, then unfolds one nibble per step while `needUnfolding(P·0) > 0`.
   These are the serial engine's own `fold`/`unfold`, so an on-disk leaf below `P`, a
   stored branch inside the bridge above it, an extension and the account/storage seam
   are all handled by the paths every key already takes. `unfold` loads a stored branch
   into a real row with **every present child nibble, touched or not** (I2).
2. **Opens an empty row when the region is empty.** If the deepest row is shallower than
   `len(P)+1`, the cell along `P` is empty; `openEmptyRow` opens the row there and marks
   the parent cell touched and present — the bits `updateCell` would have set had the
   serial walker written the first key into that cell.
3. **Mounts one clone per child nibble.** `mountTo` re-bases the clone so the fork row is
   its `grid[0]`, with `mountWall = len(P)+1`. The clone walks its own subtree, forking
   again where the grain allows, and `foldMounted(nibble)` folds back to the fork row and
   returns `grid[0][nibble]`.
4. **Releases its worker slot across the wait** and re-acquires it after, so a waiting
   walker occupies no slot and every slot holder is running. Slots are one
   `semaphore.Weighted` of `min(numWorkers, GOMAXPROCS)`; the errgroup is unlimited. The
   walker holds a **waiter permit** for the whole fork instead, taken by `TryAcquire`
   before it commits to forking — see §4.1.1.
5. **Stitches and continues.** `stitchSplitCells` overlays the returned cells onto the
   fork row and the walk resumes; the walker's next `fold` of that row writes `P`'s branch
   record. A row opened in step 2 that ends with no present cell is closed again and the
   parent's bits restored: serial skips a delete below a shallower row without setting a
   touch bit, and folding the opened row would leave one in `P`'s parent record.

Clones write only prefixes strictly below the fork row; the walker is the single writer
of `P` and of every row above it. Each clone's deferred branch updates go to the shared
accumulator, and its `Metrics` are merged on release.

### 4.1.1 Grain and nesting

`G = max(minForkGrain, roundKeys / (forkGrainPerWorker × numWorkers))` — 128 and 4 — is
computed once per round. `SetForkGrain` overrides it: `1` forks at every split point (the
correctness extreme) and `ForkGrainNever` keeps the whole walk on one trie (the serial
control).

The grain is measured on the work a fork *hands off*, not on the subtree it sits in. A
node where one child holds nearly everything hands off nothing: the straggler child still
sets the wall, and the fork costs a clone, a context and a nesting level. Along a whale's
account path every node is that shape — the whale's subtree plus a handful of neighbours —
so charging them the grain (or a nesting level) starves the fork that matters, the one at
the account's storage node. Under the offload rule a block round forks at the root and at
any hot contract whose storage node spreads `G` or more touched slots over its nibbles,
whether or not the account itself was touched; a bulk round of a million keys at 16 workers
gets `G ≈ 15.6k`, so the root and every depth-1 node fork and depth-2 nodes do not.

A second semaphore bounds the walkers parked on their children. A walker needs a slot only
long enough to *reach* its fork point — it releases before `Wait` — so the slot semaphore
bounds running walkers and says nothing about waiting ones, whose breadth per level is
bounded only by the grain. Since a parked walker keeps its rows, its pooled trie and its
`PatriciaContext` (a read transaction) for the whole subtree, that breadth is what the
read-tx budget has to bound.

Waiter permits are `min(numWorkers, GOMAXPROCS)`, taken with `TryAcquire`: a walker that
cannot get one does not fork, it walks the subtree inline. That is always correct and only
ever costs parallelism, and it makes the budget exact —
`2 × min(numWorkers, GOMAXPROCS) + 1` (`ParallelCommitmentReadTxs`): one per running
walker, one per parked walker, one for the base. Under-declaring it is not a slowdown but a
hang: the production factory (`concurrentTrieContextFactory`) blocks in `beginWorkerRo`, so
a child that cannot get a transaction blocks while holding a fork slot, and its parent holds
a transaction while blocked in `Wait` on that child.

### 4.2 Phase 3 — Commit and root publication

Workers accumulate `DeferredBranchUpdate`s rather than writing branches. After the
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
  nibbles, every branch row a fold collapses MUST first be unfolded from `ctx.Branch`
  so untouched on-disk siblings are present. The walker reaches every fork row through
  `unfoldToRow`, i.e. through the serial engine's `unfold`, which loads a stored branch
  with all its present nibbles and materialises every stored branch between the walker's
  row and the fork prefix. Positioning a fork row from a synthesized bridge instead
  drops untouched siblings and diverges the root.
- **I3 — `plainKey` follows the split.** `prefixTrie.Insert` MUST route a
  terminator's `plainKey` to the correct node across path-compression splits (§3.1).
  A misroute is a wrong DB read and a diverged root.
- **I4 — Sorted emission.** Within a mount, keys MUST be presented to
  `followAndUpdate` in ascending hashed-key order, a terminating node before its
  descendants; the DFS yields this because children are nibble-ordered.
- **I5 — Single branch writer.** All `PutBranch` calls issue from one goroutine
  (inline apply) or are handed to the caller; a clone owns the subtree strictly below its
  fork row, hence a disjoint set of branch prefixes, and each prefix has one producer.
- **I6 — Read safety.** Clones walk disjoint subtrees of the frozen prefix trie. A
  clone reads its parent only in `mountTo`, which copies the fork row; the parent writes
  that row again only after `errgroup.Wait`, so the copy, the concurrent structure and
  `plainKey` reads, and the stitch are race-free.
- **I7 — Fork equals inline walk.** The cells a fork stitches back into its row MUST
  equal what the walker would have produced walking the subtree itself, at any depth and
  in either plane. `G = 1` (fork at every split point) and `G = ForkGrainNever` (never
  fork) both satisfy I1 and byte-identical branch records over the parity corpora.

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
| `--experimental.parallel-commitment` | on | selects `VariantParallelHexPatricia` (`execctx.PickTrieVariant`); `=false`, or `COMMITMENT_PARALLEL=false`, selects `VariantHexPatriciaTrie` |
| `--experimental.streaming-commitment` | off | selects `VariantStreamingHexPatricia` (`StreamingCommitter`); takes precedence over `--experimental.parallel-commitment` |
| fork grain `G` | `max(128, roundKeys/(4·numWorkers))` | per-round minimum work a fork must hand to its non-largest children (§4.1.1); `SetForkGrain` overrides — `1` forks at every split point, `ForkGrainNever` disables forking |
| `numWorkers` | `runtime.NumCPU()` | clamped to `GOMAXPROCS`, then sizes both semaphores and divides the grain; the errgroup itself is unlimited. Override via `SetNumWorkers` |

## 8. Failure modes

| condition | behaviour |
| --- | --- |
| empty update set | return the template's existing root (matches the sequential no-op) |
| terminating node with nil `plainKey` and no children | return an error (only reachable via a hashed-only `TouchHashedKey`; that path is not wired for the parallel trie) |
| deferred apply failure (inline path) | restore the base from the same snapshot, so `RootHash` returns the pre-round root rather than the staged one. The branch records the partial apply already wrote are not rolled back — same caveat as the row below |
| worker error mid-fold | cancel the group; return pooled deferred entries |
| any error after the walk began | restore the base trie's in-memory state from the snapshot taken before it — root cell and its three flags, no open rows, no queued branch updates |
| branch records the aborted round already wrote | none, for the paths this engine takes. `processMounted` defers on the base and every worker, and `collectDeleteUpdate` now honours that flag too, so a collapsing row queues its deletion instead of writing it. `ClearDeferred` on the abort path therefore discards the whole round. Two escapes remain in `CollectDeferredUpdate` — a flush at `DefaultMaxDeferredUpdates` and one on a repeated prefix — neither of which fires on the collapse corpus; until they are removed a retry is still only sound where the caller discards the round's domain writes, which every production caller does by dropping the RwTx |

## 9. Validation

- `go test -race ./execution/commitment/...` — exercises the concurrency.
- `TestVerifyParallel_RandomBatches` / `TestVerifyParallel_AllShapes` — randomized
  and shaped batches, parallel vs sequential root equality.
- `FuzzParallelEquivalence` — fuzzes parallel-vs-sequential equality.
- `ErrWrongTrieRoot` — at block-apply time, the computed root is compared to the
  block header; the value-source substitution of §6.3 is proven here, not by the
  unit harness.
- `TestModeParallel_MidWalkErrorRestoresBaseTrie` / `...RestoresTrieNotDomain` —
  the two halves of the §8 abort contract: the in-memory state that is restored,
  and the branch records that are not.
- `TestModeParallel_DeferredApplyErrorKeepsPreRoundRoot` — the snapshot reaches
  past the walk: a failed inline apply must not leave `RootHash` on the staged root.

## 10. Relationship to the other variants *(informative)*

Both implement `commitment.Trie` and produce the same root; they differ only
in scheduling.

| | `HexPatriciaHashed` | `ParallelPatriciaHashed` |
| --- | --- | --- |
| flag | `--experimental.parallel-commitment=false` | (default) |
| `Updates` mode | `ModeDirect` / `ModeUpdate` | `ModeParallel` |
| parallel unit | none | one clone per child nibble of a forking prefix-trie node (≤16 per fork) |
| split granularity | none | every prefix-trie split point whose subtree reaches the grain, at any depth and in either plane |
| merge | single bottom-up fold | folded cells stitched into the fork row, the walk continuing on the same trie |
| branch writes | inline | deferred, applied once or handed to the caller |
| key delivery | one sorted stream | prefix trie carrying `plainKey`/`update` |
| applicability | always | any shape |

A third variant, `StreamingCommitter` (`--experimental.streaming-commitment` →
`VariantStreamingHexPatricia`), layers on this one rather than replacing it: it
reuses the same prefix trie and fold engine and upholds R1 identically. It differs
only in *when* the fold runs — touched keys are re-folded per top-nibble split in a
background worker pool overlapping execution, so `Process` collapses to a merge of
already-folded split cells. Folds are stateless (re-folded from the prefix-trie key
set, never a persistent per-split hph mutated by touches — that would break the
monotonic `followAndUpdate` contract). It uses the `streaming` flag on `Updates`
(not a new `Mode`).

## 11. Performance characteristics *(informative)*

Parallelism is bounded by the split points the grain admits, not by a fixed depth: a
round forks at the root, inside a hot contract's storage, and at any interior prefix
whose subtree hands off enough work, at any nesting. A single whale account with
hundreds of thousands of storage slots folds across its touched first-storage nibbles,
and again below them if each is still above the grain.

At `numWorkers = NumCPU` the parallel commitment is effectively core-bound: worker budget
beyond NumCPU buys little. The grain trades the per-fork fixed cost (a pooled trie, a
fresh `PatriciaContext` with its read tx, an ETL collector and metrics) against the
per-key fold cost it saves, and it is charged against the work a fork hands off so that a
straggler-shaped node does not spend it; `minForkGrain` and `forkGrainPerWorker` are the
two knobs and are set by measurement.

The benchmark `MockState` serializes reads on a shared lock and therefore under-reports
the parallel speedup relative to production's independent per-worker MDBX readers;
figures are for inspection, not a CI gate.

## 12. Source map

| file | contents |
| --- | --- |
| `execution/commitment/parallel_patricia_hashed.go` | `ParallelPatriciaHashed`, `Process` (routes to `processStreaming` when a committer is set), `dfsSubtree`, deferred apply and hand-off |
| `execution/commitment/parallel_mount.go` | `processMounted` — drive the fork walk over the whole prefix trie and fold the base to the root; `mountTo` re-basing a clone on the fork row |
| `execution/commitment/fork_walk.go` | `forkWalk` — the walk, the fork predicate, the worker slots, the grain and nesting bounds; `newForkWorker` |
| `execution/commitment/split_point.go` | depth-agnostic split-point primitives: `unfoldToRow`, `openEmptyRow`/`closeIfEmpty`, `stitchSplitCells`, `foldSplitRow` |
| `execution/commitment/hex_patricia_hashed.go` | sequential engine; `foldMounted` and the `mountWall` stop used by both fold levels |
| `execution/commitment/parallel_update.go` | `parallelUpdate`, `plainKeyArena`, `Insert`/deferred accumulation |
| `execution/commitment/prefix_trie.go` | path-compressed prefix trie + slab arena; `Insert` `plainKey` placement |
| `execution/commitment/commitment.go` | `Updates` (ModeParallel carries keys in the prefix trie), `InitializeTrieAndUpdates` |
| `execution/commitment/commitmentdb/commitment_context.go` | wires ModeParallel and caller-deferred updates into `ComputeCommitment` |
| `execution/stagedsync/committer.go` | parallel-exec commitment calculator; keeps the ModeParallel buffer, serves values via the as-of reader |
| `execution/commitment/streaming_commitment.go` | `StreamingCommitter`, the prepare-on-touch variant layered on this one |
