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

It unfolds the root branch once, then mounts a worker per touched top-level
nibble that folds that child's subtree into a single cell concurrently, and
re-folds the merged root row on the main goroutine — a *mount/fold* model driven
from the touched-key prefix trie.

The top level mounts one worker per touched root nibble (≤ 16). A second level
handles the case where the work concentrates inside one subtree: when
a worker reaches a *big-storage account* (> `deepStorageThreshold` touched storage
keys across ≥ 2 first-storage nibbles) it folds that account's storage subtree
concurrently — one worker per touched first-storage nibble — instead of streaming
it serially. This *deep storage fold* (§4.1.1) is the same mount/fold primitive
applied one level down. Splitting deeper than the first storage nibble is future
work (§11).

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
partitioning, by unfolding any row a fold collapses from the DB first: the shared
root row before mounting, and a whale's storage-root row before the deep fold
(§4.1.1).

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
doubles as the **mount base** during `Process`: it is unfolded to the root branch,
the workers' folded cells are dropped into its row 0, and it folds the merged root.
(Outside `Process` it exposes ctx/cache/metrics/trace configuration only.)

## 4. Pipeline

| phase | site | action |
| --- | --- | --- |
| 1. Touch | `Updates.TouchPlainKey` (ModeParallel) | insert each hashed key into the prefix trie, carrying its `plainKey`/`update` on the terminating node; no ETL collectors are used |
| 2. Mount + fold | `processMounted`, concurrent | unfold the base to the root branch; mount a worker per touched root nibble; each folds its child subtree into a cell (a big-storage account's storage folds concurrently, §4.1.1); drop the cells back into the base row and fold the merged root |
| 3. Commit | `Process` end | apply (or hand off) the merged deferred branch updates; publish the root |

### 4.1 Phase 2 — Mount and fold (`processMounted`, `dfsSubtreeDeep`)

1. **Unfold the base.** `processMounted` unfolds `template` down to the root
   branch (`needUnfolding`/`unfold` on the zero prefix), loading the on-disk root
   branch into `grid[0]`. This populates **every present child nibble, touched or
   not** (I2), so untouched siblings survive the final fold and the branch hash
   mixes all nibbles. The base MUST have `root.ext == 0` (§8).
2. **Mount per touched nibble.** For each set bit in the prefix trie's root
   `bitmap`, an errgroup (limit `numWorkers`) acquires a pooled
   `*HexPatriciaHashed`, calls `mountTo(base, nibble)` — inheriting a copy of the
   base's unfolded grid and sharing the base root cell read-only — and binds it to
   a fresh factory `PatriciaContext` with deferred branch writes enabled.
3. **Build.** `dfsSubtreeDeep(child, [nibble]+child.ext)` walks the nibble's subtree
   in nibble-ascending order. At each terminating node it reconstructs the full
   hashed key, reads the `plainKey`/`update` off the node, and calls
   `followAndUpdate`. A node MUST emit its own key **before** descending to its
   children, so an account at depth 64 precedes its storage keys — the sorted order
   the fold state machine requires (I4). A terminating node with a nil `plainKey`
   and no children is unsupported and MUST raise an error rather than be skipped.
   When a node is a *big-storage account* (`isDeepStorageAccount`: depth 64, plain
   key set, ≥ 2 first-storage nibbles, `subtreeCount > deepStorageThreshold`) the
   walk does **not** descend into its storage children: it computes the storage root
   via the deep storage fold (§4.1.1) and injects it into the account leaf
   (`setAccountStorageRoot`).
4. **Fold the mount.** `foldMounted(nibble)` folds the worker's subtree upward,
   stopping when it reaches `mountWall` — the depth `mountTo` records as
   `split-depth + 1`, so the top-level mount stops at depth 1, before it would absorb
   the shared base root row — and returns `grid[0][nibble]`. The worker's deferred
   branch updates are appended to the shared accumulator and the worker is returned
   to the pool.
5. **Merge and fold root.** On the main goroutine, after `errgroup.Wait`, each
   folded cell is dropped into `base.grid[0][nibble]` (stripping the leading nibble
   a hash-only sub-branch carries in its extension) and the touch/after maps are
   set. The base then folds row 0 to the root; `rootPresent` is set from
   `!root.IsEmpty()` so the encode/restore round-trip (§6.3) stays correct.

Workers share the base root cell read-only and each inherit an independent copy of
the unfolded grid; only the main goroutine mutates the base after `Wait`. There is
no fold-time barrier and no cross-worker synchronisation beyond the deferred-update
mutex.

### 4.1.1 Deep storage fold (`foldStorageRoot`, `streaming_deep_fold.go`)

A big-storage account's storage subtree (a "whale") would otherwise fold serially on
its top-nibble worker. `foldStorageRoot` folds it concurrently, applying the §4.1
mount/fold model one level down at depth 64. It runs the same primitives
(`mountTo`/`foldMounted`/`followAndUpdate`) and is shared verbatim by the streaming
variant (§10).

1. **Unfold the storage-root branch.** `unfoldStorageBase(base, accHash[:64])` seeds a
   base worker by reading the account's on-disk storage-root branch
   (`branchFromCacheOrDB` + `decodeBranchIntoRow` — the same decode the account unfold
   `unfoldBranchNode` uses, entered manually at depth 64 instead of by recursive
   descent). This is I2 applied at depth 64:
   untouched on-disk first-storage-nibble siblings MUST be present before the storage
   root is folded, or they are dropped and the storage root — hence the state root —
   diverges (see I2).
2. **Fold per first-storage nibble.** One errgroup worker per touched first-storage
   nibble: `foldStorageLeaf` mounts the shared base at that nibble (`mountWall = 65`),
   streams the nibble's sorted slots, and `foldMounted` returns the depth-65 child
   cell. Workers defer their branch writes into the shared accumulator. They own
   disjoint storage prefixes, so concurrent reads of the shared base are race-free.
3. **Aggregate.** `aggregateMountedStorageRoot` overlays the folded child cells onto
   the unfolded base row (setting/clearing each touched present bit, leaving untouched
   on-disk siblings intact) and folds once to the account's storage-root cell.
4. **Inject.** `setAccountStorageRoot` writes that hash into the account leaf
   (`cell.hash`, `hashLen = 32`); `computeCellHash` uses it as the storageRoot for an
   account whose storage cell was not streamed, so the leaf hashes identically to the
   serial path. The DFS then skips the account's storage children.

Below `deepStorageThreshold`, or with storage in a single first nibble, the account
streams inline as in §4.1 — the per-account setup cost (a pooled worker, a fresh
context, the storage-root unfold) only pays off for genuinely large storage.

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
  so untouched on-disk siblings are present. This holds at two depths: the shared
  root row before mounting (`processMounted`), and each big-storage account's
  storage-root branch before the deep fold (`unfoldStorageBase`, §4.1.1). Dropping
  either unfold drops untouched siblings and diverges the root.
- **I3 — `plainKey` follows the split.** `prefixTrie.Insert` MUST route a
  terminator's `plainKey` to the correct node across path-compression splits (§3.1).
  A misroute is a wrong DB read and a diverged root.
- **I4 — Sorted emission.** Within a mount, keys MUST be presented to
  `followAndUpdate` in ascending hashed-key order, a terminating node before its
  descendants; the DFS yields this because children are nibble-ordered.
- **I5 — Single branch writer.** All `PutBranch` calls issue from one goroutine
  (inline apply) or are handed to the caller; workers own disjoint top-nibble
  subtrees hence disjoint branch prefixes, and each subtree has a single producer.
- **I6 — Read safety.** Workers walk disjoint top-nibble subtrees of the frozen
  prefix trie and each fold an independent copy of the unfolded grid, sharing the
  base root cell read-only; the merged base row is folded only on the main
  goroutine after `errgroup.Wait`, so concurrent structure/`plainKey` reads and the
  final fold are race-free.
- **I7 — Deep fold equals inline stream.** For a big-storage account, the storage
  root from `foldStorageRoot` injected via `setAccountStorageRoot` MUST equal the
  root the serial inline stream would produce. Its per-first-nibble workers own
  disjoint storage prefixes, share the unfolded storage base read-only, and each
  defers its own branch writes (I5).

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
| `deepStorageThreshold` | 1000 | compile-time const (not a runtime flag): per-account touched-storage-key count above which the storage subtree folds concurrently (§4.1.1); mitigates the whale bottleneck of §11 |
| `numWorkers` | `runtime.NumCPU()` | worker-pool size and errgroup limit; override via `SetNumWorkers` |

## 8. Failure modes

| condition | behaviour |
| --- | --- |
| empty update set | return the template's existing root (matches the sequential no-op) |
| base root carries an extension (`root.ext != 0`) | return an error — not yet supported by the mount path |
| terminating node with nil `plainKey` and no children | return an error (only reachable via a hashed-only `TouchHashedKey`; that path is not wired for the parallel trie) |
| deferred apply failure (inline path) | discard the staged root; never surface an unpersisted root |
| worker error mid-fold | cancel the group; return pooled deferred entries |

## 9. Validation

- `go test -race ./execution/commitment/...` — exercises the concurrency.
- `TestVerifyParallel_RandomBatches` / `TestVerifyParallel_AllShapes` — randomized
  and shaped batches, parallel vs sequential root equality.
- `FuzzParallelEquivalence` — fuzzes parallel-vs-sequential equality.
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
| parallel unit | none | one worker per **touched** top nibble (≤16), plus one per first-storage nibble inside a big-storage account |
| split granularity | none | touched top nibbles at depth 1, and first-storage nibbles at depth 64 for big-storage accounts (§4.1.1) |
| merge | single bottom-up fold | per-mount cells dropped into the base row, single root fold |
| branch writes | inline | deferred, applied once or handed to the caller |
| key delivery | one sorted stream | prefix trie carrying `plainKey`/`update` |
| applicability | always | any shape with `root.ext == 0` |

A third variant, `StreamingCommitter` (`--experimental.streaming-commitment` →
`VariantStreamingHexPatricia`), layers on this one rather than replacing it: it
reuses the same prefix trie and fold engine and upholds R1 identically. It differs
only in *when* the fold runs — touched keys are re-folded per top-nibble split in a
background worker pool overlapping execution, so `Process` collapses to a merge of
already-folded split cells. Folds are stateless (re-folded from the prefix-trie key
set, never a persistent per-split hph mutated by touches — that would break the
monotonic `followAndUpdate` contract). It uses the `streaming` flag on `Updates`
(not a new `Mode`).

Big-storage accounts take the **same** deep storage fold (§4.1.1): each split's
`foldSplit` runs `dfsSubtreeDeep` with `foldStorageRoot`, shared verbatim from
`streaming_deep_fold.go` — the deep-fold logic is not duplicated. See
`execution/commitment/streaming_commitment.go`.

## 11. Performance characteristics *(informative)*

Top-level parallelism is bounded by the number of **touched** root nibbles (≤ 16);
within a big-storage account the deep fold (§4.1.1) adds a second level bounded by
its touched first-storage nibbles (≤ 16), so a single "whale" account with hundreds
of thousands of storage slots folds across up to 16 workers instead of one.

At `numWorkers = NumCPU` the parallel commitment is effectively core-bound: worker
budget beyond NumCPU buys little, and lowering `deepStorageThreshold` to detach
medium accounts costs more in per-account setup (a pooled worker, a fresh context,
the storage-root unfold) than the extra split saves. Splitting deeper than the first
storage nibble, and detaching storage below the whale threshold, are not currently
worthwhile.

The benchmark `MockState` serializes reads on a shared lock and therefore
under-reports the parallel speedup relative to production's independent per-worker
MDBX readers; figures are for inspection, not a CI gate.

## 12. Source map

| file | contents |
| --- | --- |
| `execution/commitment/parallel_patricia_hashed.go` | `ParallelPatriciaHashed`, `Process` (routes to `processStreaming` when a committer is set), `dfsSubtree`, deferred apply and hand-off |
| `execution/commitment/parallel_mount.go` | `processMounted` — unfold, per-nibble mount/fold via `dfsSubtreeDeep`, merged root fold; `mountTo`; `setAccountStorageRoot`; `deepStorageThreshold` |
| `execution/commitment/streaming_deep_fold.go` | the deep storage fold shared by the parallel and streaming paths: `dfsSubtreeDeep`, `isDeepStorageAccount`, `foldStorageRoot`, `unfoldStorageBase`, `foldStorageLeaf`, `aggregateMountedStorageRoot` |
| `execution/commitment/hex_patricia_hashed.go` | sequential engine; `foldMounted` and the `mountWall` stop used by both fold levels |
| `execution/commitment/parallel_update.go` | `parallelUpdate`, `plainKeyArena`, `Insert`/deferred accumulation |
| `execution/commitment/prefix_trie.go` | path-compressed prefix trie + slab arena; `Insert` `plainKey` placement |
| `execution/commitment/commitment.go` | `Updates` (ModeParallel carries keys in the prefix trie), `InitializeTrieAndUpdates` |
| `execution/commitment/commitmentdb/commitment_context.go` | wires ModeParallel and caller-deferred updates into `ComputeCommitment` |
| `execution/stagedsync/committer.go` | parallel-exec commitment calculator; keeps the ModeParallel buffer, serves values via the as-of reader |
| `execution/commitment/streaming_commitment.go` | `StreamingCommitter`, the prepare-on-touch variant layered on this one |
