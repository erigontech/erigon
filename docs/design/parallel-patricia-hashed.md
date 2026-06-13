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
re-folds the merged root row on the main goroutine — the *mount/fold* model of
`ConcurrentPatriciaHashed`, driven from the touched-key prefix trie instead of
per-nibble ETL collectors.

The fold is **single-level**: one worker per touched root nibble (≤ 16). A whole
child subtree — for example one account's entire storage — folds on a single
worker; nested mounting that would parallelise within a subtree is future work
(§11).

## 2. Preliminaries *(informative)*

`HexPatriciaHashed` keeps a `grid[128][16]cell` (one row per nibble depth) and a
`currentKey`. Per sorted batch it unfolds down to the next key (loading branches
from the `PatriciaContext`), applies the update, and folds completed rows upward,
hashing each branch and writing it via `PatriciaContext.PutBranch`; the final fold
to row 0 yields the root. A branch hash mixes **every present nibble** of the
branch, not only the touched ones — the property §4 must preserve under
partitioning, here by unfolding the shared root row from the DB before mounting.

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
tries, a `TrieContextFactory`, `numWorkers`, the published `rootHash`, and — for
the deferred path — a `leaveDeferredForCaller` flag with a `deferredForCaller`
hand-off slice. The `template` doubles as the **mount base** during `Process`: it
is unfolded to the root branch, the workers' folded cells are dropped into its row
0, and it folds the merged root. (Outside `Process` it exposes
ctx/cache/metrics/trace configuration only.)

## 4. Pipeline

| phase | site | action |
| --- | --- | --- |
| 1. Touch | `Updates.TouchPlainKey` (ModeParallel) | insert each hashed key into the prefix trie, carrying its `plainKey`/`update` on the terminating node; no ETL collectors are used |
| 2. Mount + fold | `processMounted`, concurrent | unfold the base to the root branch; mount a worker per touched root nibble; each folds its child subtree into a cell; drop the cells back into the base row and fold the merged root |
| 3. Commit | `Process` end | apply (or hand off) the merged deferred branch updates; publish the root |

### 4.1 Phase 2 — Mount and fold (`processMounted`, `dfsSubtree`)

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
3. **Build.** `dfsSubtree(child, [nibble]+child.ext)` walks the nibble's subtree in
   nibble-ascending order. At each terminating node it reconstructs the full hashed
   key, reads the `plainKey`/`update` off the node, and calls
   `followAndUpdate`. A node MUST emit its own key **before** descending to its
   children, so an account at depth 64 precedes its storage keys — the sorted order
   the fold state machine requires (I4). A terminating node with a nil `plainKey`
   and no children is unsupported and MUST raise an error rather than be skipped.
4. **Fold the mount.** `foldMounted(nibble)` folds the worker's subtree into a
   single cell at the nibble's child depth, stopping before it would absorb the
   shared base root row. The worker's deferred branch updates are appended to the
   shared accumulator and the worker is returned to the pool.
5. **Merge and fold root.** On the main goroutine, after `errgroup.Wait`, each
   folded cell is dropped into `base.grid[0][nibble]` (stripping the leading nibble
   a hash-only sub-branch carries in its extension) and the touch/after maps are
   set. The base then folds row 0 to the root; `rootPresent` is set from
   `!root.IsEmpty()` so the encode/restore round-trip (§6.3) stays correct.

Workers share the base root cell read-only and each inherit an independent copy of
the unfolded grid; only the main goroutine mutates the base after `Wait`. There is
no fold-time barrier and no cross-worker synchronisation beyond the deferred-update
mutex.

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
  nibbles, the shared root row MUST be unfolded from `ctx.Branch` before mounting,
  so untouched on-disk siblings are present in `grid[0]` when the merged root is
  folded; workers write only their own touched subtree.
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
| `--experimental.parallel-commitment` | off | selects `VariantParallelHexPatricia`; takes precedence over `--experimental.concurrent-commitment` (`execctx.PickTrieVariant`) |
| `numWorkers` | `runtime.NumCPU()` | worker-pool size and errgroup limit; override via `SetNumWorkers` |

## 8. Failure modes

| condition | behaviour |
| --- | --- |
| empty update set | return the template's existing root (matches the sequential no-op) |
| base root carries an extension (`root.ext != 0`) | return an error — not yet supported by the single-level mount |
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

All three implement `commitment.Trie` and produce the same root; they differ only
in scheduling.

| | `HexPatriciaHashed` | `ConcurrentPatriciaHashed` | `ParallelPatriciaHashed` |
| --- | --- | --- | --- |
| flag | (default) | `--experimental.concurrent-commitment` | `--experimental.parallel-commitment` |
| `Updates` mode | `ModeDirect` / `ModeUpdate` | `ModeDirect` + `sortPerNibble` | `ModeParallel` |
| parallel unit | none | one goroutine per top nibble (≤16) | one worker per **touched** top nibble (≤16) |
| split granularity | none | fixed 16-way at depth 1 | touched top nibbles at depth 1 (single-level) |
| merge | single bottom-up fold | per-mount fold under `rootMu` | per-mount cells dropped into the base row, single root fold |
| branch writes | inline | inline (per mount) | deferred, applied once or handed to the caller |
| key delivery | one sorted stream | 16 per-nibble ETL collectors | prefix trie carrying `plainKey`/`update` |
| applicability | always | only when the top node is a wide branch (`CanDoConcurrentNext`) | any shape with `root.ext == 0` |

`ParallelPatriciaHashed` is the mount/fold model of `ConcurrentPatriciaHashed`
applied only to the nibbles actually touched and fed from the prefix trie;
`ConcurrentPatriciaHashed` is the only consumer of the 16 per-nibble ETL
collectors, which ModeParallel does not allocate.

## 11. Performance characteristics *(informative)*

The fold is single-level: parallelism is bounded by the number of **touched** root
nibbles (≤ 16) and, within each, by the cost of folding that child's whole subtree
on one worker. Batches whose work concentrates inside one subtree — for example a
single "whale" account with hundreds of thousands of storage slots — see little
speedup, because that subtree folds serially on one worker. Nested mounting that
would parallelise within a subtree is future work. The benchmark `MockState`
serializes reads on a shared lock and therefore under-reports the parallel speedup
relative to production's independent per-worker MDBX readers. These figures are for
inspection, not a CI gate.

## 12. Source map

| file | contents |
| --- | --- |
| `execution/commitment/parallel_patricia_hashed.go` | `ParallelPatriciaHashed`, `Process`, `dfsSubtree`, deferred apply and hand-off |
| `execution/commitment/parallel_mount.go` | `processMounted` — unfold, per-nibble mount/fold, merged root fold |
| `execution/commitment/parallel_update.go` | `parallelUpdate`, `plainKeyArena`, `Insert`/deferred accumulation |
| `execution/commitment/prefix_trie.go` | path-compressed prefix trie + slab arena; `Insert` `plainKey` placement |
| `execution/commitment/hex_concurrent_patricia_hashed.go` | `mountTo`/`foldMounted` mount primitives, shared with `ConcurrentPatriciaHashed` |
| `execution/commitment/commitment.go` | `Updates` (ModeParallel carries keys in the prefix trie), `InitializeTrieAndUpdates` |
| `execution/commitment/commitmentdb/commitment_context.go` | wires ModeParallel and caller-deferred updates into `ComputeCommitment` |
| `execution/stagedsync/committer.go` | parallel-exec commitment calculator; keeps the ModeParallel buffer, serves values via the as-of reader |
