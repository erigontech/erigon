# Incremental Streaming Commitment — Design & Plan

Status: design / not started. Author thread: 2026-06-01.
Background: erigontech/metarepo-internal-issues#17 (Profile against reth).

## 1. Objective

Start computing the state-root trie **during** block execution instead of after it,
without relying on a Block Access List (BAL). Today commitment and execution overlap
only in the cheap accumulation phase; the expensive trie fold runs serially after
block end. The goal is to collapse the steady-state cost from `exec + trie` toward
`max(exec, trie)`, matching reth's "trie is ~done when exec finishes" model — on the
no-BAL path.

Target: the ~30–40 ms serial state-root burst measured in issue #17 (erigon 39.5 ms
vs reth 2.5 ms wall-clock, the latter being only the end-of-block blocking wait).

## 2. Why the current model waits

`HexPatriciaHashed.Process` opens with `updates.HashSort(...)` then a single ordered
`unfold`/`fold` pass. The pass requires the **complete sorted touched-key set** up
front: it folds a left-region only once the sort guarantees no later key falls there.
Execution produces writes in **tx order** (arbitrary w.r.t. hashed key). The streaming
calculator (`execution/stagedsync/committer.go`) bridges the two by buffering the whole
unordered stream into `cc.state` (`txResult` → `ApplyWrites`) and folding at `blockResult`.

An ordered-batch trie cannot consume an unordered stream early without knowing the
smallest key any future tx will touch — which is exactly the BAL information we are
choosing not to depend on. So the data structure, not the wiring, is what forces the wait.

## 3. Approach — streaming sparse trie with reactive invalidation

Replace the sort-then-single-pass consumer with a **radix/sparse trie that accepts
unordered insertion**; order emerges from the structure, no global sort.

- **Insert** each touched key as its tx completes (arbitrary order). Record the changed
  path in a **prefix set** (an append-only set of changed nibble paths). Inserting builds
  only structure + the changed-path mark — it is cheap and does **not** hash.
- **Defer hashing** to a flush/root boundary, *not* per tx. At the boundary, walk only the
  prefix-set-marked paths and recompute their hashes bottom-up; unchanged subtrees keep
  their cached hash. This is reth's model (`PrefixSetMut` → freeze → sorted `PrefixSet`
  with a stateful cursor; hashes computed in `update_subtrie_hashes` at `root()`, never
  per update). It sidesteps both last-write-wins (a key's final value is settled by
  boundary time) and speculative-refold overhead (a hot branch hashes once per boundary,
  not once per touch).
- **What overlaps execution** is therefore the **structure build + unfold-IO prefetch**
  (cheap, streamed per tx), while the **hash** is a parallel burst at the boundary. Both
  matter: overlap removes the unfold IO from the critical path; the parallel fold (§4)
  compresses the boundary burst. reth's ~2.5 ms end-of-block needs *both*.

`execution/commitment/prefix_trie.go` (PR #21238) already implements the front half:
a path-compressed nibble radix trie whose `Insert` is order-independent and keeps
children nibble-sorted by construction. It currently carries only structure
(`{ext, children, subtreeCount, bitmap}`) and is used only to find split-points.
PR2 grows it into a hashing node.

## 4. Concurrency — lock-free via static top-down split

Partition the trie by its top **D** nibbles. Distinct partitions occupy **disjoint
prefix subspaces**, so:

- No two workers ever write the same branch node below depth D → the `BranchCache` and
  the trie grid need **no locks** (this is the disjoint-prefix discipline
  `branch_cache.go` already requires, satisfied structurally).
- The only shared region is the **trunk** (depth 0..D), written by a **single** writer —
  the finisher that combines partition roots into the root hash. One writer ⇒ no lock.

Direction matters: top-down assigns ownership from the *prefix* at depth D up front, so a
streaming write routes to its owner with zero coordination. Bottom-up would only learn
ownership after merges (shared → locks).

**Two distinct depths — do not conflate them:**

- **Trunk-cache depth `T` = 5 nibbles.** How much of the upper account trie stays resident
  in cache across blocks. The account trie (keccak(addr), ~250M+ accounts) is dense through
  ~depth 6 (16^5 ≈ 1.05M ≪ 250M), so depths 0..5 are essentially fully populated and barely
  change block-to-block — a block touches a few thousand accounts, so the vast majority of
  the ~1M depth-5 branches are byte-identical to last block. Caching depths 0..5 keeps the
  *majority of the trunk* hot, so every block's touched-path siblings are cache hits
  regardless of which accounts it touches. This is the resident static trunk = the
  `BranchCache` pinned tiers.
- **Parallel-split depth `S`** (≈ 2). How the fold is partitioned for lock-free parallelism.
  Must stay shallow — reth uses **S=2 → 256 lower subtries** (`UPPER_TRIE_MAX_DEPTH = 2`,
  `NUM_LOWER_SUBTRIES = 256`, `/tmp/reth/crates/trie/sparse/src/parallel.rs`); erigon's
  `ConcurrentPatriciaHashed` uses S=1 → 16 mounts. Splitting at depth 5 would mean ~1M
  partitions — nonsensical. S is worker-count granularity, not cache residency.

`S ≤ T`. The spine above S (depths 0..S) is single-writer at the finisher; subtrees below S
are the disjoint lock-free partitions; **all of depths 0..T are cached resident**, spanning
both. A worker folding its subtree reads trunk siblings (depths S..T) from cache as hits.
Static split (not #21238's data-dependent split points, which need the complete key set and
are block-end) → deterministic lock-free routing of the unordered stream. Start S=2 (match
reth), T=5; PR4 metrics retune both. Adaptive sub-splitting within a hot partition layers
later.

Memory: a depth-5 resident trunk is ~1M branch nodes (order ~0.5 GB depending on encoding) —
a real budget line, feasible alongside the existing ~2.5 GB StateCache, and the reason the
cache keeps an LRU tail + adaptive pin rather than caching everything.

## 5. Cache convergence — the static trunk is the resident upper trie

`execution/commitment/branch_cache.go` (parked stack, PR #21380) is already a three-tier
depth-stratified cache:

1. `root` — single atomic-pointer slot for the root branch; never evicts; lock-free read.
2. `pinned` — per-prefix pins (`PinEntry`) for hot-contract storage trunks; never evict;
   **updated in place across blocks**.
3. `tail` — bounded LRU for cold/rewritten lower branches.

Tiers 1+2 = the **static trunk** = the resident upper trie.

**The rationale is READ hits, not writes.** The upper trie is *read* on every block — every
touched-key path traverses it, and recomputing each touched branch reads its untouched
siblings — but it is *written* only where a child actually changed (a few thousand sparse
paths). So the high-volume, highly-cacheable traffic on the trunk is the **unfold reads**;
caching depths 0..T converts them to hits. The writes are few and the cache's
update-in-place semantics exist to keep those reads **coherent**, not as a performance win.
Evidence (PR0): `branchFromCacheOrDB` = 85% of warmer CPU, account read-hit rate 99.8%,
~39k branch reads/block — the cost is reads, and the trunk is read-hot but write-cold.

The "11% ineffective" verdict came from a *flat* cache averaging the **read-hot trunk**
(high cross-block read reuse) with the **read-cold leaves** (rewritten each block, low read
reuse). Stratifying by depth and caching the read-hot trunk is what makes it effective —
pure read economics, independent of the write side.

PR2's sparse-node upper levels (depths 0..T) *are* the pinned trunk: the fold's unfold at
those depths reads the trunk as hits instead of cgo→MDBX. #21380's trunk and PR2's sparse
node converge on one object. Unwind safety already exists (`UnwindTo` watermark eviction).
The adaptive trunk-pin controller (`onMiss` → per-contract promotion) is erigon's analog of
reth's prefix-set-driven node retention.

## 6. Integration point — #21416's pipeline, new mode

PR #21416 ("BAL-driven parallel commitment") already builds the mode-dispatched parallel
pipeline in the calculator: `blockRequest` enqueued at **block-arrival** →
`handleBlockRequest` selects `calcModeBALDriven` (BAL present) **else `incremental`** →
`maybeFoldAhead`/`foldGateOpen` overlaps commitment-of-N with exec-of-N →
`shadowCrossCheck` recomputes incrementally and asserts the roots match (incremental = the
proven oracle).

Our work adds a third mode, **`incremental-streaming`**, occupying the same early-start
slot as BAL-driven but fed by the sparse trie from the live `txResult` stream instead of
`LoadFromBAL`. It upgrades #21416's current `incremental` mode from "slow block-end oracle"
to "early-start, no-BAL". `shadowCrossCheck` validates it for free against the block-end
oracle (and against BAL-driven where a BAL exists).

## 7. Node data model (PR2)

Extend the prefix node with:

- cell payload — reuse HPH's `cell` (account/storage addr, balance/nonce/codeHash or
  storage value, extension, hashedExtension). The typed `VersionedWrite[T]`
  (`ValU256`/`ValU64`/`ValBytes`) from the perf stack feeds this directly, no re-boxing.
- `hash [32]byte` + `hashLen` — cached subtree hash.
- `dirty bool` — hash invalid.
- pre-state slot — the loaded DB branch (untouched siblings' hashes), fetched via
  `TrieContext.Branch(prefix)` on first touch, prefetched by the `Warmuper`.

Operations:

- `Insert(hashedKey, cell)`: walk/split (existing), set leaf cell, clear cached hashes
  from leaf up to the trunk boundary.
- `Hash(node)`: if clean → return cached; else ensure pre-state loaded, recurse touched
  children, take untouched sibling hashes from pre-state, `hashRow`, cache, clear dirty.
  Reuses #21238's tested merge machinery (`loadSiblingsIntoGrid` / deposit / fold).

## 8. Correctness strategy

- Cardinal rule (from #21238): every test asserts **byte-equal root vs sequential
  `HexPatriciaHashed`**.
- **Fuzz over insert order** — unordered insertion must yield an identical root regardless
  of tx order. This is the load-bearing invariant; fuzz it hardest.
- Edge cases: deletions, account/storage depth-64 boundary, all-deleted subtrees,
  single-touched-key partitions, empty partitions.
- Runtime: `shadowCrossCheck` (#21416) runs streaming as the fast path and cross-checks
  against the trusted block-end oracle per block; divergence is logged before any cutover.
- Race detector green on the parallel path; cache `BRANCH_CACHE_VERIFY` divergence counter
  must stay zero.

## 9. Metrics

- **Primary: unfold read-hit rate by depth** (`rootHits`/`pinnedHits`/`tailHits`, already in
  `branch_cache.go`). The trunk is justified on READS — the upper trie is read every block,
  written rarely; the win is converting the unfold reads to hits. Watch the trunk (depths
  0..T) hit rate, not write traffic.
- Cache-on **and** cache-off baseline runs — never claim a cache win without the miss-path
  (cgo→MDBX read) baseline.
- Streaming vs block-end commitment wall-clock, and `max(exec,trie)` vs `exec+trie`.
- Dirty-rehash count per branch per block (secondary — bounds speculative-fold overhead, not
  the cache rationale).

## 10. Tracking & merge order

Build on the perf stack as-designed; do **not** detach. The parked cache stack
(#21380 BranchCache static trunk → #21386 LRU → typed-VW → #21416) is awaiting a 3.5 branch
cut, after which it merges incrementally to main.

Merge order to main (all gated on the 3.5 cut):
parked track → #21416 → #21238 (hardened) → PR2 → PR3 → PR4.

Branch: `mh/incremental-commitment` on top of #21416. Development can start now; it rebases
forward as the parked track lands. Not `performance` (30,986 commits divergent, lacks the
streaming calculator entirely; inherits via periodic main-merge).

## 11. PR breakdown (the plan)

- **PR0 (pre-work, no code): reference study.** reth `trie` + `trie-sparse` crates
  (prefix sets, revealed/blinded nodes, in-memory `TrieUpdates`/`TrieInput` carried across
  unpersisted blocks) + erigon's `branch_cache.go` and `mh/commitment-storage-trunk-pin`.
  Output: the canonical retention/invalidation model written up as the design reference.

- **PR1: #21238 hardened.** Land prefix-trie + `ParallelPatriciaHashed` with: sequential
  auto-fallback on the no-split reject path (so it can never fail a block), explicit
  disposition of the broken `ConcurrentPatriciaHashed` variant, comment cleanup to repo
  style, and a production-hardware bloatnet benchmark.

- **PR2: sparse hashing node.** Grow the prefix node (§7). Lazy hashing only (block-end
  DFS that skips clean subtrees). Byte-equal + insert-order fuzz tests. No concurrency yet.

- **PR3: streaming driver + static split.** Static top-down split at depth D; deterministic
  lock-free router; single-writer trunk merge. Wire `incremental-streaming` into #21416's
  `handleBlockRequest`; drive from the `txResult` stream instead of buffering into
  `cc.state`. Validate via `shadowCrossCheck`. Disjoint-prefix worker discipline at the
  orchestrator (cache stays lock-free).

- **PR4: eager hashing + cache effectiveness.** Background per-partition hasher folds
  settled subtrees during exec on idle cores; re-dirty on late insert. Foreground-priority
  yield so it never steals cores from exec. Full cache-on/off measurement and the
  depth-bucketed dirty-rehash metric; decide D and pin policy from the data.

- **PR5 (optional): adaptive sub-split** within hot static partitions; tune trunk depth.

## 12. Risks & open questions

- **CPU contention** (eager hasher vs exec) — mitigate with foreground-priority yield;
  ship lazy (PR2) first to de-risk.
- **Static split imbalance** — D tuning; adaptive sub-split as fallback (PR5).
- **Pre-state merge edge cases** (deletions, depth-64 account boundary) — mitigated by
  reusing #21238's tested fold path.
- **Cache coherence under repeated in-block rewrite** — the trunk's write-in-place
  semantics must hold under the streaming dirty-rehash loop; `BRANCH_CACHE_VERIFY` guards.
- **Memory** — one block's touched set with values held resident; bounded by block
  touched-key count. The trunk is resident across blocks by design (the cache).
- **Trunk depth D = 5** (exec-context value, resident trunk ≈ 2^5 entries). Confirm exact
  semantics against the exec-context constant in PR0; PR4 metrics can retune.
- **Open: does the typed-VW cell payload cover all domains** the fold needs, or do code/
  commitment domains need extra handling? Resolve against the typed-VW API when on-stack.

## 13. PR0 findings — reth + erigon reference study

**reth (`/tmp/reth/crates/trie`) confirms the design end-to-end:**
- Sparse trie nodes are `Empty|Leaf|Extension|Branch` with `Dirty` / `Cached{rlp_node}`
  state; blinded children carry stored hashes (`sparse/src/trie.rs`, `parallel.rs:323-352`).
- **Unordered insertion is first-class**: `update_leaves` drains updates in arbitrary
  order; each `update_leaf`/`remove_leaf` is atomic (reverts on a blinded-node hit and
  re-queues a proof request). No pre-sort (`parallel.rs:832-913, 1039-1150`).
- **Prefix set** is the changed-path tracker: `PrefixSetMut` (unsorted, O(1) insert) →
  `freeze()` → sorted `PrefixSet` with a stateful cursor (“inspired by Silkworm”) giving
  amortized-O(1) sequential `contains` during the hash walk (`common/src/prefix_set.rs`).
- **Cross-block retention**: `TrieUpdates` (intermediate nodes only) + `TrieInput`
  (multi-block aggregation) + a `SparseStateTrie` that is *cleared, not dropped* and reused
  (`into_trie_for_reuse`). **Upper trie at depth ≤ 2 is always resident** (`common/src/
  input.rs`, `engine/.../sparse_trie.rs`).
- **Parallelism is lock-free by disjoint ownership**: 256 lower subtries dispatched via
  rayon; storage tries taken out of the map (`take_or_create_trie`), processed, re-inserted;
  no mutexes. Upper trie finalized serially after lower subtries. Streaming via an
  `OnStateHook` → crossbeam channel → background state-root task (`parallel/src/
  state_root_task.rs`, `sparse/src/parallel.rs:300-354`).
- Metrics separate lower-parallel vs upper-serial latency and account/storage cache
  hit/miss; no hard-coded perf targets in source.

**erigon prototype (`origin/mh/perf-caches-pr`) provides the cache half:**
- `BranchCache` 3-tier (root atomic-pointer / pinned maphash / LRU tail) with per-tier
  hit/miss stats, update-in-place on pinned `Put`, `onMiss` hook, `UnwindTo` watermark
  eviction for reorg safety (`execution/commitment/branch_cache.go`).
- `AdaptivePinController` (`adaptive_pin.go`): per-contract miss-pressure attribution,
  promote (≥100 misses, ≤8 contracts, 4 MB initial budget) / extend (+8 MB/block) /
  demote (5 cold blocks) — erigon's analog of reth prefix-set retention.
- `ConcurrentPatriciaHashed` mounts at **D=1** (16 disjoint first-nibble mounts, `rootMu`
  only for the in-memory root-cell merge; cache writes are lock-free by disjoint prefix).
- Measured: account cache hits 99.8%, storage 100%; `branchFromCacheOrDB` = 85% of warmer
  CPU; ~39 k distinct branch reads/block (work-volume, not IO, is the limit). The
  "11% ineffective" verdict was a *flat* cache averaging the hot trunk with cold leaves —
  the 3-tier design is the fix.
- **D=5 not found in code** — the only "D=5" reference was this design doc itself. Treat
  D as open (§4); evidence favours D=2.

## 14. Performance expectations

Baseline (issue #17, erigon on in-mem KV, matched mainnet-tip payloads): total 107.8 ms,
state-root wall **39.5 ms** (single largest gap vs reth's 2.5 ms), exec wall ≈ 57.8 ms
(EVM CPU 25.2 + exec IO 32.6), everything-else 10.5. p50 ≈ 101 ms.

Decomposition of the 39.5 ms: unfold IO ~14–20 ms, fold CPU ~12–16 ms, serial
orchestration ~6–10 ms — all serial, all on the critical path.

Projected after this feature (ranges; reth's shipped 2.5 ms is the mature floor):
- **Overlap** (structure + unfold-IO prefetch concurrent with exec): removes ~14–20 ms of
  IO and the orchestration from the critical path.
- **Parallel fold** (§4, D-way): compresses the ~12–16 ms fold CPU to a ~2–5 ms boundary
  burst. Co-dependent with overlap — reth's low tail needs both.
- **Trunk cache** (§5): shrinks the serial spine finalize and upper-branch reads (~1–3 ms).
- **Residual non-overlappable tail** (parallel fold of late-dirtied partitions + serial
  spine + channel drain): **~2–7 ms** (first implementation; reth = 2.5 ms mature).

**Bottom line:** state-root 39.5 → ~2–7 ms ⇒ **~33–37 ms off newPayload, p50 ≈ 101 → ~71–75 ms.**
This does **not** by itself reach reth's ~26 ms — the remaining gaps are exec IO
(32.6 vs 6.7) and everything-else (10.5 vs 0.2) and EVM CPU (25.2 vs 17.0), which need the
separate exec-IO / exec-parallelism / plumbing work (issue #17). This feature is the single
biggest state-root lever; it is necessary but not sufficient for parity.

Key implementation lessons from reth, folded into §3/§4:
1. **Defer hashing** to the boundary with prefix-set marking — do not eager-rehash per tx
   (avoids speculative-refold overhead, handles last-write-wins for free).
2. **D=2 / 256 partitions** is the proven granularity; D=5 is over-partitioned.
3. **Threshold the parallelism** (`min_changed_keys`) so small blocks fold serially and
   don't pay fan-out cost.
4. **Reuse, don't reallocate** the sparse trie across blocks (clear + retain allocations);
   the resident upper trie is the trunk cache.
