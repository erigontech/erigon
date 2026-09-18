# Commitment v4 — trie encoding, and what the v2/v3 comparison actually showed

Handoff, 2026-09-18. Status: investigation complete, one cut landed, design open, one measurement
blocking.

Base: `origin/main` `d03460b07e6`. Branch `awskii/commitment-drop-dead-surface`.
Bench host: this Mac, Apple M5 Max, 18 logical CPUs. Reth read at `8db774c86c`.

Companion files:

- `docs/backlog/commitment-droppable-surface.md` — the droppable-code inventory, not repeated here.
- `~/org/mode/e/research/v2-trie-vs-parallel-commitment.org` — all measurements with sources,
  including four retired claims.

## How this started and where it went

It began as "what can be dropped from hex parallel commitment as unnecessary". That produced the
backlog inventory and one landed cut (`3d46915899c`, −40 net), and the conclusion that the parallel
engine itself is lean — the droppable surface inside `fork_walk.go` / `split_point.go` /
`prefix_trie.go` / `parallel_mount.go` was ~63 lines. Everything substantial is in satellites.

It then became a benchmark of erigon v2's trie (`execution/commitment/trie`) against the v3 parallel
trie, and finally a comparison of how each *encodes* the trie, because that is where the two
actually differ.

## Read this part first: which numbers transfer

The benchmark corpus is `whale1M()` — 750K slots under one account, plus 150K and 5K whales and a
95K-account tail. That is not mainnet's shape. A mainnet block scatters across ~1–2K accounts with
much more account-trie traffic and shallower touched branches.

**Structural — corpus-independent, safe to reason from:**

- Only changed paths are recomputed; sibling hashes come from the parent record. Measured as zero
  re-reads and zero empty reads.
- Which fields a v3 cell carries (`extension`, `accountAddr`, `storageAddr`, `hash`, `stateHash`).
- Reth's node size: `6 + (root_hash ? 32 : 0) + hash_mask.count_ones() × 32`.
- Referenced commitment files pay a random read per key on every branch read.

**Corpus-dependent — do not carry into a design decision without a mainnet number:**

- the 85.5% `stateHash` memo hit rate
- 91% of read bytes sitting at depth 33–36
- the 504 B average record and its 56.5 / 23.7 / 14.1 composition
- 3.1 branch records read+written per updated slot

## The v2 vs v3 benchmark

Both arms produce the same root; gated by `TestLegacyVsHexRoot` (4 corpus shapes to a 50K whale) and
`TestIncrementalRootsAgree`. The only encoding bridge needed was `CodeHash` — `UpdateBuilder` leaves
it zero where the hex trie defaults to `empty.CodeHash`. A corpus artifact, not a trie difference.

**Bulk, from empty, 1,095,003 keys**, medians of 3 runs × 3 iters, all arms raw plain keys → root:

| arm | cores | ms/op |
|---|---|---|
| LegacyV2 | 1 | 682 |
| HexSeq | 1 | 1,271 |
| HexPar | 1 | 1,345 |
| HexPar | 18 | 598 |

v2 is 1.97× faster per core. HexPar at w1 is slower than HexSeq — the fork machinery costs ~6% with
nothing to parallelize. Parallel efficiency 1→18 workers is 2.25×, i.e. **12.5%**.

**Incremental, 500-slot delta on an already-built whale**, medians of 3 runs × 200 iters:

| base | arm | ms/op | B/op | allocs/op |
|---|---|---|---|---|
| 1M | LegacyV2 | 0.201 | 96 KB | 1,503 |
| 1M | HexSeq | 3.61 | 1,193 KB | 5,105 |
| 1M | HexPar w1 | 3.80 | 1,357 KB | 5,467 |
| 1M | HexPar w18 | 1.82 | 1,710 KB | 6,136 |

v2 is 18× faster. Its cost is flat in state size (0.191 ms at 100K → 0.201 ms at 1M); hex's doubles
for 10× the state (0.921 → 1.82 ms).

### The 18× is residency, not algorithm

This is the single most important correction in the whole investigation, and it was reached twice —
once by measurement and once by the reviewer's own argument that 2B leaves cannot be resident.

Both engines recompute only the changed paths. Verified on the v3 side:

- `computeCellHash` (`hex_patricia_hashed.go:1270`) — `case cell.hashLen > 0: storageRootHash =
  cell.hash`. An untouched child contributes its stored hash with no descent and no read.
- 1,546 reads over 1,546 distinct prefixes: zero re-reads, **zero empty reads**. v3 never probes for
  a branch row that is not there, so it needs no `tree_mask` equivalent.

The gap is the serialization boundary. Per round v2 crossed it zero times — it walked pointers in a
resident decoded graph, invalidated the dirty paths and rehashed. v3 crosses it 3,092 times: 1,546
record reads with decode, 1,546 re-encodes with writes.

Two things make the 18× unusable as a target:

1. v2 was benchmarked **fully resident**, which is not how it ran. Its design was a partial cache
   driven by a per-block `RetainList` (`retain_list.go:272`), with `HashNode` as the not-loaded stub
   and `EvictNode` shrinking it back. The half that refilled it is deleted from this tree — no
   `flatdb_sub_trie_loader.go`, no `TrieOfAccountsBucket`, `TrieDbState` has zero references. Read
   the 18× as an upper bound on what a perfect in-memory cache could buy.
2. It measures 229 bytes/key retained (23.6 MB at 100K keys, 250.7 MB at 1.095M). At mainnet scale
   that is hundreds of GB. Off the table.

## The encoding comparison

### v3's record

Measured on a 500-slot delta at 1M: 1,546 records, 779,280 bytes, **504 B average**, 11.1 cells each.

| field | bytes | share | cells |
|---|---|---|---|
| `hash` | 440,416 | 56.5% | 13,763 |
| `storageAddr` | 178,620 | 22.9% | 3,435 |
| `stateHash` | 110,016 | 14.1% | 3,438 |
| `accountAddr` | 5,720 | 0.7% | 286 |
| `extension` | 106 | 0.0% | — |

Plain-key references are **23.7%** — 52 bytes per storage cell, 20 per account cell.

### Reth's node

`BranchNodeCompact` (alloy-trie 0.9.5 `nodes/branch.rs:262`; `Compact` impl in reth-codecs 0.7.1
`alloy/trie.rs:52`):

```
6 bytes (state_mask, tree_mask, hash_mask — 2 each)
+ 32 if root_hash is Some (trie root only)
+ hash_mask.count_ones() × 32
```

No keys, no extension, no leaf data. `hashes.len() == hash_mask.count_ones()` is asserted at
`branch.rs:300`; children in `state_mask` but not `hash_mask` are leaves and store nothing. A node
is written only if `!tree_masks[len].is_empty() || !hash_masks[len].is_empty()`
(`hash_builder/mod.rs:437`), so leaf-only branches get no row.

Stored in `AccountsTrie` (Key = `StoredNibbles`) and `StoragesTrie` (Key = hashed address, SubKey =
`StoredNibblesSubKey`, DupSort) — the same "keyed by nibble path" placement v3 uses. Leaves live in
separate `HashedAccounts`/`HashedStorages` tables.

At v3's measured 8.9 hash-carrying cells per record, reth's node would be 291 B against 504 B.

### Where the difference comes from

Erigon v3 keys state by *plain* address; the trie is keyed by *hashed* nibbles. Every cell carries
the plain key to bridge the two. Reth keeps a hashed-state index, so the trie path is the lookup key
and the bridge costs zero bytes.

Erigon had that index. `db/kv/tables.go:465` lists `HashedAccountsDeprecated` and
`HashedStorageDeprecated` in `ChaindataDeprecatedTables`, dropped by the `drop_legacy_e2_tables`
migration. **The plain-key overhead is the price of that deletion.** Reth did not improve on the
design; v3 is the side that diverged.

### Key referencing does not remove the cost

`domain_committed.go:133` — every read of a referenced commitment record calls
`ExpandShortenedKeysInBranch`, which looks each shortened key up in the Accounts and Storage `.kv`
files. `aggregator.go:2058` states the consequence: "With referenced commitment files present,
concurrent dereference does random reads". Referencing trades bytes for a random lookup per key per
branch read. `branchKeyDerefSpent` measures it, behind `KV_READ_METRICS`.

So the plain-key overhead is worse on disk than the 23.7% suggests, not better.

## The v4 shape

Bring back v2's encoding, informed by what v3 learned.

**Take from v2:** key-free branch nodes, leaves out of the trie table, and the hashed-state index
that makes both possible.

**Keep from v3:** the parallel fork walk — neither v2 nor reth's DB-backed `StateRoot` has one. The
domain/step/merge file architecture. The invariants. `BranchCache` benefits: ~40% smaller nodes and
no dereference make a decoded-cell tier cheaper than when it was costed against 504-byte records.

**Delete:** the entire referencing subsystem, which is mitigation for a cost the index removes at
the root — `ReplacePlainKeys`, `HasShortenedKeys`, `CountPlainKeys`, `ExpandShortenedKeysInBranch`,
`DecodeReferenceKey`, `CommitmentBranchReferenced` and the `.kv` version gate,
`commitmentMergeNeedsTransform`, `commitment_convert.go`, and the `seqReadahead` special case that
exists because dereference does random reads. None of it is trie logic.

**Pay:** a second hash-keyed copy of state. Open question whether it needs history — it is derivable
from the accounts and storage domains, so it may be an index rather than a full Domain, which is a
materially smaller commitment than v2's version.

### Only one of the two encoding changes is a clear win

- **Key-free nodes** win twice: fewer bytes *and* no dereference random reads.
- **Leaf-hash removal is a trade, not a win.** v3's `stateHash` memo measured an 85.5% hit rate on
  the whale corpus — 3,183 skipped loads against 538 real ones in a 500-slot round. Dropping it
  converts those 3,183 hits into hashed-state lookups plus keccaks, ~6.4 per updated slot, because a
  changed branch's unchanged siblings must be re-derived. That is what reth pays for key-free nodes,
  and why its hashed-state tables have to be fast. **The 85.5% is corpus-dependent and the mainnet
  figure decides this one.**

### What v4 can and cannot deliver

It does not recover the 18×. That number came from never crossing the serialization boundary, and
v4 still crosses it 3,092 times per round — it just carries less per crossing.

The realistic ceiling is bounded by record-volume reduction in a round that is allocation-bound:
decode and encode are only ~2.4% of CPU (`fillFromFields` 1.16%, `EncodeBranch` 0.55%,
`cellEncodeDataFromCell` 0.67%), while GC and allocator work dominate. 504 → 291 B is −42% of record
volume, or roughly −24% if `stateHash` is kept. Plus deleting the dereference random reads, which
cannot be sized from a `MockState` benchmark.

Tens of percent, not 18×.

## Blocking measurement

One run answers both open v4 questions. Both counters already exist behind `KV_READ_METRICS=1`, and
both need a synced mainnet node at debug verbosity — a remote run, not this Mac.

1. **`skipRatio` / `resetRatio`** — logged at `hex_patricia_hashed.go:2720`. The real `stateHash`
   memo hit rate. Decides whether leaf-hash removal is a win or a loss. Note the counters are global
   and never reset ("no reset" in the log message), so sample deltas rather than reading once.
2. **`branchKeyDerefSpent`** — what `ExpandShortenedKeysInBranch` costs today on referenced files.
   Prices the half of v4 that is already unambiguous, and tells you how much of it is urgent.

Until (1) lands, v4 should be specified as key-free nodes only, with `stateHash` retained.

## Cheaper work that does not wait on v4

Both measured, both in the engine, both far smaller than a storage redesign.

- **The touch phase is 69% of the parallel round at 1M** (415 ms of 599 ms), single-threaded, which
  is why parallel efficiency is 12.5%. No benchmark in the package times it — `runParallelBench`
  wraps `WrapKeyUpdates` in `b.StopTimer()` (`parallel_streaming_bench_test.go:64-77`), and
  `runIncrementalParallelBench` does the same at `:432`. Every published figure is fold-only.
- **Duplicate account touches cost 209 ns and 3 allocations each.** `TouchPlainKeyDirect` hashes
  before consulting the dedup map (`commitment.go:1553`) while `WriteSet.TouchUpdates`
  (`versionedio.go:1876`) walks five maps over the same address space, so every EOA sender pays the
  keccak at least twice per block. Merging the account loops measured −49% / −1.15 MB / −12,000
  allocs on 4,000 addresses. Note `TouchPlainKey` does *not* have the defect — it probes the map
  before hashing; the asymmetry is the bug.

## Not examined

- **Why `HashedAccounts`/`HashedStorage` were dropped.** v4's core proposal reverses that migration
  and should be argued against whatever drove it. First thing to read.
- **`crates/trie/parallel` and the arena sparse trie in `crates/trie/sparse`.** Reth's engine-path
  answer to in-memory residency, holding decoded nodes rather than bytes. Only the DB-backed
  `StateRoot` path was read. If v4 wants parallelism *and* residency, that is closer prior art than
  anything here; its eviction bound and cross-block lifetime are unknown.
- **The on-disk referenced record size.** Only the unreferenced in-DB form was measured.

## Artifacts

- `3d46915899c` on `awskii/commitment-drop-dead-surface` — the dead-surface cut, −40 net, package
  and `db/state` tests green.
- `docs/backlog/commitment-droppable-surface.md` — droppable inventory.
- `~/org/mode/e/research/v2-trie-vs-parallel-commitment.org` — 19 facts with sources, 7 ideas,
  4 retired claims.
- Scratch harness, **uncommitted**, in `execution/commitment/`: `zz_legacy_cmp_test.go` (root
  oracle), `zz_legacy_bench_test.go` (bulk arms), `zz_incremental_bench_test.go` (delta arms),
  `zz_incr_shape_test.go`, `zz_depth_shape_test.go`, `zz_record_shape_test.go`,
  `zz_statehash_test.go`.

`TestLegacyVsHexRoot` and `TestIncrementalRootsAgree` are worth promoting out of scratch — they are
the only thing in the repo that would catch a root divergence between the two implementations.
