# Commitment v4 — a new trie, designed against what the previous three cost

Design, 2026-09-21. Base `awskii/commitment-drop-dead-surface` (`55fba30c7c1`), worktree
`wt/pc-drop-main`.

Supersedes the shape section of `20260918-commitment-v4-trie-encoding.md`. That file's measurements
stand; its "v4 shape", its v2/v3/v4 lineage, and its reading of the record composition do not.

Inputs: `~/org/mode/e/research/nethermind-commitment-recon.org` (30 facts on Nethermind),
`~/org/wrk/state-scan/ethereum-state-report.md` (mainnet state, step 9,443).

## Naming — these are not a lineage

Three separate attempts at one problem. v4 is a fourth, from zero.

| name | what it is |
|---|---|
| **E2 trie** | erigon 2. `execution/commitment/trie`. Hashed state tables, a resident pointer-graph trie, `GenStructStep` / `HashBuilder` / `RetainList`. |
| **parallel commitment** | what runs on main today. `HexPatriciaHashed` + the fork walk. No version number. |
| **commitment v3** | a separate take aimed at IO: referenced branches, (file, offset) in place of plain keys. Deprecated, being deleted. |
| **v4** | this document. |

## The numbers

Measured, mainnet step 9,443:

| | |
|---|---|
| accounts | 406,456,481 — 14.73 GiB |
| storage slots | 1,620,960,991 — 78.40 GiB |
| code | 85,313,673 records — 16.97 GiB |
| **all state** | **110.10 GiB** |
| **commitment domain** | **256.48 GiB** — 2.33x all state combined |

The trie costs more than twice the state it commits to. That, not CPU, is the size of the prize.

**Mainnet is the target network.** Every figure below is judged against it, and where a corpus
number and a mainnet number disagree the corpus number loses.

Everything else quoted in the 2026-09-18 draft — 504 B average record, 11.1 cells, 1,546 records
per round, 85.5% `stateHash` hit, 91% of read bytes at depth 33-36, 23.7% plain keys — is from the
`whale1M` corpus: 750K slots under one account. **None of it is a mainnet number** and none of it is
used below as a design input.

### The fan-out identity, and why the corpus numbers mislead

Edge count in a trie is exact: every node but the root has one parent, so

```
branches x fanout = branches + leaves - 1      =>      branches = leaves / (fanout - 1)
```

With 2,027,417,472 leaves measured:

| fan-out | branch records | what a record is mostly made of |
|---:|---:|---|
| 11.1 (the corpus) | 201M | sibling branch hashes |
| 4.7 | 549M | **leaf cells** — 3.7 of every 4.7 children is a leaf |

These are different tries and they want different records. At low fan-out the record is dominated
by leaf cells, which is where the plain keys live — so the draft's "56.5% sibling hashes / 23.7%
plain keys" split *understates* what removing plain keys is worth, possibly by a lot.

Which one mainnet is, is unmeasured. **M1 settles it and everything sized below is provisional on
it.**

## What each previous attempt cost, and the rule that comes out of it

**E2 trie — residency you cannot bound is not a design.** 18x faster on an incremental round because
it never crossed the serialization boundary: pointers in a decoded graph, invalidate the dirty
paths, rehash. Benchmarked *fully resident*, which is not how it ran, at 229 bytes retained per key
— hundreds of GB at mainnet. The half that refilled it from disk is gone: no
`flatdb_sub_trie_loader.go`, and `TrieDbState` has no constructor call anywhere in the tree.
**Take:** the node model, the bottom-up streaming construction. **Avoid:** an unbounded live graph.

**Parallel commitment — the grid is why the parallel seam is nine functions instead of two.** It got
the hard part right, and all of it is already independent of the trie underneath: a prefix trie of
the batch (`prefix_trie.go`), a `subtreeCount` split rule (`fork_walk.go:146`), grain sizing
(`fork_walk.go:39`), a bounded lease pool and work-stealing fan-out (`fork_walk.go:49,202`). What it
pays for is the representation: a 128x16 `cell` grid whose rows a fork must copy and whose folding
needs a mount wall; a plain key in every cell to bridge plain-keyed state to a hash-keyed trie; a
*partial* record that needs merge-on-write; a single-threaded touch phase.
**Take:** the fork walk, unchanged. **Avoid:** the grid, the plain keys, the partial record.

**commitment v3 — it saved disk and throttled IO, and that is a loss.** Replacing plain keys with
(file, offset) references shrank the record and added a random read per key per branch read. The
subsystem is deprecated and being deleted; v4 simply never needs it.

> **The rule this yields, and the one every choice below is made against:**
> **pay disk, never IO.** The fold path must do zero reads beyond the branch records it is already
> walking. A byte saved on disk that costs a read is a regression.

## 1. The record

### Key — packed, using the encoder that already exists

Commitment keys are raw nibbles today: `PutBranch` hands the prefix straight to `DomainPut`
(`commitmentdb/commitment_context.go:1056`). One byte per nibble.

Use `HexToCompact` / `HexToCompactInto` (`execution/commitment/nibbles/nibbles.go`,
`need := len(hex)/2 + 1`). No new code, no new encoding to specify.

| record at depth | key today | packed |
|---|---:|---:|
| account trie, ~6 nibbles | 6 B | 4 B |
| storage trie, 64-128 nibbles | 64-128 B | 33-65 B |

Storage-trie records outnumber account-trie records 4:1 by leaf count, so this is the larger half.
It costs prefix ordering across parities — `HexToCompact` puts the odd/even flag in the first byte,
so `bc` (even) sorts before `abc` (odd). Nothing needs path order on this domain: it is
`AccessorHashMap`, every fold lookup is an exact prefix, and merges need *a* total order, not path
order.

### Value

```
byte 0        flags:  format version | hasExt | hasEmbedded
bytes 1..2    childMask u16 BE      which of 16 nibbles have a child
bytes 3..4    leafMask  u16 BE      subset of childMask: that child is a leaf
[bytes 5..6]  embeddedMask u16 BE   present only if hasEmbedded; subset of leafMask
then          32-byte slots, one per set bit of childMask, ascending nibble
                branch child  -> its node hash
                leaf child    -> its leaf hash
                embedded leaf -> [len u8][rlp <= 31 B]
trailer 1     [if hasExt] extLen u8, then the extension nibbles packed two per byte
trailer 2     for each bit of (leafMask &^ embeddedMask), ascending: the leaf's hashed path
              SUFFIX, packed two nibbles per byte
trailer 3     for each bit of (leafMask &^ embeddedMask), ascending: [len u8][the leaf's value]
```

Child *k*'s hash is `rec[hdr + 32*rank(childMask, k):][:32]`. **No decode, no allocation, no
varints, no per-child length byte.**

**No `refLen`.** A length byte in front of every child reference costs 1 B x fan-out to say what a
2-byte `embeddedMask` says once per record — 11 B against 2 B at fan-out 11, 5 B against 2 B at
fan-out 4.7. Embedded children are rare (only leaves whose RLP is under 32 B, which needs a short
suffix *and* a small value), so `hasEmbedded` keeps the mask off most records entirely.

**Trailer 2 carries no length byte.** Every key in the trie is the same length, so a leaf's suffix
is `keyLen - depth - 1` nibbles, with `keyLen` 64 above the account boundary and 128 below it
(`cell.hashedExtension` is `[128]byte`, `hex_patricia_hashed.go:313`). `depth` is
`len(prefix) + extLen + 1`, both in the record. Fixed stride.

**The extension hangs off the node, not each child.** `HexPatriciaHashed` puts it on the cell
(`hex_patricia_hashed.go:314`) — up to sixteen variable-length fields, which is exactly what makes
its record a sequential decode. One per record here, and the measured volume is nil: the corpus's
`extension` field totalled 106 bytes across 1,546 records. The alternative — not persisting
extensions and recovering a child's real path with an ordered seek — is not available: the
commitment domain is `AccessorHashMap`.

Only leaves can be embedded, so `embeddedMask ⊆ leafMask` by construction: a branch RLP is a
17-item list with at least two 32-byte references (>= 84 B) and an extension RLP is
`list(hexPrefix, hash)` (>= 35 B). Neither falls under the 32-byte inlining threshold.

### What the format buys, ranked

1. **Decode-free reads.** `fillFromFields` is 1.16% of CPU, but the round is allocation-bound — GC
   and allocator dominate while decode+encode together are 2.4%. Killing the decode kills the
   allocations behind it, and that is the larger half.
2. **No plain keys anywhere.** Nothing to dereference, ever.
3. **A complete record.** The in-memory node holds all its children, so encode emits a whole record
   and the merge-on-write disappears (§4).
4. **Fixed stride.** An untouched run of children is one `copy`. This falls out; it is not a reason.

## 2. The leaf payload, and why it stays in the record

The one real choice. Four ways to carry a leaf in its parent:

| | per leaf child | fold-path reads | extra keccaks |
|---|---|---|---|
| parallel commitment today | plain key + leaf hash, 84 B | 1 deref + 1 domain read on memo miss | on miss |
| A — hash + suffix | 60 B | one value read per split/collapse | none |
| B — suffix + value, no hash | 44 B | none | one per leaf per fold |
| **C — hash + suffix + value** | **76 B** | **none** | **none** |
| D — hash only | 32 B | one read per split/collapse | none |

A and D are the v3 mistake in miniature: they shrink the record by moving a lookup onto the fold
path. The rule above rejects both. B is the reverse trade and it is dead on the target network, not
on the corpus: **mainnet memoises ~80%, so only 1 account in 5 reaches disk today** (maintainer).
Dropping the stored leaf hash converts those four-in-five memo hits into a read plus a keccak
apiece. That is what `stateHash` was bought for; reth pays it only because its hashed tables are an
MDBX lookup rather than a domain read.

C goes further than preserving that 80%: it takes the remaining 20% to zero. Those reads exist
because a memo miss has to fetch the account value by plain key; under C the value is already in the
parent record, so there is nothing to fetch.

**C.** The record is self-sufficient: a changed leaf's value arrives with the update stream, an
unchanged leaf's hash is in its parent, and a leaf that must *move* — the split and collapse cases —
has its suffix and its value in its parent too. **The fold reads branch records and nothing else,
ever.**

Record size, `5 + 32*fanout + leafChildren*(suffix + 1 + value)`, at ~44 B of suffix+value per leaf:

| fan-out | leaf children | record | vs 504 B |
|---:|---:|---:|---:|
| 11.1 (corpus) | 2.2 | 457 B | −9% |
| 4.7 | 3.7 | 318 B | −37% |

Plus the key saving above. The spread is why M1 comes before the format is frozen.

## 3. The hashed domains (R2)

```
kv.HashedAccountsDomain   key keccak(addr)               32 B   value = the accounts domain value
kv.HashedStorageDomain    key keccak(addr)|keccak(slot)  64 B   value = the slot value
```

**No history on either**: `HistoryDisabled: true`, `SnapshotsDisabled: true`, `IiCfg.Enabled: false`.
No history values table, no inverted index — nothing registered, so nothing written and nothing
pruned. Not an optimisation to revisit: both are a pure function of the accounts and storage
domains, which do keep history, so an unwind rebuilds them rather than unwinding them. The schema
already expresses these flags (`db/state/statecfg/state_schema.go:289`), so no new machinery.

`Accessors: AccessorBTree`, for `Cursor.Next()` (`db/datastruct/btindex/btree_index.go:117`;
`nextNoRead` at `:104` advances without the binary search). Iteration, not repeated seeks, is the
access pattern these serve.

**Under §2 nothing on the fold path reads them.** What they are for is cold, and all of it is
iteration:

| caller | pattern |
|---|---|
| snap-sync serving in hashed order | cursor walk |
| rebuild the trie from state without the plain-space sort | full ordered scan |
| proof / witness leaf values | cursor walk — though C already carries these in the records |

Derived price, hashed keys being incompressible: accounts 406.5M x (32 + ~19) ≈ 19 GiB; storage
1.62B x (64 + ~16) ≈ 121 GiB; **~140 GiB**, plus a second domain write per changed key.

So: **build them, build them last, and make them optional.** They are an index a node chooses to
carry, not a dependency of the root. Built without re-hashing state: one offline pass over the
existing `.kv` files sharded by the first keccak byte, then maintained forward at **at most one
keccak per unique changed key per block** (§5) — a cache hit does zero, and unchanged state is
never touched.

## 4. Write volume

A domain is key -> value; there is no partial write. `HexPatriciaHashed`'s fold emits a record
covering only `afterMap`, and `mergeDeferredUpdate` merges it with the stored one
(`commitment.go:325`) before handing the **complete** result to `putBranch` (`:333`). So a whole
record per changed branch is already what happens today, at 504 B.

v4 writes 318-457 B plus a smaller key, and deletes the merge step: the in-memory node holds all
its children, so encode emits a complete record directly. `MergeHexBranches`, `BranchMerger` and
`mergeDeferredUpdate` go. The no-op skip at `commitment.go:321-323` survives and fires *more* often
— today it compares a partial record against a complete one; v4 compares complete against complete,
which is the test that actually means "nothing changed".

The amplification that dominates is the step hierarchy, not the block: a record is rewritten at
every merge level. Nothing measures it (M4). Nethermind's index/payload split (F4/F24) is the known
answer — `path -> NodeRef` at 6 B in the merged file, node bytes in an append-only arena merges
never copy — and it is a storage-engine project with its own costs (an extra `pread` per cold node,
whole-file reclaim). **Not now, and the format does not have to decide now:** this payload is
opaque, key-free, fixed-stride bytes, so moving it behind a `NodeRef` later touches the storage
layer and not one line of the trie.

## 5. The touch phase

69% of the bulk 1M-key round (415 ms of 599 ms), single-threaded, which is why parallel efficiency
is 12.5%. **That is the bulk arm** — `runParallelBench` wraps the Updates build in `b.StopTimer()`
(`parallel_streaming_bench_test.go:64-77`), so every published fold number excludes it, and the
per-block share is unmeasured (M5).

Restructure `Updates` around the type that already exists. `commitment.Update`
(`commitment.go:1961`) is the delta — `{Flags, Balance, Nonce, CodeHash, Storage, StorageLen}` —
and its merge semantics are already written (`commitment.go:1510-1523`). No new delta type.

```
accounts map[[20]byte]*Update
storage  map[[20]byte]map[[32]byte]*Update
wiped    map[[20]byte]struct{}
```

Then:

1. **Dedupe in plain-address space before hashing.** `TouchPlainKeyDirect` hashes before consulting
   its map (`commitment.go:1553`) while `TouchPlainKey` probes first — the asymmetry is the bug.
   `WriteSet.TouchUpdates` (`versionedio.go:1876`) then walks five maps over the same address space,
   so an EOA sender is keccak'd at least twice per block. Merging the account loops measured
   −49% / −1.15 MB / −12,000 allocs on 4,000 addresses.
2. **Parallel keccak over the survivors.** Partition unique addresses and slots across workers.
   Nothing shared.
3. **Sixteen prefix tries sharded by the first nibble.** `prefixTrie` is already per-path
   (`prefix_trie.go:33`), so shards merge at zero cost — the root adopts 16 children. This removes
   the last shared mutable structure from the touch path, and the shards are the fork walk's own
   boundaries.
4. **A process-wide fixed-capacity keccak cache**, key = the input bytes, value = the hash, no
   invalidation (keccak is immutable). Today's memo remembers only the most recent address in a run
   (`keys_nibbles.go:26`) and helps neither across workers nor across blocks. Its value is bounded
   by the cross-block repeat rate, which is unmeasured — so size it after M6, not before.

## 6. Passes — the root before any write (R5)

```
0  TOUCH        parallel    plain-space dedupe -> parallel keccak -> 16 sharded prefix tries
1  SHAPE+HASH   fork walk   build the sparse node graph along touched paths from branch records;
                            hash bottom-up. NOTHING WRITTEN. The root is final here.
2  ENCODE       deferred    on accept only
3  PERSIST      deferred    ApplyDeferredBranchUpdates(deferred, numWorkers, putBranch)
```

Today `Process` fuses 0 and 1 (`HashSort` streams into `followAndUpdate`,
`hex_patricia_hashed.go:2628`), encodes inside the fold, and defers only the write
(`commitment.go:359`, driven from `domain_shared.go:545,567,591,600`). Moving encode out is the
delta and it is cheap — a dirty node is two masks and a slice — and a rejected root then costs zero
encoding.

## 7. Parallelism and the seam

The fork walk is kept as-is. Verified trie-agnostic: `prefixTrie`, the split rule
(`fork_walk.go:146`, `children - largest >= grain`), `forkGrainFor` (`:39`), the lease pool and
work-stealing fan-out (`:49, :202`) touch no `cell` and no `grid`.

The HPH-bound part is nine functions over the grid and the mount wall: `unfoldToRow`,
`openEmptyRow`, `unfoldedPassThrough`, `extendSingleSurvivor`, `closeIfEmpty`, `stitchSplitCells`,
`foldSplitRow` (all of `split_point.go`), plus `mountTo` (`parallel_mount.go:13`) and `foldMounted`
(`hex_patricia_hashed.go:2413`).

Over a node graph it is two:

```go
fork(prefix []byte) *node
join(parent *node, nib int, ref []byte)
```

A worker is handed a pointer, not a copied grid row. There is no mount wall because there is no row
to fold past. `split_point.go` and `parallel_mount.go` mostly delete.

## 8. Residency — build nothing

`BranchCache` (`execution/commitment/branch_cache.go`) is aggregator-lifetime
(`db/state/aggregator.go:424-429`), on by default, and already shaped for this: a root pointer, an
`accountTrunk` for nibble depths 1-4, pinned per-account storage trunks, an LRU tail. It caches
**bytes**.

The v4 record needs no heap decoding, so cached bytes *are* the decoded form. Nethermind's 512 MiB
decoded-node tier (F11) is unnecessary, and its F13 problem — a whole auxiliary map to spot a cached
decoded node whose keccak no longer matches disk after a reorg — never arises, because there is no
decoded copy to go stale. Its per-block byte tier (F12) is worthless here: the measured within-block
re-read rate is zero, 1,546 reads over 1,546 distinct prefixes.

Keep the existing coherence model — entries carry epoch identity and are lazily rejected when stale
(`branch_cache.go:545`), unwind is an O(1) epoch change (`:607`), bytes enter only after commit
(`domain_shared.go:1331`). The cross-block hit rate is the open question and the counters already
exist (`branch_cache.go:69-73`) — that is M6.

## 9. The API seam (R6)

Unchanged: `commitment.Trie` (`commitment.go:87`), `commitment.PatriciaContext` (`:109`),
`commitment.Updates` and every public touch method, `DeferredBranchUpdate`,
`ApplyDeferredBranchUpdates`, `commitmentdb.TrieContext`, `SharedDomainsCommitmentContext`,
`BranchCache`'s public API. v4 is a third `TrieVariant` beside `VariantHexPatriciaTrie` and
`VariantParallelHexPatricia` (`:121`), selected in `InitializeTrieAndUpdates` (`:125`).

Two landmines:

**(a) `Account`/`Storage` take plain keys.** v4 never calls them from the fold — §2 makes the record
self-sufficient and the update stream carries changed values. The one gap is an account whose
*storage* changed but whose fields did not: its leaf hash moves and the trie needs
nonce/balance/codeHash. That gap is what `resetRatio` counts — `cell.stateHashLen = 0;
hadToReset.Add(1)` at `hex_patricia_hashed.go:1249` (singleton storage) and `:1269` (extension hash
under an account), which makes `cell.loaded.account()` false at `:1275` and forces a domain read;
`skipRatio` counts the times the memo saved it (`:1279`). Fix it at the source: on a storage write,
also touch the account (`commitment_context.go:311`). `SharedDomains` has the address and the value
in hand and execution just read that account. The memo stops being *needed* rather than being
missed.

**(b) `witnessCapture` type-asserts `*commitment.HexPatriciaHashed`**
(`commitmentdb/commitment_context.go:331`), and the driver switches concretely on both HPH types for
state and deferred updates (`:532`, `:615`). Replace each with a capability interface both
implementations satisfy. Outside callers unchanged.

Cold-path reads of the hashed domains go on a separate interface `TrieContext` also implements, so
`PatriciaContext` gains no methods and no external implementer breaks.

## 10. Delete

| surface | why |
|---|---|
| `hex_patricia_hashed.go` (107 KB) — grid, `cell`, `activeRows`, fold/unfold, mount wall | replaced |
| `parallel_patricia_hashed.go`, `parallel_mount.go`, most of `split_point.go` | the seam is two functions |
| `MergeHexBranches`, `BranchMerger`, `mergeDeferredUpdate` | §4 — a complete record has nothing to merge |
| `BranchEncoder` and the variable-field cell encoding | replaced |
| `trie/sub_trie_loader.go`, `execution/state/triedb_state.go` | already dead — no constructor call in the tree |

The referencing subsystem (`ReplacePlainKeys`, `ExpandShortenedKeysInBranch` at `db/state/squeeze.go:338`,
`commitment_convert.go`, the `.kv` version gate, `seqReadahead`) is already deprecated and on its way
out independently. v4 does not delete it and does not wait for it — it simply never needs it.

From the E2 trie package take only the pure reference/RLP hashing primitives. Whether the rest has
non-v4 users is unverified; scan before deleting `execution/commitment/trie` wholesale — the witness
path imports it (`rpc/jsonrpc/debug_execution_witness.go:26`).

## 11. Measurements

**M1 — mainnet commitment record shape. Blocks the format.** Already built:
`DecodeBranchAndCollectStat` (`commitment.go:1188`) collects `KeySize`, `ValSize`, `CellCount`,
`TAMapsSize`, `LeafHashSize`, `LeafHashCount` and per-field medians, wired into
`cmd/integration/commands/commitment.go:1425`. Run it over the mainnet commitment domain — a linear
scan, since `AccessorHashMap` gives no seek. Output: **fan-out, leaves per record, the depth
histogram, field composition.** Settles §1's key saving, §2's record size, and replaces every corpus
number in this document. Bed: `snap-arb1` or `arb1-dev`.

**M2 — `resetRatio`** (`hex_patricia_hashed.go:2720`, behind `KV_READ_METRICS=1`, global and never
reset, so sample deltas). Sizes how often §9(a) fires today. **`skipRatio` is already answered: ~80%
on mainnet — 1 account in 5 reaches disk** (maintainer, mainnet). Only the reset side is still open.

**M3 — split and collapse rate per block.** Two counters in the fold. Confirms §2's premise that
these are rare relative to the 1,546 branch reads.

**M4 — merge write volume per hierarchy level.** Prices §4's deferred question.

**M5 — the touch share on an incremental block**, not the bulk arm.

**M6 — BranchCache cross-block hit rate**, from the existing counters. Sizes §8 and §5(4).

M1+M2 are one node run. The rest are local.

## 12. Order

1. **Touch phase.** Plain-space dedupe on `Update`, parallel keccak, 16 sharded prefix tries. No
   format change, no new domain, ships against the current trie. Red test first: duplicate
   `TouchPlainKeyDirect` calls invoke an injected hasher once and merge flags correctly.
2. **M1 + M2.** Everything below is sized by these.
3. **The new trie behind a third `TrieVariant`, in shadow mode** — shape and hash, no writes, root
   compared against HPH on every block. Gate on `TestLegacyVsHexRoot` and `TestIncrementalRootsAgree`
   (promote both out of the `zz_` scratch harness first — they are the only thing in the repo that
   catches a root divergence).
4. **Deferred encode and persist.** v4 becomes selectable.
5. **Default**, after end-to-end wall-clock parity or better at one worker and at physical-core
   count — including the touch phase, which today's published numbers exclude.
6. **The hashed domains**, optional, for snap serving and rebuild.
7. **Delete** HPH and the old engines.

## What this does not claim

- It does not recover the E2 trie's 18x. That came from never crossing the serialization boundary at
  229 bytes retained per key.
- Every record size here is provisional on M1. The fan-out spread (4.7 vs 11.1) is a 318 B vs 457 B
  spread, and the two imply tries of different shapes.
- The 69% touch share is the bulk arm. The per-block figure is M5.
- The hashed domains' ~140 GiB is derived from measured counts, not measured.
