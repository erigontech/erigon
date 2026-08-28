# commitment.kv v3.0: one DB record per trie child, not per branch row

Design notes, 2026-08-27. Verified against `origin/main` @ `24c627d6a0`. Nothing built yet.
Supersedes the delta-encoding approach in `20260706-commitment-history-deltas.md` (see "Why not deltas").

## Problem

One commitment branch node is one DB record holding all 16 child cells
(`touchMap|afterMap|{cell fields}*`). Changing one child rewrites the whole row, so both the
domain and its history amplify by the row's arity.

**The prize is write volume, NOT file size. File size gets worse — provably.**

A branch-pointer cell costs 1 fields byte + uvarint(32) + 32 hash = **34 B inline**
(`EncodeBranch`, `commitment.go:582-622`). As its own record it costs the same value plus a key of
`ceil(d/2)+2` bytes. Splitting a row cannot shrink a file: the values are identical and each record
adds a key. Against the 2 B saved by replacing the 4-byte header with a present-mask, per node of
depth `d` with `B` branch-pointer children:

    Δ(.kv bytes) = −2 + B·(k_slot − 1)   > 0 for any B ≥ 1

+6 B on a ~72 B row at depth 6 with B=2; +64 B (~1.9x) at depth 64 with B=2. No leaf mix, no depth
distribution and no `T/A` value flips it — `.kv` size is set by the last value per key per collation
range, and the split only adds keys to that set. **This is closed-form; do not spend a measurement
on it.**

Also closed-form: the record-count multiplier is **exactly ≤2x**. Every persisted node except the
root is pointed at by exactly one branch-pointer or extension cell, so `#slots = #nodes − 1 − #(nodes
whose parent pointer is an account cell)` and total records ≤ `2·#nodes − 1`. Accessor entries double;
earlier drafts said 16x and 2-4x, both wrong.

So the three real prizes, none of them file size:

- **Per-block write volume, always on.** Rewriting ~34 B per changed child instead of a ~1 KB row,
  plus the `ctx.Branch(prefix)` read gone from every branch write. This is the axis to measure.
- **Changeset volume, always on.** Same delta reaching `DomainEntryDiff` on the reorg path.
- **History size, opt-in only.** `CommitmentDomain.Hist` is `SnapshotsDisabled: true,
  HistoryDisabled: true` (`state_schema.go:288-290`); commitment history exists only under
  `--prune.include-commitment-history`, so this is an archive/witness-bed prize, not a default-node one.

And note where the always-on *size* wins actually live: the **address hoist** and the **tombstone
fix**, both independent of the split and both landable in the current row format. Do not let the
split claim them.

The write path already computes the exact per-cell delta and then destroys it:

1. `commitment.go:474` — `prev, _, err = ctx.Branch(prefix)`, a read per touched branch per block
2. `commitment.go:483` — `EncodeBranch(bitmap, ...)` with `bitmap = touchMap & afterMap`
   (`hex_patricia_hashed.go:1687`); emits field bytes only for cells in `bitmap`
   (`commitment.go:582`). **This is the delta.**
3. `commitment.go:492` — `merger.Merge(prev, update)` carries forward every untouched cell,
   reconstituting the full row
4. `commitment.go:500` — `PutBranch(prefix, full_row, prev)`; history gets the full prev row

## The only question that matters yet

Does this data model beat the current one? Everything below — versioning, migration, sequencing,
the deferred-path collision — is engineering for a design that has not earned it. Answer this first.

**Experiment 1 is a COST BUDGET and parameter extraction, not a go/no-go.** Its sign is known (above);
what is unknown is the magnitude — how much `.kv` growth the write-volume win must pay for — plus the
parameters every other estimate in this doc currently assumes. No node, no format work, no engine
change.

Every commitment `.kv` on disk already contains everything needed. Walk it row by row and, per row:

- decode `afterMap` and the cells (`DecodeBranchInto` / `decodeCells`);
- classify each present cell: leaf-bearing (`accountAddr`/`storageAddr` set) or branch-pointer;
- compute **current** bytes = the row as stored (key + value);
- compute **Variant A** bytes = node record (key + 2 + sum of inline leaf cells) + one slot record
  per branch-pointer child (key = `ceil(d/2)+2`, value = 1 fields byte + uvarint + 32 hash — keep the
  uvarint, dropping it is a separate encoding change and must not be smuggled in here);
- bucket by **decoded nibble depth**, not key byte length — `KeySize` merges depths `2d` and `2d+1`
  while `k_slot` differs between them;
- run **four arms** so independently-landable levers are not credited to the split:
  current / current−tombstones / VariantA / VariantA+hoist.

Three traps, all of which silently corrupt the aggregate:

- **Cell classification is not a dichotomy.** An account cell carries `accountAddr` *and* a
  storage-root `hash` simultaneously (`computeCellHash`, `hex_patricia_hashed.go:1185-1207`) — that
  is the ~88 B cell. Decide and state whether its downward hash stays inline or becomes a slot; `B`
  depends on the answer.
- **Referenced files must be excluded or reported separately.** `CommitmentBranchReferenced`
  (`db/state/domain_committed.go:47`) is true for any commitment `.kv` < v2.2 past the threshold; in
  those, leaf cells hold shortened references, not plain addresses. `decodeCells` parses them without
  error and returns short lengths, deflating leaf payload and making the hoist estimate meaningless.
- **`decodeCells` iterates `touchMap`**, so cells present in `afterMap` but absent from `touchMap`
  come back nil. Handle it; do not assume every `afterMap` bit has data.

**`k` cannot be read off a walk.** `getter.Next` returns decompressed bytes, but `k` is defined as
*compressed* key bytes under `seg.CompressKeys` plus accessor and MDBX cost — and the doc's own
depth-~38 dictionary claim is exactly what plain bytes cannot show. Feed the synthesized Variant-A
key/value stream through a real `seg.Compressor` with `DomainCompressCfg` and compare produced file
sizes. Add two per-record terms measurable from existing files: seg framing per word
(`file size − Σ(k+v)` ÷ word count) and accessor bytes per key (`.kvi` size ÷ key count).

Report per depth bucket and in aggregate: record count, key bytes, value bytes, total. That is a
direct answer on the domain-size axis, computed from real data, with no assumption about `T`, `A`,
`k` or the leaf mix — all four fall out of the walk.

`DecodeBranchAndCollectStat` (`commitment.go:1221`) walks the right shape but computes the wrong
numbers — do not reuse its byte accounting. `MinCellSize`/`MaxCellSize` use `cell.Encode()`
(`hex_patricia_hashed.go:2844`), a *different* encoding that includes `hashedExtension`, omits
`stateHash`, and returns a max-size buffer rather than `buf[:pos]`; and its per-field counters are a
priority `switch`, not a partition, so an account leaf with `apk`+`hash`+`stateHash` lands only in
`APKSize`. Take per-cell bytes from parse-position deltas in the row instead. Reuse the iteration
skeleton, not the accounting.

Also fall out of the same walk, for free:
- the leaf-vs-branch-pointer mix by depth, which sizes the address hoist;
- dead-tombstone bytes (4-byte `{touchMap, afterMap=0}` records), quantifying the backlog item;
- the crossover depth for the depth-gated hybrid.

**Experiment 2 is the deciding one — write volume.** Two parts, both at the write site.

First, one counter settles all three unit unknowns at once: sum `len(updateCopy)` at `PutBranch`
(`commitment.go:499`) per block. That yields mean row size, bytes/block, and resolves whether
"50-70k per commitment" is per block or per batch — no histogram needed. Do this first; it is a
handful of lines.

Second, the histogram — but of **Variant-A quantities**, not `T/A`. The chosen variant's write cost
is `(T_leaf > 0 ? whole node record : 0) + T_bp·(s+k)`, so it needs `T_leaf`, `T_bp`, `L`, `B` —
not `T` and `A`. `cells *[16]cellEncodeData` is in scope at `CollectUpdate`
(`commitment.go:463-469`), so the classification is free at the histogram site. Bucket by prefix
depth, at both per-block and per-collation-range granularity.

**Decision rule.** Experiment 1 gives the `.kv` growth cost; Experiment 2 gives the write-volume
win. Build only if the write-volume and changeset reduction is worth the measured file growth, with
the hoist and tombstone savings attributed to *themselves* rather than to the split. If Experiment 2
shows the win is concentrated in mid-tree branch-pointer nodes only, the depth-gated hybrid is the
answer rather than a wholesale split.

## Design

Stop at step 2. Persist per child slot instead of per row.

- **Node record** — key `EncodeKeyV2(P)`, value = 16-bit present-mask (the current `afterMap`),
  plus inline leaf cells under variant A below.
- **Slot record** — key `EncodeKeyV2(P) ‖ (0x80|n)`, value = fields byte + payload. No `touchMap`,
  no `afterMap`. A branch-pointer child is 1 + 32 = 33 bytes.

**Leaf placement is the open fork, and it decides what is being built.** Leaf-bearing cells are
children too — `EncodeBranch` emits `fieldAccountAddr`/`fieldStorageAddr`/`fieldStateHash` for them
(`commitment.go:583-597`) — so "one record per present child" taken literally makes every leaf edge
its own record.

- **Variant A, leaves inline.** Node record = present-mask + leaf cells; only branch-pointer children
  become slots. Key count stays ~branch-count. But a leaf touch still rewrites the node record, and
  a leaf cell is ~1 + 21 + 33 + 33 ~= 88 B against 33 B for a branch pointer, so leaves dominate row
  bytes at the frontier. Much of the amplification the Problem section indicts survives there. The
  win concentrates in mid-tree, where all children are branch pointers.
- **Variant B, leaves as slots.** Key count becomes ~#branches + #accounts + #storage-slots — ~1.7B
  storage leaves alone on mainnet. This does not add *data* — those leaf cells already live inside
  rows today — it adds per-record key, accessor and page overhead across billions of records.

**Resolved: Variant A, plus hoist the account address out of storage leaves.**

Variant B is fatal on record count, not on bytes. Mainnet has ~1.7B storage slots (awskii); one
record each means 1.7B accessor entries, EF offsets and page slots, whatever the payload shrinks to.

But the leaf payload is where the size actually is, and that changes what this design should target.
Account and storage subtries are fused at depth 64 (invariant 7: rows 0-63 account, 64-127 storage;
storage path = keccak(addr)‖keccak(slot), 128 nibbles), and a storage leaf cell persists
`storageAddr` in full — `putUvarAndVal(cell.storageAddrLen, ...)` (`commitment.go:612-616`), 52
bytes of addr‖slot — plus a 32-byte `stateHash`. Derived, not measured: ~1.7B x ~84 B is on the order
of 140 GB, against a mainnet commitment `.kv` of ~168 GiB plain. **The domain is dominated by leaf
payload, not branch structure.** That is also why key referencing bought 1.674x (168 -> 100 GiB), and
why losing it costs so much.

The hoist: every leaf in one account's storage subtree repeats the same 20-byte address, and the
first 64 nibbles of its path already *are* keccak(addr). The code knows this in memory —
`keyToHexNibbleHashCached` (`keys_nibbles.go:56`) caches the account's nibbles in a one-entry
`addrHashCache` on `c.valid && c.addr == addr` (`:61-70`) — but writes the address to disk 1.7B times. Storage leaves carry only the
32-byte slot and inherit the address. ~20 B x 1.7B ~= 34 GB, plus a uvarint length byte per leaf.

**Inherit from the account leaf cell, not from a depth-64 node record.** An earlier draft proposed
hoisting into "the node record at the depth-64 storage-subtree root". That record often does not
exist: invariant 5 says single-child branches are never persisted, so an account whose storage
diverges at nibble 70 has an extension across rows 64-69 and nothing persisted at depth 64. The
address is already on disk anyway — the account leaf cell carries plain `accountAddr`
(`fieldAccountAddr`), and a top-down reader holds it before crossing depth 64. So the rule is
inheritance from the enclosing account cell, which needs no new home and no data-dependent target.

**Cost: records stop being self-describing.** Every standalone-decode consumer breaks —
`DecodeBranchAndCollectStat`, `Validate`, `VerifyBranchHashes`, the `db/integrity/` scans,
`cmd/integration/commands/commitment.go:257`, the converter, and the point-lookup paths this design
keeps (`warmuper`, `SeekCommitment`, RPC proof/witness). Each needs either the enclosing account
context threaded in or an explicit "cannot decode standalone" contract. Enumerate and decide per
consumer before building; invariant 8 round-tripping is not the only exposure.

**Hoist vs referencing.** Referencing bought 1.674x on the same bytes (168 -> 100 GiB); the hoist
claims ~34 GB, roughly 19%. This design re-derives a smaller version of a larger lever that is
parked, and owes an explicit answer to why. They may also be near-exclusive: with referencing on the
storage leaf's plain key is already a reference, so the hoist saves little on top. Out of scope here
(deref disabled since 3.6) — but record the comparison rather than letting the hoist read as the
best available option.

Uniqueness is not a problem: slot `0x0` recurs under many addresses, but a record's *key* is the trie
path, which is globally unique. The plain slot bytes are payload for the domain lookup, not an
identifier. Nor does it obstruct the parallel trie — a worker owning a storage subtree reads the
subtree-root record to begin with, so the address arrives with it, and the subtree becomes
self-describing.

Ordering constraint the hoist introduces: a leaf cell no longer carries its own address, so its
subtree-root record must be loaded before it. Invariant 8 requires everything `EncodeCurrentState`
persists to round-trip losslessly through `SetState`, and the current code has no such ordering
dependency. Assessed as surviving (awskii, 2026-08-27); recorded here because it is a judgment call,
not something the code proves today.

Still to measure: leaf-vs-branch-pointer cell mix by depth, which sets `s` in the break-even model
and sizes the hoist.
- **Extension slots** carry `(hashedExtension, hash)` together. The persisted extension is already
  in hashed-nibble space (`fieldExtension` decodes into `cell.hashedExtension`,
  `hex_patricia_hashed.go:604-609`; `unfold` navigates off it at `:1571`, `:1590`), so a reader
  either answers from the hash at this level or jumps straight to `P‖n‖ext` with no intermediate hop.
- **Deletion** — the domain's native zero-length value, not a bespoke marker. See "Tombstones".

### Keying: suffix byte, not re-encoded path

`EncodeKeyV2(P‖n)` does **not** keep siblings adjacent — the whole subtree interleaves between them:

```
V2(P‖1)    = [0x2f, 0x10, 0x01]
V2(P‖1‖2)  = [0x2f, 0x12, 0x00]   <- grandchild sorts between the two children
V2(P‖2)    = [0x2f, 0x20, 0x01]
```

Appending a slot byte to the parent's encoded key does keep them adjacent, and makes the node record
a true prefix of its slots. The high bit (`0x80|n`) keeps a slot byte out of the V2 parity byte's
range (`0x00`/`0x01`), so `key(P)‖0x01` can never be read as a node key.

That proves slot keys are unambiguous; it does **not** prove the 16 slots are a contiguous run. For
even-length `P`, `V2(P)` ends in parity `0x00`, and the node key of `Q = P‖0‖0‖8‖…` encodes as
`[…, 0x00, 0x8?, …]`, which sorts strictly inside P's slot range (odd-length parent: same via
`P‖0‖0‖1‖8`). Correctness survives with an **exact key-length filter** on the scan; the "sequential
run of 16" performance claim does not, since the range can contain an arbitrarily large foreign
subtree. Bound the scan by the length check, not by a count of 16.

Note V2 gives clustering, not prefix-containment: it guarantees `floor(k/2)` shared *leading* bytes
for paths sharing k nibbles, but the trailing parity byte is always rewritten on append, and for an
odd-length parent the pad nibble is overwritten too.

## What this deletes

- `BranchMerger.Merge` (`commitment.go:1056`) and `MergeHexBranches` (`:867`). Not because "there is
  no row to merge" — under Variant A the node record *is* a row for its inline leaf cells, and
  `CollectUpdate` reads `prev` precisely to carry untouched cells forward. The real reason is that
  `hashRow` already emits `cellEncodeData` for **every** present cell
  (`hex_patricia_hashed.go:1747-1838`), so a node record can be re-encoded wholly from memory
  without reading `prev` at all.
- `IsComplete` (`:857`, `^touchMap&afterMap == 0`) and `touchMap` as a persisted concept.
- The `ctx.Branch(prefix)` read at `commitment.go:474` — nothing to read-modify-write.
- The `extension`/`hashedExtension` mirror pair, which is the desync surface behind invariant 8.
- `deriveHashedKeys` over siblings (see "Sibling work" below).

**Out of scope: key dereferencing.** Disabled since 3.6 (`config3.go:42`,
`DefaultReferencesInCommitmentBranches = false`). `ReplacePlainKeys` (`:712`),
`domain_committed.go:102`/`:474` and `squeeze.go:349` stay where they are — the idea is sound but
mis-placed, parked for separate use. This design neither depends on it nor removes it.

## Costs

**Fold fan-in.** `hashRow` loops `afterMap`, not `touchMap` (`hex_patricia_hashed.go:1751`,
`for bitset, lastNib := hph.afterMap[row], 0; ; {`), and calls `computeCellHash` for every present
cell (`:1799`). No touch-based skip exists, so fold needs every present child. Today they arrive with
one record read at unfold (`DecodeBranchInto` fills every cell, `branch_decode.go:44-60`, no
selective decode). Under Variant A the node record still delivers all inline leaf cells in one read;
what becomes separate lookups is the branch-pointer children only — `1 + #branch-pointer children`
reads per node, not 16.

CommitmentDomain today is `AccessorHashMap` only (`state_schema.go:276`) with no
`ValuesOnCompressedPage` (`:274`), so 16 adjacent keys cost 16 independent MPHF lookups and 16
independent decodes — no shared page.

**The read cost depends entirely on which access shape is used, and the current API is the wrong
one.**

Point-lookup shape (what the code does today). `TrieContext.Branch(prefix)` -> `readDomain` ->
`GetLatest` -> `lookupLatestFromFiles` (`db/state/domain.go:1464`) walks `slices.Backward(dt.files)`
and returns at the first hit, with commitment excluded from the read cache and carrying no existence
filter to skip files on:

```go
useExistenceFilter := dt.d.Accessors.Has(statecfg.AccessorExistence)
useCache := dt.name != kv.CommitmentDomain && !bounded
```
(`domain.go:1471-1472`)

Today the whole row is rewritten whenever any child changes, so the row is always in the newest file
and the walk terminates immediately. Per-slot, an untouched sibling's last write is by construction
old, so under this API each cold slot walks the stack: `(files not yet satisfied) x seek`, no early
exit.

**Scan shape (what the workload actually is).** Commitment does not random-access the trie — it
unfolds from the root in sorted hashed-key order, and `needFolding` is
`!bytes.HasPrefix(hashedKey, hph.currentKey[...])` (`hex_patricia_hashed.go:1599`), so a prefix
shared with the previous key is never re-fetched. Each prefix is visited once, left to right. V2
keys make DB order match that order — `EncodeKeyV2`: "Suffix-parity preserves prefix-sort locality
across the trie" (`nibbles/nibbles_v2.go`). So the natural shape is a **k-way merge-scan over one
forward cursor per file**: each cursor advances monotonically, total cost is O(bytes scanned), and
file count is merge width rather than a per-key multiplier. Under this shape the file-stack
objection above does not apply.

**Prerequisite, therefore: replace the commitment read path's point lookups with an ordered
iterator.** `AccessorBTree` is the hard requirement — commitment is `AccessorHashMap` only
(`state_schema.go:276`), so `bindex` is nil and `IteratePrefix`/`debugIteratePrefixLatest`
(`domain_stream.go:452`, seeking via `item.src.bindex.Seek` at `:504`) cannot run against it today;
every existing call site targets `kv.StorageDomain`. The decisive property is not `Seek` cost but that `.bt` **has a cheap `Next` at all**:
`Cursor.Next` (`btindex/btree_index.go:114`) is `c.d++` then `readKV()`, one Elias-Fano
`c.ef.Get(c.d)` plus a sequential decode — the pivot binary search and interpolation search
(`bps_tree.go`) run only on `Seek`. `AccessorHashMap` has no `Next`; every access is a fresh MPHF
lookup, which makes the scan shape unrepresentable on commitment today. One `Seek` per subtree
entry then `Next` down the run is the whole read model.

`AccessorExistence` matters only for whatever point-lookup paths remain — the warmuper, `SeekCommitment`, and the RPC proof/witness paths — not
for the fold traversal itself. Its resident bloom cost feeds the Blocker below.

**Page-level compression is OUT OF SCOPE (awskii, 2026-08-27); `k` carries at its own weight.**
Domain-side paged values do not exist and are not being built for this. This is not a missing config line. `Domain.dataWriter` returns a plain `*seg.Writer`
(`db/state/domain.go:1682`) while only `History.dataWriter` returns a `*seg.PagedWriter`
(`db/state/history.go:892`); every `GetFromPage` call site is history-side. Worse, the `.bt` is built
word-by-word over `d.dataReader(valuesDecomp)` (`domain.go:1030`), so page-granular domain values
need a paged variant of `btindex`/`bpstree` — the same "bucketed pivots persisted in the `.bt`" that
#20180 records as unbuilt. **Domain-side paged values + a paged `.bt` are a prerequisite with their
own cost, not a config flip.** So `k` is a real term, and it is depth-dependent: a slot key is `ceil(d/2)+2` bytes, i.e. ~34 B at
depth 64 for a 33 B branch-pointer value, ~66 B at depth 128. Keys are still dictionary-compressed
(`Compression: seg.CompressKeys`), but `DomainCompressCfg` has `MinPatternLen: 20`
(`state_schema.go:445-452`), so a shared parent prefix only becomes a dictionary pattern from depth
~38 onward. Shallow slot keys get no dictionary help but are short; deep ones are long but do
compress. **Break-even therefore varies sharply with depth**, which is an independent argument for
the depth-gated hybrid. Measure `k` per depth bucket, do not assume it.

The paged-value argument is retained below only as the reason this could be revisited later, not as
a description of current behaviour or a dependency of this design.

 A page is
`[cnt][kLens][vLens][keys packed][values packed]`, zstd'd as a unit (`db/seg/seg_paged_rw.go:35-69`).
Keys are segregated from values and packed contiguously, so a page of sibling slot keys is a run of
near-identical byte strings — close to ideal zstd input. The shared prefix costs almost nothing, so
the `k` term in the break-even formula largely vanishes and `T/A < s/(s+k)` tends toward `T/A < 1`:
the split wins except on fully-touched nodes. CommitmentDomain has no `ValuesOnCompressedPage` today
(`state_schema.go:274`); this design needs it.

Note page grouping is a point-lookup *pessimization* — `GetFromPage` zstd-decodes the page then
linear-scans it for the key — so it is viable only because of the move to scanning. Per-slot
records, the ordered accessor with `Next`, and page compression are one change, not three: each is
what makes the next affordable.

**Warmup becomes a cursor position, not a prefetch fleet.** The warmuper today issues one
`trieCtx.Branch(prefix)` point lookup per depth per key (`warmuper.go:150`), which is most of the
measured 9192/s branch reads — i.e. most of that traffic is warmup, not fold, and the two respond
oppositely to this change. Under the scan shape it is redundant at best and actively harmful at
worst, issuing random reads ahead of a sequential one.

The parallel fold makes the replacement obvious. Each fold worker owns a prefix subtree
(invariant 9: children own `P+nib` and below), and under V2 collocation a subtree is a **contiguous
key range**. So a worker's whole read set is one `Seek` plus a `Next` run — fold locality
(invariant 2) expressed directly in storage layout. Warmup reduces to positioning one cursor per
worker at its subtree start. Revise the warmuper together with the accessor change, not after.

`.bt` is affordable at 16x the keys: it is a sparse pivot index, not a B-tree — pivots every
`DefaultBtreeM` keys (`btindex/btree_index.go:45`, a `var` defaulting to 64, overridable via `BT_M`), all offsets in one Elias-Fano array,
binary search to a <=M window then interpolation search with an 8-probe budget. Steady per-key cost
is a few EF bits.

**The scan defence is bounded to the dense regime, and the measured node is dense.** A k-way
merge-scan is O(bytes scanned) only when the update set is dense. At head one block touches ~1-3k
keys out of ~500M, so the reader must `Seek` between touched subtrees — and every `Seek` is per
file, restoring the file-stack multiplier the scan argument claims to dispel. With tens of thousands
of touched prefixes per commitment, per-subtree seeks approach per-key seeks. The read model to
state is `#files x (Seek + in-window scan)` per touched node, with the bytes-scanned form as the
bulk-sync best case.

Note also that the 16 slots are **not** a contiguous run — a foreign subtree can sort inside the
range (see Keying), so the scan is bounded by an exact key-length filter, not by a count of 16.

`PrefixIndex` (#20180) is the wrong engine here — it already regresses +24-46% on storage because
`addr‖slot` clusters into single 2-byte buckets, and commitment slot keys cluster harder. The
reusable piece is bucketed pivots persisted in the `.bt`, which that PR notes isn't built.

**Index footprint.** Under Variant A the key-count multiplier is `1 + #branch-pointer children`,
plausibly 2-4x rather than 16x — it is the branch-pointer mix, not arity, that sets it. Measure
before quoting a number.

## Sibling work: smaller prize than it looks

For the 15 untouched children there are already **zero** accounts/storage domain reads in steady
state. `fieldStateHash` is persisted (`commitment.go:597`, encoded `:622`, restored via
`fillFromFields`, `hex_patricia_hashed.go:613`) and `loadStateIfNeeded` is guarded on
`cell.stateHashLen == 0` (`:2029`). The memoization is already there.

What is wasted is `deriveHashedKeys`, called for every present cell unconditionally at
`decodeBranchIntoRow` (`:1511-1522`, call at `:1518`) — a real Keccak256 per leaf-bearing cell
(`keys_nibbles.go:107`). An untouched account sibling with a valid `stateHash` returns the cached
hash at `:1213` and never reads `hashedExtension`, so those digests are computed and thrown away.
Historically this was paired with dereferencing all 16 shortened keys per row read; dereferencing
has been disabled since 3.6, and the derivation was left behind. Under per-slot keying the descent knows the next key
outright and never needs a sibling's hashed key.

## Tombstones

Merge drops a key only when both conditions hold (`merge.go:506-509`):

```go
deleted := r.values.from == 0 && len(lastVal) == 0
if deleted { continue }
```

Commitment's delete writes a 4-byte `{touchMap, afterMap=0}` (`EncodeBranch` emits the header
unconditionally, `commitment.go:570-573`), so `len(lastVal) == 0` is never true and every deleted
branch is copied forward at every merge, bottom-most included, forever. `BranchData.IsTombstone()`
checks `len == 0` (`:642`) and never matches its own delete record.

Use the domain's native zero-length deletion (`DomainDel` -> `DeleteWithPrev` ->
`addValue(k, nil, step)`, `domain.go:446`). It reaches the `merge.go:506` drop at bottom-most. The
only reason for the 4-byte form is carrying `touchMap` for row merging, which this design abolishes.
A 1-byte marker is unambiguous by length (minimum legal `EncodeBranch` output is 4 bytes) but is live
data forever, reproducing the leak.

"Deleted vs absent" collapsing is safe in the v3.0 shape because the node record's present-mask is
the authority on slot existence, not the presence of a slot record.

**But the tombstone fix is sequenced first, standalone, in the current row format — where there is
no node record and no present-mask.** Its safety argument must be made in current-format terms:
`unfoldBranchNode` already treats absent branch data at the root as an empty root
(`hex_patricia_hashed.go:1479`) and errors elsewhere, and invariant 5 already makes absence
non-informative (single-child branches are never persisted, so "no record" never meant "no state").
Establish that before landing step 1, or fold the tombstone fix into the v3.0 bundle instead. The slot tombstone only has to shadow older
files until a bottom-most merge collects it.

Careful with invariant 5: single-child branches are never persisted, so "record absent" already does
not mean "no state". A missing slot lookup must keep meaning "consult the ordinary fold", never "empty".

## Why not deltas

`20260706-commitment-history-deltas.md` proposed synthesizing `DiffHexBranches` at collation, with
anchor frames, replay below `HistorySeek`, and a v3.0 seek-layer gate. All of that reconstructs
information that `EncodeBranch` already has in hand at step 2 above and step 3 discards. Per-slot
records make history a true delta with no chain, no anchor policy, no replay, addressable at any
block directly. The delta plan's Phase 0 measurement gate was never run and is now moot.

## Not yet analysed

Three paths this design touches and the doc does not yet cover. The first is the most serious.

**Changesets and unwind.** `DomainPut` carries `prevVal` into a `kv.DomainEntryDiff{Key, Value}` per
key (`db/state/changeset/state_changeset.go:156-165`), and unwind replays those and nothing else
(`db/state/domain.go:1391`). Today that is one entry per touched row carrying a ~1 KB prev value.
Per-slot it becomes `1 + T_b` entries carrying ~33 B each — total bytes likely fall, entry count
rises, and per-entry overhead is unmeasured. This is the **reorg hot path** over the dense 96-block
window, not a cold path, and nothing in this doc has looked at it. Do this before the task breakdown.

**`KeyCommitmentState` shares the key space.** It is `[]byte("state")`
(`execution/commitment/branch_cache.go:36`), 5 ASCII bytes ending `0x65` — so it cannot be mistaken
for a V2 parity byte (`0x00`/`0x01`) or a slot byte (`0x80`-`0x8f`), and there is no collision. But
it sorts among the trie keys, so the ordered scan must skip it explicitly rather than assume every
key in range is a node or a slot.

## Collides with the deferred/concurrent commitment path

`CollectDeferredUpdate` (`commitment.go:510`), the per-goroutine `localCollector` ETL in
`TrieContext.PutBranch`, and `readBranchAndCheckForFlushing`/`HasPendingPrefix` are all keyed on the
whole prefix and depend on prefix-granular flush ordering. Per-slot keys change what a "pending
prefix" means and what a last-write-wins ETL load produces. Invariant 9 (prefix ownership is
disjoint: children own `P+nib` and below, merge owns `P`) still holds, but its *storage* granularity
now matches its concurrency granularity, which is a behaviour change in the deferred path, not a
free simplification. This is the same code the in-flight parallel-commitment work is restructuring —
coordinate before building.

## Sequencing

Four separable changes. Only the last is this design; the first three are independently landable and
must be measured alone so their effect is not credited to the split:

1. **Tombstone fix** — the only already-existing defect here. Dead-tombstone bytes in current `.kv`
   files are directly countable; quantify before fixing.
2. **Lazy `deriveHashedKeys`** — fixable today, no format change, no V2 dependency.
3. **Commitment ordered accessor + cursor-based fold reads.** Cheaper to start than stated
   elsewhere: `AGG_COMMITMENT_BT=true` already flips
   `Schema.CommitmentDomain.Accessors = AccessorBTree | AccessorExistence`
   (`db/state/statecfg/state_schema.go:79-84`, default false), so the accessor A/B is runnable today
   after an accessor rebuild. The remaining work is replacing `Branch(prefix)` point lookups with an
   ordered iterator, and revisiting the warmuper.

   **Its measurement will not transfer to step 4.** V1 keys are `HexToCompact` with a *leading* flag
   byte, so every even-length path sorts before every odd-length one; a cursor-based fold under V1
   needs two cursors per file for the parity split (precedent:
   `execution/commitment/preload_ranges.go:26-36`), i.e. 2x merge width, and the resulting order is
   not V2's trie order. Treat a step-3 number as a lower bound, not a baseline — which is this doc's
   own thesis that collocation only pays once records are per-slot.
4. **Row to per-slot split** — justified by whatever prize remains after 1-3.

## Version: commitment.kv v3.0

**Reconciling this with Sequencing.** The two framings conflict unless stated as a table — there are
five items, not four, and two of the "independently landable" ones are bundled into the version:

| Item | Lands standalone | In v3.0 | Measured against |
| --- | --- | --- | --- |
| Tombstone fix | only if the current-format safety argument holds | yes, as native zero-length | countable dead-tombstone bytes in current `.kv` |
| Lazy `deriveHashedKeys` | yes — no format change | no | keccak count per unfold |
| Ordered accessor + cursor reads | yes — `AGG_COMMITMENT_BT` today | yes | existing workload under V1 ordering: lower bound only |
| Root state blob slimming | yes — independent and cheap | yes | ~656 B/block of churn |
| Per-slot records | no | yes | whatever prize survives the four above |

This is one major format version bundling every change above, not a sequence of independent bumps:
V2 nibble keys, per-slot records, the ordered accessor, page-grouped values, native zero-length
tombstones, and the slimmed state blob. `KVWriteVersion: commitmentKVWriteVersion`
(`state_schema.go:278`) is already a function-based stamp, so the write hook exists.

**It touches every commitment file type, because V2 re-encodes every key.** Keys appear in the
values file, the accessor, and the inverted index, so nothing is untouched. And commitment's
file-type *set* changes, not just its versions — compare storage, which already has the accessor
shape this design needs (`db/state/statecfg/versions.yaml`):

| File | storage today | commitment today | commitment v3.0 |
| --- | --- | --- | --- |
| `bt` ordered accessor | v2.0 | absent | **add** |
| `kvei` existence | v1.2 | absent | **add** (optional; only for residual point-lookup paths) |
| `kvi` hashmap accessor | absent | v2.1 | **retire** |
| `kv` values | v2.0 | v2.2 | **v3.0** — per-slot, page-grouped |
| `hist.v` | v2.0 | v2.0 | **v3.0** — per-slot history values |
| `hist.vi` | v1.1 | v1.1 | bump — keys re-encoded |
| `ii.ef` | v3.0 | v3.0 | bump — keys re-encoded *and* key count multiplied |
| `ii.efi` | v2.0 | v2.0 | bump |

Naming hazard: `ii.ef` is *already* at v3.0 for unrelated reasons. "commitment v3.0" in this doc
means `commitment.kv` v3.0, the umbrella name for the whole change — not that every file lands on
the string v3.0.

**History-side consequence.** `hist.v`/`hist.vi`/`ii.ef` for commitment exist only under
`--prune.include-commitment-history`, and they are webseed-distributed when it is on — filtered out
of the download list otherwise (`db/snapshotsync/snapshotsync.go:524-526`). So the history bump
invalidates any published commitment-history snapshot set and needs coordinating with the snap36
fleet that produces them. The domain-side bump has no such exposure on a default node, where
commitment history does not exist at all.

**Migration is the converter, not a re-sync.** #21146's wiring commit says "fresh sync only", but
that was V2-in-the-old-model with no migration written. `erigon commitment convert`
(`db/state/commitment_convert.go`) already re-encodes V1 keys to V2 offline and already detects
encoding by content vote (`detectKeyEncoding`, `:100`). Extending it to also reshape rows into slots
is the same pass over the same files — but note it is a **`.kv`-only** tool today. Converting a
history-enabled datadir additionally means rewriting `hist.v` values and rebuilding `hist.vi`/`ii.ef`
against the multiplied key set, which is new converter scope, not an extension of the existing pass. That turns a fresh-sync requirement into an offline convert,
which is the difference between this being adoptable and not.

**Three migration gaps, none yet closed:**

- **`detectKeyEncoding` breaks on slot keys.** It is a two-state canonicality vote on
  `nibbles.DecodeKeyV2` (`commitment_convert.go:100-115`). A sampled slot key ends in `0x8n`, fails
  `ErrV2KeyParity`, and votes **V1** — so a converted v3.0 file is classified unconverted and
  re-converted. The 10^-96 false-positive argument in its docstring does not cover the new shape.
  Needs a third state or version-derived detection.
- **History conversion is a new tool, not an extension.** The converter is `.kv`-only by explicit
  design — `commitment_convert.go:602-609` filters `.v`/`.ef` out with a comment saying so. Producing
  per-slot `hist.v` and a multiplied `ii.ef` from bundled-row history means deriving which cell
  changed at which txnum, i.e. diffing consecutive row versions — exactly the `DiffHexBranches`
  synthesis that "Why not deltas" calls moot. The alternative, assigning a row's full txnum set to
  every slot, multiplies `ii.ef` by arity for no benefit. Scope separately.
- **`min: v3.0` plus a `.kv`-only converter strands archive beds.** The recommendation below refuses
  old files at startup while the migration tool cannot produce new history files. Either gate
  `min: v3.0` on `--prune.include-commitment-history` being off, or ship the history converter first.

Ordering is not a gap: `convertCommitmentFile` pushes into a `TemporalMemBatch` wal and
`dumpStepRangeToPath` runs an ETL sort (`commitment_convert.go:326-334`), so a 1-to-many record
expansion still emits in order.

**Open decision: does `min` stay v1.0?**

- `min: v1.0` — old v2.x files stay readable, so a datadir can hold both shapes. Costs a read path
  that handles bundled rows and per-slot records, under two different key encodings, and a merge
  that bridges the boundary by re-encoding. The per-file version gate pattern
  (`CommitmentBranchReferenced`) is the precedent.
- `min: v3.0` — old files refused outright, converter run required before start. Far simpler code,
  and `MustSupport` already produces exactly that refusal.

Recommend `min: v3.0` with the converter as the migration, unless mixed-version datadirs are needed
for staged rollout. Two key encodings live in one read path is where the subtle bugs will be.

No downgrade story either way — record that explicitly before shipping.

## #21146 is substrate, not a standalone change

V2 keys are the substrate this data model sits on, and that reframes their stalled A/B. The
benchmark measured V2 wired into the **old row-per-node model**, where prefix-sort locality buys
nothing: one point lookup per row, so collocation has no consumer. Coming out at -6% unconstrained
is what "substrate without its superstructure" looks like. V2's value is collocation, and
collocation only pays once records are per-slot and the read path scans (`Next`, page runs, the
cursor stack above).

Alex's second question — whether key compression differs — also has a different answer here. With
one key per branch it is nearly moot. With per-slot records under page grouping, keys are many,
packed contiguously and zstd'd as a run, so key compression moves from side-effect to main term.

**Answer the heap question on the existing branch BEFORE closing it.** The wiring commit is the only
thing that reproduces the +43%; close it and the question is unanswerable until v3.0 exists, at which
point it is confounded with the split's own key-count multiplier. No hypothesis has been recorded, so
the next person restarts from zero. One cheap candidate: V2 keys are only 0-1 bytes longer than V1
(`n/2+odd+1` vs `len(hex)/2+1`), which cannot explain +43% — but V1 segregates even- and odd-length
paths into two contiguous key regions (leading flag byte) while V2 interleaves them in trie order,
changing MDBX page locality and dirty-page count in `TblCommitmentVals` within a batch.

**Disposition: close #21146 as superseded once that is answered, folding the encoding into this
design.** Nothing is lost —
`EncodeKeyV2`/`DecodeKeyV2` are already merged on main via #21933. Only the wiring commit dies, and
it is throwaway regardless: it rewrites `unfoldBranchNode`, `fold`, `CanDoConcurrentNext`,
`validatePlainKeys` and `verify.go` to call `EncodeKeyV2`, and every one of those call sites changes
shape again under per-slot keys, where a slot key is `EncodeKeyV2(P)‖slotbyte` rather than a plain
encode.

The decisive reason is migration cost. #21146's wiring is already a hard cutover ("existing
V1-encoded datadirs are not readable; fresh sync only") and per-slot records are independently
format-breaking. Landed separately that is two cutovers, two version bumps, two re-syncs. Landed
together it is one.

None of that dissolves the memory question below. It changes what the encoding has to be justified
by, not whether the regression needs explaining.

## Blocker

PR **#21146** (`awskii/nibblesv2-main`, draft since 2026-05-12) is the V2 key wiring this depends on.
Its own mainnet A/B: unconstrained V2 ~= V1 (-6% commitment time), but under a 32 GB cap it was
**OOM-killed after 26h** where V1 survived — +43% Go heap, +36% sys, 2.25x commitment time, 1.39x
major page faults. Alex asked where the RAM goes and whether key compression differs; unanswered
since 2026-06-08. The heap number is the part that still gates, and it gates harder here than it did there: the split multiplies
commitment key count by mean arity — MPHF/`.bt` entries, DB key space, and a new existence filter's
resident bloom — pushing on the same axis as that unexplained +43% heap. The split cannot be A/B'd
under a 32 GB cap until the regression is explained, and its own multiplier then adds to whatever
the answer turns out to be.

`EncodeKeyV2`/`DecodeKeyV2` are already merged on main (via #21933) but used only by the offline
converter, gated by the statistical `detectKeyEncoding` sampler (`commitment_convert.go:100`).

The wiring commit in the `erigon-nibblesv2-wire` worktree is a hard cutover: "existing V1-encoded
datadirs are not readable; fresh sync only."

## Root state blob: 656 of ~715 bytes are dead

Separate from the per-slot change, same hot path. `KeyCommitmentState` is rewritten every commitment
with a fixed skeleton from `state.Encode` (`hex_patricia_hashed.go:2720`):

| Field | Bytes |
| --- | --- |
| rootFlags + root len | 3 |
| `Depths [128]int16`, one byte per row | 128 |
| `TouchMap [128]uint16` | 256 |
| `AfterMap [128]uint16` | 256 |
| `BranchBefore` packed to 2x uint64 | 16 |
| encoded root cell | remainder (~56) |

659 fixed + ~56 = ~715 bytes per block (awskii, measured).

The four per-row arrays are traversal scratch for rows `0..activeRows-1`. The blob is only ever
written or loaded with the trie at rest: `EncodeCurrentState` panics on `currentKeyLen > 0`
(`:2947`) and `SetState` returns "target trie has active rows" unless `activeRows == 0` (`:2979-2980`).
So `SetState` copies 656 bytes into an empty range every restore (`:2995-2998`). The root's own
state is carried separately by `RootChecked`/`RootTouched`/`RootPresent` and the encoded root cell,
which are the only fields with a live consumer.

Revising this is independent of per-slot records and much cheaper: drop the arrays, keep flags plus
root cell, version the blob. Worth confirming first that no path encodes with a row active — the
`SetState` guard means such a blob would be unloadable anyway, so if one exists it is already a bug.

## Measurement that decides it

**GATE: the headline prize number does not survive its own sanity check.** An earlier draft read
"~1 KB row x 50-70k branch writes per commitment" as ~50-70 MB **per block**. That is a unit error
and it is impossible: a step is 390,625 txnums (`db/config3/config3.go:29`) ~= 2,000 mainnet blocks,
so 50 MB/block is ~100 GB written per step against a whole-chain commitment `.kv` of ~168 GiB; over
~23M blocks it implies ~1.1 PB written and ~180 GB retained, a 6,000x write amplification where
merge amplification is O(log) of the file tree, ~10-30x.

At least one of {~1 KB mean row, "per commitment" == "per block", ~168 GiB total} is wrong. The
measured node is a from-0 archive under batch commits, so "per commitment" is almost certainly a
multi-thousand-block batch, not a block. **No MB/block figure is stated here until this is
reconciled.** Nothing downstream — reduction ratio, domain-side prize, go/no-go — can be computed
before it. Measure `TblCommitmentVals` write bytes per block directly on a head-following node.

**Break-even, stated for Variant A (the chosen variant).** The node record carries the present-mask
plus all `L` inline leaf cells unconditionally, and only branch-pointer children become slots, so
the real inequality is

    2 + L*s_leaf + T_b*(s+k)  <  4 + A*s

with `T_b` the touched *branch-pointer* children. This is materially worse than the Variant-B form
below and may not clear at all near the leaf frontier, where `L` approaches `A` and the node record
is rewritten in full on any leaf touch. The frontier is also where most nodes are. An earlier draft
stated the Variant-B inequality as if it were the design's; it is kept only as the upper bound.

**Granularity matters and the two answers diverge.** `.kv` size is set by the *last* value per key
per collation range, not per block. Over a ~2,000-block step a hot node accumulates distinct touched
children toward `T/A -> 1`, where the split strictly loses — it pays `k` per slot key plus the node
record. So a per-block histogram measures write amplification into MDBX, and a per-range histogram
measures the on-disk prize. **They are different numbers and the acceptance rule must say which one
binds.** Predict divergence.

Variant-B upper bound, for reference. For a node of arity `A` with `T`
touched children, cell size `s` and per-slot key cost `k`, the split wins while
`T(s+k) < 4 + A*s`, i.e. roughly `T/A < s/(s+k)`. Every term has to be measured, not assumed:
`k` is the *compressed* key bytes under `seg.CompressKeys` plus per-key accessor and MDBX cost, and
`s` differs ~2.7x between a branch pointer (~33 B) and a leaf cell (~88 B). At `k=5` break-even is
`T/A ~= 0.87`; at `k=33` it is `0.5`. An earlier draft of this doc asserted "4 of 16" — that was
wrong on both the numerator model and the denominator. No number here until the histogram produces
one.

The win ratio is likewise bounded by mean arity, not by 16: a depth-6 node with `A=2`, `T=1` is a
2x win, not 16x.

### Measured baseline (snap-arb1 arch-0, 2026-08-27)

Live node, `--experimental.parallel-commitment`, PR #23588 metrics wiring, from-0 archive at
~blk 8.14M. Two scrapes of `localhost:6061/debug/metrics/prometheus`:

| Counter | Sample 1 | Sample 2 | Delta |
| --- | --- | --- | --- |
| `domain_commitment_keys` (state keys fed to the trie, `hex_patricia_hashed.go:2296`) | 483,074,989 | 484,402,823 | 1,327,834 |
| `domain_commitment_updates_applied` (branch records written, `commitment.go:413`/`:455`/`:506`) | 875,478,463 | 878,083,741 | 2,605,278 |

**Regime: bulk from-0 archive sync at ~blk 8.14M under batch commits — NOT head.** Every figure here
carries that caveat. The design targets head, where the update set is sparse; the scan-shape argument
and the branch-read ratio both look far better in bulk than they will at head. Two scrapes only, no
block delta and no wall-clock delta recorded, so these support a *ratio* and nothing per block.
Re-scrape on a head-following node before any read-cost conclusion is treated as settled.

**1.96 branch records written per state key processed** (1.81 lifetime). The three
`mxTrieBranchesUpdated` sites (`commitment.go:413`/`:455`/`:506`) are mutually exclusive, so no
double-count. Ancestor sharing amortizes
what would naively be depth (~7-8) writes per key down to under 2. At ~1 KB/row that is ~1.9 KB of
branch-row bytes per touched state key.

Same scrape, rate gauges: `commit_branch_read_rate` 9192/s against `commit_account_read_rate` 286/s
and `commit_storage_read_rate` 196/s — branch-read dominated 19:1 over plain state reads **in bulk
sync**. Do not carry this ratio to head unrelabelled. That is the traffic the per-slot change multiplies, and it is the strongest argument for
treating the read cost above as the governing risk. Treat the `commit_*` rate gauges with care:
`commit_blocks`, `commit_txns`, `commit_fold_rate`, `commit_unfold_rate` all read 0 on this node
because nothing writes them.

These counters do **not** give touched-children-per-node. The write-site histogram is still required.

### Still to measure

The deciding distribution is **touched children per touched node per block, by prefix depth**.
It is already in hand at the write site: `bitmap = touchMap & afterMap` is passed into
`EncodeBranch` at `commitment.go:483`. Histogram `bits.OnesCount16(bitmap)` against
`bits.OnesCount16(afterMap)`, bucketed by `len(prefix)`.

Runs on any synced node over a few thousand blocks. Commitment history does **not** need to be
enabled, so no archive-bed dependency.

That histogram alone does **not** decide it — it measures the write-byte win and says nothing about
the read risk above. Four more, all cheap and available today:

- **Arity and row-size distribution per touched node.** `DecodeBranchAndCollectStat`
  (`commitment.go:1221`) is the right tool for this one, over `.kv` files.
- **Leaf vs branch-pointer cell mix by depth.** Sets `s_leaf`/`s`, the key-count multiplier, and the
  size of the address hoist. The fork itself is already closed (Variant A); this sizes it.
- **Branch reads per block and their file-stack depth.** `depthsToTxNum` and the `step` returned by
  `readDomain` already exist for file-depth access stats — the direct proxy for the read risk.
- **Baseline totals.** Current commitment `.kv` + history bytes, and bytes/block. Without a baseline
  a favourable histogram cannot be converted into a prize.

**Acceptance rule, pre-committed:** build only if the measured `T/A` distribution puts the majority
of branch-write *bytes* below break-even AND measured file-stack depth for cold slots stays bounded
under an existence filter. Either one failing stops it.

Do not use `DecodeBranchAndCollectStat` (`commitment.go:1221`, `CellCount = OnesCount16(tm & am)`,
`:1239`) for this — over merged `.kv` rows `touchMap` is the union across the whole file range, so
it answers cells-per-row, not touched-per-block.

Expected shape, from trie geometry (a model, not a measurement, and not yet reconciled with the
50-70k writes/block figure above): levels 0-1 sit at 16/16 and lose;
level 2 around 4/16; levels 3+ around 1/16. If that holds, a depth-gated hybrid — bundle above depth
D, split below — is trivial since prefix length is known at write time.

## Verification

Root parity is a weak oracle (invariant 14). The in-repo oracle is `StateRootVerifyByHistory`
(`db/integrity/commitment_integrity.go:1162`), which is stronger not because it checks a root but
because it rebuilds state per sampled block from accounts/storage history in a fresh
`SharedDomains`, rather than trusting whatever the run under test persisted. Byte parity must be asserted over N>=3
incremental batches including a `.kv` merge — batch-2 branch damage only surfaces as batch-3
divergence.

Blast radius of the row format itself is ~16 call sites: `DecodeBranchInto`, `decodeCells`,
`Validate`, `VerifyBranchHashes`, `IsComplete`, `ChildCount`, plus `db/integrity/` scans,
`cmd/integration/commands/commitment.go:257`, and the converter.

## Open

- Reconcile the 50-70k branch writes/block against trie geometry. 50-70k touched branch nodes per
  block implies far more touched nodes than ~1k touched accounts x depth would predict; either the
  count includes storage-trie rows and repeated folds, or the geometry model in the section above is
  wrong. Whichever it is changes the depth distribution the hybrid gate depends on.
- Whether commitment keeps `AccessorHashMap` alongside the ordered accessor or replaces it.
