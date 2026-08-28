# commitment.kv v3.0: one record per trie edge

Design record, revised 2026-08-28. Verified against `origin/main` @ `0124ab5a0c`. The implementation is
complete in this worktree; this document records the decisions that were implemented and the
compatibility boundaries that remain.

Supersedes this file's previous revision, which specified node records plus per-slot records under V2
parity keys. That model does not survive its own key encoding: the ordered scan it assumes is not
available, and the node record it needs is redundant with the parent's slot record. Also supersedes
the delta-encoding approach in `20260706-commitment-history-deltas.md`.

## Implementation status

The commitment.kv v3 implementation is complete. These settled decisions are implemented and
covered by the commitment, state, integrity, and acceptance tests:

- [x] V3 terminator keys and parent-slot child keys replace the V2 parity-byte encoding.
- [x] Fixed-shape edge records carry the child hash, extension, leaf data, and branch mask.
- [x] `AccessorBTree | AccessorExistence` is enabled for the commitment domain; the old hashmap
  accessor is not retained there.
- [x] New writes use the v3 edge-record gate, while reads select the format per file version.
- [x] Branch reads synthesize the existing row-shaped API from edge records, including mixed legacy
  rows and v3 records.
- [x] Zero-length child tombstones and mask-driven reads prevent deleted records from resurfacing.
- [x] The root state blob is re-keyed, versioned, slimmed, and carries the root child mask.
- [x] Storage leaf records omit the account address and recover it from the enclosing account cell.
- [x] Deferred updates, changesets, unwind, and concurrent ownership operate at record granularity.
- [x] CLI output renders edge records; legacy row-only integrity checks refuse them explicitly.
- [x] Stored-record parity is checked across incremental batches and a merged `.kv` file, with fresh
  state reconstruction used as the root oracle.

The zero-value `DomainCfg` keeps `EdgeRecordsInCommitment` disabled so legacy callers do not stamp
v3 accidentally. The production commitment schema enables the gate for new writes.

## Implementation-forced changes

The implementation settled several choices that were open in the design notes:

- Mixed-version datadirs are supported. Commitment files keep `min: v1.0`, and the read path uses
  each file's version to choose its state key and row or edge-record representation. The earlier
  recommendation to require `min: v3.0` was not adopted.
- The commitment converter detects v3 keys exactly but refuses to reshape v3 edge records. It
  remains a legacy V1/V2 converter; edge-record migration and history conversion are separate work.
- Integrity checks that require bundled rows return an explicit unsupported error for edge-record
  files. They do not attempt to reinterpret an edge value as a row. The integration branch dump is
  the diagnostic consumer that supports rendering the edge format.
- The commitment domain replaces `AccessorHashMap` with the ordered and existence accessors. This
  is a schema property, not a per-file format choice.

## Invariants added during implementation

- A storage leaf edge record is valid only after its enclosing account cell has been loaded. The
  decoder returns an explicit error when that context is absent; no depth-64 account record may be
  assumed to exist.
- A deferred parent prefix stays pending until its complete edge-record run is applied, while
  last-write-wins is resolved by the full child key. A partial run must not be exposed as a complete
  synthesized branch row.

## Problem

One commitment branch node is one DB record holding all 16 child cells
(`touchMap|afterMap|{cell fields}*`). Changing one child rewrites the whole row, so both the domain
and its history amplify by the row's arity.

The write path already computes the exact per-cell delta and then destroys it:

1. `commitment.go:474` — `prev, _, err = ctx.Branch(prefix)`, a read per touched branch per block
2. `commitment.go:483` — `EncodeBranch(bitmap, ...)` with `bitmap = touchMap & afterMap`; emits field
   bytes only for cells in `bitmap`. **This is the delta.**
3. `commitment.go:492` — `merger.Merge(prev, update)` carries every untouched cell forward
4. `commitment.go:500` — `PutBranch(prefix, full_row, prev)`; history gets the full prev row

**The prize is write volume, not file size. File size gets worse — see Costs.**

## The model

Stop at step 2. Persist one record per present child.

### Keys: no parity byte

The V2 parity byte exists only because two nibbles pack into one byte and an odd path pads. Fold the
parity into a terminator that also carries the odd nibble:

```
key(P)  = pack(P) || term        term = 0x00           if len(P) even
                                 term = 0xf0 | last(P) if len(P) odd
record  = key(P) || (0x80 | n)   the record FOR child n of P
```

`pack(P)` packs whole nibble pairs only; the odd nibble rides in `term`. A **node key** is
`floor(d/2)+1` bytes — one shorter than V2's `ceil(d/2)+1` on odd depths, equal on even — and a
**record key** is one more, `floor(d/2)+2`. `ErrV2NonCanonicalPad` stops existing: there is no pad
nibble.

Terminal byte ranges are disjoint and exhaustive: a trie record always ends `0x80..0x8f`, and the
byte before it is `0x00` or `0xf0..0xff`. Encoding detection becomes an exact test rather than
`detectKeyEncoding`'s statistical vote.

Every trie key is at least 2 bytes, so a 1-byte key is unambiguous and sorts before all of them.
`KeyCommitmentState` moves from `[]byte("state")` — which ends `0x65`, sorts at `0x73` in the middle
of the trie, and has to be skipped by every scan — to **`[0x00]`**, the first record in the file.

### Records: one per present child

Cells are edges, not nodes: a persisted cell describes the edge into a child, carrying that child's
extension and hash. So the natural record is the edge, keyed at the parent's slot.

```
flags  bit0 kind (0 branch, 1 leaf)   bit1 ext parity
       bit2 leaf kind (0 acct, 1 stor)   bit3 has storage   bit4 hash present

branch child        [flags][mask:2][hash:32][ext:tail]                      35..67
storage leaf        [flags][hash:32][plain:32]                              65
account leaf        [flags][hash:32][plain:20]                              53
account + storage   [flags][hash:32][sroot:32][mask:2][plain:20][ext:tail]  87..119
```

All widths are fixed and every uvarint is gone. That is not a compression trick, it is what the code
already guarantees: `fold` sets `upCell.hashLen = 32` unconditionally
(`hex_patricia_hashed.go:1733`), `computeCellHash` sets it from a `common.Hash`, and `EncodeBranch`
gates `fieldStateHash` on `stateHashLen == 32` (`commitment.go:596`). With referencing off
`accountAddrLen` is 20 and `storageAddrLen` is 52. Four of the five `putUvarAndVal` sites carry a
constant.

Only the extension varies, and it is the record's tail, so its length is implied and only its parity
needs a bit. That is what buys the packing: `cell.extension` is `[64]byte` holding nibbles unpacked,
so packing two per byte halves every extension and pays back the flags byte at `|ext| >= 4`.

`fieldHash` and `fieldStateHash` collapse into one field. `computeCellHash` is exactly binary —
`stateHashLen > 0` returns `[0xa0][stateHash]` and increments `skippedLoad` without touching the DB;
`== 0` falls through to a load. No record needs two hashes of the same node. An account's `sroot` is a
*different* node's hash, so it stays a separate field.

`hash present` is not dead weight. `canEmbed := !singleton && totalLen+pl < length.Hash`
(`hex_patricia_hashed.go:776`) makes a short leaf's RLP the result instead of a keccak, so
`stateHashLen != 32` and the `== 32` gate drops it. Those records genuinely have no hash and the
reader must reload state. Rare — it needs a short remaining key, so only near depth 128 — but real.

`has storage` gates `[sroot][mask]`, so EOAs never carry an empty storage root. Cost of fusing the
depth-64 node into one record rather than splitting it across `0x80|n` and `0x90|n`: a balance-only
change on a **contract** rewrites 87..119 B instead of 53. EOAs, which are the bulk, are unaffected.

### The mask lives in the parent

There is no node record. A node's mask and hash live in the record for the edge that reaches it, one
level up. The root has no such record, so its mask goes in the state blob.

This is what makes "address it or create if nonexistent" work: reading child *n*'s record yields the
mask naming which of *its* children's keys exist. It also means the descent read and the fold read
are the same read — nothing is ever fetched twice — and it lines up with the parallel fold's existing
contract, where a worker owning `P‖n` returns exactly one cell to the coordinator and the
coordinator's write of `P`'s record is where that mask and hash land. Invariant 9 is unchanged.

The cost is that a record cannot be located without its parent. Every entry into the trie starts at
the root and descends. The fold already does; so does the warmuper, per depth per key.

## What this deletes

- `BranchMerger.Merge` (`commitment.go:1066`) and `MergeHexBranches` (`:877`) **off the v3 write
  path** — `hashRow` already emits `cellEncodeData` for **every** present cell, so records re-encode
  wholly from memory. They remain for the legacy row path and mixed-version reads.
- The `ctx.Branch(prefix)` read at `commitment.go:474` — nothing to read-modify-write.
- `IsComplete` (`:867`) and `touchMap` as persisted concepts on the v3 path; the guarded helpers
  remain for legacy bundled rows.
- The 4-byte row header.
- The persisted `extension`/`hashedExtension` mirror pair. The in-memory mirror remains because trie
  hashing and descent still need both forms; v3 records store only the packed extension tail.
- `deriveHashedKeys` over siblings — per-edge keying knows the next key outright.
- Every uvarint in v3 edge values, and the `fieldHash`/`fieldStateHash` split.
- The V2 parity byte.

**Out of scope: key dereferencing.** Disabled since 3.6 (`DefaultReferencesInCommitmentBranches =
false`). Parked, not removed. This design neither depends on it nor deletes it.

## Read model

### There is no fold-order walk

**No packed key encoding puts the file in fold order.** A node's terminator byte sits at the same key
position as a descendant's packed byte, and packed bytes span all 256 values, so whatever terminator
is chosen some descendants sort below it and some above. With `term_even = 0x00`, `subtree(P‖0‖0)`
straddles P's own child run. With `term_odd = 0xf0|n`, the odd node's entire subtree sorts *before*
its record — and that one is unfixable: an odd node would need `term < n<<4` for all children, which
for `n=0` means `term < 0x00`.

Unpacking to one nibble per byte fixes all of it — true preorder, perfect adjacency, no terminator,
no parity, state blob at `0x10` outside the nibble range, and it crosses `MinPatternLen: 20` at depth
20 instead of 40. It is not payable: storage-side branch nodes live at depth 64-72 because the path is
`keccak(addr)‖keccak(slot)`, so keys go 37 B to 71 B on the 1.7B records that dominate the domain.

A single monotonic cursor is therefore not available for the incremental fold. It **is** available for
a bulk rebuild, which processes every key and can drive the traversal from file order instead of trie
order.

### Unfold

```
unfold(P):                       # mask came from P's own record, read at the parent
  for f in files, newest -> oldest:
      c = bt[f].Seek(key(P) || 0x80)
      while c.Key() has prefix key(P) and len(c.Key()) == len(key(P))+1:
          take slot if the mask sets it and it is not already seen
          c.Next()
      stop as soon as popcount(mask) slots are covered
```

One `Seek` plus a short `Next` run per file per touched node, bailing to a re-`Seek` at the next
expected slot key when the length filter rejects. Intrusion inside the run is `subtree(P‖0‖0‖8)` for
even P (~1/4096 of P's subtree) and `subtree(P'‖15‖a‖8)` for odd P (~1/256).

**The mask is the sole authority on slot existence.** A stale record for a cleared bit can survive in
an older file; the run filters by mask and a present record never means present.

The mask also gives an exact early exit, which `lookupLatestFromFiles` cannot do today — it returns at
the first hit (`domain.go:1464`). Here the walk stops the moment every present slot is accounted for.

`TrieContext.Branch(prefix)` keeps its current signature and synthesizes a row under the hood, so
`decodeBranchIntoRow`, `BranchCache`, the warmuper, `CollectDeferredUpdate` and `HasPendingPrefix` are
untouched. The cache stays row-granular, which means one slot write invalidates a whole cached node —
read amplification reintroduced in RAM. Moving to a node cursor with a `(prefix, nibble)`-keyed cache
is a follow-up, deliberately not bundled here.

### Accessor

`AccessorBTree` is required and is now part of the commitment schema together with
`AccessorExistence` (`statecfg/state_schema.go`). `Cursor.Next` is `c.d++` plus one Elias-Fano `Get`;
the pivot binary search and interpolation search run only on `Seek`. The commitment domain no longer
uses `AccessorHashMap`, and `BuildMissedAccessors` supplies `.bt` and `.kvei` for files that lack them.

`BtIndex.Seek` is a lower-bound search — `bs()` returns the insertion point, `cur.Reset(l, g)`
positions by ordinal, and it returns `(nil, nil)` past the last key (`bps_tree.go:350-357`). The
cursor holds `d` plus `ef.Get(d)` and `resetNoRead(di, g)` repositions to any ordinal in O(1), so:

- `RSeek` (strictly greater) = `Seek` then skip-if-equal.
- `LSeek` (strictly smaller) = `Seek` then decrement, positioning at `Count()-1` explicitly for the
  past-the-end case where `Seek` returns nil.

Neither needs a new index structure. **`LSeek` cannot find the nearest ancestor record**, which is the
obvious thing to reach for it: for an odd-length ancestor `A`, `key(A)` ends `0xf0|a` while its
descendants carry `a<<4|b` at that position, so the ancestor sorts after its own subtree and "greatest
key < x" walks past it. Ancestor lookup enumerates the <=128 candidate keys and binary-searches on
depth with exact `Seek`s — about 7 probes. `LSeek` is sound for the record preceding a run and for
backward range scans.

`AccessorExistence` matters for the file-stack walk and for whatever point lookups remain. Its
resident bloom feeds the Blocker below.

**Page-level compression is out of scope.** `Domain.dataWriter` returns a plain `*seg.Writer`
(`domain.go:1682`) while only `History.dataWriter` returns a `*seg.PagedWriter`; the `.bt` is built
word-by-word over `d.dataReader(valuesDecomp)`, so page-granular domain values need a paged variant of
`btindex`/`bpstree` — the "bucketed pivots persisted in the `.bt`" that #20180 records as unbuilt.
That is a prerequisite with its own cost, not a config flip.

## Deletion

Delete child *n* of P: clear bit *n* in P's own record — rewritten anyway, since P's hash changed —
and write a **zero-length** value at `key(P)‖(0x80|n)`.

The tombstone exists only for collection. Reads are mask-driven and never ask for a cleared bit, but
without a tombstone the orphaned record is copied forward at every merge forever. Zero-length reaches
`deleted := r.values.from == 0 && len(lastVal) == 0` at `db/state/merge.go:506-509`. Today's 4-byte
`{touchMap, afterMap=0}` never can: `IsTombstone()` is `len(branchData) == 0` (`commitment.go:642`)
and `EncodeBranch` writes the header unconditionally, so **every deleted branch on mainnet today is
immortal.** That is a live defect in the current format, countable from existing `.kv` files, and
fixable independently of everything else here.

No new enumeration. `deleteCell` (`hex_patricia_hashed.go:2056`) is driven per key from the update
stream and the fold visits every node whose children changed, so per-slot tombstones cost what
per-row ones cost today. This does not reintroduce SELFDESTRUCT slot enumeration.

Careful with invariant 5: single-child branches are never persisted, so "record absent" already does
not mean "no state". A missing slot must keep meaning "consult the ordinary fold", never "empty".

## Root state blob

`KeyCommitmentState` is rewritten every commitment with a fixed skeleton from `state.Encode`
(`hex_patricia_hashed.go:2720`): 3 bytes of flags, `Depths [128]int16` at one byte per row (128),
`TouchMap [128]uint16` (256), `AfterMap [128]uint16` (256), `BranchBefore` packed to 2x uint64 (16),
then the encoded root cell (~56). 659 fixed + ~56 = ~715 bytes per block (awskii, measured).

The four per-row arrays are traversal scratch. The blob is only ever written or loaded with the trie
at rest — `EncodeCurrentState` panics on `currentKeyLen > 0` and `SetState` refuses `activeRows != 0`
— so `SetState` copies 656 bytes into an empty range every restore.

Under this model the blob stops being a checkpoint and becomes structural: the root's mask has nowhere
else to live. Drop the four arrays, add `[mask:2]`, keep flags and the root cell, version it. Net
about -654 B/block.

## Costs

### Size

Record count goes from one per persisted branch node to one per present child. The previous
revision's "record-count multiplier is exactly <=2x" applied to a model where only branch-pointer
children became records; **it does not hold here.** With ~1.7B storage leaves, ~300M accounts and
~600M branch nodes, the count is roughly 4.4x.

Per-record delta, derived and not measured — the arithmetic is exposed so it can be re-run with real
values:

| Class | count | key added | value delta | total |
| --- | --- | --- | --- | --- |
| storage leaf | 1.7B | +37 (depth 64-72) | -22 (2 uvarints, hoist) | **+25 GB** |
| branch child | 595M | +37 storage-side, +6 account-side | ~+2 - ext/2 | **+21 GB** |
| account leaf | 300M | +6 (depth <=10) | ~-2 | **+1 GB** |
| row headers | 595M | — | -4 | **-2 GB** |

About **+45 GB on ~180 GB, roughly +25%**, before compression. Keys compress — the storage-side
records under one account share a 32-byte prefix, well past `DomainCompressCfg`'s `MinPatternLen: 20`
— and hashes do not. This is worse on both axes than the previous revision's node-record model, and is
paid for by the absence of frontier write amplification, uniform records, and the descent read
doubling as the fold read.

`.bt` is affordable at 4.4x the keys: pivots every `DefaultBtreeM` (64, `BT_M`), all offsets in one
Elias-Fano array. 2.6B offsets is on the order of 1.5 GB.

### File-stack depth

This is the governing risk and it is inherent. Today a row is rewritten whenever any child changes, so
it is always in the newest file and `lookupLatestFromFiles` terminates immediately. Per-edge, an
untouched sibling's last write is by construction old, so a cold node walks the stack until the mask
is covered. Commitment is excluded from the read cache and carries no existence filter today
(`domain.go:1471-1472`).

Mitigations, in order: the mask's exact early exit; `AccessorExistence` to skip files that certainly
lack the prefix; and the structural argument that hot slots live in new files while cold ones collapse
into the bottom-most merged file, so realistic depth is 2-4 rather than the full stack. **Unmeasured.**
`depthsToTxNum` and the `step` returned by `readDomain` already exist to measure it.

## What the row-model analysis established and this does not change

### The measurement gate still binds

`.kv` size grows; the write-volume win is the only justification and remains unquantified. Two
experiments, neither run:

**Experiment 1 — cost budget from existing files.** Walk every commitment `.kv` row by row; classify
each present cell; synthesize the v3 key/value stream for the record classes above; feed it through a
real `seg.Compressor` with `DomainCompressCfg` and compare produced file sizes. Bucket by **decoded
nibble depth**, not key byte length. Run four arms so independently-landable levers are not credited
to the split: current / current-tombstones / v3 / v3+hoist.

Three traps that silently corrupt the aggregate: cell classification is not a dichotomy, since an
account cell carries `accountAddr` *and* a storage-root hash; referenced files
(`CommitmentBranchReferenced`, any commitment `.kv` < v2.2) hold shortened keys that `decodeCells`
parses without error, deflating leaf payload; and `decodeCells` iterates `touchMap`, so cells present
in `afterMap` but absent from `touchMap` come back nil.

Do not reuse `DecodeBranchAndCollectStat`'s byte accounting (`commitment.go:1231`). `MinCellSize` /
`MaxCellSize` use `cell.Encode()`, a *different* encoding that includes `hashedExtension`, omits
`stateHash`, and returns a max-size buffer rather than `buf[:pos]`; its per-field counters are a
priority `switch`, not a partition. Reuse the iteration skeleton, not the accounting.

The same walk yields, for free: the leaf-vs-branch mix by depth; dead-tombstone bytes; and `L`, the
leaf children per node, which every per-level estimate in this document assumes.

**Experiment 2 — write volume, the deciding one.** Sum `len(updateCopy)` at `PutBranch`
(`commitment.go:499`) per block. That settles mean row size, bytes/block, and whether "50-70k per
commitment" is per block or per batch — a handful of lines. Then histogram `bits.OnesCount16(bitmap)`
against `bits.OnesCount16(afterMap)` bucketed by `len(prefix)`, at both per-block and
per-collation-range granularity. **They are different numbers** — `.kv` size is set by the last value
per key per collation range, so over a ~2,000-block step a hot node accumulates touched children
toward `T/A -> 1` where the split loses — and the acceptance rule must say which binds.

**The headline prize number still fails its own sanity check.** Reading "~1 KB row x 50-70k branch
writes per commitment" as ~50-70 MB per block is impossible: a step is 390,625 txnums (~2,000 mainnet
blocks), so that is ~100 GB per step against a whole-chain commitment `.kv` of ~168 GiB, implying
~1.1 PB written where merge amplification is O(log), ~10-30x. At least one of {~1 KB mean row, "per
commitment" == "per block", ~168 GiB total} is wrong. No MB/block figure is stated here until it is
reconciled.

**Acceptance rule, pre-committed:** build only if the measured write-byte distribution puts the
majority of branch-write bytes below break-even AND measured file-stack depth for cold slots stays
bounded under an existence filter. Either one failing stops it.

### Measured baseline (snap-arb1 arch-0, 2026-08-27)

Live node, `--experimental.parallel-commitment`, PR #23588 metrics wiring, from-0 archive at
~blk 8.14M. Two scrapes of `localhost:6061/debug/metrics/prometheus`:

| Counter | Sample 1 | Sample 2 | Delta |
| --- | --- | --- | --- |
| `domain_commitment_keys` | 483,074,989 | 484,402,823 | 1,327,834 |
| `domain_commitment_updates_applied` | 875,478,463 | 878,083,741 | 2,605,278 |

1.96 branch records written per state key processed (1.81 lifetime). Same scrape:
`commit_branch_read_rate` 9192/s against `commit_account_read_rate` 286/s and
`commit_storage_read_rate` 196/s.

**Regime: bulk from-0 archive sync under batch commits, not head.** The design targets head, where the
update set is sparse and both the scan argument and the 19:1 branch-read ratio look far worse. Two
scrapes only, no block or wall-clock delta, so these support a ratio and nothing per block. Treat
`commit_blocks`, `commit_txns`, `commit_fold_rate` and `commit_unfold_rate` as dead — nothing writes
them.

### Independently landable, and must be measured alone

1. **Tombstone fix** — the only already-existing defect here, and it is a permanent leak. Dead
   tombstone bytes are directly countable.
2. **Lazy `deriveHashedKeys`** — `decodeBranchIntoRow` calls it unconditionally for every present cell
   (`:1518`), a real Keccak256 per leaf-bearing cell. An untouched sibling with a valid `stateHash`
   never reads the result. Paired historically with dereferencing all 16 shortened keys per row read;
   dereferencing has been off since 3.6 and the derivation was left behind.
3. **Extension memoization** — `computeCellHash` does `cell.stateHashLen = 0` unconditionally on the
   extension branch (`hex_patricia_hashed.go:1203-1206`, trace `EXTENSION HASH %x DROPS stateHash`),
   *before* the memoization check. Every account whose storage sits behind an extension reloads its
   balance, nonce and codehash on every fold, forever. It is conservative because a row cannot tell
   whether the storage root that produced the `stateHash` is still current. In the fused account record
   those fields are written atomically, so a read that did not rewrite the record proves they are
   consistent and the drop becomes unnecessary. **A read-side win this design enables and the row
   format cannot have.**
4. **Root state blob slimming** — independent and cheap, ~654 B/block.
5. **Commitment ordered accessor** — enabled in the v3 commitment schema. Its earlier measurement
   will not transfer:
   V1 keys are `HexToCompact` with a *leading* flag byte, so every even-length path sorts before every
   odd-length one and a cursor fold needs two cursors per file for the parity split. Treat a number
   from it as a lower bound.

Do not let the split claim 1-4.

## Migration

`erigon commitment convert` (`db/state/commitment_convert.go`) still re-encodes legacy V1/V2 keys
offline. Its detector now has an exact v3 state, but conversion refuses v3 edge-record files rather
than attempting a row-to-record reshape. The converter remains `.kv`-only; edge-record migration and
history conversion require separate tooling.

Mixed-version support is the selected rollout boundary. `min: v1.0` keeps old files readable, while
the read and merge paths select the state key and value representation from each file's version.
This avoids requiring a history converter before a staged domain-side rollout.

### File set

Commitment's file-type *set* changes, not just its versions:

| File | commitment today | v3.0 |
| --- | --- | --- |
| `bt` ordered accessor | absent | **add** |
| `kvei` existence | absent | **add** |
| `kvi` hashmap accessor | v2.1 | **retire** |
| `kv` values | v2.2 | **v3.0** |
| `hist.v` | v2.0 | **v3.0** |
| `hist.vi` / `ii.ef` / `ii.efi` | v1.1 / v3.0 / v2.0 | bump — keys re-encoded and multiplied |

`ii.ef` is already at v3.0 for unrelated reasons; "commitment v3.0" means `commitment.kv` v3.0, the
umbrella name.

History-side: `hist.v`/`hist.vi`/`ii.ef` for commitment exist only under
`--prune.include-commitment-history` and are webseed-distributed when it is on
(`snapshotsync.go:524-526`). The bump invalidates any published commitment-history snapshot set and
needs coordinating with the snap36 fleet. The domain-side bump has no such exposure on a default node.

## #21146 is substrate, and its regression still gates

V2 keys were benchmarked wired into the **old row-per-node model**, where prefix-sort locality buys
nothing — one point lookup per row, so collocation had no consumer. Coming out at -6% unconstrained is
what substrate without its superstructure looks like.

Its mainnet A/B is the blocker: unconstrained V2 ~= V1, but under a 32 GB cap it was **OOM-killed
after 26h** where V1 survived — +43% Go heap, +36% sys, 2.25x commitment time, 1.39x major page
faults. Alex asked where the RAM goes; unanswered since 2026-06-08. **It gates harder here**: 4.4x the
key count pushes `.bt` entries, DB key space and a new existence filter's resident bloom on the same
axis as that unexplained heap. Answer it on the existing branch before closing it — the wiring commit
is the only thing that reproduces it, and once v3 exists the question is confounded with the record
multiplier. One cheap candidate: V1 segregates even- and odd-length paths into two contiguous key
regions via the leading flag byte, while V2 interleaves them in trie order, changing MDBX page
locality and dirty-page count in `TblCommitmentVals` within a batch. The v3 terminator interleaves the
same way, so the hypothesis transfers.

`EncodeKeyV2`/`DecodeKeyV2` are already on main via #21933 and used only by the offline converter.
Close #21146 as superseded once the heap question is answered: only the wiring commit dies, and it
rewrites `unfoldBranchNode`, `fold`, `CanDoConcurrentNext`, `validatePlainKeys` and `verify.go` to
call `EncodeKeyV2`, every one of which changes shape again here.

## Deferred and concurrent path

`CollectDeferredUpdate` (`commitment.go:510`), the per-goroutine `localCollector` ETL in
`TrieContext.PutBranch`, and `readBranchAndCheckForFlushing`/`HasPendingPrefix` remain keyed on the
whole prefix. `TrieContext.Branch(prefix)` keeps its signature, while a pending prefix spans a run of
records and last-write-wins resolves per record rather than per row. Invariant 9 holds: prefix
ownership stays disjoint, and the coordinator owns `P` while a worker owning `P||n` returns one cell.

## Changesets and unwind

`DomainPut` carries `prevVal` into a `kv.DomainEntryDiff{Key, Value}` per key
(`changeset/state_changeset.go:156-165`) and unwind replays those and nothing else
(`domain.go:1391`). Per-edge changesets therefore contain one entry per changed record; the tests
assert byte parity stays within the bundled-row budget and entry growth stays within the record
multiplier for one-child and full-node updates. This remains the **reorg hot path** over the dense
96-block window and still needs production measurement.

## Consumers

Records are self-describing in shape but not in position — a record cannot be located without its
parent, and a storage leaf inherits its address from the enclosing account cell. Standalone row
decoders now either receive the descent context or refuse edge values explicitly:
`DecodeBranchAndCollectStat`, `Validate`, `VerifyBranchHashes`, `IsComplete`, `ChildCount`, the
`db/integrity/` scans, `cmd/integration/commands/commitment.go:257`, `db/state/squeeze.go`, and the
converter. In particular, `ReplacePlainKeys` and the other legacy row parsers reject a 35-byte edge
record instead of treating it as `touchMap|afterMap|cells`.

The current integrity consumers that require bundled rows refuse v3 files explicitly with
`ErrCommitmentEdgeRecordsUnsupported`. Supporting direct edge-record integrity verification remains
future work; the integration branch dump does decode and render individual edge records.

## Verification

Root parity is a weak oracle (invariant 14). The in-repo oracle is `StateRootVerifyByHistory`
(`db/integrity/integrity_action_type.go:91`, run by `CheckCommitmentHistAtBlkRange`,
`commitment_integrity.go:1157`), stronger because it rebuilds state per sampled block
from accounts/storage history in a fresh `SharedDomains` rather than trusting what the run under test
persisted. The acceptance tests assert stored-record byte parity over three incremental batches,
including a `.kv` merge, and then rebuild from the fresh state path. Batch-2 damage only surfaces as
batch-3 divergence.

## Open

- Reconcile 50-70k branch writes/block against trie geometry. That count implies far more touched
  nodes than ~1k touched accounts x depth predicts; either it includes storage rows and repeated
  folds, or the geometry model is wrong. Whichever it is changes `L` and every per-level number here.
- Whether the node-cursor read path and a `(prefix, nibble)`-keyed `BranchCache` land in v3.0 or after.
