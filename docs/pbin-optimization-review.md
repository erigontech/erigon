# PBin optimisation review

Six agents surveyed `execution/commitment/pbin_*.go` for architectural and performance wins.
This is the reviewed result: what measurement supports, what it refutes, and what to build.
Numbers without a stated source are measured on snap-arb1 against the converted mainnet
state (root `ac1aad1f`, block 25743399).

## The measurement that reframes everything

Seg headers of both variants' commitment `.kv`, all seven ranges. The bin column reproduces
`pbin-mainnet-conversion.md` exactly, which validates the parse.

| range | hex recs | hex B/rec | bin recs | bin B/rec | rec ratio | total |
|---|---:|---:|---:|---:|---:|---:|
| 0-8192 | 702,783,840 | 256.79 | 2,530,448,382 | 126.49 | 3.601 | 1.774 |
| 8192-9216 | 203,536,325 | 297.85 | 658,227,540 | 112.29 | 3.234 | 1.219 |
| 9216-9472 | 84,111,887 | 362.95 | 248,864,141 | 104.76 | 2.959 | 0.854 |
| 9472-9504 | 19,099,009 | 419.41 | 53,904,798 | 97.32 | 2.822 | 0.655 |
| 9504-9520 | 10,293,944 | 431.86 | 31,060,897 | 111.74 | 3.017 | 0.781 |
| 9520-9524 | 2,950,662 | 476.82 | 9,482,352 | 106.52 | 3.214 | 0.718 |
| 9524-9526 | 1,748,683 | 487.26 | 5,606,345 | 105.74 | 3.206 | 0.696 |
| TOTAL | 1,024,524,350 | 279.48 | 3,537,594,455 | 121.66 | 3.453 | 1.503 |

Bin's bytes/record is flat, 97-126 B everywhere. Hex's nearly doubles as ranges shrink,
256.79 -> 487.26 B. The record-count ratio barely moves.

The inversion is a hex effect, not a bin effect. A bin node always has exactly two children,
so its record size is depth-invariant. A hex node has up to sixteen, and in a small
incremental file the surviving record population shifts toward the root, where hex nodes are
dense and fat; in a bulk file most hex records are deep and thin.

**Consequence.** In the bulk regime the only lever that matters is record count. Shaving dead
bytes out of a 121 B record buys single-digit percentages; cutting record count toward hex's
buys up to 3.45x, and the same factor off the read amplification, since each record read is a
page fault.

## Ranked work

### 1. Block several branch nodes per record, on the rebuild path only

One record per binary branch node is the entire bulk disadvantage. A hex node collapses 3.601
binary nodes; a block of n binary nodes has 2n edges of which n-1 are internal, leaving n+1
boundary child hashes to store. At n=3.601 that is 4.601 hashes -- exactly the average hex
node's child count. Plain keys are stored once per leaf either way. The only structural
residual is shape: hex spends a 16-bit child bitmap, a bin block must encode its internal tree
shape.

| shape encoding | range 0-8192 | vs hex | off today's 320.1 GB |
|---|---:|---:|---:|
| 4 B/block | 180.5 GB | 1.00x | 44% |
| 7 B/block | 182.6 GB | 1.01x | 43% |
| 12 B/block | 186.1 GB | 1.03x | 42% |

Bin's size disadvantage is not the binary structure. It is paying a whole record's framing,
key and maps for every single two-child node.

No new hashing machinery is required. `pbinHasher.cellHash` already has both paths: a branch
cell with `childrenSet` recomputes via `branchHash`, otherwise it uses the stored hash.
`foldBranch` sets `childrenSet` on parent cells today, so the path is live. A blocked record
decodes bottom-up -- internal nodes get `childrenSet`, boundary nodes take the stored hash.
Blocking is a decode-order change, not a hashing change, and the EIP-8297 root is unchanged by
construction because internal hashes never leave the record.

Scope it to the rebuild path. Merge cannot produce it: the only per-record hook the aggregator
offers on the commitment domain is `commitmentValTransformDomain`'s transformer, typed
`func(val []byte, startTxNum, endTxNum uint64) ([]byte, error)` — one value in, one out, never the
key, unable to drop or coalesce records, and not even run for the bin variant. So a tip-grown
datadir stays unblocked and only a rebuilt one shrinks. Blocks must also be a linear spine rather
than an arbitrary subtree, because `pbinGrid` represents a single root-to-probe spine and an
off-spine internal node has no row to occupy; a spine of k nodes exposes k+1 boundary cells, which
is the measured average hex node's child count, so the estimate is unchanged. Touching one leaf rewrites a whole block, which is
precisely hex's behaviour and precisely why hex's small files are fat. Blocking at the hot tip
would recreate that pathology in the one regime where bin currently wins at 0.70x.

Cost: fold/unfold bookkeeping becomes per-block; `unfold` currently adds exactly one row per
call and `unfoldBranchNode` fills one row from one record.

### 2. Store the leaf's hash in its parent record — BLOCKED

`PBinPatriciaHashed.cellHash` calls `loadCellState` for every leaf cell. `pbinDecodeCell`
never sets `loaded`, so a leaf that arrived from a record always misses and issues a fresh
`ctx.Account` / `ctx.Storage` read. `hashRowCell` caches the hash back only when
`c.kind == pbinNodeBranch`, so a leaf never carries one.

Every `foldBranch` hashes both cells. The untouched sibling therefore costs one random
state-domain read purely to recompute a hash that could have been stored.

In the bulk regime this rarely fires -- siblings are usually written together and already
loaded. In the incremental tip regime it fires on nearly every touched key, because a mature
tree's sibling is almost always cold. This is the tip-regime optimisation, and the tip is the
regime that matters once the tree exists.

**BLOCKED — this item cannot be built as described.** Taking the stored hash skips
`loadCellState`, and that call is the only source of `errPBinDeleteUnsupported`, which fires when
a record outlives its state. The only property separating a safe untouched sibling from a
dangerous one is whether the state still holds its value, and reading that *is* the call this
item exists to delete. `TestPBinStorageZeroOnUntouchedSiblingRefuses` constructs the failure on
the ordinary **tip** path — `pbinTestEngine` plus `pph.Process`, no rebuild — so gating the fast
path by regime does not rescue it. Skipping the read would also take the leaf out from under
`leafCellHash`'s `emitNode`, silently dropping untouched leaves from witnesses.

Cost, had it been buildable: +32 B per leaf cell in every regime, which is **more** than items 3
and 4 remove together — item 3 budgets 11.33 B per storage-leaf cell and item 4 saves 12 B per
record. So it would have cost disk as well as the backstop.

Unblocking it means moving the removal-completeness check somewhere cheaper than the fold — the
update stream asserting it, rather than the fold detecting it by reading. That is its own design
problem and is not scoped here.

### 3. Drop the leaf's prefix; it is recomputable

A **storage** leaf cell stores its plain key and its prefix. `storageAddr` is the full 52-byte
`addr || slot`, which determines the tree key outright through `pbinDigestCache.storageKey`,
and every caller already holds the descent depth before it can build the DB key to fetch the
record. So a storage leaf's prefix can be recomputed.

**This does not extend to account leaves.** An account tree key is
`zone | 32-byte stem | sub-index`, and the same 20-byte `accountAddr` produces the BASIC_DATA,
CODE_HASH and DELEGATION leaves. `pbinCell` has no sub-index field, and
`pbinDigestCache.treeKey` hardcodes `accountKey(plainKey, pbinBasicDataLeafKey)`, so the prefix
is the only carrier of which leaf it is. Dropping it makes an account with zero basic data and a
non-empty code hash decode as sub-index 0 and move the root. Nothing is lost by keeping it: two
header leaves in one stem separate at the final path bit, so their prefixes are empty anyway.

These are digest tail bits: high-entropy, matching no dictionary pattern, copied verbatim by
`db/seg`. Removing them removes close to 1:1 disk bytes.

Size, bounded from the measured 121.66 B/record rather than estimated:

| leaf shape in the record | fixed bytes | prefix budget, both cells | bits/cell |
|---|---:|---:|---:|
| storage leaf (52 B key) | 99 | 22.66 B | 90.6 |
| account leaf (20 B key) | 67 | 54.66 B | 218.6 |
| code leaf (32 B value) | 79 | 42.66 B | 170.6 |

Dropping the **storage** leaf cell's prefix saves ~11.33 B per such cell. The earlier
"39-96 GB, 9-22%" figure assumed every leaf shape qualified and is **withdrawn**: account leaves
are excluded on correctness grounds and carry empty prefixes in the common case anyway, so the
real saving is the storage-leaf share of that range and needs the record-shape histogram to pin
down.

Decode gains a dependency on the digest cache and hash suite that it does not have today.
That is a real architectural change and should be scoped as its own step.

Code-chunk leaves are the exception: `updateCell`'s empty-plainKey branch keeps only the raw
value, never `codeHash`/`chunkID`, so there is nothing to rehash from. They keep their prefix.

### 4. Drop the dead header and the dead length prefixes

`touchMap` is write-only -- `unfoldBranchNode`, `dropSubtreeRecords` and `materializeBranch`
all discard it with `_`. Every *persisted* `afterMap` is `0b11`, because `foldBranch` guards
`bits.OnesCount16(...) != 2` and is the only caller whose output reaches disk. `foldBranch` is
**not** the only caller of `encode`: `pbinWitnessContext.branchRecord` is a second production
caller, passing `pbinCellBits` for both maps. It synthesizes records in memory and never
persists them, so the conclusion holds — but any change to `encode`'s signature must update it.

Separately, `pbinAppendLenAndVal` writes a uvarint length before every fixed-size field, and
`pbinDecodeFixedVal` then rejects any length that is not the compile-time constant. The byte
carries no information and the decoder proves it: `accountAddr` 20, `storageAddr` 52,
`leafValue` 32, `hash` 32.

Together 12 B of a ~121 B record, ~10%. Unlike item 3 these are low-cardinality fixed-position
bytes that seg's dictionary is built to eat, so the on-disk residual must be measured, not
assumed.

### 5. Bound the grid to real depth

`pbinGrid` is 422,264 B, from `rows[528][2]pbinCell` at 384 B per cell, embedded as a fixed
array and therefore eager. Real peak depth is ~30-60 rows, not 528, since a row consumes a
whole compressed prefix rather than one bit.

Two caveats keep this off the top of the list. `HexPatriciaHashed`'s grid is 951,760 B, so
bin's is already 2.25x smaller -- the port did not inflate this, it moved the
over-provisioning from column width to row count. And `PBinPatriciaHashed` is recycled through
`pbinPool`, so the footprint is per pooled instance, not per operation; the 64% of test-suite
allocation attributed to `NewPBinPatriciaHashed` is a test artefact of creating many engines.

Use a growable slice, not a smaller constant. A fixed bound is a correctness risk on a
legitimately deep path.

### 6. Shard by tree-key range, not plain-key range

`RebuildCommitmentFiles` slices one plain-key-ordered iterator into shards by count, and each
shard starts a fresh `SharedDomains`. Tree keys are a hash permutation of plain keys, so every
shard's keys scatter across the whole tree, and shard i must unfold against the tree shards
0..i-1 already built. Cost tracks the inherited tree, not the keys converted -- which is why
the 64 shards of range 0-8192 ran 4m38s to 2h29m17s, monotonically increasing with index.

Bucketing by tree-key range gives each shard a disjoint subtree. It also converges with the
sort-then-bulk-load converter that `pbin-mainnet-conversion.md` already flags as unmeasured.

## Rejected, with the reason

- **"Exploit path compression / fix the depth."** Already optimal. `pbinUnfoldConsumed`
  returns `prefix.bitLen` for a descend, consuming the whole compressed prefix in one step,
  and `pbinCommonPrefixBitsAt` finds divergence a 64-bit word at a time. The arity-2 depth
  cost is structurally forced by EIP-8297 and shows up as record reads, not iterations.
- **BLAKE3 call-path micro-optimisation.** Already zero-allocation: the preimage is built into
  a fixed `[133]byte` embedded in the hasher and the engine is pooled. No library API batches
  independent tiny preimages across SIMD lanes.
- **Replacing `OnesCount16`/`TrailingZeros16` with 2-bit masks.** Same instruction count. The
  `!= 2` check is a real correctness guard, not vestigial machinery.
- **`afterMapUpdateKind` sharing with the hex engine.** At arity 2 the generic classifier
  degrades to exactly bin's three cases with no waste. Sharing was correct.
- **`branchBefore` / `prevRecord`.** Not arity-specific; both are needed at any arity for the
  `PutBranch(key, new, prev)` API.

## Claims that did not survive review

- **"The tree has 3.54G leaves."** That total sums seven independent range files, each a
  complete tree. The N-1 identity applies per file: range 0-8192 has 2,530,448,382 records,
  hence ~2.53G leaves.
- **"Storage-leaf prefixes are ~496 bits."** An asymptotic PATRICIA estimate. At 62 B/cell it
  would put ~124 B of prefix in every record, exceeding the measured 121.66 B/record total.
  The measured bound is 91-219 bits/cell depending on leaf shape.
- **"Hex inflates in sparse ranges because a 2-of-16 node still pays 16-way framing."**
  Predicts the wrong direction -- a 2-child hex node is small. The measured rise, 256.79 ->
  487.26 B/rec, comes from the record population shifting toward the dense near-root nodes
  that every scattered path traverses.
- **"seg compression will eat the dead bytes."** Partly. Measured 121.66 B/record on disk
  against ~122 B uncompressed for the dominant shape puts overall compression near 1.0x on
  this hash-heavy payload. It will still eat item 4's fixed-position bytes; it cannot eat item
  3's digest tails.

## BLAKE3 versus Keccak at these sizes

Every pbin preimage is 67-133 B: `branchHash` 67-133, account/code leaf 69, storage leaf 101.
All are far under BLAKE3's 1024 B chunk, so `Sum256` routes to a scalar `CompressChunk` and
the AVX2/AVX512 assembly in `lukechampine/blake3` is unreachable for any input pbin produces.

| preimage | arm64 M5 blake3 | arm64 keccak | amd64 EPYC blake3 | amd64 keccak |
|---|---:|---:|---:|---:|
| 69 B | 147.1 ns | 121.9 ns | 168.2 ns | 237.7 ns |
| 101 B | 147.4 ns | 123.2 ns | 172.1 ns | 240.8 ns |
| 133 B | 211.3 ns | 123.4 ns | 242.8 ns | 247.8 ns |

The winner flips by platform: keccak by 1.20-1.71x on arm64, BLAKE3 by 1.40x on the amd64
production box. What holds on both is the mechanism -- blake3 steps with block count crossing
2 to 3 compressions at 133 B, while keccak stays flat because its 136 B sponge rate absorbs
every pbin preimage in one permutation.

This is not a performance lever: 3.5G hashes at 25 ns of difference is 88 seconds against a
109-hour conversion, and the IO dominates by orders of magnitude. It is an EIP-8297 argument.
The spec states the hash choice is open and lists BLAKE3 as "good native performance,
reasonable in-circuit". The parallelism half of that case cannot engage at the sizes this tree
produces. The in-circuit half is untouched and remains the real reason to leave Keccak.

## Sequencing

The record format carries no version marker. A change to the *embedding* -- key derivation or
leaf layout -- changes roots and forces a rebuild from genesis. A change to the *record
encoding* changes no root and costs a commitment rebuild from existing state, the 109h13m
operation. Old and new records cannot coexist: today's `fields` byte values overlap any
natural new tag scheme numerically, so a stale file would be misread rather than rejected.

Therefore batch items 1 through 4 into one flag day, and spend one byte on a format version in
that same change so the next one is cheap.

## Open measurements

- Record-shape histogram over one bin `.kv`: the interior/leaf-bearing split and the prefix
  length distribution. Firms up items 1, 3 and 4, and settles the compression question.
- `pbinCounters.splitsInsidePrefix` / `materializeReads` for a real run. The counters exist
  and print via `traceW`; no trace from the mainnet conversion was kept.
- Peak `activeRows`. No counter exists anywhere.
- The sort-then-bulk-load converter arm, still unmeasured.
