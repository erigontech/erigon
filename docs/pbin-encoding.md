# How erigon encodes the EIP-8297 partitioned binary tree

All file references are relative to `execution/commitment/` and name an identifier rather than a
line number, which drifts on every edit. Every identifier belonging to this engine carries a `pbin`
prefix; the hex MPT engine lives in the same package and owns the unprefixed names.

The engine is `PBinPatriciaHashed` (`pbin_patricia_hashed.go`). It borrows the hex engine's
grid/unfold/fold skeleton and none of its node model: arity 2, no extension node, no storage root,
and a leaf commits its complete tree key (the type's doc comment).

Everything below was produced by running the engine. Hex is real.

---

## 1. Tree keys

A tree key is `zone(1) || treePosition || subIndex(1)`, assembled by `pbinTreeKey`
(`pbin_keys.go`). Three zones exist, each admitting exactly one key length
(`pbinZoneKeyLength`, `pbin_keys.go`):

| zone | name    | key length | treePosition                    |
|------|---------|-----------:|---------------------------------|
| 0x00 | account |         34 | `stem = H(addr32)`              |
| 0x01 | code    |         34 | `H(codeHash \|\| 0*24 \|\| u64BE(codeIndex))` |
| 0xFF | storage |         66 | `stem \|\| H(addr32 \|\| u256BE(slotIndex))` |

The two indexes are unrelated quantities, and neither preimage is a bare concatenation of
naturally-sized values — both are exactly 64 bytes, with the index widened to fill the tail:

- `codeIndex = chunkID / 256`, the chunk's code group, written as 8 big-endian bytes after 24 zero
  bytes (§10, `codeChunkKey`, `pbin_keys.go`). No address takes part: the code zone is
  content-addressed, so two accounts running the same bytecode share one set of leaves.
- `slotIndex = slot >> 8`, written as a 32-byte big-endian value, which is `0x00 || slot[0:31]`
  (§9, `groupDigest`, `pbin_keys.go`).

The trailing `subIndex` byte is `chunkID % 256` for code and `slot & 0xFF` for storage.

Zones `0x02..0xFE` have no length and `pbinTreeKey` panics on them. The fixed length per zone *is*
the prefix-free invariant, and it is re-asserted at hash time from the key's own first byte
(`leafCellHash`, `pbin_hash.go`) so a malformed key cannot reach the hasher.

`addr32` is the 20-byte address left-zero-padded to 32 (`pbinAddr32`, `pbin_keys.go`).
`H` is Keccak-256 by default, blake3 under `--experimental.bin-commitment.hash`
(`SetPBinHashSuite`, `pbin_hash.go`). Key derivation and node hashing both use `H`, and
`setHashSuite` (`pbin_patricia_hashed.go`) swaps both seams at once so neither can be configured
alone.

Two digests are memoized per `pbinDigestCache` (`pbin_keys.go`): the stem, keyed on
`addr32`, and the storage group hash, keyed on `(addr32, slot[0:31])`. The group entry is bound to
the address as well as the index, so an address change cannot yield a stale hit.

## 2. `pbinBitpath`

A path through the tree is up to 528 bits — the longest key, a 66-byte storage leaf
(`pbinMaxPathBits`, `pbin_bitpath.go`). It is held as nine big-endian words plus a bit count
(`pbinBitpath`, `pbin_bitpath.go`):

```
bit index  0                63 64             127  ...       512          527
           +------------------+------------------+  ...  +----------------+
           |       w[0]       |       w[1]       |       | w[8], 16 used  |
           +------------------+------------------+  ...  +----------------+
           MSB first          MSB first                  bits 512..527
```

Bit `d` lives in `w[d/64]` at shift `63-(d%64)` (`bit`, `setBitAt`); source byte `i` loads into
`w[i/8] << (56-8*(i%8))` (`pbinPathFromBits`). Word order therefore equals descent order and
divergence is XOR plus `LeadingZeros64` with no reversal — the reason for the layout
(`pbinBitpath`'s doc comment).

```
key  = 00b10e2d527612073b26eecdfd717e6a320cf44b4afac2b0732d9fcbe2b7fa0cf600  (34 B)
bitLen = 272
w[0] = 00b10e2d52761207   w[1] = 3b26eecdfd717e6a   w[2] = 320cf44b4afac2b0
w[3] = 732d9fcbe2b7fa0c   w[4] = f600000000000000   w[5..8] = 0
```

`bitLen` is the only authority on length. `bit` panics past it, and `appendPackedBits` emits
`ceil(bitLen/8)` bytes and re-masks the last.

**Masking invariant.** Every path this engine builds holds zero bits at and past `bitLen`.
`maskTail` enforces it, called from `pbinPathFromBits`, `truncate` and the canonicality check in
`pbinDecodeBitPath`; `slice`, `appendBit` and `append` preserve it by construction, writing only
bits below the new `bitLen`. `setBitAt` is the one mutator that *can* dirty the tail — it is
bounded by `pbinMaxPathBits`, not by `bitLen` — and every caller writes inside the path. The type's
own doc comment is weaker, allowing anything past `bitLen`: read it as what a reader may assume,
not as what the constructors produce. The invariant is not what makes the common-prefix scan safe
— that is the `limit` clamp in `pbinCommonPrefixBitsAt`, pinned against a deliberately dirty tail
at `TestPBinCommonPrefixBits_IgnoresBitsBeyondBitLen` (`pbin_bitpath_test.go`). What depends on it
is struct equality: paths and cells are compared with `==`, and `pbinDecodeBitPath` rejects a
non-canonical key by masking a copy and comparing words.

`pbinCommonPrefixBitsAt(key, from, prefix)` counts agreeing bits between `key` read
from bit `from` and `prefix` read from bit 0. The asymmetry exists because the descent compares a
whole tree key against a cell prefix that starts partway down:

```
        from=4
key  w[wi]   : b b b b|X X X X X X X X ...        << 4
     w[wi+1] :               h h h h| ...         >> 60, spliced in
prefix w[0]  : X X X X X X X X X X X X ...        word-aligned
               ^ XOR, LeadingZeros64 = agreeing bits in this word
```

## 3. Three prefix encodings

The same bit string is spelled three different ways depending on where it is written.

| where | count | layout | code |
|---|---|---|---|
| node preimage | `u16` big-endian, leading | `u16(bitLen) \|\| packed` | `pbinAppendBitPrefix`, `pbin_hash.go` |
| cell in a branch record | uvarint, leading | `uvarint(bitLen) \|\| packed` | `pbinAppendCell`, `pbin_branch.go` |
| domain key | one byte `bitLen mod 8`, **trailing** | `packed \|\| byte(bitLen%8)` | `pbinAppendBitPath`, `pbin_bitpath.go` |

```
encode_bit_prefix                domain key
  0 bits          0000             0 bits          00
  1 bit  '1'      000180           1 bit  0x80     8001
  3 bits '101'    0003a0           3 bits 0xE0     e003
  7 bits  all-1   0007fe           7 bits 0xB0     b007
  8 bits  0xAA    0008aa           8 bits 0xB1     b100     <- mod 8 == 0
  9 bits          0009aa80         9 bits 0xB180   b18001
528 bits all-1    0210 || ff*66
```

The preimage count is what keeps a 7-bit prefix from colliding with an 8-bit one that agrees with it
on the pad bit (`pbinAppendBitPrefix`'s doc comment).

The domain key puts its count *last* so a subtree stays contiguous in the keyspace: every descendant
of a `b`-bit path repeats its first `floor(b/8)` whole packed bytes and the leading `b mod 8` bits
of the next, so the whole subtree lands in one byte-range. A leading length field would sort by
depth first and scatter that range (`pbinAppendBitPath`'s doc comment). Contiguity is all the
layout buys — the order inside the range is **not** ancestors-before-descendants, and the comment
says so.
Counterexample, measured:

```
7 bits 1111111  -> fe07
8 bits 11111110 -> fe00        fe00 < fe07, yet the 7-bit path is a prefix of the 8-bit one
```

Nothing range-scans the domain today: every access is a point lookup by exact key
(`unfoldBranchNode`, `foldBranch`, `foldDelete`, `materializeBranch`). Contiguity is a property a
future scan could rely on, not one anything currently depends on.

`pbinDecodeBitPath` is total and canonical — one path, one key. It rejects an empty buffer, a tail
byte above 7, a non-zero tail with no payload, over 528 bits and set pad bits. Bijectivity is
fuzz-pinned by `FuzzPBinBitPathCodec` (`pbin_bitpath_test.go`).

## 4. Node preimages

Two shapes, distinguished by a leading tag byte (`pbinLeafTag` / `pbinBranchTag`, `pbin_hash.go`).

```
leaf    0x00 || tree key (34 or 66) || value (32)          leafCellHash
branch  0x01 || u16(bitLen) || packed prefix || left(32) || right(32)
                                                            branchHash
```

A leaf carries its **complete** tree key, not a suffix — `leafCellHash` concatenates the descent
path with the cell's own prefix and requires the result to be whole bytes (`pbin_hash.go`).
A branch's prefix here is **relative**: the bits between the parent's split and this node's own,
cut at fold time by `pph.currentKey.slice(upDepth, depth-1)` (`foldBranch`). The
domain key for that same node holds the *absolute* path. They coincide only at the top.

An absent child hashes as 32 zero bytes and is never omitted (`pbinEmptyTreeHash`, `branchHash`) —
`pbinEmptyTreeHash`, deliberately not `empty.RootHash`, which would build a different tree. So the
empty tree's root is 32 zero bytes, and a one-key tree's root is the leaf hash itself with no branch
wrapping it (`RootHash`).

Real leaf preimage and its hash, for the account of §5.1 (code_size 6, balance 0x3e8):

```
00 0012b9c2d7398802bddf3d70e0e8cf9074f4819101be174b883975a79061d53e7a 00
   0000000000000006 0000000000000003 000000000000000000000000000003e8
-> 5dbe9906fc51df4ac846a8fe44ed92a3ec310d2edaeeea605289a290f6b38eba
```

Real branch preimage, 5-bit prefix, both children being the leaves above:

```
01 0005 00
   5dbe9906fc51df4ac846a8fe44ed92a3ec310d2edaeeea605289a290f6b38eba
   970021c05f854ea9f1b9dd97d180ae62d0d2b9bb4acc23869cc5879919434ef8
-> de50844a66c2a773d715492d679fee88416467c0c5a7802a6b033199b357b0a4
```

`de5084…` is the byte string stored as cell 0's hash in the 265-bit record dumped in §5.

## 5. The branch record

### 5.1 The example tree

One account (address `0102…14`, nonce 3, balance 1000, code `60aabb000102`), storage slot 5 holding
5, storage slot 300 holding 0x2c. Four branch records plus the root record.
`stem = 12b9c2d7…61d53e7a`, `codeHash = 1d6423ed…7696574d`.

Those values are the corpus, not something the records carry: a record names a leaf's identity only
(§6), so every hash below and in §12 needs them supplied from outside. The tree's five leaves in
full: BASIC_DATA packing nonce 3 / balance 1000 / code_size 6; CODE_HASH =
`keccak256(60aabb000102) = 1d6423ed…7696574d`; code chunk 0 in the code zone, that code padded to
31 bytes; slot 5 = `0000…0005`; slot 300 = `0000…002c` (44 — the value, not the slot number).

The account holds no DELEGATION leaf: its code is contract code, so it takes the CODE_HASH branch
of the exclusive pair (§8).

Each record below is named by its domain key, with the parent cell it hangs off:

```
  key 08                              root cell: branch, 0-bit prefix, hash = state root
                                      its node is the record at key 00

  key 00                  [  0 bits]  splits on bit 0 (the zone byte's top bit)
    |-- bit0  branch,   6-bit prefix ---------------------> node at 7 bits = key 0007
    `-- bit1  leaf,   527-bit prefix, storageAddr --------> slot 300  (528-bit key, zone 0xFF)

  key 0007                [  7 bits]  splits on bit 7, the zone byte's last:
                                      account zone 0x00 against code zone 0x01
                                      child of key 00, cell bit 0
    |-- bit0  branch, 257-bit prefix ---------------------> node at 265 bits = key 0012b9…7a0001
    `-- bit1  leaf,   264-bit prefix, leafValue ----------> code chunk 0 (zone 0x01, 272-bit key)

  key 0012b9…7a0001       [265 bits]  = 0x00 || stem || sub-index bit 264, which is 0 for every
                                      allocated sub-index; splits on sub-index bit 265
                                      child of key 0007, cell bit 0
    |-- bit0  branch, 5-bit prefix -----------------------> node at 271 bits = key 0012b9…7a0007
    `-- bit1  leaf,   6-bit prefix 000101, storageAddr ---> slot 5 (sub 0x45 = 64+5)

  key 0012b9…7a0007       [271 bits]  splits on the sub-index's last bit
                                      child of key 0012b9…7a0001, cell bit 0
    |-- bit0  leaf, 0-bit prefix, accountAddr ------------> BASIC_DATA (sub 0x00)
    `-- bit1  leaf, 0-bit prefix, accountAddr ------------> CODE_HASH  (sub 0x01)
```

Every chain descends through cell bit 0; the bit-1 cells are all leaves.

The chunk leaf hanging off the zone byte rather than off the account's stem is what content
addressing looks like in the tree: the account's three header keys and its code share nothing below
bit 7.

Depth arithmetic closes at every step: `record bits + 1 branch bit + cell prefix bits = child's
absolute depth`. `0+1+6 = 7`, `7+1+257 = 265`, `265+1+5 = 271`, `271+1+0 = 272` (the 34-byte account
key), `7+1+264 = 272` (the 34-byte code key), `0+1+527 = 528` (the 66-byte storage key).

### 5.2 Layout

```
+=====================+   written by encode,
| touchMap   u16 BE   |   read by pbinDecodeBranch
| afterMap   u16 BE   |
+=====================+
| cell body for bit 0 |   present iff afterMap & 1
| cell body for bit 1 |   present iff afterMap & 2
+=====================+
```

Cells are emitted in ascending bit order (`bitset & -bitset` / `TrailingZeros16`,
`encode`, `pbin_branch.go`); the decoder mirrors it exactly (`pbinDecodeBranch`).

```
cell body                                      pbinAppendCell / pbinDecodeCell
  fields    1 byte     bitmask, below
  bitLen    uvarint    prefix length in BITS, 0..528
  prefix    ceil(bitLen/8) bytes, MSB-first, pad bits zero
  [accAddr] uvarint(20)=0x14 || 20 bytes
  [stoAddr] uvarint(52)=0x34 || 52 bytes
  [value]   uvarint(32)=0x20 || 32 bytes
  [hash]    uvarint(32)=0x20 || 32 bytes
```

`fields` (`pbinCellFields`, `pbin_branch.go`): bit0 LEAF, bit1 BRANCH, bit2 ACCOUNT_ADDR,
bit3 STORAGE_ADDR, bit4 HASH, bit5 LEAF_VALUE. The optional blocks appear in one fixed order in
both encoder and decoder — accAddr, stoAddr, LEAF_VALUE, HASH (`pbinAppendCell` /
`pbinDecodeCell`) — and that is
**not** the bit order: LEAF_VALUE is bit 5 and HASH is bit 4, so LEAF_VALUE is written first. The
fields byte says which blocks are present, not what order to read them in; a decoder that walks it
LSB-to-MSB takes HASH before LEAF_VALUE and desynchronises the cursor on any cell carrying both
(the §5.5 format-ceiling row). The length prefixes are uvarints but `pbinDecodeFixedVal`
demands the one exact width per field, making `0x14` / `0x34` / `0x20` the only legal
tag bytes.

The cell prefix is relative to the record's own key plus the branch bit: the record's key is
`pbinAppendBitPath(currentKey)`, the child sits at `keyBits+1`, and `prefix` carries the remainder
down to the child node.

### 5.3 A real record, byte by byte

The 265-bit record from §5.1 — a branch child and a header-storage leaf.

```
key 0012b9c2d7398802bddf3d70e0e8cf9074f4819101be174b883975a79061d53e7a0001

00000000  00 03 00 03 12 05 00 20  de 50 84 4a 66 c2 a7 73  |....... .P.Jf..s|
00000010  d7 15 49 2d 67 9f ee 88  41 64 67 c0 c5 a7 80 2a  |..I-g...Adg....*|
00000020  6b 03 31 99 b3 57 b0 a4  09 06 14 34 01 02 03 04  |k.1..W.....4....|
00000030  05 06 07 08 09 0a 0b 0c  0d 0e 0f 10 11 12 13 14  |................|
00000040  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 00  |................|
00000050  00 00 00 00 00 00 00 00  00 00 00 00 00 00 00 05  |................|

[00..01]  0003      touchMap = 0b11
[02..03]  0003      afterMap = 0b11
cell bit 0
[04]      12        fields  = 00010010  BRANCH | HASH
[05]      05        bitLen  = uvarint 5
[06]      00        prefix  = 00000 + 3 zero pad bits
[07..27]  20 ||     hash    = de50844a66c2a773d715492d679fee88416467c0c5a7802a6b033199b357b0a4
cell bit 1
[28]      09        fields  = 00001001  LEAF | STORAGE_ADDR
[29]      06        bitLen  = uvarint 6
[2a]      14        prefix  = 000101 + 2 zero pad bits
[2b..5f]  34 ||     stoAddr = 0102030405060708090a0b0c0d0e0f1011121314
                              0000…0005                (addr || slot, 52 bytes)
```

Sub-index reconstruction for cell 1: the record sits at 265 bits, so the sub-index's top bit is
already fixed to `0` by the prefix above it and this record's branch bit supplies the next, `1`.
The cell prefix then supplies `000101`. Full sub-index `0b01000101 = 0x45 = 64 + 5` — storage slot 5
in the account header (§9).

The other three records of the same tree:

```
key 00  [0 bits]  162 bytes
  0003 0003
  12 06 00 20 c9aca54ec7a6c2fe06fc1cee22bd609b559f1c16217ce2ea48793349b8d61be5
  09 8f04 fe257385ae7310057bbe7ae1c1d19f20e9e90322037c2e971072eb4f20c3aa7cf4
          3211d8496e2c633f71a67a015a0551623e46676cc65d3acc04301137a5fc5a8458
     34 0102030405060708090a0b0c0d0e0f1011121314
        000000000000000000000000000000000000000000000000000000000000012c

key 0007  [7 bits]  142 bytes
  0003 0003
  12 8102 12b9c2d7398802bddf3d70e0e8cf9074f4819101be174b883975a79061d53e7a00
     20 ac8c75fc4b6f6e25d0831229dd10d3ad353c56dc573141f6f7e62707a5076b5d
  21 8802 073be86901ad75392dc6c8cd03071cf8e0c17da59c33a1911c7b85c09f969b5a00
     20 0060aabb00010200000000000000000000000000000000000000000000000000

key 0012b9…7a0007  [271 bits]  50 bytes
  0003 0003
  05 00 14 0102030405060708090a0b0c0d0e0f1011121314
  05 00 14 0102030405060708090a0b0c0d0e0f1011121314
```

The 7-bit record shows the two zones side by side and needs no shift to read: seven bits are
consumed above it and one more by its own branch, so both cell prefixes start on a byte boundary —
cell 0 carries `stem || 0x00` (the account key from byte 1 on), cell 1 the chunk key's own 33 bytes.
The top record is where the shift shows: the storage key starts `ff 12 b9…` and its cell prefix
(bits 1..527) starts `fe 25 73…`, since shifting left by one turns `ff 12 b9` into `fe 25 73`
(`0xff<<1 | 0x12>>7 = 0xfe`).

The 271-bit record is the account pair: two leaf cells, zero-bit prefixes, both naming the *same*
20-byte plain key. Which leaf each is is decided by the last bit of the reconstructed tree key and
resolved at hash time by `pbinLeafValue` (`pbin_hash.go`), not by anything in the record.

### 5.4 touchMap and afterMap

Both are `uint16` at offsets 0 and 2 (`encode` and `pbinDecodeBranch`, `pbin_branch.go`) purely so
the `OnesCount16` / `TrailingZeros16` arithmetic ports from the hex engine unchanged (`pbinGrid`'s
doc comment, `pbin_cell.go`). Only bits 0 and 1 may be set; `pbinCheckCellMaps` rejects anything
outside `pbinCellBits = 0b11` on both encode and decode.

`afterMap` is structural — it says which cell bodies follow. `touchMap` is write-time bookkeeping
only. The reader throws it away (`_, afterMap, err := pbinDecodeBranch`,
in `unfoldBranchNode`; the only other call site is `materializeBranch`), and
nothing downstream parses the record either: `TrieContext.PutBranch` hands the bytes straight to
`DomainPut` (`commitmentdb/commitment_context.go`). There is no `BranchData` merge.

On disk, `afterMap` of a branch record is always `0b11`: `foldBranch` refuses a row that does not
keep exactly two cells (`foldBranch`). A row collapsing to one survivor writes
no record at all — the node moves up and the consumed bits are prepended to the survivor's prefix
(`foldPropagate`); a row keeping nothing writes a zero-length value, which is the deletion
encoding (`foldDelete`). `touchMap` does vary: bits are set at update time (`updateCell`) and
carried upward by `propagateTouch`.

Both non-branch outcomes are reachable:

- **One survivor** is routine, and has nothing to do with removal. An unfold that descends into a
  cell seeds the new row with that one cell (`unfold`), so a row that no later update splits folds
  straight back through `foldPropagate` — the exact inverse of the unfold that opened it.
- **No survivor** needs a parent cell that was touched and is now absent, which `unfoldBranchNode`
  loads as `after = 0` through its `deleted` flag. A write of 32 zero bytes is a
  deletion (§11), so zeroing a subtree's last leaf reaches it; pinned at
  `TestPBinFoldDeleteRunsOnProcess` (`pbin_zerovalue_test.go`).

A reader still needs an answer for a zero-length value, and it differs by key: at a bit-path key
`unfoldBranchNode` rejects it as a missing branch, so it is not a shape a decoder has to parse; at
the root key `0x08` it is legal and means the empty tree (`loadRoot`).

That every record carries both children is what removes the merge path
(`pbinBranchEncoder`'s doc comment, `pbin_branch.go`): at arity 2 the untouched sibling is the
whole other half of the subtree, so a record read back replaces its predecessor outright.

### 5.5 Size

Per cell: `1 (fields) + 1..2 (bitLen uvarint) + 0..66 (packed prefix) + one value block`. Value
blocks are 21 (account), 53 (storage), 33 (verbatim value), 33 (hash).

| shape | bytes | reachable |
|---|---:|---|
| `afterMap = 0` | 4 | decodes; `foldBranch` never writes it |
| one bare BRANCH cell `000100010200` | 6 | same |
| two bare BRANCH cells `0003000302000200` | 8 | same |
| two hashed branch cells | 74 | yes |
| **writer floor** — two 0-prefix account leaves | **50** | yes, once in §5.1 (the 271-bit record) |
| **writer ceiling** — two 527-bit-prefix storage leaves | **248** | only at a depth-0 record |
| **format ceiling** — the same plus a HASH block on each | **314** | decodes; writer never emits it |

All seven rows encode-and-decode round-trip. Measured record sizes for the §5.1 corpus: 162, 142,
96, 50, plus a 35-byte root record. The root record is framed differently and sized in §7.

Size is driven, in order of weight, by: the two prefix bit lengths (up to 66 bytes each — all the
variance lives here, and it is inverse to depth); which value each child names (53 > 33 > 21); and
the 1-vs-2-byte `bitLen` uvarint at the 128-bit boundary.

### 5.6 Decoding

`pbinDecodeBranch(data, cells *[2]pbinCell)` (`pbin_branch.go`) resets both cells
unconditionally, requires ≥4 bytes, reads the maps, re-checks them, then walks
`afterMap` in ascending bit order filling `cells[TrailingZeros16(bit)]`. A cell whose bit is clear
stays zeroed — that is how an absent child is spelled. Any leftover byte is an error.

Each body restores kind from the LEAF/BRANCH bits; the prefix from the explicit bit count, never
from the byte length, with pad bits asserted zero (`pbinDecodePrefix`); `accountAddrLen` /
`storageAddrLen` / `hashLen` as side effects of their fields being present; and a LEAF_VALUE as
`Update{Flags: StorageUpdate, StorageLen: 32}`.

Rejections, each observed firing:

```
pbinDecodeCell      unknown field bits; neither or both node kinds; a leaf naming
                    0 or 2+ value sources; a branch carrying a leaf value
pbinDecodePrefix    a prefix over 528 bits; non-zero pad bits
pbinDecodeFixedVal  a wrong length tag
pbinDecodeBranch    trailing bytes

  leaf with both addrs -> malformed branch record: leaf cell fields 00001101 name no single value source
  kind = leaf|branch   -> malformed branch record: cell fields 00000111 name no single node kind
  dirty pad bits       -> malformed branch record: non-zero pad bits after a 3-bit prefix
  trailing byte        -> malformed branch record: 1 trailing bytes
```

One asymmetry against the "one canonical form" claim in `pbinDecodeBranch`'s doc comment: a BRANCH
cell carrying ACCOUNT_ADDR or STORAGE_ADDR decodes cleanly (only LEAF_VALUE is refused for
branches). The writer cannot produce it — `foldBranch` resets the upCell before setting kind — so
it is an unreachable spelling the decoder still accepts,
not a live bug.

Caller side: `unfoldBranchNode` keeps `afterMap` and discards the record's `touchMap`, setting
`touch=0, after=afterMap` normally, or `touch=afterMap, after=0` when the parent cell was touched
and is now gone, which is how a whole subtree is dropped (`unfoldBranchNode`).

## 6. Leaf cells in a record

Yes — a leaf child is stored as a full cell body, not as a hash. What it carries is its *identity*,
never its state value, from exactly one of three sources (the decoder enforces exactly one,
`pbinDecodeCell`, `pbin_branch.go`). This section describes records **written by the fold**; the
witness context spells the same three fields differently, below.

- **ACCOUNT_ADDR** — the 20-byte plain key, set from an update with `len(plainKey)==20`
  (`updateCell`, `pbin_patricia_hashed.go`).
- **STORAGE_ADDR** — `addr||slot`, `len(plainKey)==52`.
- **LEAF_VALUE** — 32 raw bytes, and only when the leaf has no plain key at all: the encoder sets it
  iff no address field is present (`pbinAppendCell`). That is the code chunk, the
  EIP-7702 delegation indicator and any reserved sub-index — every leaf whose value no state domain
  holds as a field (`pbinFieldLeafValue`, `pbin_branch.go`; `pbinRecordLeafValue`, `pbin_code.go`).

A leaf's own hash is **not** in the record. `hashRowCell` writes a computed hash back only for
branch cells (`pbin_patricia_hashed.go`), and no other site sets `hashLen` on a leaf, so the
encoder's HASH field is never emitted for one in practice. The consequence is that rehashing a
decoded record's leaf child requires state-domain reads — `loadCellState`
(`pbin_patricia_hashed.go`) fetches the account or slot behind the plain key. Only branch
children are hash-only.

Balance, nonce, code hash and storage value are absent for address-bearing leaves: the plain key is
the pointer back into the state domains.

**Witness-produced records read differently.** A witness has no state domains behind it, so
`fillLeafCell` (`pbin_witness_context.go`) picks the field by re-encoding, not by zone: any
leaf whose 32 bytes round-trip through `pbinLeafValue` verbatim — storage slots, header slots, code
chunks — is written as LEAF_VALUE, and the rest — BASIC_DATA and CODE_HASH, which are
packed from account fields — go into ACCOUNT_ADDR as a synthetic 20-byte *handle*, the first 20
bytes of the node hash, which the context resolves back to the account state. So on a
witness record ACCOUNT_ADDR is not an address and LEAF_VALUE is not evidence of a code chunk.
Consumers must know which producer wrote the record.

## 7. The root record

`pbinRootKey = {0x08}` (`pbin_patricia_hashed.go`) holds a **bare cell body with no 4-byte
header**: `storeRoot` calls `pbinAppendCell` directly and `loadRoot` calls `pbinDecodeCell` at
position 0, rejecting trailing bytes. A zero-length value at that key is the deletion encoding for
an emptied tree (`storeRoot`).

```
key 08, 35 bytes — the §5.1 tree:
  12 00 20 658b62aba5ac2933e86f1100cce5084bdb35b32d797b05d247abf27a2018c064
  ^ BRANCH|HASH
     ^ bitLen 0
        ^ len 32 || the state root
```

That is the common shape, not the only one. `storeRoot` serialises whatever the root cell is,
and a one-key tree's root is the leaf itself with no branch wrapping it (§4), so the
record can equally be a LEAF cell — with a full-length prefix, since no descent sits above it to
consume any of the key. Measured, the §5.1 address holding slot 300 and nothing else:

```
key 08, 122 bytes:
  09 9004 ff12b9…d2fe2d42 2c 34 0102…1314 0000…012c
  ^ LEAF|STORAGE_ADDR
     ^ uvarint 528 bits
          ^ the whole 66-byte tree key, packed
                            ^ len 52 || addr || slot
```

Sizes follow the §5.5 per-cell arithmetic with no 4-byte header: `1 (fields) + 1..2 (bitLen uvarint)
+ 0..66 (packed prefix) + one value block of 21 / 33 / 53`. That is 35 bytes for the branch-and-hash
spelling above, 58 / 70 / 90 for a 272-bit leaf root naming an address, a verbatim value or an
`addr||slot`, and 122 at most — the 528-bit storage leaf shown. A decoder that assumes BRANCH|HASH
fails on every one-key tree.

The root cell needs a key of its own, and not because nothing names it — the empty path does. The
problem is that the empty path already encodes to the 1-byte key `00`, which is the record of the
top branch node (see §5.1, where `00` and `08` are two different records of the same tree). Every
other node is found by the path that reaches it, so only the root is left needing a key nothing else
claims (`pbinRootKey`'s doc comment). The zero-length key is no alternative either: domain
iteration reads it as end-of-stream and it sorts first, truncating the table, so the datadir would
read back as fresh — pinned against the real `TblCommitmentVals` by
`TestPBinRootRecordRealTableIteration` (`pbin_rootkey_test.go`).

`0x08` works because the trailing byte of every path key is `bitLen mod 8`, so its range is exactly
`0..7` and `pbinDecodeBitPath` rejects anything above (`pbinDecodeBitPath`). `0x08` is the
smallest byte that can never be a trailing bit count, so a 1-byte key of `0x08` cannot be any
encoded path — checked exhaustively over every `bitLen` 0..528 by
`TestPBinRootKeySentinelNotABitPath`. The same bound keeps `KeyCommitmentState` (`"state"`, tail
`0x65`) out of the path image (`TestPBinBitPathNeverEncodesToStateKey`, `pbin_bitpath_test.go`):

```
pbinDecodeBitPath(08)         -> pbin: invalid trailing bit count 8 in bit-path key
pbinDecodeBitPath(7374617465) -> pbin: invalid trailing bit count 101 in bit-path key
```

The witness-side `PatriciaContext` uses the same two record framings — a bare cell for the root
(`rootRecord`, `pbin_witness_context.go`), a full header plus two cells for a branch, with
`touchMap` set equal to `afterMap` because a read discards it anyway (`branchRecord`). The framing
is shared; what goes into a leaf cell is not — see the witness paragraph in §6.

## 8. The account header stem

Zone `0x00`, 34 bytes, `treePosition = stem = H(addr32)`. The trailing byte is the sub-index, and it
partitions a 256-wide subtree under one stem:

```
byte:  0        1 ............................ 32       33
      +----+------------------------------------+------+
      | 00 |          stem = H(addr32)          | sub  |
      +----+------------------------------------+------+

sub    0        BASIC_DATA    packed from account state
       1        CODE_HASH     32 raw bytes
       2        DELEGATION    the 23-byte indicator, right-padded with nine zeros
       3 ..  63 reserved      not packed; leaf carries 32 verbatim bytes
                              (pbinLeafValue -> pbinRecordLeafValue)
      64 .. 127 storage slots 0..63, value left-padded
     128 .. 255 unallocated   no key this embedding derives lands here
```

Constants in `pbin_keys.go`; the dispatch that turns a sub-index into a leaf value is
`pbinLeafValue` (`pbin_hash.go`).

Sub-indices 128..255 held the first 128 code chunks before every chunk moved into the code zone
(§10). They are now reserved like 3..63, and the dispatch treats both ranges the same: a leaf there
carries its 32 bytes verbatim rather than being packed from state, which is the right answer for a
sub-index whose meaning is not yet defined.

BASIC_DATA packing (`pbinEncodeBasicData` and its offset constants, `pbin_values.go`),
big-endian throughout:

```
off:  0    1   2   3    4          8                16                      32
     +----+-----------+-----------+----------------+-----------------------+
     |ver | reserved  | code_size |     nonce      |    balance (128 bit)  |
     | 0  |  0  0  0  |    u32    |      u64       |        16 bytes       |
     +----+-----------+-----------+----------------+-----------------------+
```

Bytes 0..3 are never written; the zero value of the array supplies them. A balance over 128 bits or
a code size over 2^32-1 is an error, not a truncation — a silent truncation would commit a wrong
root (`pbinEncodeBasicData`).

The CODE_HASH leaf is the raw 32-byte hash, with the zero hash mapped to `keccak256("")` for a
codeless account (`pbinCodeHashValue`). The DELEGATION leaf holds an EIP-7702 indicator — the 23
bytes `0xef0100 || target` — right-padded with nine zeros (`pbinEncodeDelegation`,
`pbin_values.go`). That is *not* the chunk encoding of §10: an indicator never executes, so
byte 0 carries code rather than a PUSHDATA count.

**An existing account holds exactly one of the two**, decided by its code bytes alone
(`pbinIsDelegation`, `pbin_values.go`) and never by its hash — a contract whose *hash* opens
`0xef0100` is still contract code. So a write emits one of the pair and deletes the other
unconditionally, since the stream is told nothing about what the account held a moment ago
(`emitCodeLeaves`, `pbin_update_stream.go`). A delegated account holds no code-zone chunks
at all: its leaf *is* its code, a read takes the leading `code_size` bytes and `EXTCODEHASH` hashes
them. Clearing a delegation restores a CODE_HASH leaf of `keccak256("")` with `code_size` zeroed.

Neither sibling has a key derivation of its own: `treeKey` (`pbin_keys.go`) only ever
derives BASIC_DATA for an address, and the stream produces the sibling by overwriting the last key
byte inside the same visit (`emitSibling`, `pbin_update_stream.go`).

Because the delegation leaf also marks an account present, a reader asking whether an account
exists must accept **either** sibling. BASIC_DATA is not that marker: an account with zero nonce,
zero balance and no code stores none (`PBinWitnessState.Account`, `pbin_witness_state.go`).

```
addr    = 0102030405060708090a0b0c0d0e0f1011121314
addr32  = 0000000000000000000000000102030405060708090a0b0c0d0e0f1011121314
stem    = 12b9c2d7398802bddf3d70e0e8cf9074f4819101be174b883975a79061d53e7a

BASIC_DATA key   00 12b9…3e7a 00
CODE_HASH  key   00 12b9…3e7a 01
DELEGATION key   00 12b9…3e7a 02

BASIC_DATA value, nonce=3 balance=1e18 code_size=100:
  00000000 00000064 0000000000000003 00000000000000000de0b6b3a7640000
  ^ver+rsv ^size    ^nonce           ^balance
CODE_HASH value — a *separate* example, for a codeless account (code_size 0), where the zero
hash maps to keccak256(""):
  c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470
DELEGATION value — a third account, delegating to 00…aa, so code_size is 23:
  ef0100 00000000000000000000000000000000000000aa 000000000000000000
  ^marker ^target (20 B)                          ^nine zero bytes
```

The three value lines are three different accounts. Pairing the first two would describe an account
running 100 code bytes whose code hashes empty, which no state can produce and the witness rejects
outright — `codeFromLeaves` re-checks the reassembled code against CODE_HASH
(`pbin_witness_state.go`). For one consistent account, see §5.1: code_size 6, CODE_HASH
`1d6423ed…7696574d`.

## 9. The storage sub-trie

`pbinSlotInHeader` (`pbin_keys.go`) decides: slot bytes `[0:31]` all zero **and**
`slot[31] < HEADER_STORAGE_SLOTS = 64`. So slots 0..63 only. The spec's invariant is
`HEADER_STORAGE_OFFSET + HEADER_STORAGE_SLOTS <= STEM_SUBTREE_WIDTH`, which pins the header slots
to sub-indices 64..127.

Header slots take an account-zone key at sub-index `64 + slot` (`storageKey`, `pbin_keys.go`) — same
34-byte shape, same stem, no extra hash. Everything else goes to zone `0xFF`, 66 bytes
(`storageKey`):

```
byte:  0        1 .................. 32   33 ....................... 64    65
      +----+-----------------------------+----------------------------+------+
      | FF |      stem = H(addr32)       | group = H(addr32||treeIdx) | sub  |
      +----+-----------------------------+----------------------------+------+

treeIdx = slot >> 8, as a 32-byte big-endian value = 0x00 || slot[0:31]
sub     = slot & 0xFF = slot[31]

a group = the 256 consecutive slots sharing one treeIdx:

  slot 300  ff | 12b9…3e7a | 1908ec24…d2fe2d42 | 2c   \
  slot 301  ff | 12b9…3e7a | 1908ec24…d2fe2d42 | 2d    |  identical 65-byte prefix
     …                                                 |  -> one dense subtree
  slot 319  ff | 12b9…3e7a | 1908ec24…d2fe2d42 | 3f   /
```

The group preimage is built by `groupDigest` (`pbin_keys.go`) as
`addr32 || 0x00 || slot[0:31]`, 64 bytes. Co-location of a group in one subtree is the point of the
layout, and the digest is memoized per group.

The key carries **both** digests, and the stem digest is the same one the account leaves use, so a
storage key costs one extra hash over an account key, not two.

Sharpest discontinuity in the embedding — slot 63 is a 34-byte key inside the account's own header,
slot 64 is a 66-byte key in a different zone (pinned by `TestPBinStorageLayoutCost`,
`pbin_storage_layout_test.go`):

```
slot   5  (34)  00 12b9…3e7a 45
slot  63  (34)  00 12b9…3e7a 7f
slot  64  (66)  ff 12b9…3e7a 1fef389e506c6134e0d9befd0702f549c08b2aeeba1bdf45776999fc988076f4 40
slot 300  (66)  ff 12b9…3e7a 1908ec24b716319fb8d33d00ad02a8b11f2333b6632e9d660218089bd2fe2d42 2c

group preimage for slot 300:
  0000…0102030405060708090a0b0c0d0e0f1011121314 00 00000000000000000000000000000000000000000000000000000000000001
  -> 1908ec24b716319fb8d33d00ad02a8b11f2333b6632e9d660218089bd2fe2d42
```

## 10. The code sub-trie

A chunk value is 32 bytes: byte 0 is metadata, bytes 1..31 are code
(`pbinChunkDataLen = 31`, `pbin_code.go`).

```
+----+---------------------------------+
| n  |       31 bytes of bytecode      |
+----+---------------------------------+
  ^ leading bytes of this chunk that are PUSHDATA, clamped to 31
```

The code is zero-padded to a multiple of 31 **before** the PUSHDATA scan
(`pbinChunkifyCode`, `pbin_code.go`). That ordering buys two things: the last chunk is always a
full 32 bytes with a zero tail, and a PUSH whose data runs off the end of the real code counts
against the padded tail instead of being dropped. `pushdataAt[i]` is how many bytes from `i` on are
still PUSHDATA; the table is allocated a whole chunk past the padded code so a PUSH32 on the last
byte has room, and `chunk[0] = min(pushdataAt[pos], 31)`. A PUSH is any opcode in `[0x60, 0x7f]`
(`pbinPush1` .. `pbinPush32`). The scan runs over the whole code, so residual PUSHDATA carries
across chunk boundaries — which is exactly what the byte reports.

```
code (40 bytes), PUSH2 at offset 0 and PUSH32 at offset 30 so its data crosses the boundary:
  61aabb 000000000000000000000000000000000000000000000000000000 7f f0f1f2f3f4f5f6f7f8
         ^ 27 zero bytes, offsets 3..29

chunk 0 = 00 61aabb0000000000000000000000000000000000000000000000000000007f
          ^^ offset 0 is an opcode
chunk 1 = 1f f0f1f2f3f4f5f6f7f800000000000000000000000000000000000000000000
          ^^ 31: every byte of this chunk is PUSH32 data, clamped from 32;
             the tail is padding added before the scan

short code 000102 -> one chunk:  00 000102 0000…00
empty code       -> zero chunks (pbinChunkifyCode)
```

**Every** chunk lives in the code zone; the account header holds none. One deriver takes a code hash
and a chunk id (`codeChunkKey`, `pbin_keys.go`):

```
treeIndex       = chunkID / 256          the chunk's code group
preimage (64 B) = codeHash(32) || 24 zero bytes || u64BE(treeIndex)
key      (34 B) = 0x01 || H(preimage) || byte(chunkID % 256)
```

An aligned run of 256 chunks sharing one `treeIndex` is a code group: its chunks share a stem and
differ only in the sub-index byte, so a contract of at most 256 chunks (7936 bytes) occupies one
dense subtree and the group edge is the only boundary in the layout.

The derivation names no address — only the code hash. Two accounts running the same bytecode derive
identical keys and share one set of leaves, whatever the code's size
(`pbinTreeKeyCodeChunk`, `pbin_keys.go`). The dedup is realised at emit time: chunks are
buffered, sorted by key, and duplicate keys collapse to one emission, with an error if two carry
different values (`flushCodeChunks`, `pbin_update_stream.go`). The chunk digest is
deliberately not memoized: the digest cache's entries are bound to an address these keys do not
have (`codeChunkKey`'s doc comment).

```
codeHash = 7b1e263ffcf71ebd01a2edd752b53eb24ed6abf042e8678a4a1db8d05d5d31b0
  chunk   0 (group 0) 01 1b05bf4b082e83c2b306efdbfdd460ba5193adeebcec3ca8453a5cff437d3f4d 00
  chunk 255 (group 0) 01 1b05bf4b082e83c2b306efdbfdd460ba5193adeebcec3ca8453a5cff437d3f4d ff
  chunk 256 (group 1) 01 2aeb430d323776088db507c7efbad5c4797d0f748b8b8a0112153cb665a413f5 00
  chunk 512 (group 2) 01 aa179620390ea03ed4cd924bbb94938f8162bf6741205ed18fbd5a34e449b9c0 00
                         ^ each group is a fresh stem; no address in any of them
```

A chunk of 32 zero bytes is stored as no leaf at all, like any other zero value (§11). That takes
31 zero code bytes **and** a zero PUSHDATA count in byte 0 — zero bytes continuing PUSHDATA from an
earlier chunk do not qualify, since byte 0 then records the continuation. Chunk presence therefore
does not delimit the code: `code_size` does, and an absent chunk reads back as the zeros it stands
for (`codeFromLeaves`, `pbin_witness_state.go`).

Chunk leaves carry no plain key: no state domain holds a code chunk, since chunking is a property of
the tree rather than of the account. They are emitted with a nil plain key
(`flushCodeChunks`), validated in `updateCell` (`pbin_patricia_hashed.go`), and stored under
`pbinFieldLeafValue` (§6). Emission ordering keeps the trie walk monotone: chunks are queued as
accounts are visited (`queueChunks`, `pbin_update_stream.go`) and flushed once a key past
the code zone appears (`flushCodeChunksBefore`).

A delegated account queues nothing: its indicator lives in the header and it owns no chunk leaves
at all (§8).

Read-back, for a stateless verifier: concatenate `value[1:]` of chunks `0..ceil(size/31)-1`,
truncate to `code_size`, verify against the CODE_HASH leaf (`pbin_witness_state.go`).

## 11. There is no storage root

Nothing computes one. The engine doc comment says so (`PBinPatriciaHashed`) and the
witness account type says so (`PBinAccount`) — those two comments are all
`grep -i "storage root" pbin_*.go` finds. The absence of code is a different grep, over the
identifier: `grep -n "storageRoot\|StorageRoot" pbin_*.go` returns nothing at all — no producer, no
consumer. `PBinAccount` is exactly Nonce, Balance, CodeSize, CodeHash
(`pbin_witness_state.go`). BASIC_DATA has no room for one either:
`1 + 3 + 4 + 8 + 16 = 32`, fully accounted (`pbin_values.go`).

What replaces it is a single flat global trie. Account fields, storage slots and code chunks are all
ordinary leaves of *one* binary trie, each addressed by its own 34- or 66-byte tree key. There is no
nesting, so there is no second trie to have a root. `pbinLeafValue` (`pbin_hash.go`)
enumerates every value a leaf may hold — BASIC_DATA, CODE_HASH, a padded storage word, a verbatim
32-byte record value — and none of them is a subtree hash.

An account and its storage are related only by sharing a key **prefix**: bytes 1..32 of the
account-zone key and bytes 1..32 of the storage-zone key are the same `H(addr32)`
(`accountHeaderStem` vs `storageKey`, `pbin_keys.go`). Prefix, not containment. The account's code
shares not even that: it is keyed by code hash and sits in a third zone.

```
account BASIC_DATA : 00 |12b9c2d7…61d53e7a| 00
account CODE_HASH  : 00 |12b9c2d7…61d53e7a| 01
storage slot 5     : 00 |12b9c2d7…61d53e7a| 45
storage slot 300   : ff |12b9c2d7…61d53e7a| 1908ec24…d2fe2d42 2c
                        ^^^^^^^^^^^^^^^^^^ same stem, different zone
code chunk 0       : 01 |073be869…9f969b5a| 00
                        ^^^^^^^^^^^^^^^^^^ H(codeHash || 0), no stem at all
```

Compare the hex engine in the same package, where the MPT structure is explicit:
`accountForHashing(buffer, storageRootHash)` writes the 32-byte root into the account RLP
(`hex_patricia_hashed.go`), called from `computeCellHash` and
`witnessComputeCellHashWithStorage`; both derive `storageRootHash` down the fold, the witness one
threading a `storageRootHashIsSet` flag with it, and both give a storage-less account
`empty.RootHash`. The pbin fold has no equivalent variable.

The near-miss worth naming so it is not mistaken for one: the subtree under the 264-bit prefix
`0xFF || stem` does hold exactly one account's non-header slots, and the cell at that point has a
hash. But nothing references it — no leaf value, no record field, no API — and it excludes slots
0..63, which live in the account zone.

Behavioural consequences:

- Proving a slot is a root-to-leaf walk of the global trie, per slot:
  `Storage` is `tree.leaf(storageKey(addr, slot))` (`PBinWitnessState.Storage`,
  `pbin_witness_state.go`). There is no per-account root to prove first and descend from.
- Deleting an account is deleting two key-space regions, not dropping one node. `removeAccount`
  (`pbin_update_stream.go`) emits a drop at the account's header stem and another at its
  storage prefix once the walk reaches that zone, because nothing enumerates the slots an account
  holds. Its code-zone leaves are content-addressed and stay: another account may run the same
  bytecode. Under the MPT, self-destruct drops one storage root; here there is no such node.
- Zero and absent are the same state, so a write of 32 zero bytes removes the leaf rather than
  storing zeros (`state_write`, eip:"Zero values and deletion"), and the fold collapses whatever
  subtree that empties (§5.4).
- The one persisted root record, under key `0x08`, is the root cell of the whole trie — one per
  trie, never per account.

## 12. Reconstruction check

Rebuilding the §5.1 tree by hand from the stored records **plus the leaf values read from the state
domains**, using only §4's two preimage shapes, reproduces the engine's `Process()` root. The
records alone are not enough input: only the code chunk carries its 32 bytes in the record
(LEAF_VALUE, §6), while an address-bearing leaf names a plain key and nothing else, so BASIC_DATA's
fields, CODE_HASH and the two storage values come from outside. Both storage values are elided
below: `0000…0005` is slot 5 holding 5, and `0000…002c` is slot 300 holding 0x2c (44) — not the slot
number 300 = 0x12c, which would give `L_slot300 = 120daed1…` and `root = 78497b28…` instead.

```
L_basic  = H(0x00 || 0012b9…7a00 || 0000000000000006 0000000000000003 …03e8)
         = 5dbe9906fc51df4ac846a8fe44ed92a3ec310d2edaeeea605289a290f6b38eba
L_code   = H(0x00 || 0012b9…7a01 || 1d6423ed…7696574d)
         = 970021c05f854ea9f1b9dd97d180ae62d0d2b9bb4acc23869cc5879919434ef8
N271     = H(0x01 || 0005 || 00 || L_basic || L_code)
         = de50844a66c2a773d715492d679fee88416467c0c5a7802a6b033199b357b0a4   [265-bit rec, cell 0]
L_slot5  = H(0x00 || 0012b9…7a45 || 0000…0005)
         = a3fe2808a326a445d72d6488cf48f5a73fa6e9eb552e7e4b224785b8a0208305
N265     = H(0x01 || 0101 || 12b9c2d7…61d53e7a 00 || N271 || L_slot5)
         = ac8c75fc4b6f6e25d0831229dd10d3ad353c56dc573141f6f7e62707a5076b5d   [7-bit rec, cell 0]
L_chunk0 = H(0x00 || 01073be8…9f969b5a 00 || 0060aabb000102 0000…00)
         = c2b8ca4b597abfe8f13fa11cebfcf945d417addef7841108b1064abe064c50e0
N7       = H(0x01 || 0006 || 00 || N265 || L_chunk0)
         = c9aca54ec7a6c2fe06fc1cee22bd609b559f1c16217ce2ea48793349b8d61be5   [0-bit rec, cell 0]
L_slot300= H(0x00 || ff12b9…d2fe2d42 2c || 0000…002c)
         = 8cfca105b43b269e0b12a1fcd0649a8b193381be942582566dc573ab8749fa49
root     = H(0x01 || 0000 || N7 || L_slot300)
         = 658b62aba5ac2933e86f1100cce5084bdb35b32d797b05d247abf27a2018c064   [key 08]
```

`0x0101` is `u16(257)` — the branch preimage's fixed-width count, where the same 257-bit prefix is
spelled `8102` as a uvarint inside the record. Every intermediate hash equals the bytes stored in
the corresponding record.

`L_chunk0` is where content addressing shows in the arithmetic: the chunk's key names the code hash,
not the account, so an identical contract at any other address produces this same leaf and the same
`N7` input.
