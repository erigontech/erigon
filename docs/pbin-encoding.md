# EIP-8297 row encoding

The binary commitment engine is `execution/commitment/v3/pbt`. Its EIP-8297 rules are in
`execution/commitment/eip8297`; `TreeKey`, `Bitpath`, `EncodeBitPrefix`, the value codec and the
hash primitives are shared by the reference and the engine. The engine stores rows, not a binary
node for every level.

## Keys

An account or code key is 34 bytes: a zone byte, a 32-byte position, and a one-byte sub-index.
An account key uses zone `0x00` and the hash of the left-padded address. A code chunk uses zone
`0x01` and the hash of its code hash and chunk group. Storage slots 0 through 63 live in the
account header and use 34-byte account-zone keys with sub-indices 64 through 127. Slots 64 and
above use 66-byte storage-zone keys: zone `0xff`, the address stem, the storage-group hash, and
the slot sub-index. `TreeKeyAccount`, `TreeKeyCodeChunk`, and `TreeKeyStorage` in `eip8297/keys.go`
derive these keys.

Rows use `AppendBitPath` in `eip8297/bitpath.go`: packed path bits followed by one byte containing
`BitLen % 8`. The final byte is therefore in the range 0 through 7; a path ending on a full byte
uses 0, not 8. For example, an 8-bit path containing `01` is encoded as `01 00`, and
`DecodeBitPath` rejects a trailing value of 8. Row keys are nibble-aligned. `{0x08}` is the global
root key, not a path key and not an `AppendBitPath` encoding. A bucket root is the 264-bit path for
`0xff || H(address32)`.

## Rows and cells

A row covers one four-bit window. A cell occupies the true four-bit slot in that window. A leaf
cell contains the suffix from the next window to the end of its complete tree key and a compact
value. A branch cell contains the prefix from the next window to its child's first split and the
two child hashes. The child row key is the row path extended by the slot and that prefix, truncated
at the child's window.

Rows are materialized only for windows that hold a split. The fold derives all binary levels inside
the row. The row may be a global or bucket row root, or an ordinary row. A bucket descriptor is
empty, a leaf with its complete key and value, or a branch with `selfExt = bits[264,D)` and its
two child hashes. The descriptor builds both the bucket record and the upper storage cell.

## Record value

The bin state uses marker `0xb1` and row format `0x20`. The shared framing validator is
`PBinValidateRowStateFormat` in `execution/commitment/pbin_state_format.go`. The state blob is
stored under `KeyCommitmentState` in a binary domain. The engine's current state is framed with
the same marker and format.

An ordinary record is:

```
hdr | childMask | leafMask | extMask? | branches | extensions | leaves
```

The header's low nibble is the record format. Bit 4 marks an extension root, bit 5 marks the
extension mask, bit 6 is reserved, and bit 7 marks a leaf root. A row sets bits 4 and 7 clear.
Masks are big-endian `u16` values; the leaf and extension masks are subsets of the child mask and
are encoded in ascending slot order. A branch cell stores 64 bytes of left and right hashes. An
extension stores `u16 bitLen` followed by packed prefix bits. A leaf stores packed suffix bits, a
one-byte value length, and the canonical compact value.

An extension root is `hdr | u16 bitLen | packed selfExt | left | right`. A leaf root is
`hdr | packed suffix | value length | compact value`. The masks and every `u16 bitLen` are
big-endian, as written by `EncodeRecord`, `encodeRow`, and `encodeExtRoot` in
`execution/commitment/v3/pbt/record.go` and read by their decoder counterparts. The one-byte leaf
value length is limited to 255 bytes. These forms are valid only at the global root and a bucket
root. An empty root has no record and hashes to 32 zero bytes. A zero-length record value is a
tombstone; a 32-byte zero value is a leaf deletion before compact encoding.

Compact values follow the leaf zone and sub-index rules in `EncodeLeafValue` and `DecodeLeafValue`
in `execution/commitment/eip8297/values.go`:

- Storage leaves and account-header storage leaves remove leading zero bytes. The compact value is
  non-empty and its first byte is non-zero.
- Code chunks remove trailing zero bytes. The compact value is non-empty and its last byte is
  non-zero.
- `BASIC_DATA` starts with a big-endian `u16` whose reserved high four bits are zero. Its remaining
  fields are `(code_size_bytes << 9) | (nonce_bytes << 5) | balance_bytes`, followed by minimal
  big-endian field bytes in that order; zero-width fields are omitted.
- `CODE_HASH` is empty for the empty-code hash and otherwise exactly 32 bytes.
- `DELEGATION` is encoded as the 20-byte target. Decoding restores the `0xef0100` marker and
  rejects any other length.
- Other account leaves use exactly 32 bytes.

`DecodeLeafValue` re-encodes the decoded 32-byte value with `EncodeLeafValue` and rejects any
different byte sequence, so these forms have one canonical compact spelling. Zero storage values,
zero code chunks, and zero account-header storage values are rejected.

Decoding is total and canonical. It rejects an unknown format, reserved bits, contradictory root
bits, root forms at ordinary keys, invalid mask subsets, rows with fewer than two cells, invalid
extension lengths, a split at or beyond the key length, non-zero padding, and trailing or missing
bytes. Compact values must re-encode to exactly the bytes that were decoded.

## Roots and fold

The empty root is zero. A one-entry subtree is its leaf hash. A row root folds occupied slots in
order. Leaf hashes are `H(0x00 || completeKey || value)`. Branch hashes are
`H(0x01 || EncodeBitPrefix(prefix) || left || right)`, where `EncodeBitPrefix` is the EIP-8297
variable-length bit-prefix encoding in `eip8297/reference.go`. A top split after a non-empty prefix is
an extension root with the same branch formula. The selected hash suite applies to both node
hashes and key derivation; `SetHashSuite` in `eip8297/hash.go` selects Keccak or BLAKE3.

The stored record contains structure, suffixes, values and branch pairs. It does not contain a
leaf hash. Leaf references are derived by the execution-side prefetcher and are accepted only
when the referenced raw record is byte-identical to the record being folded. An untouched cell
uses its raw record or a valid cached reference; changed cells are recomputed.

`Verify` checks canonical records, row and child prefixes, child folds, and bucket descriptors.
The decoder and cross-row checks are implemented by `execution/commitment/v3/pbt/record.go` and
`verify.go`.

## Format versioning

The `0x20` row format is distinct from the removed legacy `0x10` format and from legacy flags
values `0x00` through `0x07`. Datadir opening and staged rebuild validation reject those older
formats with an instruction to run `commitment convert-pbt`. A change to the embedding or hash
suite also requires converting into a fresh binary datadir; the selected suite is stored
in `erigondb.toml` as `trie_hash`.
