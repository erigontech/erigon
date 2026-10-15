# Commitment v3

Commitment v3 is a self-contained commitment trie selected with the
`commitment-v3` trie variant. It reads commitment records during the fold and computes the root
without reading account or storage values from the state domain.

## Input mode

The implementation accepts `ModeCollect` updates only. The other modes are rejected because their
current streams do not provide the final value required by the read-free fold.

## Keys

Paths are packed two nibbles per byte. An odd final nibble occupies the high half and the low half
is zero. The final byte is the nibble count.

```
account node    40 || packed path || nibble count
storage node   41 || keccak(address) || packed path || nibble count
state          42
account root   40 00
storage root   41 || keccak(address) || 00
```

The v3 key space is disjoint from V1 `HexToCompact` commitment keys. It is not disjoint from V2
keys: `EncodeKeyV2([4,0])` equals `40 00`. The package therefore requires a V1-keyed commitment
domain and rejects V2-keyed domains.

## Records

Non-root records start with a format and flag byte, followed by `childMask` and `leafMask`. An
optional extension mask follows. Branch-child slots are fixed 32-byte hashes in ascending slot
order. Extension trailers and leaf trailers follow the slots in ascending slot order. The embedded
flag bit is reserved and rejected.

Leaf entries contain the packed hashed suffix, a one-byte value length, and the value. Root records
may carry a self-extension inline. A one-slot trie uses the leaf-root form. A zero-length record is
a tombstone.

The record body stores the compact account representation, while account consensus hashing uses the
full four-field account RLP. Record encoding happens during the fold; persistence uses v3-local
complete deltas, including tombstones and the previous record bytes.
