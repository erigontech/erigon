# PBT witnesses

Erigon exposes execution witnesses through `debug_executionWitness`. A PBT witness is built from the state before the
requested block and is checked by replaying that block without reading the database. MPT replay keeps the existing
`witnessVerifySkipped` assertion gate.

## Request parameters

`trie` selects the commitment tree. When omitted, the request uses the tree that is canonical at the block: MPT before
the binary-trie fork and PBT after it. The explicit values are `"mpt"` and `"pbt"`. Any other value is an
`rpc.InvalidParamsError`.

`mode` selects the MPT response form. When omitted, it is `legacy`; the empty string and `"legacy"` have the same
meaning. `"canonical"` requests the canonical MPT form. Other values are rejected. `mode` is an MPT-only parameter,
so supplying it with a PBT result is an error saying that the mode applies to the MPT witness only. Canonical mode is
valid for MPT after the binary-trie fork as well.

The request resolution is implemented by `resolveWitnessRequest`, `resolveWitnessTrie` and `resolveWitnessMode` in
`rpc/jsonrpc`.

## Anchors and availability

For each block endpoint, the requested trie uses its canonical header root when it is canonical at that endpoint. When
it is not canonical, it uses the block's shadow root. The parent and post-state endpoints are resolved independently.

| Requested trie | Pre-fork block | Post-fork block |
| --- | --- | --- |
| MPT | header root | shadow root |
| PBT | shadow root | header root |

This also defines the transition boundary. For the first binary block, an MPT request uses the pre-fork parent's header
root and the binary block's shadow root; a PBT request uses the parent's shadow root and the binary block's header root.
`witnessAnchorForBlock` and `witnessAnchors` implement this table.

Before building a witness, `checkWitnessAvailability` verifies that the datadir contains the requested trie's domain,
that the domain covers the parent, and that it was not frozen or stopped before the parent. It also checks
`HistoryStartFrom` for retained commitment history and requires every shadow root needed by the anchor table. A failure
names the requested trie and the failed condition. The check does not fall back to the other trie.

In dual mode, the hex trie is `kv.CommitmentDomain` and the binary trie is `kv.CommitmentBinDomain`. In a binary-only
datadir, the binary trie uses `kv.CommitmentDomain`.

## Blob format

The PBT blobs follow geth-pbt ref `793dedb`, in `trie/bintrie`, but Erigon uses its own witness implementation. The
shared rules are in `execution/commitment/eip8297`.

Leaf blobs and branch blobs are encoded through `PBinEncodeLeaf` and `PBinEncodeBranch`, using the EIP-8297 leaf and
branch preimages. A group blob is encoded by `PBinEncodeGroup`. It stores its consumed position, stem, suffix
sub-indices and 32-byte values. Its hash is the positional fold at that stored position, computed by
`PBinHashBlob`.

The root path is empty. Every other path is the bit-prefix encoding produced by the witness path encoder. Paths are
ordered by their encoded bytes, and `keys` and `state` remain parallel in that order.

## Node sets

The PBT builder resolves only pre-state nodes needed by the requested reads and writes. A node created during the block
is not added merely because a later operation reads it again. Every returned blob is checked against the pointer that
reached it, including the root against the requested pre-state anchor. Collapse survivors are resolved at their old
paths before they move.

The PBT response carries the exact resolved set: removing any path/blob pair must make verification fail, and adding or
altering an unused pair is also rejected. The empty-tree form is the only response with `keys` set to null and empty
`state` and `codes` arrays. For a non-empty tree the root blob is present.

PBT `codes` is a content-keyed set. Code read through code-size access is included; modified code that is never read is
not. MPT code collection keeps its existing address-based behaviour.

## Verification

`verifyPBinWitnessAgainstBlock` constructs the EIP-8297 witness tree from `keys` and `state`, checks each blob hash and
parent pointer, and checks the root against the pre-state anchor. `replayBlockOverWitness` executes the block against
that tree, the supplied code blobs, and the in-block overlays. It applies the block's writes through the same PBT
driver and compares the resulting root with the post-state anchor.

The verifier suppresses the missing-node error only for the synthetic system-call touch of `SYSTEM_ADDRESS`. The scope
covers system calls from `Initialize`, its `FinalizeTx`, `Finalize`, and `CommitBlock`, but resolver errors are latched
throughout those scopes except for that synthetic touch. A user transaction access to `SYSTEM_ADDRESS` is retained when
the per-transaction access set records it, or when `ResolveCode` or `ResolveCodeHash` follows a delegation designator
to it. Loading a designator with `EXTCODE*` does not follow it, and an access-list entry alone is not enough. The
delegation case needs the basic-data proof when the system call has already warmed the account. Genuine reads of system
contracts need their proofs. PBT replay also compares the receipt root after
Byzantium, gas used, and blob gas used with the block header. Contract creation over an
existing account with storage wipes that storage; creation of a previously absent account does not walk an unproved
storage subtree. An account without `BASIC_DATA` is considered present only after its code-hash or delegation leaf is
resolved. All supplied PBT entries must be consumed before verification succeeds.

## Witness cache

The cache is keyed by block hash, but a cached result is served only when the request is a legacy request for the
default trie at that block. `serveFromWitnessCache` enforces this routing. A non-default-trie request builds directly,
so it cannot join a build for the other trie. The eager paths `buildAndCache` and `buildAndCacheHeadCapture` build the
default trie; non-default requests are on demand.

Canonical MPT requests are not served by the legacy cache. A cache-only node reports a distinct non-default-trie error,
while the existing out-of-window, reorged-away and canonical-unavailable errors remain specific to their cache cases.

## Differences from geth-pbt

The geth-pbt comparison is against ref `793dedb`.

- Geth's `triePrefetcher.prefetch` and `subfetcher.loop` deduplicate PBT storage requests with slot-keyed sets. The
  owner is used to group calls to `BinaryTrie.PrefetchStorage`, not as part of the duplicate key. Erigon's BAL and
  state prefetch paths carry the owner explicitly because its row resolver needs the full address and slot.
- Geth's `BinaryTrie.DeleteAccount` calls `DeletePrefix`, and `deleteSubtree` resolves and records every node below a
  deleted storage prefix. Erigon's `PBinTree.DeleteAccount` uses `deletePrefix` and drops a covered subtree at its cut
  point. This keeps the witness proportional to the proof needed for the deletion.
- Geth's `EVM.create` keeps an existing empty account object, so `CreateContract` can leave its storage in place when a
  contract is created at an account that already holds storage. Erigon's `pbinWitnessStateless.CreateContract` wipes
  the existing storage through the PBT driver. The witness verifier follows Erigon's state-transition rule; the
  specification leaves this address-collision case open.
