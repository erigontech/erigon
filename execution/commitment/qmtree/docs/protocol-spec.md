# QMTree Protocol Specification

This document is the normative reference for the QMTree wire formats, proof
structures, RPC API contracts, and digest encoding. Implementers building
compatible verifiers, provers, or tooling should use this document as the
single source of truth.

---

## 1. Tree Structure

### 1.1 Serial numbering

Every transaction in Erigon's chain history receives a monotonically increasing
**txNum** (`txNum`), starting from 0. This is Erigon's global
transaction number, reused directly as the qmtree txNum.

```
Block 0:  txNums  0 .. B0-1
Block 1:  txNums  B0 .. B1-1
...
```

### 1.2 Leaves and twigs

Leaves are grouped into **twigs** of 2048 leaves each.

```
TWIG_SHIFT        = 11          // log2(leaves per twig)
LEAF_COUNT_IN_TWIG = 2048       // 1 << TWIG_SHIFT
TWIG_MASK         = 2047

twigId       = txNum >> TWIG_SHIFT
posInTwig    = txNum &  TWIG_MASK
```

### 1.3 Leaf hash

Each leaf is keyed by its txNum and contains four 32-byte hash fields
committed by the executor:

```
preStateHash     = DeriveSha_MPT({ (domain, key) → value } for all state reads)
stateChangeHash  = DeriveSha_MPT({ (domain, key) → value } for all state writes)
transitionHash   = keccak256(EVM opcode trace + transition records — see transition-format.md)
previousLeafHash = leafHash(txNum - 1), or 0x00..00 for txNum=0
```

The leaf hash is:

```
leafHash = keccak256(preStateHash || stateChangeHash || transitionHash || previousLeafHash)
```

All hashes are 32 bytes. Domain values: 0 = accounts, 1 = storage, 2 = code.

### 1.4 Intra-twig Merkle tree (levels 0–12)

Within a twig, leaf hashes are combined into a 2048-leaf binary Merkle tree
using the **keccak256 hasher** with a level prefix byte:

```
internal_node(level, left, right) = keccak256([level_byte] || left_hash || right_hash)
```

The twig root sits at level 12 (`FirstLevelAboveTwig - 1`).

### 1.5 Upper tree (levels 13+)

Twig roots are combined in a binary tree at levels ≥ 13
(`FIRST_LEVEL_ABOVE_TWIG = 13`) using the same hash function. The tree root is
at level `13 + 63 - bits.LeadingZeros64(youngestTwigId)` for
`youngestTwigId > 0`, or level 13 for a single twig.

---

## 2. Proof Wire Format

### 2.1 `ProofPath` — single-leaf inclusion proof

A `ProofPath` proves that the leaf at `txNum` is committed in the tree with
the given `Root`. It is serialised by `ProofPath.ToBytes()` as:

```
[8 bytes]       txNum      (little-endian uint64)
[32 bytes]      selfHash       = LeftOfTwig[0].SelfHash = leafHash(txNum)
[11 × 32 bytes] intraTwigPeers = LeftOfTwig[0..10].PeerHash
[U × 32 bytes]  upperPeers     = UpperPath[0..U-1].PeerHash
[32 bytes]      root           = tree root at serialisation time
```

Total size: **424 + U×32 bytes** where U = len(UpperPath).

`OTHER_NODE_COUNT = 1 (selfHash) + 11 (intraTwig) + 1 (root) = 13`, so
`len = 8 + (U + 13) × 32`.

Deserialise with `qmtree.BytesToProofPath(bytes)` (returns error if truncated).

#### Upper-path length (U) as a function of tree size

| Leaves (N) | Twigs | U | ProofPath size |
|---|---|---|---|
| ≤ 2,048 | 1 | 0 | 424 B |
| 4,096 | 2 | 1 | 456 B |
| 1,261,568 (~hoodi 100k blks) | 616 | 10 | 744 B |
| 2,097,152 | 1,024 | 10 | 744 B |
| 1,073,741,824 | 524,288 | 19 | 1,032 B |
| 2.2 × 10⁹ (~mainnet) | 1,074,219 | 20 | 1,064 B |

### 2.2 `RangeWitness` — block-range proof

A range witness for transactions `[from, to]` consists of:
- `FirstProof`: `ProofPath` for `from`
- `LastProof`: `ProofPath` for `to`
- `Leaves[]`: leaf data for all txNums in `[from, to]`

The hash chain through `previousLeafHash` links all intermediate leaves,
allowing a verifier to confirm the full range from just the two boundary proofs.

Serialised as `qmtree.WitnessToBytes(witness)` (concatenation of header,
FirstProof, LastProof, and leaf array). Deserialise with
`qmtree.BytesToWitness(bytes)`.

---

## 3. RPC API (`qm_` namespace)

All methods are served by `QMAPIImpl` under the `qm_` JSON-RPC namespace.
Enabled when `--experimental.qmtree` is set.

### Common types

```typescript
type Hash        = "0x" + 64 hex chars    // 32 bytes
type Bytes       = "0x" + hex             // variable length
type Uint64      = "0x" + hex uint64
type BlockNumber = Uint64 | "latest" | "earliest" | "pending"

type LeafData = {
  preStateHash:     Hash
  stateChangeHash:  Hash
  transitionHash:   Hash
  previousLeafHash: Hash
}
```

### 3.1 `qm_getProof(address, blockNrOrHash) → QMProofResult`

Returns the qmtree inclusion proof for the most recent transaction that
modified `address` at or before the given block.

```typescript
type QMProofResult = {
  address:     "0x" + 40 hex      // 20-byte Ethereum address
  txNum:       Uint64
  blockNumber: Uint64
  leafData:    LeafData
  leafHash:    Hash
  merkleProof: Bytes               // ProofPath.ToBytes()
  qmtreeRoot:  Hash
  verified:    bool
}
```

### 3.2 `qm_getWitness(blockNrOrHash) → QMWitnessResult`

Returns a range witness covering all transactions in the given block.

```typescript
type QMWitnessLeaf = { txNum: Uint64, leafData: LeafData, leafHash: Hash }

type QMWitnessResult = {
  blockNumber: Uint64
  qmtreeRoot:  Hash
  firstProof:  Bytes               // ProofPath.ToBytes() for first tx in block
  lastProof:   Bytes               // ProofPath.ToBytes() for last tx in block
  leaves:      QMWitnessLeaf[]
  verified:    bool
}
```

### 3.3 `qm_getTxWitness(blockNrOrHash, txIndex) → QMProofResult`

Returns the qmtree proof for a single transaction at the given 0-based index
within the block. Returns the same `QMProofResult` shape as `qm_getProof`.

### 3.4 `qm_getAccountStateProof(address, storageKeys[], blockNrOrHash) → QMStateProofResult`

Returns the account's current state values (balance, nonce, codeHash, storage
slots) together with the qmtree proof for the most recent transaction that
touched the account.

```typescript
type QMStorageSlotProof = { slot: Hash, value: Bytes }

type QMStateProofResult = {
  address:       "0x" + 40 hex
  blockNumber:   Uint64
  txNum:         Uint64
  balance:       Uint64
  nonce:         Uint64
  codeHash:      Hash
  storageProofs: QMStorageSlotProof[]
  witness:       QMProofResult
  verified:      bool
}
```

### 3.5 `qm_getTxStateProof(address, storageKeys[], blockNrOrHash, txIndex) → QMStateProofResult`

Same as `qm_getAccountStateProof` but pinned to the given transaction index
within the block, not the latest writer.

### 3.6 `qm_getLatestStateProof(address, storageKeys[]) → QMStateProofResult`

Same as `qm_getAccountStateProof` but always queries the chain tip (no block
argument). Returns the most recent writer across all history.

### 3.7 `qm_call(callArgs, blockNrOrHash) → QMCallResult`

Executes a simulated call against historical state, tracks every state read, and
returns the combined qmtree witnesses for all unique last-writer transactions.

```typescript
type QMAccessedElement = {
  domain:         number   // 0=accounts, 1=storage, 2=code
  key:            Bytes    // 20-byte address for accounts/code; 52-byte addr||slot for storage
  value:          Bytes    // value that was read
  lastWriteTxNum: Uint64   // txNum of the last write ≤ blockEnd; 0 if never written
}

type QMCallResult = {
  returnData:   Bytes
  gasUsed:      Uint64
  failed:       bool
  revertReason: Bytes                // omitted if not reverted
  accessed:     QMAccessedElement[]  // one per state read during execution
  witnesses:    QMProofResult[]      // one per unique lastWriteTxNum, sorted ascending
}
```

**Algorithm**: execute via `DoCall` with `readTracker` wrapping the history
state reader; for each `(domain, key)` read, call `IndexRange` with descending
order and `to=endTxNum` to obtain `lastWriteTxNum`; deduplicate; call
`tracker.GetWitness(txNum)` for each unique txNum.

### 3.8 `qm_callProof(callArgs, blockNrOrHash) → QMCallProof`

Same execution as `qm_call`, but returns a **compact twig-grouped proof** where
upper-tree peer hashes shared by leaves in the same twig are stored once, plus a
32-byte Merkle **digest** committing to all proof fields.

```typescript
type QMCallProofTwig = {
  twigId:          Uint64    // txNum >> TWIG_SHIFT
  upperPeerHashes: Hash[]    // U peer hashes, levels FIRST_LEVEL_ABOVE_TWIG..root-1
}

type QMCallProofLeaf = {
  txNum:               Uint64
  twigIndex:           number    // index into QMCallProof.twigs
  leafData:            LeafData
  selfHash:            Hash      // = leafHash(txNum)
  intraTwigPeerHashes: Hash[]    // 11 peer hashes, levels 0..10 within twig
  verified:            bool
}

type QMCallProof = {
  returnData:   Bytes
  gasUsed:      Uint64
  failed:       bool
  revertReason: Bytes
  accessed:     QMAccessedElement[]
  root:         Hash              // single qmtree root, shared by all leaves
  twigs:        QMCallProofTwig[] // one entry per unique twig
  leaves:       QMCallProofLeaf[] // one per unique lastWriteTxNum
  digest:       Hash              // Merkle commitment over all fields (see §4)
}
```

#### Size comparison: individual witnesses vs compact proof

For M accessed leaves across T unique twigs, at U upper levels:

| Format | Size formula | M=20, T=5, U=10 |
|---|---|---|
| Individual witnesses | M × (424 + U×32) | 14,880 B |
| Compact proof root | 32 | 32 B |
| Compact proof twigs | T × (8 + U×32) | 1,640 B |
| Compact proof leaves | M × 520 | 10,400 B |
| **Compact total** | **32 + T×(8+U×32) + M×520** | **12,072 B** (**19% smaller**) |

Compact is smaller when average twig occupancy (`M/T`) exceeds a break-even
threshold. Break-even occurs at `T/M = (U×32 − 96) / (8 + U×32)`:

| Scale | U | Break-even M/T | Typical M/T | Result |
|---|---|---|---|---|
| Hoodi 100k blks | 10 | 1.47 leaves/twig | 4+ | **~20% smaller** |
| Mainnet tip | 21 | 1.18 leaves/twig | 4+ | **~37% smaller** |
| Any | any | T = M (all different) | — | 10-14% **larger** |

When a call accesses few unique addresses (common for token transfers, DeFi),
twigs cluster naturally and compact format wins. For maximally diverse accesses
(T = M), prefer individual witnesses.

### 3.9 `qm_verifyProof(proofBytes) → bool`

Deserialises `proofBytes` as a `ProofPath` (see §2.1) and calls
`ProofPath.Check(hasher, true)`. Stateless — does not require a running tracker.
Returns `false` (not an error) if the proof is cryptographically invalid.

### 3.10 `qm_verifyWitness(witnessBytes) → bool`

Deserialises `witnessBytes` as a `Witness` (see §2.2) and calls
`Witness.Verify(hasher)`. Stateless. Returns `false` for invalid witnesses.

---

## 4. Proof Digest (`QMCallProof.Digest`)

The `digest` field of `QMCallProof` is an SSZ-inspired `hash_tree_root` over all
proof fields, producing a 32-byte commitment. A verifier can store only the
digest and later confirm a full proof has not been tampered with.

### 4.1 Primitives

```
merkleize(chunks[]):
  pad chunks to next power of 2 with zero-hashes
  reduce bottom-up: chunks[i] = keccak256(chunks[2i] || chunks[2i+1])
  return chunks[0]

merkleizeList(elems[]):
  root = merkleize(elems)
  lenChunk = uint64_le(len(elems)) padded to 32 bytes
  return keccak256(root || lenChunk)
```

### 4.2 Element leaf hashes

```
accessedLeaf(a):
  = keccak256(uint64_le(a.domain) || a.key || a.value || uint64_le(a.lastWriteTxNum))

twigLeaf(t):
  = keccak256(uint64_le(t.twigId) || concat(t.upperPeerHashes))

leafLeaf(l):
  = keccak256(
      uint64_le(l.txNum) ||
      l.leafData.preStateHash ||
      l.leafData.stateChangeHash ||
      l.leafData.transitionHash ||
      l.leafData.previousLeafHash ||
      l.selfHash ||
      concat(l.intraTwigPeerHashes)
    )
```

### 4.3 Digest computation

```
digest = merkleize([
  c0: keccak256(proof.returnData),
  c1: uint64_le(proof.gasUsed) padded to 32 bytes,
  c2: 0x01 at byte 0 if proof.failed, else 0x00..00,
  c3: keccak256(proof.revertReason),
  c4: merkleizeList([accessedLeaf(a) for a in proof.accessed]),
  c5: proof.root,         // qmtree root embedded directly (already a Merkle commitment)
  c6: merkleizeList([twigLeaf(t) for t in proof.twigs]),
  c7: merkleizeList([leafLeaf(l) for l in proof.leaves]),
])
```

The qmtree root (chunk 5) is embedded directly rather than re-hashed, preserving
the two-level structure: the qmtree root commits to on-chain state; the digest
commits to the proof itself.

**SSZ compatibility**: the encoding follows SSZ `hash_tree_root` conventions
(power-of-2 padding, length mixing) so a future full SSZ serialisation of
`QMCallProof` will produce an identical digest.

---

## 5. Verification Algorithm

To verify a `QMCallProof` received from an untrusted source:

### Step 1 — Digest check (tamper detection)
Recompute `callProofDigest(proof)` and confirm it equals `proof.Digest`.

### Step 2 — Leaf Merkle paths
For each leaf `l` in `proof.Leaves`:
1. Recompute `leafHash = keccak256(l.leafData.preStateHash || ... || l.leafData.previousLeafHash)`
2. Assert `l.selfHash == leafHash`
3. Walk `l.IntraTwigPeerHashes` (11 steps) bottom-up to obtain the twig's left-subtree root.
   At step `i`: `PeerAtLeft = (l.txNum >> i) & 1`
4. Walk `Twigs[l.TwigIndex].UpperPeerHashes` (U steps) to the tree root.
   At step `i`: `PeerAtLeft = (twigId >> i) & 1`  where `twigId = l.txNum >> TWIG_SHIFT`
5. Assert the recomputed root equals `proof.Root`

### Step 3 — State consistency (requires SMT — future work)
Verify that the state values in `Accessed` are consistent with the
`preStateHash` fields in the relevant leaves, once `preStateHash` is an SMT
root (see `state-proof-analysis.md §Question 3b`).

### Step 4 — Call binding (future work)
Verify that the `Accessed` set and return data match the call's execution trace.
`transitionHash` already embeds the EVM execution hash (the rolling keccak256 over every
opcode — see [design.md §2.2](design.md#execution-hash-per-opcode-record-format-exechasher)
for the per-opcode record format). Full call binding requires completing Step 3 (SMT
pre-state root) so individual reads can be traced to the opcode that issued them.

---

## 6. Key Constants Reference

| Constant | Value | Meaning |
|---|---|---|
| `TWIG_SHIFT` | 11 | log2(leaves per twig) |
| `LEAF_COUNT_IN_TWIG` | 2048 | leaves per twig |
| `FirstLevelAboveTwig` | 13 | first upper-tree level |
| `TwigRootLevel` | 12 | level of the twig root node |
| `OTHER_NODE_COUNT` | 13 | fixed nodes in ProofPath.ToBytes() |
| Domain: accounts | 0 | key = 20-byte address |
| Domain: storage | 1 | key = 20-byte address ++ 32-byte slot |
| Domain: code | 2 | key = 20-byte address |

---

## 7. Document Index

| Document | Description |
|---|---|
| `README.md` | Overview, status, and reading guide |
| `protocol-spec.md` | **This file** — normative wire format and API reference |
| `design.md` | Architecture, tree design, keyset indexing, RPC namespace, future work |
| `state-proof-analysis.md` | Analysis Q&A, RPC implementation details, compact proof sizing, provable calls vision |
| `transition-design.md` | Proof-of-transition design rationale |
| `transition-format.md` | Transition record wire format specification (normative) |
