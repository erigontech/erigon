# QMTree Ethereum State Commitment — Design

## 1. Introduction

This document describes the design of QMTree, an alternative state commitment
scheme for Ethereum that uses the
[QMDB Twig Merkle Tree](https://github.com/LayerZero-Labs/qmdb/tree/main/docs)
in place of the keccak256-hashed hex patricia trie. The tree is indexed by
Erigon's global transaction number (`txnum`).

### 1.1 References

- [QMDB Design — General Ideas](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/design.md) — covers entry structure, twig Merkle trees, pruning, and exclusion proofs (note: ActiveBits are not used in this implementation — see §2.5)
- [QMDB Architecture](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/architecture.md) — covers the prefetcher-updater-flusher pipeline, HPFile, TwigFile, EntryFile, B-tree indexer, edge nodes, and batch sync
- [Go port of qmtree](../execution/commitment/qmtree/) — the tree, twig, proof, and hpfile implementations

### 1.2 Motivation

The current hex patricia trie has well-known costs:

- **O(log n) random reads per update** — each key modification traverses ~7-8 trie nodes, each requiring a database read
- **Large branch nodes** — RLP-encoded branch nodes contain up to 16 child hashes (512+ bytes)
- **Nested storage tries** — each contract has its own storage trie, requiring separate root computation
- **Expensive proofs** — `eth_getProof` must traverse the full trie depth

The QMDB twig Merkle tree offers a fundamentally different model:

- **O(1) append** — new entries are appended sequentially; no random trie traversal
- **Compact nodes** — binary tree with 32-byte SHA256 hashes; no RLP overhead
- **Flat namespace** — all state changes (accounts + storage) are leaves in one tree; no nested tries
- **Compact proofs** — ~(14 + log2(twigs)) x 32 bytes per proof

## 2. Core Design

### 2.1 Mapping TxNum to Serial Number

The QMDB design (Section 1.0) defines entries as `Entry := (Height, Key, Value, TxNum)` where TxNum is a monotonically increasing index. Erigon already maintains an equivalent: the global **transaction number** (`txnum`), a sequential counter that increments for every transaction across all blocks.

```
Block 0:   txnums 0..4
Block 1:   txnums 5..12
Block 2:   txnums 13..25
...
```

Each txnum maps 1:1 to a qmtree txNum. This gives us:

- **Twig ID** = `txnum >> 11` (each twig holds 2048 leaves)
- **Position in twig** = `txnum & 2047`
- **Block-level root** = `tree.Root()` after appending the block's final txnum

### 2.2 Leaf Hash Construction

Each leaf at txNum `txnum` commits to four 32-byte fields:

```
preStateHash     = DeriveSha_MPT({ (domain, key) → value } for all state reads by this tx)
stateChangeHash  = DeriveSha_MPT({ (domain, key) → value } for all state writes by this tx)
transitionHash   = keccak256(transition record sequence — see transition-format.md)
previousLeafHash = leafHash(txnum - 1),  or 0x00..00 for txnum = 0
```

The leaf hash is:

```
leafHash = keccak256(preStateHash || stateChangeHash || transitionHash || previousLeafHash)
```

**preStateHash and stateChangeHash** use Ethereum's `DeriveSha` MPT construction over a sorted map of `(domain, key) → value` pairs. This means each hash is itself a Merkle root that can prove individual key/value pairs without disclosing the full changeset (see `state-proof-analysis.md §Question 3b`).

**transitionHash** commits to the complete execution trace: the EVM opcode-by-opcode record (the _execution hash_) plus the 25 spec-mandated state transition operations outside the EVM (nonce increment, gas purchase, fee distribution, etc.). The execution hash component makes every opcode and its inputs/outputs part of the leaf commitment, enabling fraud proofs and verifiable `eth_call`. See `transition-format.md` for the exact binary encoding of each record type.

#### Execution hash: per-opcode record format (`ExecHasher`)

The execution hash is a rolling keccak256 over every EVM instruction executed in the transaction. Each instruction produces a variable-length record:

```
┌─────────┬──────┬────┬──────┬──────┬───────────────────┬────────────────────┐
│ depth:2 │ pc:8 │ op │ gas:8│cost:8│ stack_in: P × 32  │ stack_out: Q × 32  │
└─────────┴──────┴────┴──────┴──────┴───────────────────┴────────────────────┘
```

| Field     | Bytes  | Encoding   | Description                               |
|-----------|--------|------------|-------------------------------------------|
| depth     | 2      | uint16 BE  | EVM call depth (0 = top-level)            |
| pc        | 8      | uint64 BE  | Program counter before execution          |
| op        | 1      | byte       | Opcode byte (0x00–0xFF)                   |
| gas       | 8      | uint64 BE  | Gas remaining before this instruction     |
| cost      | 8      | uint64 BE  | Total gas cost (constant + dynamic)       |
| stack_in  | P × 32 | uint256 BE | Top P stack items consumed (P = numPop)   |
| stack_out | Q × 32 | uint256 BE | Top Q stack items produced (Q = numPush)  |

All nested call frames within the transaction feed into the same single rolling hash; the `depth` field disambiguates frames. Reverted inner calls are included — the hash captures what was _executed_, not what was _committed_. Reverts are reflected in `stateChangeHash` (which excludes reverted mutations).

**Precompile calls** bypass the interpreter loop and emit a synthetic record:

```
┌─────────┬──────────────┬───────────────┬────────────────┬──────┐
│ depth:2 │ 0xFE (mark.) │ input_hash:32 │ output_hash:32 │ gas:8│
└─────────┴──────────────┴───────────────┴────────────────┴──────┘
```

followed by the precompile address (20 bytes). `input_hash` and `output_hash` are keccak256 of the raw input/output bytes.

**Cross-client determinism**: the fields (pc, opcode, gas remaining, gas cost, stack inputs/outputs, call depth) are all Yellow Paper-specified. Any spec-compliant EVM produces identical values for the same transaction against the same pre-state, making the execution hash deterministic across clients.

**previousLeafHash** chains leaves together sequentially, so any tampering with an earlier leaf invalidates all subsequent leaf hashes.

Domains: 0 = accounts, 1 = storage, 2 = code.

### 2.3 Tree Structure

The tree is a binary Merkle tree over twigs:

```
                    Root
                   /    \
            Upper Tree (levels 13+)
           /          \
     TwigRoot_0     TwigRoot_1    ...   (level 12)
       /    \           /    \
     ...    ...       ...    ...        (levels 0-11: 2048-leaf binary Merkle tree)
```

Each **twig** is a 2048-leaf binary Merkle tree (levels 0–11). The twig root at level 12 is `keccak256([12] || leftChild || rightChild)`. The upper tree (levels 13+) combines twig roots into the final tree root using the same hash function.

Internal nodes are computed as `keccak256([level_byte] || left || right)`. The level prefix prevents second-preimage attacks between tree levels.

### 2.4 Block-Level and Tx-Level Hashes

The tree supports two granularities of commitment:

- **Tx-level root**: `tree.Root()` after appending leaf for txnum T. This commits to all state changes through transaction T.
- **Block-level root**: `tree.Root()` after appending the final leaf of block N (at `max_txnum(N)`). This is the candidate "state root" for block N.

Both are computed and logged for analysis. The block-level root is the one that would replace the current hex patricia state root in block headers.

### 2.5 State currency

State currency — answering "is this leaf the latest write to key K?" — is served by
the `KeyIndex` (§5.2) rather than by in-tree metadata. The twig root is therefore
simply the binary Merkle root over the 2048 leaf hashes.

The `previousLeafHash` chain links all leaves for a key in sequence, so the full
write history for any key is traversable without additional tree metadata.

## 3. Proofs and Witnesses

### 3.1 QMTree Proof Structure

A qmtree proof for a given `txNum` (see `proof.go`) consists of:

| Component | Size | Purpose |
|---|---|---|
| `LeftOfTwig[11]` | 11 x 32 bytes | Sibling hashes through the 2048-leaf Merkle tree |
| `UpperPath[N]` | N x 32 bytes | Sibling hashes through the upper tree to root |
| `TxNum` | 8 bytes | The txnum being proven |
| `Root` | 32 bytes | The tree root |

Total proof size: `(13 + N) x 32` bytes where N = `log2(num_twigs)`. For 1 billion transactions (~10 years of Ethereum mainnet), N ~ 19, giving **~1024 bytes** per proof.

Compare to hex patricia proofs which are typically 3-5 KB per proof due to RLP-encoded branch nodes.

### 3.2 Account State Proof

To prove "account A has value V at block B":

1. **Find the txnum**: Query the inverted index for the latest txnum <= `max_txnum(B)` that modified account A
2. **Get the leaf preimage**: The sorted `(domain, key, value)` list for that txnum
3. **Get the tree proof**: `tree.GetProof(txnum)` — Merkle path from leaf to root
4. **Verify**:
   - `tree.CheckProof(proof)` passes (leaf is in tree)
   - `SHA256(canonical_encoding(preimage))` matches the leaf hash in the proof
   - `(AccountsDomain, A, V)` is present in the preimage
   - `proof.Root` matches the block's committed root

### 3.3 Storage Proof

Identical to account proofs, but the key is `address + storage_slot` and the domain is StorageDomain.

### 3.4 Comparison with Current Proofs

| Aspect | Hex Patricia | QMTree |
|---|---|---|
| Proof target | "Key K has value V in state S" | "Tx T changed key K to V; T is committed in tree with root R" |
| Proof size | ~3-5 KB (RLP branch nodes) | ~1-1.2 KB (32-byte hashes) |
| Verification | Traverse trie path, verify hashes | Verify binary Merkle path + check preimage |
| Nested storage | Separate storage trie proof | Flat — same proof structure for accounts and storage |
| Negative proof | Requires extension/branch node structure | Requires NextKeyHash field (QMDB Section 7.0, future work) |

## 4. Unwinding (Chain Reorganizations)

### 4.1 The Problem

At the tip of the chain, blocks can be replaced by competing blocks during reorganizations. When an unwind occurs, the node must:

1. Remove all state changes from the unwound blocks
2. Restore the tree root to its state at the unwind target block
3. Resume appending from the new chain tip

The QMDB design addresses this in the architecture doc under "Crash recovery mechanism": the flusher can truncate away partially-written entries. The append-only nature of the tree makes this straightforward.

### 4.2 Unwind Strategy

The qmtree is append-only and indexed by sequential txnum. Unwinding block N means reverting to `max_txnum(N-1)`. This requires:

1. **Determine the revert point**: `revertToTxNum = max_txnum(targetBlock)`
2. **Truncate the entry storage**: Remove all entries with `txNum > revertToTxNum`
3. **Truncate the twig file**: Discard any twigs beyond `revertToTxNum >> 11`
4. **Rebuild the youngest twig**: Re-read remaining entries for the youngest twig from storage and recompute its Merkle tree
5. **Recompute upper tree**: Sync upper nodes from the now-youngest twig upward

The key property: **all data for txnums <= revertToTxNum is untouched**. The tree root at the revert point is exactly what it was when that block was originally committed.

### 4.3 Implementation

```go
func (t *Tree) UnwindTo(targetSN uint64) error {
    // 1. Calculate new youngest twig
    newYoungestTwig := targetSN >> TWIG_SHIFT
    posInTwig := targetSN & TWIG_MASK

    // 2. Truncate entry storage beyond targetSN
    t.entryStorage.Truncate(entryEndPos(targetSN))

    // 3. Truncate twig file — remove twigs beyond newYoungestTwig
    t.twigStorage.Truncate(twigFilePos(newYoungestTwig + 1))

    // 4. Reset youngest twig state
    t.youngestTwigId = newYoungestTwig
    // Rebuild the partial twig's Merkle tree from stored entries
    t.rebuildYoungestTwig(targetSN)

    // 5. Prune upper tree nodes beyond newYoungestTwig
    t.upperTree.PruneAbove(newYoungestTwig)

    // 6. Resync upper tree
    t.SyncUpperNodes()

    return nil
}
```

### 4.4 Unwind Depth

The PoC must support unwinding up to **500 blocks** from the tip. On Ethereum mainnet, 500 blocks contains roughly 50,000-100,000 transactions, which spans ~25-50 twigs. This is a modest truncation that completes in milliseconds since:

- Entry/twig file truncation is O(1) (file system operation)
- Upper tree node removal is O(depth) ~ O(20)
- Youngest twig rebuild reads at most 2048 entries from disk

### 4.5 Consistency Guarantees

The unwind operation is atomic in the same sense as the QMDB flusher's crash recovery: if interrupted, the stored `max_txnum` in metadata determines the authoritative state, and recovery truncates any data beyond that point.

The implementation persists:
- `lastCommittedTxNum` — the highest fully-committed txnum
- `lastCommittedBlock` — the corresponding block number
- `edgeNodes` — boundary nodes needed for proof construction after pruning (per QMDB architecture, "The edge nodes" section)

## 5. Keyset and Indexing

### 5.1 The keyset question

To verify a state proof, the verifier needs to know **which txnum** last modified a given key. This is the "keyset" — a mapping from `(domain, key)` to `txnum`.

Options explored:

| Approach | Storage | Trust Model |
|---|---|---|
| **A: External inverted index** | Already exists in Erigon (HistoryRange) | Trusted node |
| **B: In-leaf keyset** | Encoded in leaf preimage | Self-contained but large |
| **C: Separate commitment** | Second Merkle tree over key->txnum | Trustless but complex |

The current implementation uses **Approach A** for key lookup (Erigon's existing
inverted index, which maps `(domain, key) -> [txnums]` efficiently) and
**Approach C** for committed exclusion proofs (the `KeyIndex` described below).

### 5.2 Exclusion proofs

An exclusion proof answers: "no transaction between txnum T+1 and max_txnum(B)
wrote to key K". Without this, a verifier cannot confirm that txnum T is the
_latest_ write to key K before block B, so an inclusion proof alone is
insufficient for state currency.

**KeyIndex** — the committed key-set (`keyindex.go`):

The `KeyIndex` maintains a sorted array of `(keyHash, latestTxNum)` pairs where
`keyHash = keccak256(domain_byte || key_bytes)`. After each transaction,
`Tracker.NotifyKeyWrites` updates the index with all keys written by that
transaction. After each block, `KeyIndexRoot()` returns the Merkle root over the
sorted entries.

Key design choices:
- **keyHash = keccak256(domain_byte || key_bytes)** — uniform 32-byte identifier
  for sorting and proof generation across all domains (accounts, storage, code).
- **Binary Merkle tree** — leaves are `keccak256(keyHash[32] || txNum_LE8[8])`,
  internal nodes are `keccak256(left[32] || right[32])`. The root plus leaf count
  commits to the complete key set.
- **Adjacency proof** — to prove key K was never written, the proof contains the
  two adjacent entries (prev, next) in sorted order with their Merkle paths and
  indices. The verifier checks `prev.keyHash < K < next.keyHash` and that
  `nextIndex == prevIndex + 1` (they are consecutive in the sorted array).

**ExclusionProof** structure (`ExclusionProof` in `keyindex.go`):

```
// Inclusion (Exists=true):
KeyHash, TxNum, LeafIndex, Path[]  →  verifies leaf is in KeyIndex at Root

// Non-membership (Exists=false):
PrevEntry{KeyHash, TxNum}, PrevIndex, PrevPath[]
NextEntry{KeyHash, TxNum}, NextIndex, NextPath[]  →  NextIndex == PrevIndex+1
```

Verification: `ExclusionProof.Verify()` checks both Merkle paths against Root,
the key ordering bounds, and adjacency.

**Unwind**: The `KeyIndex` does not currently support incremental unwind. On a
reorg the index must be rebuilt from genesis for the affected keys. This is
acceptable because reorgs are rare and the rebuild is bounded by the total number
of unique keys written (not all transactions).

## 6. RPC Namespace

The `qm_` RPC namespace is implemented and wired into rpcdaemon. For complete
type signatures and request/response formats see [protocol-spec.md](protocol-spec.md).

| Method | Description |
|---|---|
| `qm_getProof(address, block)` | Inclusion proof for the last tx touching `address` |
| `qm_getWitness(block)` | Range witness for all txs in a block |
| `qm_getTxWitness(block, txIndex)` | Witness for a single tx by index |
| `qm_getAccountStateProof(address, slots[], block)` | State values + qmtree witness |
| `qm_getTxStateProof(address, slots[], block, txIndex)` | Same, pinned to a specific tx |
| `qm_getLatestStateProof(address, slots[])` | State at chain tip + latest witness |
| `qm_call(callArgs, block)` | Provable `eth_call` — execution result + combined witness set |
| `qm_callProof(callArgs, block)` | Compact twig-grouped proof + 32-byte Merkle digest |
| `qm_verifyProof(proofBytes)` | Stateless binary proof verification |
| `qm_verifyWitness(witnessBytes)` | Stateless binary witness verification |

`qm_callProof` deduplicates upper-tree peer hashes across leaves in the same
twig (~19–37% smaller than individual witnesses, depending on access pattern).
The `digest` field is a 32-byte SSZ-inspired `hash_tree_root` commitment over
all proof fields; a verifier can store the digest and later check that a full
proof has not been tampered with.

## 7. Roadmap

See [README.md](README.md) for the full roadmap and task breakdown.
