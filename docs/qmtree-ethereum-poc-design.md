# QMTree Ethereum State Commitment PoC — Design Document

## 1. Introduction

This document describes a proof-of-concept (PoC) for using the [QMDB Twig Merkle Tree](https://github.com/LayerZero-Labs/qmdb/tree/main/docs) as an alternative state commitment scheme for Ethereum. The goal is to replace the keccak256-hashed hex patricia trie with a sequential, append-only binary Merkle tree indexed by Erigon's global transaction number (`txnum`).

### 1.1 References

- [QMDB Design — General Ideas](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/design.md) — covers entry structure, twig Merkle trees, ActiveBits, pruning, and exclusion proofs
- [QMDB Architecture](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/architecture.md) — covers the prefetcher-updater-flusher pipeline, HPFile, TwigFile, EntryFile, B-tree indexer, edge nodes, and batch sync
- [Go port of qmtree](../execution/commitment/qmtree/) — the tree, twig, proof, and hpfile implementations used by this PoC

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

The QMDB design (Section 1.0) defines entries as `Entry := (Height, Key, Value, SerialNum)` where SerialNum is a monotonically increasing index. Erigon already maintains an equivalent: the global **transaction number** (`txnum`), a sequential counter that increments for every transaction across all blocks.

```
Block 0:   txnums 0..4
Block 1:   txnums 5..12
Block 2:   txnums 13..25
...
```

Each txnum maps 1:1 to a qmtree serial number. This gives us:

- **Twig ID** = `txnum >> 11` (each twig holds 2048 leaves)
- **Position in twig** = `txnum & 2047`
- **Block-level root** = `tree.Root()` after appending the block's final txnum

### 2.2 Leaf Hash Construction

Each leaf at serial number `txnum` contains a deterministic hash of that transaction's state changes. The leaf hash is computed as:

```
leaf_hash = SHA256(canonical_encoding(sorted_changes))
```

Where `sorted_changes` is the lexicographically sorted list of `(domain, key, value)` tuples modified by that transaction. The domains are AccountsDomain and StorageDomain (matching Erigon's existing domain structure).

**Encoding format** (per change):
```
[1 byte: domain_id] [2 bytes: key_length] [key_bytes] [4 bytes: value_length] [value_bytes]
```

Changes are sorted by `(domain_id, key_bytes)` before hashing. This ensures determinism regardless of the order state changes are produced during execution.

**Empty transactions** (no state changes) produce a leaf hash of `SHA256([])` — the hash of the empty byte sequence.

### 2.3 Tree Structure

The tree follows the QMDB twig Merkle tree design (Sections 2.0 and 4.0):

```
                    Root
                   /    \
            Upper Tree (levels 13+)
           /          \
     TwigRoot_0     TwigRoot_1    ...
      /     \         /     \
  LeftRoot  ABRoot  LeftRoot  ABRoot    (level 12)
   /    \    / \
 ...    ...          (levels 0-11: 2048-leaf binary Merkle tree)
```

Each twig contains:
- **Left subtree** (levels 0-11): Binary Merkle tree over 2048 leaf hashes
- **Right subtree** (levels 8-11): ActiveBits tree (2048 bits indicating which entries are current)
- **TwigRoot** (level 12): `hash(11, leftRoot, activeBitsRoot)`

The upper tree (levels 13+) combines twig roots into the final tree root.

### 2.4 Block-Level and Tx-Level Hashes

The PoC computes two granularities of commitment:

- **Tx-level root**: `tree.Root()` after appending leaf for txnum T. This commits to all state changes through transaction T.
- **Block-level root**: `tree.Root()` after appending the final leaf of block N (at `max_txnum(N)`). This is the candidate "state root" for block N.

Both are computed and logged for analysis. The block-level root is the one that would replace the current hex patricia state root in block headers.

### 2.5 ActiveBits and State Currency

Per QMDB design Section 3.0, each leaf has an ActiveBit indicating whether the entry represents the **current** value for its key. When a key is updated by a later transaction, the earlier entry's ActiveBit is cleared.

In the PoC, ActiveBits are used for:
1. **State currency proofs** — proving that a particular state change is the most recent for a given key
2. **Pruning** — once all entries in a twig are inactive, the twig can be evicted from memory (Section 6.0)

For the initial PoC, we set ActiveBits for all appended entries and do not implement deactivation or pruning. This is Phase 1 functionality; full ActiveBit management is a follow-up.

## 3. Proofs and Witnesses

### 3.1 QMTree Proof Structure

A qmtree proof for serial number `sn` (see `proof.go`) consists of:

| Component | Size | Purpose |
|---|---|---|
| `LeftOfTwig[11]` | 11 x 32 bytes | Sibling hashes through the 2048-leaf Merkle tree |
| `RightOfTwig[3]` | 3 x 32 bytes | Sibling hashes through the ActiveBits tree |
| `UpperPath[N]` | N x 32 bytes | Sibling hashes through the upper tree to root |
| `SerialNum` | 8 bytes | The txnum being proven |
| `Root` | 32 bytes | The tree root |

Total proof size: `(16 + N) x 32` bytes where N = `log2(num_twigs)`. For 1 billion transactions (~10 years of Ethereum mainnet), N ~ 19, giving **~1120 bytes** per proof.

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
2. **Truncate the entry storage**: Remove all entries with `serialNum > revertToTxNum`
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

For the PoC, we persist:
- `lastCommittedTxNum` — the highest fully-committed txnum
- `lastCommittedBlock` — the corresponding block number
- `edgeNodes` — boundary nodes needed for proof construction after pruning (per QMDB architecture, "The edge nodes" section)

## 5. PoC Architecture

### 5.1 Directory Structure

```
execution/commitment/qmtree/poc/
    poc.go              # Main runner (models backtester pattern)
    state_entry.go      # StateEntry: Entry implementation for state changes
    unwind.go           # Unwind/truncation logic
    proof.go            # Account/storage proof generation and verification
    poc_test.go         # Tests
    README.md           # Usage and results
```

### 5.2 Data Flow

```
Hoodi chain (read-only datadir)
    |
    v
TxNumsReader: block -> [fromTxNum, toTxNum]
    |
    v
HistoryRange(domain, fromTxNum, toTxNum):
    yields (key, value) per txnum
    |
    v
StateEntry{txNum, hash, changes}:
    sorted, hashed per tx
    |
    v
tree.AppendEntry(entry):
    leaf inserted at serial number = txnum
    |
    v
tree.Root():
    block-level commitment after each block
    |
    v
Compare / analyze / generate proofs
```

### 5.3 Interaction with Existing Backtester

The PoC reuses the backtester's infrastructure:

- `TxNumsReader` for block-to-txnum mapping
- `HistoryRange` for iterating state changes per block
- `SharedDomains` for state access
- Block header reader for canonical root comparison (values will differ but structure is validated)

The PoC does **not** replace the existing commitment system. It runs alongside it, computing qmtree roots for analysis while the hex patricia trie remains authoritative.

## 6. Keyset and Indexing

### 6.1 The Keyset Question

To verify a state proof, the verifier needs to know **which txnum** last modified a given key. This is the "keyset" — a mapping from `(domain, key)` to `txnum`.

Options explored:

| Approach | Storage | Trust Model |
|---|---|---|
| **A: External inverted index** | Already exists in Erigon (HistoryRange) | Trusted node |
| **B: In-leaf keyset** | Encoded in leaf preimage | Self-contained but large |
| **C: Separate commitment** | Second Merkle tree over key->txnum | Trustless but complex |

The PoC uses **Approach A** — Erigon's existing inverted index. This is sufficient for the PoC because:
- The index already maps `(domain, key) -> [txnums]` efficiently
- No new storage is needed
- The focus is on validating the tree structure and hash computation, not building a production-grade light client protocol

Approach C (a committed key index) is noted as future work for trustless proofs. The QMDB design addresses this via the B-tree indexer (architecture doc, "B-tree" section) which maps key hashes to entry offsets.

### 6.2 QMDB's Exclusion Proof Extension

The QMDB design (Section 7.0) extends entries with `NextKeyHash` to enable exclusion proofs — proving that no key exists between two adjacent keys in hash order. This is analogous to what the hex patricia trie provides via its branch/extension node structure.

The PoC does not implement exclusion proofs, but the design accommodates them as a future extension by adding `NextKeyHash` to the `StateEntry` structure.

## 7. Future Work (Beyond PoC)

1. **ActiveBit management** — deactivate old entries when keys are updated; implement compaction (QMDB Section 5.0)
2. **Exclusion proofs** — add `NextKeyHash` field for negative proofs (QMDB Section 7.0)
3. **Inclusion proofs at arbitrary heights** — add `LastHeight` and `DeactivatedSerialNum` fields (QMDB Section 8.0)
4. **Head pruning** — prune inactive twigs from SSD using HPFile (QMDB Section 10.0)
5. **Prefetcher-updater-flusher pipeline** — pipelined execution for production throughput (QMDB architecture, "Timing of the pipeline")
6. **HybridIndexer** — SSD+DRAM indexing for production memory efficiency (QMDB architecture, "HybridIndexer")
7. **eth_getProof compatibility** — adapter that translates qmtree proofs to the format expected by existing tooling
8. **Parallel twig sync** — leverage the 4-shard design for parallel hash computation (tree.go TWIG_SHARD_COUNT=4)
