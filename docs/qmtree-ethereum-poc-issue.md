# [Experiment] QMTree: Append-Only Merkle Tree for Ethereum State Commitment

## Summary

Build a proof-of-concept that uses the [QMDB Twig Merkle Tree](https://github.com/LayerZero-Labs/qmdb/tree/main/docs) as an alternative state commitment scheme for Ethereum, replacing the keccak256-hashed hex patricia trie with a sequential, append-only binary Merkle tree indexed by Erigon's global transaction number (`txnum`).

The PoC will:
- Compute block-level and tx-level state roots using qmtree over real Ethereum data (Hoodi testnet)
- Generate and verify inclusion proofs equivalent to the current trie's `eth_getProof`
- Support chain tip unwinding (reorgs) up to 500 blocks
- Compare performance, proof size, and storage costs against the existing hex patricia trie

**Design document**: [docs/qmtree-ethereum-poc-design.md](../docs/qmtree-ethereum-poc-design.md)

## Background

### The Current Model

Erigon computes state roots using a hex patricia trie (radix-16, keccak256-hashed keys). Each block's state root is the trie root after applying all transactions. Proof generation (`eth_getProof`) traverses the trie from root to leaf, collecting branch/extension nodes along the path.

Costs:
- O(log16 n) random reads per key update (~7-8 trie nodes)
- Branch nodes are RLP-encoded with up to 16 child hashes (~512+ bytes each)
- Each contract has a nested storage trie requiring separate root computation
- Proofs are typically 3-5 KB

### The QMDB Model

The [QMDB design](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/design.md) introduces a fundamentally different approach:

- **Entries** are `(Height, Key, Value, SerialNum)` tuples appended sequentially ([design.md Section 1.0](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/design.md))
- Entries are grouped into **twigs** of 2048 leaves, each with a binary Merkle tree ([design.md Section 2.0](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/design.md))
- **ActiveBits** track which entries represent current state ([design.md Section 3.0](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/design.md))
- The **twig Merkle tree** combines leaf hashes and ActiveBits into twig roots, then upper-level nodes form the final root ([design.md Section 4.0](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/design.md))
- Storage uses **head-prunable files** (HPFile) for efficient old data removal ([architecture.md](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/architecture.md))

Key advantages:
- O(1) append per transaction (no trie traversal)
- Compact binary Merkle proofs: ~(14 + log2(twigs)) x 32 bytes (~1 KB vs ~4 KB)
- Flat namespace: accounts and storage in one tree (no nested tries)
- Natural pruning model via twig eviction

### Go Port

The qmtree Go port lives at `execution/commitment/qmtree/` and implements the core tree, twig, proof, twigfile, and hpfile components from the QMDB design. This PoC builds on that port.

## Design

### Mapping TxNum to QMTree

Erigon's global `txnum` maps directly to qmtree serial numbers:

| Concept | Erigon | QMTree |
|---|---|---|
| Sequential index | `txnum` | `SerialNum` |
| Group | Block | Twig (2048 entries) |
| Group ID | `blockNum` | `txnum >> 11` |
| Commitment point | Block state root | `tree.Root()` after block's last txnum |

### Leaf Hash

Each leaf at `txnum` is the SHA256 hash of the transaction's state changes:

```
leaf_hash = SHA256(canonical_encoding(sorted[(domain, key, value)...]))
```

Changes are sorted by `(domain_id, key_bytes)` for determinism. The sorted change list is the leaf's **preimage** and serves as witness data for proof verification.

### Proofs

To prove "account A has value V at block B":

1. Query the inverted index for the latest txnum that modified A at <= `max_txnum(B)`
2. Retrieve the leaf preimage for that txnum
3. Generate `tree.GetProof(txnum)` — binary Merkle path to root
4. Verify: check proof, check preimage hashes to leaf, check A->V is in preimage

Proof size: ~1 KB for 1 billion transactions (vs ~4 KB for hex patricia).

### Unwind (Reorg Handling)

The append-only tree supports efficient unwinding by truncation:

1. Determine `revertToTxNum = max_txnum(targetBlock)`
2. Truncate entry and twig storage beyond that point
3. Rebuild the youngest twig's Merkle tree from remaining entries
4. Prune and resync upper tree nodes

This mirrors the QMDB flusher's crash recovery mechanism ([architecture.md, "Crash recovery mechanism"](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/architecture.md)). The PoC must support unwinding up to **500 blocks** from the tip (~25-50 twigs), which completes in milliseconds since file truncation is O(1) and upper tree resync is O(depth).

## Implementation Plan

### Phase 1 — Leaf Construction & Tree Building

Build the qmtree from Hoodi chain historical data using the backtester pattern.

**Work:**
- Implement `StateEntry` (qmtree `Entry` interface)
- Implement `buildLeafHash()` — iterate `HistoryRange` for both domains, sort changes, SHA256
- Implement `PocRunner` — opens Hoodi datadir, iterates blocks, appends entries, records roots
- Store tree in temp directory via hpfile + twigfile

**Tests:**
- `TestStateEntryHashDeterminism` — same changes in different order produce same hash
- `TestStateEntryHashUniqueness` — different values produce different hashes
- `TestSingleBlockTreeBuild` — one block, verify non-zero deterministic root
- `TestMultiBlockTreeBuild` — 10 blocks, verify roots change per block and are deterministic

### Phase 2 — Block & Tx Level Hash Comparison

Compute and log both granularities for analysis.

**Work:**
- Capture `tree.Root()` after each tx append (tx-level) and after each block's final tx (block-level)
- Output CSV: `block_num, tx_num, tx_level_root, block_level_root, num_changes, elapsed_ns`
- Summary stats: avg changes/tx, tree depth, total leaves, throughput

**Tests:**
- `TestTxLevelRootsProgression` — each append produces a different root
- `TestBlockLevelRootDeterminism` — same range produces identical roots on re-run
- `TestEmptyBlock` — block with no state changes produces valid root

### Phase 3 — Unwind Support

Implement and test chain tip unwinding up to 500 blocks.

**Work:**
- Implement `Tree.UnwindTo(targetSN)` — truncate entries, twigs, rebuild youngest twig, resync upper tree
- Persist `lastCommittedTxNum`, `lastCommittedBlock`, and edge nodes for recovery
- Verify that `tree.Root()` after unwind matches the root at the target block

**Tests:**
- `TestUnwindSingleBlock` — append 2 blocks, unwind 1, verify root matches block 1's root exactly
- `TestUnwindMultipleBlocks` — append 100 blocks, unwind 50, verify root matches block 50's root
- `TestUnwindMaxDepth` — append 600 blocks, unwind 500, verify root matches block 100's root
- `TestUnwindAndReappend` — unwind then append new blocks, verify tree integrity
- `TestUnwindTwigBoundary` — unwind to exact twig boundary (txnum divisible by 2048)
- `TestUnwindMidTwig` — unwind to middle of a twig, verify partial twig rebuild
- `TestUnwindProofConsistency` — generate proof before unwind, unwind, re-append same data, verify proof still valid
- `TestUnwindIdempotent` — unwind to same point twice, verify same result
- `TestUnwindRecovery` — simulate crash during unwind (truncate metadata but not files), verify recovery

### Phase 4 — Proofs & Witnesses

Generate and verify proofs equivalent to the current trie.

**Work:**
- Implement `GenerateAccountProof(tree, tx, address, blockNum)` and `GenerateStorageProof`
- Implement `VerifyAccountProof(proof, address, value, blockRoot)` and `VerifyStorageProof`
- Proof structure: `QMProof{TreeProof, LeafPreimage, TxNum, BlockRoot}`

**Tests:**
- `TestAccountProofRoundtrip` — generate + verify for known account
- `TestStorageProofRoundtrip` — generate + verify for storage slot
- `TestProofTampered` — modify preimage value, verify proof fails
- `TestProofAtDifferentBlocks` — same account, different blocks, both valid
- `TestProofAfterUnwind` — generate proof, unwind past it, re-append, verify new proof
- `TestProofSize` — log sizes and compare to hex patricia

### Phase 5 — Comparative Analysis

Run against Hoodi and compare with existing commitment.

**Work:**
- Run PoC and backtester over same block range
- Compare: root computation time, proof generation time, proof size, disk usage, memory
- Output comparison report (CSV/HTML)

**Tests:**
- `TestHoodiBlockRange` — integration: build tree for 1000 blocks on Hoodi, all roots deterministic
- `TestProofSizeComparison` — 100 random accounts, both proof types, log comparison
- `TestBuildPerformance` — benchmark: throughput for 1000 blocks

## Key Differences from Current Trie

| Aspect | Hex Patricia (current) | QMTree (PoC) |
|---|---|---|
| Index key | keccak256(address) | txnum (sequential) |
| Leaf content | RLP(nonce, balance, storageRoot, codeHash) | SHA256(sorted state changes) |
| Root semantics | Current state snapshot | History of all changes to this point |
| Proof target | "Account X has value V" | "Tx T committed changes including X->V" |
| Update cost | O(log n) trie traversal | O(1) append |
| Proof size | ~3-5 KB | ~1 KB |
| Storage | Branch nodes in CommitmentDomain | HPFile segments + twigfiles |
| Unwind | Reverse state changes in trie | Truncate entries + twig files |
| Nested storage | Separate trie per contract | Flat — one tree for all |

## Open Questions

1. **Per-tx vs per-block leaves** — Should we use one leaf per tx or one per block? Per-tx gives finer granularity but more leaves. The PoC will measure both.
2. **Keyset commitment** — Is the existing inverted index sufficient for proofs, or do we need a committed key->txnum mapping for trustless verification?
3. **eth_getProof compatibility** — Can qmtree proofs be adapted to the existing JSON-RPC proof format, or does the API need extension?
4. **Exclusion proofs** — The QMDB design supports them via `NextKeyHash` ([design.md Section 7.0](https://github.com/LayerZero-Labs/qmdb/blob/main/docs/design.md)). Should the PoC include this?
5. **State expiry** — ActiveBits + twig pruning naturally support state expiry. What's the interaction with EIP-7736 (Verkle) proposals?

## Running

```bash
# Build and run against Hoodi datadir
go run ./execution/commitment/qmtree/poc \
  --datadir /path/to/hoodi \
  --from 1 --to 10000 \
  --output-dir ./qmtree-poc-results

# Run tests
go test ./execution/commitment/qmtree/poc/... -v -count=1

# Run unwind tests specifically
go test ./execution/commitment/qmtree/poc/... -run TestUnwind -v
```

## Labels

`experiment`, `commitment`, `qmtree`
