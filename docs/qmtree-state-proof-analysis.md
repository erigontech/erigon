# QMTree State Proof Analysis, Design & Plan

## Context

The qmtree PoC has demonstrated feasibility: ~21x smaller than the existing
commitment domain, append-only O(1) writes, ~1120-byte Merkle proofs. The
tree is built from four-component leaves:

```
leaf = keccak256(preStateHash || stateChangeHash || transitionHash || previousLeafHash)
```

We now need to determine whether this tree can serve as a **complete state
commitment** — replacing the MPT root — and what RPC interfaces are needed.

**Deployment strategy**: Rollup-first. The qmtree replaces MPT in a rollup
where we control the state commitment scheme. Mainnet adoption follows via
two related EIPs: (1) linearisation / transaction numbering, (2) the new
tree as state commitment. Both EIPs are prerequisites for each other.

---

## Question 1: Is the State Root Sufficient?

**Yes, with caveats around negative proofs and key lookup.**

### What the MPT root provides today

| Capability | MPT | QMTree |
|---|---|---|
| Positive inclusion proof ("key K has value V") | Yes — trie path | Yes — Merkle path to leaf containing (K,V) in stateChangeHash |
| Negative/exclusion proof ("key K does not exist") | Yes — branch/extension | **Not yet** — needs NextKeyHash (QMDB §7.0) |
| Latest-value proof ("V is *current* value of K") | Implicit — stores only latest | ActiveBit proof + inverted index |
| Block-level binding | Root in block header | Root after final txnum of block |
| Light client sync | Yes (snap sync) | Yes — proof size ~3-4x smaller |

### Gaps to close

1. **Negative proofs** — needs NextKeyHash extension. Medium priority — most
   rollup consumers don't need negative proofs initially, but full
   eth_getProof compat requires it.

2. **Key-to-txnum lookup** — inverted index (trusted) works now. Committed
   key index needed for trustless light clients. Low priority for rollup.

3. **ActiveBit management** — must deactivate old entries when keys update.
   High priority — without this, stale leaves look current.

### Verdict

The root is sufficient for a rollup where we control the commitment scheme.
The three gaps are engineering work, not fundamental blockers.

---

## Question 2: Key Validity Proof

Given (account key, block number), prove inclusion:

1. **Resolve txnum**: inverted index → latest txnum T modifying key before block end. O(1).
2. **Get leaf data**: LeafData for SN=T. O(1).
3. **Get Merkle proof**: `tree.GetProof(T)` — 14-20 levels. O(log N).
4. **Reconstruct SMT**: load all writes for txnum T from history files; build
   `SMT(writes)`; confirm `SMT.Root() == stateChangeHash`. O(writes in tx).
5. **Get SMT branch**: `SMT.GetBranch(keccak256(domain || key))`. O(log N_keys).
6. **Verify**: recompute leaf hash, walk qmtree path, compare root; walk SMT
   branch, confirm key→value is included.

Full proof = qmtree witness (~1.1 KB) + SMT branch (~O(log N_keys) × 32B).
For a tx touching 100 keys, SMT depth ~7, branch ~224B. Total ~1.3 KB.

| Metric | QMTree + SMT | MPT |
|---|---|---|
| Proof generation | ~100-500 μs | ~1-50 ms |
| Proof size | ~1.3 KB | ~3-5 KB |
| Verification | ~20-50 μs | ~100-500 μs |

*Note: Step 0 of the implementation plan must complete before SMT proofs work.
Until then, proof generation requires revealing the full read/write set.*

---

## Question 3: Pre/Post State Changeset Retrieval

The leaf embeds preStateHash and stateChangeHash — cryptographic commitments
over the actual changesets. Retrieval:

1. Query Erigon's existing history tables (HistoryRange) for the txnum
2. Hash the returned data, verify against the leaf's preStateHash / stateChangeHash
3. Return changesets + Merkle proof

No new storage needed — reuse existing history. The qmtree hashes bind it.

---

## Question 3b: Proving an Individual Key (The Flat-Hash Limitation)

**Current limitation**: `preStateHash` and `stateChangeHash` are currently
computed as `keccak256(sorted_reads)` and `keccak256(sorted_writes)` — flat
hashes over the entire read/write set for a transaction. This means:

- To prove "address A had value V before this tx", you must reveal the
  **entire** read set for the tx (potentially hundreds of (key, value) pairs)
- There is no way to produce a short proof for a single key
- This is a disclosure and scalability problem: high-activity txs have large
  read sets that must be fully transmitted

**Fix: replace flat hashes with Sparse Merkle Tree (SMT) roots**

Instead of `keccak256(sorted_reads)`, compute the SMT root over the read set:

```
preStateHash  = SMT_root({ (domain, key) → value } for all reads)
stateChangeHash = SMT_root({ (domain, key) → value } for all writes)
```

A state proof for a specific key then becomes:
1. qmtree witness for the tx (proves the leaf is committed in the tree)
2. SMT Merkle branch within `preStateHash` for the requested key

This gives an O(log N_keys) proof for any single key, without revealing
the full read/write set.

### SMT Storage Strategy

Building a per-tx SMT and storing it permanently would be expensive
(~1-2 KB per tx × 2 billion mainnet txs = 2-4 TB). Instead:

**On-demand reconstruction (primary approach)**:
- The SMT is built on-the-fly per RPC request from history files
- On an archival node, history files already store every key's value at
  every txnum — they are the source of truth
- Flow: `GetAccountStateProof(address, keys, block)` →
  1. Inverted index → txnum T touching address near block
  2. `HistoryRange(T)` → all reads/writes for tx T
  3. Build SMT in memory from those reads/writes
  4. Verify `SMT.Root() == witness.preStateHash` (proves SMT matches leaf)
  5. Return: qmtree witness + SMT branch for requested key
- No new permanent storage required

**Optional persistent cache**:
- Store only the 64 bytes of SMT roots per tx (preStateHash + stateChangeHash)
  alongside the entry files — this is already what we do today
- Full per-tx SMTs (~1-2 KB each) are only needed transiently per RPC call
- Hot-path caching (LRU of recent block SMTs) covers most use cases

**This approach aligns with Erigon's existing architecture**: history files are
already queryable per txnum, and archival nodes are the ones expected to serve
detailed state proofs. Non-archival nodes without history files serve the
qmtree witness only; archival nodes additionally provide the SMT branch.

### Required Changes

To implement this, `preStateHash` and `stateChangeHash` must change from
flat keccak256 to Merkle roots. This is a **breaking format change** to the
entry file — all existing qmtree data would need to be regenerated.

### Canonical Format (implemented in `execution/exec/qmtree_reader.go`)

The Merkle root uses **`DeriveSha`** — the same Merkle Patricia Trie format
Ethereum uses for transaction roots, receipt roots, and withdrawal roots.
This ensures format consistency across all Ethereum commitment structures and
allows existing Ethereum tooling to verify state change proofs.

**Ordering (Block Access List / EIP-7928 rules):**

State operations are sorted before building the trie, using the same strict
ordering defined by EIP-7928 Block Access Lists:
- Primary sort: domain (AccountsDomain=0 < StorageDomain=1 < CodeDomain=2)
- Secondary sort: key bytes ascending
  - AccountsDomain key = 20-byte address
  - StorageDomain key = 20-byte address || 32-byte slot (52 bytes total)
  - CodeDomain key = 20-byte address
- Result: addresses strictly increasing; within each address, storage slots
  strictly increasing — exactly matching BAL canonical ordering

**Leaf encoding:**

Each item in the trie is RLP-encoded as a 3-element list:
```
RLP([domain_uint, key_bytes, value_bytes])
```
- `domain_uint`: RLP uint (0 = accounts, 1 = storage, 2 = code)
- `key_bytes`: RLP byte array — address (20B) or address+slot (52B)
- `value_bytes`: RLP byte array — serialised account, storage value, or code
                 (nil/empty bytes for deletions)

**Trie keys:** sequential integers 0, 1, 2 … (same as transactions/receipts).

**Empty set:** returns `trie.EmptyRoot`
(`0x56e81f171bcc55a6ff8345e692c0f86e5b48e01b996cadc001622fb5e363b421`)
rather than the zero hash — consistent with DeriveSha on an empty list.

**Reproduction:** any implementation that:
1. Collects (domain, key, value) triples for all reads (preStateHash) or
   writes (stateChangeHash) executed by a transaction
2. Sorts them by the BAL ordering above
3. RLP-encodes each as `[domain_uint, key_bytes, value_bytes]`
4. Passes them to `DeriveSha` (MPT with sequential index keys)

will produce the same root as stored in the qmtree entry file.

---

## Question 4: RPC Re-Execution Proof

Yes. The leaf already commits to the full execution:
- **preStateHash** — what was read (inputs)
- **transitionHash** (embeds execHash) — what process ran
- **stateChangeHash** — what was written (outputs)

A verifier checks: given inputs + process + outputs, the leaf is in the tree
at the root committed in the block header. This covers eth_call,
debug_traceTransaction, and receipt verification.

---

## Question 5: RPC APIs

### `qm_getProof(address, storageKeys[], blockNumberOrHash)`

State inclusion proof. Returns:

```json
{
    "address": "0x...",
    "balance": "0x...", "nonce": "0x...", "codeHash": "0x...",
    "storageValues": {"0xkey1": "0xval1"},
    "txNum": 12345,
    "leafData": {
        "preStateHash": "0x...",
        "stateChangeHash": "0x...",
        "transitionHash": "0x...",
        "previousLeafHash": "0x..."
    },
    "merkleProof": "0x...",
    "qmtreeRoot": "0x...",
    "stateChangePreimage": [
        {"domain": 0, "key": "0x...", "value": "0x..."}
    ]
}
```

Verification: recompute leaf → walk Merkle path → compare root → hash
preimage → compare stateChangeHash → find (address, value) in preimage.

### `qm_getWitness(blockNumberOrHash)` / `qm_getTxWitness(block, txIndex)`

Execution witness using RangeWitness. Only 2 Merkle proofs for an entire
block — the hash chain (previousLeafHash) links intermediate leaves.

```json
{
    "blockNumber": 12345,
    "qmtreeRoot": "0x...",
    "rangeWitness": {
        "firstProof": "0x...",
        "lastProof": "0x...",
        "leaves": [{
            "serialNum": 100,
            "txHash": "0x...",
            "preStateHash": "0x...",
            "stateChangeHash": "0x...",
            "transitionHash": "0x...",
            "previousLeafHash": "0x...",
            "preState": [...],
            "stateChanges": [...]
        }]
    }
}
```

---

## EIP Strategy (Rollup → Mainnet)

Two related EIPs, both prerequisites for mainnet adoption:

1. **EIP: Transaction Linearisation** — formalise the global transaction
   numbering (txnum) that Erigon already uses internally. Every transaction
   across all blocks gets a monotonically increasing serial number. This is
   the foundation the qmtree indexes on.

2. **EIP: QMTree State Commitment** — replace MPT root with qmtree root in
   block headers. Depends on EIP-1 for the serial numbering. Specifies leaf
   format, hash function, twig structure, proof format.

**Rollup-first** validates both EIPs in production before proposing for L1.

---

## Implementation Plan

This is PoC/experimental code on the qmdb branch. No production constraints.

### Step 0: SMT hash format (2-3 days) ⚠️ breaking change

This must happen before any other step — it changes the on-disk entry format
and invalidates all existing qmtree data. After this change, the n1 dataset
must be regenerated.

**What changes:**
- `qmtree_reader.go`: replace `hashStateOps()` flat-hash with SMT construction.
  `hashingReader.Finalize()` must return `SMT_root(reads)` not `keccak256(all_reads)`.
- `qmtree_writer.go`: same change for writes. `hashingWriter.Finalize()` returns
  `SMT_root(writes)`.
- SMT implementation: a simple binary sparse Merkle tree over 32-byte keys with
  keccak256 internal nodes. Can be a lightweight in-package implementation — no
  need for a full library. Keyed by `keccak256(domain_byte || key_bytes)`.
- `GetAccountStateProof` (and related RPCs): extend to accept a list of storage
  keys, reconstruct the per-tx SMT from `HistoryRange(txnum)`, verify
  `SMT.Root() == leaf.preStateHash`, and return the SMT branch.

**Verification:** after regenerating, `TestTracker_LoadFromDisk` plus a new
`TestSMT_RoundTrip` that checks SMT_root(reads) matches the stored leaf.

### Step 1: ActiveBit deactivation (1-2 days)

- Add key→lastSN tracking to Tracker
- On AppendLeaf, if key was previously written at SN_old, call DeactiveEntry(SN_old)
- Wire through the hashingWriter which already sees all writes

### Step 2: Persist leafData to disk (1 day)

- Add a leafData file in qmtree/ alongside entries/twigs
- Format: `[8B sn][32B preState][32B stateChange][32B transition][32B prevLeaf]` = 136 bytes/leaf
- Load on recovery, truncate on unwind

### Step 3: `qm_getProof` RPC (1-2 days)

- Add `qm` namespace to the RPC daemon
- Implement proof generation:
  - Inverted index → txnum
  - tracker.GetWitness(txnum) → Merkle proof
  - HistoryRange → preimage
  - Hash + verify internally
  - Return QMProofResult

### Step 4: `qm_getWitness` / `qm_getTxWitness` RPC (1-2 days)

- Block → txnum range → tracker.GetRangeWitness()
- Include preState/stateChange preimages per leaf from history tables
- Single-tx variant for qm_getTxWitness

### Step 5: Verification endpoints (half day)

- `qm_verifyProof(proof)` — stateless proof check
- `qm_verifyWitness(witness)` — stateless witness check
- These are thin wrappers over ProofPath.Check() and RangeWitness.Verify()

**Total: ~5-7 days of work.**
