# QMTree: Append-Only Merkle State Commitment

QMTree is an alternative Ethereum state commitment scheme that replaces the hex
patricia trie with a sequential, append-only binary Merkle tree indexed by
Erigon's global transaction number (`txNum`). Each leaf records a single
transaction's state changes; the tree root after a block's final transaction is
that block's state commitment.

The key advantages over the hex patricia trie:

- **O(1) append** — new leaves are appended sequentially; no trie traversal
- **Compact proofs** — binary Merkle paths of ~1 KB versus ~4 KB for trie proofs
- **Flat namespace** — accounts and storage share one tree; no nested tries
- **Proof-of-execution** — each leaf commits to the EVM trace and state transition,
  enabling fraud proofs and verifiable `eth_call`

## What is implemented

On the `qmtree` branch:

- Core tree, twig, proof, and file storage (`execution/commitment/qmtree/`)
- Live block execution with qmtree roots (`--experimental.qmtree` flag)
- `qm_` RPC namespace — 10 methods including `qm_call` and `qm_callProof`
- Compact twig-grouped proof format with 32-byte Merkle digest
- Format-size analysis and integration benchmarks (`analysis_test.go`)

## Documents

| Document | What it covers |
|---|---|
| [design.md](design.md) | Tree architecture: serial numbering, leaf hash construction, proof structure, unwind/reorg handling, keyset strategy, future work |
| [protocol-spec.md](protocol-spec.md) | **Normative.** Wire formats, RPC type signatures, proof digest algorithm, and verification steps |
| [state-proof-analysis.md](state-proof-analysis.md) | Design decisions, RPC implementation details, compact proof sizing analysis, and the vision for provable calls and agent authorization |
| [transition-design.md](transition-design.md) | Architecture for proof-of-transition: the 25 spec-mandated operations outside the EVM that complete the leaf hash |
| [transition-format.md](transition-format.md) | **Normative.** Exact byte layout for all 11 transition record types with worked examples |

## Reading order

**First time:** README → [design.md](design.md) → [protocol-spec.md](protocol-spec.md)

**Building a verifier or client:** [protocol-spec.md](protocol-spec.md) §§ 2–5, then [transition-format.md](transition-format.md)

**Background and rationale:** [state-proof-analysis.md](state-proof-analysis.md) + [transition-design.md](transition-design.md)

---

## Roadmap

### Required to complete this work

These items are needed before qmtree proofs are fully self-contained and verifiable by a third party:

1. **Exclusion proofs** — prove that no state entry exists for a given key (i.e. negative proofs). Without this, a verifier cannot confirm that a key was unmodified; only inclusion can be proven. Requires adding `NextKeyHash` to entries per QMDB §7, and extending the RPC to serve them.

2. **Transition hash completion** — fully implement the EVM opcode-trace encoding (`transitionHash`) so `qm_callProof` verification can bind the call result to the proof. The record types and format are specified in `transition-format.md`; the implementation needs to record all 25 spec-mandated operations.

3. **SMT state roots** — replace the current `DeriveSha_MPT` hash for `preStateHash`/`stateChangeHash` with a Sparse Merkle Tree root. This allows a verifier to check individual state key/value pairs within the proof without disclosing the full changeset. Tracked in `state-proof-analysis.md §Question 3b`.

4. **Integration benchmarks** — run `TestBench_GetWitness` and `TestBench_ProofSizeByTwig` against a fully synced hoodi datadir (set `QMTREE_DATADIR` to the qmtree snap directory). A node is accumulating data on `dev-bm-e3-ethmainnet-n1`.

### Future enhancements

These improve production readiness and performance but are not needed for a working proof system:

5. **Committed keyset index** — a second Merkle tree over `(domain, key) → txnum` for trustless key lookup without relying on the node's inverted index (QMDB B-tree indexer).

6. **Head pruning** — prune inactive twigs from disk using HPFile once twig eviction logic is added (QMDB §10).

7. **Prefetcher-updater-flusher pipeline** — pipelined execution for production throughput (QMDB architecture).

8. **HybridIndexer** — SSD+DRAM indexing for production memory efficiency.

9. **eth_getProof compatibility** — adapter translating qmtree proofs to the format expected by existing Ethereum tooling.

10. **Parallel twig sync** — use the 4-shard design (`TWIG_SHARD_COUNT=4`) for parallel hash computation during execution.
