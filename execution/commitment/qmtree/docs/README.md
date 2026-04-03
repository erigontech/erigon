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
- 4-component leaf hash: `preStateHash` (DeriveSha MPT), `stateChangeHash` (DeriveSha MPT), `transitionHash` (EVM opcode trace + 25 spec-mandated operations), `previousLeafHash`
- `qm_` RPC namespace — 10 methods including `qm_call` and `qm_callProof`
- Compact twig-grouped proof format with 32-byte Merkle digest
- Format-size analysis and integration benchmarks (`analysis_test.go`)
- **KeyIndex** — committed sorted key-set with inclusion and exclusion (non-membership) proofs (`keyindex.go`). Each block commits a Merkle root over all `(keyHash → latestTxNum)` pairs written so far.

## Documents

| Document | What it covers |
|---|---|
| [design.md](design.md) | Tree architecture: serial numbering, leaf hash construction (including [execution hash per-opcode format](design.md#execution-hash-per-opcode-record-format-exechasher)), proof structure, unwind/reorg handling, keyset strategy, exclusion proofs |
| [protocol-spec.md](protocol-spec.md) | **Normative.** Wire formats, RPC type signatures, proof digest algorithm, and verification steps |
| [state-proof-analysis.md](state-proof-analysis.md) | Design decisions, RPC implementation details, compact proof sizing analysis, and the vision for provable calls and agent authorization |
| [transition-design.md](transition-design.md) | Architecture for proof-of-transition: the 25 spec-mandated operations outside the EVM that complete the leaf hash |
| [transition-format.md](transition-format.md) | **Normative.** Exact byte layout for all 11 transition record types with worked examples |
| [keyindex-persistence-plan.md](keyindex-persistence-plan.md) | Implementation plan: persist KeyIndex to disk using RecSplit + segmented data files |

## Reading order

**First time:** README → [design.md](design.md) → [protocol-spec.md](protocol-spec.md)

**Building a verifier or client:** [protocol-spec.md](protocol-spec.md) §§ 2–5, then [transition-format.md](transition-format.md)

**Understanding proof-of-execution / fraud proofs:** [design.md §2.2](design.md#22-leaf-hash-construction) (execution hash per-opcode format) → [transition-design.md](transition-design.md) → [transition-format.md](transition-format.md)

**Background and rationale:** [state-proof-analysis.md](state-proof-analysis.md) + [transition-design.md](transition-design.md)

---

## Roadmap

### Required to complete this work

These items are needed before qmtree proofs are fully self-contained and verifiable by a third party:

1. **Integration benchmarks** — run `TestBench_GetWitness` and `TestBench_ProofSizeByTwig` against a fully synced hoodi datadir (set `QMTREE_DATADIR` to the qmtree snap directory). A node is accumulating data on `dev-bm-e3-ethmainnet-n1`.

2. **RPC exposure of exclusion proofs** — extend the `qm_` RPC namespace to serve `KeyIndexRoot` alongside the qmtree root, and to serve `ExclusionProof` responses from `GetExclusionProof`. The on-node data structures are implemented; only the RPC layer remains.

3. **KeyIndex unwind** — the `KeyIndex` does not yet support incremental reorg unwind (requires rebuilding from genesis for affected keys). Needed for correctness under reorgs.

### Future enhancements

These improve production readiness and performance but are not needed for a working proof system:

4. **KeyIndex persistence** — the current `KeyIndex` is in-memory only and is rebuilt during `LoadFromDisk`. Plan: segmented `.kv` data files + RecSplit `.kvi` indices, matching Erigon's Domain pattern. See [keyindex-persistence-plan.md](keyindex-persistence-plan.md).

5. **Head pruning** — prune inactive twigs from disk using HPFile once twig eviction logic is added (QMDB §10).

6. **Prefetcher-updater-flusher pipeline** — pipelined execution for production throughput (QMDB architecture).

7. **HybridIndexer** — SSD+DRAM indexing for production memory efficiency.

8. **eth_getProof compatibility** — adapter translating qmtree proofs to the format expected by existing Ethereum tooling.

9. **Parallel twig sync** — use the 4-shard design (`TWIG_SHARD_COUNT=4`) for parallel hash computation during execution.
