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
| [design.md](design.md) | Tree architecture: txNum-based ordering, leaf hash construction (including [execution hash per-opcode format](design.md#execution-hash-per-opcode-record-format-exechasher)), proof structure, unwind/reorg handling, keyset strategy, exclusion proofs |
| [protocol-spec.md](protocol-spec.md) | **Normative.** Wire formats, RPC type signatures, proof digest algorithm, and verification steps |
| [state-proof-analysis.md](state-proof-analysis.md) | Design decisions, RPC implementation details, compact proof sizing analysis, and the vision for provable calls and agent authorization |
| [transition-design.md](transition-design.md) | Architecture for proof-of-transition: the 25 spec-mandated operations outside the EVM that complete the leaf hash |
| [transition-format.md](transition-format.md) | **Normative.** Exact byte layout for all 11 transition record types with worked examples |
| [keyindex-persistence-plan.md](keyindex-persistence-plan.md) | Implementation plan: persist KeyIndex to disk using RecSplit + segmented data files |
| [dataset-generation.md](dataset-generation.md) | How to generate a qmtree dataset from a synced datadir (mainnet or hoodi) |
| [snapshot-format.md](snapshot-format.md) | On-disk snapshot file format: .kv entry data, .kvi RecSplit indices, MDBX hot tables |
| [domain-integration-plan.md](domain-integration-plan.md) | Plan: integrate qmtree into Erigon's domain/snapshot/torrent pipeline (6 phases) |

## Reading order

**First time:** README → [design.md](design.md) → [protocol-spec.md](protocol-spec.md)

**Building a verifier or client:** [protocol-spec.md](protocol-spec.md) §§ 2–5, then [transition-format.md](transition-format.md)

**Understanding proof-of-execution / fraud proofs:** [design.md §2.2](design.md#22-leaf-hash-construction) (execution hash per-opcode format) → [transition-design.md](transition-design.md) → [transition-format.md](transition-format.md)

**Background and rationale:** [state-proof-analysis.md](state-proof-analysis.md) + [transition-design.md](transition-design.md)

---

## Roadmap

### Completed

- **MDBX hot tables** — entries written to MDBX during execution, collated
  to `.kv`/`.kvi` snapshot files at step boundaries with binary-doubling merge
- **KeyIndex removed** — redundant with Erigon's existing inverted index
  (`IndexRange` with desc limit=1 gives latest txNum for any key)
- **Domain registration** — `QMTreeDomain` registered in the Aggregator
  (Disable=true for now; standalone SnapshotManager handles collation/merge)

### TODO

1. **Twig root persistence for pruning/fresh-start** — `LoadFromDB` currently
   replays ALL entries from txNum 0 to rebuild the in-memory tree. This
   blocks both historical pruning and fresh-start from downloaded snapshots.
   **Root cause**: the upper tree needs twig roots for every completed twig
   to compute the current qmtree root. Without individual entries, we can't
   derive twig roots.
   **Fix**: persist twig roots as MDBX metadata (`twigId → twigRoot`, 32
   bytes each, ~24 KB per step). On load, populate the upper tree directly
   from twig roots instead of replaying entries. Then:
   - Pruned entries → upper tree still works (twig roots preserved)
   - Fresh-start from snapshots → load twig roots from snapshot trailer or
     separate file, skip entry replay entirely
   - Proofs for pruned entries return "not available" (non-archival)
   - Proofs for recent entries still work (entries in MDBX)

2. **Aggregator integration** — setting `IiCfg.Disable=false` causes the
   Aggregator to hang during initialization on large datadirs (24M+ blocks).
   Once fixed, the Aggregator handles collation/merge/prune automatically
   and the standalone SnapshotManager can be removed.

3. **Integration benchmarks** — run `TestBench_GetWitness` and
   `TestBench_ProofSizeByTwig` against a fully synced mainnet dataset.

4. **Torrent distribution** — register qmtree snapshot files with the
   downloader so new nodes download instead of replaying from genesis.

5. **eth_getProof compatibility** — adapter translating qmtree proofs to
   the format expected by existing Ethereum tooling.

6. **Parallel twig sync** — use the 4-shard design (`TWIG_SHARD_COUNT=4`)
   for parallel hash computation during execution.
