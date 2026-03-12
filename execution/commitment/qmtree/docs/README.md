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
