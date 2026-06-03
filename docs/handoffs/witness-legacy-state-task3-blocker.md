# Witness legacy state (Task 3) — partial + blocker

Plan: `docs/plans/20260603-witness-legacy-state-and-parity.md`, Task 3.

## Done (committed, verified, root-safe)

- Threaded a `legacy bool` end-to-end: `SharedDomainsCommitmentContext.Witness` →
  `HexPatriciaHashed.GenerateWitness` → `witnessLegacy` field on the trie.
- The 3 non-`debug_executionWitness` callers (`eth_call.go:500,556,755`) and the
  internal postState verify call (`debug_execution_witness.go:1317`) pass
  `legacy=false` → byte-unchanged.
- Producer threads `witnessMode` into `detectCollapseSiblings` and `buildWitnessTrie`.
  The `childCount>=2` transient-collapse-sibling drop is now gated on
  `mode==witnessModeCanonical`; **legacy keeps the siblings** (intended Gap-3a divergence;
  default mode is legacy, so default output now retains them).
- `buildWitnessTrie` passes `mode==witnessModeLegacy` into `sdCtx.Witness`.

Gates run: `go build` + `go vet` clean on commitment/commitmentdb/jsonrpc;
`Test_WitnessTrie_GenerateWitness` passes. Canonical corpus (Task 5) unaffected — it
runs with `ERIGON_WITNESS_MODE=canonical`, which still drops siblings.

## Blocked (needs follow-up + live oracle)

Storage-root node materialization (3b) and empty-node inclusion (3c).

Finding: in `witnessCreateAccountNode` (hex_patricia_hashed.go:~1429) the account node's
`Storage` is `trie.NewHashNode(storageRootHash)` — the cell carries only the storage root
*hash*; `toWitnessTrie` stops at the 64-nibble account boundary. To emit the storage-root
*node*:

- Branch root case: read `hph.ctx.Branch(<64-nibble prefix>)` and build the root branch
  node with `HashNode` children.
- **Single-slot / leaf root case: no branch exists at the prefix; the root node is an
  embedded leaf whose construction needs the storage slot key, which the producer does not
  have.** This is the hard blocker — needs storage-trie resolution infrastructure.

Safety note: `hasher.hashInternal` (hasher.go:187) writes the materialized storage
subtree's computed hash into `ac.Root`, so a wrong storage node breaks the account leaf
hash and the existing `buildWitnessTrie` root-vs-`expectedParentRoot` check catches it.
But a *valid-hashing wrong node set* (over/under vs reth) is only caught by the
live-mainnet oracle (`~/dev/wit-oracle/`, Post-Completion) — that's why exact-set match is
the human gate, not automatable here.

Recommended next step: build the storage-root resolution covering branch/extension/leaf
roots, extend `Test_WitnessTrie_GenerateWitness` with a multi-slot (branch root) legacy
case as the automatable gate, then run the oracle for over/under=0.
