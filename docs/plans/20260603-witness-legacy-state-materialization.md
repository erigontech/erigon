# Legacy witness: materialize storage-root trie nodes

## Overview
The last gap to make erigon's `debug_executionWitness` match the reference **legacy**
witness 1:1. Everything else matches: keys, codes (empty trigger fixed on account-load),
EIP-7702 designators, and the `mode` RPC param. The remaining ~44 nodes/block `state`
under-inclusion is **storage-root nodes** that legacy keeps but erigon prunes: for a
witnessed account whose storage slots were *not* touched this block, erigon emits the
storage root as a **hash**, not the expanded **node**.

Mode-gated and state-root-critical: the canonical path must stay byte-identical, and the
witness root must be unchanged (we only ADD nodes).

## Context (from discovery)
- `execution/commitment/hex_patricia_hashed.go`:
  - `witnessCreateAccountNode` (~L1401): builds the witness account node; at **L1439**
    sets `Storage: trie.NewHashNode(storageRootHash)` — the inject point. The
    `!storageIsSet` branch (L1435-1437) sets `Storage: nil` (empty storage).
  - `witnessComputeCellHashWithStorage` (~L961): computes the storage root hash and,
    for a single fused slot, **loads** the slot (`storageFromCacheOrDB`, ~L1009) and
    builds the leaf — leverage it for the single-slot case.
  - `toWitnessTrie` (~L1473): stops at the account boundary; branch→`FullNode` decode
    pattern at ~L1573-1613 (reusable for the multi-slot case).
  - `GenerateWitness` (~L2606) takes `legacy bool`; `hph.witnessLegacy` is set (L2620)
    but **read nowhere** — this task makes it load-bearing.
- `trie.AccountNode.Storage` is a `Node` interface (`execution/commitment/trie/node.go:71`)
  → can hold a real materialized node.
- Root-invariant guard test already exists (commit `73da896e`).
- Corpus `eest_zkevm_witness` present on this branch via temporary merge (`d988f667`).
- Oracle: `~/dev/wit-oracle/reth-legacy/` (blocks 25230831–850; exclude flaky
  25230834/841/845/846).

## Development Approach
- **Testing**: TDD for the materialization — extend `Test_WitnessTrie_GenerateWitness`
  first (Red), then implement (Green). This is the autonomous gate so the executor cannot
  defer/silent-pass.
- Mode-gated: every change behind `hph.witnessLegacy`; canonical output unchanged.
- KISS, minimal, guarded. `make lint` before each commit. No external-client names in
  comments.

## Testing Strategy
- **Unit (autonomous gate)**: `Test_WitnessTrie_GenerateWitness`
  (`execution/commitment/hex_patricia_hashed_test.go` ~L2225; `t.Skip`s under `-short`, so
  run **without** `-short`): a small trie with an account that has (i) a single storage
  slot and (ii) multiple slots, witnessed in **legacy** mode, asserting
  `AccountNode.Storage` is a materialized node (not a `HashNode`) and `RootHash()` equals
  the canonical-mode root. Run: `go test -run Test_WitnessTrie_GenerateWitness ./execution/commitment/`.
- **Root-invariant**: keep the existing guard test green.
- **Canonical regression**: `ERIGON_WITNESS_MODE=canonical go test -count=1 -timeout 40m
  -run 'TestExecutionSpecWitness/for_amsterdam' ./execution/tests/eest_zkevm_witness/...`
  (default 600s timeout false-fails the large corpus).

## Progress Tracking
- `[x]` when done; ➕ new tasks; ⚠️ blockers.

## Solution Overview
In `witnessCreateAccountNode`, when `hph.witnessLegacy` and the account has non-empty
storage, replace `NewHashNode(storageRootHash)` with the **materialized** storage-root
node, by shape:
- **empty** (`storageRootHash == empty.RootHash`): unchanged (`Storage: nil`); confirm the
  oracle wants no extra node (do not add spurious nodes).
- **single-slot / fused leaf**: build the leaf from the cell directly (the function
  `witnessComputeCellHashWithStorage` returns only the *hash*, not the slot key/value).
  Ensure the slot is loaded (`storageFromCacheOrDB` if `!c.loaded.storage()`), re-derive the
  hashed slot key with `hashKey(hph.keccak, c.storageAddr[koffset:c.storageAddrLen], …)` +
  terminator (mirror ~L985-988), and build a `trie.ShortNode` from that key and
  `c.Storage[:c.StorageLen]` — reuse the existing storage-leaf construction at ~L1518-1524.
- **multi-slot branch**: resolve the storage-root branch **read-only** via
  `hph.ctx.Branch(nibbles.HexToCompact(hashedKey[:64]))` and decode into a node reusing the
  branch→`FullNode` pattern at ~L1573-1613. **Do NOT use `hph.unfold`** — it mutates
  `activeRows`/`currentKey`/grid mid-traversal (it's part of the `GenerateWitness` fold/unfold
  state machine) and would corrupt the outer loop's witness for other keys and can change
  `RootHash()`. `ctx.Branch` is read-only and safe here.

**Depth = one level (validated, not assumed).** Only the storage-ROOT node is materialized;
its children stay `HashNode`s. Confirmed empirically: every missing node in the oracle is
referenced by an *included* node (the account leaf), i.e. they are direct children of account
leaves = storage roots, never deeper subtree nodes.

Canonical mode path is untouched. The witness `RootHash()` must be unchanged.

## Technical Details
- Gate: `if hph.witnessLegacy { …materialize… } else { NewHashNode(storageRootHash) }`.
- Single-slot leaf: re-derive the hashed slot key + read `c.Storage`; reuse the storage-leaf
  construction at ~L1518-1524 (NOT the hash return of `witnessComputeCellHashWithStorage`).
- Multi-slot: decode `ctx.Branch(...)` into a `FullNode` whose children are `HashNode`s of
  the branch's child hashes — one level (the storage ROOT node only).
- **Correctness check (mandatory):** the materialized `Storage` node, hashed in isolation
  (`trie.NewInMemoryTrie(storageNode).Root()`, as L1531-1535 already does for account
  subtrees), MUST equal `storageRootHash`. This catches wrong keys/values/children that a
  "not a HashNode" type check would miss.
- Never mutate `hph` traversal state (`activeRows`/`currentKey`/grid) — `ctx.Branch` reads
  are safe; `unfold` is not. Use temp buffers like the existing code.
- Unit gate is constructible with no live DB: reuse the existing `StorageSingleton` /
  `StorageSubtrieWithCommonPrefix` subtests (test ~L2409/L2431) over `MockState` (whose
  `Branch`/`Storage` are in-memory backed by `Process`).

## What Goes Where
- **Implementation Steps**: the unit-test gate, the materialization (by shape), corpus.
- **Post-Completion**: manual oracle acceptance (live node); revert temp corpus merge;
  extract clean PR off `main`.

## Implementation Steps

### Task 1: Failing unit test for legacy storage-root materialization (Red)

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed_test.go`

- [x] extend `Test_WitnessTrie_GenerateWitness` with a legacy case, reusing the existing
      `StorageSingleton` (single untouched slot) and `StorageSubtrieWithCommonPrefix`
      (multi-slot) fixtures over `MockState` (no live DB)
- [x] assert (currently failing): in legacy mode `AccountNode.Storage` is a materialized
      node (not `*trie.HashNode`) for both accounts
- [x] assert the materialized node's own hash equals `storageRootHash`
      (`trie.NewInMemoryTrie(storageNode).Root()` == the cell's storage root) — pins content,
      not just the Go type
- [x] assert `RootHash()` in legacy == canonical (root unchanged)
- [x] confirm it runs without `-short` and fails for the right reason (materialization
      missing), not a setup error

### Task 2: Materialize empty + single-slot storage roots (legacy)

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [x] in `witnessCreateAccountNode`, gate on `hph.witnessLegacy`; empty storage stays
      `Storage: nil`
- [x] single-slot: ensure the slot is loaded (`storageFromCacheOrDB` if
      `!c.loaded.storage()`), re-derive the hashed slot key (`hashKey` on `c.storageAddr`,
      ~L985-988) and build a `trie.ShortNode` from it + `c.Storage` (reuse ~L1518-1524) —
      attach instead of a `HashNode`
- [x] canonical path byte-identical (no change when `!hph.witnessLegacy`)
- [x] unit test single-slot assertions pass (incl. node-hash == storageRootHash);
      root-invariant green; `make lint` clean

### Task 3: Materialize multi-slot storage-root branch (legacy)

**Files:**
- Modify: `execution/commitment/hex_patricia_hashed.go`

- [ ] resolve the storage-root branch **read-only** via
      `ctx.Branch(nibbles.HexToCompact(hashedKey[:64]))` and decode into a `FullNode` reusing
      the ~L1573-1613 pattern; attach as `AccountNode.Storage` (one level)
- [ ] children stay `HashNode`s of the branch's child hashes (root node only); **do not call
      `hph.unfold`** (mutates traversal state → corrupts other keys / `RootHash`)
- [ ] canonical path unchanged; `RootHash()` unchanged
- [ ] unit test multi-slot assertions pass (incl. node-hash == storageRootHash);
      root-invariant green; `make lint` clean

### Task 4: Canonical corpus regression (proves canonical unchanged only)
**Files:** (no source changes)
- [ ] `make erigon integration` + `make lint` clean
- [ ] `go test -count=1 -timeout 40m -run 'TestExecutionSpecWitness/for_amsterdam'
      ./execution/tests/eest_zkevm_witness/...` — no new failures. NOTE: the corpus test
      hard-codes `t.Setenv("ERIGON_WITNESS_MODE","canonical")` (witness_test.go:52), so it
      only proves the canonical path is byte-identical; it does NOT exercise legacy. The
      legacy materialization is gated solely by the unit test (Task 1) + the manual oracle.

### Task 5: Verify acceptance criteria (code-level)
- [ ] legacy materializes storage-root nodes (empty/single/multi); canonical byte-identical
- [ ] `RootHash()` unchanged across modes; root-invariant + materialization unit tests green
- [ ] canonical corpus green; build + lint clean

### Task 6: [Final] Documentation and plan close-out
- [ ] update the witness memory note with the materialization outcome
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion
*Manual / external — no checkboxes*

**Oracle acceptance (manual, human — needs the live mainnet node):**
- Rebuild, restart (`--datadir ~/dev --chain mainnet --prune.mode=full
  --prune.include-commitment-history --http --http.api=eth,debug,erigon,web3,net,trace`),
  wait for witness-readiness (poll a real `debug_executionWitness`; erigon returns empty
  witnesses for some near-tip blocks — use blocks with non-empty state), unique-set diff
  erigon(legacy) vs `~/dev/wit-oracle/reth-legacy/` (blocks 25230831–850, exclude the 4
  flaky). Assert `state` under→0 over→0; keys/codes/headers stay 0/0.

**Separate known residuals (NOT this plan):**
- keys +~1.25/block over on some blocks (the `accountExists` predicate missed an edge —
  accessed nonexistent accounts) — follow-up.

**Branch hygiene**
- Revert the temporary corpus merge (`d988f667`); extract to a clean PR off `main`.
