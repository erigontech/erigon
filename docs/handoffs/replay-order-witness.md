# Continue: replay_order witness fix (debug_executionWitness extra node)

Continue fixing the 2 `witness_state_replay_order` EEST zkevm fixtures where
erigon's `debug_executionWitness` emits **one extra state node** vs the canonical
(reth-filled) witness. Producer-only; state-root-critical commitment work.

## Where things are
- Worktree `/Users/awskii/org/wrk/erigon-eest-zkevm`, branch `awskii/witness-replay-order-wip`
  (off the suite branch `awskii/eest-zkevm-witness`, PR #21487, which has the suite +
  the `test-fixtures-cache/eest_zkevm` corpus, ~2.3G).
- The 2 fixtures (only remaining genuine producer gap in eip8025):
  `for_amsterdam/amsterdam/eip8025_optional_proofs/witness_state_replay_order/`
  `{witness_state_delete_then_insert_uses_insert_before_delete_order,
    witness_state_block_diff_delete_insert_before_delete_order}`
- Verify cmd:
  `go test -count=1 -v -run 'TestExecutionSpecWitness/for_amsterdam/amsterdam/eip8025_optional_proofs/witness_state_replay_order' ./execution/tests/eest_zkevm_witness/...`
- Full regression guard (run after ANY change; must not red a currently-passing fixture
  or perturb roots):
  `go test -count=1 -run 'TestExecutionSpecWitness/for_amsterdam' ./execution/tests/eest_zkevm_witness/...`

## Root cause
Post-state root computation replays the block's updates in **key-sorted** order.
Deleting slot 1 from `{1,2}` transiently collapses the branch (2→1 child), firing
the collapse tracer, which `TouchHashedKey`s the surviving sibling (slot 2) into the
witness. But a later insert (slot 3) refills the branch, so the **final** branch has
≥2 children and that sibling node is **not** needed. Correct (insert-before-delete)
replay avoids the collapse. erigon's extra node = a transient-collapse sibling.

## Code map
- `rpc/jsonrpc/debug_execution_witness.go`:
  - `detectCollapseSiblings` (~L928): post-state `ComputeCommitment` with a
    `CollapseTracer` recording sibling hashed paths into `siblingPaths`.
  - `buildWitnessTrie` (~L975): `TouchHashedKey`s those siblingPaths into the witness.
- `execution/commitment/hex_patricia_hashed.go`:
  - `CollapseTracer` type (~L62, currently `func(hashedKeyPath []byte)`).
  - `detectCollapseBeforeDelete` (~L2357) and `detectCascadingCollapseAtRow` (~L2412)
    call `hph.collapseTracer(siblingPath)`. Branch prefix at collapse =
    `hph.currentKey[:depth]`, `depth = hph.depths[parentRow]-1` (and `depths[row]-1`).
- `execution/commitment/commitmentdb/commitment_context.go`:
  - `ComputeCommitment` (~L302): `Process()` (~L135) writes post-state branches
    INLINE via `trieContext.PutBranch` regardless of `saveState` (saveState only gates
    `encodeAndStoreCommitmentState`, ~L178). So post-state branches ARE in the
    in-memory `sd` domain after the pass.
  - `TrieContext.Branch(prefix)` (~L799) → `readDomain(kv.CommitmentDomain, prefix)`.
  - `sdc.trieContext(tx, blockNum, txNum)` (~L201) builds a TrieContext.
- `execution/commitment/commitment.go`:
  - `BranchData.decodeCells` (~L1044): `afterMap = BigEndian.Uint16(branchData[2:])`;
    child count = `bits.OnesCount16(afterMap)`.

## Approach A (recommended): drop siblings whose final branch has >=2 children
1. Change `CollapseTracer` to `func(siblingPath, branchPrefix []byte)`; both call sites
   pass `append([]byte(nil), hph.currentKey[:depth]...)`. (Tests/commitmentdb passthrough
   compile-check.)
2. In `detectCollapseSiblings`, collect `{siblingPath, branchPrefix}` candidates; after
   `ComputeCommitment`, read the FINAL branch at `branchPrefix`, and keep the sibling
   only if the branch is absent or has 1 child (persistent collapse); drop it if >=2
   (transient).
3. Add `BranchData.ChildCount()` (commitment.go) = `bits.OnesCount16(BigEndian.Uint16(branchData[2:]))`.

**BLOCKER to solve first:** a *fresh* `sdc.trieContext(tx, blockNum, firstTxNumInBlock).Branch(prefix)`
returned 0 for these. Branches ARE written post-compute, so the read resolves through the
wrong source — likely the custom split-history reader set via `SetCustomHistoryStateReader`
(parent state, ~L940 in detectCollapseSiblings) and/or a txNum mismatch, NOT the in-memory
post-compute writes. **First verify a branch read works**: read a KNOWN existing prefix
right after ComputeCommitment and confirm non-zero child count; figure out the correct
context (the one `Process` used / sd in-memory) before wiring the filter. branchPrefix is a
nibble array (`currentKey[:depth]`, 1 byte/nibble) — confirm that matches the commitment
domain key encoding for these branches.

## Approach B (fallback, no branch read)
Pass `branchPrefix` + `siblingNibble` via the tracer. In the driver, hash each touched key
(account = `keccak(addr)`; storage = `keccak(addr)++keccak(slot)`, nibblized — find
erigon's exact scheme; `Updates.TouchPlainKey` in commitment.go ~L1585 hashes plain keys),
and drop a sibling iff some touched key with non-zero post-state value shares `branchPrefix`
at a different branch nibble (refills the branch). The post reader is `splitStateReader`
already available in `detectCollapseSiblings`.

## Constraints
- State-root-critical / sensitive commitment subsystem — minimal, guarded changes.
- Do NOT name reth/revm in code comments (user rule). Document controlling state in 1–2 sentences.
- Keep the existing skips: gas (#21563) and verifier-negative (#21566).
- The earlier A-scaffolding (CollapseTracer 2-arg, ChildCount, ReadBranchChildCount, driver
  filter) was drafted and reverted clean — re-create from this note.
- Final: extract producer changes to a clean branch off `main` for a PR (verified locally
  via the suite, since the zkevm suite lives on #21487 until merged).

## Context (already shipped, don't redo)
- gap1 SYSTEM_ADDRESS fix = PR #21565 (off main). 22 verifier-negative skips on #21487
  (#21566). gas mismatches were stale fixtures (#21555 closed, #21563 skips). 22/25 of the
  original "failures" were verifier-negative tests, not producer bugs.
