# Amsterdam Witness Commitment-Pipeline Divergence (Change 4)

## Overview

`debug_executionWitness` for Amsterdam blocks fails to reproduce `block.Root()` even when the
access set is sourced directly from the persisted Block-Level Access List (BAL). The bug is
in the witness's commitment-recompute pipeline, not in the access-set provider — it
reproduces under the new BAL path and a force-routed `RecordingState` re-exec path with the
full access set. This change owns finding and fixing that divergence so the Amsterdam witness
tests skipped in Change 3 can flip green.

## Status

Stub. Created at the end of Change 3
(`docs/plans/20260527-witness-bal-access-set.md`) to track the verification blocker logged
under Risk 1 there. Investigation has not started.

## Context

Change 3 shipped the BAL-driven access-set provider and the `IsAmsterdam` branch in
`ExecutionWitness`. Mapping unit tests, the path-assertion (no `RecordingState` constructed),
and the pre-Amsterdam byte-identical regression are all green. The verifyWitnessStateless
assertion on Amsterdam blocks is skipped with a tracking link to this plan.

### Observations from Change 3 (Task 3 instrumentation)

For the in-tree Amsterdam test fixture (`TestExecutionWitnessAmsterdamBAL`):

- `IsAmsterdam(header.Time) == true`; the BAL branch fires (verified via
  `installRecordingHookCounter` count == 0).
- `buildAccessedStateFromBAL` returns 7 addresses + 8 storage keys + 2 code addresses,
  matching the persisted BAL exactly.
- All access keys are touched on `sdCtx` before `ComputeCommitment` runs.
- Plain state at `endTxNum` contains post-block values (`InsertChain` succeeded; the chain's
  own `ComputeCommitment` matched `block.Root()`).
- `detectCollapseSiblings.ComputeCommitment` returns a root that diverges from
  `block.Root()`.

The same divergence reproduces when the Amsterdam block is force-routed through the
pre-Amsterdam `RecordingState` re-exec path with `collectAccessedState` instead of
`buildAccessedStateFromBAL`. The access-set source is not the variable.

### What this tells us

Block insertion's `ComputeCommitment` (via `chain_makers` with `versionMap`+`blockIO`)
diverges from the witness's `SplitHistoryReader`-backed `ComputeCommitment` under Amsterdam.
Pre-Amsterdam blocks produce matching roots on both paths today, so something in the
Amsterdam-only commitment state is read or written differently across the two pipelines.

## Investigation Entry Points

1. **Diff the two `ComputeCommitment` invocations.**
   - Chain insertion path: `execution/chain_makers` builds the block, computes the post-state
     root with the `versionMap`+`blockIO` overlay, and matches `block.Root()`.
   - Witness path: `rpc/jsonrpc/debug_execution_witness.go` calls `detectCollapseSiblings`,
     which runs `ComputeCommitment` via a `SplitHistoryReader` over committed state at
     `endTxNum`.
   - Capture both contexts (storage values, account values, touched keys, deletion markers)
     for the failing fixture and diff. Start with the addresses present in the divergent
     trie subset, not the whole state.

2. **Amsterdam EIPs that touch commitment state.** Each is a candidate for "state the witness
   pipeline isn't observing the same way the executor did":
   - EIP-2935 — history-storage contract (BLOCKHASH from contract state). The system update
     happens at block boundary; check whether `SplitHistoryReader` sees it at the right
     `txNum`.
   - EIP-4788 — beacon-root system contract. Same boundary-write question.
   - EIP-7708 — value-transfer logs. Touches receipts/logs more than state, but worth
     ruling out.
   - EIP-8037 — state-gas accounting. May affect when account writes land relative to
     `endTxNum`.

3. **Tombstones / deletions.** Amsterdam BAL semantics include selfdestruct accounting via
   the parallel executor (see `execution/execmodule/bal_selfdestruct_systemaddress_test.go`).
   If the witness pipeline reads deleted accounts as still-present (or vice versa) the
   recomputed root diverges. Check `versionedio.go` exclusion-read handling alongside the
   commitment trie's tombstone view.

4. **SystemAddress accounting.** Change 3 deliberately removed the SystemAddress heuristic on
   the access-set side, but `ComputeCommitment` may need a parallel update — confirm the
   shared trie walk doesn't depend on the heuristic that used to be applied upstream.

## Acceptance

- The two Amsterdam tests skipped in Change 3 (`TestExecutionWitnessAmsterdamBAL`,
  `TestExecutionWitnessAmsterdamActivationBlock` in `rpc/jsonrpc/debug_witness_bal_test.go`)
  flip green after this change lands. Removal of the `t.Skip` is part of the Change 4 PR —
  the issue is not closed by closing the tracking link with the skip still in place.
- `verifyWitnessStateless` passes on the in-tree Amsterdam fixture without
  `ERIGON_WITNESS_NO_VERIFY`.
- The pre-Amsterdam path stays byte-for-byte unchanged (the `else` branch in
  `ExecutionWitness`).

## Out of Scope

- Codes-multiset precision (Risk 2 in Change 3): the over-approximation rule stays unless
  the corpus run after Change 4 lands shows specific failures attributable to it.
- Absent-account exclusion reads (Risk 3): revisit only if `verifyWitnessStateless` still
  fails after the commitment-pipeline fix.
- Headers / BLOCKHASH set (Risk 4): same — revisit after the verify oracle is unblocked.
