# EEST Witness: include parent header + sort ascending

## Overview

`debug_executionWitness` currently populates `result.Headers` only with headers touched by the `BLOCKHASH` opcode, in execution order. EIP-8025 requires:

1. **Parent header (block N-1) is always included**, even when the block makes no `BLOCKHASH` call.
2. **All headers (parent + BLOCKHASH-accessed) are emitted in ascending block-number order**, deduplicated.

This gap accounts for ~99% of the EEST zkVM witness fixture failures on the headers field. Fixing it is a self-contained change to one function body in `rpc/jsonrpc/debug_execution_witness.go` and does not alter the RPC schema.

Tracked by erigontech/erigon#20534 (follow-up to erigontech/erigon#20533).

## Context (from discovery)

- **Files involved:**
  - Implementation: `rpc/jsonrpc/debug_execution_witness.go` (header-collection loop at lines 844-866)
  - Schema: `rpc/jsonrpc/debug_api.go` — `ExecutionWitnessResult.Headers` field already exists, no change
  - Existing unit test: `rpc/jsonrpc/debug_api_test.go::TestExecutionWitness` (does not assert on `Headers`)
  - Integration test: `execution/tests/eest_zkevm_witness/witness_test.go` (currently broad-failed via `bt.Fails(".")`)
- **Spec evidence (from inspecting fixtures):**
  - `witness_headers_empty_block.json` block N=1 expects `headers = [<header for block 0>]` — just the parent.
  - `witness_headers_blockhash_at_offset.json` block N=11 expects `headers` containing ascending block numbers `[1,2,3,...,10]` — parent (10) deduped against BLOCKHASH-accessed range [1..10].
  - `witness_headers_blockhash_boundary.json` 258 blocks each expect `headers_count: 1` (just parent) since no BLOCKHASH is called.
- **Downstream consumers within the same function:** `execBlockStatelessly` (line 1604+) reads `result.Headers` to build the BLOCKHASH lookup; adding the parent is harmless (parent hash itself comes from `block.ParentHash`, not the lookup map).

## Development Approach

- **testing approach**: TDD (write the failing unit test in `debug_api_test.go` first, then implement the fix)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- run tests after each change
- maintain backward compatibility (no API schema change)

## Testing Strategy

- **Unit tests** (`rpc/jsonrpc/debug_api_test.go`):
  - New subtest within `TestExecutionWitness` that decodes `result.Headers`, asserts `len(headers) >= 1` for any non-genesis block, asserts the set always contains `blockNum - 1`, and asserts ascending order by block number.
  - Tested across all blocks of the existing test chain (loop already exists in `t.Run("multiple blocks")`).
- **Integration tests** (`execution/tests/eest_zkevm_witness/witness_test.go`):
  - Remove the broad `bt.Fails(".")` annotation. Allow each fixture to report its true outcome.
  - Capture which categories newly pass (the `witness_headers/*` empty-block + simple-BLOCKHASH cases) vs. which still fail on state/codes/extra-nodes.
  - Re-add narrowed `bt.Fails` patterns only for categories still broken on the other root causes (state ordering, codes ordering, extra state nodes), so CI remains green and the suite documents "what we know is still broken." Full green suite is **not** an outcome requirement of this task.

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

In the header-collection block of `ExecutionWitness` (currently `debug_execution_witness.go:844-866`):

1. Initialize a `map[uint64]struct{}` with the parent block number `blockNum - 1` (skip when `blockNum == 0` for genesis defensiveness).
2. Insert every entry from `accessedBlockHashes` into the same map (natural dedup).
3. Materialize the keys into `sorted []uint64` and call `slices.Sort(sorted)` (the file already imports `slices` at line 7).
4. Loop in sorted order, fetching `HeaderByNumber`, RLP-encoding, appending to `result.Headers`.

The existing fetch+encode+error-handling structure is preserved; only the iteration source, ordering, and one error-message phrasing change (drop "accessed" — see Technical Details).

## Technical Details

**Current code shape (lines ~844-866):**

```go
seenBlockNums := make(map[uint64]struct{})
for _, bn := range accessedBlockHashes {
    if _, seen := seenBlockNums[bn]; seen { continue }
    seenBlockNums[bn] = struct{}{}
    blockHeader, err := api._blockReader.HeaderByNumber(ctx, tx, bn)
    // ... error handling ...
    headerRLP, err := rlp.EncodeToBytes(blockHeader)
    // ... error handling ...
    result.Headers = append(result.Headers, headerRLP)
}
```

**Target shape:**

```go
// Always include the parent header (required for EIP-8025 stateless
// verification), then add any BLOCKHASH-accessed ancestors.
// Emit in ascending block-number order, deduped.
headerNums := make(map[uint64]struct{})
if blockNum > 0 {
    headerNums[blockNum-1] = struct{}{}
}
for _, bn := range accessedBlockHashes {
    headerNums[bn] = struct{}{}
}
sorted := make([]uint64, 0, len(headerNums))
for bn := range headerNums {
    sorted = append(sorted, bn)
}
slices.Sort(sorted)

for _, bn := range sorted {
    blockHeader, err := api._blockReader.HeaderByNumber(ctx, tx, bn)
    if err != nil {
        return nil, fmt.Errorf("failed to load header for block number %d: %w", bn, err)
    }
    if blockHeader == nil {
        return nil, fmt.Errorf("missing header for block number %d", bn)
    }
    headerRLP, err := rlp.EncodeToBytes(blockHeader)
    if err != nil {
        return nil, fmt.Errorf("failed to encode header for block number %d: %w", bn, err)
    }
    result.Headers = append(result.Headers, headerRLP)
}
```

**Import additions:** none — `slices` is already imported at `debug_execution_witness.go:7`.

**Error-message change (intentional):** the existing strings say "accessed block number %d" because every header came from `accessedBlockHashes`. After the fix, the parent header is included unconditionally regardless of "access," so the wording becomes inaccurate. Drop "accessed" to read "block number %d". This is internal error text; no callers should be substring-matching on it. Task 2 below reflects this change.

**Edge cases:**
- `blockNum == 0` (genesis): no parent — skip seeding, fall through to (likely empty) `accessedBlockHashes`.
- `blockNum == 1`: parent is genesis — `result.Headers = [genesis_header]` exactly. Task 1's loop covers this naturally.
- BLOCKHASH calls accessing block `N-1`: deduped against parent automatically by the map.
- BLOCKHASH calls accessing block `0` (genesis): legal, included via `accessedBlockHashes` insertion path; still correctly sorted.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): unit test, code change, integration test marker change, verification.
- **Post-Completion** (no checkboxes): cherry-pick / backport to release branches if needed; informal note on PR linking back to #20534.

## Implementation Steps

### Task 1: Add TDD unit test asserting parent-included + ascending order

**Files:**
- Modify: `rpc/jsonrpc/debug_api_test.go`

**Coverage note:** `CreateTestExecModule`'s test chain does NOT exercise the `BLOCKHASH` opcode (verified — no references in `cmd/rpcdaemon/rpcdaemontest/test_util.go`). So at the unit level the ascending-order and dedup invariants will be vacuously true (every block expected to yield exactly one header — the parent). The full ordering/dedup behavior is covered by the EEST integration suite in Task 3. Document this gap in a code comment on the new subtest.

- [x] add a new subtest `t.Run("headers always include parent", ...)` inside `TestExecutionWitness`
- [x] for each block in `1..latestBlockNum`: call `api.ExecutionWitness(...)`, decode every `result.Headers[i]` into `*types.Header` via `rlp.DecodeBytes`, collect their `Number.Uint64()` values
- [x] assert `len(blockNums) >= 1` — parent is always present
- [x] assert `blockNums[len(blockNums)-1] == blockNum - 1` — parent is the *last* element (ascending sort places it last because all BLOCKHASH-reachable block numbers are `< blockNum`, so `blockNum-1` is the maximum)
- [x] assert strictly ascending order: `for i := 1; i < len(blockNums); i++ { require.Less(t, blockNums[i-1], blockNums[i]) }` — vacuous in this test chain but documents the invariant
- [x] assert content correctness: fetch the canonical header via `m.BlockReader.HeaderByNumber(ctx, tx, blockNum-1)`, compare `decodedParent.Hash() == canonicalParent.Hash()` to verify the RLP encode/decode path is correct
- [x] add a code comment in the subtest body referencing the BLOCKHASH-coverage gap and pointing at the EEST suite
- [x] run `go test -run TestExecutionWitness ./rpc/jsonrpc/...` — Task 1's subtest must FAIL with current code (parent missing); other subtests must still PASS
- [x] mark this task completed only after confirming the failing-as-expected behavior

### Task 2: Implement the fix in ExecutionWitness handler

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [ ] confirm `slices` is in the import block (already at line 7) — no new imports needed
- [ ] replace the loop body at lines 844-866 with the union+sort logic per Technical Details target shape
- [ ] update error message wording: `"failed to load header for block number %d"`, `"missing header for block number %d"`, `"failed to encode header for block number %d"` (drop the word "accessed" — see Technical Details rationale)
- [ ] re-run `go test -run TestExecutionWitness ./rpc/jsonrpc/...` — Task 1's new subtest must now PASS
- [ ] re-run sibling subtests (`genesis block`, `by block number`, `by block hash`, `multiple blocks`, `latest block`, `non-existent block`) — must remain PASS
- [ ] run `make lint` — must be clean (re-run if non-deterministic per CLAUDE.md)
- [ ] commit with prefix `rpc/jsonrpc:` per Erigon convention, e.g. `rpc/jsonrpc: include parent header in debug_executionWitness, sort ascending`

### Task 3: Remove broad `bt.Fails(".")` from EEST witness suite

**Files:**
- Modify: `execution/tests/eest_zkevm_witness/witness_test.go`

- [ ] delete the entire `bt.Fails(".", "witness generation mismatch (#20442): ...")` statement (currently around line 67) — removing the call, not just one line
- [ ] run the suite, captured to a log: `go test -v -run TestExecutionSpecWitness -count=1 ./execution/tests/eest_zkevm_witness/... 2>&1 | tee /tmp/eest_witness_task3.log`
- [ ] count outcomes: `grep -c '^    --- PASS' /tmp/eest_witness_task3.log` and `grep -c '^    --- FAIL' /tmp/eest_witness_task3.log`
- [ ] enumerate failing fixtures by category: `grep '^--- FAIL' /tmp/eest_witness_task3.log | sed 's,/.*,,' | sort -u`
- [ ] document the pass/fail breakdown in this plan file under a new section "Task 3 results" (categories that newly pass vs. still fail, with rough counts per category)
- [ ] tests do NOT need to all pass — full green is not required for this task

### Task 4: Re-narrow `bt.Fails` to remaining-broken categories so suite stays green

**Files:**
- Modify: `execution/tests/eest_zkevm_witness/witness_test.go`

- [ ] using the Task 3 results, identify fixture path patterns that still fail (state ordering, codes ordering, extra state nodes)
- [ ] add one or more `bt.Fails("<regex>", "<reason linking #20534 / #20442>")` calls scoped to those patterns only — leave headers-only fixtures unguarded
- [ ] re-run the EEST suite — all subtests should report PASS (newly-fixed are real PASS, still-broken are expected-failure PASS)
- [ ] update Task 3 results section with the final regex(es) chosen and rationale

### Task 5: Verify acceptance criteria

- [ ] verify `result.Headers` always contains the parent header for `blockNum > 0`
- [ ] verify ordering is strictly ascending in every response
- [ ] run full unit-test slice for the package: `go test ./rpc/jsonrpc/...`
- [ ] run full EEST witness suite: `go test -v -run TestExecutionSpecWitness -count=1 ./execution/tests/eest_zkevm_witness/...`
- [ ] run `make lint` — clean
- [ ] run `make erigon integration` — builds

### Task 6: Final — commit and cleanup

- [ ] update `agents.md` only if a new pattern was introduced (none expected — this is a localized fix)
- [ ] move this plan to `docs/plans/completed/`

## Post-Completion

*Items requiring manual intervention or external systems — no checkboxes, informational only*

**External tracking:**
- Update GitHub issue erigontech/erigon#20534 with the PR link and pass-count delta after Task 3.
- The remaining 3 root causes (state node ordering, extra state nodes, codes ordering) stay open under #20534 and #20442 — separate plans.

**RPC consumers (informational):**
- Any third-party consumer of `debug_executionWitness` will start receiving the parent block header in `headers[]`. This is a move toward EIP-8025 compliance, not a breaking-contract change. No consumer-side action required unless they were specifically relying on parent-header *absence*.
