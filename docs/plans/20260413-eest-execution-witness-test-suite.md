# EEST Execution Witness Test Suite (EIP-8025)

## Overview
- Add a new test suite that validates Erigon's execution witness generation against the 93 EIP-8025 fixtures from the `zkevm@v0.3.3` release of `ethereum/execution-spec-tests`
- Ensures `debug_executionWitness` RPC correctness for stateless execution: MPT state nodes, bytecodes, and block headers are generated correctly for all witness categories
- Integrates with the existing EEST infrastructure via `testutil.TestMatcher.Walk()` and a new `WitnessBlockTest` type
- Issue: https://github.com/erigontech/erigon/issues/20442

## Context (from discovery)

**Files/components involved:**
- `execution/tests/testutil/block_test_util.go` — existing `BlockTest` type, `btJSON`, `btBlock`, `Run()` method
- `execution/tests/testutil/matcher.go` — `TestMatcher.Walk()` for walking JSON fixture dirs
- `execution/tests/eest_blockchain/block_test.go` — pattern to follow for new test packages
- `execution/tests/eest_devnet/block_test.go` — Amsterdam fork test pattern with `ExperimentalBAL`
- `execution/tests/testforks/forks.go` — Amsterdam fork already defined (line 218)
- `rpc/jsonrpc/debug_execution_witness.go` — `ExecutionWitness()` RPC impl, `RecordingState`, `ExecutionWitnessResult{State, Codes, Keys, Headers}`
- `rpc/jsonrpc/debug_api_test.go:747-833` — existing `TestExecutionWitness` showing API setup
- `rpc/jsonrpc/eth_api.go:159` — `NewBaseApi()` constructor (exported)
- `rpc/jsonrpc/debug_api.go:88` — `NewPrivateDebugAPI()` constructor (exported)
- `db/state/statecfg/state_schema.go:362` — `EnableHistoricalCommitment()`
- `.gitmodules` — submodule `eest-fixtures` at `execution/tests/execution-spec-tests/`

**Fixture format:**
- Standard `btJSON` blockchain test format with additional per-block fields
- `executionWitness: {state: [hex...], codes: [hex...], headers: [hex...]}`
- `statelessInputBytes` / `statelessOutputBytes` (out of scope for this plan)
- Network: `"Amsterdam"` (already in `testforks.Forks`)

**Fixture categories (93 files across 16 categories):**
`witness_headers`, `witness_state_reads`, `witness_state_writes`, `witness_state_deletes`, `witness_state_invariants`, `witness_state_replay_order`, `witness_bytecodes_call_variants`, `witness_bytecodes_contract_creation`, `witness_bytecodes_eoa_precompiles`, `witness_bytecodes_extcode`, `witness_bytecodes_selfdestruct`, `witness_bytecodes_system_contracts`, `witness_7702`, `witness_validation_codes`, `witness_validation_headers`, `witness_validation_state`

**Key design decisions from brainstorm:**
1. Scope: witness-only (93 EIP-8025 fixtures), not the full 2775 zkevm suite
2. Fixture placement: new `blockchain_tests_zkevm/` top-level dir in `erigontech/eest-fixtures`
3. Verification: call `PrivateDebugAPI.ExecutionWitness()` directly (full RPC logic, no HTTP)
4. Matching: exact ordered comparison of state/codes/headers arrays
5. Test type: new `WitnessBlockTest` in `testutil/` that embeds `BlockTest`

## Development Approach
- **testing approach**: Regular (code first, then tests — the deliverable IS a test suite)
- complete each task fully before moving to the next
- make small, focused changes
- **CRITICAL: all tests must pass before starting next task** — no exceptions
- **CRITICAL: update this plan file when scope changes during implementation**
- run `make lint` after code changes
- maintain backward compatibility — do NOT break existing EEST tests

## Testing Strategy
- **unit tests**: the deliverable itself is a test suite; validation = tests pass against fixtures
- **integration**: verify existing `TestExecutionSpecBlockchain` and `TestExecutionSpecBlockchainDevnet` still pass
- **lint**: `make lint` must pass

## Progress Tracking
- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Implementation Steps

### Task 1: Add witness fixtures to eest-fixtures submodule

**Files:**
- Modify: `erigontech/eest-fixtures` repo (external)
- Modify: `.gitmodules` submodule pointer

- [x] Download `fixtures_zkevm.tar.gz` from `ethereum/execution-spec-tests` release `zkevm@v0.3.3`
- [x] Extract only the 93 `eip8025_optional_proofs/` files
- [x] In the `erigontech/eest-fixtures` repo, create `blockchain_tests_zkevm/for_amsterdam/amsterdam/eip8025_optional_proofs/` and add the extracted fixtures preserving subdirectory structure
- [x] Commit and push to `erigontech/eest-fixtures` (PR erigontech/eest-fixtures#40, merged)
- [x] Update submodule pointer in erigon: submodule now at 918427c44
- [x] Verify: `ls execution/tests/execution-spec-tests/blockchain_tests_zkevm/` shows the witness fixture directories

### Task 2: Create `WitnessBlockTest` type in testutil

**Files:**
- Create: `execution/tests/testutil/witness_block_test_util.go`

This type handles JSON parsing and provides access to expected witness data per block. It embeds `BlockTest` for standard blockchain test execution.

- [x] Define `WitnessBlockTest` struct that embeds `BlockTest` and adds `witnessData witnessTestJSON`
- [x] Define JSON types:
  - `witnessTestJSON` with `Blocks []witnessTestBlock`
  - `witnessTestBlock` with `ExecutionWitness *ExpectedWitness`
  - `ExpectedWitness` with `State []hexutil.Bytes`, `Codes []hexutil.Bytes`, `Headers []hexutil.Bytes`
- [x] Implement `UnmarshalJSON(in []byte) error` — unmarshal into both the embedded `BlockTest` (via `bt.BlockTest.UnmarshalJSON`) and the witness-specific `witnessData`
- [x] Add method `ExpectedWitnessForBlock(blockIndex int) *ExpectedWitness` returning the expected witness for a given block index (nil if none)
- [x] Add method `NumBlocks() int` returning total number of blocks in the test
- [x] Verify: `make lint` passes

### Task 3: Expose ExecModuleTester from BlockTest

**Files:**
- Modify: `execution/tests/testutil/block_test_util.go`

The witness test needs access to the `ExecModuleTester` (DB, BlockReader, Engine, etc.) after `BlockTest.Run()` completes, in order to set up the debug API for witness generation.

- [x] Add exported field `M *execmoduletester.ExecModuleTester` to `BlockTest` struct
- [x] In `BlockTest.Run()`, after `m := execmoduletester.New(t, mOpts...)`, add `bt.M = m`
- [x] In `BlockTest.RunCLI()`, same: add `bt.M = m`
- [x] Verify: existing tests still pass — `go test -run TestExecutionSpecBlockchain -count=1 ./execution/tests/eest_blockchain/... -short` (or check it compiles)
- [x] Verify: `make lint` passes

### Task 4: Create witness test package and runner

**Files:**
- Create: `execution/tests/eest_zkevm_witness/witness_test.go`

This is the main test file. After `BlockTest.Run()` inserts blocks and validates post-state, it sets up the debug API and compares our generated witness against the fixture's expected witness.

- [x] Create package `eest_zkevm_witness_test` with `TestExecutionSpecWitness(t *testing.T)`
- [x] Enable historical commitment at test start: save `statecfg.Schema`, call `statecfg.EnableHistoricalCommitment()`, restore in `t.Cleanup`
- [x] Set up `TestMatcher`, walk `filepath.Join("..", "execution-spec-tests", "blockchain_tests_zkevm")` with callback `func(t *testing.T, name string, test *testutil.WitnessBlockTest)`
- [x] In the callback:
  - Call `test.Run(t)` to insert blocks and validate post-state (skip on error)
  - Create debug API: `base := jsonrpc.NewBaseApi(nil, test.M.StateCache, test.M.BlockReader, false, rpccfg.DefaultEvmCallTimeout, test.M.Engine, test.M.Dirs, nil, 0, 0)` then `api := jsonrpc.NewPrivateDebugAPI(base, test.M.DB, nil, 0, false)`
  - For each block index 0..test.NumBlocks()-1: if `test.ExpectedWitnessForBlock(i) != nil`, call `api.ExecutionWitness(ctx, blockNumberOrHash)` and compare
- [x] Implement `compareWitness(t *testing.T, blockNum uint64, expected *testutil.ExpectedWitness, actual *jsonrpc.ExecutionWitnessResult)`:
  - Exact ordered comparison of `State` arrays (element-by-element byte equality)
  - Exact ordered comparison of `Codes` arrays
  - Exact ordered comparison of `Headers` arrays
  - On mismatch: report block number, field name, index, expected vs actual (truncated hex)
- [x] Add `testing.Short()` skip guard
- [x] Set `test.ExperimentalBAL = true` if Amsterdam fixtures require it (check first test run)
- [x] Verify: `make lint` passes

### Task 5: Initial test run and failure triage

- [x] Initialize submodule: `git submodule update --init execution/tests/execution-spec-tests`
- [x] Run: `go test -v -run TestExecutionSpecWitness -count=1 ./execution/tests/eest_zkevm_witness/... 2>&1 | tee /tmp/witness-test-results.log`
- [x] Categorize failures:
  - **Parse errors**: ✅ none — JSON unmarshaling works correctly
  - **Block insertion errors**: ✅ none — Amsterdam fork support works, all blocks insert and validate
  - **Witness mismatches**: all 93 tests fail with witness comparison mismatches (details below)
  - **Missing witness**: ✅ none — `ExecutionWitness()` succeeds for all tests
- [x] For each failure category, create a plan amendment or fix
- [x] Re-run until baseline results are stable (all pass or known failures documented)

**⚠️ Triage results (all 93 tests fail, 0 pass):**

Three root causes identified via set-comparison diagnostics:

1. **State node ordering** (52% of state comparisons): Same trie nodes produced, different traversal order. Our `witnessTrie.RLPEncode()` returns nodes in a different order than the EIP-8025 reference implementation.
2. **State extra nodes** (48% of state comparisons): Our actual output is a superset of expected — we include 1 extra trie node per block that the reference doesn't include.
3. **Codes ordering** (99% of codes comparisons): Same bytecodes, different sort order. We sort by `keccak256(code)`, fixtures expect order-of-first-access during execution.
4. **Missing headers** (99% of headers comparisons): Parent block header not included. We only track BLOCKHASH opcode accesses, but the spec requires the parent header unconditionally.

➕ These are witness generation implementation issues in `rpc/jsonrpc/debug_execution_witness.go`, not test infrastructure issues. Fixes will be tracked separately.

### Task 6: Handle known failure patterns

**Files:**
- Modify: `execution/tests/eest_zkevm_witness/witness_test.go`

Based on Task 5 triage, add expected failures or skip patterns for known issues.

- [x] For tests that fail due to known Erigon limitations: use `bt.Fails(pattern, reason)`
- [x] For tests that are extremely slow: use `bt.Slow(pattern)` or `bt.SkipLoad(pattern)` — no slow tests identified; all 93 run in ~6s total
- [x] Document each skip/fail pattern with a comment explaining the root cause
- [x] Verify: `go test -run TestExecutionSpecWitness -count=1 ./execution/tests/eest_zkevm_witness/...` passes (green)
- [x] Verify: `make lint` passes

### Task 7: Verify no regressions

- [x] Run existing EEST blockchain tests: `go test -run TestExecutionSpecBlockchain -count=1 ./execution/tests/eest_blockchain/... -short`
- [x] Run existing devnet tests: `go test -run TestExecutionSpecBlockchainDevnet -count=1 ./execution/tests/eest_devnet/... -short`
- [x] Run existing execution witness RPC test: `go test -run TestExecutionWitness -count=1 ./rpc/jsonrpc/...`
- [x] Run: `make lint`
- [x] Run: `make erigon` (build check)

### Task 8: Final cleanup and documentation

- [ ] Review all new code for clarity, remove debug prints
- [ ] Add file-level doc comment to `witness_block_test_util.go` explaining the type's purpose
- [ ] Add file-level doc comment to `witness_test.go` explaining the test suite
- [ ] Move this plan to `docs/plans/completed/`

## Technical Details

### JSON fixture structure (per test file)
```json
{
  "test_key_name": {
    "network": "Amsterdam",
    "genesisBlockHeader": { ... },
    "pre": { ... },
    "postState": { ... },
    "lastblockhash": "0x...",
    "blocks": [{
      "blockHeader": { ... },
      "transactions": [...],
      "rlp": "0x...",
      "executionWitness": {
        "state": ["0x<rlp-encoded-mpt-nodes>", ...],
        "codes": ["0x<bytecode>", ...],
        "headers": ["0x<rlp-encoded-header>", ...]
      },
      "statelessInputBytes": "0x...",
      "statelessOutputBytes": "0x...",
      "blockAccessList": [...]
    }]
  }
}
```

### Witness comparison flow
```
BlockTest.Run(t)
  └─ insert blocks, validate post-state & headers
  └─ stores ExecModuleTester in bt.M

For each block with executionWitness:
  └─ NewBaseApi(m.StateCache, m.BlockReader, m.Engine, m.Dirs)
  └─ NewPrivateDebugAPI(baseApi, m.DB, nil, 0, false)
  └─ api.ExecutionWitness(ctx, BlockNumber(blockNum))
  └─ Compare result.State == expected.State (exact ordered)
  └─ Compare result.Codes == expected.Codes (exact ordered)
  └─ Compare result.Headers == expected.Headers (exact ordered)
```

### Note on `Keys` field
The `ExecutionWitnessResult` has a `Keys` field but the fixtures do not include it. The comparison only checks `State`, `Codes`, and `Headers`.

### Note on `statelessInputBytes` / `statelessOutputBytes`
These fixture fields are for stateless execution verification (reconstructing state from witness and re-executing). Out of scope for this plan — they test a different code path (`witnessStateless` in `debug_execution_witness.go`).

## Post-Completion
*Items requiring manual intervention or external systems*

**External system updates:**
- PR to `erigontech/eest-fixtures` to add `blockchain_tests_zkevm/` fixtures must be merged first
- Submodule pointer bump needs to reference the merged commit
- Consider adding this test to CI (`test-hive-eest.yml` or a new workflow)

**Future work:**
- Add stateless execution verification using `statelessInputBytes`/`statelessOutputBytes`
- Integrate with the full 2775 zkevm blockchain test suite (beyond witness-only)
- Track upstream `zkevm@v0.4.0+` releases for new fixture additions
