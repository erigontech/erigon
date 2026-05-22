# Port EIP-8025 Witness Test Suite (from PR #20533)

## Overview
- Port the EIP-8025 execution witness test suite from PR #20533 onto fresh origin/main
- Adds test runner for the zkevm fixtures from ethereum/execution-spec-tests
  (the exact version pin lives in `test-fixtures.json` under key `eest_zkevm`
  and follows the lazy-download manifest machinery introduced in #21002)
- All fixtures are marked as expected failures via `bt.Fails` — documents current witness mismatches
- Test-suite-only scope: no RPC fixes, no signer change, no MDBX changes (those go in separate PRs)

## Context (from discovery)
- Files/components involved: 3 new files, 3 edits to existing files (see Tasks below)
- Dependencies verified on current main:
  - `TestMatcher.Walk/Fails/CheckFailure` in `execution/tests/testutil/matcher.go`
  - `ExecutionWitnessResult` in `rpc/jsonrpc/debug_execution_witness.go`
  - `execmoduletester.ExecModuleTester` in `execution/execmodule/execmoduletester/`
  - `ExperimentalBAL` field on `BlockTest`
  - `statecfg.EnableHistoricalCommitment` in `db/state/statecfg/`
  - Submodule at `ec66995f` already contains `blockchain_tests_zkevm/`
- No submodule pointer change needed — main already references the correct commit
- No merge conflicts expected — all new files or non-overlapping edits

## Development Approach
- **testing approach**: Regular — the PR itself IS a test suite; verification is running the suite
- Copy the 3 new files verbatim from PR branch final state (`origin/awskii/eest-witness`)
- Apply 3 minimal edits to existing files
- Complete each task fully before moving to the next
- **CRITICAL: all tests must pass before starting next task**
- **CRITICAL: update this plan file when scope changes during implementation**

## Testing Strategy
- **unit tests**: the new files ARE the tests — 93 EIP-8025 witness fixtures
- **verification**: `make lint`, `make erigon integration`, run the witness test suite
- All 93 fixtures expected to pass (via `bt.Fails` expected-failure annotation)

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix

## Solution Overview
- `WitnessBlockTest` extends `BlockTest` with per-block witness expectations (state MPT nodes, bytecodes, headers)
- Test runner loads fixtures, executes blocks, calls `api.ExecutionWitness()`, compares output element-by-element
- Mismatches routed through `bt.Fails`/`CheckFailure` so known issues pass green
- CI wiring: add test group + git-lfs pattern for fixture JSON files

## Implementation Steps

### Task 1: Add WitnessBlockTest type

**Files:**
- Create: `execution/tests/testutil/witness_block_test_util.go`

- [x] Create `witness_block_test_util.go` with `WitnessBlockTest` struct (wraps `BlockTest` + witness data)
- [x] Add `ExpectedWitness` struct with State/Codes/Headers fields
- [x] Add `UnmarshalJSON` that deserializes both BlockTest and witness fields
- [x] Add `ExpectedWitnessForBlock(blockIndex)` and `NumBlocks()` accessors
- [x] Verify file compiles: `go build ./execution/tests/testutil/...`

### Task 2: Expose ExecModuleTester from BlockTest

**Files:**
- Modify: `execution/tests/testutil/block_test_util.go`

- [x] Add `M *execmoduletester.ExecModuleTester` field to `BlockTest` struct
- [x] Assign `bt.M = m` in `BlockTest.Run()` after `execmoduletester.New()`
- [x] Assign `bt.M = m` in `BlockTest.RunCLI()` after `execmoduletester.New()`
- [x] Verify existing tests still pass: `go test -short ./execution/tests/testutil/...`

### Task 3: Add witness test runner

**Files:**
- Create: `execution/tests/eest_zkevm_witness/testmain_test.go`
- Create: `execution/tests/eest_zkevm_witness/witness_test.go`

- [x] Create `testmain_test.go` with `TestMain` entrypoint calling `testutil.RunTestMain`
- [x] Create `witness_test.go` with `TestExecutionSpecWitness` function
- [x] Implement `compareWitness` for element-by-element ordered comparison
- [x] Implement `reportSetDiff` for unordered set-diff diagnostics
- [x] Add `bt.Fails(".", ...)` annotation marking all 93 fixtures as expected failures
- [x] Verify file compiles: `go build ./execution/tests/eest_zkevm_witness/...`

### Task 4: Wire CI test group and fixture fetching

**Files:**
- Modify: `tools/test-groups`
- Modify: `.github/actions/setup-erigon/action.yml`

- [x] Add `("execution-eest-zkevm", {"./execution/tests/eest_zkevm_witness/..."})` to `tools/test-groups` between `execution-eest-devnet` and `execution-tests`
- [x] Append `,blockchain_tests_zkevm/**/*.json` to git-lfs pull pattern in `setup-erigon/action.yml`

### Task 5: Lint and build verification

- [x] Run `make lint` — fix any issues (run multiple times, linter is non-deterministic)
- [x] Run `make erigon integration` — verify build passes

### Task 6: Run witness test suite

- [x] Download & extract fixtures: `make test-fixtures-zkevm` (populates `test-fixtures-cache/eest_zkevm/` from the pinned tarball in `test-fixtures.json`)
- [x] Run test suite: `go test -count=1 -v -run TestExecutionSpecWitness ./execution/tests/eest_zkevm_witness/...`
- [x] Verify the corpus runs end-to-end with all mismatches absorbed by `bt.Fails`
- [x] No deviations from expected results

### Task 7: [Final] Move plan to completed

- [x] Move this plan to `docs/plans/completed/`

## Post-Completion

**Separate PRs needed:**
- RPC header logic rewrite (`debug_execution_witness.go`) — fixes parent header inclusion
- Parent header TDD test (`debug_api_test.go`)
- `LatestSigner` change in `stage_senders.go`
- MDBX Windows mapSize + semaphore scaling in `kv_mdbx.go`
- These fixes tracked by #20442 and #20534
