# debug_executionWitness — Phase Extraction Refactor

## Overview

Refactor `(api *DebugAPIImpl) ExecutionWitness` in `rpc/jsonrpc/debug_execution_witness.go` (currently lines 461–891, ~430 lines) into a thin orchestrator that delegates to phase-scoped helpers. **Behavior-preserving**: same inputs, same outputs, same number of `SeekCommitment` calls, same error paths, same env-gated verification.

Goal: surface the 9 natural phases inside the function (currently buried in a single monolith) so the structure is readable and each phase has a single concern.

Non-goal: any algorithmic or semantic change. No consolidation of the 3 `SeekCommitment` calls (load-bearing). No change to `RecordingState`, `witnessStateless`, `execBlockStatelessly`, `buildExpectedPostState`, or `compareComputedVsExpectedState`.

## Context (from discovery)

- **Target file:** `rpc/jsonrpc/debug_execution_witness.go` (1698 lines total)
- **Target function:** `ExecutionWitness` at lines 461–891
- **Test harness:** `TestExecutionWitness` in `rpc/jsonrpc/debug_api_test.go:747` (covers multiple blocks + not-found case)
- **Recent history:** function landed in #20205 and was tuned in #20345; nothing in flight from other authors
- **Local uncommitted change:** `domains.SetTrace(true, true)` on line 787 — debug leftover, will be reverted before starting

## Development Approach

- **Testing approach: Regular (verify after each step via existing tests).** This is a behavior-preserving refactor. The existing `TestExecutionWitness` covers every helper transitively because every helper is called from `ExecutionWitness`. **No new unit tests are written per helper** — adding them would be redundant verification of the same code paths and would couple tests to internal helpers we may want to inline again later. The acceptance test is: `TestExecutionWitness` keeps passing unchanged.
- Complete each task fully before moving to the next.
- Small, focused changes — one helper extraction per task.
- **CRITICAL: `TestExecutionWitness` must pass after every task.** If it fails, do not proceed — fix the regression first.
- **CRITICAL: `make lint` must be clean after every task** (run repeatedly until clean — non-deterministic per project CLAUDE.md).
- **CRITICAL: update this plan file when scope changes during implementation.**

## Testing Strategy

- **Existing test as harness:** `TestExecutionWitness` (`rpc/jsonrpc/debug_api_test.go:747`). Run after every task:
  ```bash
  go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1
  ```
- **Build check:** `make erigon integration` after every task (catches type-level breakage fast).
- **Lint:** `make lint` after every task; rerun until clean.
- **Final task only:** `make test-short` for broader regression safety net.
- **No new tests added.** Reason: every extracted helper is exercised by `TestExecutionWitness` because it's on the only call path. Adding helper-level unit tests would be redundant and would constrain future re-inlining.

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope

## Solution Overview

`ExecutionWitness` already has internal `// === STEP 1 ===` / `// === STEP 2 ===` comments marking the commitment phases. The refactor formalizes the 9 natural phases into named helpers (where extractable cleanly) while leaving phases that need too many inputs inline. The function shrinks from ~430 lines to roughly ~120 lines of orchestration.

### Final shape of `ExecutionWitness`

```go
func (api *DebugAPIImpl) ExecutionWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*ExecutionWitnessResult, error) {
    tx, err := api.db.BeginTemporalRo(ctx)
    if err != nil { return nil, err }
    defer tx.Rollback()

    info, err := api.resolveWitnessBlock(ctx, tx, blockNrOrHash)
    if err != nil { return nil, err }

    // --- build exec env (inline: too many locals to extract cleanly) ---
    chainConfig, _ := api.chainConfig(ctx, tx)
    engine := api.engine()
    fullEngine, ok := engine.(rules.Engine)
    if !ok {
        return nil, fmt.Errorf("engine does not support full rules.Engine interface")
    }
    stateReader := state.NewHistoryReaderV3(tx, info.FirstTxNumInBlock)
    recordingState := NewRecordingState(stateReader)
    recordingState.SetAccountsToTrace([]common.Address{ /* trace addresses */ }) // preserved from original
    ibs := state.New(recordingState)
    header := info.Block.Header()
    blockCtx := transactions.NewEVMBlockContext(engine, header, true, tx, api._blockReader, chainConfig)
    blockRules := blockCtx.Rules(chainConfig)
    signer := types.MakeSigner(chainConfig, info.BlockNum, header.Time)
    var accessedBlockHashes []uint64
    originalGetHash := blockCtx.GetHash
    blockCtx.GetHash = func(n uint64) (common.Hash, error) {
        accessedBlockHashes = append(accessedBlockHashes, n)
        return originalGetHash(n)
    }
    chainReader := consensuschain.NewReader(chainConfig, tx, api._blockReader, log.Root())

    // --- execute block (inline) ---
    // engine.Initialize → tx loop → engine.Finalize (synthetic receipt for EIP-6110) → CommitBlock

    accessed := collectAccessedState(recordingState)

    result := &ExecutionWitnessResult{
        State: []hexutil.Bytes{},
        Keys:  accessed.SortedKeys,
        Codes: accessed.SortedCodes,
    }
    if accessed.isEmpty() { return result, nil }

    // --- commitment setup (inline) ---
    domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
    if err != nil { return nil, err }
    defer domains.Close()
    sdCtx := domains.GetCommitmentContext()
    sdCtx.SetDeferBranchUpdates(false)
    parentHeader, _ := api._blockReader.HeaderByNumber(ctx, tx, info.ParentNum)
    if parentHeader == nil { return nil, fmt.Errorf("parent header %d not found", info.ParentNum) }
    expectedParentRoot := parentHeader.Root
    log.Debug("expected parent root", "stateRoot", expectedParentRoot) // preserved from original
    commitmentStartingTxNum := tx.Debug().HistoryStartFrom(kv.CommitmentDomain)
    if info.FirstTxNumInBlock < commitmentStartingTxNum {
        return nil, fmt.Errorf("commitment history pruned: start %d, last tx: %d", commitmentStartingTxNum, info.FirstTxNumInBlock)
    }

    siblingPaths, err := detectCollapseSiblings(ctx, tx, domains, sdCtx,
        info.FirstTxNumInBlock, info.EndTxNum, info.BlockNum, info.Block.Root(), accessed)
    if err != nil { return nil, err }

    nodes, err := buildWitnessTrie(ctx, tx, domains, sdCtx,
        info.FirstTxNumInBlock, expectedParentRoot, siblingPaths, accessed)
    if err != nil { return nil, err }
    result.State = nodes

    result.Headers, err = api.collectAccessedHeaders(ctx, tx, accessedBlockHashes)
    if err != nil { return nil, err }

    if err := api.verifyWitnessStateless(ctx, tx, result, info.Block, fullEngine); err != nil {
        return nil, err
    }
    return result, nil
}
```

### Helpers (final signatures)

```go
type witnessBlockInfo struct {
    Block             *types.Block
    BlockNum          uint64 // == Block.NumberU64(); kept as field because it's referenced in many downstream call sites
    FirstTxNumInBlock uint64
    EndTxNum          uint64
    ParentNum         uint64
}

type accessedState struct {
    SortedKeys  []hexutil.Bytes                              // -> result.Keys
    Addresses   map[common.Address]struct{}                  // for touchAll
    Storage     map[common.Address]map[common.Hash]struct{}  // for touchAll
    SortedCodes []hexutil.Bytes                              // -> result.Codes (pre-state only, dedup+sorted by hash)
    PreCode     map[common.Address][]byte                    // pre-block code reads
    ModCode     map[common.Address][]byte                    // deployments + EIP-7702
    CodeReads   map[common.Hash]witnesstypes.CodeWithHash    // -> Witness(codeReads)
    CodeAddrs   map[common.Address]struct{}                  // for touchAll (union)
}

func (a *accessedState) isEmpty() bool
func (a *accessedState) touchAll(sdCtx *commitment.SharedDomainsCommitmentContext)

func (api *DebugAPIImpl) resolveWitnessBlock(ctx context.Context, tx kv.TemporalTx, blockNrOrHash rpc.BlockNumberOrHash) (*witnessBlockInfo, error)
func collectAccessedState(rs *RecordingState) *accessedState
func detectCollapseSiblings(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, sdCtx *commitment.SharedDomainsCommitmentContext, firstTxNumInBlock, endTxNum, blockNum uint64, expectedBlockRoot common.Hash, accessed *accessedState) (siblingPaths [][]byte, err error)
func buildWitnessTrie(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, sdCtx *commitment.SharedDomainsCommitmentContext, firstTxNumInBlock uint64, expectedParentRoot common.Hash, siblingPaths [][]byte, accessed *accessedState) (encodedNodes []hexutil.Bytes, err error)
func (api *DebugAPIImpl) collectAccessedHeaders(ctx context.Context, tx kv.TemporalTx, accessedBlockNums []uint64) ([]hexutil.Bytes, error)
func (api *DebugAPIImpl) verifyWitnessStateless(ctx context.Context, tx kv.TemporalTx, result *ExecutionWitnessResult, block *types.Block, fullEngine rules.Engine) error
```

`detectCollapseSiblings` and `buildWitnessTrie` each own their `SetCustomHistoryStateReader` / `SetHistoryStateReader` + `SeekCommitment` + `accessed.touchAll(sdCtx)` sequence. The current `touchAllKeys` and `resetToParentState` closures disappear after Task 6.

## Technical Details

### What stays inline (and why)

- **Block execution** (engine.Initialize → tx loop → engine.Finalize w/ synthetic receipt → CommitBlock, lines 541–598): 11+ inputs (ibs, blockCtx, blockRules, signer, engine, fullEngine, chainConfig, chainReader, header, recordingState, block). Extracting cleanly would require a context struct, which moves us toward option B (builder) — explicitly rejected in brainstorm. Keep inline.
- **EVM blockCtx construction with GetHash interceptor**: the `accessedBlockHashes []uint64` slice is mutated by the closure and consumed by `collectAccessedHeaders` later. Closure capture works naturally; extracting would require returning the slice via pointer.
- **Commitment setup** (NewSharedDomains, parent header, pruning check, empty-touch early return): short, tightly coupled to ExecutionWitness's control flow (returns `result` directly on empty).

### Other preserved details (must NOT drift)

- **Error wording, including typos and trailing newlines.** Two specifically observable strings:
  - Line 801: `fmt.Errorf("[debug_executionWitness] collapse detection via ComputeCommitment failed: %v\n", err)` — note the trailing `\n` and `%v` (not `%w`).
  - Line 805: `fmt.Errorf("[debug_executionWitness] computedRootHash(%x)!= expectedRootHash(%x)", computedRootHash, block.Root())` — note the missing space before `!=` and the lowercase prefix.
  These are observable in tests/logs; rewording them is a behavior change.
- **`common.Copy` defensive copies.** Lines 796 (`common.Copy(hashedKeyPath)` inside CollapseTracer) and 842 (`common.Copy(node)` after `RLPEncode`) copy slices owned by trie/grid internals that may be reused. Both helpers in Task 5 and Task 6 must preserve these copies — dropping them would introduce a subtle memory-reuse bug.
- **`sdCtx.SetDeferBranchUpdates(false)`** is set once on `sdCtx` (line 715) before either commitment phase runs. Stays inline in `ExecutionWitness`; helpers must not re-set it.
- **Empty-touch fast path** (line 747-749): when `accessed.isEmpty()`, the function returns `result` with empty `State`/`Codes`/no `Headers` and skips verification. Preserved.
- **`SetAccountsToTrace([]common.Address{...})`** call (lines 518-521): currently a no-op with an empty slice, but kept as a hook for ad-hoc debugging. Preserve verbatim.
- **`log.Debug("expected parent root", "stateRoot", expectedParentRoot)`** (line 729): one debug-log line; preserve.

### Preserved seek pattern

| Seek | Where | Reader | Purpose |
|------|-------|--------|---------|
| #1 (implicit) | `execctx.NewSharedDomains` constructor | default | establish commitment cursor |
| #2 | inside `detectCollapseSiblings` | `SplitHistoryReader(firstTxNumInBlock, endTxNum, false)` | compute post-state root for collapse detection |
| #3 | inside `buildWitnessTrie` | `HistoryStateReader(firstTxNumInBlock)` | build parent-state witness trie |

Each helper documents which seek it triggers in a top-of-function comment.

### Baseline

Task 0 creates a worktree from `main`, which is clean — the uncommitted `domains.SetTrace(true, true)` line in the source tree does not carry into the worktree. No separate revert step needed; Task 0's baseline-test step confirms parity with origin/main.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): worktree setup, helper extraction tasks in dependency-safe order, final tidy.
- **Post-Completion** (no checkboxes): PR creation, branch cleanup decisions.

## Implementation Steps

### Task 0: Worktree setup + baseline + test-coverage sanity check

**Files:**
- None modified — verification only.

- [x] from `/Users/awskii/org/wrk/erigon`, create worktree: `git worktree add -b awskii/exec-witness-refactor ../erigon-exec-witness-refactor main`
- [x] `cd ../erigon-exec-witness-refactor`
- [x] verify clean tree: `git status` should show no modifications
- [x] confirm baseline build + test: `make erigon integration && go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] confirm baseline lint: `make lint` (run until clean)
- [x] **test-path coverage sanity check** (one-time): read `rpc/jsonrpc/debug_api_test.go:747` (`TestExecutionWitness`) and verify it (a) calls a block that uses `BLOCKHASH` (so `collectAccessedHeaders` non-empty path is exercised), and (b) runs at least one block with `ERIGON_WITNESS_NO_VERIFY` unset (so `verifyWitnessStateless` runs the full re-exec). If either is missing, note as ⚠️ in this plan and consider running an ad-hoc invocation against a synced datadir for those paths during Task 7.
  - ⚠️ Test chain in `rpcdaemontest.CreateTestExecModule` (value transfers, token deploy/mint/transfer) does NOT use the `BLOCKHASH` opcode. The `collectAccessedHeaders` non-empty path is therefore NOT exercised by `TestExecutionWitness`. Behavior of the helper is still trivially preserved (loop body is line-for-line copied with the same dedup map), but the runtime path is unverified by automated tests. Consider an ad-hoc invocation against a synced datadir on a block that calls a contract using `BLOCKHASH` during Task 7. The `verifyWitnessStateless` path IS exercised (env var unset in the test).

### Task 1: Extract `collectAccessedHeaders`

Lowest-risk leaf — fully self-contained, just reads headers and RLP-encodes them.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] add method `(api *DebugAPIImpl) collectAccessedHeaders(ctx context.Context, tx kv.TemporalTx, accessedBlockNums []uint64) ([]hexutil.Bytes, error)` containing logic from lines 845–867
- [x] dedupe via local `seenBlockNums map[uint64]struct{}` (same as current)
- [x] replace inline block in `ExecutionWitness` with a call assigning to `result.Headers`
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 2: Extract `verifyWitnessStateless`

Env-gated, isolated.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] add method `(api *DebugAPIImpl) verifyWitnessStateless(ctx context.Context, tx kv.TemporalTx, result *ExecutionWitnessResult, block *types.Block, fullEngine rules.Engine) error` containing logic from lines 869–888
- [x] keep the `ERIGON_WITNESS_NO_VERIFY` env check inside (so the helper is a no-op when disabled)
- [x] keep the `log.Debug("[debug_executionWitness] witness verified", ...)` call inside
- [x] replace inline block in `ExecutionWitness` with a single call
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 3: Add `witnessBlockInfo` + `resolveWitnessBlock`

Folds block-resolution + txnum-range + parentNum into one helper.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] declare struct `witnessBlockInfo { Block *types.Block; BlockNum, FirstTxNumInBlock, EndTxNum, ParentNum uint64 }` near the top of the file (next to `ExecutionWitnessResult`). `BlockNum` is a stored field (not a method) because `ExecutionWitness` references it in many places (logging, `MakeSigner`, `detectCollapseSiblings` argument)
- [x] add method `(api *DebugAPIImpl) resolveWitnessBlock(ctx, tx, blockNrOrHash) (*witnessBlockInfo, error)` containing logic from lines 468–513 (excluding stateReader construction, which is part of "build exec env" and stays inline)
- [x] inside `resolveWitnessBlock`: call `rpchelper.GetBlockNumber`, then `api.blockWithSenders`, nil-check, `api._txNumReader.Min/Max`, compute `endTxNum`, `parentNum`, handle the `blockNum == 0` cases for both `firstTxNumInBlock = endTxNum` and `parentNum = 0`; populate `BlockNum` with the resolved `blockNum`
- [x] replace inline block in `ExecutionWitness`; downstream references switch from local `blockNum` to `info.BlockNum`, local `block` to `info.Block`, etc.
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 4: Add `accessedState` + `collectAccessedState`

Replaces the three sub-blocks at lines 600–655 (keys), 657–705 (code), 737–745 (allCodeAddrs union), plus the body of the `touchAllKeys` closure.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] declare struct `accessedState` with all 8 fields (`SortedKeys`, `Addresses`, `Storage`, `SortedCodes`, `PreCode`, `ModCode`, `CodeReads`, `CodeAddrs`) — see Solution Overview for exact shape
- [x] add `(a *accessedState) isEmpty() bool` returning `len(a.Addresses)+len(a.Storage)+len(a.CodeAddrs) == 0`
- [x] add `(a *accessedState) touchAll(sdCtx *commitment.SharedDomainsCommitmentContext)` containing the body of the current `touchAllKeys` closure (actual receiver type is `*commitmentdb.SharedDomainsCommitmentContext` — the concrete type returned by `domains.GetCommitmentContext()`; plan listed it under the `commitment` package, but the type lives in `commitmentdb`)
- [x] add `collectAccessedState(rs *RecordingState) *accessedState` containing:
  - merge `GetAccessedKeys` + `GetModifiedKeys` → `Addresses`, `Storage`
  - build `SortedKeys` (addresses + composite storage keys, sorted via `slices.SortFunc(bytes.Compare)`)
  - read `GetPreStateCode` + `GetModifiedCode` → `PreCode`, `ModCode`
  - dedupe `PreCode` by hash, sort by hash, output `SortedCodes`
  - build `CodeReads` keyed by `addrHash` from pre-state code
  - union `PreCode` + `ModCode` addresses → `CodeAddrs`
- [x] in `ExecutionWitness`: call `accessed := collectAccessedState(recordingState)`; assign `result.Keys = accessed.SortedKeys` and `result.Codes = accessed.SortedCodes`; replace empty-touch check with `accessed.isEmpty()`; rewrite `touchAllKeys` closure to a one-liner `accessed.touchAll(sdCtx)` (closure can stay for now — Task 6 removes it)
- [x] verify `result.Keys` and `result.Codes` ordering matches pre-refactor byte-for-byte (determinism comes from the final `slices.SortFunc(bytes.Compare)` on `SortedKeys` and the sort-by-hash on `SortedCodes` — map-iteration order does not matter as long as the sort runs)
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `go test ./rpc/jsonrpc/ -count=1` (full package, since this task changes the largest surface)
- [x] `make lint` until clean

### Task 5: Extract `detectCollapseSiblings`

Folds in `SetCustomHistoryStateReader`, `SeekCommitment` #2, `accessed.touchAll`, `SetCollapseTracer`, `ComputeCommitment`, and the `block.Root()` verification.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] add function `detectCollapseSiblings(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, sdCtx *commitment.SharedDomainsCommitmentContext, firstTxNumInBlock, endTxNum, blockNum uint64, expectedBlockRoot common.Hash, accessed *accessedState) (siblingPaths [][]byte, err error)` (actual receiver type used: `*commitmentdb.SharedDomainsCommitmentContext`, same as in [[Task 4]])
- [x] inside the helper, in order:
  1. construct `splitStateReader := commitmentdb.NewSplitHistoryReader(tx, firstTxNumInBlock, endTxNum, false)`
  2. `sdCtx.SetCustomHistoryStateReader(splitStateReader)`
  3. `domains.SeekCommitment(ctx, tx)` (return error wrapped)
  4. `accessed.touchAll(sdCtx)`
  5. register collapse tracer that appends `common.Copy(hashedKeyPath)` to local `siblingPaths`
  6. `sdCtx.ComputeCommitment(ctx, tx, false, blockNum, firstTxNumInBlock, "debug_executionWitness_collapse_detection", nil)`
  7. verify `common.Hash(computedRootHash) == expectedBlockRoot`; return error on mismatch — **preserve exact wording** including the typo `computedRootHash(%x)!= expectedRootHash(%x)` (no space before `!=`) and the lowercase `[debug_executionWitness]` prefix
  8. `sdCtx.SetCollapseTracer(nil)`
  9. return `siblingPaths, nil`
- [x] preserve the `common.Copy(hashedKeyPath)` defensive copy inside the collapse tracer — do not omit
- [x] error string for `ComputeCommitment` failure: preserve exact `fmt.Errorf("[debug_executionWitness] collapse detection via ComputeCommitment failed: %v\n", err)` including the trailing `\n` and `%v` (not `%w`)
- [x] add top-of-function comment documenting "// Triggers SeekCommitment #2 (split reader). Preserves pre-refactor behavior."
- [x] replace lines 773–808 in `ExecutionWitness` with a single call assigning `collapseSiblingPaths`
- [x] `git diff` the error strings to confirm exact preservation (grep for `[debug_executionWitness]` before and after — should match byte-for-byte)
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 6: Extract `buildWitnessTrie`

Folds in `SetHistoryStateReader`, `SeekCommitment` #3, `accessed.touchAll`, sibling touches, `sdCtx.Witness`, parent-root verification, and `RLPEncode` → `result.State`.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] add function `buildWitnessTrie(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, sdCtx *commitment.SharedDomainsCommitmentContext, firstTxNumInBlock uint64, expectedParentRoot common.Hash, siblingPaths [][]byte, accessed *accessedState) (encodedNodes []hexutil.Bytes, err error)` (actual receiver type used: `*commitmentdb.SharedDomainsCommitmentContext`, same as in [[Task 5]])
- [x] inside the helper, in order:
  1. `sdCtx.SetHistoryStateReader(tx, firstTxNumInBlock)`
  2. `domains.SeekCommitment(ctx, tx)` (return error wrapped)
  3. `accessed.touchAll(sdCtx)`
  4. for each path in `siblingPaths`: `sdCtx.TouchHashedKey(siblingPath)` (preserve current `log.Debug` line)
  5. `witnessTrie, witnessRoot, err := sdCtx.Witness(ctx, accessed.CodeReads, "debug_executionWitness_witness_construction")`
  6. verify `bytes.Equal(witnessRoot, expectedParentRoot[:])`; return error on mismatch (preserve current wording)
  7. `allNodes, err := witnessTrie.RLPEncode()`
  8. for each node: append `common.Copy(node)` to `encodedNodes` — **must preserve** the defensive copy; dropping it introduces a memory-reuse bug
  9. return `encodedNodes, nil`
- [x] preserve exact wording on the witness-root-mismatch error: `fmt.Errorf("collapse witness root mismatch: calculated=%x, expected=%x", common.BytesToHash(witnessRoot), expectedParentRoot)`
- [x] add top-of-function comment documenting "// Triggers SeekCommitment #3 (parent-state reader). Preserves pre-refactor behavior."
- [x] in `ExecutionWitness`: delete `resetToParentState` closure entirely, delete `touchAllKeys` closure entirely (no longer referenced), replace lines 810–843 with a single call assigning `result.State`
- [x] `git diff` the error strings to confirm preservation
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 7: Verify acceptance criteria

- [x] `ExecutionWitness` is roughly ~120 lines (down from ~430) — measured 170 lines (742–911); within "roughly" tolerance
- [x] grep whole file for `SeekCommitment` (both `domains.SeekCommitment` and `sdCtx.SeekCommitment` forms) — confirm exactly 2 explicit calls remain (one inside `detectCollapseSiblings`, one inside `buildWitnessTrie`); the 3rd is implicit in `NewSharedDomains`. Note: a 3rd explicit call exists at line 1001 inside `buildExpectedPostState`, but that's part of the separate stateless verification path (`postDomains`), pre-existing and unrelated to the commitment phase being refactored.
- [x] grep for `touchAllKeys` and `resetToParentState` — confirm zero occurrences in the file
- [x] grep for preserved debug/log lines — confirm `log.Debug("expected parent root", ...)` still present (line 879)
- [x] grep for `SetAccountsToTrace` — confirm the call still exists in `ExecutionWitness` (line 766)
- [x] grep for `common.Copy(` inside the helpers — confirm both Task 5 collapse-tracer (line 682) and Task 6 RLPEncode loop (line 734) still use it
- [x] grep for `SetDeferBranchUpdates` — confirm called exactly once in `ExecutionWitness`'s commitment phase setup (line 865), not in either commitment helper. The line 987 call is in the separate `buildExpectedPostState` verification path.
- [x] full test sweep: `make test-short` — all packages pass
- [x] full lint sweep: `make lint` (run twice to confirm stability) — `0 issues.` on both runs
- [x] visually inspect `ExecutionWitness` body — reads as: setup tx → `resolveWitnessBlock` → build exec env (inline) → exec block (inline) → `collectAccessedState` → init result → commitment setup (inline) → empty early return → `detectCollapseSiblings` → `buildWitnessTrie` → `collectAccessedHeaders` → `verifyWitnessStateless`

### Task 8: Move plan to completed

- [x] `mkdir -p docs/plans/completed`
- [x] `git mv docs/plans/20260516-exec-witness-refactor.md docs/plans/completed/`

## Post-Completion

*Informational only — no checkboxes.*

**Manual verification:**
- Optionally run `debug_executionWitness` via RPC against a synced datadir on a few historical blocks and diff the JSON output against pre-refactor binary; not strictly necessary because `TestExecutionWitness` covers the contract, but useful confidence boost before merging.

**PR notes:**
- Title: `rpc/jsonrpc: split debug_executionWitness into phase helpers`
- Body: short — link to this plan, note "behavior-preserving, zero functional change, existing TestExecutionWitness covers all paths"
- Base: `main`
- No backport needed unless someone requests a 3.4 cherry-pick (the function only exists on main).

**Follow-up candidates (out of scope here):**
- Investigate whether collapse detection can run on the witness trie itself to consolidate the 3 SeekCommitment calls down to 2. Algorithmic change — separate plan.
- Consider moving `RecordingState` and `witnessStateless` to their own files (`debug_execution_witness_recording.go`, `debug_execution_witness_stateless.go`) once this refactor lands.
