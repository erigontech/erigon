# debug_executionWitness — Phase Extraction Refactor (on top of lupin012)

## Overview

Apply the phase-extraction refactor shipped in [PR #21227](https://github.com/erigontech/erigon/pull/21227) on top of `lupin012/add_tests_for_debug_executionWitnes`, which contains a separate behavior fix for `result.Codes` (codes now sourced from `recordingState.GetAccessedCode()` instead of `preStateCode+modifiedCode`, matching Geth's `witness.AddCode` semantics).

**Behavior-preserving.** Same inputs, same outputs, same number of `SeekCommitment` calls, same error wording, same env-gated verification. The only goal is to surface the 9 natural phases inside `ExecutionWitness` (currently a ~440-line monolith on this branch) so each phase has a single concern.

**No effect on PR #21227 or the lupin012 branch.** The refactor lands on a brand-new branch (`awskii/exec-witness-refactor-on-lupin`) created via a worktree off `lupin012/add_tests_for_debug_executionWitnes`. PR #21227 stays in flight unmodified; the lupin012 branch is unchanged.

## Context (from discovery)

- **Reference PR:** #21227 — "rpc/jsonrpc: split debug_executionWitness into phase helpers". Same 6 helpers + 2 structs are mirrored here, with each helper's body retargeted to lupin012's logic.
- **Reference plan (read-only, do NOT rewrite):** `docs/plans/20260516-exec-witness-refactor.md` — the original plan that drove PR #21227. Use it for the task-shape template; the lupin012 deltas listed below are what's different.
- **Base branch:** `lupin012/add_tests_for_debug_executionWitnes` (currently checked out at HEAD `64a905aabb`).
- **Target file:** `rpc/jsonrpc/debug_execution_witness.go` (lupin012 version, 1738 lines).
- **Target function:** `ExecutionWitness` at `rpc/jsonrpc/debug_execution_witness.go:510–949` (~440 lines).
- **Test harness:** `TestExecutionWitness` in `rpc/jsonrpc/debug_api_test.go:747` — exercises every helper transitively because they're all on the only call path.
- **New branch:** `awskii/exec-witness-refactor-on-lupin`.
- **Worktree path:** `../erigon-exec-witness-refactor-lupin` (sibling to `/Users/awskii/org/wrk/erigon`).

### Lupin012 deltas vs PR #21227 baseline (the entire point — must be preserved verbatim)

1. **Mandatory `commitmentHistoryEnabled` precondition** at top of `ExecutionWitness` (`rpc/jsonrpc/debug_execution_witness.go:517-523`): calls `rawdb.ReadDBCommitmentHistoryEnabled(tx)`, returns `"debug_executionWitness requires commitment history: restart the node with --prune.experimental.include-commitment-history"` if disabled. Stays inline — not extracted.
2. **`result.Codes` sourced from `accessedCode`** (`rpc/jsonrpc/debug_execution_witness.go:701-730`): bytecode is now collected only for contracts actually called (`GetCode`/`GetCodeSize`), not all deployed contracts. The `codeWithHash` struct, `allCodesByHash` dedup, and `slices.SortFunc` by hash all stay.
3. **`ExecutionWitnessResult` has `headerByNumber map[uint64]*types.Header`** (replaces the old `rawHeaders []*types.Header`) plus a `getHashFn(blockNum) (common.Hash, error)` method (`rpc/jsonrpc/debug_execution_witness.go:485-505` — struct at 485-498, method at 500-505). The map is populated by `addHeader` at runtime and read by stateless verification. Also note `Codes` has `json:"codes"` (NO `omitempty`) — nil emits `"codes": null`, which is a behavior regression we must avoid (see Task 4 critical note). `Headers` has `json:"headers,omitempty"`. `Keys` has `json:"keys"` (no omitempty, intentionally always-null per Geth compat — preserve as nil).
4. **`marshalWitnessHeader`** is the only allowed header encoder (`rpc/jsonrpc/debug_execution_witness.go:459-482`). It returns `map[string]any` (NOT `[]hexutil.Bytes`), handles `balHash`, null-fills `blobGasUsed`/`excessBlobGas`/`parentBeaconBlockRoot`/`requestsHash`/`slotNumber`/`withdrawalsRoot`, and removes `size`.
5. **Parent header added FIRST**, then BLOCKHASH-accessed blocks deduped via `result.headerByNumber` (`rpc/jsonrpc/debug_execution_witness.go:900-925`).
6. **`seekBlockNum != parentNum` parent verification** inside STEP 1 (`rpc/jsonrpc/debug_execution_witness.go:837-842`). Exact multi-line wording must be preserved:
   ```go
   fmt.Errorf(
       "debug_executionWitness: commitment trie for block %d is at block %d instead of parent %d; "+
           "commitment history may be pruned for this block range",
       blockNum, seekBlockNum, parentNum)
   ```
7. **`callCodeAccessHook` lives in `execution/state/intra_block_state.go`** — out of scope for this refactor. Do not touch.

### Other invariants carried over from PR #21227's plan

- Error wordings VERBATIM, including typos:
  - `"[debug_executionWitness] collapse detection via ComputeCommitment failed: %v\n"` — note `%v` (not `%w`) and trailing `\n`.
  - `"[debug_executionWitness] computedRootHash(%x)!= expectedRootHash(%x)"` — note missing space before `!=`.
  - `"collapse witness root mismatch: calculated=%x, expected=%x"`.
- `common.Copy` defensive copies in the collapse tracer (`hashedKeyPath`) AND the RLP encode loop (`node`). Removing either introduces a memory-reuse bug.
- `sdCtx.SetDeferBranchUpdates(false)` called exactly ONCE in `ExecutionWitness`, not inside helpers.
- Empty-touch fast path: when `accessed.isEmpty()` is true, return `result` with empty `State`/`Codes` BEFORE Headers are populated and BEFORE verify runs.
- `SetAccountsToTrace([]common.Address{})` call preserved verbatim — debug hook with no current callers.
- `log.Debug("expected parent root", "stateRoot", expectedParentRoot)` line preserved.
- Exactly 3 `SeekCommitment` calls remain after refactor: 1 implicit in `execctx.NewSharedDomains`, 1 explicit inside `detectCollapseSiblings`, 1 explicit inside `buildWitnessTrie`.

## Development Approach

- **Testing approach: Regular (existing test as harness, no new tests).** This is a behavior-preserving refactor of a single function. Every extracted helper is on the only call path of `TestExecutionWitness`, so it's exercised end-to-end after each task. Adding helper-level unit tests would (a) duplicate coverage of the existing test and (b) couple tests to internal structure we may want to re-inline later. **This decision intentionally diverges from the planning skill's default "every task adds tests" rule** — it mirrors PR #21227's approved strategy and was confirmed in brainstorm.
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
- **After Task 4 only:** full package test — `go test ./rpc/jsonrpc/ -count=1` — because Task 4 has the largest surface change (`accessedState` + `collectAccessedState`).
- **Final task only:** `make test-short` for broader regression safety net.
- **No new unit tests added.** See Development Approach for rationale.

### Coverage caveat (carried over from PR #21227)

`TestExecutionWitness` likely does not exercise the BLOCKHASH-touched path and may run with `ERIGON_WITNESS_NO_VERIFY` set. Task 0 includes a one-time read of the test fixtures to confirm or flag this as a ⚠️ — if either path is missing, document in this plan and consider ad-hoc validation against a synced datadir during Task 7.

## Progress Tracking

- mark completed items with `[x]` immediately when done
- add newly discovered tasks with ➕ prefix
- document issues/blockers with ⚠️ prefix
- update plan if implementation deviates from original scope
- keep plan in sync with actual work done

## Solution Overview

`ExecutionWitness` shrinks from ~440 lines to ~130 lines of orchestration. Final shape:

```go
func (api *DebugAPIImpl) ExecutionWitness(ctx context.Context, blockNrOrHash rpc.BlockNumberOrHash) (*ExecutionWitnessResult, error) {
    tx, err := api.db.BeginTemporalRo(ctx)
    if err != nil { return nil, err }
    defer tx.Rollback()

    // --- lupin012 commitment-history precondition (INLINE; not extracted) ---
    commitmentHistoryEnabled, _, err := rawdb.ReadDBCommitmentHistoryEnabled(tx)
    if err != nil { return nil, err }
    if !commitmentHistoryEnabled {
        return nil, fmt.Errorf("debug_executionWitness requires commitment history: restart the node with --prune.experimental.include-commitment-history")
    }

    info, err := api.resolveWitnessBlock(ctx, tx, blockNrOrHash)
    if err != nil { return nil, err }

    // --- build exec env (INLINE: 11+ locals, extraction rejected) ---
    chainConfig, _ := api.chainConfig(ctx, tx)
    engine := api.engine()
    fullEngine, ok := engine.(rules.Engine)
    if !ok { return nil, fmt.Errorf("engine does not support full rules.Engine interface") }
    stateReader := state.NewHistoryReaderV3(tx, info.FirstTxNumInBlock)
    recordingState := NewRecordingState(stateReader)
    recordingState.SetAccountsToTrace([]common.Address{ /* debug hook, preserved */ })
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

    // --- execute block (INLINE) ---
    // engine.Initialize → tx loop → engine.Finalize (synthetic receipt for EIP-6110) → CommitBlock

    accessed := collectAccessedState(recordingState)

    result := &ExecutionWitnessResult{
        State:          []hexutil.Bytes{},
        Codes:          accessed.SortedCodes,
        headerByNumber: make(map[uint64]*types.Header),
    }
    if accessed.isEmpty() { return result, nil }

    // --- commitment setup (INLINE) ---
    domains, err := execctx.NewSharedDomains(ctx, tx, log.New())
    if err != nil { return nil, err }
    defer domains.Close()
    sdCtx := domains.GetCommitmentContext()
    sdCtx.SetDeferBranchUpdates(false)
    parentHeader, _ := api._blockReader.HeaderByNumber(ctx, tx, info.ParentNum)
    if parentHeader == nil { return nil, fmt.Errorf("parent header %d not found", info.ParentNum) }
    expectedParentRoot := parentHeader.Root
    log.Debug("expected parent root", "stateRoot", expectedParentRoot)
    commitmentStartingTxNum := tx.Debug().HistoryStartFrom(kv.CommitmentDomain)
    if info.FirstTxNumInBlock < commitmentStartingTxNum {
        return nil, fmt.Errorf("commitment history pruned: start %d, last tx: %d", commitmentStartingTxNum, info.FirstTxNumInBlock)
    }

    siblingPaths, err := detectCollapseSiblings(ctx, tx, domains, sdCtx,
        info.FirstTxNumInBlock, info.EndTxNum, info.BlockNum, info.ParentNum,
        info.Block.Root(), accessed)
    if err != nil { return nil, err }

    nodes, err := buildWitnessTrie(ctx, tx, domains, sdCtx,
        info.FirstTxNumInBlock, expectedParentRoot, siblingPaths, accessed)
    if err != nil { return nil, err }
    result.State = nodes

    headers, byNumber, err := api.collectAccessedHeaders(ctx, tx, info.ParentNum, accessedBlockHashes)
    if err != nil { return nil, err }
    result.Headers = headers
    result.headerByNumber = byNumber

    if err := api.verifyWitnessStateless(ctx, tx, result, info.Block, fullEngine); err != nil {
        return nil, err
    }
    return result, nil
}
```

## Technical Details

### Helper signatures (final)

```go
type witnessBlockInfo struct {
    Block             *types.Block
    BlockNum          uint64 // == Block.NumberU64(); kept as field — referenced many times downstream
    FirstTxNumInBlock uint64
    EndTxNum          uint64
    ParentNum         uint64
}

type accessedState struct {
    SortedKeys  []hexutil.Bytes                              // serialized to result.Keys if/when added
    Addresses   map[common.Address]struct{}                  // touchAll → AccountsDomain
    Storage     map[common.Address]map[common.Hash]struct{}  // touchAll → StorageDomain
    CodeAddrs   map[common.Address]struct{}                  // touchAll → CodeDomain; union(preCode, modCode)
    SortedCodes []hexutil.Bytes                              // → result.Codes; sourced from accessedCode (Geth-compat)
    CodeReads   map[common.Hash]witnesstypes.CodeWithHash    // → sdCtx.Witness arg; sourced from preCode
}

func (a *accessedState) isEmpty() bool
func (a *accessedState) touchAll(sdCtx *commitment.SharedDomainsCommitmentContext)

func collectAccessedState(rs *RecordingState) *accessedState
func (api *DebugAPIImpl) resolveWitnessBlock(ctx context.Context, tx kv.TemporalTx, blockNrOrHash rpc.BlockNumberOrHash) (*witnessBlockInfo, error)
func detectCollapseSiblings(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, sdCtx *commitment.SharedDomainsCommitmentContext, firstTxNumInBlock, endTxNum, blockNum, parentNum uint64, expectedBlockRoot common.Hash, accessed *accessedState) (siblingPaths [][]byte, err error)
func buildWitnessTrie(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, sdCtx *commitment.SharedDomainsCommitmentContext, firstTxNumInBlock uint64, expectedParentRoot common.Hash, siblingPaths [][]byte, accessed *accessedState) (encodedNodes []hexutil.Bytes, err error)
func (api *DebugAPIImpl) collectAccessedHeaders(ctx context.Context, tx kv.TemporalTx, parentNum uint64, accessedBlockNums []uint64) (headers []map[string]any, byNumber map[uint64]*types.Header, err error)
func (api *DebugAPIImpl) verifyWitnessStateless(ctx context.Context, tx kv.TemporalTx, result *ExecutionWitnessResult, block *types.Block, fullEngine rules.Engine) error
```

### What stays inline (and why)

- **commitment-history precondition** (3 lines, top of function): single guard, no reuse, no benefit to extraction.
- **Build exec env** (`engine.Initialize` arg setup, `blockCtx` with `GetHash` interceptor closure, `ibs`, `signer`, `chainReader`): 11+ live locals. Extraction would require a context struct → moves toward a builder pattern explicitly rejected in PR #21227's brainstorm.
- **Block execution loop** (engine.Initialize → tx loop → engine.Finalize with synthetic receipt → CommitBlock): same 11+ inputs.
- **Commitment setup** (`NewSharedDomains`, parent header lookup, pruning check, empty-touch fast return): short, tightly coupled to control flow because the empty path returns `result` directly.

### Preserved seek pattern

| Seek | Where | Reader | Purpose |
|------|-------|--------|---------|
| #1 (implicit) | `execctx.NewSharedDomains` constructor | default | establish commitment cursor |
| #2 | inside `detectCollapseSiblings` | `commitmentdb.NewSplitHistoryReader(tx, firstTxNumInBlock, endTxNum, false)` | compute post-state root for collapse detection |
| #3 | inside `buildWitnessTrie` | `SetHistoryStateReader(tx, firstTxNumInBlock)` | build parent-state witness trie |

Each helper documents which seek it triggers in a top-of-function comment.

## What Goes Where

- **Implementation Steps** (`[ ]` checkboxes): worktree setup, helper extractions in dependency-safe order, acceptance sweep, file move.
- **Post-Completion** (no checkboxes): PR creation, branch cleanup decisions, optional manual validation.

## Implementation Steps

### Task 0: Worktree setup + baseline + test-coverage sanity check

**Files:** none modified — verification only.

- [x] from `/Users/awskii/org/wrk/erigon`, create worktree: `git worktree add -b awskii/exec-witness-refactor-on-lupin ../erigon-exec-witness-refactor-lupin lupin012/add_tests_for_debug_executionWitnes`
- [x] `cd ../erigon-exec-witness-refactor-lupin`
- [x] verify clean tree: `git status` should show no modifications
- [x] confirm baseline build + test: `make erigon integration && go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1` (all 6 subtests PASS)
- [x] confirm baseline lint: `make lint` (run until clean — 0 issues)
- [x] **test-path coverage sanity check** (one-time): read `rpc/jsonrpc/debug_api_test.go:747` (`TestExecutionWitness`) and verify it (a) calls a block that uses `BLOCKHASH` so `collectAccessedHeaders` non-empty path is exercised, and (b) runs at least one block with `ERIGON_WITNESS_NO_VERIFY` unset so `verifyWitnessStateless` runs the full re-exec.
  - (a) ⚠️ **BLOCKHASH path is NOT exercised by the test fixture.** `cmd/rpcdaemon/rpcdaemontest/test_util.go:127` (`CreateTestExecModule`) generates 13 blocks containing plain ETH transfers and `contracts.Token` operations (Deploy/Mint/Transfer). None of these emit a `BLOCKHASH` opcode, so `accessedBlockHashes` will be empty for every test block; only the parent-header path inside `collectAccessedHeaders` is exercised, not the dedup loop over BLOCKHASH-touched blocks. Consider ad-hoc invocation against a synced datadir during Task 7 to validate the BLOCKHASH dedup path end-to-end.
  - (b) ✅ **Verification IS exercised.** The test does not set `ERIGON_WITNESS_NO_VERIFY`, and `dbg.EnvBool("ERIGON_WITNESS_NO_VERIFY", false)` defaults to false (`rpc/jsonrpc/debug_execution_witness.go:929`), so `verifyWitnessStateless`'s full re-exec runs for every test block.

### Task 1: Extract `(api).collectAccessedHeaders`

Lowest-risk leaf — the headers helper. Signature returns `(headers []map[string]any, byNumber map[uint64]*types.Header, err error)`; the caller assigns both onto `result`. **No result mutation inside the helper.**

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] add method `(api *DebugAPIImpl) collectAccessedHeaders(ctx context.Context, tx kv.TemporalTx, parentNum uint64, accessedBlockNums []uint64) (headers []map[string]any, byNumber map[uint64]*types.Header, err error)` containing the logic currently in the `addHeader` closure + driving loop (`rpc/jsonrpc/debug_execution_witness.go:900-925`)
- [x] initialize `byNumber = make(map[uint64]*types.Header)` and `headers = []map[string]any{}` (defensive — in practice the parent header is always appended first so the slice is always non-empty after this helper; the `Headers` field is `json:"headers,omitempty"` so nil-vs-empty doesn't affect JSON shape, but non-nil-init keeps downstream callers safe from accidental nil-append edge cases)
- [x] add parent header FIRST via internal `addHeader` step
- [x] then loop over `accessedBlockNums`, deduping via `byNumber`
- [x] each header fetched via `api._blockReader.HeaderByNumber(ctx, tx, bn)`; load failure → `"failed to load header for block %d: %w"`; nil → `"missing header for block %d"`
- [x] each kept header passed through `marshalWitnessHeader` for `headers` slice; raw `*types.Header` stored in `byNumber[h.Number.Uint64()]`
- [x] in `ExecutionWitness`: replace lines 900-925 with `headers, byNumber, err := api.collectAccessedHeaders(...)`; on success assign `result.Headers = headers` and `result.headerByNumber = byNumber`. The `addHeader` closure can be deleted now.
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 2: Extract `(api).verifyWitnessStateless`

Env-gated, isolated. The internal `api.chainConfig` re-fetch stays inside (preserves current code path; chainConfig already loaded once at the top of `ExecutionWitness` but the helper re-loads — leave alone, no extra refactoring).

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] add method `(api *DebugAPIImpl) verifyWitnessStateless(ctx context.Context, tx kv.TemporalTx, result *ExecutionWitnessResult, block *types.Block, fullEngine rules.Engine) error` containing logic from `rpc/jsonrpc/debug_execution_witness.go:929-946`
- [x] keep the `dbg.EnvBool("ERIGON_WITNESS_NO_VERIFY", false)` check inside (helper is a no-op when disabled)
- [x] keep the internal `api.chainConfig(ctx, tx)` re-fetch + error wrap: `"failed to get chain config: %w"`
- [x] keep `execBlockStatelessly` call + error wrap: `"[debug_executionWitness] stateless block execution failed: %w"`
- [x] keep the state-root mismatch error: `"[debug_executionWitness] state root mismatch after stateless execution : got %x, expected %x"` (preserve exact spacing/punctuation)
- [x] keep the `log.Debug("[debug_executionWitness] witness verified", "blockNum", blockNum)` call inside — blockNum must come from `block.NumberU64()` (no longer available as local in caller after Task 3)
- [x] replace `rpc/jsonrpc/debug_execution_witness.go:929-946` in `ExecutionWitness` with a single call
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 3: Add `witnessBlockInfo` + `(api).resolveWitnessBlock`

Folds block resolution + txnum-range + parentNum into one helper. **The `commitmentHistoryEnabled` precondition STAYS INLINE in `ExecutionWitness`** — do NOT move it into this helper.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] declare struct `witnessBlockInfo { Block *types.Block; BlockNum, FirstTxNumInBlock, EndTxNum, ParentNum uint64 }` near `ExecutionWitnessResult`. `BlockNum` is stored as a field (not a method) because `ExecutionWitness` references it many places (logging, `MakeSigner`, `detectCollapseSiblings` arg).
- [x] add method `(api *DebugAPIImpl) resolveWitnessBlock(ctx context.Context, tx kv.TemporalTx, blockNrOrHash rpc.BlockNumberOrHash) (*witnessBlockInfo, error)` containing logic from `rpc/jsonrpc/debug_execution_witness.go:525-536` (block resolution: `GetBlockNumber` + `blockWithSenders` + nil check) AND `:547-570` (txnum range + parentNum branching). **Excluded** from the helper: lines 538-543 (`chainConfig`, `engine`) and line 571 (`stateReader := state.NewHistoryReaderV3(...)`) — those belong to "build exec env" and stay inline.
- [x] inside `resolveWitnessBlock`: call `rpchelper.GetBlockNumber`, then `api.blockWithSenders`, nil-check (`"block %d not found"`), `api._txNumReader.Min` (firstTxNumInBlock), `api._txNumReader.Max` (lastTxNumInBlock); compute `endTxNum = lastTxNumInBlock + 1`; if `blockNum == 0` set `firstTxNumInBlock = endTxNum` AND `parentNum = 0`, else `parentNum = blockNum - 1`. Populate all five fields.
- [x] in `ExecutionWitness`: keep `commitmentHistoryEnabled` precondition at the top unchanged; replace lines 525-570 with `info, err := api.resolveWitnessBlock(ctx, tx, blockNrOrHash); if err != nil { return nil, err }`; downstream references switch from local `blockNum` to `info.BlockNum`, `block` to `info.Block`, `firstTxNumInBlock` to `info.FirstTxNumInBlock`, `endTxNum` to `info.EndTxNum`, `parentNum` to `info.ParentNum`.
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 4: Add `accessedState` + `collectAccessedState`

Biggest delta vs PR #21227 — the `SortedCodes` source flips to `accessedCode`. Replaces three sub-blocks (`rpc/jsonrpc/debug_execution_witness.go:665-694` keys, `697-730` codes, `780-786` allCodeAddrs union) plus the body of the `touchAllKeys` closure.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] declare struct `accessedState` with 6 fields (`SortedKeys`, `Addresses`, `Storage`, `CodeAddrs`, `SortedCodes`, `CodeReads`) — see "Helper signatures" above for exact shape. `PreCode`, `ModCode`, `AccessedCode` are NOT struct fields — they exist only as locals inside `collectAccessedState`.
- [x] **CRITICAL: initialize `out.SortedCodes = []hexutil.Bytes{}` at the start of `collectAccessedState` — NOT nil.** The final shape assigns `result.Codes = accessed.SortedCodes` BEFORE the `if accessed.isEmpty() { return result, nil }` fast path. `Codes` has `json:"codes"` with NO `omitempty`, so nil would emit `"codes": null` for empty-touch blocks — a JSON regression. The current code initializes `Codes: []hexutil.Bytes{}` at the result-struct construction site (line 660); we're moving the empty-slice init into `collectAccessedState` instead.
- [x] `SortedKeys` is built for forward-compat but the final shape does NOT assign `result.Keys = accessed.SortedKeys` — `Keys` is intentionally always-null per Geth compat (file line 490-491). Keep `SortedKeys` on the struct for symmetry with PR #21227; document this as vestigial.
- [x] add `func (a *accessedState) isEmpty() bool` → `len(a.Addresses)+len(a.Storage)+len(a.CodeAddrs) == 0`. Note: does NOT include `CodeReads` or `SortedCodes`.
- [x] add `func (a *accessedState) touchAll(sdCtx *commitmentdb.SharedDomainsCommitmentContext)` containing the current `touchAllKeys` closure body (lines 793-806): Accounts → Storage → Code, in that order. (Plan signature listed `commitment.SharedDomainsCommitmentContext`; the type actually lives in `commitmentdb`, so the implementation uses `commitmentdb.SharedDomainsCommitmentContext`.)
- [x] add `func collectAccessedState(rs *RecordingState) *accessedState`:
  - merge `GetAccessedKeys` + `GetModifiedKeys` → `Addresses`, `Storage`
  - build `SortedKeys`: addresses (`addr.Bytes()`) + composite storage keys (`append(addr.Bytes(), key.Bytes()...)`); final `slices.SortFunc(bytes.Compare)` for determinism
  - locally compute `accessedCode := rs.GetAccessedCode()` → hash-dedup via `allCodesByHash map[common.Hash][]byte` → sort `uniqueCodes []codeWithHash` by `bytes.Compare(a.hash[:], b.hash[:])` → output `SortedCodes` slice. **Preserve the local `codeWithHash` struct exactly as currently in `ExecutionWitness`**.
  - locally compute `preCode := rs.GetPreStateCode()` → build `CodeReads` keyed by `addrHash = crypto.Keccak256Hash(addr.Bytes())`, values wrap `witnesstypes.CodeWithHash{Code: code, CodeHash: accounts.InternCodeHash(crypto.Keccak256Hash(code))}`
  - locally compute `modCode := rs.GetModifiedCode()` → `CodeAddrs = union(preCode, modCode)` keys
- [x] in `ExecutionWitness`: call `accessed := collectAccessedState(recordingState)`; remove the inline keys/codes/allCodeAddrs blocks; rewrite the empty-touch check (line 788) to `accessed.isEmpty()`; rewrite the `touchAllKeys` closure body to a one-liner `accessed.touchAll(sdCtx)` (closure may stay temporarily — Task 6 removes it).
- [x] assign `result.Codes = accessed.SortedCodes` instead of the current append loop (lines 727-730).
- [x] verify `result.Codes` ordering matches pre-refactor byte-for-byte: deterministic via the final `slices.SortFunc(bytes.Compare)` on `SortedCodes`; map iteration order doesn't matter as long as the sort runs.
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `go test ./rpc/jsonrpc/ -count=1` (full package — largest surface change)
- [x] `make lint` until clean

### Task 5: Extract `detectCollapseSiblings`

Folds `SetCustomHistoryStateReader`, `SeekCommitment` #2, the lupin012 `seekBlockNum` guard, `accessed.touchAll`, `SetCollapseTracer`, `ComputeCommitment`, and the `block.Root()` verification.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] add function `detectCollapseSiblings(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, sdCtx *commitment.SharedDomainsCommitmentContext, firstTxNumInBlock, endTxNum, blockNum, parentNum uint64, expectedBlockRoot common.Hash, accessed *accessedState) (siblingPaths [][]byte, err error)`
- [x] add top-of-function comment: `// Triggers SeekCommitment #2 (split history reader). Preserves pre-refactor behavior including the seekBlockNum != parentNum guard.`
- [x] inside the helper, in exact order:
  1. `splitStateReader := commitmentdb.NewSplitHistoryReader(tx, firstTxNumInBlock, endTxNum, false /* withHistory */)`
  2. `sdCtx.SetCustomHistoryStateReader(splitStateReader)`
  3. `_, seekBlockNum, err := domains.SeekCommitment(ctx, tx)` — capture `seekBlockNum` and `err`; the first return (txNum) is unused
  4. if err: return `nil, fmt.Errorf("failed to re-seek commitment for collapse detection: %w", err)`
  5. **lupin012-specific guard** — if `seekBlockNum != parentNum`: return `nil, fmt.Errorf("debug_executionWitness: commitment trie for block %d is at block %d instead of parent %d; "+"commitment history may be pruned for this block range", blockNum, seekBlockNum, parentNum)` — preserve exact multi-line wording
  6. `accessed.touchAll(sdCtx)`
  7. register collapse tracer:
     ```go
     sdCtx.SetCollapseTracer(func(hashedKeyPath []byte) {
         log.Debug("[debug_executionWitness] node collapse detected", "path", commitment.NibblesToString(hashedKeyPath), "len", len(hashedKeyPath))
         siblingPaths = append(siblingPaths, common.Copy(hashedKeyPath))
     })
     ```
     — `common.Copy(hashedKeyPath)` is mandatory; dropping it introduces a slice-reuse bug.
  8. `computedRootHash, err := sdCtx.ComputeCommitment(ctx, tx, false, blockNum, firstTxNumInBlock, "debug_executionWitness_collapse_detection", nil)`
  9. if err: return `nil, fmt.Errorf("[debug_executionWitness] collapse detection via ComputeCommitment failed: %v\n", err)` — preserve `%v` (not `%w`) and trailing `\n`
  10. if `common.Hash(computedRootHash) != expectedBlockRoot`: return `nil, fmt.Errorf("[debug_executionWitness] computedRootHash(%x)!= expectedRootHash(%x)", computedRootHash, expectedBlockRoot)` — preserve missing space before `!=`
  11. `sdCtx.SetCollapseTracer(nil)`
  12. return `siblingPaths, nil`
- [x] in `ExecutionWitness`: replace lines 814-860 with `siblingPaths, err := detectCollapseSiblings(ctx, tx, domains, sdCtx, info.FirstTxNumInBlock, info.EndTxNum, info.BlockNum, info.ParentNum, info.Block.Root(), accessed); if err != nil { return nil, err }`
- [x] `git diff` the error strings — grep for `[debug_executionWitness]` before and after refactor; the two collapse-detection strings must match byte-for-byte
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 6: Extract `buildWitnessTrie`

Folds `SetHistoryStateReader`, `SeekCommitment` #3, `accessed.touchAll`, sibling-path touches, `sdCtx.Witness`, parent-root verification, and the `RLPEncode` → `result.State` loop.

**Files:**
- Modify: `rpc/jsonrpc/debug_execution_witness.go`

- [x] add function `buildWitnessTrie(ctx context.Context, tx kv.TemporalTx, domains *execctx.SharedDomains, sdCtx *commitment.SharedDomainsCommitmentContext, firstTxNumInBlock uint64, expectedParentRoot common.Hash, siblingPaths [][]byte, accessed *accessedState) (encodedNodes []hexutil.Bytes, err error)` — actual implementation uses `commitmentdb.SharedDomainsCommitmentContext` (same note as Task 5)
- [x] add top-of-function comment: `// Triggers SeekCommitment #3 (parent-state reader). Preserves pre-refactor behavior.`
- [x] **CRITICAL**: pre-initialize `encodedNodes := []hexutil.Bytes{}` at the start of the function — NOT nil. The named return defaults to nil; a zero-node trie would otherwise produce `"state": null` instead of `"state": []` (the field has no `omitempty`).
- [x] inside the helper, in exact order:
  1. `sdCtx.SetHistoryStateReader(tx, firstTxNumInBlock)`
  2. `if _, _, err := domains.SeekCommitment(ctx, tx); err != nil { return nil, fmt.Errorf("failed to reset commitment for regular witness: %w", err) }` — note: re-use the current wording from lupin012's `resetToParentState` closure error wrap
  3. `accessed.touchAll(sdCtx)`
  4. if `len(siblingPaths) > 0`: preserve `log.Debug("[debug_executionWitness] detected sibling paths", "count", len(siblingPaths))`; loop over siblings — preserve the `compactSiblingPath := commitment.NibblesToString(siblingPath)` local on the line above the Debug call, plus `log.Debug("[debug_executionWitness] touching sibling hashed key", "path", compactSiblingPath, "len", len(siblingPath))` + `sdCtx.TouchHashedKey(siblingPath)`
  5. `witnessTrie, witnessRoot, err := sdCtx.Witness(ctx, accessed.CodeReads, "debug_executionWitness_witness_construction")`
  6. if err: `nil, fmt.Errorf("failed to generate witness: %w", err)`
  7. if `!bytes.Equal(witnessRoot, expectedParentRoot[:])`: `nil, fmt.Errorf("collapse witness root mismatch: calculated=%x, expected=%x", common.BytesToHash(witnessRoot), expectedParentRoot)`
  8. `allNodes, err := witnessTrie.RLPEncode()` — if err: `nil, fmt.Errorf("failed to encode trie nodes: %w", err)`
  9. `for _, node := range allNodes { encodedNodes = append(encodedNodes, common.Copy(node)) }` — `common.Copy` mandatory (drop = memory-reuse bug)
  10. return `encodedNodes, nil`
- [x] in `ExecutionWitness`: replace lines 862-895 with `nodes, err := buildWitnessTrie(...); if err != nil { return nil, err }; result.State = nodes`
- [x] **delete** `resetToParentState` closure (no longer referenced)
- [x] **delete** `touchAllKeys` closure (no longer referenced — was reduced to a one-liner in Task 4)
- [x] `git diff` the error strings to confirm preservation
- [x] `make erigon integration`
- [x] `go test ./rpc/jsonrpc/ -run TestExecutionWitness -v -count=1`
- [x] `make lint` until clean

### Task 7: Verify acceptance criteria

**Files:** none modified — verification only.

- [ ] `ExecutionWitness` is roughly ~130 lines (down from ~440)
- [ ] grep whole file for `SeekCommitment` (both `domains.SeekCommitment` and `sdCtx.SeekCommitment` forms) — confirm exactly 2 explicit calls remain (one inside `detectCollapseSiblings`, one inside `buildWitnessTrie`); the 3rd is implicit in `NewSharedDomains`
- [ ] grep for `touchAllKeys` and `resetToParentState` — confirm zero occurrences in the file
- [ ] grep for preserved debug/log lines — confirm `log.Debug("expected parent root", ...)` still present in `ExecutionWitness`
- [ ] grep for `SetAccountsToTrace` — confirm the call still exists in `ExecutionWitness`
- [ ] grep for `ReadDBCommitmentHistoryEnabled` — confirm the precondition still in `ExecutionWitness` (NOT moved into `resolveWitnessBlock`)
- [ ] grep for `marshalWitnessHeader` — confirm referenced only from `collectAccessedHeaders` (not duplicated)
- [ ] grep for `common.Copy(` inside the helpers — confirm both Task 5 collapse-tracer and Task 6 RLPEncode loop still use it
- [ ] grep for `SetDeferBranchUpdates` — confirm called exactly once in `ExecutionWitness`, not in either helper
- [ ] grep for the lupin012 error strings — confirm `"commitment trie for block %d is at block %d instead of parent %d"` still present inside `detectCollapseSiblings`
- [ ] full test sweep: `make test-short`
- [ ] full lint sweep: `make lint` (run twice to confirm stability)
- [ ] visually inspect `ExecutionWitness` body — should read as: tx → commitment-history guard → resolveWitnessBlock → exec env (inline) → exec block (inline) → collectAccessedState → empty-touch return → commitment setup (inline) → detectCollapseSiblings → buildWitnessTrie → collectAccessedHeaders → verifyWitnessStateless → return

### Task 8: Move plan to completed

**Files:**
- Modify: filesystem only

- [ ] `mkdir -p docs/plans/completed`
- [ ] `git mv docs/plans/20260518-exec-witness-refactor-on-lupin012.md docs/plans/completed/`

## Post-Completion

*Informational only — no checkboxes.*

**Manual verification:**
- Optionally run `debug_executionWitness` via RPC against a synced datadir on a few historical blocks and diff the JSON output against the lupin012 baseline; not strictly necessary because `TestExecutionWitness` covers the contract, but useful confidence boost before merging.

**PR notes:**
- Branch: `awskii/exec-witness-refactor-on-lupin`
- Base: `main` (NOT `lupin012/...` — the bug fix in lupin012 is independently in flight and this PR should merge on its own once lupin012 lands)
- Title: `rpc/jsonrpc: split debug_executionWitness into phase helpers (on top of lupin012 codes fix)`
- Body: short — note "behavior-preserving, mirrors PR #21227 shape, retargeted to lupin012's accessedCode-based Codes path", link to PR #21227 for context, link to this plan.

**External system updates:** none — pure internal refactor.

**Follow-up candidates (out of scope here):**
- Investigate whether collapse detection can run on the witness trie itself to consolidate the 3 SeekCommitment calls down to 2. Algorithmic change — separate plan.
- Consider moving `RecordingState` and `witnessStateless` to their own files (`debug_execution_witness_recording.go`, `debug_execution_witness_stateless.go`) once this refactor lands.
- Once both this PR and lupin012 land on `main`, the two refactors converge naturally (different commit shapes, same end state).
