# Wire up wasmDB for Stylus WASM Execution

## Overview
Block 241933976 on Arbitrum One panics with `IBS: wasmDB not set` when a Stylus WASM program executes. The wasmdb infrastructure exists (`OpenArbitrumWasmDB`, `SetWasmDB`, `WasmDB` struct) but is never connected to the execution pipeline. Every `IntraBlockState` created during execution has `wasmDB == nil`.

This follows the exact pattern from erigon's `arbitrum` branch — no new design, just wiring up existing code.

## Context
- **Panic site**: `execution/state/arb.go:208` — `ActivatedAsm()` panics on nil wasmDB
- **Call chain**: `programs.CallProgram` → `getLocalAsm` → `TryGetActivatedAsm` → `ActivatedAsm` → panic
- **Existing infrastructure** (all already implemented, just disconnected):
  - `arb/ethdb/wasmdb/wasmdb.go`: `WasmIface`, `WasmDB`, `OpenArbitrumWasmDB` (singleton), `WrapDatabaseWithWasm`
  - `execution/state/arb.go:352`: `SetWasmDB(wasmDB wasmdb.WasmIface)` — setter on IBS
  - `execution/state/intra_block_state.go:196`: `wasmDB wasmdb.WasmIface` field on IBS

### How the `arbitrum` branch does it (our reference)
```
arbitrum:db/datadir/dirs.go:45           — ArbitrumWasm field in Dirs
arbitrum:db/datadir/dirs.go:119          — filepath.Join(datadir, "arbitrumwasm")
arbitrum:execution/stagedsync/stage_execute.go:95  — arbitrumWasmDB field in ExecuteBlockCfg
arbitrum:execution/stagedsync/stage_execute.go:116 — param to StageExecuteBlocksCfg
arbitrum:execution/exec3/state.go:200    — Worker.SetArbitrumWasmDB method
arbitrum:execution/stagedsync/exec3_serial.go:46   — called before each RunTxTaskNoLock
arbitrum:execution/stages/stageloop.go:681+        — callers pass OpenArbitrumWasmDB(ctx, dirs.ArbitrumWasm)
```

## Development Approach
- **Testing approach**: Regular (code first, then tests)
- Follow the arbitrum branch pattern exactly
- Arbitrum uses serial execution only — no parallel executor changes needed

## Implementation Steps

### Task 1: Add `ArbitrumWasm` path to Dirs

**Files:**
- Modify: `erigon/db/datadir/dirs.go`

- [x] Add `ArbitrumWasm string` field to `Dirs` struct (after `Log string`, line 63)
- [x] Set `ArbitrumWasm: filepath.Join(datadir, "arbitrumwasm")` in `Open()` (after line 131)
- [x] Add `dirs.ArbitrumWasm` to `dir.MustExist()` in `New()` (line ~82)
- [x] Run `go build ./db/datadir/...` to verify

### Task 2: Add `arbitrumWasmDB` to `ExecuteBlockCfg`

**Files:**
- Modify: `erigon/execution/stagedsync/stage_execute.go`

- [x] Add import `"github.com/erigontech/erigon/arb/ethdb/wasmdb"`
- [x] Add `arbitrumWasmDB wasmdb.WasmIface` field to `ExecuteBlockCfg` (after line 90)
- [x] Add `arbitrumWasmDB wasmdb.WasmIface` param to `StageExecuteBlocksCfg` (after `experimentalBAL bool`)
- [x] Set `arbitrumWasmDB: arbitrumWasmDB` in the return struct literal (line ~134)
- [x] Run `go build ./execution/stagedsync/...` (will fail until callers updated — that's OK)

### Task 3: Add `SetArbitrumWasmDB` method to Worker

**Files:**
- Modify: `erigon/execution/exec/state.go`

- [x] Add import `"github.com/erigontech/erigon/arb/ethdb/wasmdb"`
- [x] Add method after line ~162:
  ```go
  func (rw *Worker) SetArbitrumWasmDB(wasmDB wasmdb.WasmIface) {
      if rw.chainConfig.IsArbitrum() {
          rw.ibs.SetWasmDB(wasmDB)
      }
  }
  ```
- [x] Run `go build ./execution/exec/...` to verify

### Task 4: Call `SetArbitrumWasmDB` in serial executor

**Files:**
- Modify: `erigon/execution/stagedsync/exec3_serial.go`

- [x] In `executeBlock()`, before `se.worker.RunTxTask(txTask)` (line 349), add:
  ```go
  se.worker.SetArbitrumWasmDB(se.cfg.arbitrumWasmDB)
  ```
- [x] Run `go build ./execution/stagedsync/...` to verify

### Task 5: Update all `StageExecuteBlocksCfg` callers

**Files:**
- Modify: `erigon/execution/stagedsync/stageloop/stageloop.go` (lines 749, 784, 810)
- Modify: `erigon/cmd/integration/commands/stages.go` (line 817)
- Modify: `erigon/cmd/integration/commands/state_stages.go` (lines 189, 379)
- Modify: `erigon/execution/tests/mock/mock_sentry.go` (lines 535, 580, 638)
- Modify: `erigon/execution/stagedsync/stage_witness.go` (line 109)

- [x] `stageloop.go:749` — add `wasmdb.OpenArbitrumWasmDB(ctx, dirs.ArbitrumWasm)` as last param
- [x] `stageloop.go:784` — same
- [x] `stageloop.go:810` — add `wasmdb.OpenArbitrumWasmDB(ctx, cfg.Dirs.ArbitrumWasm)` (uses `cfg.Dirs`)
- [x] `stages.go:817` — add `wasmdb.OpenArbitrumWasmDB(ctx, dirs.ArbitrumWasm)`
- [x] `state_stages.go:189` — add `wasmdb.OpenArbitrumWasmDB(ctx, dirs.ArbitrumWasm)`
- [x] `state_stages.go:379` — add `wasmdb.OpenArbitrumWasmDB(ctx, dirs.ArbitrumWasm)`
- [x] `mock_sentry.go:535,580,638` — add `nil` (tests don't use Arbitrum)
- [x] `stage_witness.go:109` — add `nil` (witness stage doesn't need wasmDB for now)
- [x] Add `"github.com/erigontech/erigon/arb/ethdb/wasmdb"` import to files that need it
- [x] Run `make erigon` to verify full build

### Task 6: Write tests

**Files:**
- Create: `erigon/execution/exec/state_arb_test.go`
- Create: `erigon/arb/ethdb/wasmdb/wasmdb_test.go` (extend if exists)

- [x] Test `SetArbitrumWasmDB` sets wasmDB on Worker's IBS when `IsArbitrum()` is true
- [x] Test `SetArbitrumWasmDB` is no-op when `IsArbitrum()` is false (non-Arbitrum chain config)
- [x] Test `OpenArbitrumWasmDB` singleton returns same instance on repeated calls
- [x] Test `WrapDatabaseWithWasm` write/read round-trip for `ActivatedAsm`
- [x] Test `ActivatedAsm` returns error for missing module hash
- [x] Run `go test ./execution/exec/... ./arb/ethdb/wasmdb/... -count=1 -v`

### Task 7: Verify and lint

- [x] Run `make lint`
- [x] Run `make erigon`
- [x] Verify all tests pass: `go test ./execution/exec/... ./arb/ethdb/wasmdb/... ./db/datadir/... -count=1`

## Technical Details

### Data flow
```
OpenArbitrumWasmDB(ctx, dirs.ArbitrumWasm)  ← singleton, opens MDBX at <datadir>/arbitrumwasm
  ↓
StageExecuteBlocksCfg(..., arbitrumWasmDB)   ← stored in ExecuteBlockCfg
  ↓
serialExecutor.cfg.arbitrumWasmDB            ← accessed from exec config
  ↓
worker.SetArbitrumWasmDB(wasmDB)             ← called before each RunTxTask
  ↓
ibs.SetWasmDB(wasmDB)                        ← sets on IntraBlockState
  ↓
ibs.ActivatedAsm() / WasmStore() / etc.      ← no longer panics
```

### Thread safety
- `SizeConstrainedCache` uses `sync.Mutex` (`arb/lru/blob_lru.go:39`) — thread-safe
- MDBX read transactions are thread-safe
- Singleton `openedArbitrumWasmDB` set once at startup

### Empty wasmDB on first sync
The wasmDB starts empty. On first Stylus program encounter, `getLocalAsm()` reactivates from WASM bytecode and stores to wasmDB. Subsequent calls hit the LRU cache.

## Post-Completion

**Manual verification:**
- Run against arb1 datadir past block 241933976 to confirm no panic
- `STOP_AFTER_BLOCK=241933977 ./build/bin/erigon --datadir=<arb1-datadir> --chain=arb1`
