# Port MultiGas Flow and Remaining Arbitrum Patches

## Overview
Port the complete multi-dimensional gas tracking (multigas) from the `arbitrum` branch to `arbitrum-rem`, along with missing state transition patches. MultiGas is an accounting layer that categorizes gas by spending type (computation, storage, calldata, etc.) — the sum of all categories equals the same total as single-gas on mainline erigon. This is required for ArbOS 50+ and Stylus support.

Additionally, several state transition gaps need porting: DropTip pre-preCheck override, Arbitrum intrinsic gas gating, handleRevertedTx, tip payment routing, and Prague calldata pricing guards.

## Context (from discovery)

**Primary files to modify:**
- `execution/vm/evm.go` — Add multigas return values to Call/Create/CallCode/Create2/DelegateCall/StaticCall
- `execution/vm/instructions.go` — Update opcode handlers for new EVM method signatures
- `execution/protocol/state_transition.go` — Wire multigas tracking end-to-end in TransitionDb
- `execution/protocol/state_transition_arb.go` — Add handleRevertedTx function
- `execution/protocol/state_processor.go` — Update ApplyTransaction callers

**Reference files (arbitrum branch, extracted to /tmp/):**
- `/tmp/arb_st.go` — arbitrum's core/state_transition.go
- `/tmp/arb_evm.go` — arbitrum's core/vm/evm.go

**Existing infrastructure already on arbitrum-rem:**
- `arb/multigas/resources.go` — Full MultiGas type with IntrinsicMultiGas function
- `execution/vm/evm_arb_tx_hook.go` — TxProcessingHook interface (already returns multigas from StartTxHook/GasChargingHook)
- `execution/vm/evmtypes/` — ExecutionResult.UsedMultiGas field
- ProcessingHook integration in TransitionDb (partial — early return path works, normal path doesn't)

**Callers affected by EVM signature changes:**
- `execution/vm/instructions.go:1104` — CALL opcode
- `execution/vm/instructions.go:998` — CREATE opcode
- `execution/vm/instructions.go:1052` — CREATE2 opcode
- `execution/vm/instructions.go:1149` — CALLCODE opcode
- `execution/vm/instructions.go:1189` — DELEGATECALL opcode
- `execution/vm/instructions.go:1229` — STATICCALL opcode
- `execution/protocol/state_transition.go:417,588` — ApplyFrame and TransitionDb
- `execution/protocol/state_processor.go:266` — SysCallContract
- `execution/protocol/block_exec.go:277` — system call

## Development Approach
- **testing approach**: Regular (code first, then tests) — multigas is pure accounting and testable via unit tests on TransitionDb
- Complete each task fully before moving to the next
- Make small, focused changes
- **CRITICAL: every task MUST include new/updated tests** for code changes in that task
- **CRITICAL: all tests must pass before starting next task** — run `go build ./...` and `go test ./execution/...` after each
- **CRITICAL: update this plan file when scope changes during implementation**
- Maintain backward compatibility — multigas total must always equal single-gas

## Testing Strategy
- **unit tests**: Test multigas accumulation in TransitionDb via mock ProcessingHook
- **integration tests**: Existing tests in `execution/protocol/` must continue passing unchanged
- **property**: `usedMultiGas.SingleGas() == result.ReceiptGasUsed` for all non-Arbitrum transactions

## Progress Tracking
- Mark completed items with `[x]` immediately when done
- Add newly discovered tasks with ➕ prefix
- Document issues/blockers with ⚠️ prefix

## Implementation Steps

### Task 1: Add multigas return value to EVM Call/Create methods

**Files:**
- Modify: `execution/vm/evm.go`

- [x] Add `multigas.MultiGas` as 4th return value to `Call()` signature: `(ret []byte, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error)`
- [x] Add `multigas.MultiGas` as 4th return value to `CallCode()` signature
- [x] Add `multigas.MultiGas` as 5th return value to `Create()` signature (after contractAddr)
- [x] Add `multigas.MultiGas` as 5th return value to `Create2()` signature
- [x] Add `multigas.MultiGas` as 4th return value to `DelegateCall()` signature
- [x] Add `multigas.MultiGas` as 4th return value to `StaticCall()` signature
- [x] Update `SysCreate()` to return multigas
- [x] In `call()` internal method, initialize `usedMultiGas := multigas.ZeroGas()` and return it (passthrough for now)
- [x] In `create()` internal method, same treatment
- [x] Verify `go build ./execution/vm/...` compiles (will fail — callers need updating)

### Task 2: Update all EVM method callers for new signatures

**Files:**
- Modify: `execution/vm/instructions.go`
- Modify: `execution/protocol/state_transition.go`
- Modify: `execution/protocol/state_processor.go`
- Modify: `execution/protocol/block_exec.go`

- [x] Update CALL opcode handler (instructions.go:1104) — capture and discard multigas for now: `ret, returnGas, _, err := evm.Call(...)`
- [x] Update CREATE opcode handler (instructions.go:998) — same pattern
- [x] Update CREATE2 opcode handler (instructions.go:1052) — same pattern
- [x] Update CALLCODE opcode handler (instructions.go:1149) — same pattern
- [x] Update DELEGATECALL opcode handler (instructions.go:1189) — same pattern
- [x] Update STATICCALL opcode handler (instructions.go:1229) — same pattern
- [x] Update `TransitionDb` Call/Create calls (state_transition.go:586-588) — capture multigas
- [x] Update `ApplyFrame` Call (state_transition.go:417) — capture multigas
- [x] Update `SysCallContract` (state_processor.go:266) — discard multigas
- [x] Update `block_exec.go:277` — discard multigas
- [x] Run `go build ./execution/...` — must compile
- [x] Run `go test ./execution/protocol/... -count=1 -short` — must pass

### Task 3: Wire multigas tracking through TransitionDb

**Files:**
- Modify: `execution/protocol/state_transition.go`

- [x] Add `"github.com/erigontech/erigon/arb/multigas"` import
- [x] Initialize `usedMultiGas := multigas.ZeroGas()` at start of TransitionDb
- [x] Call `multigas.IntrinsicMultiGas()` alongside `fixedgas.IntrinsicGas()` — use multiGas from it
- [x] Add sanity check: panic if `multiGas.SingleGas() != gas` (same as arbitrum branch)
- [x] Accumulate: `usedMultiGas = usedMultiGas.SaturatingAdd(multiGas)` after intrinsic gas subtraction
- [x] Capture multigas from `GasChargingHook`: change `tipRecipient, _, err` to `tipRecipient, hookMultiGas, err` and accumulate
- [x] Capture multigas from `evm.Create()` / `evm.Call()` and accumulate
- [x] Set `result.UsedMultiGas = usedMultiGas` in the ExecutionResult at end of TransitionDb
- [x] Write test: `TestTransitionDb_MultiGasAccumulation` — verify `usedMultiGas.SingleGas() == result.ReceiptGasUsed` for a simple transfer
- [x] Write test: `TestTransitionDb_MultiGasStartHookEarlyReturn` — verify UsedMultiGas is set on early return path
- [x] Run `go test ./execution/protocol/... -count=1` — must pass

### Task 4: Port DropTip pre-preCheck price override

**Files:**
- Modify: `execution/protocol/state_transition.go`

- [x] Add DropTip + price override block BEFORE `st.preCheck()` in TransitionDb (reference: /tmp/arb_st.go lines 525-535):
  ```
  if st.evm.ProcessingHook.DropTip() && st.msg.GasPrice().Cmp(&st.evm.Context.BaseFee) > 0 {
      mmsg := st.msg.(*types.Message)
      mmsg.SetGasPrice(&st.evm.Context.BaseFee)
      ...
  }
  ```
- [x] Keep existing DropTip check at line 635 as fallback (sets effectiveTip to zero)
- [x] Write test: `TestTransitionDb_DropTipPrePreCheck` — verify gasPrice is overridden before execution for Arbitrum delayed messages
- [x] Run `go test ./execution/protocol/... -count=1` — must pass

### Task 5: Gate intrinsic gas check for Arbitrum

**Files:**
- Modify: `execution/protocol/state_transition.go`

- [x] Change line 534 from `if st.gasRemaining < gas || st.gasRemaining < floorGas7623` to `if !rules.IsArbitrum && (st.gasRemaining < gas || st.gasRemaining < floorGas7623)`
- [x] Write test: `TestTransitionDb_ArbitrumSkipsIntrinsicGasCheck` — verify Arbitrum transactions don't fail on insufficient intrinsic gas
- [x] Run `go test ./execution/protocol/... -count=1` — must pass

### Task 6: Port Arbitrum-specific refund path with multigas

**Files:**
- Modify: `execution/protocol/state_transition.go`

- [x] Split refund logic: `if st.evm.ProcessingHook.IsArbitrum() { ... } else { ... }` (reference: /tmp/arb_st.go lines 678-722)
- [x] In Arbitrum path: ForceRefundGas first, then nonrefundable-capped refund with multigas.WithRefund
- [x] Add `IsCalldataPricingIncreaseEnabled()` Prague guard for floor gas (reference: lines 698-711)
- [x] In Arbitrum path, use `usedMultiGas.SaturatingIncrement(multigas.ResourceKindL2Calldata, ...)` for Prague floor
- [x] Non-Arbitrum path: keep existing refund logic unchanged
- [x] Write test: `TestTransitionDb_ArbitrumRefundMultiGas` — verify multigas.WithRefund is set correctly
- [x] Write test: `TestTransitionDb_NonArbitrumRefundUnchanged` — verify standard refund path unchanged
- [x] Run `go test ./execution/protocol/... -count=1` — must pass

### Task 7: Fix EndTxHook signature and tip payment

**Files:**
- Modify: `execution/protocol/state_transition.go`

- [x] Change EndTxHook call from `EndTxHook(st.gasUsed(), !errors.Is(vmerr, vm.ErrExecutionReverted))` to `EndTxHook(st.gasRemaining, vmerr == nil)` to match arbitrum branch semantics
- [x] Add Arbitrum-specific tip payment: when `rules.IsArbitrum`, also pay `coinbase` directly (reference: lines 742-747)
- [x] Add tip tracing after burn: `CaptureArbitrumTransfer(nil, &tipRecipient, tracingTipAmount, false, "tip")` (reference: lines 787-791)
- [x] Track `TopLevelDeployed` from Create result and set `result.TopLevelDeployed`
- [x] Set `result.EvmRefund = st.state.GetRefund()`
- [x] Write test: `TestTransitionDb_EndTxHookSignature` — verify EndTxHook receives gasRemaining and success bool
- [x] Run `go test ./execution/protocol/... -count=1` — must pass

### Task 8: Port handleRevertedTx function

**Files:**
- Modify: `execution/protocol/state_transition_arb.go`
- Modify: `execution/protocol/state_transition_arb_test.go`

- [x] Add `handleRevertedTx` method to StateTransition (reference: /tmp/arb_st.go lines 857-889)
- [x] Function takes `(msg *types.Message, usedMultiGas multigas.MultiGas)` and returns `(multigas.MultiGas, error)`
- [x] Checks `RevertedTxGasUsed` map, adjusts nonce, gas, and multigas accordingly
- [x] Returns `vm.ErrExecutionReverted` if match found
- [x] Write test: `TestHandleRevertedTx_MatchingHash` — verify gas adjustment and ErrExecutionReverted
- [x] Write test: `TestHandleRevertedTx_NoMatch` — verify passthrough
- [x] Run `go test ./execution/protocol/... -count=1` — must pass

### Task 9: Add FloorDataGas function

**Files:**
- Modify: `execution/protocol/state_transition.go`

- [x] Port `FloorDataGas(data []byte) (uint64, error)` function (reference: /tmp/arb_st.go lines 825-840)
- [x] Write test: `TestFloorDataGas` — verify computation matches EIP-7623 spec
- [x] Run `go test ./execution/protocol/... -count=1` — must pass

### Task 10: Verify acceptance criteria

- [x] Verify multigas flows end-to-end: intrinsic → GasChargingHook → EVM exec → refund → result
- [x] Verify `usedMultiGas.SingleGas() == result.ReceiptGasUsed` invariant holds
- [x] Verify non-Arbitrum transactions produce `multigas.ZeroGas()` from hooks (DefaultTxProcessor)
- [x] Run full test suite: `go test ./execution/... -count=1 -short`
- [x] Run `go build ./...` — full codebase must compile
- [x] Verify no regressions in existing test coverage

### Task 11: [Final] Update documentation

- [x] Update CLAUDE.md if new patterns discovered
- [x] Move this plan to `docs/plans/completed/`

## Technical Details

### MultiGas accumulation flow in TransitionDb
```
usedMultiGas = ZeroGas()
├── += IntrinsicMultiGas(data, accessList, ...)     // L2Calldata, Computation, StorageAccess
├── += GasChargingHook(&gasRemaining, intrinsicGas) // L1 poster cost (set by arbos TxProcessor)
├── += evm.Call() or evm.Create()                   // Computation, Storage (from opcodes)
├── .WithRefund(refund)                             // SSTORE refunds
├── .SaturatingIncrement(L2Calldata, floorDelta)    // Prague EIP-7623 floor (if enabled)
└── → result.UsedMultiGas
```

### EVM method signature changes
```go
// Before (arbitrum-rem current):
func (evm *EVM) Call(...) (ret []byte, leftOverGas uint64, err error)
func (evm *EVM) Create(...) (ret []byte, contractAddr accounts.Address, leftOverGas uint64, err error)

// After (matching arbitrum branch):
func (evm *EVM) Call(...) (ret []byte, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error)
func (evm *EVM) Create(...) (ret []byte, contractAddr accounts.Address, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error)
```

### EndTxHook semantics change
```go
// Before (arbitrum-rem):
st.evm.ProcessingHook.EndTxHook(st.gasUsed(), !errors.Is(vmerr, vm.ErrExecutionReverted))

// After (matching arbitrum branch):
st.evm.ProcessingHook.EndTxHook(st.gasRemaining, vmerr == nil)
```

## Post-Completion

**Manual verification:**
- Run arb-sepolia sync test to verify state roots match
- Verify multigas JSON is exposed correctly via `eth_getTransactionReceipt` if ExposeMultiGas config is set
- Compare gas usage on known Arbitrum transactions between branches
