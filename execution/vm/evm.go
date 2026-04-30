// Copyright 2014 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"errors"
	"fmt"
	"sync/atomic"

	keccak "github.com/erigontech/fastkeccak"
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func (evm *EVM) precompile(addr accounts.Address) (PrecompiledContract, bool) {
	// Precompiled contracts can be overridden, otherwise determine the active set based on chain rules
	precompiles := evm.precompiles
	if precompiles == nil {
		precompiles = Precompiles(evm.chainRules)
	}
	p, ok := precompiles[addr]
	return p, ok
}

// EVM is the Ethereum Virtual Machine base object and provides
// the necessary tools to run a contract on the given state with
// the provided context. It should be noted that any error
// generated through any of the calls should be considered a
// revert-state-and-consume-all-gas operation, no checks on
// specific errors should ever be performed. The interpreter makes
// sure that any errors generated are to be considered faulty code.
//
// The EVM should never be reused and is not thread safe.
type EVM struct {
	// Context provides auxiliary blockchain related information
	Context evmtypes.BlockContext
	evmtypes.TxContext
	// IntraBlockState gives access to the underlying state
	intraBlockState *state.IntraBlockState

	// table holds the opcode specific handlers
	jt *JumpTable

	// depth is the current call stack
	depth int

	// chainConfig contains information about the current chain
	chainConfig *chain.Config
	// chain rules contains the chain rules for the current epoch
	chainRules *chain.Rules
	// virtual machine configuration options used to initialise the
	// evm.
	config Config
	// abort is used to abort the EVM calling operations
	abort atomic.Bool
	// callGasTemp holds the gas available for the current call. This is needed because the
	// available gas is calculated in gasCall* according to the 63/64 rule and later
	// applied in opCall*.
	callGasTemp uint64
	// optional overridden set of precompiled contracts
	precompiles PrecompiledContracts

	hasher    keccak.KeccakState // Keccak256 hasher instance shared across opcodes
	hasherBuf common.Hash        // Keccak256 hasher result array shared across opcodes

	readOnly   bool   // Whether to throw on stateful modifications
	returnData []byte // Last CALL's return data for subsequent reuse

	regularGasConsumed uint64 // total regular gas charged during tx execution (for block-level accounting)
	// executionStateGas accumulates the EIP-8037 state-gas charges (and credits)
	// across all frame commits in this tx. Charged at frame-commit time via the
	// journal-walk-and-delta algorithm (see IntraBlockState.ComputeFrameStateBytes).
	// Reset to 0 on top-level revert (depth==0 && err != nil). Signed because
	// per-tx refunds (EIP-6780 same-tx selfdestruct, EIP-7702 auth-list refund)
	// can drive this counter negative; the block-level computation clamps to 0
	// (block_state_gas_used = max(0, intrinsic_state + executionStateGas)).
	executionStateGas int64
	// committedChildBytes is a per-depth stack of running totals — when a child
	// frame commits, its walkTotal is added to the parent's slot. Used at
	// frame-commit to compute delta = walkTotal - committedChildBytes[depth].
	// Signed because cleared-slot rules (EIP-8037) and child frames that net
	// shrink state can produce negative walkTotals that propagate to parents.
	committedChildBytes []int64
}

// NewEVM returns a new EVM. The returned EVM is not thread safe and should
// only ever be used *once*.
func NewEVM(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, ibs *state.IntraBlockState, chainConfig *chain.Config, vmConfig Config) *EVM {
	if vmConfig.NoBaseFee {
		if txCtx.GasPrice.IsZero() {
			blockCtx.BaseFee = uint256.Int{}
		}
	}
	evm := &EVM{
		Context:         blockCtx,
		TxContext:       txCtx,
		intraBlockState: ibs,
		config:          vmConfig,
		chainConfig:     chainConfig,
		chainRules:      blockCtx.Rules(chainConfig),
	}
	evm.jt = jumpTable(evm.chainRules, vmConfig)

	return evm
}

// Reset resets the EVM with a new transaction context.Reset
// This is not threadsafe and should only be done very cautiously.
func (evm *EVM) Reset(txCtx evmtypes.TxContext, ibs *state.IntraBlockState) {
	evm.TxContext = txCtx
	evm.intraBlockState = ibs

	// ensure the evm is reset to be used again
	evm.abort.Store(false)
}

func (evm *EVM) ResetBetweenBlocks(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, ibs *state.IntraBlockState, vmConfig Config, chainRules *chain.Rules) {
	if vmConfig.NoBaseFee {
		if txCtx.GasPrice.IsZero() {
			blockCtx.BaseFee = uint256.Int{}
		}
	}
	evm.Context = blockCtx
	evm.TxContext = txCtx
	evm.intraBlockState = ibs
	evm.config = vmConfig
	evm.chainRules = chainRules

	evm.depth = 0
	evm.returnData = nil
	evm.jt = jumpTable(chainRules, vmConfig)

	// ensure the evm is reset to be used again
	evm.abort.Store(false)
}

// Cancel cancels any running EVM operation. This may be called concurrently and
// it's safe to be called multiple times.
func (evm *EVM) Cancel() { evm.abort.Store(true) }

// Cancelled returns true if Cancel has been called
func (evm *EVM) Cancelled() bool { return evm.abort.Load() }

// ExecutionStateGas returns the EIP-8037 execution state gas accumulated across
// all frame commits in the current tx. Reset to 0 on top-level revert.
func (evm *EVM) ExecutionStateGas() int64 { return evm.executionStateGas }

// RegularGasConsumed returns the total regular gas charged during tx execution (for block-level accounting)
func (evm *EVM) RegularGasConsumed() uint64 { return evm.regularGasConsumed }

// ResetGasConsumed resets the gas consumed counters for a new transaction
func (evm *EVM) ResetGasConsumed() {
	evm.regularGasConsumed = 0
	evm.executionStateGas = 0
	evm.committedChildBytes = evm.committedChildBytes[:0]
}

// pushFrameAccumulator pushes a 0 onto the per-depth committedChildBytes stack
// at frame entry. Pair with popFrameAccumulator at frame exit (commit or revert).
func (evm *EVM) pushFrameAccumulator() {
	evm.committedChildBytes = append(evm.committedChildBytes, 0)
}

// popFrameAccumulator pops the top of committedChildBytes and returns its value.
// At commit time the caller passes the popped value to the parent via
// propagateChildBytes. On revert the caller discards it.
func (evm *EVM) popFrameAccumulator() int64 {
	n := len(evm.committedChildBytes)
	if n == 0 {
		return 0
	}
	top := evm.committedChildBytes[n-1]
	evm.committedChildBytes = evm.committedChildBytes[:n-1]
	return top
}

// propagateChildBytes adds walkTotal to the parent's committedChildBytes slot
// after a child frame commits. If we are at depth 0 (no parent) this is a no-op.
//
// Negative walkTotals (from EIP-8037 cleared-slot credits) are NOT propagated:
// matching EELS, the reservoir credit happens at the child frame, while the
// parent's `already_paid` (committedChildBytes) tracks only positive charges.
// Reservoir gas itself is propagated via the gas return mechanism (the parent
// receives the child's leftover gas, which includes any credited state gas).
func (evm *EVM) propagateChildBytes(walkTotal int64) {
	n := len(evm.committedChildBytes)
	if n == 0 {
		return
	}
	if walkTotal > 0 {
		evm.committedChildBytes[n-1] += walkTotal
	}
}

// adjustGasStateAndExecution applies a signed `restoreGas` adjustment to the
// frame's state-gas reservoir (gas.State) and the running per-tx
// `executionStateGas` counter. Used on frame revert to undo the per-byte effects
// committed by descendants: positive `restoreGas` returns previously-charged
// gas to the reservoir (and decrements executionStateGas); negative
// `restoreGas` reverses a previously-credited descendant (e.g. EIP-8037
// cleared-slot) and decrements the reservoir.
func adjustGasStateAndExecution(gas *mdgas.MdGas, executionStateGas *int64, restoreGas int64) {
	if restoreGas >= 0 {
		gas.State += uint64(restoreGas)
	} else {
		sub := uint64(-restoreGas)
		if gas.State >= sub {
			gas.State -= sub
		} else {
			gas.State = 0
		}
	}
	*executionStateGas -= restoreGas
}

// chargeFrameStateGas runs the EIP-8037 frame-end state-gas accounting for a
// successful call/create commit, when Amsterdam is active and the EVM is not
// in RestoreState mode. It walks the journal segment [frameStart, frameEnd),
// computes delta = walkTotal - committedChildBytes[depth] - accountRefund, and:
//   - delta > 0 → charge delta×CPSB from gas.State (with spill into gas.Regular).
//     On insufficient gas, returns ErrFrameStateGasOOG (caller treats as REVERT).
//   - delta < 0 → credit |delta|×CPSB back to gas.State and decrement
//     evm.executionStateGas.
//
// Returns the walkTotal so the caller can propagate it to the parent's
// accumulator. The caller is responsible for pushing/popping the per-depth
// accumulator slot via pushFrameAccumulator/popFrameAccumulator.
//
// At depth==0 (top frame), the walk computes accountRefund — bytes for accounts
// in (newlyCreated && selfdestructed), mirroring EELS's accounts_to_delete ∩
// created_accounts refund. This naturally subsumes the per-tx
// CREATE+SELFDESTRUCT case (e.g. test_selfdestruct_in_create_tx_initcode) and
// the cross-frame CREATE-then-SELFDESTRUCT cases (cancun/eip6780_selfdestruct).
//
// excludeCreate is the contract address for top-level CREATE txs (depth==0
// inside evm.create) — its 112-byte account record is intrinsic-charged so
// the walk skips it. NilAddress in all other cases.
func (evm *EVM) chargeFrameStateGas(
	gas *mdgas.MdGas,
	frameStart int,
	depth int,
	excludeCreate accounts.Address,
) (walkTotal int64, err error) {
	frameEnd := evm.intraBlockState.JournalLength()
	applyFilter := depth == 0
	var accountRefund uint64
	walkTotal, accountRefund = evm.intraBlockState.ComputeFrameStateBytes(frameStart, frameEnd, applyFilter, excludeCreate)

	n := len(evm.committedChildBytes)
	var childTotal int64
	if n > 0 {
		childTotal = evm.committedChildBytes[n-1]
	}

	cpsb := evm.Context.CostPerStateByte
	delta := walkTotal - childTotal - int64(accountRefund)
	if delta == 0 {
		return walkTotal, nil
	}

	if delta > 0 {
		stateGas := uint64(delta) * cpsb
		var ok bool
		*gas, ok = useMdGas(evm, *gas, stateGas, mdgas.StateGas, evm.config.Tracer, tracing.GasChangeFrameStateGas)
		if !ok {
			return walkTotal, ErrFrameStateGasOOG
		}
		return walkTotal, nil
	}
	creditGas := uint64(-delta) * cpsb
	if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
		evm.config.Tracer.OnGasChange(gas.State, gas.State+creditGas, tracing.GasChangeFrameStateGas)
	}
	gas.State += creditGas
	// EIP-8037: when delta < 0 (cleared-slot credits, EIP-6780 same-tx
	// selfdestruct refunds) we credit the reservoir but DO NOT decrement
	// executionStateGas. This matches EELS's apply_frame_state_gas, where the
	// negative this_call_cost path only updates state_gas_reservoir and leaves
	// state_gas_used unchanged. The reservoir credit propagates to the parent
	// via the gas return mechanism, while the per-tx executionStateGas counter
	// only tracks positive charges (which determine block_state_gas_used).
	//
	// Exception: account-refund (EIP-6780 same-tx-CREATE+SELFDESTRUCT of an
	// account that EXISTED at tx-entry) DOES need to drive executionStateGas
	// negative to offset the intrinsic_state contribution. That refund is in
	// `accountRefund`; the cleared-slot credit is the part beyond accountRefund.
	if int64(accountRefund) > 0 {
		// Decrement executionStateGas only by the accountRefund portion of
		// the credit (capped by total credit). The remainder of the credit
		// (cleared-slot rules) does not affect executionStateGas.
		acctCreditGas := accountRefund * cpsb
		if acctCreditGas > creditGas {
			acctCreditGas = creditGas
		}
		evm.executionStateGas -= int64(acctCreditGas)
	}
	return walkTotal, nil
}

// handleFrameRevert handles the error path for a call or create frame:
// it reverts journal state and burns remaining regular gas on exceptional halt.
//
// EIP-8037 state-gas accounting is journal-walk-based at frame commit, so a
// reverted/halted frame contributes nothing — there's nothing to roll back
// here for state gas (the frame-end charge never fired).
//
// ErrFrameStateGasOOG is treated like REVERT: state is rolled back but the
// frame's remaining gas is preserved (returned to parent), matching EELS
// where apply_frame_state_gas's OOG sets evm.error directly without raising
// ExceptionalHalt.
func (evm *EVM) handleFrameRevert(gas *mdgas.MdGas, err error, depth int, snapshot int) {
	// 1. Revert state changes.
	evm.intraBlockState.RevertToSnapshot(snapshot, err)

	// 2. On exceptional halt (not REVERT, not soft OOG from frame-state-gas),
	//    burn remaining regular gas.
	if err != ErrExecutionReverted && err != ErrFrameStateGasOOG {
		if evm.chainRules.IsAmsterdam {
			evm.regularGasConsumed += gas.Regular
		}
		if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
			evm.config.Tracer.OnGasChange(gas.Regular, 0, tracing.GasChangeCallFailedExecution)
		}
		gas.Regular = 0
	}
}

// CallGasTemp returns the callGasTemp for the EVM
func (evm *EVM) CallGasTemp() uint64 {
	return evm.callGasTemp
}

// SetCallGasTemp sets the callGasTemp for the EVM
func (evm *EVM) SetCallGasTemp(gas uint64) {
	evm.callGasTemp = gas
}

func isSystemCall(caller accounts.Address) bool {
	return caller == params.SystemAddress
}

// SetPrecompiles sets the precompiles for the EVM
func (evm *EVM) SetPrecompiles(precompiles PrecompiledContracts) {
	evm.precompiles = precompiles
}

func (evm *EVM) call(typ OpCode, caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int, bailout bool) (ret []byte, leftOverGas mdgas.MdGas, err error) {
	if evm.abort.Load() {
		return ret, leftOverGas, nil
	}

	depth := evm.depth

	version := evm.intraBlockState.Version()
	if (dbg.TraceTransactionIO && !dbg.TraceInstructions) && (evm.intraBlockState.Trace() || dbg.TraceAccount(caller.Handle())) {
		fmt.Printf("%d (%d.%d) %s: %x %x\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, typ, addr, input)
		defer func() {
			fmt.Printf("%d (%d.%d) RETURN (%s): %x: %x, %d, %v\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, typ, addr, ret, leftOverGas, err)
		}()
	}

	p, isPrecompile := evm.precompile(addr)
	var code []byte
	if !isPrecompile {
		code, err = evm.intraBlockState.ResolveCode(addr)
		if err != nil {
			return nil, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
	}

	// Invoke tracer hooks that signal entering/exiting a call frame
	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, addr, isPrecompile, input, gas, value, code)
		defer func(startGas mdgas.MdGas) {
			evm.captureEnd(depth, typ, startGas, leftOverGas, ret, err)
		}(gas)
	}

	// BAL: record address access even if call fails due to gas/call depth/insufficient balance
	evm.intraBlockState.MarkAddressAccess(addr, false)

	if evm.config.NoRecursion && depth > 0 {
		return nil, gas, nil
	}
	// Fail if we're trying to execute above the call depth limit
	if depth > int(params.CallCreateDepth) {
		return nil, gas, ErrDepth
	}
	syscall := isSystemCall(caller)

	if typ == CALL || typ == CALLCODE {
		// Fail if we're trying to transfer more than the available balance.
		// Skip the check for zero-value calls, matching geth's short-circuit.
		if !value.IsZero() {
			canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
			if err != nil {
				return nil, mdgas.MdGas{}, err
			}
			if !canTransfer && !bailout {
				return nil, gas, ErrInsufficientBalance
			}
		}
	}

	snapshot := evm.intraBlockState.PushSnapshot()
	defer evm.intraBlockState.PopSnapshot(snapshot)

	if typ == CALL {
		exist, err := evm.intraBlockState.Exist(addr)
		if err != nil {
			return nil, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if !exist {
			// Under Spurious Dragon, a zero-value CALL to a non-existent
			// non-precompile account short-circuits as a no-op instead of
			// creating the account. This also preserves the EIP-4788
			// beacon-root syscall's "no-op when not deployed" semantics at
			// the fork-transition block, before the contract is deployed.
			if !isPrecompile && evm.chainRules.IsSpuriousDragon && value.IsZero() {
				return nil, gas, nil
			}
			evm.intraBlockState.CreateAccount(addr, false)
		}
		// System calls use TouchAccount instead of Transfer to avoid
		// spurious balance reads on the caller that would pollute the
		// Block Access List (EIP-7928). The touch is still needed so
		// AuRa/Gnosis keeps the empty system account in the PMT.
		if syscall && value.IsZero() {
			if err := evm.intraBlockState.TouchAccount(caller); err != nil {
				return nil, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
			}
		} else {
			// Normal (non-syscall) calls always go through Transfer —
			// this handles both value movement and the zero-balance touch
			// required for state clearing.
			if err := evm.Context.Transfer(evm.intraBlockState, caller, addr, value, bailout, evm.chainRules); err != nil {
				return nil, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
			}
		}
	} else if typ == STATICCALL {
		// We do an AddBalance of zero here, just in order to trigger a touch.
		// This doesn't matter on Mainnet, where all empties are gone at the time of Byzantium,
		// but is the correct thing to do and matters on other networks, in tests, and potential
		// future scenarios
		evm.intraBlockState.AddBalance(addr, u256.Num0, tracing.BalanceChangeTouchAccount)
	}

	// EIP-8037 frame-end state-gas accounting: capture the journal index at
	// frame entry, push a per-depth committed-children accumulator slot.
	frameStart := evm.intraBlockState.JournalLength()
	evm.pushFrameAccumulator()
	frameAccumulatorPopped := false

	// It is allowed to call precompiles, even via delegatecall
	if isPrecompile {
		preGas := gas.Regular
		ret, gas.Regular, err = RunPrecompiledContract(p, input, gas.Regular, evm.Config().Tracer)
		if evm.chainRules.IsAmsterdam {
			evm.regularGasConsumed += preGas - gas.Regular
		}
	} else if len(code) == 0 {
		// If the account has no code, we can abort here
		// The depth-check is already done, and precompiles handled above
		ret, err = nil, nil // gas is unchanged
	} else {
		// Initialise a new contract and set the code that is to be used by the EVM.
		// The contract is a scoped environment for this execution context only.
		var codeHash accounts.CodeHash
		codeHash, err = evm.intraBlockState.ResolveCodeHash(addr)
		if err != nil {
			evm.popFrameAccumulator()
			return nil, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		var contract Contract
		if typ == CALLCODE {
			contract = Contract{
				caller:   caller,
				addr:     caller,
				value:    value,
				Code:     code,
				CodeHash: codeHash,
			}
		} else if typ == DELEGATECALL {
			contract = Contract{
				caller:   callerAddress,
				addr:     caller,
				value:    value,
				Code:     code,
				CodeHash: codeHash,
			}
		} else {
			contract = Contract{
				caller:   caller,
				addr:     addr,
				value:    value,
				Code:     code,
				CodeHash: codeHash,
			}
		}
		readOnly := false
		if typ == STATICCALL {
			readOnly = true
		}
		ret, gas, err = evm.Run(contract, gas, input, readOnly)
	}

	// EIP-8037 frame commit (success path, Amsterdam, not in RestoreState):
	// run the journal-walk-and-charge before we evaluate the error path so
	// that an OOG at the frame-end charge correctly triggers a revert.
	if err == nil && !evm.config.RestoreState && evm.chainRules.IsAmsterdam {
		walkTotal, chargeErr := evm.chargeFrameStateGas(&gas, frameStart, depth, accounts.NilAddress)
		if chargeErr != nil {
			err = chargeErr
		} else {
			// Commit succeeded: pop self, propagate walkTotal to parent's accumulator.
			evm.popFrameAccumulator()
			frameAccumulatorPopped = true
			evm.propagateChildBytes(walkTotal)
		}
	}

	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in Homestead this also counts for code storage gas errors.
	if err != nil || evm.config.RestoreState {
		if !frameAccumulatorPopped {
			poppedChildBytes := evm.popFrameAccumulator()
			// EIP-8037: on child revert or exceptional halt, all state gas
			// consumed by committed descendants — both from the reservoir and
			// any that spilled into gas_left — is restored to this frame's
			// reservoir (which is then propagated to the parent via
			// restoreChildGas). Skip in RestoreState mode (caller discards
			// results anyway). At depth==0 this also naturally zeroes
			// evm.executionStateGas because the top frame's accumulator holds
			// the sum of all charged bytes for the tx.
			if !evm.config.RestoreState && evm.chainRules.IsAmsterdam && poppedChildBytes != 0 {
				restoreGas := poppedChildBytes * int64(evm.Context.CostPerStateByte)
				adjustGasStateAndExecution(&gas, &evm.executionStateGas, restoreGas)
			}
		}
		evm.handleFrameRevert(&gas, err, depth, snapshot)
	} else if !frameAccumulatorPopped {
		// Pre-Amsterdam: no state-gas charging happens in this branch; pop the
		// accumulator without propagating.
		evm.popFrameAccumulator()
	}

	return ret, gas, err
}

// Call executes the contract associated with the addr with the given input as
// parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
func (evm *EVM) Call(caller accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int, bailout bool) (ret []byte, leftOverGas mdgas.MdGas, err error) {
	return evm.call(CALL, caller, caller, addr, input, gas, value, bailout)
}

// CallCode executes the contract associated with the addr with the given input
// as parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
//
// CallCode differs from Call in the sense that it executes the given address'
// code with the caller as context.
func (evm *EVM) CallCode(caller accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int) (ret []byte, leftOverGas mdgas.MdGas, err error) {
	return evm.call(CALLCODE, caller, caller, addr, input, gas, value, false)
}

// DelegateCall executes the contract associated with the addr with the given input
// as parameters. It reverses the state in case of an execution error.
//
// DelegateCall differs from CallCode in the sense that it executes the given address'
// code with the caller as context and the caller is set to the caller of the caller.
func (evm *EVM) DelegateCall(caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, value uint256.Int, gas mdgas.MdGas) (ret []byte, leftOverGas mdgas.MdGas, err error) {
	return evm.call(DELEGATECALL, caller, callerAddress, addr, input, gas, value, false)
}

// StaticCall executes the contract associated with the addr with the given input
// as parameters while disallowing any modifications to the state during the call.
// Opcodes that attempt to perform such modifications will result in exceptions
// instead of performing the modifications.
func (evm *EVM) StaticCall(caller accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas) (ret []byte, leftOverGas mdgas.MdGas, err error) {
	return evm.call(STATICCALL, caller, caller, addr, input, gas, uint256.Int{}, false)
}

type codeAndHash struct {
	code []byte
	hash accounts.CodeHash
}

func NewCodeAndHash(code []byte) *codeAndHash {
	return &codeAndHash{code: code}
}

func (c *codeAndHash) Hash() accounts.CodeHash {
	if c.hash.IsZero() {
		c.hash = accounts.InternCodeHash(crypto.HashData(c.code))
	}
	return c.hash
}

func (evm *EVM) OverlayCreate(caller accounts.Address, codeAndHash *codeAndHash, gas mdgas.MdGas, value uint256.Int, address accounts.Address, typ OpCode, incrementNonce bool) ([]byte, accounts.Address, mdgas.MdGas, error) {
	return evm.create(caller, codeAndHash, gas, value, address, typ, incrementNonce, false)
}

// create creates a new contract using code as deployment code.
func (evm *EVM) create(caller accounts.Address, codeAndHash *codeAndHash, gasRemaining mdgas.MdGas, value uint256.Int, address accounts.Address, typ OpCode, incrementNonce bool, bailout bool) (ret []byte, createAddress accounts.Address, leftOverGas mdgas.MdGas, err error) {
	if dbg.TraceTransactionIO && (evm.intraBlockState.Trace() || dbg.TraceAccount(caller.Handle())) {
		defer func() {
			version := evm.intraBlockState.Version()
			if err != nil {
				fmt.Printf("%d (%d.%d) Create Contract: %x, err=%s\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, createAddress, err)
			} else {
				fmt.Printf("%d (%d.%d) Create Contract: %x, gas=%d\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, createAddress, leftOverGas)
			}
		}()
	}

	depth := evm.depth

	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, address, false, codeAndHash.code, gasRemaining, value, nil)
		defer func(startGas mdgas.MdGas) {
			evm.captureEnd(depth, typ, startGas, leftOverGas, ret, err)
		}(gasRemaining)
	}

	// Depth check execution. Fail if we're trying to execute above the
	// limit.
	if depth > int(params.CallCreateDepth) {
		err = ErrDepth
		return nil, accounts.NilAddress, gasRemaining, err
	}
	canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
	if err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, err
	}
	if !canTransfer {
		if !bailout {
			err = ErrInsufficientBalance
			return nil, accounts.NilAddress, gasRemaining, err
		}
	}
	if incrementNonce {
		nonce, err := evm.intraBlockState.GetNonce(caller)
		if err != nil {
			return nil, accounts.NilAddress, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if nonce+1 < nonce {
			err = ErrNonceUintOverflow
			return nil, accounts.NilAddress, gasRemaining, err
		}
		evm.intraBlockState.SetNonce(caller, nonce+1)
	}
	// We add this to the access list _before_ taking a snapshot. Even if the creation fails,
	// the access-list change should not be rolled back
	if evm.chainRules.IsBerlin {
		evm.intraBlockState.AddAddressToAccessList(address)
	}
	// BAL: record target address even on failed CREATE/CREATE2 calls
	evm.intraBlockState.MarkAddressAccess(address, false)

	// Ensure there's no existing contract already at the designated address.
	// Use GetCodeHash (not ResolveCodeHash) so that an EIP-7702 delegation
	// designator (0xef0100...) is seen as non-empty code, triggering a collision.
	// This matches geth's behavior: CREATE/CREATE2 must not overwrite a
	// delegated account even if the delegation target is empty.
	contractHash, err := evm.intraBlockState.GetCodeHash(address)
	if err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	nonce, err := evm.intraBlockState.GetNonce(address)
	if err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	hasStorage, err := evm.intraBlockState.HasStorage(address)
	if err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	if nonce != 0 || !contractHash.IsEmpty() || hasStorage {
		err = ErrContractAddressCollision
		// EIP-8037: At depth > 0, track collision-burned gas in regularGasConsumed
		// so 2D block gas accounting reflects the gas consumed on EIP-684 collision.
		// At depth 0 (CREATE transaction), the burned gas is accounted for through
		// the zero-gas return in txn_executor.go (regular_gas_used=0 per spec).
		if evm.chainRules.IsAmsterdam && depth > 0 {
			evm.regularGasConsumed += gasRemaining.Regular
		}
		if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
			evm.Config().Tracer.OnGasChange(gasRemaining.Regular, 0, tracing.GasChangeCallFailedExecution)
		}
		// Preserve State so the parent's reservoir is restored by restoreChildGas.
		return nil, accounts.NilAddress, mdgas.MdGas{State: gasRemaining.State}, err
	}
	// Create a new account on the state
	snapshot := evm.intraBlockState.PushSnapshot()
	defer evm.intraBlockState.PopSnapshot(snapshot)

	evm.intraBlockState.CreateAccount(address, true)
	if evm.chainRules.IsSpuriousDragon {
		evm.intraBlockState.SetNonce(address, 1)
	}
	if err := evm.Context.Transfer(evm.intraBlockState, caller, address, value, bailout, evm.chainRules); err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}

	// Initialise a new contract and set the code that is to be used by the EVM.
	// The contract is a scoped environment for this execution context only.
	contract := Contract{
		caller:   caller,
		addr:     address,
		value:    value,
		Code:     codeAndHash.code,
		CodeHash: codeAndHash.hash,
	}

	if evm.config.NoRecursion && depth > 0 {
		return nil, address, gasRemaining, nil
	}

	// EIP-8037 frame-end state-gas accounting: capture the journal index at
	// frame entry, push the per-depth committed-children accumulator slot.
	frameStart := evm.intraBlockState.JournalLength()
	evm.pushFrameAccumulator()
	frameAccumulatorPopped := false

	ret, gasRemaining, err = evm.Run(contract, gasRemaining, nil, false)

	// EIP-170: Contract code size limit
	if err == nil {
		err = CheckMaxCodeSize(len(ret), evm.chainRules)
	}
	// Reject code starting with 0xEF if EIP-3541 is enabled.
	if err == nil && evm.chainRules.IsLondon && len(ret) >= 1 && ret[0] == 0xEF {
		err = ErrInvalidCode
	}
	// If the contract creation ran successfully and no errors were returned,
	// calculate the regular gas required to store the code. If the code could
	// not be stored due to not enough gas, set an error when we're in Homestead
	// and let it be handled by the error checking condition below.
	//
	// EIP-8037: state gas for code deposit is NOT charged here — it falls out
	// of the journal-walk at frame commit (via the codeChange entry SetCode
	// emits below). Only the regular gas for code deposit is charged inline.
	if err == nil {
		preDepositGas := gasRemaining

		var regularGas uint64
		if evm.chainRules.IsAmsterdam {
			// EIP-8037 "Contract deployment cost calculation", success path:
			// HASH_COST(L) = 6*ceil(L/32). The state component (cpsb*L) is
			// derived at frame commit from the codeChange journal entry.
			regularGas = params.Keccak256WordGas * ToWordSize(uint64(len(ret)))
		} else {
			regularGas = uint64(len(ret)) * params.CreateDataGas
		}
		var regularGasOk bool
		gasRemaining, regularGasOk = useMdGas(evm, gasRemaining, regularGas, mdgas.RegularGas, evm.Config().Tracer, tracing.GasChangeCallCodeStorage)

		if regularGasOk {
			evm.intraBlockState.SetCode(address, ret)
		} else {
			if evm.chainRules.IsAmsterdam {
				// Code-deposit OOG: per EIP-8037 the failure cost is
				// GAS_CREATE + initcode_execution_cost only; code-deposit
				// gas is excluded. Restore regular gas to pre-deposit state
				// (state gas needs no rollback — SetCode hasn't fired so
				// the codeChange isn't in the journal).
				gasRemaining = preDepositGas
			}
			// If we run out of gas, we do not store the code: the returned code must be empty.
			ret = []byte{}
			if evm.chainRules.IsHomestead {
				err = ErrCodeStoreOutOfGas
			}
		}
	}

	// EIP-8037 frame commit (success path, Amsterdam, not in RestoreState):
	// run the journal-walk-and-charge before the error path so that an OOG at
	// the frame-end charge correctly triggers a revert. excludeCreate is the
	// contract address when this is the top-level CREATE tx (depth==0); the
	// 112×CPSB intrinsic already covers it, so the walk must skip it to avoid
	// double-counting.
	if err == nil && !evm.config.RestoreState && evm.chainRules.IsAmsterdam {
		excludeCreate := accounts.NilAddress
		if depth == 0 {
			excludeCreate = address
		}
		walkTotal, chargeErr := evm.chargeFrameStateGas(&gasRemaining, frameStart, depth, excludeCreate)
		if chargeErr != nil {
			err = chargeErr
		} else {
			evm.popFrameAccumulator()
			frameAccumulatorPopped = true
			evm.propagateChildBytes(walkTotal)
		}
	}

	// When an error was returned by the EVM or when setting the creation code
	// above, we revert to the snapshot and consume any gas remaining. Additionally,
	// when we're in Homestead, this also counts for code storage gas errors.
	if err != nil && (evm.chainRules.IsHomestead || err != ErrCodeStoreOutOfGas) {
		if !frameAccumulatorPopped {
			poppedChildBytes := evm.popFrameAccumulator()
			// EIP-8037: on child revert or exceptional halt, restore all state
			// gas consumed by committed descendants to this frame's reservoir.
			// See evm.call() for the full rationale.
			if !evm.config.RestoreState && evm.chainRules.IsAmsterdam && poppedChildBytes != 0 {
				restoreGas := poppedChildBytes * int64(evm.Context.CostPerStateByte)
				adjustGasStateAndExecution(&gasRemaining, &evm.executionStateGas, restoreGas)
			}
		}
		evm.handleFrameRevert(&gasRemaining, err, depth, snapshot)
	} else if !frameAccumulatorPopped {
		// Pre-Amsterdam or RestoreState: pop the accumulator without
		// propagating (no state-gas charging happens in this branch).
		evm.popFrameAccumulator()
	}

	return ret, address, gasRemaining, err
}

// Create creates a new contract using code as deployment code.
// If salt is non-nil, CREATE2 addressing is used (keccak256(0xff ++ msg.sender ++ salt ++ keccak256(init_code))[12:]);
// otherwise the usual sender-and-nonce-hash is used (CREATE).
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (evm *EVM) Create(caller accounts.Address, code []byte, gasRemaining mdgas.MdGas, endowment uint256.Int, salt *uint256.Int, bailout bool) (ret []byte, contractAddr accounts.Address, leftOverGas mdgas.MdGas, err error) {
	ch := &codeAndHash{code: code}
	op := CREATE
	if salt != nil {
		op = CREATE2
		contractAddr = accounts.InternAddress(types.CreateAddress2(caller.Value(), salt.Bytes32(), ch.Hash()))
	} else {
		var nonce uint64
		nonce, err = evm.intraBlockState.GetNonce(caller)
		if err != nil {
			return nil, accounts.NilAddress, mdgas.MdGas{}, err
		}
		contractAddr = accounts.InternAddress(types.CreateAddress(caller.Value(), nonce))
	}
	return evm.create(caller, ch, gasRemaining, endowment, contractAddr, op, true /* incrementNonce */, bailout)
}

// SysCreate is a special (system) contract creation methods for genesis constructors.
// Unlike the normal Create & Create2, it doesn't increment caller's nonce.
func (evm *EVM) SysCreate(caller accounts.Address, code []byte, gas mdgas.MdGas, endowment uint256.Int, contractAddr accounts.Address) (ret []byte, leftOverGas mdgas.MdGas, err error) {
	ret, _, leftOverGas, err = evm.create(caller, &codeAndHash{code: code}, gas, endowment, contractAddr, CREATE, false /* incrementNonce */, false)
	return
}

// Config returns the environment's chain configuration
func (evm *EVM) Config() Config {
	return evm.config
}

// ChainConfig returns the environment's chain configuration
func (evm *EVM) ChainConfig() *chain.Config {
	return evm.chainConfig
}

// ChainRules returns the environment's chain rules
func (evm *EVM) ChainRules() *chain.Rules {
	return evm.chainRules
}

// IntraBlockState returns the EVM's IntraBlockState
func (evm *EVM) IntraBlockState() *state.IntraBlockState {
	return evm.intraBlockState
}

// GetVMContext provides context about the block being executed as well as state
// to the tracers.
func (evm *EVM) GetVMContext() *tracing.VMContext {
	return &tracing.VMContext{
		Coinbase:        evm.Context.Coinbase,
		BlockNumber:     evm.Context.BlockNumber,
		Time:            evm.Context.Time,
		Random:          evm.Context.PrevRanDao,
		GasPrice:        evm.TxContext.GasPrice,
		ChainConfig:     evm.ChainConfig(),
		IntraBlockState: evm.IntraBlockState(),
		TxHash:          evm.TxHash,
	}
}

func (evm *EVM) captureBegin(depth int, typ OpCode, from accounts.Address, to accounts.Address, precompile bool, input []byte, startGas mdgas.MdGas, value uint256.Int, code []byte) {
	tracer := evm.Config().Tracer

	if tracer.OnEnter != nil {
		tracer.OnEnter(depth, byte(typ), from, to, precompile, input, startGas.Regular, value, code)
	}
	if tracer.OnGasChange != nil {
		tracer.OnGasChange(0, startGas.Regular, tracing.GasChangeCallInitialBalance)
	}
}

func (evm *EVM) captureEnd(depth int, typ OpCode, startGas mdgas.MdGas, leftOverGas mdgas.MdGas, ret []byte, err error) {
	tracer := evm.Config().Tracer

	if leftOverGas.Regular != 0 && tracer.OnGasChange != nil {
		tracer.OnGasChange(leftOverGas.Regular, 0, tracing.GasChangeCallLeftOverReturned)
	}

	var reverted bool
	if err != nil {
		reverted = true
	}
	if !evm.chainRules.IsHomestead && errors.Is(err, ErrCodeStoreOutOfGas) {
		reverted = false
	}

	if tracer.OnExit != nil {
		tracer.OnExit(depth, ret, startGas.Regular-leftOverGas.Regular, VMErrorFromErr(err), reverted)
	}
}
