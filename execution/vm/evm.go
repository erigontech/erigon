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

// handleFrameRevert handles the full error path for a call or create frame:
// state revert, regular gas burning on exceptional halt, and EIP-8037
// state-gas reservoir restoration for child frames.
//
// At depth 0 there is no parent reservoir to restore to: the evm.call /
// evm.create depth==0 defers fold the error semantics into gasUsed.State
// (reset to 0 for CALL, -StateGasNewAccount for CREATE) so TxnExecutor
// can read gasUsed.State directly without checking the error.
func (evm *EVM) handleFrameRevert(gasRemaining *mdgas.MdGas, err error, depth int, snapshot int, stateGasUsed int64) {

	// 1. Revert state changes.
	evm.intraBlockState.RevertToSnapshot(snapshot, err)

	// 2. On exceptional halt (not REVERT), burn remaining regular gas.
	if err != ErrExecutionReverted {
		if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
			evm.config.Tracer.OnGasChange(gasRemaining.Regular, 0, tracing.GasChangeCallFailedExecution)
		}
		gasRemaining.Regular = 0
	}

	// 3. EIP-8037: at depth>0 restore the original child reservoir to the
	// parent. Per the spec's incorporate_child_on_error invariant:
	//   parent.state_gas_left += child.state_gas_used + child.state_gas_left
	// (the sum equals what the parent originally passed in, after any
	// inline refunds the child credited are dropped along with reverted
	// state). Regular gas: REVERT preserves it (step 2 didn't apply);
	// exceptional halt burnt it in step 2. At depth==0 the user-side
	// refund flows through gasUsed.State (see func doc).
	//
	// The sum must be >= 0. `gasRemaining.State + stateGasUsed` starts at
	// the parent-passed-in reservoir R (≥ 0) and is monotonically
	// non-decreasing: charges within the reservoir and inline refunds
	// both leave the sum unchanged (charge: `stateGas -= a;
	// frameStateUsed += a`; refund: `stateGas += a; frameStateUsed -=
	// a`), and charges that spill into regular gas only grow it
	// (stateGas clamps at 0 while frameStateUsed gets the full amount).
	// So when stateGasUsed is negative, `gasRemaining.State >= |stateGasUsed|`
	// and the cast doesn't underflow. If the sum does go negative, an
	// EIP-8037 accounting invariant is broken upstream — panic rather
	// than silently wrapping and corrupting the parent's reservoir.
	if evm.chainRules.IsAmsterdam && depth > 0 {
		restored := int64(gasRemaining.State) + stateGasUsed
		if restored < 0 {
			panic(fmt.Sprintf("EIP-8037 invariant violated: state_gas_left (%d) + child state_gas_used (%d) = %d < 0",
				gasRemaining.State, stateGasUsed, restored))
		}
		gasRemaining.State = uint64(restored)
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

func (evm *EVM) call(typ OpCode, caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int, bailout bool) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	if evm.abort.Load() {
		return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, nil
	}

	depth := evm.depth
	gasRemaining = gas

	// Derive gasUsed.Regular from the final gasRemaining at function exit,
	// uniformly across Run / precompile / no-code paths and after any
	// handleFrameRevert gas burn. gasUsed.State is set by Run's defer
	// (signed frameStateUsed = charges − inline refunds) and is 0 for
	// precompile/no-code frames. Regular = (input − leftover) − state,
	// computed in signed int64 so a negative net state correctly grows
	// the regular component (refund credit lands in the reservoir,
	// which leftover absorbs back).
	//
	// At depth==0 on error, the spec resets the top-frame's state_gas_used
	// to 0 (the refund-on-failure invariant). Mirror that on the returned
	// gasUsed.State so TxnExecutor's tx_state_gas computation is uniform
	// across success and error paths. (At depth>0, handleFrameRevert
	// already restored the child's reservoir to the parent.)
	inputTotal := gas.Total()
	defer func() {
		leftOverTotal := gasRemaining.Total()
		if leftOverTotal <= inputTotal {
			delta := int64(inputTotal - leftOverTotal)
			gasUsed.Regular = uint64(delta - gasUsed.State)
		}
		if depth == 0 && evm.chainRules.IsAmsterdam && err != nil {
			gasUsed.State = 0
		}
	}()

	version := evm.intraBlockState.Version()
	if (dbg.TraceTransactionIO && !dbg.TraceInstructions) && (evm.intraBlockState.Trace() || dbg.TraceAccount(caller.Handle())) {
		fmt.Printf("%d (%d.%d) %s: %x %x\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, typ, addr, input)
		defer func() {
			fmt.Printf("%d (%d.%d) RETURN (%s): %x: %x, %d, %v\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, typ, addr, ret, gasRemaining, err)
		}()
	}

	p, isPrecompile := evm.precompile(addr)
	var code []byte
	if !isPrecompile {
		code, err = evm.intraBlockState.ResolveCode(addr)
		if err != nil {
			return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
	}

	// Invoke tracer hooks that signal entering/exiting a call frame
	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, addr, isPrecompile, input, gas, value, code)
		defer func(startGas mdgas.MdGas) {
			evm.captureEnd(depth, typ, startGas, gasRemaining, ret, err)
		}(gas)
	}

	// BAL: record address access even if call fails due to gas/call depth/insufficient balance
	evm.intraBlockState.MarkAddressAccess(addr, false)

	if evm.config.NoRecursion && depth > 0 {
		return nil, gasRemaining, mdgas.MdGasUsage{}, nil
	}
	// Fail if we're trying to execute above the call depth limit
	if depth > int(params.CallCreateDepth) {
		return nil, gasRemaining, mdgas.MdGasUsage{}, ErrDepth
	}
	syscall := isSystemCall(caller)

	if typ == CALL || typ == CALLCODE {
		// Fail if we're trying to transfer more than the available balance.
		// Skip the check for zero-value calls, matching geth's short-circuit.
		if !value.IsZero() {
			canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
			if err != nil {
				return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, err
			}
			if !canTransfer && !bailout {
				return nil, gasRemaining, mdgas.MdGasUsage{}, ErrInsufficientBalance
			}
		}
	}

	snapshot := evm.intraBlockState.PushSnapshot()
	defer evm.intraBlockState.PopSnapshot(snapshot)

	if typ == CALL {
		exist, err := evm.intraBlockState.Exist(addr)
		if err != nil {
			return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if !exist {
			// Under Spurious Dragon, a zero-value CALL to a non-existent
			// non-precompile account short-circuits as a no-op instead of
			// creating the account. This also preserves the EIP-4788
			// beacon-root syscall's "no-op when not deployed" semantics at
			// the fork-transition block, before the contract is deployed.
			if !isPrecompile && evm.chainRules.IsSpuriousDragon && value.IsZero() {
				return nil, gasRemaining, mdgas.MdGasUsage{}, nil
			}
			evm.intraBlockState.CreateAccount(addr, false)
		}
		// System calls use TouchAccount instead of Transfer to avoid
		// spurious balance reads on the caller that would pollute the
		// Block Access List (EIP-7928). The touch is still needed so
		// AuRa/Gnosis keeps the empty system account in the PMT.
		if syscall && value.IsZero() {
			if err := evm.intraBlockState.TouchAccount(caller); err != nil {
				return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
			}
		} else {
			// Normal (non-syscall) calls always go through Transfer —
			// this handles both value movement and the zero-balance touch
			// required for state clearing.
			if err := evm.Context.Transfer(evm.intraBlockState, caller, addr, value, bailout, evm.chainRules); err != nil {
				return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
			}
		}
	} else if typ == STATICCALL {
		// Trigger a touch on the callee so EIP-161 state clearing applies to
		// empty accounts (matters on test networks; on Mainnet all empties are
		// gone by Byzantium). Use TouchAccount rather than AddBalance(0): the
		// latter has a serial-mode shortcut for the RIPEMD-160 precompile
		// (special-snowflake balance-increase path) that bypasses
		// GetOrNewStateObject. Without loading the account the FinalizeTx
		// "exists in dirties but not stateObjects → skip" branch fires and
		// the touch never reaches state-clearing — diverging from
		// CALL's behavior, which loads the account via Exist() before the
		// zero-value Transfer. Affects ethereum/tests RevertPrecompiledTouch_d3.
		if err := evm.intraBlockState.TouchAccount(addr); err != nil {
			return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
	}

	// It is allowed to call precompiles, even via delegatecall
	if isPrecompile {
		ret, gasRemaining.Regular, err = RunPrecompiledContract(p, input, gasRemaining.Regular, evm.Config().Tracer)
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
			return nil, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
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
		ret, gasRemaining, gasUsed, err = evm.Run(contract, gasRemaining, input, readOnly)
	}
	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in Homestead this also counts for code storage gas errors.
	if err != nil || evm.config.RestoreState {
		evm.handleFrameRevert(&gasRemaining, err, depth, snapshot, gasUsed.State)
	}

	return ret, gasRemaining, gasUsed, err
}

// Call executes the contract associated with the addr with the given input as
// parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
func (evm *EVM) Call(caller accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int, bailout bool) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	return evm.call(CALL, caller, caller, addr, input, gas, value, bailout)
}

// CallCode executes the contract associated with the addr with the given input
// as parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
//
// CallCode differs from Call in the sense that it executes the given address'
// code with the caller as context.
func (evm *EVM) CallCode(caller accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas, value uint256.Int) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	return evm.call(CALLCODE, caller, caller, addr, input, gas, value, false)
}

// DelegateCall executes the contract associated with the addr with the given input
// as parameters. It reverses the state in case of an execution error.
//
// DelegateCall differs from CallCode in the sense that it executes the given address'
// code with the caller as context and the caller is set to the caller of the caller.
func (evm *EVM) DelegateCall(caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, value uint256.Int, gas mdgas.MdGas) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	return evm.call(DELEGATECALL, caller, callerAddress, addr, input, gas, value, false)
}

// StaticCall executes the contract associated with the addr with the given input
// as parameters while disallowing any modifications to the state during the call.
// Opcodes that attempt to perform such modifications will result in exceptions
// instead of performing the modifications.
func (evm *EVM) StaticCall(caller accounts.Address, addr accounts.Address, input []byte, gas mdgas.MdGas) (ret []byte, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
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

func (evm *EVM) OverlayCreate(caller accounts.Address, codeAndHash *codeAndHash, gas mdgas.MdGas, value uint256.Int, address accounts.Address, typ OpCode, incrementNonce bool) ([]byte, accounts.Address, mdgas.MdGas, mdgas.MdGasUsage, error) {
	return evm.create(caller, codeAndHash, gas, value, address, typ, incrementNonce, false)
}

// create creates a new contract using code as deployment code.
func (evm *EVM) create(caller accounts.Address, codeAndHash *codeAndHash, gas mdgas.MdGas, value uint256.Int, address accounts.Address, typ OpCode, incrementNonce bool, bailout bool) (ret []byte, createAddress accounts.Address, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	gasRemaining = gas

	if dbg.TraceTransactionIO && (evm.intraBlockState.Trace() || dbg.TraceAccount(caller.Handle())) {
		defer func() {
			version := evm.intraBlockState.Version()
			if err != nil {
				fmt.Printf("%d (%d.%d) Create Contract: %x, err=%s\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, createAddress, err)
			} else {
				fmt.Printf("%d (%d.%d) Create Contract: %x, gas=%d\n", evm.intraBlockState.BlockNumber(), version.TxIndex, version.Incarnation, createAddress, gasRemaining)
			}
		}()
	}

	depth := evm.depth

	// Derive gasUsed.Regular from the final gasRemaining at function exit,
	// uniformly across all Create exit paths (Run, depth/balance/collision
	// errors, post-handleFrameRevert gas burn). gasUsed.State is set by Run's
	// defer for the initcode frame (signed net) and stays 0 on early-exit
	// paths. Regular = (input − leftover) − state in signed int64 so a
	// negative net state correctly grows the regular component.
	//
	// At depth==0 on error, the spec resets the top-frame's state_gas_used
	// to 0 AND adds STATE_BYTES_PER_NEW_ACCOUNT * COST_PER_STATE_BYTE to
	// state_refund (the contract was never created, so the intrinsic
	// NEW_ACCOUNT state-gas is returned). Mirror both by setting
	// gasUsed.State = −StateGasNewAccount so TxnExecutor's
	// tx_state_gas = intrinsic_state + gasUsed.State naturally yields the
	// "intrinsic refunded" outcome (= 0 for a CREATE tx, since its
	// intrinsic_state == StateGasNewAccount). The Call counterpart does NOT
	// refund intrinsic AUTH state-gas — EIP-7702 auth side effects persist
	// even on call failure.
	inputTotal := gas.Total()
	defer func() {
		leftOverTotal := gasRemaining.Total()
		if leftOverTotal <= inputTotal {
			delta := int64(inputTotal - leftOverTotal)
			gasUsed.Regular = uint64(delta - gasUsed.State)
		}
		if depth == 0 && evm.chainRules.IsAmsterdam && err != nil {
			gasUsed.State = -int64(params.StateGasNewAccount)
		}
	}()

	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, address, false, codeAndHash.code, gas, value, nil)
		defer func(startGas mdgas.MdGas) {
			evm.captureEnd(depth, typ, startGas, gasRemaining, ret, err)
		}(gas)
	}

	// Depth check execution. Fail if we're trying to execute above the
	// limit.
	if depth > int(params.CallCreateDepth) {
		err = ErrDepth
		return nil, accounts.NilAddress, gasRemaining, mdgas.MdGasUsage{}, err
	}
	canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
	if err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, mdgas.MdGasUsage{}, err
	}
	if !canTransfer {
		if !bailout {
			err = ErrInsufficientBalance
			return nil, accounts.NilAddress, gasRemaining, mdgas.MdGasUsage{}, err
		}
	}
	if incrementNonce {
		nonce, err := evm.intraBlockState.GetNonce(caller)
		if err != nil {
			return nil, accounts.NilAddress, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if nonce+1 < nonce {
			err = ErrNonceUintOverflow
			return nil, accounts.NilAddress, gasRemaining, mdgas.MdGasUsage{}, err
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
		return nil, accounts.NilAddress, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	nonce, err := evm.intraBlockState.GetNonce(address)
	if err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	hasStorage, err := evm.intraBlockState.HasStorage(address)
	if err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	if nonce != 0 || !contractHash.IsEmpty() || hasStorage {
		err = ErrContractAddressCollision
		if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
			evm.Config().Tracer.OnGasChange(gasRemaining.Regular, 0, tracing.GasChangeCallFailedExecution)
		}
		// Preserve State so the parent's reservoir is restored by restoreChildGas.
		return nil, accounts.NilAddress, mdgas.MdGas{State: gasRemaining.State}, mdgas.MdGasUsage{}, err
	}
	// Create a new account on the state
	snapshot := evm.intraBlockState.PushSnapshot()
	defer evm.intraBlockState.PopSnapshot(snapshot)

	evm.intraBlockState.CreateAccount(address, true)
	if evm.chainRules.IsSpuriousDragon {
		evm.intraBlockState.SetNonce(address, 1)
	}
	if err := evm.Context.Transfer(evm.intraBlockState, caller, address, value, bailout, evm.chainRules); err != nil {
		return nil, accounts.NilAddress, mdgas.MdGas{}, mdgas.MdGasUsage{}, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
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
		return nil, address, gasRemaining, mdgas.MdGasUsage{}, nil
	}

	ret, gasRemaining, gasUsed, err = evm.Run(contract, gasRemaining, nil, false)

	// EIP-170: Contract code size limit
	if err == nil {
		err = CheckMaxCodeSize(len(ret), evm.chainRules)
	}
	// Reject code starting with 0xEF if EIP-3541 is enabled.
	if err == nil && evm.chainRules.IsLondon && len(ret) >= 1 && ret[0] == 0xEF {
		err = ErrInvalidCode
	}
	// If the contract creation ran successfully and no errors were returned,
	// calculate the gas required to store the code. If the code could not
	// be stored due to not enough gas, set an error when we're in Homestead and let it be handled
	// by the error checking condition below.
	if err == nil {
		// EIP-8037: GAS_CODE_DEPOSIT = cpsb/byte (state) + 6*ceil(len/32) (regular)
		// Pre-Amsterdam: GAS_CODE_DEPOSIT = 200/byte (regular only)
		preDepositGas := gasRemaining

		// Charge state gas (Amsterdam only).
		stateGasOk := true
		var stateGas uint64
		if evm.chainRules.IsAmsterdam {
			stateGas = uint64(len(ret)) * params.CostPerStateByte
			gasRemaining, stateGasOk = useMdGas(gasRemaining, stateGas, mdgas.StateGas, evm.Config().Tracer, tracing.GasChangeCallCodeStorage)
		}

		// Charge regular gas.
		var regularGasOk bool
		if stateGasOk {
			var regularGas uint64
			if evm.chainRules.IsAmsterdam {
				// EIP-8037 "Contract deployment cost calculation", success path:
				// HASH_COST(L) = 6*ceil(L/32); the state component (cpsb*L) is charged above.
				regularGas = params.Keccak256WordGas * ToWordSize(uint64(len(ret)))
			} else {
				regularGas = uint64(len(ret)) * params.CreateDataGas
			}
			gasRemaining, regularGasOk = useMdGas(gasRemaining, regularGas, mdgas.RegularGas, evm.Config().Tracer, tracing.GasChangeCallCodeStorage)
		}

		if stateGasOk && regularGasOk {
			evm.intraBlockState.SetCode(address, ret)
			// EIP-8037: post-Run code-deposit state charge counts toward this
			// frame's state-gas usage.
			gasUsed.State += int64(stateGas)
		} else {
			if evm.chainRules.IsAmsterdam {
				// Code deposit failed: per EIP-8037 the failure cost is
				// GAS_CREATE + initcode_execution_cost only; code deposit
				// gas (both state and regular) is excluded.
				gasRemaining = preDepositGas
			}
			// If we run out of gas, we do not store the code: the returned code must be empty.
			ret = []byte{}
			if evm.chainRules.IsHomestead {
				err = ErrCodeStoreOutOfGas
			}
		}
	}

	// When an error was returned by the EVM or when setting the creation code
	// above, we revert to the snapshot and consume any gas remaining. Additionally,
	// when we're in Homestead, this also counts for code storage gas errors.
	if err != nil && (evm.chainRules.IsHomestead || err != ErrCodeStoreOutOfGas) {
		evm.handleFrameRevert(&gasRemaining, err, depth, snapshot, gasUsed.State)
	}

	return ret, address, gasRemaining, gasUsed, err
}

// Create creates a new contract using code as deployment code.
// If salt is non-nil, CREATE2 addressing is used (keccak256(0xff ++ msg.sender ++ salt ++ keccak256(init_code))[12:]);
// otherwise the usual sender-and-nonce-hash is used (CREATE).
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (evm *EVM) Create(caller accounts.Address, code []byte, gas mdgas.MdGas, endowment uint256.Int, salt *uint256.Int, bailout bool) (ret []byte, contractAddr accounts.Address, gasRemaining mdgas.MdGas, gasUsed mdgas.MdGasUsage, err error) {
	ch := &codeAndHash{code: code}
	op := CREATE
	if salt != nil {
		op = CREATE2
		contractAddr = accounts.InternAddress(types.CreateAddress2(caller.Value(), salt.Bytes32(), ch.Hash()))
	} else {
		var nonce uint64
		nonce, err = evm.intraBlockState.GetNonce(caller)
		if err != nil {
			return nil, accounts.NilAddress, mdgas.MdGas{}, mdgas.MdGasUsage{}, err
		}
		contractAddr = accounts.InternAddress(types.CreateAddress(caller.Value(), nonce))
	}
	return evm.create(caller, ch, gas, endowment, contractAddr, op, true /* incrementNonce */, bailout)
}

// SysCreate is a special (system) contract creation methods for genesis constructors.
// Unlike the normal Create & Create2, it doesn't increment caller's nonce.
func (evm *EVM) SysCreate(caller accounts.Address, code []byte, gas mdgas.MdGas, endowment uint256.Int, contractAddr accounts.Address) (ret []byte, gasRemaining mdgas.MdGas, err error) {
	ret, _, gasRemaining, _, err = evm.create(caller, &codeAndHash{code: code}, gas, endowment, contractAddr, CREATE, false /* incrementNonce */, false)
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
