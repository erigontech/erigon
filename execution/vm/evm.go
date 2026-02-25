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
	"math/big"
	"sync/atomic"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/arb/chainparams"
	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/u256"
	"github.com/erigontech/erigon/execution/chain"
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

	// chainConfig contains information about the current chain
	chainConfig *chain.Config
	// chain rules contains the chain rules for the current epoch
	chainRules *chain.Rules
	// virtual machine configuration options used to initialise the
	// evm.
	config Config
	// global (to this context) ethereum virtual machine
	// used throughout the execution of the tx.
	interpreter Interpreter
	// abort is used to abort the EVM calling operations
	abort atomic.Bool
	// callGasTemp holds the gas available for the current call. This is needed because the
	// available gas is calculated in gasCall* according to the 63/64 rule and later
	// applied in opCall*.
	callGasTemp uint64
	// optional overridden set of precompiled contracts
	precompiles PrecompiledContracts

	//Arbitrum processing hook
	ProcessingHookSet atomic.Bool
	ProcessingHook    TxProcessingHook
}

// NewEVM returns a new EVM. The returned EVM is not thread safe and should
// only ever be used *once*.
func NewEVM(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, ibs *state.IntraBlockState, chainConfig *chain.Config, vmConfig Config) *EVM {
	if vmConfig.NoBaseFee {
		if txCtx.GasPrice.IsZero() {
			if chainConfig.IsArbitrum() {
				blockCtx.BaseFeeInBlock = new(uint256.Int)
				if !blockCtx.BaseFee.IsZero() {
					blockCtx.BaseFeeInBlock.Set(&blockCtx.BaseFee)
				}
			}
			blockCtx.BaseFee = uint256.Int{}
		}
		if chainConfig.IsArbitrum() && txCtx.BlobFee.IsZero() {
			blockCtx.BlobBaseFee = u256.Num0
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

	evm.ProcessingHook = DefaultTxProcessor{evm: evm}
	evm.interpreter = NewEVMInterpreter(evm, vmConfig)

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

	evm.interpreter = NewEVMInterpreter(evm, vmConfig)

	// ensure the evm is reset to be used again
	evm.abort.Store(false)
}

// Cancel cancels any running EVM operation. This may be called concurrently and
// it's safe to be called multiple times.
func (evm *EVM) Cancel() { evm.abort.Store(true) }

// Cancelled returns true if Cancel has been called
func (evm *EVM) Cancelled() bool { return evm.abort.Load() }

// CallGasTemp returns the callGasTemp for the EVM
func (evm *EVM) CallGasTemp() uint64 {
	return evm.callGasTemp
}

// SetCallGasTemp sets the callGasTemp for the EVM
func (evm *EVM) SetCallGasTemp(gas uint64) {
	evm.callGasTemp = gas
}

// SetPrecompiles sets the precompiles for the EVM
func (evm *EVM) SetPrecompiles(precompiles PrecompiledContracts) {
	evm.precompiles = precompiles
}

// Interpreter returns the current interpreter
func (evm *EVM) Interpreter() Interpreter {
	return evm.interpreter
}

func isSystemCall(caller accounts.Address) bool {
	return caller == chainparams.SystemAddress
}

func (evm *EVM) call(typ OpCode, caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, gas uint64, value uint256.Int, bailout bool, arbInfo *AdvancedPrecompileCall) (ret []byte, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
	if evm.abort.Load() {
		return ret, leftOverGas, multigas.ZeroGas(), nil
	}

	depth := evm.interpreter.Depth()

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
			return nil, 0, multigas.ZeroGas(), fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
	}

	// Invoke tracer hooks that signal entering/exiting a call frame
	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, addr, isPrecompile, input, gas, value, code)
		defer func(startGas uint64) {
			evm.captureEnd(depth, typ, startGas, leftOverGas, ret, err)
		}(gas)
	}

	if evm.config.NoRecursion && depth > 0 {
		return nil, gas, multigas.ZeroGas(), nil
	}
	// Fail if we're trying to execute above the call depth limit
	if depth > int(params.CallCreateDepth) {
		return nil, gas, multigas.ZeroGas(), ErrDepth
	}
	if typ == CALL || typ == CALLCODE {
		// Fail if we're trying to transfer more than the available balance
		canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
		if err != nil {
			return nil, 0, multigas.ZeroGas(), err
		}
		if !bailout && !value.IsZero() && !canTransfer {
			return nil, gas, multigas.ZeroGas(), ErrInsufficientBalance
		}
	}

	// BAL: record address access even if call fails due to gas/call depth and to precompiles
	// ARBITRUM_MERGE: verify the following line
	evm.intraBlockState.MarkAddressAccess(addr)

	snapshot := evm.intraBlockState.PushSnapshot()
	defer evm.intraBlockState.PopSnapshot(snapshot)

	usedMultiGas = multigas.ZeroGas()

	if typ == CALL {
		exist, err := evm.intraBlockState.Exist(addr)
		if err != nil {
			return nil, 0, multigas.ZeroGas(), fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if !exist {
			if !isPrecompile && evm.chainRules.IsSpuriousDragon && value.IsZero() {
				return nil, gas, multigas.ZeroGas(), nil
			}
			evm.intraBlockState.CreateAccount(addr, false)
		}
		if err = evm.Context.Transfer(evm.intraBlockState, caller, addr, value, bailout); err != nil {
			return nil, gas, multigas.ComputationGas(gas), err
		}
	} else if typ == STATICCALL {
		// We do an AddBalance of zero here, just in order to trigger a touch.
		// This doesn't matter on Mainnet, where all empties are gone at the time of Byzantium,
		// but is the correct thing to do and matters on other networks, in tests, and potential
		// future scenarios
		evm.intraBlockState.AddBalance(addr, u256.Num0, tracing.BalanceChangeTouchAccount)
	}

	// It is allowed to call precompiles, even via delegatecall
	if isPrecompile {
		var precompileMultiGas multigas.MultiGas
		ret, gas, precompileMultiGas, err = RunPrecompiledContract(p, input, gas, evm.Config().Tracer, arbInfo)
		usedMultiGas.SaturatingAddInto(precompileMultiGas)
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
			return nil, 0, multigas.ZeroGas(), fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}

		contract := Contract{
			caller:   caller,
			addr:     addr,
			value:    value,
			Code:     code,
			CodeHash: codeHash,
		}

		if typ == CALLCODE {
			contract.addr = caller
			contract.delegateOrCallcode = true
		}
		if typ == DELEGATECALL {
			contract.caller = callerAddress
			contract.addr = caller
			contract.delegateOrCallcode = true
		}

		contract.IsSystemCall = isSystemCall(caller)
		// TODO addrCopy could rewrite contract.addr we set in callcode or delegatecall, eh?
		//contract.SetCallCode(&addrCopy, codeHash, code)
		readOnly := false
		if typ == STATICCALL {
			readOnly = true
		}
		runGasCost := multigas.ZeroGas()
		ret, gas, runGasCost, err = evm.interpreter.Run(contract, gas, runGasCost, input, readOnly)

		usedMultiGas.SaturatingAddInto(runGasCost)
		//usedMultiGas.SaturatingAddInto(ctx.GetTotalUsedMultiGas())
	}
	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in Homestead this also counts for code storage gas errors.
	if err != nil || evm.config.RestoreState {
		evm.intraBlockState.RevertToSnapshot(snapshot, err)
		if err != ErrExecutionReverted {
			if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
				evm.Config().Tracer.OnGasChange(gas, 0, tracing.GasChangeCallFailedExecution)
			}

			// Attribute all leftover gas to computation
			usedMultiGas.SaturatingIncrementInto(multigas.ResourceKindComputation, gas)
			gas = 0
		}
	}
	return ret, gas, usedMultiGas, err
}

// Call executes the contract associated with the addr with the given input as
// parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
func (evm *EVM) Call(caller accounts.Address, addr accounts.Address, input []byte, gas uint64, value *uint256.Int, bailout bool) (ret []byte, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
	return evm.call(CALL, caller, caller, addr, input, gas, *value, bailout, &AdvancedPrecompileCall{
		PrecompileAddress: addr.Value(),
		ActingAsAddress:   addr.Value(),
		Caller:            caller.Value(),
		Value:             value.ToBig(),
		ReadOnly:          false,
		Evm:               evm,
	})
}

// CallCode executes the contract associated with the addr with the given input
// as parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
//
// CallCode differs from Call in the sense that it executes the given address'
// code with the caller as context.
func (evm *EVM) CallCode(caller accounts.Address, addr accounts.Address, input []byte, gas uint64, value *uint256.Int) (ret []byte, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
	return evm.call(CALLCODE, caller, caller, addr, input, gas, *value, false, &AdvancedPrecompileCall{
		PrecompileAddress: addr.Value(),
		ActingAsAddress:   caller.Value(),
		Caller:            caller.Value(),
		Value:             value.ToBig(),
		ReadOnly:          false,
		Evm:               evm,
	})
}

// DelegateCall executes the contract associated with the addr with the given input
// as parameters. It reverses the state in case of an execution error.
//
// DelegateCall differs from CallCode in the sense that it executes the given address'
// code with the caller as context and the caller is set to the caller of the caller.
func (evm *EVM) DelegateCall(caller accounts.Address, callerAddress accounts.Address, addr accounts.Address, input []byte, value uint256.Int, gas uint64) (ret []byte, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
	return evm.call(DELEGATECALL, caller, callerAddress, addr, input, gas, value, false, &AdvancedPrecompileCall{
		PrecompileAddress: addr.Value(),
		ActingAsAddress:   caller.Value(),
		Caller:            callerAddress.Value(),
		Value:             value.ToBig(),
		ReadOnly:          false,
		Evm:               evm,
	})
}

// StaticCall executes the contract associated with the addr with the given input
// as parameters while disallowing any modifications to the state during the call.
// Opcodes that attempt to perform such modifications will result in exceptions
// instead of performing the modifications.
func (evm *EVM) StaticCall(caller accounts.Address, addr accounts.Address, input []byte, gas uint64) (ret []byte, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
	return evm.call(STATICCALL, caller, caller, addr, input, gas, u256.Num0, false, &AdvancedPrecompileCall{
		PrecompileAddress: addr.Value(),
		ActingAsAddress:   addr.Value(),
		Caller:            caller.Value(),
		Value:             new(big.Int),
		ReadOnly:          true,
		Evm:               evm,
	})
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
		c.hash = accounts.InternCodeHash(crypto.Keccak256Hash(c.code))
	}
	return c.hash
}

func (evm *EVM) OverlayCreate(caller accounts.Address, codeAndHash *codeAndHash, gas uint64, value *uint256.Int, address accounts.Address, typ OpCode, incrementNonce bool) ([]byte, accounts.Address, uint64, multigas.MultiGas, error) {
	//getCallContext(caller.)
	return evm.create(caller, codeAndHash, gas, *value, address, typ, incrementNonce, false)
}

// create creates a new contract using code as deployment code.
func (evm *EVM) create(caller accounts.Address, codeAndHash *codeAndHash, gasRemaining uint64, value uint256.Int, address accounts.Address, typ OpCode, incrementNonce bool, bailout bool) (ret []byte, createAddress accounts.Address, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
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

	depth := evm.interpreter.Depth()

	// BAL: record target address even on failed CREATE/CREATE2 calls
	evm.intraBlockState.MarkAddressAccess(address)

	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller, address, false, codeAndHash.code, gasRemaining, value, nil)
		defer func(startGas uint64) {
			evm.captureEnd(depth, typ, startGas, leftOverGas, ret, err)
		}(gasRemaining)
	}

	// Depth check execution. Fail if we're trying to execute above the
	// limit.
	if depth > int(params.CallCreateDepth) {
		err = ErrDepth
		return nil, accounts.NilAddress, gasRemaining, multigas.ZeroGas(), err
	}
	canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller, value)
	if err != nil {
		return nil, accounts.NilAddress, 0, multigas.ZeroGas(), err
	}
	if !canTransfer {
		if !bailout {
			err = ErrInsufficientBalance
			return nil, accounts.NilAddress, gasRemaining, multigas.ZeroGas(), err
		}
	}
	if incrementNonce {
		nonce, err := evm.intraBlockState.GetNonce(caller)
		if err != nil {
			return nil, accounts.NilAddress, 0, multigas.ZeroGas(), fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if nonce+1 < nonce {
			err = ErrNonceUintOverflow
			return nil, accounts.NilAddress, gasRemaining, multigas.ZeroGas(), err
		}
		evm.intraBlockState.SetNonce(caller, nonce+1)
	}
	// We add this to the access list _before_ taking a snapshot. Even if the creation fails,
	// the access-list change should not be rolled back
	if evm.chainRules.IsBerlin {
		evm.intraBlockState.AddAddressToAccessList(address)
	}
	// Ensure there's no existing contract already at the designated address
	contractHash, err := evm.intraBlockState.ResolveCodeHash(address)
	if err != nil {
		return nil, accounts.NilAddress, 0, multigas.ZeroGas(), fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	nonce, err := evm.intraBlockState.GetNonce(address)
	if err != nil {
		return nil, accounts.NilAddress, 0, multigas.ZeroGas(), fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	hasStorage, err := evm.intraBlockState.HasStorage(address)
	if err != nil {
		return nil, accounts.NilAddress, 0, multigas.ZeroGas(), fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	if nonce != 0 || !contractHash.IsEmpty() || hasStorage {
		err = ErrContractAddressCollision
		if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
			evm.Config().Tracer.OnGasChange(gasRemaining, 0, tracing.GasChangeCallFailedExecution)
		}
		return nil, accounts.NilAddress, 0, multigas.ComputationGas(gasRemaining), err
	}
	// Create a new account on the state
	snapshot := evm.intraBlockState.PushSnapshot()
	defer evm.intraBlockState.PopSnapshot(snapshot)

	evm.intraBlockState.CreateAccount(address, true)
	if evm.chainRules.IsSpuriousDragon {
		evm.intraBlockState.SetNonce(address, 1)
	}
	evm.Context.Transfer(evm.intraBlockState, caller, address, value, bailout)

	// Initialise a new contract and set the code that is to be used by the EVM.
	// The contract is a scoped environment for this execution context only.
	contract := Contract{
		caller:   caller,
		addr:     address,
		value:    value,
		Code:     codeAndHash.code,
		CodeHash: codeAndHash.hash,
	}

	scope := getCallContext(contract, value.Bytes(), gasRemaining)

	if evm.config.NoRecursion && depth > 0 {
		return nil, address, gasRemaining, scope.UsedMultiGas, nil
	}

	multiGasSpent := multigas.ZeroGas()
	ret, gasRemaining, multiGasSpent, err = evm.interpreter.Run(contract, gasRemaining, multiGasSpent, nil, false)
	scope.gas = gasRemaining
	scope.UsedMultiGas.SaturatingAddInto(multiGasSpent)

	// EIP-170: Contract code size limit
	if err == nil && evm.chainRules.IsSpuriousDragon && len(ret) > evm.maxCodeSize() {
		// Gnosis Chain prior to Shanghai didn't have EIP-170 enabled,
		// but EIP-3860 (part of Shanghai) requires EIP-170.
		if !evm.chainRules.IsAura || evm.config.HasEip3860(evm.chainRules) {
			err = ErrMaxCodeSizeExceeded
		}
	}

	// Reject code starting with 0xEF if EIP-3541 is enabled.
	if err == nil && evm.chainRules.IsLondon && len(ret) >= 1 && ret[0] == 0xEF {
		err = ErrInvalidCode

		// Arbitrum: retain Stylus programs and instead store them in the DB alongside normal EVM bytecode.
		if evm.chainRules.IsStylus && state.IsStylusProgram(ret) {
			err = nil
		}
	}

	// If the contract creation ran successfully and no errors were returned,
	// calculate the gas required to store the code. If the code could not
	// be stored due to not enough gas, set an error when we're in Homestead and let it be handled
	// by the error checking condition below.
	if err == nil {
		createDataGas := uint64(len(ret)) * params.CreateDataGas
		if scope.UseMultiGas(multigas.StorageGrowthGas(createDataGas), evm.Config().Tracer, tracing.GasChangeCallCodeStorage) {
			evm.intraBlockState.SetCode(address, ret)
		} else {
			// If we run out of gas, we do not store the code: the returned code must be empty.
			ret = []byte{}
			if evm.chainRules.IsHomestead { // TODO ARBitrum! does not check IsHomestead; but not affects on stylus exec
				err = ErrCodeStoreOutOfGas
			}
		}
	}

	// When an error was returned by the EVM or when setting the creation code
	// above, we revert to the snapshot and consume any gas remaining. Additionally,
	// when we're in Homestead, this also counts for code storage gas errors.
	if err != nil && (evm.chainRules.IsHomestead || err != ErrCodeStoreOutOfGas) {
		evm.intraBlockState.RevertToSnapshot(snapshot, err)
		if err != ErrExecutionReverted {
			//scope.UseMultiGas(multigas.ComputationGas(scope.gas), evm.Config().Tracer, tracing.GasChangeCallFailedExecution)
			scope.UseMultiGas(multiGasSpent, evm.Config().Tracer, tracing.GasChangeCallFailedExecution)
			/*MERGE_ARBITRUM, main erigon has:
			gasRemaining, _ = useGas(gasRemaining, gasRemaining, evm.Config().Tracer, tracing.GasChangeCallFailedExecution)
			*/
		}
	}
	//if gasRemaining != scope.gas {
	//	panic(fmt.Errorf("gas remains %d scope gas %d", gasRemaining, scope.gas))
	//}
	return ret, address, scope.Gas(), scope.GetTotalUsedMultiGas(), err
}

func (evm *EVM) maxCodeSize() int {
	if evm.chainConfig.Bor != nil && evm.chainConfig.Bor.IsAhmedabad(evm.Context.BlockNumber) {
		return params.MaxCodeSizePostAhmedabad
	}
	return params.MaxCodeSize
}

//func (evm *EVM) CreateWithContext(ctx *CallContext) (ret []byte, contractAddr accounts.Address, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
//	caller := ctx.Contract.Address()
//
//	var (
//		value   = ctx.Stack.pop()
//		offset  = ctx.Stack.pop()
//		size    = ctx.Stack.peek()
//		input   = ctx.Memory.GetCopy(offset.Uint64(), size.Uint64())
//		gas     = ctx.gas
//		bailout = false
//	)
//	if evm.ChainRules().IsTangerineWhistle {
//		gas -= gas / 64
//	}
//
//	return evm.create(caller, NewCodeAndHash(input), gas, value, ctx.CreateAddress, CREATE, true, bailout)
//}

// Create creates a new contract using code as deployment code.
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (evm *EVM) Create(caller accounts.Address, code []byte, gasRemaining uint64, endowment *uint256.Int, bailout bool) (ret []byte, contractAddr accounts.Address, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
	nonce, err := evm.intraBlockState.GetNonce(caller)
	if err != nil {
		return nil, accounts.NilAddress, 0, multigas.ZeroGas(), err
	}
	contractAddr = accounts.InternAddress(types.CreateAddress(caller.Value(), nonce))
	return evm.create(caller, &codeAndHash{code: code}, gasRemaining, *endowment, contractAddr, CREATE, true, bailout)
}

// Create2 creates a new contract using code as deployment code.
//
// The different between Create2 with Create is Create2 uses keccak256(0xff ++ msg.sender ++ salt ++ keccak256(init_code))[12:]
// instead of the usual sender-and-nonce-hash as the address where the contract is initialized at.
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (evm *EVM) Create2(caller accounts.Address, code []byte, gasRemaining uint64, endowment uint256.Int, salt *uint256.Int, bailout bool) (ret []byte, contractAddr accounts.Address, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
	codeAndHash := &codeAndHash{code: code}
	contractAddr = accounts.InternAddress(types.CreateAddress2(caller.Value(), salt.Bytes32(), codeAndHash.Hash()))
	return evm.create(caller, codeAndHash, gasRemaining, endowment, contractAddr, CREATE2, true, bailout)
}

// SysCreate is a special (system) contract creation methods for genesis constructors.
// Unlike the normal Create & Create2, it doesn't increment caller's nonce.
func (evm *EVM) SysCreate(caller accounts.Address, code []byte, gas uint64, endowment uint256.Int, contractAddr accounts.Address) (ret []byte, leftOverGas uint64, usedMultiGas multigas.MultiGas, err error) {
	ret, _, leftOverGas, usedMultiGas, err = evm.create(caller, &codeAndHash{code: code}, gas, endowment, contractAddr, CREATE, false, false)
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

		ArbOSVersion: evm.Context.ArbOSVersion,
	}
}

func (evm *EVM) captureBegin(depth int, typ OpCode, from accounts.Address, to accounts.Address, precompile bool, input []byte, startGas uint64, value uint256.Int, code []byte) {
	tracer := evm.Config().Tracer

	if tracer.OnEnter != nil {
		tracer.OnEnter(depth, byte(typ), from, to, precompile, input, startGas, value, code)
	}
	if tracer.OnGasChange != nil {
		tracer.OnGasChange(0, startGas, tracing.GasChangeCallInitialBalance)
	}
}

func (evm *EVM) captureEnd(depth int, typ OpCode, startGas uint64, leftOverGas uint64, ret []byte, err error) {
	tracer := evm.Config().Tracer

	if leftOverGas != 0 && tracer.OnGasChange != nil {
		tracer.OnGasChange(leftOverGas, 0, tracing.GasChangeCallLeftOverReturned)
	}

	var reverted bool
	if err != nil {
		reverted = true
	}
	if !evm.chainRules.IsHomestead && errors.Is(err, ErrCodeStoreOutOfGas) {
		reverted = false
	}

	if tracer.OnExit != nil {
		tracer.OnExit(depth, ret, startGas-leftOverGas, VMErrorFromErr(err), reverted)
	}
}

// Depth returns the current depth
func (evm *EVM) Depth() int {
	return evm.interpreter.Depth()
}
