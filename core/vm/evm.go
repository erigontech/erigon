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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/chain"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/u256"
	"github.com/erigontech/erigon-lib/crypto"
	"github.com/erigontech/erigon-lib/trie"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm/evmtypes"
	"github.com/erigontech/erigon/params"
)

var emptyHash = libcommon.Hash{}

func (evm *EVM) precompile(addr libcommon.Address) (PrecompiledContract, bool) {
	var precompiles map[libcommon.Address]PrecompiledContract
	switch {
	case evm.chainRules.IsPrague:
		precompiles = PrecompiledContractsPrague
	case evm.chainRules.IsNapoli:
		precompiles = PrecompiledContractsNapoli
	case evm.chainRules.IsCancun:
		precompiles = PrecompiledContractsCancun
	case evm.chainRules.IsBerlin:
		precompiles = PrecompiledContractsBerlin
	case evm.chainRules.IsIstanbul:
		precompiles = PrecompiledContractsIstanbul
	case evm.chainRules.IsByzantium:
		precompiles = PrecompiledContractsByzantium
	default:
		precompiles = PrecompiledContractsHomestead
	}
	p, ok := precompiles[addr]
	return p, ok
}

// run runs the given contract and takes care of running precompiles with a fallback to the byte code interpreter.
func run(evm *EVM, contract *Contract, input []byte, readOnly bool) ([]byte, error) {
	return evm.interpreter.Run(contract, input, readOnly)
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
	intraBlockState evmtypes.IntraBlockState

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

	JumpDestCache *JumpDestCache
}

// NewEVM returns a new EVM. The returned EVM is not thread safe and should
// only ever be used *once*.
func NewEVM(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, state evmtypes.IntraBlockState, chainConfig *chain.Config, vmConfig Config) *EVM {
	if vmConfig.NoBaseFee {
		if txCtx.GasPrice.IsZero() {
			blockCtx.BaseFee = new(uint256.Int)
		}
	}
	evm := &EVM{
		Context:         blockCtx,
		TxContext:       txCtx,
		intraBlockState: state,
		config:          vmConfig,
		chainConfig:     chainConfig,
		chainRules:      chainConfig.Rules(blockCtx.BlockNumber, blockCtx.Time),
		JumpDestCache:   NewJumpDestCache(),
	}
	interpreter := NewEVMInterpreter(evm, vmConfig)
	evm.interpreter = interpreter
	evm.config.JumpTableEOF = interpreter.cfg.JumpTableEOF
	return evm
}

// Reset resets the EVM with a new transaction context.Reset
// This is not threadsafe and should only be done very cautiously.
func (evm *EVM) Reset(txCtx evmtypes.TxContext, ibs evmtypes.IntraBlockState) {
	evm.TxContext = txCtx
	evm.intraBlockState = ibs

	// ensure the evm is reset to be used again
	evm.abort.Store(false)
}

func (evm *EVM) ResetBetweenBlocks(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, ibs evmtypes.IntraBlockState, vmConfig Config, chainRules *chain.Rules) {
	if vmConfig.NoBaseFee {
		if txCtx.GasPrice.IsZero() {
			blockCtx.BaseFee = new(uint256.Int)
		}
	}
	evm.Context = blockCtx
	evm.TxContext = txCtx
	evm.intraBlockState = ibs
	evm.config = vmConfig
	evm.chainRules = chainRules

	evm.interpreter = NewEVMInterpreter(evm, vmConfig)
	// TODO(racytech): jumptableEOF is nil at this point for some reason, so we set it manually
	if chainRules.IsOsaka {
		evm.config.JumpTableEOF = &eofInstructionSet
	}
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

// Interpreter returns the current interpreter
func (evm *EVM) Interpreter() Interpreter {
	return evm.interpreter
}

func (evm *EVM) call(typ OpCode, caller ContractRef, addr libcommon.Address, input []byte, gas uint64, value *uint256.Int, bailout bool) (ret []byte, leftOverGas uint64, err error) {
	depth := evm.interpreter.Depth()

	p, isPrecompile := evm.precompile(addr)
	var code []byte
	if !isPrecompile {
		code, err = evm.intraBlockState.ResolveCode(addr)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
	}

	// Invoke tracer hooks that signal entering/exiting a call frame
	if evm.Config().Tracer != nil {
		v := value
		if typ == STATICCALL {
			v = new(uint256.Int)
		} else if typ == DELEGATECALL {
			// NOTE: caller must, at all times be a contract. It should never happen
			// that caller is something other than a Contract.
			parent := caller.(*Contract)
			// DELEGATECALL inherits value from parent call
			v = parent.value
		}
		evm.captureBegin(depth, typ, caller.Address(), addr, isPrecompile, input, gas, v, code)
		defer func(startGas uint64) {
			evm.captureEnd(depth, typ, startGas, leftOverGas, ret, err)
		}(gas)
	}

	if evm.config.NoRecursion && depth > 0 {
		return nil, gas, nil
	}
	// Fail if we're trying to execute above the call depth limit
	if depth > int(params.CallCreateDepth) {
		return nil, gas, ErrDepth
	}
	if typ == CALL || typ == CALLCODE {
		// Fail if we're trying to transfer more than the available balance
		canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller.Address(), value)
		if err != nil {
			return nil, 0, err
		}
		if !value.IsZero() && !canTransfer {
			if !bailout {
				return nil, gas, ErrInsufficientBalance
			}
		}
	}

	snapshot := evm.intraBlockState.Snapshot()

	if typ == CALL {
		exist, err := evm.intraBlockState.Exist(addr)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if !exist {
			if !isPrecompile && evm.chainRules.IsSpuriousDragon && value.IsZero() {
				return nil, gas, nil
			}
			evm.intraBlockState.CreateAccount(addr, false)
		}
		evm.Context.Transfer(evm.intraBlockState, caller.Address(), addr, value, bailout)
	} else if typ == STATICCALL {
		// We do an AddBalance of zero here, just in order to trigger a touch.
		// This doesn't matter on Mainnet, where all empties are gone at the time of Byzantium,
		// but is the correct thing to do and matters on other networks, in tests, and potential
		// future scenarios
		evm.intraBlockState.AddBalance(addr, u256.Num0, tracing.BalanceChangeTouchAccount)
	}

	// It is allowed to call precompiles, even via delegatecall
	if isPrecompile {
		ret, gas, err = RunPrecompiledContract(p, input, gas, evm.Config().Tracer)
	} else if len(code) == 0 {
		// If the account has no code, we can abort here
		// The depth-check is already done, and precompiles handled above
		ret, err = nil, nil // gas is unchanged
	} else {
		// At this point, we use a copy of address. If we don't, the go compiler will
		// leak the 'contract' to the outer scope, and make allocation for 'contract'
		// even if the actual execution ends on RunPrecompiled above.
		addrCopy := addr
		// Initialise a new contract and set the code that is to be used by the EVM.
		// The contract is a scoped environment for this execution context only.
		var codeHash libcommon.Hash
		codeHash, err = evm.intraBlockState.ResolveCodeHash(addrCopy)
		if err != nil {
			return nil, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}

		var contract *Contract
		if typ == CALLCODE {
			contract = NewContract(caller, caller.Address(), value, gas, evm.config.SkipAnalysis, evm.JumpDestCache)
		} else if typ == DELEGATECALL {
			contract = NewContract(caller, caller.Address(), value, gas, evm.config.SkipAnalysis, evm.JumpDestCache).AsDelegate()
		} else {
			if value == nil { // TODO(racytech): see if this can be removed, eip7069_extcall/calls
				value = new(uint256.Int)
			}
			contract = NewContract(caller, addrCopy, value, gas, evm.config.SkipAnalysis, evm.JumpDestCache)
		}
		contract.SetCallCode(&addrCopy, codeHash, code, evm.parseContainer(code))
		readOnly := false
		if typ == STATICCALL {
			readOnly = true
		}
		ret, err = run(evm, contract, input, readOnly)
		gas = contract.Gas
	}

	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in Homestead this also counts for code storage gas errors.
	if err != nil || evm.config.RestoreState {
		evm.intraBlockState.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
				evm.Config().Tracer.OnGasChange(gas, 0, tracing.GasChangeCallFailedExecution)
			}
			gas = 0
		}
		// TODO: consider clearing up unused snapshots:
		//} else {
		//	evm.StateDB.DiscardSnapshot(snapshot)
	}
	return ret, gas, err
}

// Call executes the contract associated with the addr with the given input as
// parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
func (evm *EVM) Call(caller ContractRef, addr libcommon.Address, input []byte, gas uint64, value *uint256.Int, bailout bool) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(CALL, caller, addr, input, gas, value, bailout)
}

// CallCode executes the contract associated with the addr with the given input
// as parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
//
// CallCode differs from Call in the sense that it executes the given address'
// code with the caller as context.
func (evm *EVM) CallCode(caller ContractRef, addr libcommon.Address, input []byte, gas uint64, value *uint256.Int) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(CALLCODE, caller, addr, input, gas, value, false)
}

// DelegateCall executes the contract associated with the addr with the given input
// as parameters. It reverses the state in case of an execution error.
//
// DelegateCall differs from CallCode in the sense that it executes the given address'
// code with the caller as context and the caller is set to the caller of the caller.
func (evm *EVM) DelegateCall(caller ContractRef, addr libcommon.Address, input []byte, gas uint64) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(DELEGATECALL, caller, addr, input, gas, nil, false)
}

// StaticCall executes the contract associated with the addr with the given input
// as parameters while disallowing any modifications to the state during the call.
// Opcodes that attempt to perform such modifications will result in exceptions
// instead of performing the modifications.
func (evm *EVM) StaticCall(caller ContractRef, addr libcommon.Address, input []byte, gas uint64) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(STATICCALL, caller, addr, input, gas, new(uint256.Int), false)
}

func (evm *EVM) ExtCall(caller ContractRef, addr libcommon.Address, input []byte, gas uint64, value *uint256.Int) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(CALL, caller, addr, input, gas, value, false)
}
func (evm *EVM) ExtDelegateCall(caller ContractRef, addr libcommon.Address, input []byte, gas uint64) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(DELEGATECALL, caller, addr, input, gas, nil, false)
}
func (evm *EVM) ExtStaticCall(caller ContractRef, addr libcommon.Address, input []byte, gas uint64) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(STATICCALL, caller, addr, input, gas, nil, false)
}

type codeAndHash struct {
	code []byte
	hash libcommon.Hash
}

func NewCodeAndHash(code []byte) *codeAndHash {
	return &codeAndHash{code: code}
}

func (c *codeAndHash) Hash() libcommon.Hash {
	if c.hash == emptyHash {
		c.hash = crypto.Keccak256Hash(c.code)
	}
	return c.hash
}

func (evm *EVM) OverlayCreate(caller ContractRef, codeAndHash *codeAndHash, gas uint64, value *uint256.Int, address libcommon.Address, typ OpCode, incrementNonce bool) ([]byte, libcommon.Address, uint64, error) {
	return evm.create(caller, codeAndHash, gas, value, address, typ, nil /*input*/, incrementNonce, false, false /* allowEOF */)
}

// create creates a new contract using code as deployment code.
func (evm *EVM) create(caller ContractRef, codeAndHash *codeAndHash, gasRemaining uint64, value *uint256.Int, address libcommon.Address, typ OpCode, input []byte, incrementNonce, bailout, allowEOF bool) (ret []byte, createAddress libcommon.Address, leftOverGas uint64, err error) {
	depth := evm.interpreter.Depth()

	if evm.Config().Tracer != nil {
		evm.captureBegin(depth, typ, caller.Address(), address, false, codeAndHash.code, gasRemaining, value, nil)
		defer func(startGas uint64) {
			evm.captureEnd(depth, typ, startGas, leftOverGas, ret, err)
		}(gasRemaining)
	}

	// Depth check execution. Fail if we're trying to execute above the
	// limit.
	if depth > int(params.CallCreateDepth) {
		err = ErrDepth
		return nil, libcommon.Address{}, gasRemaining, err
	}
	canTransfer, err := evm.Context.CanTransfer(evm.intraBlockState, caller.Address(), value)
	if err != nil {
		return nil, libcommon.Address{}, 0, err
	}
	if !canTransfer {
		if !bailout {
			err = ErrInsufficientBalance
		}
		return nil, libcommon.Address{}, gasRemaining, err
	}

	// Initialise a new contract and set the code that is to be used by the EVM.
	// The contract is a scoped environment for this execution context only.
	contract := NewContract(caller, address, value, gasRemaining, evm.config.SkipAnalysis, evm.JumpDestCache)
	contract.SetCodeOptionalHash(&address, codeAndHash)

	isInitcodeEOF := hasEOFMagic(codeAndHash.code)
	// placing this after  nonce != 0 || (contractHash != (libcommon.Hash{}) && contractHash != trie.EmptyCodeHash) check
	// fixes JUMPF tests but brakes EOFCREATE tests
	if isInitcodeEOF {
		if allowEOF { // TODO(racytech): remake this entire allowEOF business?
			var containerKind byte = 1
			if depth == 0 || typ == EOFCREATE {
				containerKind = 0
			}
			if c, err := UnmarshalEOF(codeAndHash.code, 0, containerKind, evm.config.JumpTableEOF, true); err != nil {
				return nil, libcommon.Address{}, gasRemaining, fmt.Errorf("%w: %v", ErrInvalidEOFInitcode, err)
			} else {
				contract.Container = c
			}
		} else {
			// Don't allow EOF contract to execute legacy initcode.
			return nil, libcommon.Address{}, gasRemaining, fmt.Errorf("attmept to create EOF from legacy call (CREATE, CREATE2)")
		}
	}

	if incrementNonce {
		nonce, err := evm.intraBlockState.GetNonce(caller.Address())
		if err != nil {
			return nil, libcommon.Address{}, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
		}
		if nonce+1 < nonce {
			err = ErrNonceUintOverflow
			return nil, libcommon.Address{}, gasRemaining, err
		}
		evm.intraBlockState.SetNonce(caller.Address(), nonce+1)
	}
	// We add this to the access list _before_ taking a snapshot. Even if the creation fails,
	// the access-list change should not be rolled back
	if evm.chainRules.IsBerlin {
		evm.intraBlockState.AddAddressToAccessList(address)
	}

	// Ensure there's no existing contract already at the designated address
	contractHash, err := evm.intraBlockState.ResolveCodeHash(address)
	if err != nil {
		return nil, libcommon.Address{}, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	nonce, err := evm.intraBlockState.GetNonce(address)
	if err != nil {
		return nil, libcommon.Address{}, 0, fmt.Errorf("%w: %w", ErrIntraBlockStateFailed, err)
	}
	if nonce != 0 || (contractHash != (libcommon.Hash{}) && contractHash != trie.EmptyCodeHash) {
		err = ErrContractAddressCollision
		if evm.config.Tracer != nil && evm.config.Tracer.OnGasChange != nil {
			evm.Config().Tracer.OnGasChange(gasRemaining, 0, tracing.GasChangeCallFailedExecution)
		}
		return nil, libcommon.Address{}, 0, err
	}
	nonEmptyStorage, err := evm.intraBlockState.HasAtLeastOneStorage(address)
	if err != nil || nonEmptyStorage {
		err = ErrContractAddressCollision
		return nil, libcommon.Address{}, 0, err
	}

	// Create a new account on the state
	snapshot := evm.intraBlockState.Snapshot()
	evm.intraBlockState.CreateAccount(address, true)
	if evm.chainRules.IsSpuriousDragon {
		evm.intraBlockState.SetNonce(address, 1)
	}
	evm.Context.Transfer(evm.intraBlockState, caller.Address(), address, value, bailout)

	if evm.config.NoRecursion && depth > 0 {
		return nil, address, gasRemaining, nil
	}
	ret, err = run(evm, contract, input, false)

	// EIP-170: Contract code size limit
	if err == nil && evm.chainRules.IsSpuriousDragon && len(ret) > evm.maxCodeSize() {
		// Gnosis Chain prior to Shanghai didn't have EIP-170 enabled,
		// but EIP-3860 (part of Shanghai) requires EIP-170.
		if !evm.chainRules.IsAura || evm.config.HasEip3860(evm.chainRules) {
			err = ErrMaxCodeSizeExceeded
		}
	}

	// check whether the max code size has been exceeded
	if err == nil && evm.chainRules.IsSpuriousDragon && len(ret) > params.MaxCodeSize && !evm.chainRules.IsAura {
		err = ErrMaxCodeSizeExceeded
	}

	// TODO(racytech): see EIP-7620 "test cases" for more info
	if typ == CREATE || typ == CREATE2 {
		if len(ret) == 1 && ret[0] == eofMagic[0] {
			err = ErrInvalidCode
		}
	}

	// Reject legacy contract deployment from EOF.
	if err == nil && isInitcodeEOF && len(ret) > 0 && !hasEOFMagic(ret) {
		err = ErrLegacyCode
	}

	// Reject EOF deployment from legacy.
	if !isInitcodeEOF && hasEOFMagic(ret) {
		err = ErrLegacyCode
	}

	// Reject code starting with 0xEF if EIP-3541 is enabled.
	if err == nil && len(ret) >= 1 && HasEOFByte(ret) {
		if evm.chainRules.IsShanghai {
			// Don't reject EOF contracts after Shanghai
		} else if evm.chainRules.IsLondon {
			err = ErrInvalidCode
		}
	}

	// if the contract creation ran successfully and no errors were returned
	// calculate the gas required to store the code. If the code could not
	// be stored due to not enough gas set an error and let it be handled
	// by the error checking condition below.
	if err == nil {
		createDataGas := uint64(len(ret)) * params.CreateDataGas
		if contract.UseGas(createDataGas, evm.Config().Tracer, tracing.GasChangeCallCodeStorage) {
			evm.intraBlockState.SetCode(address, ret)
		} else if evm.chainRules.IsHomestead {
			err = ErrCodeStoreOutOfGas
		}
	}

	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in homestead this also counts for code storage gas errors.
	if err != nil && (evm.chainRules.IsHomestead || err != ErrCodeStoreOutOfGas) {
		evm.intraBlockState.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas, evm.Config().Tracer, tracing.GasChangeCallFailedExecution)
		}
	}

	return ret, address, contract.Gas, err
}

func (evm *EVM) maxCodeSize() int {
	if evm.chainConfig.Bor != nil && evm.chainConfig.Bor.IsAhmedabad(evm.Context.BlockNumber) {
		return params.MaxCodeSizePostAhmedabad
	}
	return params.MaxCodeSize
}

// Create creates a new contract using code as deployment code.
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (evm *EVM) Create(caller ContractRef, code []byte, gasRemaining uint64, endowment *uint256.Int, bailout bool, allowEOF bool) (ret []byte, contractAddr libcommon.Address, leftOverGas uint64, err error) {
	nonce, err := evm.intraBlockState.GetNonce(caller.Address())
	if err != nil {
		return nil, libcommon.Address{}, 0, err
	}
	contractAddr = crypto.CreateAddress(caller.Address(), nonce)
	return evm.create(caller, &codeAndHash{code: code}, gasRemaining, endowment, contractAddr, CREATE, nil, true /* incrementNonce */, bailout, allowEOF)
}

// Create2 creates a new contract using code as deployment code.
//
// The different between Create2 with Create is Create2 uses keccak256(0xff ++ msg.sender ++ salt ++ keccak256(init_code))[12:]
// instead of the usual sender-and-nonce-hash as the address where the contract is initialized at.
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (evm *EVM) Create2(caller ContractRef, code []byte, gas uint64, endowment *uint256.Int, salt *uint256.Int, bailout bool) (ret []byte, contractAddr libcommon.Address, leftOverGas uint64, err error) {
	codeAndHash := &codeAndHash{code: code}
	contractAddr = crypto.CreateAddress2(caller.Address(), salt.Bytes32(), codeAndHash.Hash().Bytes())
	// isCallerEOF := hasEOFMagic(evm.intraBlockState.GetCode(caller.Address())) // TODO(racytech): do we need this?
	return evm.create(caller, codeAndHash, gas, endowment, contractAddr, CREATE2, nil /* input */, true /* incrementNonce */, bailout, false)
}

func (evm *EVM) EOFCreate(caller ContractRef, input, initContainer []byte, gas uint64, endowment *uint256.Int, salt *uint256.Int, bailout bool) (ret []byte, contractAddr libcommon.Address, leftOverGas uint64, err error) {
	codeAndHash := &codeAndHash{code: initContainer}
	contractAddr = crypto.CreateEOFAddress(caller.Address(), salt.Bytes32(), initContainer)
	return evm.create(caller, codeAndHash, gas, endowment, contractAddr, EOFCREATE, input /* input */, true /* incrementNonce */, bailout, true)
}

func (evm *EVM) TxnCreate(caller ContractRef, code, initContainer []byte, gas uint64, endowment *uint256.Int, salt *uint256.Int, bailout bool) (ret []byte, contractAddr libcommon.Address, leftOverGas uint64, err error) {
	codeAndHash := &codeAndHash{code: initContainer}
	nonce, err := evm.intraBlockState.GetNonce(caller.Address())
	if err != nil {
		return nil, libcommon.Address{}, 0, err
	}
	contractAddr = crypto.CreateAddress(caller.Address(), nonce)
	return evm.create(caller, codeAndHash, gas, endowment, contractAddr, EOFCREATE, nil /* input */, true /* incrementNonce */, bailout, true)
}

// SysCreate is a special (system) contract creation methods for genesis constructors.
// Unlike the normal Create & Create2, it doesn't increment caller's nonce.
func (evm *EVM) SysCreate(caller ContractRef, code []byte, gas uint64, endowment *uint256.Int, contractAddr libcommon.Address) (ret []byte, leftOverGas uint64, err error) {
	ret, _, leftOverGas, err = evm.create(caller, &codeAndHash{code: code}, gas, endowment, contractAddr, CREATE, nil /* input */, false /* incrementNonce */, false, evm.chainRules.IsPrague)
	return
}

// ChainConfig returns the environment's chain configuration
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
func (evm *EVM) IntraBlockState() evmtypes.IntraBlockState {
	return evm.intraBlockState
}

// parseContainer tries to parse an EOF container if the Cancun fork is active. It expects the code to already be validated.
func (evm *EVM) parseContainer(b []byte) *EOFContainer {
	if evm.chainRules.IsOsaka && hasEOFMagic(b) {
		if c, err := UnmarshalEOF(b, 0, 1, evm.interpreter.Config().JumpTableEOF, true); err != nil {
			panic(fmt.Sprintf("error validating container: %v", err))
		} else {
			return c
		}
	}
	return nil
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

func (evm *EVM) captureBegin(depth int, typ OpCode, from libcommon.Address, to libcommon.Address, precompile bool, input []byte, startGas uint64, value *uint256.Int, code []byte) {
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
