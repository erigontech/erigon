// Copyright 2014 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package vm

import (
	"sync/atomic"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

// emptyCodeHash is used by create to ensure deployment is disallowed to already
// deployed contract addresses (relevant after the account abstraction).
var emptyCodeHash = crypto.Keccak256Hash(nil)

func (evm *EVM) precompile(addr common.Address) (PrecompiledContract, bool) {
	var precompiles map[common.Address]PrecompiledContract
	switch {
	case evm.chainRules.IsMoran:
		precompiles = PrecompiledContractsIsMoran
	case evm.chainRules.IsNano:
		precompiles = PrecompiledContractsNano
	case evm.chainRules.IsBerlin:
		precompiles = PrecompiledContractsBerlin
	case evm.chainRules.IsIstanbul:
		if evm.chainRules.IsParlia {
			precompiles = PrecompiledContractsIstanbulForBSC
		} else {
			precompiles = PrecompiledContractsIstanbul
		}
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
	context   evmtypes.BlockContext
	txContext evmtypes.TxContext
	// IntraBlockState gives access to the underlying state
	intraBlockState evmtypes.IntraBlockState
	// Depth is the current call stack
	depth int

	// chainConfig contains information about the current chain
	chainConfig *params.ChainConfig
	// chain rules contains the chain rules for the current epoch
	chainRules *params.Rules
	// virtual machine configuration options used to initialise the
	// evm.
	config Config
	// global (to this context) ethereum virtual machine
	// used throughout the execution of the tx.
	interpreter Interpreter
	// abort is used to abort the EVM calling operations
	// NOTE: must be set atomically
	abort int32
	// callGasTemp holds the gas available for the current call. This is needed because the
	// available gas is calculated in gasCall* according to the 63/64 rule and later
	// applied in opCall*.
	callGasTemp uint64
}

// NewEVM returns a new EVM. The returned EVM is not thread safe and should
// only ever be used *once*.
func NewEVM(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, state evmtypes.IntraBlockState, chainConfig *params.ChainConfig, vmConfig Config) *EVM {
	evm := &EVM{
		context:         blockCtx,
		txContext:       txCtx,
		intraBlockState: state,
		config:          vmConfig,
		chainConfig:     chainConfig,
		chainRules:      chainConfig.Rules(blockCtx.BlockNumber, blockCtx.Time),
	}

	evm.interpreter = NewEVMInterpreter(evm, vmConfig)

	return evm
}

// Reset resets the EVM with a new transaction context.Reset
// This is not threadsafe and should only be done very cautiously.
func (evm *EVM) Reset(txCtx evmtypes.TxContext, ibs evmtypes.IntraBlockState) {
	evm.txContext = txCtx
	evm.intraBlockState = ibs

	// ensure the evm is reset to be used again
	atomic.StoreInt32(&evm.abort, 0)
}

func (evm *EVM) ResetBetweenBlocks(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, ibs evmtypes.IntraBlockState, vmConfig Config, chainRules *params.Rules) {
	evm.context = blockCtx
	evm.txContext = txCtx
	evm.intraBlockState = ibs
	evm.config = vmConfig
	evm.chainRules = chainRules

	evm.interpreter = NewEVMInterpreter(evm, vmConfig)

	// ensure the evm is reset to be used again
	atomic.StoreInt32(&evm.abort, 0)
}

// Cancel cancels any running EVM operation. This may be called concurrently and
// it's safe to be called multiple times.
func (evm *EVM) Cancel() {
	atomic.StoreInt32(&evm.abort, 1)
}

// Cancelled returns true if Cancel has been called
func (evm *EVM) Cancelled() bool {
	return atomic.LoadInt32(&evm.abort) == 1
}

// Interpreter returns the current interpreter
func (evm *EVM) Interpreter() Interpreter {
	return evm.interpreter
}

func (evm *EVM) call(callType CallType, caller ContractRef, addr common.Address, input []byte, gas uint64, value *uint256.Int, bailout bool) (ret []byte, leftOverGas uint64, err error) {
	if evm.config.NoRecursion && evm.depth > 0 {
		return nil, gas, nil
	}
	// Fail if we're trying to execute above the call depth limit
	if evm.depth > int(params.CallCreateDepth) {
		return nil, gas, ErrDepth
	}
	if callType == CALLT || callType == CALLCODET {
		// Fail if we're trying to transfer more than the available balance
		if !value.IsZero() && !evm.context.CanTransfer(evm.intraBlockState, caller.Address(), value) {
			if !bailout {
				return nil, gas, ErrInsufficientBalance
			}
		}
	}
	p, isPrecompile := evm.precompile(addr)
	var code []byte
	if !isPrecompile {
		code = evm.intraBlockState.GetCode(addr)
	}
	// Capture the tracer start/end events in debug mode
	if evm.config.Debug {
		evm.config.Tracer.CaptureStart(evm, evm.depth, caller.Address(), addr, isPrecompile, false /* create */, callType, input, gas, value, code)
		defer func(startGas uint64, startTime time.Time) { // Lazy evaluation of the parameters
			evm.config.Tracer.CaptureEnd(evm.depth, ret, startGas, gas, time.Since(startTime), err)
		}(gas, time.Now())
	}

	snapshot := evm.intraBlockState.Snapshot()

	if callType == CALLT {
		if !evm.intraBlockState.Exist(addr) {
			if !isPrecompile && evm.chainRules.IsSpuriousDragon && value.IsZero() {
				return nil, gas, nil
			}
			evm.intraBlockState.CreateAccount(addr, false)
		}
		evm.context.Transfer(evm.intraBlockState, caller.Address(), addr, value, bailout)
	} else if callType == STATICCALLT {
		// We do an AddBalance of zero here, just in order to trigger a touch.
		// This doesn't matter on Mainnet, where all empties are gone at the time of Byzantium,
		// but is the correct thing to do and matters on other networks, in tests, and potential
		// future scenarios
		evm.intraBlockState.AddBalance(addr, u256.Num0)
	}

	// It is allowed to call precompiles, even via delegatecall
	if isPrecompile {
		ret, gas, err = RunPrecompiledContract(p, input, gas)
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
		codeHash := evm.intraBlockState.GetCodeHash(addrCopy)
		var contract *Contract
		if callType == CALLCODET {
			contract = NewContract(caller, AccountRef(caller.Address()), value, gas, evm.config.SkipAnalysis)
		} else if callType == DELEGATECALLT {
			contract = NewContract(caller, AccountRef(caller.Address()), value, gas, evm.config.SkipAnalysis).AsDelegate()
		} else {
			contract = NewContract(caller, AccountRef(addrCopy), value, gas, evm.config.SkipAnalysis)
		}
		contract.SetCallCode(&addrCopy, codeHash, code)
		readOnly := false
		if callType == STATICCALLT {
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
func (evm *EVM) Call(caller ContractRef, addr common.Address, input []byte, gas uint64, value *uint256.Int, bailout bool) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(CALLT, caller, addr, input, gas, value, bailout)
}

// CallCode executes the contract associated with the addr with the given input
// as parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
//
// CallCode differs from Call in the sense that it executes the given address'
// code with the caller as context.
func (evm *EVM) CallCode(caller ContractRef, addr common.Address, input []byte, gas uint64, value *uint256.Int) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(CALLCODET, caller, addr, input, gas, value, false)
}

// DelegateCall executes the contract associated with the addr with the given input
// as parameters. It reverses the state in case of an execution error.
//
// DelegateCall differs from CallCode in the sense that it executes the given address'
// code with the caller as context and the caller is set to the caller of the caller.
func (evm *EVM) DelegateCall(caller ContractRef, addr common.Address, input []byte, gas uint64) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(DELEGATECALLT, caller, addr, input, gas, nil, false)
}

// StaticCall executes the contract associated with the addr with the given input
// as parameters while disallowing any modifications to the state during the call.
// Opcodes that attempt to perform such modifications will result in exceptions
// instead of performing the modifications.
func (evm *EVM) StaticCall(caller ContractRef, addr common.Address, input []byte, gas uint64) (ret []byte, leftOverGas uint64, err error) {
	return evm.call(STATICCALLT, caller, addr, input, gas, new(uint256.Int), false)
}

type codeAndHash struct {
	code []byte
	hash common.Hash
}

func (c *codeAndHash) Hash() common.Hash {
	if c.hash == (common.Hash{}) {
		c.hash = crypto.Keccak256Hash(c.code)
	}
	return c.hash
}

// create creates a new contract using code as deployment code.
func (evm *EVM) create(caller ContractRef, codeAndHash *codeAndHash, gas uint64, value *uint256.Int, address common.Address, calltype CallType, incrementNonce bool) ([]byte, common.Address, uint64, error) {
	var ret []byte
	var err error
	// Depth check execution. Fail if we're trying to execute above the
	// limit.
	if evm.depth > int(params.CallCreateDepth) {
		return nil, common.Address{}, gas, ErrDepth
	}
	if !evm.context.CanTransfer(evm.intraBlockState, caller.Address(), value) {
		return nil, common.Address{}, gas, ErrInsufficientBalance
	}
	// Check whether the init code size has been exceeded.
	if evm.config.HasEip3860(evm.chainRules) && len(codeAndHash.code) > params.MaxInitCodeSize {
		return nil, address, gas, ErrMaxInitCodeSizeExceeded
	}
	if evm.config.Debug {
		evm.config.Tracer.CaptureStart(evm, evm.depth, caller.Address(), address, false /* precompile */, true /* create */, calltype, codeAndHash.code, gas, value, nil)
		defer func(startGas uint64, startTime time.Time) { // Lazy evaluation of the parameters
			evm.config.Tracer.CaptureEnd(evm.depth, ret, startGas, gas, time.Since(startTime), err)
		}(gas, time.Now())
	}
	if incrementNonce {
		nonce := evm.intraBlockState.GetNonce(caller.Address())
		if nonce+1 < nonce {
			return nil, common.Address{}, gas, ErrNonceUintOverflow
		}
		evm.intraBlockState.SetNonce(caller.Address(), nonce+1)
	}
	// We add this to the access list _before_ taking a snapshot. Even if the creation fails,
	// the access-list change should not be rolled back
	if evm.chainRules.IsBerlin {
		evm.intraBlockState.AddAddressToAccessList(address)
	}
	// Ensure there's no existing contract already at the designated address
	contractHash := evm.intraBlockState.GetCodeHash(address)
	if evm.intraBlockState.GetNonce(address) != 0 || (contractHash != (common.Hash{}) && contractHash != emptyCodeHash) {
		err = ErrContractAddressCollision
		return nil, common.Address{}, 0, err
	}
	// Create a new account on the state
	snapshot := evm.intraBlockState.Snapshot()
	evm.intraBlockState.CreateAccount(address, true)
	if evm.chainRules.IsSpuriousDragon {
		evm.intraBlockState.SetNonce(address, 1)
	}
	evm.context.Transfer(evm.intraBlockState, caller.Address(), address, value, false /* bailout */)

	// Initialise a new contract and set the code that is to be used by the EVM.
	// The contract is a scoped environment for this execution context only.
	contract := NewContract(caller, AccountRef(address), value, gas, evm.config.SkipAnalysis)
	contract.SetCodeOptionalHash(&address, codeAndHash)

	if evm.config.NoRecursion && evm.depth > 0 {
		return nil, address, gas, nil
	}

	ret, err = run(evm, contract, nil, false)

	// check whether the max code size has been exceeded
	maxCodeSizeExceeded := evm.chainRules.IsSpuriousDragon && len(ret) > params.MaxCodeSize && !evm.chainRules.IsAura

	// Reject code starting with 0xEF if EIP-3541 is enabled.
	if err == nil && !maxCodeSizeExceeded {
		if evm.chainRules.IsLondon && len(ret) >= 1 && ret[0] == 0xEF {
			err = ErrInvalidCode
		}
	}
	// if the contract creation ran successfully and no errors were returned
	// calculate the gas required to store the code. If the code could not
	// be stored due to not enough gas set an error and let it be handled
	// by the error checking condition below.
	if err == nil && !maxCodeSizeExceeded {
		createDataGas := uint64(len(ret)) * params.CreateDataGas
		if contract.UseGas(createDataGas) {
			evm.intraBlockState.SetCode(address, ret)
		} else if evm.chainRules.IsHomestead {
			err = ErrCodeStoreOutOfGas
		}
	}

	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in homestead this also counts for code storage gas errors.
	if maxCodeSizeExceeded || (err != nil && (evm.chainRules.IsHomestead || err != ErrCodeStoreOutOfGas)) {
		evm.intraBlockState.RevertToSnapshot(snapshot)
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas)
		}
	}
	gas = contract.Gas // For the CaptureEnd to work corrently with gasUsed
	// Assign err if contract code size exceeds the max while the err is still empty.
	if maxCodeSizeExceeded && err == nil {
		err = ErrMaxCodeSizeExceeded
	}

	return ret, address, contract.Gas, err

}

// Create creates a new contract using code as deployment code.
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (evm *EVM) Create(caller ContractRef, code []byte, gas uint64, endowment *uint256.Int) (ret []byte, contractAddr common.Address, leftOverGas uint64, err error) {
	contractAddr = crypto.CreateAddress(caller.Address(), evm.intraBlockState.GetNonce(caller.Address()))
	return evm.create(caller, &codeAndHash{code: code}, gas, endowment, contractAddr, CREATET, true /* incrementNonce */)
}

// Create2 creates a new contract using code as deployment code.
//
// The different between Create2 with Create is Create2 uses keccak256(0xff ++ msg.sender ++ salt ++ keccak256(init_code))[12:]
// instead of the usual sender-and-nonce-hash as the address where the contract is initialized at.
// DESCRIBED: docs/programmers_guide/guide.md#nonce
func (evm *EVM) Create2(caller ContractRef, code []byte, gas uint64, endowment *uint256.Int, salt *uint256.Int) (ret []byte, contractAddr common.Address, leftOverGas uint64, err error) {
	codeAndHash := &codeAndHash{code: code}
	contractAddr = crypto.CreateAddress2(caller.Address(), salt.Bytes32(), codeAndHash.Hash().Bytes())
	return evm.create(caller, codeAndHash, gas, endowment, contractAddr, CREATE2T, true /* incrementNonce */)
}

// SysCreate is a special (system) contract creation methods for genesis constructors.
// Unlike the normal Create & Create2, it doesn't increment caller's nonce.
func (evm *EVM) SysCreate(caller ContractRef, code []byte, gas uint64, endowment *uint256.Int, contractAddr common.Address) (ret []byte, leftOverGas uint64, err error) {
	ret, _, leftOverGas, err = evm.create(caller, &codeAndHash{code: code}, gas, endowment, contractAddr, CREATET, false /* incrementNonce */)
	return
}

// ChainConfig returns the environment's chain configuration
func (evm *EVM) Config() Config {
	return evm.config
}

// ChainConfig returns the environment's chain configuration
func (evm *EVM) ChainConfig() *params.ChainConfig {
	return evm.chainConfig
}

func (evm *EVM) ChainRules() *params.Rules {
	return evm.chainRules
}

func (evm *EVM) Context() evmtypes.BlockContext {
	return evm.context
}

func (evm *EVM) TxContext() evmtypes.TxContext {
	return evm.txContext
}

func (evm *EVM) IntraBlockState() evmtypes.IntraBlockState {
	return evm.intraBlockState
}
