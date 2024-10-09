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
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/common/u256"
	"github.com/ledgerwatch/erigon/core/vm/evmtypes"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/params"
)

// [zkevm] contains the list of zkevm precompiles
func (evm *EVM) precompile_zkevm(addr libcommon.Address, retSize int) (PrecompiledContract_zkEvm, bool) {
	var precompiles map[libcommon.Address]PrecompiledContract_zkEvm
	switch {
	case evm.chainRules.IsForkID8Elderberry:
		precompiles = PrecompiledContractsForkID8Elderberry
	case evm.chainRules.IsForkID7Etrog:
		precompiles = PrecompiledContractForkID7Etrog
	default:
		precompiles = PrecompiledContractsForkID5Dragonfruit
	}
	p, ok := precompiles[addr]

	if evm.zkConfig != nil && evm.zkConfig.CounterCollector != nil && ok {
		p.SetCounterCollector(evm.zkConfig.CounterCollector)
		p.SetOutputLength(retSize)
	}

	return p, ok
}

// NewEVM returns a new EVM. The returned EVM is not thread safe and should
// only ever be used *once*.
func NewZkEVM(blockCtx evmtypes.BlockContext, txCtx evmtypes.TxContext, state evmtypes.IntraBlockState, chainConfig *chain.Config, zkVmConfig ZkConfig) *EVM {
	if chainConfig.Rules(blockCtx.BlockNumber, blockCtx.Time).IsNormalcy {
		return NewEVM(blockCtx, txCtx, state, chainConfig, zkVmConfig.Config)
	}

	evm :=
		&EVM{
			Context:         blockCtx,
			TxContext:       txCtx,
			intraBlockState: state,
			config:          zkVmConfig.Config,
			chainConfig:     chainConfig,
			chainRules:      chainConfig.Rules(blockCtx.BlockNumber, blockCtx.Time),
			zkConfig:        &zkVmConfig,
		}

	// [zkevm] change
	evm.interpreter = NewZKEVMInterpreter(evm, zkVmConfig)

	return evm
}

func (evm *EVM) Deploy(caller ContractRef, code []byte, gas uint64, endowment *uint256.Int, intrinsicGas uint64) (ret []byte, contractAddr libcommon.Address, leftOverGas uint64, err error) {
	if evm.ChainRules().IsNormalcy {
		return evm.Create(caller, code, gas, endowment, intrinsicGas)
	}

	contractAddr = crypto.CreateAddress(caller.Address(), evm.intraBlockState.GetNonce(caller.Address()))
	return evm.createZkEvm(caller, &codeAndHash{code: code}, gas, endowment, contractAddr, CREATE, true /* incrementNonce */, intrinsicGas)
}

// create creates a new contract using code as deployment code.
func (evm *EVM) createZkEvm(caller ContractRef, codeAndHash *codeAndHash, gas uint64, value *uint256.Int, address libcommon.Address, typ OpCode, incrementNonce bool, intrinsicGas uint64) ([]byte, libcommon.Address, uint64, error) {
	var ret []byte
	var err error
	var gasConsumption uint64
	depth := evm.interpreter.Depth()

	if evm.config.Debug {
		if depth == 0 {
			evm.config.Tracer.CaptureStart(evm, caller.Address(), address, false /* precompile */, true /* create */, codeAndHash.code, gas+intrinsicGas, value, nil)
			defer func() {
				evm.config.Tracer.CaptureEnd(ret, gasConsumption, err)
			}()
		} else {
			evm.config.Tracer.CaptureEnter(typ, caller.Address(), address, false /* precompile */, true /* create */, codeAndHash.code, gas, value, nil)
			defer func() {
				evm.config.Tracer.CaptureExit(ret, gasConsumption, err)
			}()
		}
	}

	// Depth check execution. Fail if we're trying to execute above the
	// limit.
	if depth > int(params.CallCreateDepth) {
		err = ErrDepth
		return nil, libcommon.Address{}, gas, err
	}
	if !evm.Context.CanTransfer(evm.intraBlockState, caller.Address(), value) {
		err = ErrInsufficientBalance
		return nil, libcommon.Address{}, gas, err
	}
	if incrementNonce {
		nonce := evm.intraBlockState.GetNonce(caller.Address())
		if nonce+1 < nonce {
			err = ErrNonceUintOverflow
			return nil, libcommon.Address{}, gas, err
		}
		evm.intraBlockState.SetNonce(caller.Address(), nonce+1)
	}

	if evm.chainConfig.IsForkID12Banana(evm.Context.BlockNumber) {
		// add the address to the access list before taking a snapshot so that it will stay
		// in warm storage even if the create call fails
		if evm.chainRules.IsBerlin {
			evm.intraBlockState.AddAddressToAccessList(address)
		}
	}

	// Ensure there's no existing contract already at the designated address
	contractHash := evm.intraBlockState.GetCodeHash(address)
	if evm.intraBlockState.GetNonce(address) != 0 || (contractHash != (libcommon.Hash{}) && contractHash != emptyCodeHash) {
		err = ErrContractAddressCollision
		return nil, libcommon.Address{}, 0, err
	}

	// Create a new account on the state
	snapshot := evm.intraBlockState.Snapshot()

	if !evm.chainConfig.IsForkID12Banana(evm.Context.BlockNumber) {
		// if we aren't at fork 12 then we haven't already added this address to the list so we
		// need to add it now.  If the create call fails it will be removed from warm storage
		if evm.chainRules.IsBerlin {
			evm.intraBlockState.AddAddressToAccessList(address)
		}
	}

	evm.intraBlockState.CreateAccount(address, true)
	if evm.chainRules.IsSpuriousDragon {
		evm.intraBlockState.SetNonce(address, 1)
	}
	evm.Context.Transfer(evm.intraBlockState, caller.Address(), address, value, false /* bailout */)

	// Initialise a new contract and set the code that is to be used by the EVM.
	// The contract is a scoped environment for this execution context only.
	contract := NewContract(caller, address, value, gas, evm.config.SkipAnalysis)
	contract.SetCodeOptionalHash(&address, codeAndHash)

	if typ == CREATE {
		contract.IsCreate = true
	} else if typ == CREATE2 {
		contract.IsCreate2 = true
	}

	if evm.config.NoRecursion && depth > 0 {
		return nil, address, gas, nil
	}

	ret, err = runZk(evm, contract, nil, false)

	// EIP-170: Contract code size limit
	if err == nil && evm.chainRules.IsSpuriousDragon && len(ret) > params.MaxCodeSize {
		// Gnosis Chain prior to Shanghai didn't have EIP-170 enabled,
		// but EIP-3860 (part of Shanghai) requires EIP-170.
		if !evm.chainRules.IsAura || evm.config.HasEip3860(evm.chainRules) {
			err = ErrMaxCodeSizeExceeded
		}
	}

	// Reject code starting with 0xEF if EIP-3541 is enabled.
	if err == nil && len(ret) >= 1 && ret[0] == 0xEF {
		err = ErrInvalidCode
	}
	// if the contract creation ran successfully and no errors were returned
	// calculate the gas required to store the code. If the code could not
	// be stored due to not enough gas set an error and let it be handled
	// by the error checking condition below.
	if err == nil {
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
	if err != nil && (evm.chainRules.IsHomestead || err != ErrCodeStoreOutOfGas) {
		evm.intraBlockState.RevertToSnapshot(snapshot)
		if !IsErrTypeRevert(err) {
			contract.UseGas(contract.Gas)
		}
	}

	// calculate gasConsumption for deferred captures
	gasConsumption = gas - contract.Gas

	return ret, address, contract.Gas, err
}

func (evm *EVM) Call_zkEvm(caller ContractRef, addr libcommon.Address, input []byte, gas uint64, value *uint256.Int, bailout bool, intrinsicGas uint64, retSize int) (ret []byte, leftOverGas uint64, err error) {
	return evm.call_zkevm(CALL, caller, addr, input, gas, value, bailout, intrinsicGas, retSize)
}

// CallCode executes the contract associated with the addr with the given input
// as parameters. It also handles any necessary value transfer required and takes
// the necessary steps to create accounts and reverses the state in case of an
// execution error or failed value transfer.
//
// CallCode differs from Call in the sense that it executes the given address'
// code with the caller as context.
func (evm *EVM) CallCode_zkEvm(caller ContractRef, addr libcommon.Address, input []byte, gas uint64, value *uint256.Int, retSize int) (ret []byte, leftOverGas uint64, err error) {
	return evm.call_zkevm(CALLCODE, caller, addr, input, gas, value, false, 0 /* intrinsicGas is zero here */, retSize)
}

// DelegateCall executes the contract associated with the addr with the given input
// as parameters. It reverses the state in case of an execution error.
//
// DelegateCall differs from CallCode in the sense that it executes the given address'
// code with the caller as context and the caller is set to the caller of the caller.
func (evm *EVM) DelegateCall_zkEvm(caller ContractRef, addr libcommon.Address, input []byte, gas uint64, retSize int) (ret []byte, leftOverGas uint64, err error) {
	return evm.call_zkevm(DELEGATECALL, caller, addr, input, gas, nil, false, 0 /* intrinsicGas is zero here */, retSize)
}

// StaticCall executes the contract associated with the addr with the given input
// as parameters while disallowing any modifications to the state during the call.
// Opcodes that attempt to perform such modifications will result in exceptions
// instead of performing the modifications.
func (evm *EVM) StaticCall_zkEvm(caller ContractRef, addr libcommon.Address, input []byte, gas uint64, retSize int) (ret []byte, leftOverGas uint64, err error) {
	return evm.call_zkevm(STATICCALL, caller, addr, input, gas, new(uint256.Int), false, 0 /* intrinsicGas is zero here */, retSize)
}

func (evm *EVM) call_zkevm(typ OpCode, caller ContractRef, addr libcommon.Address, input []byte, gas uint64, value *uint256.Int, bailout bool, intrinsicGas uint64, retSize int) (ret []byte, leftOverGas uint64, err error) {
	if evm.ChainRules().IsNormalcy {
		return evm.call(typ, caller, addr, input, gas, value, bailout, intrinsicGas)
	}

	depth := evm.interpreter.Depth()

	if evm.config.NoRecursion && depth > 0 {
		return nil, gas, nil
	}
	// Fail if we're trying to execute above the call depth limit
	if depth > int(params.CallCreateDepth) {
		return nil, gas, ErrDepth
	}
	if typ == CALL || typ == CALLCODE {
		// Fail if we're trying to transfer more than the available balance
		if !value.IsZero() && !evm.Context.CanTransfer(evm.intraBlockState, caller.Address(), value) {
			if !bailout {
				return nil, gas, ErrInsufficientBalance
			}
		}
	}
	p, isPrecompile := evm.precompile_zkevm(addr, retSize)
	var code []byte
	if !isPrecompile {
		code = evm.intraBlockState.GetCode(addr)
	}

	snapshot := evm.intraBlockState.Snapshot()

	if typ == CALL {
		if !evm.intraBlockState.Exist(addr) {
			if !isPrecompile && evm.chainRules.IsSpuriousDragon && value.IsZero() {
				if evm.config.Debug {
					v := value
					if typ == STATICCALL {
						v = nil
					}
					// Calling a non existing account, don't do anything, but ping the tracer
					if depth == 0 {
						evm.config.Tracer.CaptureStart(evm, caller.Address(), addr, isPrecompile, false /* create */, input, gas+intrinsicGas, v, code)
						evm.config.Tracer.CaptureEnd(ret, 0, nil)
					} else {
						evm.config.Tracer.CaptureEnter(typ, caller.Address(), addr, isPrecompile, false /* create */, input, gas, v, code)
						evm.config.Tracer.CaptureExit(ret, 0, nil)
					}
				}
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
		evm.intraBlockState.AddBalance(addr, u256.Num0)
	}
	if evm.config.Debug {
		v := value
		if typ == STATICCALL {
			v = nil
		}
		if depth == 0 {
			evm.config.Tracer.CaptureStart(evm, caller.Address(), addr, isPrecompile, false /* create */, input, gas+intrinsicGas, v, code)
			defer func(startGas uint64) { // Lazy evaluation of the parameters
				evm.config.Tracer.CaptureEnd(ret, startGas-gas, err)
			}(gas)
		} else {
			evm.config.Tracer.CaptureEnter(typ, caller.Address(), addr, isPrecompile, false /* create */, input, gas, v, code)
			defer func(startGas uint64) { // Lazy evaluation of the parameters
				evm.config.Tracer.CaptureExit(ret, startGas-gas, err)
			}(gas)
		}
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
		if typ == CALLCODE {
			contract = NewContract(caller, caller.Address(), value, gas, evm.config.SkipAnalysis)
		} else if typ == DELEGATECALL {
			contract = NewContract(caller, caller.Address(), value, gas, evm.config.SkipAnalysis).AsDelegate()
		} else {
			contract = NewContract(caller, addrCopy, value, gas, evm.config.SkipAnalysis)
		}
		contract.SetCallCode(&addrCopy, codeHash, code)
		readOnly := false
		if typ == STATICCALL {
			readOnly = true
		}
		ret, err = runZk(evm, contract, input, readOnly)
		gas = contract.Gas
	}
	// When an error was returned by the EVM or when setting the creation code
	// above we revert to the snapshot and consume any gas remaining. Additionally
	// when we're in Homestead this also counts for code storage gas errors.
	if err != nil || evm.config.RestoreState {
		evm.intraBlockState.RevertToSnapshot(snapshot)
		if !IsErrTypeRevert(err) {
			gas = 0
		}
		// TODO: consider clearing up unused snapshots:
		//} else {
		//	evm.StateDB.DiscardSnapshot(snapshot)
	}
	return ret, gas, err
}
