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
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/params"
)

// [zkevm] contains the list of zkevm precompiles
func (evm *EVM) precompile(addr libcommon.Address) (PrecompiledContract, bool) {
	var precompiles map[libcommon.Address]PrecompiledContract
	switch {
	default:
		precompiles = PrecompiledContractsZKEVMDragonfruit
	}
	p, ok := precompiles[addr]
	return p, ok
}

// create creates a new contract using code as deployment code.
func (evm *EVM) createZkEvm(caller ContractRef, codeAndHash *codeAndHash, gas uint64, value *uint256.Int, address libcommon.Address, typ OpCode, incrementNonce bool) ([]byte, libcommon.Address, uint64, error) {
	var ret []byte
	var err error
	var gasConsumption uint64
	depth := evm.interpreter.Depth()

	if evm.config.Debug {
		if depth == 0 {
			evm.config.Tracer.CaptureStart(evm, caller.Address(), address, false /* precompile */, true /* create */, codeAndHash.code, gas, value, nil)
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
	if !evm.context.CanTransfer(evm.intraBlockState, caller.Address(), value) {
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

	// Ensure there's no existing contract already at the designated address
	contractHash := evm.intraBlockState.GetCodeHash(address)
	if evm.intraBlockState.GetNonce(address) != 0 || (contractHash != (libcommon.Hash{}) && contractHash != emptyCodeHash) {
		err = ErrContractAddressCollision
		return nil, libcommon.Address{}, 0, err
	}

	// Create a new account on the state
	snapshot := evm.intraBlockState.Snapshot()

	if evm.chainRules.IsBerlin {
		evm.intraBlockState.AddAddressToAccessList(address)
	}

	evm.intraBlockState.CreateAccount(address, true)
	if evm.chainRules.IsSpuriousDragon {
		evm.intraBlockState.SetNonce(address, 1)
	}
	evm.context.Transfer(evm.intraBlockState, caller.Address(), address, value, false /* bailout */)

	// Initialise a new contract and set the code that is to be used by the EVM.
	// The contract is a scoped environment for this execution context only.
	contract := NewContract(caller, AccountRef(address), value, gas, evm.config.SkipAnalysis)
	contract.SetCodeOptionalHash(&address, codeAndHash)

	if evm.config.NoRecursion && depth > 0 {
		return nil, address, gas, nil
	}

	ret, err = run(evm, contract, nil, false)

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
		if err != ErrExecutionReverted {
			contract.UseGas(contract.Gas)
		}
	}

	// calculate gasConsumption for deferred captures
	gasConsumption = gas - contract.Gas

	return ret, address, contract.Gas, err
}
