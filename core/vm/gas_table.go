// Copyright 2017 The go-ethereum Authors
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
	"errors"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/common/math"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/params"
)

// memoryGasCost calculates the quadratic gas for memory expansion. It does so
// only for the memory region that is expanded, not the total memory.
func memoryGasCost(mem *Memory, newMemSize uint64) (uint64, error) {
	if newMemSize == 0 {
		return 0, nil
	}
	// The maximum that will fit in a uint64 is max_word_count - 1. Anything above
	// that will result in an overflow. Additionally, a newMemSize which results in
	// a newMemSizeWords larger than 0xFFFFFFFF will cause the square operation to
	// overflow. The constant 0x1FFFFFFFE0 is the highest number that can be used
	// without overflowing the gas calculation.
	if newMemSize > 0x1FFFFFFFE0 {
		return 0, ErrGasUintOverflow
	}
	newMemSizeWords := ToWordSize(newMemSize)
	newMemSize = newMemSizeWords * 32

	if newMemSize > uint64(mem.Len()) {
		square := newMemSizeWords * newMemSizeWords
		linCoef := newMemSizeWords * params.MemoryGas
		quadCoef := square / params.QuadCoeffDiv
		newTotalFee := linCoef + quadCoef

		fee := newTotalFee - mem.lastGasCost
		mem.lastGasCost = newTotalFee

		return fee, nil
	}
	return 0, nil
}

// memoryCopierGas creates the gas functions for the following opcodes, and takes
// the stack position of the operand which determines the size of the data to copy
// as argument:
// CALLDATACOPY (stack position 2)
// CODECOPY (stack position 2)
// EXTCODECOPY (stack poition 3)
// RETURNDATACOPY (stack position 2)
func memoryCopierGas(stackpos int) gasFunc {
	return func(_ VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
		// Gas for expanding the memory
		gas, err := memoryGasCost(mem, memorySize)
		if err != nil {
			return 0, err
		}
		// And gas for copying data, charged per word at param.CopyGas
		words, overflow := stack.Back(stackpos).Uint64WithOverflow()
		if overflow {
			return 0, ErrGasUintOverflow
		}

		if words, overflow = math.SafeMul(ToWordSize(words), params.CopyGas); overflow {
			return 0, ErrGasUintOverflow
		}

		if gas, overflow = math.SafeAdd(gas, words); overflow {
			return 0, ErrGasUintOverflow
		}
		return gas, nil
	}
}

var (
	gasCallDataCopy   = memoryCopierGas(2)
	gasCodeCopy       = memoryCopierGas(2)
	gasExtCodeCopy    = memoryCopierGas(3)
	gasReturnDataCopy = memoryCopierGas(2)
)

func gasSStore(evm VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	value, x := stack.Back(1), stack.Back(0)
	key := libcommon.Hash(x.Bytes32())
	var current uint256.Int
	evm.IntraBlockState().GetState(contract.Address(), &key, &current)
	// The legacy gas metering only takes into consideration the current state
	// Legacy rules should be applied if we are in Petersburg (removal of EIP-1283)
	// OR Constantinople is not active
	if evm.ChainRules().IsPetersburg || !evm.ChainRules().IsConstantinople {
		// This checks for 3 scenario's and calculates gas accordingly:
		//
		// 1. From a zero-value address to a non-zero value         (NEW VALUE)
		// 2. From a non-zero value address to a zero-value address (DELETE)
		// 3. From a non-zero to a non-zero                         (CHANGE)
		switch {
		case current.IsZero() && !value.IsZero(): // 0 => non 0
			return params.SstoreSetGas, nil
		case !current.IsZero() && value.IsZero(): // non 0 => 0
			evm.IntraBlockState().AddRefund(params.SstoreRefundGas)
			return params.SstoreClearGas, nil
		default: // non 0 => non 0 (or 0 => 0)
			return params.SstoreResetGas, nil
		}
	}
	// The new gas metering is based on net gas costs (EIP-1283):
	//
	// 1. If current value equals new value (this is a no-op), 200 gas is deducted.
	// 2. If current value does not equal new value
	//   2.1. If original value equals current value (this storage slot has not been changed by the current execution context)
	//     2.1.1. If original value is 0, 20000 gas is deducted.
	// 	   2.1.2. Otherwise, 5000 gas is deducted. If new value is 0, add 15000 gas to refund counter.
	// 	2.2. If original value does not equal current value (this storage slot is dirty), 200 gas is deducted. Apply both of the following clauses.
	// 	  2.2.1. If original value is not 0
	//       2.2.1.1. If current value is 0 (also means that new value is not 0), remove 15000 gas from refund counter. We can prove that refund counter will never go below 0.
	//       2.2.1.2. If new value is 0 (also means that current value is not 0), add 15000 gas to refund counter.
	// 	  2.2.2. If original value equals new value (this storage slot is reset)
	//       2.2.2.1. If original value is 0, add 19800 gas to refund counter.
	// 	     2.2.2.2. Otherwise, add 4800 gas to refund counter.
	if current.Eq(value) { // noop (1)
		return params.NetSstoreNoopGas, nil
	}
	var original uint256.Int
	evm.IntraBlockState().GetCommittedState(contract.Address(), &key, &original)
	if original == current {
		if original.IsZero() { // create slot (2.1.1)
			return params.NetSstoreInitGas, nil
		}
		if value.IsZero() { // delete slot (2.1.2b)
			evm.IntraBlockState().AddRefund(params.NetSstoreClearRefund)
		}
		return params.NetSstoreCleanGas, nil // write existing slot (2.1.2)
	}
	if !original.IsZero() {
		if current.IsZero() { // recreate slot (2.2.1.1)
			evm.IntraBlockState().SubRefund(params.NetSstoreClearRefund)
		} else if value.IsZero() { // delete slot (2.2.1.2)
			evm.IntraBlockState().AddRefund(params.NetSstoreClearRefund)
		}
	}
	if original.Eq(value) {
		if original.IsZero() { // reset to original inexistent slot (2.2.2.1)
			evm.IntraBlockState().AddRefund(params.NetSstoreResetClearRefund)
		} else { // reset to original existing slot (2.2.2.2)
			evm.IntraBlockState().AddRefund(params.NetSstoreResetRefund)
		}
	}

	return params.NetSstoreDirtyGas, nil
}

//  0. If *gasleft* is less than or equal to 2300, fail the current call.
//  1. If current value equals new value (this is a no-op), SLOAD_GAS is deducted.
//  2. If current value does not equal new value:
//     2.1. If original value equals current value (this storage slot has not been changed by the current execution context):
//     2.1.1. If original value is 0, SSTORE_SET_GAS (20K) gas is deducted.
//     2.1.2. Otherwise, SSTORE_RESET_GAS gas is deducted. If new value is 0, add SSTORE_CLEARS_SCHEDULE to refund counter.
//     2.2. If original value does not equal current value (this storage slot is dirty), SLOAD_GAS gas is deducted. Apply both of the following clauses:
//     2.2.1. If original value is not 0:
//     2.2.1.1. If current value is 0 (also means that new value is not 0), subtract SSTORE_CLEARS_SCHEDULE gas from refund counter.
//     2.2.1.2. If new value is 0 (also means that current value is not 0), add SSTORE_CLEARS_SCHEDULE gas to refund counter.
//     2.2.2. If original value equals new value (this storage slot is reset):
//     2.2.2.1. If original value is 0, add SSTORE_SET_GAS - SLOAD_GAS to refund counter.
//     2.2.2.2. Otherwise, add SSTORE_RESET_GAS - SLOAD_GAS gas to refund counter.
func gasSStoreEIP2200(evm VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	// If we fail the minimum gas availability invariant, fail (0)
	if contract.Gas <= params.SstoreSentryGasEIP2200 {
		return 0, errors.New("not enough gas for reentrancy sentry")
	}
	// Gas sentry honoured, do the actual gas calculation based on the stored value
	value, x := stack.Back(1), stack.Back(0)
	key := libcommon.Hash(x.Bytes32())
	var current uint256.Int
	evm.IntraBlockState().GetState(contract.Address(), &key, &current)

	if current.Eq(value) { // noop (1)
		return params.SloadGasEIP2200, nil
	}

	var original uint256.Int
	evm.IntraBlockState().GetCommittedState(contract.Address(), &key, &original)
	if original == current {
		if original.IsZero() { // create slot (2.1.1)
			return params.SstoreSetGasEIP2200, nil
		}
		if value.IsZero() { // delete slot (2.1.2b)
			evm.IntraBlockState().AddRefund(params.SstoreClearsScheduleRefundEIP2200)
		}
		return params.SstoreResetGasEIP2200, nil // write existing slot (2.1.2)
	}
	if !original.IsZero() {
		if current.IsZero() { // recreate slot (2.2.1.1)
			evm.IntraBlockState().SubRefund(params.SstoreClearsScheduleRefundEIP2200)
		} else if value.IsZero() { // delete slot (2.2.1.2)
			evm.IntraBlockState().AddRefund(params.SstoreClearsScheduleRefundEIP2200)
		}
	}
	if original.Eq(value) {
		if original.IsZero() { // reset to original inexistent slot (2.2.2.1)
			evm.IntraBlockState().AddRefund(params.SstoreSetGasEIP2200 - params.SloadGasEIP2200)
		} else { // reset to original existing slot (2.2.2.2)
			evm.IntraBlockState().AddRefund(params.SstoreResetGasEIP2200 - params.SloadGasEIP2200)
		}
	}
	return params.SloadGasEIP2200, nil // dirty update (2.2)
}

func makeGasLog(n uint64) gasFunc {
	return func(_ VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
		requestedSize, overflow := stack.Back(1).Uint64WithOverflow()
		if overflow {
			return 0, ErrGasUintOverflow
		}

		gas, err := memoryGasCost(mem, memorySize)
		if err != nil {
			return 0, err
		}

		if gas, overflow = math.SafeAdd(gas, params.LogGas); overflow {
			return 0, ErrGasUintOverflow
		}
		if gas, overflow = math.SafeAdd(gas, n*params.LogTopicGas); overflow {
			return 0, ErrGasUintOverflow
		}

		var memorySizeGas uint64
		if memorySizeGas, overflow = math.SafeMul(requestedSize, params.LogDataGas); overflow {
			return 0, ErrGasUintOverflow
		}
		if gas, overflow = math.SafeAdd(gas, memorySizeGas); overflow {
			return 0, ErrGasUintOverflow
		}
		return gas, nil
	}
}

func gasKeccak256(_ VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	gas, err := memoryGasCost(mem, memorySize)
	if err != nil {
		return 0, err
	}
	wordGas, overflow := stack.Back(1).Uint64WithOverflow()
	if overflow {
		return 0, ErrGasUintOverflow
	}
	if wordGas, overflow = math.SafeMul(ToWordSize(wordGas), params.Keccak256WordGas); overflow {
		return 0, ErrGasUintOverflow
	}
	if gas, overflow = math.SafeAdd(gas, wordGas); overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

// pureMemoryGascost is used by several operations, which aside from their
// static cost have a dynamic cost which is solely based on the memory
// expansion
func pureMemoryGascost(_ VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	return memoryGasCost(mem, memorySize)
}

var (
	gasReturn  = pureMemoryGascost
	gasRevert  = pureMemoryGascost
	gasMLoad   = pureMemoryGascost
	gasMStore8 = pureMemoryGascost
	gasMStore  = pureMemoryGascost
	gasCreate  = pureMemoryGascost
)

func gasCreate2(_ VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	gas, err := memoryGasCost(mem, memorySize)
	if err != nil {
		return 0, err
	}
	len, overflow := stack.Back(2).Uint64WithOverflow()
	if overflow {
		return 0, ErrGasUintOverflow
	}
	numWords := ToWordSize(len)
	wordGas, overflow := math.SafeMul(numWords, params.Keccak256WordGas)
	if overflow {
		return 0, ErrGasUintOverflow
	}
	gas, overflow = math.SafeAdd(gas, wordGas)
	if overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

func gasCreateEip3860(_ VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	gas, err := memoryGasCost(mem, memorySize)
	if err != nil {
		return 0, err
	}
	len, overflow := stack.Back(2).Uint64WithOverflow()
	if overflow || len > params.MaxInitCodeSize {
		return 0, ErrGasUintOverflow
	}
	numWords := ToWordSize(len)
	// Since size <= params.MaxInitCodeSize, this multiplication cannot overflow
	wordGas := params.InitCodeWordGas * numWords
	gas, overflow = math.SafeAdd(gas, wordGas)
	if overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

func gasCreate2Eip3860(_ VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	gas, err := memoryGasCost(mem, memorySize)
	if err != nil {
		return 0, err
	}
	len, overflow := stack.Back(2).Uint64WithOverflow()
	if overflow || len > params.MaxInitCodeSize {
		return 0, ErrGasUintOverflow
	}
	numWords := ToWordSize(len)
	// Since size <= params.MaxInitCodeSize, this multiplication cannot overflow
	wordGas := (params.InitCodeWordGas + params.Keccak256WordGas) * numWords
	gas, overflow = math.SafeAdd(gas, wordGas)
	if overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

func gasExpFrontier(_ VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	expByteLen := uint64((stack.Data[stack.Len()-2].BitLen() + 7) / 8)

	var (
		gas      = expByteLen * params.ExpByteFrontier // no overflow check required. Max is 256 * ExpByte gas
		overflow bool
	)
	if gas, overflow = math.SafeAdd(gas, params.ExpGas); overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

func gasExpEIP160(_ VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	expByteLen := uint64((stack.Data[stack.Len()-2].BitLen() + 7) / 8)

	var (
		gas      = expByteLen * params.ExpByteEIP160 // no overflow check required. Max is 256 * ExpByte gas
		overflow bool
	)
	if gas, overflow = math.SafeAdd(gas, params.ExpGas); overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

func gasCall(evm VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	var (
		gas            uint64
		transfersValue = !stack.Back(2).IsZero()
		address        = libcommon.Address(stack.Back(1).Bytes20())
	)
	if evm.ChainRules().IsSpuriousDragon {
		if transfersValue && evm.IntraBlockState().Empty(address) {
			gas += params.CallNewAccountGas
		}
	} else if !evm.IntraBlockState().Exist(address) {
		gas += params.CallNewAccountGas
	}
	if transfersValue {
		gas += params.CallValueTransferGas
	}
	memoryGas, err := memoryGasCost(mem, memorySize)
	if err != nil {
		return 0, err
	}
	var overflow bool
	if gas, overflow = math.SafeAdd(gas, memoryGas); overflow {
		return 0, ErrGasUintOverflow
	}

	var callGasTemp uint64
	callGasTemp, err = callGas(evm.ChainRules().IsTangerineWhistle, contract.Gas, gas, stack.Back(0))
	evm.SetCallGasTemp(callGasTemp)

	if err != nil {
		return 0, err
	}
	if gas, overflow = math.SafeAdd(gas, callGasTemp); overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

func gasCallCode(evm VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	memoryGas, err := memoryGasCost(mem, memorySize)
	if err != nil {
		return 0, err
	}
	var (
		gas      uint64
		overflow bool
	)
	if !stack.Back(2).IsZero() {
		gas += params.CallValueTransferGas
	}
	if gas, overflow = math.SafeAdd(gas, memoryGas); overflow {
		return 0, ErrGasUintOverflow
	}
	var callGasTemp uint64
	callGasTemp, err = callGas(evm.ChainRules().IsTangerineWhistle, contract.Gas, gas, stack.Back(0))
	evm.SetCallGasTemp(callGasTemp)

	if err != nil {
		return 0, err
	}
	if gas, overflow = math.SafeAdd(gas, callGasTemp); overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

func gasDelegateCall(evm VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	gas, err := memoryGasCost(mem, memorySize)
	if err != nil {
		return 0, err
	}

	var callGasTemp uint64
	callGasTemp, err = callGas(evm.ChainRules().IsTangerineWhistle, contract.Gas, gas, stack.Back(0))
	evm.SetCallGasTemp(callGasTemp)

	if err != nil {
		return 0, err
	}
	var overflow bool
	if gas, overflow = math.SafeAdd(gas, callGasTemp); overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

func gasStaticCall(evm VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	gas, err := memoryGasCost(mem, memorySize)
	if err != nil {
		return 0, err
	}

	var callGasTemp uint64
	callGasTemp, err = callGas(evm.ChainRules().IsTangerineWhistle, contract.Gas, gas, stack.Back(0))
	evm.SetCallGasTemp(callGasTemp)

	if err != nil {
		return 0, err
	}
	var overflow bool
	if gas, overflow = math.SafeAdd(gas, callGasTemp); overflow {
		return 0, ErrGasUintOverflow
	}
	return gas, nil
}

func gasSelfdestruct(evm VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	var gas uint64
	// TangerineWhistle (EIP150) gas reprice fork:
	if evm.ChainRules().IsTangerineWhistle {
		gas = params.SelfdestructGasEIP150
		var address = libcommon.Address(stack.Back(0).Bytes20())

		if evm.ChainRules().IsSpuriousDragon {
			// if empty and transfers value
			if evm.IntraBlockState().Empty(address) && !evm.IntraBlockState().GetBalance(contract.Address()).IsZero() {
				gas += params.CreateBySelfdestructGas
			}
		} else if !evm.IntraBlockState().Exist(address) {
			gas += params.CreateBySelfdestructGas
		}
	}

	if !evm.IntraBlockState().HasSelfdestructed(contract.Address()) {
		evm.IntraBlockState().AddRefund(params.SelfdestructRefundGas)
	}
	return gas, nil
}
