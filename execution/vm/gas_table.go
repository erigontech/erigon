// Copyright 2017 The go-ethereum Authors
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

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// memoryGasCost calculates the quadratic gas for memory expansion. It does so
// only for the memory region that is expanded, not the total memory.
func memoryGasCost(callContext *CallContext, newMemSize uint64) (uint64, error) {
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

	if newMemSize > uint64(callContext.Memory.Len()) {
		square := newMemSizeWords * newMemSizeWords
		linCoef := newMemSizeWords * params.MemoryGas
		quadCoef := square / params.QuadCoeffDiv
		newTotalFee := linCoef + quadCoef

		fee := newTotalFee - callContext.Memory.lastGasCost
		callContext.Memory.lastGasCost = newTotalFee

		return fee, nil
	}
	return 0, nil
}

// memoryCopierGas creates the gas functions for the following opcodes, and takes
// the stack position of the operand which determines the size of the data to copy
// as argument:
// CALLDATACOPY (stack position 2)
// CODECOPY (stack position 2)
// MCOPY (stack position 2)
// EXTCODECOPY (stack position 3)
// RETURNDATACOPY (stack position 2)
func memoryCopierGas(stackpos int) gasFunc {
	return func(_ *EVM, callContext *CallContext, scaopeGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
		// Gas for expanding the memory
		gas, err := memoryGasCost(callContext, memorySize)
		if err != nil {
			return mdgas.MdGas{}, err
		}
		// And gas for copying data, charged per word at param.CopyGas
		words, overflow := callContext.Stack.Back(stackpos).Uint64WithOverflow()
		if overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}

		if words, overflow = math.SafeMul(ToWordSize(words), params.CopyGas); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}

		if gas, overflow = math.SafeAdd(gas, words); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}
		return mdgas.MdGas{Regular: gas}, nil
	}
}

var (
	gasCallDataCopy   = memoryCopierGas(2)
	gasCodeCopy       = memoryCopierGas(2)
	gasMcopy          = memoryCopierGas(2)
	gasExtCodeCopy    = memoryCopierGas(3)
	gasReturnDataCopy = memoryCopierGas(2)
)

func gasSStore(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	if evm.readOnly {
		return mdgas.MdGas{}, ErrWriteProtection
	}
	value, x := callContext.Stack.Back(1), callContext.Stack.Back(0)
	key := accounts.InternKey(x.Bytes32())
	current, _ := evm.IntraBlockState().GetState(callContext.Address(), key)
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
			return mdgas.MdGas{Regular: params.SstoreSetGas}, nil
		case !current.IsZero() && value.IsZero(): // non 0 => 0
			evm.IntraBlockState().AddRefund(params.SstoreRefundGas)
			return mdgas.MdGas{Regular: params.SstoreClearGas}, nil
		default: // non 0 => non 0 (or 0 => 0)
			return mdgas.MdGas{Regular: params.SstoreResetGas}, nil
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
		return mdgas.MdGas{Regular: params.NetSstoreNoopGas}, nil
	}
	var original, _ = evm.IntraBlockState().GetCommittedState(callContext.Address(), key)
	if original == current {
		if original.IsZero() { // create slot (2.1.1)
			return mdgas.MdGas{Regular: params.NetSstoreInitGas}, nil
		}
		if value.IsZero() { // delete slot (2.1.2b)
			evm.IntraBlockState().AddRefund(params.NetSstoreClearRefund)
		}
		return mdgas.MdGas{Regular: params.NetSstoreCleanGas}, nil // write existing slot (2.1.2)
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

	return mdgas.MdGas{Regular: params.NetSstoreDirtyGas}, nil
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
func gasSStoreEIP2200(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	if evm.readOnly {
		return mdgas.MdGas{}, ErrWriteProtection
	}
	// If we fail the minimum gas availability invariant, fail (0)
	if callContext.gas <= params.SstoreSentryGasEIP2200 {
		return mdgas.MdGas{}, errors.New("not enough gas for reentrancy sentry")
	}
	// Gas sentry honoured, do the actual gas calculation based on the stored value
	value, x := callContext.Stack.Back(1), callContext.Stack.Back(0)
	key := accounts.InternKey(x.Bytes32())
	current, _ := evm.IntraBlockState().GetState(callContext.Address(), key)

	if current.Eq(value) { // noop (1)
		return mdgas.MdGas{Regular: params.SloadGasEIP2200}, nil
	}

	var original, _ = evm.IntraBlockState().GetCommittedState(callContext.Address(), key)
	if original == current {
		if original.IsZero() { // create slot (2.1.1)
			return mdgas.MdGas{Regular: params.SstoreSetGasEIP2200}, nil
		}
		if value.IsZero() { // delete slot (2.1.2b)
			evm.IntraBlockState().AddRefund(params.SstoreClearsScheduleRefundEIP2200)
		}
		return mdgas.MdGas{Regular: params.SstoreResetGasEIP2200}, nil // write existing slot (2.1.2)
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
	return mdgas.MdGas{Regular: params.SloadGasEIP2200}, nil // dirty update (2.2)
}

func makeGasLog(n uint64) gasFunc {
	return func(_ *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
		requestedSize, overflow := callContext.Stack.Back(1).Uint64WithOverflow()
		if overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}

		gas, err := memoryGasCost(callContext, memorySize)
		if err != nil {
			return mdgas.MdGas{}, err
		}

		if gas, overflow = math.SafeAdd(gas, params.LogGas); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}
		if gas, overflow = math.SafeAdd(gas, n*params.LogTopicGas); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}

		var memorySizeGas uint64
		if memorySizeGas, overflow = math.SafeMul(requestedSize, params.LogDataGas); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}
		if gas, overflow = math.SafeAdd(gas, memorySizeGas); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}
		return mdgas.MdGas{Regular: gas}, nil
	}
}

func gasKeccak256(_ *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	gas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return mdgas.MdGas{}, err
	}
	wordGas, overflow := callContext.Stack.Back(1).Uint64WithOverflow()
	if overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	if wordGas, overflow = math.SafeMul(ToWordSize(wordGas), params.Keccak256WordGas); overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	if gas, overflow = math.SafeAdd(gas, wordGas); overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	return mdgas.MdGas{Regular: gas}, nil
}

// pureMemoryGascost is used by several operations, which aside from their
// static cost have a dynamic cost which is solely based on the memory
// expansion
func pureMemoryGascost(_ *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	g, err := memoryGasCost(callContext, memorySize)
	return mdgas.MdGas{Regular: g}, err
}

var (
	gasReturn  = pureMemoryGascost
	gasRevert  = pureMemoryGascost
	gasMLoad   = pureMemoryGascost
	gasMStore8 = pureMemoryGascost
	gasMStore  = pureMemoryGascost
	gasCreate  = pureMemoryGascost
)

func gasCreate2(_ *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	gas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return mdgas.MdGas{}, err
	}
	size, overflow := callContext.Stack.Back(2).Uint64WithOverflow()
	if overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	numWords := ToWordSize(size)
	wordGas, overflow := math.SafeMul(numWords, params.Keccak256WordGas)
	if overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	gas, overflow = math.SafeAdd(gas, wordGas)
	if overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	return mdgas.MdGas{Regular: gas}, nil
}

func gasCreateEip3860(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (gas mdgas.MdGas, err error) {
	gas.Regular, err = memoryGasCost(callContext, memorySize)
	if err != nil {
		return mdgas.MdGas{}, err
	}
	size, overflow := callContext.Stack.Back(2).Uint64WithOverflow()
	if overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	if err := CheckMaxInitCodeSize(size, evm.ChainRules().IsShanghai, evm.ChainRules().IsAmsterdam); err != nil {
		return mdgas.MdGas{}, err
	}
	numWords := ToWordSize(size)
	// Since size <= params.MaxInitCodeSize(Amsterdam), this multiplication cannot overflow
	wordGas := params.InitCodeWordGas * numWords
	gas.Regular, overflow = math.SafeAdd(gas.Regular, wordGas)
	if overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	if evm.chainRules.IsAmsterdam {
		gas.State += params.StateBytesNewAccount * evm.Context.CostPerStateByte
	}
	return gas, nil
}

func gasCreate2Eip3860(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (gas mdgas.MdGas, err error) {
	gas.Regular, err = memoryGasCost(callContext, memorySize)
	if err != nil {
		return mdgas.MdGas{}, err
	}
	size, overflow := callContext.Stack.Back(2).Uint64WithOverflow()
	if overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	if err := CheckMaxInitCodeSize(size, evm.ChainRules().IsShanghai, evm.ChainRules().IsAmsterdam); err != nil {
		return mdgas.MdGas{}, err
	}
	numWords := ToWordSize(size)
	// Since size <= params.MaxInitCodeSize(Amsterdam), this multiplication cannot overflow
	wordGas := (params.InitCodeWordGas + params.Keccak256WordGas) * numWords
	gas.Regular, overflow = math.SafeAdd(gas.Regular, wordGas)
	if overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	if evm.chainRules.IsAmsterdam {
		gas.State += params.StateBytesNewAccount * evm.Context.CostPerStateByte
	}
	return gas, nil
}

func gasExpFrontier(_ *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	expByteLen := uint64(common.BitLenToByteLen(callContext.Stack.data[callContext.Stack.len()-2].BitLen()))

	var (
		gas      = expByteLen * params.ExpByteFrontier // no overflow check required. Max is 256 * ExpByte gas
		overflow bool
	)
	if gas, overflow = math.SafeAdd(gas, params.ExpGas); overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	return mdgas.MdGas{Regular: gas}, nil
}

func gasExpEIP160(_ *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	expByteLen := uint64(common.BitLenToByteLen(callContext.Stack.data[callContext.Stack.len()-2].BitLen()))

	var (
		gas      = expByteLen * params.ExpByteEIP160 // no overflow check required. Max is 256 * ExpByte gas
		overflow bool
	)
	if gas, overflow = math.SafeAdd(gas, params.ExpGas); overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}
	return mdgas.MdGas{Regular: gas}, nil
}

func gasCall(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	gas, _, err := statelessGasCall(evm, callContext, availableGas, memorySize, true)
	return gas, err
}

func statelessGasCall(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64, withCallGasCalc bool) (mdgas.MdGas, bool, error) {
	var gas mdgas.MdGas

	transfersValue := !callContext.Stack.Back(2).IsZero()
	if transfersValue {
		gas.Regular += params.CallValueTransferGas
	}
	memoryGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return mdgas.MdGas{}, transfersValue, err
	}

	if evm.readOnly && transfersValue {
		return mdgas.MdGas{}, false, ErrWriteProtection
	}

	var overflow bool
	if gas.Regular, overflow = math.SafeAdd(gas.Regular, memoryGas); overflow {
		return mdgas.MdGas{}, false, ErrGasUintOverflow
	}

	if availableGas.Regular < gas.Regular {
		return mdgas.MdGas{}, false, ErrOutOfGas
	}

	if !withCallGasCalc {
		if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
			fmt.Printf("%d (%d.%d) Call Gas: avail: %d, base: %d memory(%d): %d\n",
				evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), availableGas, gas.Regular-memoryGas, memorySize, memoryGas)
		}
		return gas, transfersValue, nil
	}

	gas, err = statefulGasCall(evm, callContext, gas, availableGas, transfersValue)
	if err != nil {
		return mdgas.MdGas{}, false, err
	}

	if availableGas.Regular < gas.Regular {
		return mdgas.MdGas{}, false, ErrOutOfGas
	}
	callGas, err := calcCallGas(evm, callContext, availableGas.Regular, gas.Regular)
	if err != nil {
		return mdgas.MdGas{}, false, err
	}

	if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
		fmt.Printf("%d (%d.%d) Call Gas: avail: %d, base: %d memory(%d): %d call: %d\n",
			evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), availableGas, gas.Regular-memoryGas, memorySize, memoryGas, callGas)
	}

	gas.Regular, overflow = math.SafeAdd(gas.Regular, callGas)
	if overflow {
		return mdgas.MdGas{}, false, ErrGasUintOverflow
	}
	return gas, transfersValue, nil
}

func statefulGasCall(evm *EVM, callContext *CallContext, gas mdgas.MdGas, availableGas mdgas.MdGas, transfersValue bool) (mdgas.MdGas, error) {
	var accountGas, stateGas uint64
	var address = accounts.InternAddress(callContext.Stack.Back(1).Bytes20())
	rules := evm.ChainRules()
	if rules.IsSpuriousDragon {
		empty, err := evm.IntraBlockState().Empty(address)
		if err != nil {
			return mdgas.MdGas{}, err
		}
		// Empty() reads account state for gas calculation — record for BAL
		// tracking unconditionally, since the read happens regardless of
		// whether the CALL proceeds or transfers value.
		evm.IntraBlockState().MarkAddressAccess(address, false)
		if transfersValue && empty {
			if rules.IsAmsterdam {
				stateGas = params.StateBytesNewAccount * evm.Context.CostPerStateByte
			} else {
				accountGas = params.CallNewAccountGas
			}
		}
	} else {
		exists, err := evm.IntraBlockState().Exist(address)
		if err != nil {
			return mdgas.MdGas{}, err
		}
		// Exist() reads account state for gas calculation — record for BAL.
		evm.IntraBlockState().MarkAddressAccess(address, false)
		if !exists {
			// note this doesn't need updating for amsterdam since
			// this branch is only for paths before spurious dragon
			accountGas = params.CallNewAccountGas
		}
	}

	var overflow bool
	if gas.Regular, overflow = math.SafeAdd(gas.Regular, accountGas); overflow {
		return mdgas.MdGas{}, ErrGasUintOverflow
	}

	if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
		fmt.Printf("%d (%d.%d) Call Gas: account: %d\n",
			evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), accountGas)
	}

	if stateGas > 0 {
		gas.State, overflow = math.SafeAdd(gas.State, stateGas)
		if overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}
	}

	return gas, nil
}

func calcCallGas(evm *EVM, callContext *CallContext, availableGas, baseGas uint64) (uint64, error) {
	callGas, err := callGas(evm.ChainRules().IsTangerineWhistle, availableGas, baseGas, callContext.Stack.Back(0))
	if err != nil {
		return 0, err
	}
	evm.SetCallGasTemp(callGas)
	return callGas, nil
}

func gasCallCode(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	gas, _, err := statelessGasCallCode(evm, callContext, availableGas, memorySize, true)
	return gas, err
}

func statefulGasCallCode(evm *EVM, callContext *CallContext, gas mdgas.MdGas, availableGas mdgas.MdGas, transfersValue bool) (mdgas.MdGas, error) {
	return gas, nil
}

func statelessGasCallCode(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64, withCallGasCalc bool) (mdgas.MdGas, bool, error) {
	memoryGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return mdgas.MdGas{}, false, err
	}
	var (
		gas      mdgas.MdGas
		overflow bool
	)
	if !callContext.Stack.Back(2).IsZero() {
		gas.Regular += params.CallValueTransferGas
	}

	if gas.Regular, overflow = math.SafeAdd(gas.Regular, memoryGas); overflow {
		return mdgas.MdGas{}, false, ErrGasUintOverflow
	}

	if availableGas.Regular < gas.Regular {
		return mdgas.MdGas{}, false, ErrOutOfGas
	}

	if !withCallGasCalc {
		if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
			fmt.Printf("%d (%d.%d) CallCode Gas: base: %d memory(%d): %d\n",
				evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), gas.Regular-memoryGas, memorySize, memoryGas)
		}
		return gas, false, nil
	}

	callGas, err := calcCallGas(evm, callContext, availableGas.Regular, gas.Regular)

	if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
		fmt.Printf("%d (%d.%d) CallCode Gas: base: %d memory(%d): %d call: %d\n",
			evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), gas.Regular-memoryGas, memorySize, memoryGas, callGas)
	}

	gas.Regular, overflow = math.SafeAdd(gas.Regular, callGas)
	if overflow {
		return mdgas.MdGas{}, false, ErrGasUintOverflow
	}

	return gas, false, err
}

func gasDelegateCall(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	gas, _, err := statelessGasDelegateCall(evm, callContext, availableGas, memorySize, true)
	return gas, err
}

func statefulGasDelegateCall(evm *EVM, callContext *CallContext, gas mdgas.MdGas, availableGas mdgas.MdGas, transfersValue bool) (mdgas.MdGas, error) {
	return gas, nil
}

func statelessGasDelegateCall(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64, withCallGasCalc bool) (mdgas.MdGas, bool, error) {
	gas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return mdgas.MdGas{}, false, err
	}

	if availableGas.Regular < gas {
		return mdgas.MdGas{}, false, ErrOutOfGas
	}

	var callGasTemp uint64
	callGasTemp, err = callGas(evm.ChainRules().IsTangerineWhistle, availableGas.Regular, gas, callContext.Stack.Back(0))
	evm.SetCallGasTemp(callGasTemp)

	if err != nil {
		return mdgas.MdGas{}, false, err
	}

	if !withCallGasCalc {
		if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
			fmt.Printf("%d (%d.%d) DelegateCall Gas: memory(%d): %d\n",
				evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), memorySize, gas)
		}
		return mdgas.MdGas{Regular: gas}, false, nil
	}

	callGas, err := calcCallGas(evm, callContext, availableGas.Regular, gas)

	if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
		fmt.Printf("%d (%d.%d) DelegateCall Gas: memory(%d): %d call: %d\n",
			evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), memorySize, gas, callGas)
	}

	var overflow bool
	gas, overflow = math.SafeAdd(gas, callGas)
	if overflow {
		return mdgas.MdGas{}, false, ErrGasUintOverflow
	}

	return mdgas.MdGas{Regular: gas}, false, err
}

func gasStaticCall(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	gas, _, err := statelessGasStaticCall(evm, callContext, availableGas, memorySize, true)
	return gas, err
}

func statefulGasStaticCall(evm *EVM, callContext *CallContext, gas mdgas.MdGas, availableGas mdgas.MdGas, transfersValue bool) (mdgas.MdGas, error) {
	return gas, nil
}

func statelessGasStaticCall(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64, withCallGasCalc bool) (mdgas.MdGas, bool, error) {
	gas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return mdgas.MdGas{}, false, err
	}

	if availableGas.Regular < gas {
		return mdgas.MdGas{}, false, ErrOutOfGas
	}

	if !withCallGasCalc {
		if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
			fmt.Printf("%d (%d.%d) StaticCall Gas: memory(%d): %d\n",
				evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), memorySize, gas)
		}
		return mdgas.MdGas{Regular: gas}, false, nil
	}

	callGas, err := calcCallGas(evm, callContext, availableGas.Regular, gas)

	if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
		fmt.Printf("%d (%d.%d) StaticCall Gas: memory(%d): %d call: %d\n",
			evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(), memorySize, gas, callGas)
	}

	var overflow bool
	gas, overflow = math.SafeAdd(gas, callGas)
	if overflow {
		return mdgas.MdGas{}, false, ErrGasUintOverflow
	}

	return mdgas.MdGas{Regular: gas}, false, err
}

func gasSelfdestruct(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	if evm.readOnly {
		return mdgas.MdGas{}, ErrWriteProtection
	}

	var gas mdgas.MdGas
	// TangerineWhistle (EIP150) gas reprice fork:
	if evm.ChainRules().IsTangerineWhistle {
		gas.Regular = params.SelfdestructGasEIP150
		var address = accounts.InternAddress(callContext.Stack.Back(0).Bytes20())

		if evm.ChainRules().IsSpuriousDragon {
			// if empty and transfers value
			empty, err := evm.IntraBlockState().Empty(address)
			if err != nil {
				return mdgas.MdGas{}, err
			}
			// Empty() reads account state for gas calculation — record for BAL.
			evm.IntraBlockState().MarkAddressAccess(address, false)
			balance, err := evm.IntraBlockState().GetBalance(callContext.Address())
			if err != nil {
				return mdgas.MdGas{}, err
			}
			if empty && !balance.IsZero() {
				gas.Regular += params.CreateBySelfdestructGas
			}
		} else {
			exist, err := evm.IntraBlockState().Exist(address)
			if err != nil {
				return mdgas.MdGas{}, err
			}
			// Exist() reads account state for gas calculation — record for BAL.
			evm.IntraBlockState().MarkAddressAccess(address, false)
			if !exist {
				gas.Regular += params.CreateBySelfdestructGas
			}
		}
	}

	hasSelfdestructed, err := evm.IntraBlockState().HasSelfdestructed(callContext.Address())
	if err != nil {
		return mdgas.MdGas{}, err
	}
	if !hasSelfdestructed {
		evm.IntraBlockState().AddRefund(params.SelfdestructRefundGas)
	}
	return gas, nil
}
