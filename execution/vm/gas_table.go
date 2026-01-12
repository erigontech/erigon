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
	"github.com/erigontech/erigon/arb/multigas"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types/accounts"
)

// memoryGasCost calculates the quadratic gas for memory expansion. It does so
// only for the memory region that is expanded, not the total memory.
func memoryGasCost(mem *Memory, newMemSize uint64) (multigas.MultiGas, error) {
	if newMemSize == 0 {
		return multigas.ZeroGas(), nil
	}
	// The maximum that will fit in a uint64 is max_word_count - 1. Anything above
	// that will result in an overflow. Additionally, a newMemSize which results in
	// a newMemSizeWords larger than 0xFFFFFFFF will cause the square operation to
	// overflow. The constant 0x1FFFFFFFE0 is the highest number that can be used
	// without overflowing the gas calculation.
	if newMemSize > 0x1FFFFFFFE0 {
		return multigas.ZeroGas(), ErrGasUintOverflow
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

		// Memory expansion considered as computation.
		// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
		return multigas.ComputationGas(fee), nil
	}
	return multigas.ZeroGas(), nil
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
	return func(_ *EVM, callContext *CallContext, scaopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
		// Gas for expanding the memory
		multiGas, err := memoryGasCost(callContext, memorySize)
		if err != nil {
			return multigas.ZeroGas(), err
		}
		// And gas for copying data, charged per word at param.CopyGas
		words, overflow := callContext.Stack.Back(stackpos).Uint64WithOverflow()
		if overflow {
			return multigas.ZeroGas(), ErrGasUintOverflow
		}

		var wordCopyGas uint64
		if wordCopyGas, overflow = math.SafeMul(ToWordSize(words), params.CopyGas); overflow {
			return multigas.ZeroGas(), ErrGasUintOverflow
		}

		// Distribute copy gas by dimension:
		// - For EXTCODECOPY: count as ResourceKindStorageAccess since it is the only opcode
		// using stack position 3 and reading from the state trie.
		// - For others: count as ResourceKindComputation since they are in-memory operations
		// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
		var dim multigas.ResourceKind
		if stackpos == 3 {
			dim = multigas.ResourceKindStorageAccess // EXTCODECOPY
		} else {
			dim = multigas.ResourceKindComputation // CALLDATACOPY, CODECOPY, MCOPY, RETURNDATACOPY
		}
		if multiGas, overflow = multiGas.SafeIncrement(dim, wordCopyGas); overflow {
			return multigas.ZeroGas(), ErrGasUintOverflow
		}
		return multiGas, nil
	}
}

var (
	gasCallDataCopy   = memoryCopierGas(2)
	gasCodeCopy       = memoryCopierGas(2)
	gasMcopy          = memoryCopierGas(2)
	gasExtCodeCopy    = memoryCopierGas(3)
	gasReturnDataCopy = memoryCopierGas(2)
)

func gasSStore(evm *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
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
			return multigas.StorageGrowthGas(params.SstoreSetGas), nil
		case !current.IsZero() && value.IsZero(): // non 0 => 0
			evm.IntraBlockState().AddRefund(params.SstoreRefundGas)
			return multigas.StorageAccessGas(params.SstoreClearGas), nil
		default: // non 0 => non 0 (or 0 => 0)
			return multigas.StorageAccessGas(params.SstoreResetGas), nil
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
		return multigas.StorageAccessGas(params.NetSstoreNoopGas), nil
	}
	var original, _ = evm.IntraBlockState().GetCommittedState(callContext.Address(), key)
	if original == current {
		if original.IsZero() { // create slot (2.1.1)
			return multigas.StorageGrowthGas(params.NetSstoreInitGas), nil
		}
		if value.IsZero() { // delete slot (2.1.2b)
			evm.IntraBlockState().AddRefund(params.NetSstoreClearRefund)
		}
		return multigas.StorageAccessGas(params.NetSstoreCleanGas), nil // write existing slot (2.1.2)
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

	return multigas.StorageAccessGas(params.NetSstoreDirtyGas), nil
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
func gasSStoreEIP2200(evm *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	// If we fail the minimum gas availability invariant, fail (0)
	if callContext.gas <= params.SstoreSentryGasEIP2200 {
		return multigas.ZeroGas(), errors.New("not enough gas for reentrancy sentry")
	}
	// Gas sentry honoured, do the actual gas calculation based on the stored value
	value, x := callContext.Stack.Back(1), callContext.Stack.Back(0)
	key := accounts.InternKey(x.Bytes32())
	current, _ := evm.IntraBlockState().GetState(callContext.Address(), key)

	if current.Eq(value) { // noop (1)
		return multigas.StorageAccessGas(params.SloadGasEIP2200), nil
	}

	var original, _ = evm.IntraBlockState().GetCommittedState(callContext.Address(), key)
	if original == current {
		if original.IsZero() { // create slot (2.1.1)
			return multigas.StorageGrowthGas(params.SstoreSetGasEIP2200), nil
		}
		if value.IsZero() { // delete slot (2.1.2b)
			evm.IntraBlockState().AddRefund(params.SstoreClearsScheduleRefundEIP2200)
		}
		return multigas.StorageAccessGas(params.SstoreResetGasEIP2200), nil
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
	return multigas.StorageAccessGas(params.SloadGasEIP2200), nil // dirty update (2.2)
}

func makeGasLog(n uint64) gasFunc {
	return func(_ *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
		requestedSize, overflow := callContext.Stack.Back(1).Uint64WithOverflow()
		if overflow {
			return multigas.ZeroGas(), ErrGasUintOverflow
		}

		multiGas, err := memoryGasCost(callContext, memorySize)
		if err != nil {
			return multigas.ZeroGas(), err
		}

		// Base LOG operation considered as computation.
		// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
		if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, params.LogGas); overflow {
			return multigas.ZeroGas(), ErrGasUintOverflow
		}
		if e.chainRules.IsArbitrum {
			// Per-topic cost is split between history growth and computation:
			// - A fixed number of bytes per topic is persisted in history (topicBytes),
			//   and those bytes are charged at LogDataGas (gas per byte) as history growth.
			// - The remainder of the per-topic cost is attributed to computation (e.g. hashing/bloom work).

			// Scale by number of topics for LOG0..LOG4
			var topicHistTotal, topicCompTotal uint64
			if topicHistTotal, overflow = math.SafeMul(n, params.LogTopicHistoryGas); overflow {
				return multigas.ZeroGas(), ErrGasUintOverflow
			}
			if topicCompTotal, overflow = math.SafeMul(n, params.LogTopicComputationGas); overflow {
				return multigas.ZeroGas(), ErrGasUintOverflow
			}

			// Apply the split.
			if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindHistoryGrowth, topicHistTotal); overflow {
				return multigas.ZeroGas(), ErrGasUintOverflow
			}
			if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, topicCompTotal); overflow {
				return multigas.ZeroGas(), ErrGasUintOverflow
			}
		} else {
			if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, n*params.LogTopicGas); overflow {
				return multigas.ZeroGas(), ErrGasUintOverflow
			}
		}

		// Data payload bytes → history growth at LogDataGas (gas per byte).
		var memorySizeGas uint64
		if memorySizeGas, overflow = math.SafeMul(requestedSize, params.LogDataGas); overflow {
			return multigas.ZeroGas(), ErrGasUintOverflow
		}
		// Event log data considered as history growth.
		// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
		if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindHistoryGrowth, memorySizeGas); overflow {
			return multigas.ZeroGas(), ErrGasUintOverflow
		}
		return multiGas, nil
	}
}

func gasKeccak256(_ *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	multiGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return multigas.ZeroGas(), err
	}
	wordGas, overflow := callContext.Stack.Back(1).Uint64WithOverflow()
	if overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	if wordGas, overflow = math.SafeMul(ToWordSize(wordGas), params.Keccak256WordGas); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, wordGas); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	return multiGas, nil
}

// pureMemoryGascost is used by several operations, which aside from their
// static cost have a dynamic cost which is solely based on the memory
// expansion
func pureMemoryGascost(_ *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	return memoryGasCost(callContext, memorySize)
}

var (
	gasReturn  = pureMemoryGascost
	gasRevert  = pureMemoryGascost
	gasMLoad   = pureMemoryGascost
	gasMStore8 = pureMemoryGascost
	gasMStore  = pureMemoryGascost
	gasCreate  = pureMemoryGascost
)

func gasCreate2(_ *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	multiGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return multigas.ZeroGas(), err
	}
	wordGas, overflow := callContext.stack.Back(2).Uint64WithOverflow()
	if overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	wordGas, overflow := callContext.stack.Back(2).Uint64WithOverflow()
	if wordGas, overflow = math.SafeMul(ToWordSize(wordGas), params.Keccak256WordGas); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	if wordGas, overflow = math.SafeMul(ToWordSize(wordGas), params.Keccak256WordGas); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	// Keccak hashing considered as computation.
	// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
	if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, wordGas); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	return multiGas, nil
}

func gasCreateEip3860(_ *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	multiGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return multigas.ZeroGas(), err
	}
	size, overflow := callContext.Stack.Back(2).Uint64WithOverflow()
	if overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	//var ics = uint64(params.MaxCodeSize)
	//if evm.chainRules.IsArbitrum {
	//	ics = evm.chainConfig.MaxInitCodeSize()
	//}
	if size > evm.chainConfig.MaxInitCodeSize() {
		return multigas.ZeroGas(), fmt.Errorf("%w: size %d", ErrMaxInitCodeSizeExceeded, size)
	}
	// Since size <= params.MaxInitCodeSize, these multiplication cannot overflow
	moreGas := params.InitCodeWordGas * ((size + 31) / 32) // numWords

	// Init code execution considered as computation.
	// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
	if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, moreGas); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	return multiGas, nil
}

func gasCreate2Eip3860(evm *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	multiGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return multigas.ZeroGas(), err
	}
	size, overflow := callContext.Stack.Back(2).Uint64WithOverflow()
	if overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	//var ics = uint64(params.MaxCodeSize)
	//if evm.chainRules.IsArbitrum {
	//	ics = evm.chainConfig.MaxInitCodeSize()
	//}
	if size > evm.chainConfig.MaxInitCodeSize() {
		return multigas.ZeroGas(), fmt.Errorf("%w: size %d", ErrMaxInitCodeSizeExceeded, size)
	}
	// Since size <= params.MaxInitCodeSize, these multiplication cannot overflow
	moreGas := (params.InitCodeWordGas + params.Keccak256WordGas) * ((size + 31) / 32) // numWords

	// Init code execution and Keccak hashing both considered as computation.
	// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
	if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, moreGas); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	return multiGas, nil
}

func gasExpFrontier(_ *EVM, contract *Contract, stack *Stack, mem *Memory, memorySize uint64) (multigas.MultiGas, error) {
	expByteLen := uint64(common.BitLenToByteLen(callContext.Stack.data[callContext.Stack.len()-2].BitLen()))

	var (
		gas      = expByteLen * params.ExpByteFrontier // no overflow check required. Max is 256 * ExpByte gas
		overflow bool
	)
	if gas, overflow = math.SafeAdd(gas, params.ExpGas); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	return multigas.ComputationGas(gas), nil
}

func gasExpEIP160(_ *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	expByteLen := uint64(common.BitLenToByteLen(stack.data[stack.len()-2].BitLen()))

	var (
		gas      = expByteLen * params.ExpByteEIP160 // no overflow check required. Max is 256 * ExpByte gas
		overflow bool
	)

	if gas, overflow = math.SafeAdd(gas, params.ExpGas); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	return multigas.ComputationGas(gas), nil
}

func gasCall(evm *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	var (
		multiGas       = multigas.ZeroGas()
		transfersValue = !callContext.Stack.Back(2).IsZero()
		address        = accounts.InternAddress(callContext.Stack.Back(1).Bytes20())
	)
	if evm.ChainRules().IsSpuriousDragon {
		empty, err := evm.IntraBlockState().Empty(address)
		if err != nil {
			return multigas.ZeroGas(), err
		}
		if transfersValue && empty {
			multiGas = multiGas.SaturatingIncrement(multigas.ResourceKindStorageGrowth, params.CallNewAccountGas)
		}
	} else {
		exists, err := evm.IntraBlockState().Exist(address)
		if err != nil {
			return multigas.ZeroGas(), err
		}
		if !exists {
			multiGas = multiGas.SaturatingIncrement(multigas.ResourceKindStorageGrowth, params.CallNewAccountGas)
		}
	}
	// Value transfer to non-empty account considered as computation.
	// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
	if transfersValue { // &&  !evm.chainRules.IsEIP4762
		multiGas = multiGas.SaturatingIncrement(multigas.ResourceKindComputation, params.CallValueTransferGas)
	}

	memoryMultiGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return multigas.ZeroGas(), err
	}
	multiGas, overflow := multiGas.SafeAdd(memoryMultiGas)
	if overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}

	var callGasTemp uint64
	singleGas := multiGas.SingleGas()
	callGasTemp, err = callGas(evm.ChainRules().IsTangerineWhistle, scopeGas, singleGas, callContext.Stack.Back(0))
	evm.SetCallGasTemp(callGasTemp)

	if err != nil {
		return multigas.ZeroGas(), err
	}
	// Call gas forwarding considered as computation.
	// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
	if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, callGasTemp); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow

	if gas, overflow = math.SafeAdd(gas, callGasTemp); overflow {
		return 0, ErrGasUintOverflow
	}

	return multiGas, nil
}

func gasCallCode(evm *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	memoryMultiGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return multigas.ZeroGas(), err
	}
	multiGas := multigas.ZeroGas()
	if !stack.Back(2).IsZero() {
		// Value transfer to non-empty account considered as computation.
		// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
		multiGas = multiGas.SaturatingIncrement(multigas.ResourceKindComputation, params.CallValueTransferGas)
	}

	if gas, overflow = math.SafeAdd(gas, memoryGas); overflow {
		return 0, ErrGasUintOverflow

	var overflow bool
	multiGas, overflow = multiGas.SafeAdd(memoryMultiGas)
	if overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	singleGas := multiGas.SingleGas()

	callGasTemp, err := callGas(evm.ChainRules().IsTangerineWhistle, contract.Gas, singleGas, stack.Back(0))
	evm.SetCallGasTemp(callGasTemp)

	if err != nil {
		return multigas.ZeroGas(), err
	}
	// Call gas forwarding considered as computation.
	// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
	if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, callGasTemp); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}
	return multiGas, nil
}

func gasDelegateCall(evm *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	multiGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return multigas.ZeroGas(), err
	}

	gas := multiGas.SingleGas()
	callGasTemp, err := callGas(evm.ChainRules().IsTangerineWhistle, scopeGas, gas, callContext.Stack.Back(0))
	evm.SetCallGasTemp(callGasTemp)

	if err != nil {
		return multigas.ZeroGas(), err
	}
	// Call gas forwarding considered as computation.
	// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
	var overflow bool
	if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, callGasTemp); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}

	return multiGas, nil
}

func gasStaticCall(evm *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
	multiGas, err := memoryGasCost(callContext, memorySize)
	if err != nil {
		return multigas.ZeroGas(), err
	}

	gas := multiGas.SingleGas()
	callGasTemp, err := callGas(evm.ChainRules().IsTangerineWhistle, scopeGas, gas, callContext.Stack.Back(0))
	var callGasTemp uint64
	callGasTemp, err = callGas(evm.ChainRules().IsTangerineWhistle, scopeGas, gas, callContext.Stack.Back(0))
	evm.SetCallGasTemp(callGasTemp)

	if err != nil {
		return multigas.ZeroGas(), err
	}
	// Call gas forwarding considered as computation.
	// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
	var overflow bool
	if multiGas, overflow = multiGas.SafeIncrement(multigas.ResourceKindComputation, callGasTemp); overflow {
		return multigas.ZeroGas(), ErrGasUintOverflow
	}

	return multiGas, nil
}

func gasSelfdestruct(evm *EVM, callContext *CallContext, scopeGas uint64, memorySize uint64) (multigas.MultiGas, error) {
			multiGas := multigas.ZeroGas()
	// TangerineWhistle (EIP150) gas reprice fork:
	if evm.ChainRules().IsTangerineWhistle {
		// Selfdestruct operation considered as storage access.
		// See rationale in: https://github.com/OffchainLabs/nitro/blob/master/docs/decisions/0002-multi-dimensional-gas-metering.md
		multiGas = multiGas.SaturatingIncrement(multigas.ResourceKindStorageAccess, params.SelfdestructGasEIP150)
		var address = accounts.InternAddress(callContext.Stack.Back(0).Bytes20())

		if evm.ChainRules().IsSpuriousDragon {
			// if empty and transfers value
			empty, err := evm.IntraBlockState().Empty(address)
			if err != nil {
				return multigas.ZeroGas(), err
			}
			balance, err := evm.IntraBlockState().GetBalance(callContext.Address())
			if err != nil {
				return multigas.ZeroGas(), err
			}
			if empty && !balance.IsZero() {
				multiGas = multiGas.SaturatingIncrement(multigas.ResourceKindStorageGrowth, params.CreateBySelfdestructGas)
			}
		} else {
			exist, err := evm.IntraBlockState().Exist(address)
			if err != nil {
				return multigas.ZeroGas(), err
			}
			if !exist {
				multiGas = multiGas.SaturatingIncrement(multigas.ResourceKindStorageGrowth, params.CreateBySelfdestructGas)
			}
		}
	}

	hasSelfdestructed, err := evm.IntraBlockState().HasSelfdestructed(callContext.Address())
	if err != nil {
		return multigas.ZeroGas(), err
	}
	if !hasSelfdestructed {
		evm.IntraBlockState().AddRefund(params.SelfdestructRefundGas)
	}
	return multiGas, nil
}
