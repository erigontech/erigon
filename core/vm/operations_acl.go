// Copyright 2020 The go-ethereum Authors
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

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/math"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/execution/chain/params"
)

func makeGasSStoreFunc(clearingRefund uint64) gasFunc {
	return func(evm *EVM, contract *Contract, stack *Stack, mem *Memory, memorySize uint64) (uint64, error) {
		// If we fail the minimum gas availability invariant, fail (0)
		if contract.Gas <= params.SstoreSentryGasEIP2200 {
			return 0, errors.New("not enough gas for reentrancy sentry")
		}
		// Gas sentry honoured, do the actual gas calculation based on the stored value
		var (
			y, x    = stack.Back(1), stack.peek()
			slot    = common.Hash(x.Bytes32())
			current uint256.Int
			cost    = uint64(0)
		)

		evm.IntraBlockState().GetState(contract.Address(), slot, &current)
		// If the caller cannot afford the cost, this change will be rolled back
		if _, slotMod := evm.IntraBlockState().AddSlotToAccessList(contract.Address(), slot); slotMod {
			cost = params.ColdSloadCostEIP2929
		}
		var value uint256.Int
		value.Set(y)

		if current.Eq(&value) { // noop (1)
			// EIP 2200 original clause:
			//		return params.SloadGasEIP2200, nil
			return cost + params.WarmStorageReadCostEIP2929, nil // SLOAD_GAS
		}
		var original uint256.Int
		slotCommited := common.Hash(x.Bytes32())
		evm.IntraBlockState().GetCommittedState(contract.Address(), slotCommited, &original)
		if original.Eq(&current) {
			if original.IsZero() { // create slot (2.1.1)
				return cost + params.SstoreSetGasEIP2200, nil
			}
			if value.IsZero() { // delete slot (2.1.2b)
				evm.IntraBlockState().AddRefund(clearingRefund)
			}
			// EIP-2200 original clause:
			//		return params.SstoreResetGasEIP2200, nil // write existing slot (2.1.2)
			return cost + (params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929), nil // write existing slot (2.1.2)
		}
		if !original.IsZero() {
			if current.IsZero() { // recreate slot (2.2.1.1)
				evm.IntraBlockState().SubRefund(clearingRefund)
			} else if value.IsZero() { // delete slot (2.2.1.2)
				evm.IntraBlockState().AddRefund(clearingRefund)
			}
		}
		if original.Eq(&value) {
			if original.IsZero() { // reset to original inexistent slot (2.2.2.1)
				// EIP 2200 Original clause:
				//evm.StateDB.AddRefund(params.SstoreSetGasEIP2200 - params.SloadGasEIP2200)
				evm.IntraBlockState().AddRefund(params.SstoreSetGasEIP2200 - params.WarmStorageReadCostEIP2929)
			} else { // reset to original existing slot (2.2.2.2)
				// EIP 2200 Original clause:
				//	evm.StateDB.AddRefund(params.SstoreResetGasEIP2200 - params.SloadGasEIP2200)
				// - SSTORE_RESET_GAS redefined as (5000 - COLD_SLOAD_COST)
				// - SLOAD_GAS redefined as WARM_STORAGE_READ_COST
				// Final: (5000 - COLD_SLOAD_COST) - WARM_STORAGE_READ_COST
				evm.IntraBlockState().AddRefund((params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929) - params.WarmStorageReadCostEIP2929)
			}
		}
		// EIP-2200 original clause:
		//return params.SloadGasEIP2200, nil // dirty update (2.2)
		return cost + params.WarmStorageReadCostEIP2929, nil // dirty update (2.2)
	}
}

// gasSLoadEIP2929 calculates dynamic gas for SLOAD according to EIP-2929
// For SLOAD, if the (address, storage_key) pair (where address is the address of the contract
// whose storage is being read) is not yet in accessed_storage_keys,
// charge 2100 gas and add the pair to accessed_storage_keys.
// If the pair is already in accessed_storage_keys, charge 100 gas.
func gasSLoadEIP2929(evm *EVM, contract *Contract, stack *Stack, mem *Memory, memorySize uint64) (uint64, error) {
	loc := stack.peek()
	// If the caller cannot afford the cost, this change will be rolled back
	// If he does afford it, we can skip checking the same thing later on, during execution
	if _, slotMod := evm.IntraBlockState().AddSlotToAccessList(contract.Address(), loc.Bytes32()); slotMod {
		return params.ColdSloadCostEIP2929, nil
	}
	return params.WarmStorageReadCostEIP2929, nil
}

// gasExtCodeCopyEIP2929 implements extcodecopy according to EIP-2929
// EIP spec:
// > If the target is not in accessed_addresses,
// > charge COLD_ACCOUNT_ACCESS_COST gas, and add the address to accessed_addresses.
// > Otherwise, charge WARM_STORAGE_READ_COST gas.
func gasExtCodeCopyEIP2929(evm *EVM, contract *Contract, stack *Stack, mem *Memory, memorySize uint64) (uint64, error) {
	// memory expansion first (dynamic part of pre-2929 implementation)
	gas, err := gasExtCodeCopy(evm, contract, stack, mem, memorySize)
	if err != nil {
		return 0, err
	}
	addr := common.Address(stack.peek().Bytes20())
	// Check slot presence in the access list
	if evm.IntraBlockState().AddAddressToAccessList(addr) {
		var overflow bool
		// We charge (cold-warm), since 'warm' is already charged as constantGas
		if gas, overflow = math.SafeAdd(gas, params.ColdAccountAccessCostEIP2929-params.WarmStorageReadCostEIP2929); overflow {
			return 0, ErrGasUintOverflow
		}
		return gas, nil
	}
	return gas, nil
}

// gasEip2929AccountCheck checks whether the first stack item (as address) is present in the access list.
// If it is, this method returns '0', otherwise 'cold-warm' gas, presuming that the opcode using it
// is also using 'warm' as constant factor.
// This method is used by:
// - extcodehash,
// - extcodesize,
// - (ext) balance
func gasEip2929AccountCheck(evm *EVM, contract *Contract, stack *Stack, mem *Memory, memorySize uint64) (uint64, error) {
	addr := common.Address(stack.peek().Bytes20())
	// If the caller cannot afford the cost, this change will be rolled back
	if evm.IntraBlockState().AddAddressToAccessList(addr) {
		// The warm storage read cost is already charged as constantGas
		return params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929, nil
	}
	return 0, nil
}

func makeCallVariantGasCallEIP2929(oldCalculator gasFunc) gasFunc {
	return func(evm *EVM, contract *Contract, stack *Stack, mem *Memory, memorySize uint64) (uint64, error) {
		addr := common.Address(stack.Back(1).Bytes20())
		// The WarmStorageReadCostEIP2929 (100) is already deducted in the form of a constant cost, so
		// the cost to charge for cold access, if any, is Cold - Warm
		coldCost := params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929

		addrMod := evm.IntraBlockState().AddAddressToAccessList(addr)
		warmAccess := !addrMod
		if addrMod {
			// Charge the remaining difference here already, to correctly calculate available
			// gas for call
			if !contract.UseGas(coldCost, evm.Config().Tracer, tracing.GasChangeCallStorageColdAccess) {
				return 0, ErrOutOfGas
			}
		}

		// Now call the old calculator, which takes into account
		// - create new account
		// - transfer value
		// - memory expansion
		// - 63/64ths rule
		gas, err := oldCalculator(evm, contract, stack, mem, memorySize)
		if warmAccess || err != nil {
			return gas, err
		}
		// In case of a cold access, we temporarily add the cold charge back, and also
		// add it to the returned gas. By adding it to the return, it will be charged
		// outside of this function, as part of the dynamic gas, and that will make it
		// also become correctly reported to tracers.
		contract.Gas += coldCost
		return gas + coldCost, nil
	}
}

var (
	gasCallEIP2929         = makeCallVariantGasCallEIP2929(gasCall)
	gasDelegateCallEIP2929 = makeCallVariantGasCallEIP2929(gasDelegateCall)
	gasStaticCallEIP2929   = makeCallVariantGasCallEIP2929(gasStaticCall)
	gasCallCodeEIP2929     = makeCallVariantGasCallEIP2929(gasCallCode)
	gasSelfdestructEIP2929 = makeSelfdestructGasFn(true)
	// gasSelfdestructEIP3529 implements the changes in EIP-2539 (no refunds)
	gasSelfdestructEIP3529 = makeSelfdestructGasFn(false)

	// gasSStoreEIP2929 implements gas cost for SSTORE according to EIP-2929
	//
	// When calling SSTORE, check if the (address, storage_key) pair is in accessed_storage_keys.
	// If it is not, charge an additional COLD_SLOAD_COST gas, and add the pair to accessed_storage_keys.
	// Additionally, modify the parameters defined in EIP 2200 as follows:
	//
	// Parameter 	Old value 	New value
	// SLOAD_GAS 	800 	= WARM_STORAGE_READ_COST
	// SSTORE_RESET_GAS 	5000 	5000 - COLD_SLOAD_COST
	//
	//The other parameters defined in EIP 2200 are unchanged.
	// see gasSStoreEIP2200(...) in core/vm/gas_table.go for more info about how EIP 2200 is specified
	gasSStoreEIP2929 = makeGasSStoreFunc(params.SstoreClearsScheduleRefundEIP2200)

	// gasSStoreEIP2539 implements gas cost for SSTORE according to EPI-2539
	// Replace `SSTORE_CLEARS_SCHEDULE` with `SSTORE_RESET_GAS + ACCESS_LIST_STORAGE_KEY_COST` (4,800)
	gasSStoreEIP3529 = makeGasSStoreFunc(params.SstoreClearsScheduleRefundEIP3529)
)

// makeSelfdestructGasFn can create the selfdestruct dynamic gas function for EIP-2929 and EIP-2539
func makeSelfdestructGasFn(refundsEnabled bool) gasFunc {
	gasFunc := func(evm *EVM, contract *Contract, stack *Stack, mem *Memory, memorySize uint64) (uint64, error) {
		var (
			gas     uint64
			address = common.Address(stack.peek().Bytes20())
		)
		// If the caller cannot afford the cost, this change will be rolled back
		if evm.IntraBlockState().AddAddressToAccessList(address) {
			gas = params.ColdAccountAccessCostEIP2929
		}
		// if empty and transfers value
		empty, err := evm.IntraBlockState().Empty(address)
		if err != nil {
			return 0, err
		}
		balance, err := evm.IntraBlockState().GetBalance(contract.Address())
		if err != nil {
			return 0, err
		}
		if empty && !balance.IsZero() {
			gas += params.CreateBySelfdestructGas
		}
		hasSelfdestructed, err := evm.IntraBlockState().HasSelfdestructed(contract.Address())
		if err != nil {
			return 0, err
		}
		if refundsEnabled && !hasSelfdestructed {
			evm.IntraBlockState().AddRefund(params.SelfdestructRefundGas)
		}
		return gas, nil
	}
	return gasFunc
}

var (
	gasCallEIP7702         = makeCallVariantGasCallEIP7702(gasCall)
	gasDelegateCallEIP7702 = makeCallVariantGasCallEIP7702(gasDelegateCall)
	gasStaticCallEIP7702   = makeCallVariantGasCallEIP7702(gasStaticCall)
	gasCallCodeEIP7702     = makeCallVariantGasCallEIP7702(gasCallCode)
)

func makeCallVariantGasCallEIP7702(oldCalculator gasFunc) gasFunc {
	return func(evm *EVM, contract *Contract, stack *Stack, mem *Memory, memorySize uint64) (uint64, error) {
		addr := common.Address(stack.Back(1).Bytes20())
		// Check slot presence in the access list
		var dynCost uint64
		if evm.intraBlockState.AddAddressToAccessList(addr) {
			// The WarmStorageReadCostEIP2929 (100) is already deducted in the form of a constant cost, so
			// the cost to charge for cold access, if any, is Cold - Warm
			dynCost = params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929
			// Charge the remaining difference here already, to correctly calculate available
			// gas for call
			if !contract.UseGas(dynCost, evm.Config().Tracer, tracing.GasChangeCallStorageColdAccess) {
				return 0, ErrOutOfGas
			}
		}

		// Check if code is a delegation and if so, charge for resolution.
		dd, ok, err := evm.intraBlockState.GetDelegatedDesignation(addr)
		if err != nil {
			return 0, err
		}
		if ok {
			var ddCost uint64
			if evm.intraBlockState.AddAddressToAccessList(dd) {
				ddCost = params.ColdAccountAccessCostEIP2929
			} else {
				ddCost = params.WarmStorageReadCostEIP2929
			}

			if !contract.UseGas(ddCost, evm.Config().Tracer, tracing.GasChangeDelegatedDesignation) {
				return 0, ErrOutOfGas
			}
			dynCost += ddCost
		}
		// Now call the old calculator, which takes into account
		// - create new account
		// - transfer value
		// - memory expansion
		// - 63/64ths rule
		gas, err := oldCalculator(evm, contract, stack, mem, memorySize)
		if dynCost == 0 || err != nil {
			return gas, err
		}
		// In case of a cold access, we temporarily add the cold charge back, and also
		// add it to the returned gas. By adding it to the return, it will be charged
		// outside of this function, as part of the dynamic gas, and that will make it
		// also become correctly reported to tracers.
		contract.Gas += dynCost

		var overflow bool
		if gas, overflow = math.SafeAdd(gas, dynCost); overflow {
			return 0, ErrGasUintOverflow
		}
		return gas, nil
	}
}
