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
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/types/accounts"
)

func makeGasSStoreFunc(clearingRefund uint64) gasFunc {
	return func(evm *EVM, callContext *CallContext, scopeGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
		rules := evm.chainRules
		if evm.readOnly {
			return mdgas.MdGas{}, ErrWriteProtection
		}
		// If we fail the minimum gas availability invariant, fail (0)
		if scopeGas.Regular <= params.SstoreSentryGasEIP2200 {
			return mdgas.MdGas{}, errors.New("not enough gas for reentrancy sentry")
		}
		// Gas sentry honoured, do the actual gas calculation based on the stored value
		var (
			y       = callContext.Stack.Back(1)
			slot    = callContext.peekStorageKey()
			current uint256.Int
			cost    = uint64(0)
		)

		current, _ = evm.IntraBlockState().GetState(callContext.Address(), slot)
		// If the caller cannot afford the cost, this change will be rolled back
		if _, slotMod := evm.IntraBlockState().AddSlotToAccessList(callContext.Address(), slot); slotMod {
			cost = params.ColdSloadCostEIP2929
			// Abort gas evaluation if there isn’t enough gas left,
			// ensuring no state access happens afterward.
			if callContext.gas < cost {
				return mdgas.MdGas{}, ErrOutOfGas
			}
		}
		var value uint256.Int
		value.Set(y)

		if current.Eq(&value) { // noop (1)
			// EIP 2200 original clause:
			//		return params.SloadGasEIP2200, nil
			return mdgas.MdGas{Regular: cost + params.WarmStorageReadCostEIP2929}, nil // SLOAD_GAS
		}

		var original, _ = evm.IntraBlockState().GetCommittedState(callContext.Address(), slot)
		if original.Eq(&current) {
			if original.IsZero() { // create slot (2.1.1)
				if rules.IsAmsterdam {
					return mdgas.MdGas{Regular: cost + params.SstoreSetGasEIP8037, State: 32 * evm.Context.CostPerStateByte}, nil
				} else {
					return mdgas.MdGas{Regular: cost + params.SstoreSetGasEIP2200}, nil
				}
			}
			if value.IsZero() { // delete slot (2.1.2b)
				evm.IntraBlockState().AddRefund(clearingRefund)
			}
			// EIP-2200 original clause:
			//		return params.SstoreResetGasEIP2200, nil // write existing slot (2.1.2)
			return mdgas.MdGas{Regular: cost + (params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929)}, nil // write existing slot (2.1.2)
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
				if rules.IsAmsterdam {
					evm.IntraBlockState().AddRefund(params.SstoreSetGasEIP8037 - params.WarmStorageReadCostEIP2929)
					evm.IntraBlockState().AddStateRefund(32 * evm.Context.CostPerStateByte)
				} else {
					evm.IntraBlockState().AddRefund(params.SstoreSetGasEIP2200 - params.WarmStorageReadCostEIP2929)
				}
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
		return mdgas.MdGas{Regular: cost + params.WarmStorageReadCostEIP2929}, nil // dirty update (2.2)
	}
}

// gasSLoadEIP2929 calculates dynamic gas for SLOAD according to EIP-2929
// For SLOAD, if the (address, storage_key) pair (where address is the address of the contract
// whose storage is being read) is not yet in accessed_storage_keys,
// charge 2100 gas and add the pair to accessed_storage_keys.
// If the pair is already in accessed_storage_keys, charge 100 gas.
func gasSLoadEIP2929(evm *EVM, callContext *CallContext, scopeGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	// If the caller cannot afford the cost, this change will be rolled back
	// If he does afford it, we can skip checking the same thing later on, during execution
	if _, slotMod := evm.IntraBlockState().AddSlotToAccessList(callContext.Address(), callContext.peekStorageKey()); slotMod {
		return mdgas.MdGas{Regular: params.ColdSloadCostEIP2929}, nil
	}
	return mdgas.MdGas{Regular: params.WarmStorageReadCostEIP2929}, nil
}

// gasExtCodeCopyEIP2929 implements extcodecopy according to EIP-2929
// EIP spec:
// > If the target is not in accessed_addresses,
// > charge COLD_ACCOUNT_ACCESS_COST gas, and add the address to accessed_addresses.
// > Otherwise, charge WARM_STORAGE_READ_COST gas.
func gasExtCodeCopyEIP2929(evm *EVM, callContext *CallContext, scopeGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	// memory expansion first (dynamic part of pre-2929 implementation)
	gas, err := gasExtCodeCopy(evm, callContext, scopeGas, memorySize)
	if err != nil {
		return mdgas.MdGas{}, err
	}
	addr := callContext.peekAddress()
	// Check slot presence in the access list
	if evm.IntraBlockState().AddAddressToAccessList(addr) {
		var overflow bool
		// We charge (cold-warm), since 'warm' is already charged as constantGas
		if gas.Regular, overflow = math.SafeAdd(gas.Regular, params.ColdAccountAccessCostEIP2929-params.WarmStorageReadCostEIP2929); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
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
func gasEip2929AccountCheck(evm *EVM, callContext *CallContext, scopeGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
	addr := callContext.peekAddress()
	// If the caller cannot afford the cost, this change will be rolled back
	if evm.IntraBlockState().AddAddressToAccessList(addr) {
		// The warm storage read cost is already charged as constantGas
		return mdgas.MdGas{Regular: params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929}, nil
	}
	return mdgas.MdGas{}, nil
}

func makeCallVariantGasCallEIP2929(oldCalculator gasFunc) gasFunc {
	return func(evm *EVM, callContext *CallContext, scopeGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
		addr := accounts.InternAddress(callContext.Stack.Back(1).Bytes20())
		// The WarmStorageReadCostEIP2929 (100) is already deducted in the form of a constant cost, so
		// the cost to charge for cold access, if any, is Cold - Warm
		coldCost := params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929
		warmAccess := evm.IntraBlockState().AddressInAccessList(addr)
		if !warmAccess {
			// Charge the remaining difference here already, to correctly calculate available
			// gas for call
			if _, ok := useGas(scopeGas.Regular, coldCost, evm.Config().Tracer, tracing.GasChangeCallStorageColdAccess); !ok {
				return mdgas.MdGas{}, ErrOutOfGas
			}
			evm.IntraBlockState().AddAddressToAccessList(addr)
			scopeGas.Regular -= coldCost
		}

		// Now call the old calculator, which takes into account
		// - create new account
		// - transfer value
		// - memory expansion
		// - 63/64ths rule
		gas, err := oldCalculator(evm, callContext, scopeGas, memorySize)
		if warmAccess || err != nil {
			return gas, err
		}
		// In case of a cold access, we temporarily add the cold charge back, and also
		// add it to the returned gas. By adding it to the return, it will be charged
		// outside of this function, as part of the dynamic gas, and that will make it
		// also become correctly reported to tracers.
		gas.Regular += coldCost
		return gas, nil
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
	gasFunc := func(evm *EVM, callContext *CallContext, scopeGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
		var (
			gas     mdgas.MdGas
			address = callContext.peekAddress()
		)
		if evm.readOnly {
			return mdgas.MdGas{}, ErrWriteProtection
		}
		// If the caller cannot afford the cost, this change will be rolled back
		if !evm.IntraBlockState().AddressInAccessList(address) {
			gas.Regular = params.ColdAccountAccessCostEIP2929
			if _, ok := useGas(scopeGas.Regular, gas.Regular, evm.Config().Tracer, tracing.GasChangeCallStorageColdAccess); !ok {
				return mdgas.MdGas{}, ErrOutOfGas
			}
			evm.IntraBlockState().AddAddressToAccessList(address)
		}

		// if empty and transfers value
		empty, err := evm.IntraBlockState().Empty(address)
		if err != nil {
			return mdgas.MdGas{}, err
		}
		balance, err := evm.IntraBlockState().GetBalance(callContext.Address())
		if err != nil {
			return mdgas.MdGas{}, err
		}
		// Record the beneficiary address access for BAL tracking when the
		// contract has non-zero balance.  A zero-balance selfdestruct does
		// not transfer value, so the beneficiary should not appear in the
		// block access list.  Skip in read-only context (STATICCALL) where
		// SELFDESTRUCT will be rejected by ErrWriteProtection.
		if !evm.readOnly && !balance.IsZero() {
			evm.IntraBlockState().MarkAddressAccess(address, false)
		}
		// When balance is zero OR we're in a read-only (STATICCALL) context,
		// and the beneficiary differs from self, mark the beneficiary's reads
		// as internal.  In both cases the Empty() call above recorded versioned
		// reads for the beneficiary purely for gas calculation — no value is
		// actually transferred (zero balance) or SELFDESTRUCT will be rejected
		// (read-only).  Skip when beneficiary == self to avoid incorrectly
		// marking the contract's own legitimate reads.
		if (balance.IsZero() || evm.readOnly) && address != callContext.Address() {
			evm.IntraBlockState().MarkReadsInternal(address)
		}
		if empty && !balance.IsZero() {
			if evm.chainRules.IsAmsterdam {
				gas.State = params.StateBytesNewAccount * evm.Context.CostPerStateByte
			} else {
				gas.Regular += params.CreateBySelfdestructGas
			}
		}

		hasSelfdestructed, err := evm.IntraBlockState().HasSelfdestructed(callContext.Address())
		if err != nil {
			return mdgas.MdGas{}, err
		}
		if refundsEnabled && !hasSelfdestructed {
			evm.IntraBlockState().AddRefund(params.SelfdestructRefundGas)
		}
		return gas, nil
	}
	return gasFunc
}

var (
	gasCallEIP7702         = makeCallVariantGasCallEIP7702(statelessGasCall, statefulGasCall, true)
	gasDelegateCallEIP7702 = makeCallVariantGasCallEIP7702(statelessGasDelegateCall, statefulGasDelegateCall, false)
	gasStaticCallEIP7702   = makeCallVariantGasCallEIP7702(statelessGasStaticCall, statefulGasStaticCall, false)
	gasCallCodeEIP7702     = makeCallVariantGasCallEIP7702(statelessGasCallCode, statefulGasCallCode, false)
)

func makeCallVariantGasCallEIP7702(statelessCalculator statelessGasFunc, statefulCalculator statefulGasFunc, rejectStaticValueTransfer bool) gasFunc {
	return func(evm *EVM, callContext *CallContext, availableGas mdgas.MdGas, memorySize uint64) (mdgas.MdGas, error) {
		// In static mode, CALL with value must fail before EIP-7702 can warm
		// the target or delegated address.
		if rejectStaticValueTransfer && evm.readOnly && !callContext.Stack.Back(2).IsZero() {
			return mdgas.MdGas{}, ErrWriteProtection
		}
		addr := accounts.InternAddress(callContext.Stack.Back(1).Bytes20())
		// Check slot presence in the access list
		var gas mdgas.MdGas
		var accessGas uint64
		if !evm.intraBlockState.AddressInAccessList(addr) {
			// The WarmStorageReadCostEIP2929 (100) is already deducted in the form of a constant cost, so
			// the cost to charge for cold access, if any, is Cold - Warm
			accessGas = params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929
			// Charge the remaining difference here already, to correctly calculate available
			// gas for call
			if availableGas.Regular < accessGas {
				return mdgas.MdGas{}, ErrOutOfGas
			}

			evm.intraBlockState.AddAddressToAccessList(addr)
		}

		// Call the old calculator, which takes into account
		// - create new account
		// - transfer value
		// - memory expansion
		statelessBaseGas, transfersValue, err := statelessCalculator(evm, callContext, availableGas, memorySize, false)
		if err != nil {
			return mdgas.MdGas{}, err
		}
		if statelessGas, overflow := math.SafeAdd(statelessBaseGas.Regular, accessGas); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		} else if availableGas.Regular < statelessGas {
			return mdgas.MdGas{}, ErrOutOfGas
		}

		statefulBaseGas, err := statefulCalculator(evm, callContext, statelessBaseGas, mdgas.MdGas{Regular: availableGas.Regular - accessGas}, transfersValue)
		if err != nil {
			return mdgas.MdGas{}, err
		}

		// EIP-8037: Match the reference execution spec charge order:
		//   regular base gas → state gas → 63/64 rule.
		//
		// Temporarily deduct the regular base from callContext so that the
		// state gas charge (which may spill into gas_left) sees the correct
		// reduced balance.  After computing the 63/64 rule we restore the
		// base so the interpreter can deduct (and trace) the full amount.
		regularBase := accessGas + statefulBaseGas.Regular
		if callContext.gas < regularBase {
			return mdgas.MdGas{}, ErrOutOfGas
		}
		callContext.gas -= regularBase // temporary

		if statefulBaseGas.State > 0 {
			ok := callContext.useMdGas(evm, statefulBaseGas.State, mdgas.StateGas, nil, tracing.GasChangeIgnored)
			if !ok {
				callContext.gas += regularBase // restore before error
				return mdgas.MdGas{}, ErrOutOfGas
			}
		}

		// Check if code is a delegation and if so, charge for resolution.
		dd, ok, err := evm.intraBlockState.GetDelegatedDesignation(addr)
		if err != nil {
			return mdgas.MdGas{}, err
		}

		var delegationGas uint64
		if ok {
			if !evm.intraBlockState.AddressInAccessList(dd) {
				delegationGas = params.ColdAccountAccessCostEIP2929
			} else {
				delegationGas = params.WarmStorageReadCostEIP2929
			}
			_, err := evm.intraBlockState.GetCode(addr)
			if err != nil {
				return mdgas.MdGas{}, err
			}
			if callContext.gas < delegationGas {
				callContext.gas += regularBase
				return mdgas.MdGas{}, ErrOutOfGas
			}
			callContext.gas -= delegationGas // temporary
			evm.intraBlockState.AddAddressToAccessList(dd)
		}

		availableGas = callContext.Gas()
		// 63/64ths rule with the reduced gas (after base + state + delegation).
		callGas, err := calcCallGas(evm, callContext, availableGas.Regular, 0)
		if err != nil {
			return mdgas.MdGas{}, err
		}

		// Restore the temporarily deducted base + delegation so the
		// interpreter deducts (and traces) the full dynamic cost.
		callContext.gas += regularBase + delegationGas

		if dbg.TraceDynamicGas && evm.intraBlockState.Trace() {
			fmt.Printf("%d (%d.%d) Variant Gas: base %d, access: %d, delegation: %d, call: %d\n",
				evm.intraBlockState.BlockNumber(), evm.intraBlockState.TxIndex(), evm.intraBlockState.Incarnation(),
				statefulBaseGas, accessGas, delegationGas, callGas)
		}
		var overflow bool
		if gas.Regular, overflow = math.SafeAdd(gas.Regular, accessGas+delegationGas); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}
		if gas.Regular, overflow = math.SafeAdd(gas.Regular, statefulBaseGas.Regular); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}
		if gas.Regular, overflow = math.SafeAdd(gas.Regular, callGas); overflow {
			return mdgas.MdGas{}, ErrGasUintOverflow
		}
		return gas, nil
	}
}
