package vm

import (
	"fmt"
	"github.com/erigontech/erigon/core/state"
	"github.com/erigontech/erigon/core/vm/evmtypes"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/chain/params"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/nitro-erigon/util/arbmath"
	"github.com/holiman/uint256"
)

// Computes the cost of doing a state load in wasm
// Note: the code here is adapted from gasSLoadEIP2929
func WasmStateLoadCost(db *state.IntraBlockState, program common.Address, key common.Hash) uint64 {
	// Check slot presence in the access list
	if _, slotPresent := db.SlotInAccessList(program, key); !slotPresent {
		// If the caller cannot afford the cost, this change will be rolled back
		// If he does afford it, we can skip checking the same thing later on, during execution
		db.AddSlotToAccessList(program, key)
		return params.ColdSloadCostEIP2929
	}
	return params.WarmStorageReadCostEIP2929
}

// Computes the cost of doing a state store in wasm
// Note: the code here is adapted from makeGasSStoreFunc with the most recent parameters as of The Merge
// Note: the sentry check must be done by the caller
func WasmStateStoreCost(db *state.IntraBlockState, program common.Address, key, value common.Hash) uint64 {
	clearingRefund := params.SstoreClearsScheduleRefundEIP3529

	current := new(uint256.Int)
	if err := db.GetState(program, key, current); err != nil {
		panic(err)
	}

	var cost uint64
	// Check slot presence in the access list
	if addrPresent, slotPresent := db.SlotInAccessList(program, key); !slotPresent {
		cost = params.ColdSloadCostEIP2929
		// If the caller cannot afford the cost, this change will be rolled back
		db.AddSlotToAccessList(program, key)
		if !addrPresent {
			panic(fmt.Sprintf("impossible case: address %v was not present in access list", program))
		}
	}

	if arbmath.BigEquals(current.ToBig(), value.Big()) { // noop (1)
		// EIP 2200 original clause:
		//		return params.SloadGasEIP2200, nil
		return cost + params.WarmStorageReadCostEIP2929 // SLOAD_GAS
	}
	original := uint256.NewInt(0)
	if err := db.GetCommittedState(program, key, original); err != nil {
		panic(err)
	}
	if original.Eq(current) {
		if original.IsZero() { // create slot (2.1.1)
			return cost + params.SstoreSetGasEIP2200
		}
		if value.Cmp(common.Hash{}) == 0 { // delete slot (2.1.2b)
			db.AddRefund(clearingRefund)
		}
		// EIP-2200 original clause:
		//		return params.SstoreResetGasEIP2200, nil // write existing slot (2.1.2)
		return cost + (params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929) // write existing slot (2.1.2)
	}
	if !original.IsZero() {
		if current.IsZero() { // recreate slot (2.2.1.1)
			db.SubRefund(clearingRefund)
		} else if value.Cmp(common.Hash{}) == 0 { // delete slot (2.2.1.2)
			db.AddRefund(clearingRefund)
		}
	}

	if arbmath.BigEquals(original.ToBig(), value.Big()) {
		if original.IsZero() { // reset to original inexistent slot (2.2.2.1)
			// EIP 2200 Original clause:
			//evm.StateDB.AddRefund(params.SstoreSetGasEIP2200 - params.SloadGasEIP2200)
			db.AddRefund(params.SstoreSetGasEIP2200 - params.WarmStorageReadCostEIP2929)
		} else { // reset to original existing slot (2.2.2.2)
			// EIP 2200 Original clause:
			//	evm.StateDB.AddRefund(params.SstoreResetGasEIP2200 - params.SloadGasEIP2200)
			// - SSTORE_RESET_GAS redefined as (5000 - COLD_SLOAD_COST)
			// - SLOAD_GAS redefined as WARM_STORAGE_READ_COST
			// Final: (5000 - COLD_SLOAD_COST) - WARM_STORAGE_READ_COST
			db.AddRefund((params.SstoreResetGasEIP2200 - params.ColdSloadCostEIP2929) - params.WarmStorageReadCostEIP2929)
		}
	}
	// EIP-2200 original clause:
	//return params.SloadGasEIP2200, nil // dirty update (2.2)
	return cost + params.WarmStorageReadCostEIP2929 // dirty update (2.2)
}

// Computes the cost of starting a call from wasm
//
// The code here is adapted from the following functions with the most recent parameters as of The Merge
//   - operations_acl.go makeCallVariantGasCallEIP2929()
//   - gas_table.go      gasCall()
func WasmCallCost(db evmtypes.IntraBlockState, contract common.Address, value *uint256.Int, budget uint64) (uint64, error) {
	total := uint64(0)
	apply := func(amount uint64) bool {
		total += amount
		return total > budget
	}

	// EIP 2929: the static cost
	if apply(params.WarmStorageReadCostEIP2929) {
		return total, ErrOutOfGas
	}

	// EIP 2929: first dynamic cost if cold (makeCallVariantGasCallEIP2929)
	warmAccess := db.AddressInAccessList(contract)
	coldCost := params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929
	if !warmAccess {
		db.AddAddressToAccessList(contract)

		if apply(coldCost) {
			return total, ErrOutOfGas
		}
	}

	// gasCall()
	transfersValue := value.Sign() != 0
	if transfersValue {
		if empty, _ := db.Empty(contract); empty {
			if apply(params.CallNewAccountGas) {
				return total, ErrOutOfGas
			}
		}
		if apply(params.CallValueTransferGas) {
			return total, ErrOutOfGas
		}
	}
	return total, nil
}

// Computes the cost of touching an account in wasm
// Note: the code here is adapted from gasEip2929AccountCheck with the most recent parameters as of The Merge
func WasmAccountTouchCost(cfg *chain.Config, db evmtypes.IntraBlockState, addr common.Address, withCode bool) uint64 {
	cost := uint64(0)
	if withCode {
		cost = cfg.MaxCodeSize() / 24576 * params.ExtcodeSizeGasEIP150
	}

	if !db.AddressInAccessList(addr) {
		db.AddAddressToAccessList(addr)
		return cost + params.ColdAccountAccessCostEIP2929
	}
	return cost + params.WarmStorageReadCostEIP2929
}
