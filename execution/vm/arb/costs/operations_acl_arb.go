package costs

import (
	"fmt"

	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/types/accounts"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

// Computes the cost of doing a state load in wasm
// Note: the code here is adapted from gasSLoadEIP2929
func WasmStateLoadCost(db *state.IntraBlockState, program accounts.Address, key accounts.StorageKey) multigas.MultiGas {
	// Check slot presence in the access list
	if _, slotPresent := db.SlotInAccessList(program, key); !slotPresent {
		// If the caller cannot afford the cost, this change will be rolled back
		// If he does afford it, we can skip checking the same thing later on, during execution
		db.AddSlotToAccessList(program, key)

		// TODO consider that og code counted only params.ColdSloadCostEIP2929
		// Cold slot access considered as storage access + computation
		return multigas.MultiGasFromPairs(
			multigas.Pair{Kind: multigas.ResourceKindStorageAccess, Amount: params.ColdSloadCostEIP2929 - params.WarmStorageReadCostEIP2929},
			multigas.Pair{Kind: multigas.ResourceKindComputation, Amount: params.WarmStorageReadCostEIP2929},
		)
	}
	// Warm slot access considered as computation
	return multigas.ComputationGas(params.WarmStorageReadCostEIP2929)
}

// Computes the cost of doing a state store in wasm
// Note: the code here is adapted from makeGasSStoreFunc with the most recent parameters as of The Merge
// Note: the sentry check must be done by the caller
func WasmStateStoreCost(db *state.IntraBlockState, program accounts.Address, key accounts.StorageKey, value common.Hash) multigas.MultiGas {
	clearingRefund := params.SstoreClearsScheduleRefundEIP3529

	current, err := db.GetState(program, key)
	if err != nil {
		panic(err)
	}

	cost := multigas.ZeroGas()
	// Check slot presence in the access list
	if addrPresent, slotPresent := db.SlotInAccessList(program, key); !slotPresent {
		cost.SaturatingIncrementInto(multigas.ResourceKindStorageAccess, params.ColdSloadCostEIP2929)
		// If the caller cannot afford the cost, this change will be rolled back
		db.AddSlotToAccessList(program, key)
		if !addrPresent {
			panic(fmt.Sprintf("impossible case: address %v was not present in access list", program))
		}
	}

	valueU256 := new(uint256.Int).SetBytes(value.Bytes())
	if current.Eq(valueU256) { // noop (1)
		// EIP 2200 original clause:
		//		return params.SloadGasEIP2200, nil
		return cost.SaturatingIncrement(multigas.ResourceKindComputation, params.WarmStorageReadCostEIP2929) // SLOAD_GAS
	}

	original, err := db.GetCommittedState(program, key)
	if err != nil {
		panic(err)
	}
	if original.Eq(&current) {
		if original.IsZero() { // create slot (2.1.1)
			return cost.SaturatingIncrement(multigas.ResourceKindStorageGrowth, params.SstoreSetGasEIP2200)
		}
		if value.Cmp(common.Hash{}) == 0 { // delete slot (2.1.2b)
			db.AddRefund(clearingRefund)
		}
		// EIP-2200 original clause:
		//		return params.SstoreResetGasEIP2200, nil // write existing slot (2.1.2)
		return cost.SaturatingIncrement(multigas.ResourceKindStorageAccess, params.SstoreResetGasEIP2200-params.ColdSloadCostEIP2929)
	}
	if !original.IsZero() {
		if current.IsZero() { // recreate slot (2.2.1.1)
			db.SubRefund(clearingRefund)
		} else if value.Cmp(common.Hash{}) == 0 { // delete slot (2.2.1.2)
			db.AddRefund(clearingRefund)
		}
	}

	if original.Eq(valueU256) {
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
	return cost.SaturatingIncrement(multigas.ResourceKindComputation, params.WarmStorageReadCostEIP2929) // dirty update (2.2)
}

// Computes the cost of starting a call from wasm
//
// The code here is adapted from the following functions with the most recent parameters as of The Merge
//   - operations_acl.go makeCallVariantGasCallEIP2929()
//   - gas_table.go      gasCall()
func WasmCallCost(db evmtypes.IntraBlockState, contract accounts.Address, value *uint256.Int, budget uint64) (multigas.MultiGas, error) {
	total := multigas.ZeroGas()
	apply := func(resource multigas.ResourceKind, amount uint64) bool {
		total.SaturatingIncrementInto(resource, amount)
		return total.SingleGas() > budget
	}

	// EIP 2929: the static cost considered as computation
	if apply(multigas.ResourceKindComputation, params.WarmStorageReadCostEIP2929) {
		return total, vm.ErrOutOfGas
	}

	// EIP 2929: first dynamic cost if cold (makeCallVariantGasCallEIP2929)
	warmAccess := db.AddressInAccessList(contract)
	coldCost := params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929
	if !warmAccess {
		db.AddAddressToAccessList(contract)

		// Cold slot access considered as storage access.
		if apply(multigas.ResourceKindStorageAccess, coldCost) {
			return total, vm.ErrOutOfGas
		}
	}

	// gasCall()
	transfersValue := value.Sign() != 0
	if transfersValue {
		if empty, _ := db.Empty(contract); empty {
			// Storage slot writes (zero -> nonzero) considered as storage growth.
			if apply(multigas.ResourceKindStorageGrowth, params.CallNewAccountGas) {
				return total, vm.ErrOutOfGas
			}
		}
		// Value transfer to non-empty account considered as computation.
		if apply(multigas.ResourceKindComputation, params.CallValueTransferGas) {
			return total, vm.ErrOutOfGas
		}
	}
	return total, nil
}

// Computes the cost of touching an account in wasm
// Note: the code here is adapted from gasEip2929AccountCheck with the most recent parameters as of The Merge
func WasmAccountTouchCost(cfg *chain.Config, db evmtypes.IntraBlockState, addr accounts.Address, withCode bool) multigas.MultiGas {
	cost := multigas.ZeroGas()
	if withCode {
		extCodeCost := cfg.MaxCodeSize() / params.MaxCodeSize * params.ExtcodeSizeGasEIP150
		cost.SaturatingIncrementInto(multigas.ResourceKindStorageAccess, extCodeCost)
	}

	if !db.AddressInAccessList(addr) {
		db.AddAddressToAccessList(addr)
		//return cost + params.ColdAccountAccessCostEIP2929
		// TODO arbitrum - pricing differs?

		// Cold slot read -> storage access + computation
		return cost.SaturatingAdd(multigas.MultiGasFromPairs(
			multigas.Pair{Kind: multigas.ResourceKindStorageAccess, Amount: params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929},
			multigas.Pair{Kind: multigas.ResourceKindComputation, Amount: params.WarmStorageReadCostEIP2929},
		))
	}
	//return cost + params.WarmStorageReadCostEIP2929
	// Warm slot read considered as computation.
	return cost.SaturatingIncrement(multigas.ResourceKindComputation, params.WarmStorageReadCostEIP2929)
}

// Computes the history growth part cost of log operation in wasm,
// Full cost is charged on the WASM side at the emit_log function
// Note: the code here is adapted from makeGasLog
func WasmLogCost(numTopics uint64, dataBytes uint64) multigas.MultiGas {
	// Bloom/topic history growth: LogTopicHistoryGas per topic
	bloomHistoryGrowthCost := params.LogTopicHistoryGas * numTopics

	// Payload history growth: LogDataGas per payload byte
	payloadHistoryGrowthCost := params.LogDataGas * dataBytes

	return multigas.HistoryGrowthGas(bloomHistoryGrowthCost + payloadHistoryGrowthCost)
}
