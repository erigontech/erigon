package vm

import (
	libcommon "github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/params"
)

var (
	gasCallEIP2929_zkevm         = makeCallVariantGasCallEIP2929_zkevm(gasCall)
	gasDelegateCallEIP2929_zkevm = makeCallVariantGasCallEIP2929_zkevm(gasDelegateCall)
	gasStaticCallEIP2929_zkevm   = makeCallVariantGasCallEIP2929_zkevm(gasStaticCall)
	gasCallCodeEIP2929_zkevm     = makeCallVariantGasCallEIP2929_zkevm(gasCallCode)
	gasSelfdestructEIP2929_zkevm = makeSelfdestructGasFn_zkevm(true)
	// gasSelfdestructEIP3529_zkevm implements the changes in EIP-2539 (no refunds)
	gasSelfdestructEIP3529_zkevm = makeSelfdestructGasFn_zkevm(false)
)

// makeCallGasFn_zkevm can create the call dynamic gas function for EIP-2929 and EIP-2539 for Polygon zkEVM
func makeCallVariantGasCallEIP2929_zkevm(oldCalculator gasFunc) gasFunc {
	return func(evm VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
		addr := libcommon.Address(stack.Back(1).Bytes20())
		// Check slot presence in the access list
		warmAccess := evm.IntraBlockState().AddressInAccessList(addr)
		// The WarmStorageReadCostEIP2929 (100) is already deducted in the form of a constant cost, so
		// the cost to charge for cold access, if any, is Cold - Warm
		coldCost := params.ColdAccountAccessCostEIP2929 - params.WarmStorageReadCostEIP2929
		if !warmAccess {
			//[zkevm] - moved after err check, because zkevm reverts the address add
			// evm.IntraBlockState().AddAddressToAccessList(addr)

			// Charge the remaining difference here already, to correctly calculate available
			// gas for call
			if !contract.UseGas(coldCost) {
				return 0, ErrOutOfGas
			}
			evm.IntraBlockState().AddAddressToAccessList(addr)
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

// makeSelfdestructGasFn_zkevm can create the selfdestruct dynamic gas function for EIP-2929 and EIP-2539 for Polygon zkEVM
func makeSelfdestructGasFn_zkevm(refundsEnabled bool) gasFunc {
	gasFunc := func(evm VMInterpreter, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
		var (
			gas     uint64
			address = libcommon.Address(stack.Peek().Bytes20())
		)
		if !evm.IntraBlockState().AddressInAccessList(address) {
			// If the caller cannot afford the cost, this change will be rolled back
			evm.IntraBlockState().AddAddressToAccessList(address)
			gas = params.ColdAccountAccessCostEIP2929
		}
		// if empty and transfers value
		if evm.IntraBlockState().Empty(address) && !evm.IntraBlockState().GetBalance(contract.Address()).IsZero() {
			gas += params.CreateBySelfdestructGas
		}
		//[zkevm] - according to eip-4758 this is removed
		// if refundsEnabled && !evm.IntraBlockState().HasSelfdestructed(contract.Address()) {
		// 	evm.IntraBlockState().AddRefund(params.SelfdestructRefundGas)
		// }
		return gas, nil
	}
	return gasFunc
}
