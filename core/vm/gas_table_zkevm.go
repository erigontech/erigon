package vm

import (
	"bytes"

	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/core/vm/stack"
	"github.com/ledgerwatch/erigon/params"
)

func gasSelfdestruct_zkevm(evm *EVM, contract *Contract, stack *stack.Stack, mem *Memory, memorySize uint64) (uint64, error) {
	var gas uint64
	var address = libcommon.Address(stack.Back(0).Bytes20())

	callerAddr := contract.Address()
	balance := evm.IntraBlockState().GetBalance(callerAddr)

	//[zkevm] - cold access gas if there was a transfer
	if !evm.IntraBlockState().AddressInAccessList(address) {
		evm.IntraBlockState().AddAddressToAccessList(address)

		// in this case there was no transfer, so don't take cold access gas
		if !bytes.Equal(callerAddr.Bytes(), address.Bytes()) && balance.GtUint64(0) {
			// If the caller cannot afford the cost, this change will be rolled back
			gas += params.ColdAccountAccessCostEIP2929
		}
	}

	// TangerineWhistle (EIP150) gas reprice fork:
	if evm.ChainRules().IsTangerineWhistle {
		gas += params.SelfdestructGasEIP150

		if evm.ChainRules().IsSpuriousDragon {
			// if empty and transfers value
			if evm.IntraBlockState().Empty(address) && !evm.IntraBlockState().GetBalance(contract.Address()).IsZero() {
				gas += params.CreateBySelfdestructGas
			}
		} else if !evm.IntraBlockState().Exist(address) {
			gas += params.CreateBySelfdestructGas
		}
	}
	//[zkevm] - according to eip-4758 this is removed
	// if !evm.IntraBlockState().HasSelfdestructed(contract.Address()) {
	// 	evm.IntraBlockState().AddRefund(params.SelfdestructRefundGas)
	// }
	return gas, nil
}
