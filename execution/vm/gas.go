// Copyright 2015 The go-ethereum Authors
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
	"github.com/erigontech/erigon/arb/multigas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/holiman/uint256"
)

// Gas costs
const (
	GasQuickStep   uint64 = 2
	GasFastestStep uint64 = 3
	GasFastStep    uint64 = 5
	GasMidStep     uint64 = 8
	GasSlowStep    uint64 = 10
	GasExtStep     uint64 = 20
)

// callGas returns the actual gas cost of the call.
//
// The cost of gas was changed during the homestead price change HF.
// As part of EIP 150 (TangerineWhistle), the returned gas is gas - base * 63 / 64.
func callGas(isEip150 bool, availableGas, base uint64, callCost *uint256.Int) (uint64, error) {
	if isEip150 {
		availableGas = availableGas - base
		gas := availableGas - availableGas/64
		// If the bit length exceeds 64 bit we know that the newly calculated "gas" for EIP150
		// is smaller than the requested amount. Therefore we return the new gas instead
		// of returning an error.
		if !callCost.IsUint64() || gas < callCost.Uint64() {
			return gas, nil
		}
	}
	if !callCost.IsUint64() {
		return 0, ErrGasUintOverflow
	}

	return callCost.Uint64(), nil
}

// addConstantMultiGas adds to usedMultiGas the constant multi-gas cost of an opcode.
func addConstantMultiGas(usedMultiGas *multigas.MultiGas, cost uint64, op OpCode) {
	// SELFDESTRUCT is a special case because it charges for storage access but it isn't
	// dependent on any input data. We charge a small computational cost for warm access like
	// other multi-dimensional gas opcodes, and the rest is storage access to delete the
	// contract from the database.
	// Note we only need to cover EIP150 because it the current cost, and SELFDESTRUCT cost was
	// zero previously.
	if op == SELFDESTRUCT && cost == params.SelfdestructGasEIP150 {
		usedMultiGas.SaturatingIncrementInto(multigas.ResourceKindComputation, params.WarmStorageReadCostEIP2929)
		usedMultiGas.SaturatingIncrementInto(multigas.ResourceKindStorageAccess, cost-params.WarmStorageReadCostEIP2929)
	} else {
		usedMultiGas.SaturatingIncrementInto(multigas.ResourceKindComputation, cost)
	}
}
