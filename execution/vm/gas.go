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
	"github.com/holiman/uint256"

	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/tracing"
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
		availableGas -= base
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

// State-gas charges and refills follow EIP-8037's reservoir rules, except that
// frame transactions use EIP-8141's isolated state-gas pool:
// https://eips.ethereum.org/EIPS/eip-8037#transaction-level-gas-accounting-reservoir-model
// https://eips.ethereum.org/EIPS/eip-8141#frame-gas-pools
func applyStateGas(callContext *CallContext, gas int64) bool {
	if gas > 0 {
		return callContext.useMdGas(uint64(gas), mdgas.StateGas, nil, tracing.GasChangeIgnored)
	}
	// Frame refills were applied to their owner by the gas calculator. Keep the
	// negative cost for tracing without crediting the current frame a second time.
	if callContext.frameContext == nil {
		callContext.refillStateGas(uint64(-gas), nil, tracing.GasChangeIgnored)
	}
	return true
}
