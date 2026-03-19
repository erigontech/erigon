// Copyright 2026 The Erigon Authors
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

package misc

import (
	"math"
	"math/bits"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
)

func CostPerStateByte(gasLimit uint64) uint64 {
	// TODO this should be removed after bal-devnet-3 (we use hardcoded cspb=1174 for now)
	const balDevnet3Spec = true
	if balDevnet3Spec {
		return 1174
	}
	//raw = ceil((gas_limit * 2_628_000) / (2 * TARGET_STATE_GROWTH_PER_YEAR))
	//shifted = raw + CPSB_OFFSET
	//shift = max(bit_length(shifted) - CPSB_SIGNIFICANT_BITS, 0)
	//cost_per_state_byte = max(((shifted >> shift) << shift) - CPSB_OFFSET, 1)
	raw := uint64(math.Ceil(float64(gasLimit*2_628_000) / float64(2*params.TargetStateGrowthPerYear)))
	shifted := raw + params.CpsbOffset
	shift := max(bits.Len64(shifted)-params.CpsbSignificantBits, 0)
	rounded := (shifted >> shift) << shift
	if rounded <= params.CpsbOffset {
		return 1
	}
	return rounded - params.CpsbOffset
}

func SplitIntoMdGas(txnGasLimit uint64, igas mdgas.MdGas, rules *chain.Rules) mdgas.MdGas {
	if rules.IsAmsterdam {
		//intrinsic_gas = intrinsic_regular_gas + intrinsic_state_gas
		//execution_gas = tx.gas - intrinsic_gas
		//regular_gas_budget = TX_MAX_GAS_LIMIT - intrinsic_regular_gas
		//gas_left = min(regular_gas_budget, execution_gas)
		//state_gas_reservoir = execution_gas - gas_left
		intrinsicGas := igas.Regular + igas.State
		executionGas := txnGasLimit - intrinsicGas
		regularGasBudget := params.MaxTxnGasLimit - igas.Regular
		gasLeft := min(regularGasBudget, executionGas)
		stateGasReservoir := executionGas - gasLeft
		return mdgas.MdGas{Regular: gasLeft, State: stateGasReservoir}
	}
	gas := mdgas.MdGas{Regular: txnGasLimit}
	gas.Regular -= igas.Regular
	return gas
}
