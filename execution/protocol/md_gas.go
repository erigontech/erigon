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

package protocol

import (
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/vm/evmtypes"
)

func SplitIntoMdGas(txnGasLimit uint64, intrinsicGas evmtypes.MdGas, rules *chain.Rules) evmtypes.MdGas {
	if rules.IsAmsterdam {
		//intrinsic_gas = intrinsic_regular_gas + intrinsic_state_gas
		//execution_gas = tx.gas - intrinsic_gas
		//regular_gas_budget = TX_MAX_GAS_LIMIT - intrinsic_regular_gas
		//gas_left = min(regular_gas_budget, execution_gas)
		//state_gas_reservoir = execution_gas - gas_left
		intrinsicGas := intrinsicGas.Regular + intrinsicGas.State
		executionGas := txnGasLimit - intrinsicGas
		regularGasBudget := params.MaxTxnGasLimit - intrinsicGas
		gasLeft := min(regularGasBudget, executionGas)
		stateGasReservoir := executionGas - gasLeft
		return evmtypes.MdGas{Regular: gasLeft, State: stateGasReservoir}
	}
	gas := evmtypes.MdGas{Regular: txnGasLimit}
	gas.Regular -= intrinsicGas.Regular
	return gas
}
