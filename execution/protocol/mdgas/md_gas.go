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

package mdgas

import (
	"fmt"

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/params"
)

// MdGas represents multi-dimensional gas
type MdGas struct {
	Regular uint64
	State   uint64
}

func (g MdGas) Minus(other MdGas) MdGas {
	return MdGas{
		Regular: g.Regular - other.Regular,
		State:   g.State - other.State,
	}
}

func (g MdGas) Plus(other MdGas) MdGas {
	return MdGas{
		Regular: g.Regular + other.Regular,
		State:   g.State + other.State,
	}
}

func (g MdGas) Total() uint64 {
	return g.Regular + g.State
}

type MdGasType uint8

const (
	RegularGas MdGasType = iota
	StateGas
)

func (t MdGasType) String() string {
	switch t {
	case RegularGas:
		return "regularGas"
	case StateGas:
		return "stateGas"
	default:
		panic(fmt.Errorf("unknown gas type: %d", t))
	}
}

// SplitTxnGasLimit splits a transaction's gas limit into regular and state dimensions.
// EIP-8037: when tx.gas > TX_MAX_GAS_LIMIT, excess gas beyond the regular budget
// becomes the state gas reservoir, which state-creation opcodes (SSTORE, CREATE,
// code deposit) draw from before spilling to regular gas.
// Pre-Amsterdam: all gas is regular (state reservoir is 0).
// See process_transaction in EIP-8037.
func SplitTxnGasLimit(txnGasLimit uint64, igas MdGas, rules *chain.Rules) MdGas {
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
		return MdGas{Regular: gasLeft, State: stateGasReservoir}
	}
	gas := MdGas{Regular: txnGasLimit}
	gas.Regular -= igas.Regular
	return gas
}
