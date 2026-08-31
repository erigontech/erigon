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

// MdGas represents multi-dimensional gas (execution + state) for reservoirs
// and leftover balances. Both fields are non-negative by construction.
type MdGas struct {
	Execution uint64
	State     uint64
}

func (g MdGas) Plus(other MdGas) MdGas {
	return MdGas{
		Execution: g.Execution + other.Execution,
		State:     g.State + other.State,
	}
}

func (g MdGas) Total() uint64 {
	return g.Execution + g.State
}

func NewFullMdGas(execution, state, blob uint64) FullMdGas {
	return FullMdGas{MdGas: MdGas{Execution: execution, State: state}, Blob: blob}
}

// FullMdGas extends MdGas with blob gas.
type FullMdGas struct {
	MdGas
	Blob uint64
}

// MdGasUsage reports per-frame gas usage.
type MdGasUsage struct {
	Execution  uint64
	State      int64 // can be negative due to state clearing (e.g. SSTORE clear)
	StateSpill uint64
}

// PlusIntrinsic folds intrinsic execution gas into the frame-usage report.
func (u MdGasUsage) PlusIntrinsic(intrinsicGas uint64) MdGasUsage {
	u.Execution += intrinsicGas
	return u
}

// StateClamped returns max(0, State) as uint64. State is signed to model
// EIP-8037 inline refunds; for block accounting (block_state_gas_used)
// the spec uses max(0, tx_state_gas), so negative net state contributes 0
// at the block level even though it does reduce the sender's tx_gas_used.
func (u MdGasUsage) StateClamped() uint64 {
	return uint64(max(0, u.State))
}

func (u MdGasUsage) Total() uint64 {
	return u.Execution + u.StateClamped()
}

// Consume atomically deducts a charge and records its multidimensional usage.
func Consume(remaining *MdGas, used *MdGasUsage, amount uint64, typ MdGasType) bool {
	switch typ {
	case ExecutionGas:
		if remaining.Execution < amount {
			return false
		}
		remaining.Execution -= amount
		used.Execution += amount
		return true
	case StateGas:
		stateGas := min(amount, remaining.State)
		spill := amount - stateGas
		if remaining.Execution < spill {
			return false
		}
		remaining.State -= stateGas
		remaining.Execution -= spill
		used.State += int64(amount)
		used.StateSpill += spill
		return true
	default:
		panic(fmt.Errorf("unknown gas type: %d", typ))
	}
}

// Refill reverses a gas charge, restoring state-gas spill before reservoir gas.
func Refill(remaining *MdGas, used *MdGasUsage, amount uint64, typ MdGasType) {
	switch typ {
	case ExecutionGas:
		remaining.Execution += amount
		used.Execution -= amount
	case StateGas:
		spill := min(amount, used.StateSpill)
		remaining.Execution += spill
		remaining.State += amount - spill
		used.State -= int64(amount)
		used.StateSpill -= spill
	default:
		panic(fmt.Errorf("unknown gas type: %d", typ))
	}
}

type MdGasType uint8

const (
	ExecutionGas MdGasType = iota
	StateGas
)

func (t MdGasType) String() string {
	switch t {
	case ExecutionGas:
		return "executionGas"
	case StateGas:
		return "stateGas"
	default:
		panic(fmt.Errorf("unknown gas type: %d", t))
	}
}

// SplitTxnGasLimit splits a transaction's gas limit into execution and state dimensions.
// EIP-8037: when tx.gas > TX_MAX_GAS_LIMIT, excess gas beyond the execution budget
// becomes the state gas reservoir, which state-creation opcodes (SSTORE, CREATE,
// code deposit) draw from before spilling to execution gas.
// Pre-Amsterdam: all gas is execution gas (state reservoir is 0).
// See process_transaction in EIP-8037.
func SplitTxnGasLimit(txnGasLimit, intrinsicGas uint64, rules *chain.Rules) MdGas {
	if rules.IsAmsterdam {
		// evm_gas = tx.gas - intrinsic_gas
		// execution_gas_budget = TX_MAX_GAS_LIMIT - intrinsic_gas
		// gas_left = min(execution_gas_budget, evm_gas)
		// state_gas_reservoir = evm_gas - gas_left
		evmGas := txnGasLimit - intrinsicGas
		executionGasBudget := params.MaxTxnGasLimit - intrinsicGas
		gasLeft := min(executionGasBudget, evmGas)
		stateGasReservoir := evmGas - gasLeft
		return MdGas{Execution: gasLeft, State: stateGasReservoir}
	}
	return MdGas{Execution: txnGasLimit - intrinsicGas}
}
