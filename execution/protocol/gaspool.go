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

package protocol

import (
	"fmt"
	"math"
	"sync"

	"github.com/erigontech/erigon/execution/protocol/params"
)

// GasPool tracks block-level gas availability across the two EIP-8037
// dimensions, plus the EIP-4844 blob-gas budget.
//
// Each field is the remaining budget for its dimension, decremented as
// transactions execute: execution and state start at the block gas limit, blob
// at the block blob-gas budget. A transaction is includable iff its EIP-8037
// contribution fits in the remaining budget: min(TX_MAX_GAS_LIMIT, tx.gas) for
// execution and tx.gas for state. Pre-Amsterdam only the execution dimension is
// exercised.
type GasPool struct {
	mu           sync.RWMutex
	executionGas uint64
	stateGas     uint64
	blobGas      uint64
}

// NewGasPool constructs a pool with the given block gas limit and blob budget.
// Both execution and state dimensions start at gasLimit.
func NewGasPool(gasLimit, blobGas uint64) *GasPool {
	return &GasPool{executionGas: gasLimit, stateGas: gasLimit, blobGas: blobGas}
}

func NewBlockGasPool(executionGas, stateGas, blobGas uint64) *GasPool {
	return &GasPool{executionGas: executionGas, stateGas: stateGas, blobGas: blobGas}
}

// Reset reinitialises the pool: both gas dimensions return to gasLimit and
// the blob budget to blobGas.
func (gp *GasPool) Reset(gasLimit, blobGas uint64) {
	if gp == nil {
		return
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()
	gp.executionGas = gasLimit
	gp.stateGas = gasLimit
	gp.blobGas = blobGas
}

// ExecutionGasAvailable returns the remaining execution-dimension gas.
func (gp *GasPool) ExecutionGasAvailable() uint64 {
	if gp == nil {
		return 0
	}
	gp.mu.RLock()
	defer gp.mu.RUnlock()
	return gp.executionGas
}

// StateGasAvailable returns the remaining state-dimension gas. Pre-Amsterdam
// blocks never consume from this side, so it stays at the block's gasLimit.
func (gp *GasPool) StateGasAvailable() uint64 {
	if gp == nil {
		return 0
	}
	gp.mu.RLock()
	defer gp.mu.RUnlock()
	return gp.stateGas
}

// ConsumeExecution deducts amount from the execution dimension, failing if the
// remainder would go negative.
func (gp *GasPool) ConsumeExecution(amount uint64) error {
	if gp == nil {
		return nil
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()
	if gp.executionGas < amount {
		return ErrGasLimitReached
	}
	gp.executionGas -= amount
	return nil
}

// ConsumeState deducts amount from the state dimension, failing if the
// remainder would go negative. Only used post-Amsterdam.
func (gp *GasPool) ConsumeState(amount uint64) error {
	if gp == nil {
		return nil
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()
	if gp.stateGas < amount {
		return ErrGasLimitReached
	}
	gp.stateGas -= amount
	return nil
}

// AddGas extends both dimensions by amount. Kept for the RPC chained
// construction idiom: `new(GasPool).AddGas(N).AddBlobGas(M)`. Pre-Amsterdam
// stateGas is never consumed, so seeding it has no observable effect there.
func (gp *GasPool) AddGas(amount uint64) *GasPool {
	if gp == nil {
		return gp
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()
	if gp.executionGas > math.MaxUint64-amount {
		panic("gas pool pushed above uint64")
	}
	gp.executionGas += amount
	gp.stateGas += amount
	return gp
}

// SubGas is a legacy alias for ConsumeExecution.
func (gp *GasPool) SubGas(amount uint64) error {
	return gp.ConsumeExecution(amount)
}

// Gas is a legacy alias for ExecutionGasAvailable.
func (gp *GasPool) Gas() uint64 {
	return gp.ExecutionGasAvailable()
}

// AddBlobGas extends the blob-gas budget.
func (gp *GasPool) AddBlobGas(amount uint64) *GasPool {
	if gp == nil {
		return gp
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()
	if gp.blobGas > math.MaxUint64-amount {
		panic("blob gas pool pushed above uint64")
	}
	gp.blobGas += amount
	return gp
}

// SubBlobGas deducts amount from the blob budget.
func (gp *GasPool) SubBlobGas(amount uint64) error {
	if gp == nil {
		return nil
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()
	if gp.blobGas < amount {
		return ErrBlobGasLimitReached
	}
	gp.blobGas -= amount
	return nil
}

// CheckBlockGasInclusion verifies that the supplied gas fits in the block's
// remaining budgets: execution and state in the EIP-8037 reservoirs, and blob in
// the EIP-4844 blob-gas pool. Callers obtain the execution and state contributions
// from InclusionContributions; blobGas is the transaction's blob gas.
func CheckBlockGasInclusion(gp *GasPool, executionGas, stateGas, blobGas uint64) error {
	if gp == nil {
		return nil
	}
	gp.mu.RLock()
	defer gp.mu.RUnlock()
	if executionGas > gp.executionGas {
		return ErrGasLimitReached
	}
	if stateGas > gp.stateGas {
		return ErrGasLimitReached
	}
	if blobGas > gp.blobGas {
		return ErrBlobGasLimitReached
	}
	return nil
}

// InclusionContributions returns the per-dimension gas the EIP-8037 block-pool
// inclusion check must reserve for a tx with the given declared gas_limit.
// Callers feed the result to CheckBlockGasInclusion.
//
// Pre-Amsterdam: only the execution dimension is exercised; state is 0.
// Amsterdam onwards, the full gas_limit must fit in each remaining reservoir
// (EIP-8037 check_transaction), with the execution side bounded by the EIP-7825
// per-tx gas cap:
//
//	execution = min(MaxTxnGasLimit, tx.gas)
//	state     = tx.gas
func InclusionContributions(gas uint64, isAmsterdam bool) (uint64, uint64) {
	if !isAmsterdam {
		return gas, 0
	}
	return min(params.MaxTxnGasLimit, gas), gas
}

// BlobGas returns the blob gas remaining.
func (gp *GasPool) BlobGas() uint64 {
	if gp == nil {
		return 0
	}
	gp.mu.RLock()
	defer gp.mu.RUnlock()
	return gp.blobGas
}

func (gp *GasPool) String() string {
	if gp == nil {
		return "executionGas: 0, stateGas: 0, blobGas: 0"
	}
	gp.mu.RLock()
	defer gp.mu.RUnlock()
	return fmt.Sprintf("executionGas: %d, stateGas: %d, blobGas: %d", gp.executionGas, gp.stateGas, gp.blobGas)
}
