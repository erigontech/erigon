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
)

// GasPool tracks block-level gas availability across the two EIP-8037 dimensions.
//
// Each field is the remaining budget for its dimension, starting at the block
// gas limit and decremented as transactions execute. A transaction is
// includable iff its EIP-8037 contribution fits in the remaining budget:
// min(TX_MAX_GAS_LIMIT, tx.gas) for regular and tx.gas for state. Pre-Amsterdam
// only the regular dimension is exercised.
type GasPool struct {
	mu         sync.RWMutex
	regularGas uint64
	stateGas   uint64
	blobGas    uint64
}

// NewGasPool constructs a pool with the given block gas limit and blob budget.
// Both regular and state dimensions start at gasLimit.
func NewGasPool(gasLimit, blobGas uint64) *GasPool {
	return &GasPool{regularGas: gasLimit, stateGas: gasLimit, blobGas: blobGas}
}

// Reset reinitialises the pool. Both gas dimensions return to gasLimit.
func (gp *GasPool) Reset(gasLimit, blobGas uint64) {
	if gp == nil {
		return
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()
	gp.regularGas = gasLimit
	gp.stateGas = gasLimit
	gp.blobGas = blobGas
}

// RegularGasAvailable returns the remaining regular-dimension gas.
func (gp *GasPool) RegularGasAvailable() uint64 {
	if gp == nil {
		return 0
	}
	gp.mu.RLock()
	defer gp.mu.RUnlock()
	return gp.regularGas
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

// ConsumeRegular deducts amount from the regular dimension, failing if the
// remainder would go negative.
func (gp *GasPool) ConsumeRegular(amount uint64) error {
	if gp == nil {
		return nil
	}
	gp.mu.Lock()
	defer gp.mu.Unlock()
	if gp.regularGas < amount {
		return ErrGasLimitReached
	}
	gp.regularGas -= amount
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
	if gp.regularGas > math.MaxUint64-amount {
		panic("gas pool pushed above uint64")
	}
	gp.regularGas += amount
	gp.stateGas += amount
	return gp
}

// SubGas is a legacy alias for ConsumeRegular.
func (gp *GasPool) SubGas(amount uint64) error {
	return gp.ConsumeRegular(amount)
}

// Gas is a legacy alias for RegularGasAvailable.
func (gp *GasPool) Gas() uint64 {
	return gp.RegularGasAvailable()
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
		return "regularGas: 0, stateGas: 0, blobGas: 0"
	}
	gp.mu.RLock()
	defer gp.mu.RUnlock()
	return fmt.Sprintf("regularGas: %d, stateGas: %d, blobGas: %d", gp.regularGas, gp.stateGas, gp.blobGas)
}
