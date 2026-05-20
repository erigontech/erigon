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

	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/protocol/mdgas"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
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

func NewBlockGasPool(regularGas, stateGas, blobGas uint64) *GasPool {
	return &GasPool{regularGas: regularGas, stateGas: stateGas, blobGas: blobGas}
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

// CheckBlockGasInclusion verifies that the supplied per-dimension gas
// values fit in the remaining EIP-8037 reservoirs. Callers compute the
// dimension contributions via InclusionContributions for pre-execution
// inclusion checks.
func CheckBlockGasInclusion(gp *GasPool, regularGas, stateGas uint64) error {
	if gp == nil {
		return nil
	}
	if regularGas > gp.RegularGasAvailable() {
		return ErrGasLimitReached
	}
	if stateGas > gp.StateGasAvailable() {
		return ErrGasLimitReached
	}
	return nil
}

// InclusionContributions returns the per-dimension gas contributions for
// the EIP-8037 block-pool inclusion check. Use this from call sites that
// don't already have the intrinsic gas computed; otherwise prefer
// InclusionContributionsWithIgas to avoid the second IntrinsicGas pass.
// Returns ErrGasUintOverflow if intrinsic gas computation overflows uint64.
func InclusionContributions(txn types.Transaction, rules *chain.Rules) (uint64, uint64, error) {
	if txn == nil {
		return 0, 0, nil
	}
	gas := txn.GetGasLimit()
	if !rules.IsAmsterdam {
		return gas, 0, nil
	}

	accessList := txn.GetAccessList()
	intrinsic, overflow := mdgas.IntrinsicGas(mdgas.IntrinsicGasCalcArgs{
		Data:               txn.GetData(),
		AuthorizationsLen:  uint64(len(txn.GetAuthorizations())),
		AccessListLen:      uint64(len(accessList)),
		StorageKeysLen:     uint64(accessList.StorageKeys()),
		IsContractCreation: txn.GetTo() == nil,
		IsEIP2:             rules.IsHomestead,
		IsEIP2028:          rules.IsIstanbul,
		IsEIP3860:          rules.IsShanghai,
		IsEIP7623:          rules.IsPrague,
		IsEIP7976:          rules.IsAmsterdam,
		IsEIP7981:          rules.IsAmsterdam,
		IsEIP8037:          rules.IsAmsterdam,
	})
	if overflow {
		return 0, 0, ErrGasUintOverflow
	}
	regular, state := InclusionContributionsWithIgas(gas, intrinsic, rules.IsAmsterdam)
	return regular, state, nil
}

// InclusionContributionsWithIgas returns the per-dimension gas contributions
// for the EIP-8037 block-pool inclusion check, given the tx's declared
// gas_limit and the precomputed intrinsic gas result.
//
// Pre-Amsterdam: only the regular dimension is exercised; state is 0.
// Amsterdam onwards (EIP-8037), per the spec the inclusion check is:
//
//	regular_contribution = min(MaxTxnGasLimit, tx.gas - intrinsic.state)
//	state_contribution   = tx.gas - intrinsic.regular
//
// Subtracting the matching intrinsic dimension from tx.gas before checking
// against the opposite reservoir is what lets a tx with a high gas_limit
// (e.g. a creation paying intrinsic.state up front, or an EIP-7702 tx
// paying per-auth state cost) still be includable when the un-subtracted
// gas_limit would over-budget the opposite dimension.
func InclusionContributionsWithIgas(gas uint64, intrinsic mdgas.IntrinsicGasCalcResult, isAmsterdam bool) (uint64, uint64) {
	if !isAmsterdam {
		return gas, 0
	}
	var regularContribution, stateContribution uint64
	if gas >= intrinsic.StateGas {
		regularContribution = gas - intrinsic.StateGas
	}
	if regularContribution > params.MaxTxnGasLimit {
		regularContribution = params.MaxTxnGasLimit
	}
	if gas >= intrinsic.RegularGas {
		stateContribution = gas - intrinsic.RegularGas
	}
	return regularContribution, stateContribution
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
