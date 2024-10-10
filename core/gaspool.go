// Copyright 2015 The go-ethereum Authors
// This file is part of the go-ethereum library.
//
// The go-ethereum library is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// The go-ethereum library is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with the go-ethereum library. If not, see <http://www.gnu.org/licenses/>.

package core

import (
	"fmt"
	"math"
)

// GasPool tracks the amount of gas available during execution of the transactions
// in a block. The zero value is a pool with zero gas available.
type GasPool struct {
	gas, blobGas uint64
}

func (gp *GasPool) Reset(amount uint64) {
	gp.gas = amount
}

// AddGas makes gas available for execution.
func (gp *GasPool) AddGas(amount uint64) *GasPool {
	if gp.gas > math.MaxUint64-amount {
		panic("gas pool pushed above uint64")
	}
	gp.gas += amount
	return gp
}

// SubGas deducts the given amount from the pool if enough gas is
// available and returns an error otherwise.
func (gp *GasPool) SubGas(amount uint64) error {
	if gp.gas < amount {
		return ErrGasLimitReached
	}
	gp.gas -= amount
	return nil
}

// Gas returns the amount of gas remaining in the pool.
func (gp *GasPool) Gas() uint64 {
	return gp.gas
}

// AddBlobGas makes blob gas available for execution.
func (gp *GasPool) AddBlobGas(amount uint64) *GasPool {
	if gp.blobGas > math.MaxUint64-amount {
		panic("blob gas pool pushed above uint64")
	}
	gp.blobGas += amount
	return gp
}

// SubBlobGas deducts the given amount from the pool if enough blob gas is available and returns an
// error otherwise.
func (gp *GasPool) SubBlobGas(amount uint64) error {
	if gp.blobGas < amount {
		return ErrBlobGasLimitReached
	}
	gp.blobGas -= amount
	return nil
}

// BlobGas returns the amount of blob gas remaining in the pool.
func (gp *GasPool) BlobGas() uint64 {
	return gp.blobGas
}

func (gp *GasPool) String() string {
	return fmt.Sprintf("gas: %d, blob_gas: %d", gp.gas, gp.blobGas)
}
