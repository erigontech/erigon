// Copyright 2025 The Erigon Authors
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

package shutter

import (
	"context"

	"github.com/erigontech/erigon/core/types"
)

type DecryptedTxns struct {
	TotalGas     uint64
	Transactions []types.Transaction
}

type DecryptionMark struct {
	Slot uint64
	Eon  EonIndex
}

type DecryptedTxnsPool struct {
}

func (p DecryptedTxnsPool) Wait(ctx context.Context, mark DecryptionMark) error {
	//
	// TODO - return instantly if mark already present, otherwise use sync.Cond to wait
	//
	return nil
}

func (p DecryptedTxnsPool) DecryptedTxns(mark DecryptionMark) (DecryptedTxns, error) {
	return DecryptedTxns{}, nil
}

func (p DecryptedTxnsPool) AddDecryptedTxns(mark DecryptionMark, txns []types.Transaction) {
}

func (p DecryptedTxnsPool) Run(ctx context.Context) error {
	return nil
}
