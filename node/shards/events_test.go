// Copyright 2024 The Erigon Authors
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

package shards

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/types"
)

func TestRecentReceipts(t *testing.T) {
	t.Parallel()
	t.Run("Evict", func(t *testing.T) {
		e := NewRecentReceipts(3)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(1)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(11)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(21)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 3)

		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(31)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 1)
	})
	t.Run("Nil", func(t *testing.T) {
		e := NewRecentReceipts(3)
		e.Add(types.Receipts{nil, {BlockNumber: uint256.NewInt(1)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(21)}, nil}, []types.Transaction{}, nil)
		e.Add(types.Receipts{nil, nil, {BlockNumber: uint256.NewInt(31)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 3)
	})
	t.Run("Order", func(t *testing.T) {
		e := NewRecentReceipts(3)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(1)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(11)}}, []types.Transaction{}, nil)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(1)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 2)
		e.Add(types.Receipts{{BlockNumber: uint256.NewInt(11)}}, []types.Transaction{}, nil)
		require.Len(t, e.receipts, 2)
	})
}
