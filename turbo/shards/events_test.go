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
	"math/big"
	"testing"

	"github.com/erigontech/erigon/core/types"
	"github.com/stretchr/testify/require"
)

func TestRecentLogs(t *testing.T) {
	t.Parallel()
	t.Run("Evict", func(t *testing.T) {
		e := NewRecentLogs(3)
		e.Add(types.Receipts{{BlockNumber: big.NewInt(1)}})
		e.Add(types.Receipts{{BlockNumber: big.NewInt(11)}})
		e.Add(types.Receipts{{BlockNumber: big.NewInt(21)}})
		require.Equal(t, 3, len(e.receipts))

		e.Add(types.Receipts{{BlockNumber: big.NewInt(31)}})
		require.Equal(t, 1, len(e.receipts))
	})
	t.Run("Nil", func(t *testing.T) {
		e := NewRecentLogs(3)
		e.Add(types.Receipts{nil, {BlockNumber: big.NewInt(1)}})
		e.Add(types.Receipts{{BlockNumber: big.NewInt(21)}, nil})
		e.Add(types.Receipts{nil, nil, {BlockNumber: big.NewInt(31)}})
		require.Equal(t, 3, len(e.receipts))
	})
	t.Run("Order", func(t *testing.T) {
		e := NewRecentLogs(3)
		e.Add(types.Receipts{{BlockNumber: big.NewInt(1)}})
		e.Add(types.Receipts{{BlockNumber: big.NewInt(11)}})
		e.Add(types.Receipts{{BlockNumber: big.NewInt(1)}})
		require.Equal(t, 2, len(e.receipts))
		e.Add(types.Receipts{{BlockNumber: big.NewInt(11)}})
		require.Equal(t, 2, len(e.receipts))
	})
}
