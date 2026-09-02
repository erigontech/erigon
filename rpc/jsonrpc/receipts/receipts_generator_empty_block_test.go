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

package receipts_test

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// TestGetReceiptsCachesAnEmptyBlock pins that a block with no transactions is cached
// like any other: it has nothing to derive, so repeating the request must not keep
// paying the execution mutex and the execution semaphore to find that out again.
func TestGetReceiptsCachesAnEmptyBlock(t *testing.T) {
	m := mockWithGenerator(t, 1, func(i int, block *blockgen.BlockGen) {})

	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()

	block, err := m.BlockReader.BlockByNumber(m.Ctx, tx, 1)
	require.NoError(t, err)
	require.Empty(t, block.Transactions(), "this test needs a block without transactions")

	gen := m.ReceiptsReader
	got, err := gen.GetReceipts(m.Ctx, m.ChainConfig, tx, block, eth.ReceiptsOpts{})
	require.NoError(t, err)
	require.Empty(t, got)

	cached, ok := gen.GetCachedReceipts(m.Ctx, block.Hash())
	require.True(t, ok, "the next request must be answered by the cache")
	require.Empty(t, cached)
}
