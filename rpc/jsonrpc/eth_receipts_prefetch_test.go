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

package jsonrpc

import (
	"slices"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p/protocols/eth"
)

// TestPrefetchReceiptsServesAnOverlayBlock pins that the prefetch tx reads the block overlay as the
// request tx does: the receipts of a block executed but not yet committed are only there.
func TestPrefetchReceiptsServesAnOverlayBlock(t *testing.T) {
	defer func(prev bool) { dbg.AssertEnabled = prev }(dbg.AssertEnabled)
	dbg.AssertEnabled = false // assertions re-execute instead of serving the persistent cache

	h := newOverlayAheadHarnessWithTips(t, slices.Repeat([]uint64{overlayRaceLowTip}, 16))
	num, hash := h.overlayHeader.Number.Uint64(), h.overlayHeader.Hash()

	tx, err := h.m.DB.BeginTemporalRo(h.m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	minTxNum, err := h.m.BlockReader.TxnumReader().Min(h.m.Ctx, tx, num)
	require.NoError(t, err)
	maxTxNum := minTxNum + 1 + uint64(len(h.overlayBody.Transactions))
	require.NoError(t, rawdbv3.TxNums.Append(h.doms.BlockOverlay(), num, maxTxNum))

	// As execution does: the system txs at both ends of the block store an empty receipt.
	putDel := h.doms.AsPutDel(tx)
	require.NoError(t, rawdb.WriteReceiptCacheV2(putDel, nil, minTxNum))
	for i, txn := range h.overlayBody.Transactions {
		receipt := &types.Receipt{
			Type:              txn.Type(),
			Status:            types.ReceiptStatusSuccessful,
			CumulativeGasUsed: uint64(i+1) * params.TxGas,
			GasUsed:           params.TxGas,
			TransactionIndex:  uint(i),
		}
		require.NoError(t, rawdb.WriteReceiptCacheV2(putDel, receipt, minTxNum+1+uint64(i)))
	}
	require.NoError(t, rawdb.WriteReceiptCacheV2(putDel, nil, maxTxNum))

	block := types.NewBlockFromStorage(hash, h.overlayHeader, h.overlayBody.Transactions, nil, nil, nil)
	want, err := rawdb.ReadReceiptsCacheV2(h.base.filters.WithTemporalOverlay(tx), block, h.m.BlockReader.TxnumReader())
	require.NoError(t, err)
	require.Len(t, want, len(h.overlayBody.Transactions), "fixture: the request tx must serve the receipts through the overlay")

	p := h.base.receiptsGenerator.PrefetchReceipts(h.m.Ctx, h.m.ChainConfig, h.m.DB, tx, num, hash, eth.ReceiptsOpts{})
	defer p.Close()
	got := p.Receipts(block)
	require.Equal(t, want, got)
}
