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
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// Pages run backwards from the end of the block, and the transactions and the
// receipts are cropped separately, so the two can drift apart.
func TestOtsGetBlockTransactionsPaging(t *testing.T) {
	m, chain, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)

	for _, block := range chain.Blocks {
		number := rpc.BlockNumber(block.NumberU64())
		full, err := api.GetBlockTransactions(m.Ctx, number, 0, 255)
		require.NoError(t, err)
		require.NotNil(t, full)
		wantTxs, wantReceipts := blockTransactionHashes(t, full)
		require.Len(t, wantTxs, block.Transactions().Len())

		for _, pageSize := range []uint8{1, 2} {
			var gotTxs, gotReceipts []common.Hash
			pages := (len(wantTxs) + int(pageSize) - 1) / int(pageSize)
			for page := pages - 1; page >= 0; page-- {
				res, err := api.GetBlockTransactions(m.Ctx, number, uint8(page), pageSize)
				require.NoError(t, err)
				txs, receipts := blockTransactionHashes(t, res)
				gotTxs = append(gotTxs, txs...)
				gotReceipts = append(gotReceipts, receipts...)
			}
			assert.Equal(t, wantTxs, gotTxs, "block %d, page size %d", block.NumberU64(), pageSize)
			assert.Equal(t, wantReceipts, gotReceipts, "block %d, page size %d", block.NumberU64(), pageSize)
		}
	}
}

// Each page carries the transactions eth_getBlockByNumber returns for the same
// positions, with the input cut to the 4-byte selector.
func TestOtsGetBlockTransactionsPageContent(t *testing.T) {
	m, chain, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)

	for _, block := range chain.Blocks {
		want := ethapi.RPCMarshalBlock(block, true, true).Transactions.([]*ethapi.RPCTransaction)
		for _, rpcTx := range want {
			if len(rpcTx.Input) >= 4 {
				rpcTx.Input = rpcTx.Input[:4]
			}
		}
		for page := range want {
			res, err := api.GetBlockTransactions(m.Ctx, rpc.BlockNumber(block.NumberU64()), uint8(page), 1)
			require.NoError(t, err)
			fullblock := res["fullblock"].(*ethapi.RPCBlock)
			i := len(want) - 1 - page
			wantJSON, err := json.Marshal(want[i : i+1])
			require.NoError(t, err)
			gotJSON, err := json.Marshal(fullblock.Transactions)
			require.NoError(t, err)
			assert.JSONEq(t, string(wantJSON), string(gotJSON), "block %d, page %d", block.NumberU64(), page)
			assert.Equal(t, uint64(len(want)), *fullblock.TransactionCount, "block %d", block.NumberU64())
		}
	}
}

func blockTransactionHashes(t *testing.T, res map[string]any) (txs, receipts []common.Hash) {
	t.Helper()

	fullblock, ok := res["fullblock"].(*ethapi.RPCBlock)
	require.True(t, ok, "unexpected fullblock type %T", res["fullblock"])
	for _, rpcTx := range fullblock.Transactions.([]*ethapi.RPCTransaction) {
		txs = append(txs, rpcTx.Hash)
	}
	for _, receipt := range res["receipts"].([]*ethutils.RPCReceipt) {
		receipts = append(receipts, receipt.TransactionHash)
	}
	return txs, receipts
}
