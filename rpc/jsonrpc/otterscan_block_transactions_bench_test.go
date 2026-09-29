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
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/protocol/params"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

// BenchmarkOtsGetBlockTransactionsPage reads one small page of a block with
// many transactions, so its cost shows whether work scales with the page or
// with the block.
func BenchmarkOtsGetBlockTransactionsPage(b *testing.B) {
	const txCount = 150
	m := execmoduletester.New(b)
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	chain, err := m.GenerateChain(1, func(i int, gen *blockgen.BlockGen) {
		for nonce := range uint64(txCount) {
			txn, err := types.SignTx(types.NewTransaction(nonce, common.Address{1}, uint256.NewInt(1), params.TxGas, uint256.NewInt(10*common.GWei), nil), signer, m.Key)
			require.NoError(b, err)
			gen.AddTx(txn)
		}
	})
	require.NoError(b, err)
	require.NoError(b, m.InsertChain(chain))
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)

	b.ReportAllocs()
	for b.Loop() {
		res, err := api.GetBlockTransactions(m.Ctx, rpc.BlockNumber(1), 0, 25)
		require.NoError(b, err)
		require.Len(b, res["receipts"], 25)
	}
}
