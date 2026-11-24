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
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
)

func TestLatestViewPoolFollowsNewHead(t *testing.T) {
	t.Parallel()

	m := execmoduletester.New(t, execmoduletester.WithChainConfig(chain.TestChainBerlinConfig))
	signer := *types.LatestSignerForChainID(m.ChainConfig.ChainID)
	to := common.Address{1}
	chainPack, err := m.GenerateChain(3, func(i int, gen *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(gen.TxNonce(m.Address), to, uint256.NewInt(1), 21000, uint256.NewInt(common.GWei), nil), signer, m.Key)
		require.NoError(t, err)
		gen.AddTx(txn)
	})
	require.NoError(t, err)
	api := newEthApiForTest(newBaseApiForTest(m), m.DB, nil, nil)
	ctx := t.Context()
	latest := rpc.BlockNumberOrHashWithNumber(rpc.LatestBlockNumber)

	for n := 1; n <= 3; n++ {
		require.NoError(t, m.InsertChain(chainPack.Slice(n-1, n)))
		for range 2 {
			balance, err := api.GetBalance(ctx, to, &latest)
			require.NoError(t, err)
			require.Equal(t, uint64(n), balance.ToInt().Uint64(), "balance after block %d", n)
			nonce, err := api.GetTransactionCount(ctx, m.Address, &latest)
			require.NoError(t, err)
			require.Equal(t, uint64(n), uint64(*nonce), "nonce after block %d", n)
		}
	}
}
