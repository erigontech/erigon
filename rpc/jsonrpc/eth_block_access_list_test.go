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
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/ethapi"
)

// TestGetBlockAccessListRegeneratesPrunedBAL verifies that eth_getBlockAccessList
// serves BALs whose stored copy has been pruned (kept only for the reorg window)
// by regenerating them via re-execution.
func TestGetBlockAccessListRegeneratesPrunedBAL(t *testing.T) {
	t.Parallel()
	ctx := t.Context()
	privKey, err := crypto.GenerateKey()
	require.NoError(t, err)
	senderAddr := crypto.PubkeyToAddress(privKey.PublicKey)
	genesis := &types.Genesis{
		Config: chain.AllProtocolChanges,
		Alloc: types.GenesisAlloc{
			senderAddr: {Balance: new(big.Int).Exp(big.NewInt(10), big.NewInt(18), nil)},
		},
	}
	m := execmoduletester.New(
		t,
		execmoduletester.WithGenesisSpec(genesis),
		execmoduletester.WithKey(privKey),
		execmoduletester.WithAmsterdamBuilderContracts(),
	)
	signer := types.LatestSignerForChainID(m.ChainConfig.ChainID)
	baseFee := uint256.NewInt(m.Genesis.BaseFee().Uint64())
	chainPack, err := blockgen.GenerateChain(m.ChainConfig, m.Genesis, m.Engine, m.DB, 2, func(i int, b *blockgen.BlockGen) {
		txn, err := types.SignTx(types.NewTransaction(uint64(i), common.Address{1}, uint256.NewInt(10_000), 50_000, baseFee, nil), *signer, privKey)
		require.NoError(t, err)
		b.AddTx(txn)
	})
	require.NoError(t, err)
	err = m.InsertChain(chainPack)
	require.NoError(t, err)
	// Simulate the exec-stage prune: drop every stored BAL row.
	err = m.DB.Update(ctx, func(tx kv.RwTx) error {
		return tx.ForEach(kv.BlockAccessList, nil, func(k, _ []byte) error {
			return tx.Delete(kv.BlockAccessList, k)
		})
	})
	require.NoError(t, err)
	err = m.DB.View(ctx, func(tx kv.Tx) error {
		count, err := tx.Count(kv.BlockAccessList)
		require.NoError(t, err)
		require.Zero(t, count, "stored BALs should be fully pruned")
		return nil
	})
	require.NoError(t, err)
	api := NewEthAPI(newBaseApiForTest(m), m.DB, nil, nil, nil, &EthApiConfig{}, log.New())
	for i, block := range chainPack.Blocks {
		blockNum := rpc.BlockNumber(block.NumberU64())
		got, err := api.GetBlockAccessList(ctx, rpc.BlockNumberOrHash{BlockNumber: &blockNum})
		require.NoError(t, err, "block %d", block.NumberU64())
		canonical, err := types.DecodeBlockAccessListBytes(chainPack.BlockAccessLists[i])
		require.NoError(t, err)
		require.Equal(t, ethapi.MarshalBlockAccessList(canonical), got, "block %d", block.NumberU64())
	}
}
