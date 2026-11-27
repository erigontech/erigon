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

package bal_test

import (
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/execution/bal"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestEIP8253BuildImportAndRegenerateBAL(t *testing.T) {
	t.Parallel()
	target := common.HexToAddress("0x8253")
	config := chain.AllProtocolChanges.Copy()
	config.BogotaTime = common.NewUint64(20)
	config.ZeroNonceStorageAccounts = []common.Address{target}
	m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(&types.Genesis{
		Config: config,
		Alloc: types.GenesisAlloc{
			target: {Balance: big.NewInt(100), Storage: map[common.Hash]common.Hash{{}: common.HexToHash("0x42")}},
		},
	}))
	blocks, err := m.GenerateChain(3, func(i int, b *blockgen.BlockGen) {
		wantNonce := uint64(0)
		if i >= 1 {
			wantNonce = 1
		}
		require.Equal(t, wantNonce, b.TxNonce(target))
	})
	require.NoError(t, err)
	require.Equal(t, uint64(20), blocks.Blocks[1].Time())
	require.NoError(t, m.InsertChain(blocks))
	for i, block := range blocks.Blocks {
		found := false
		for _, changes := range block.BlockAccessList() {
			if changes.Address != target {
				continue
			}
			found = true
			require.Equal(t, []*types.NonceChange{{Index: 0, Value: 1}}, changes.NonceChanges)
			require.Empty(t, changes.BalanceChanges)
			require.Empty(t, changes.CodeChanges)
			require.Empty(t, changes.StorageChanges)
			require.Empty(t, changes.StorageReads)
		}
		require.Equal(t, i == 1, found)
		require.Zero(t, block.GasUsed())
		require.Empty(t, block.Transactions())
		require.Empty(t, blocks.Receipts[i])
	}
	tx, err := m.DB.BeginTemporalRo(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	regenerator := bal.NewRegenerator(m.BlockReader, m.Engine, m.Log)
	for _, block := range blocks.Blocks {
		expected, err := types.EncodeBlockAccessListBytes(block.BlockAccessList())
		require.NoError(t, err)
		actual, err := regenerator.GetBlockAccessListBytes(t.Context(), config, tx, block.Hash(), block.NumberU64(), nil)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
	}
}
