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

package jsonrpc

import (
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/cmd/rpcdaemon/rpcdaemontest"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv/prune"
	"github.com/erigontech/erigon/execution/state"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestGetContractCreator(t *testing.T) {
	m, _, _ := rpcdaemontest.CreateTestExecModule(t)
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)

	addr := common.HexToAddress("0x537e697c7ab75a26f9ecf0ce810e3154dfcaaf44")
	expectCreator := common.HexToAddress("0x71562b71999873db5b286df957af199ec94617f7")
	expectCredByTx := common.HexToHash("0x6e25f89e24254ba3eb460291393a4715fd3c33d805334cbd05c1b2efe1080f18")
	t.Run("valid inputs", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, addr)
		require.NoError(err)
		require.Equal(expectCreator, results.Creator)
		require.Equal(expectCredByTx, results.Tx)
	})
	for _, tc := range []struct {
		name            string
		history, blocks prune.BlockAmount
	}{
		{"pruned history", prune.Distance(1), prune.ArchiveMode.Blocks},
		{"pruned transactions", prune.ArchiveMode.History, prune.Distance(1)},
	} {
		t.Run(tc.name, func(t *testing.T) {
			base := newBaseApiForTest(m)
			base._pruneMode.Store(&prune.Mode{Initialised: true, History: tc.history, Blocks: tc.blocks})
			api := NewOtterscanAPI(base, m.DB, 25)

			result, err := api.GetContractCreator(m.Ctx, addr)
			require.ErrorIs(t, err, state.ErrPruned)
			require.Nil(t, result)
		})
	}
	t.Run("not existing addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, common.HexToAddress("0x1234"))
		require.NoError(err)
		require.Nil(results)
	})
	t.Run("pass creator as addr", func(t *testing.T) {
		require := require.New(t)
		results, err := api.GetContractCreator(m.Ctx, expectCreator)
		require.NoError(err)
		require.Nil(results)
	})
}

func TestGetContractCreatorAtHistoryBoundary(t *testing.T) {
	signer := types.LatestSignerForChainID(nil)
	initCode := common.FromHex("0x60016000f3") // Return a one-byte STOP contract.
	var creations [3]types.Transaction
	m := mockWithGenerator(t, 2, func(i int, block *blockgen.BlockGen) {
		if i != 0 {
			return
		}
		for index := range creations {
			txn, err := types.SignTx(types.NewContractCreation(block.TxNonce(testAddr), uint256.NewInt(0), 100_000, uint256.NewInt(1), initCode), *signer, testKey)
			require.NoError(t, err)
			block.AddTx(txn)
			creations[index] = txn
		}
	})
	api := NewOtterscanAPI(newBaseApiForTest(m), m.DB, 25)
	tx, err := m.DB.BeginTemporalRo(m.Ctx)
	require.NoError(t, err)
	defer tx.Rollback()
	minTxNum, err := api._txNumReader.Min(m.Ctx, tx, 1)
	require.NoError(t, err)
	api.db = historyFloorDB{TemporalRoDB: m.DB, startTxNum: minTxNum + 3}

	creator, err := api.GetContractCreator(m.Ctx, types.CreateAddress(testAddr, 2))
	require.NoError(t, err, "the creation transaction's pre-state is retained")
	require.Equal(t, &ContractCreatorData{Creator: testAddr, Tx: creations[2].Hash()}, creator)

	creator, err = api.GetContractCreator(m.Ctx, types.CreateAddress(testAddr, 1))
	require.ErrorIs(t, err, state.ErrPruned, "the preceding creation's pre-state is pruned")
	require.Nil(t, creator)
}
