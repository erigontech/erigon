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
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Lesser
// General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync_test

import (
	"context"
	"math/big"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/diagnostics/metrics"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/tests/blockgen"
	"github.com/erigontech/erigon/execution/types"
)

func TestReadAheadInitialExecMatchesNormalExecution(t *testing.T) {
	oldInitial, oldExec, oldCommitment, oldWorkers, oldDistance := dbg.ReadAheadInitial, dbg.ReadAheadExec, dbg.ReadAheadExecCommitment, dbg.ReadAheadBlocksWorkers, dbg.ReadAheadDistance
	t.Cleanup(func() {
		dbg.ReadAheadInitial = oldInitial
		dbg.ReadAheadExec = oldExec
		dbg.ReadAheadExecCommitment = oldCommitment
		dbg.ReadAheadBlocksWorkers = oldWorkers
		dbg.ReadAheadDistance = oldDistance
	})
	blocks := metrics.GetOrCreateCounterVec("read_ahead_exec_blocks_total", []string{"result"})
	results := func() (float64, float64) {
		return blocks.WithLabelValues("done").GetValue(), blocks.WithLabelValues("late").GetValue()
	}
	chainPack := func(t *testing.T) (*types.Block, types.Receipts) {
		t.Helper()
		key, err := crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f491")
		require.NoError(t, err)
		from := crypto.PubkeyToAddress(key.PublicKey)
		contract := common.Address{19: 0xc0}
		code := []byte{0x60, 0, 0x54, 0x50, 0x60, 9, 0x60, 1, 0x55, 0}
		genesis := &types.Genesis{
			Config:     chain.TestChainBerlinConfig,
			Difficulty: uint256.NewInt(0),
			Alloc: types.GenesisAlloc{
				from:     {Balance: new(big.Int).Mul(big.NewInt(100), big.NewInt(1e18))},
				contract: {Code: code},
			},
		}
		m := execmoduletester.New(t, execmoduletester.WithGenesisSpec(genesis), execmoduletester.WithKey(key))
		t.Cleanup(m.Close)
		chainPack, err := m.GenerateChain(17, func(_ int, gen *blockgen.BlockGen) {
			txn, signErr := types.SignTx(types.NewTransaction(gen.TxNonce(from), contract, uint256.NewInt(0), 100_000, uint256.NewInt(1), nil), *types.LatestSignerForChainID(genesis.Config.ChainID), key)
			require.NoError(t, signErr)
			gen.AddTx(txn)
		})
		require.NoError(t, err)
		execmoduletester.InsertUnexecutedCanonical(t, m, chainPack.Blocks)
		applyTx, err := m.DB.BeginTemporalRw(context.Background())
		require.NoError(t, err)
		doms, err := execctx.NewSharedDomains(context.Background(), applyTx, m.Log)
		require.NoError(t, err)
		_, err = m.Sync.Run(doms, applyTx, true, true)
		require.NoError(t, err)
		doms.Close()
		require.NoError(t, applyTx.Commit())
		tx, err := m.DB.BeginTemporalRo(context.Background())
		require.NoError(t, err)
		defer tx.Rollback()
		current := m.Current(tx)
		require.NotNil(t, current)
		var receipts types.Receipts
		for _, block := range chainPack.Blocks {
			stored, err := m.BlockReader.BlockByNumber(context.Background(), tx, block.NumberU64())
			require.NoError(t, err)
			require.NotNil(t, stored)
			got, err := rawdb.ReadReceiptsCacheV2(tx, stored, m.BlockReader.TxnumReader())
			require.NoError(t, err)
			receipts = append(receipts, got...)
		}
		return current, receipts
	}

	dbg.ReadAheadInitial, dbg.ReadAheadExec, dbg.ReadAheadExecCommitment, dbg.ReadAheadBlocksWorkers, dbg.ReadAheadDistance = false, false, false, 2, 500
	defaultRoot, defaultReceipts := chainPack(t)
	dbg.ReadAheadInitial, dbg.ReadAheadExec, dbg.ReadAheadExecCommitment, dbg.ReadAheadBlocksWorkers, dbg.ReadAheadDistance = true, true, false, 2, 1
	beforeDone, beforeLate := results()
	prefetchedRoot, prefetchedReceipts := chainPack(t)
	afterDone, afterLate := results()
	require.Equal(t, defaultRoot.Root(), prefetchedRoot.Root())
	require.Equal(t, defaultReceipts, prefetchedReceipts)
	require.Greater(t, afterDone+afterLate, beforeDone+beforeLate, "initial-cycle read-ahead must reach the speculative execution branch")
}
