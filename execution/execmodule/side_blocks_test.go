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

package execmodule_test

import (
	"context"
	"crypto/ecdsa"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/execmodule/execmoduletester"
	"github.com/erigontech/erigon/execution/types"
)

func TestForkChoiceLeavesSideBlocksOutOfDB(t *testing.T) {
	for _, mode := range []struct {
		name  string
		merge bool
	}{{"merged", true}, {"executed", false}} {
		t.Run(mode.name, func(t *testing.T) {
			m, key, _ := newMetricsTester(t)
			side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
			winner := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
			insertAndValidateBlocks(t, m, side, winner)
			if !mode.merge {
				m.ForkValidator.ClearWithUnwind()
			}
			updateForkChoiceTo(t, m, winner)

			require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
				require.NotNil(t, rawdb.ReadHeaderNumber(tx, winner.Hash()))
				require.Nil(t, rawdb.ReadHeaderNumber(tx, side.Hash()))
				return nil
			}))
		})
	}
}

func TestForkChoiceReorgsToRetainedSideBlock(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	winner := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
	insertAndValidateBlocks(t, m, side, winner)
	updateForkChoiceTo(t, m, winner)

	hash, number := side.Hash(), side.NumberU64()
	header, err := m.ExecModule.GetHeader(t.Context(), &hash, &number)
	require.NoError(t, err)
	require.NotNil(t, header)
	has, err := m.ExecModule.HasBlock(t.Context(), &hash, nil)
	require.NoError(t, err)
	require.True(t, has)

	updateForkChoiceTo(t, m, side)
	requireCanonicalWithTransactions(t, m, side)
}

func TestValidateChildOfRetainedSideBlock(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	sideChain := generateTransferBlocks(t, m, key, 2, 0x0b)
	winner := generateTransferBlocks(t, m, key, 1, 0x0a)[0]
	insertAndValidateBlocks(t, m, sideChain[0], winner)
	updateForkChoiceTo(t, m, winner)

	insertAndValidateBlocks(t, m, sideChain[1])
	updateForkChoiceTo(t, m, sideChain[1])
	requireCanonicalWithTransactions(t, m, sideChain[0])
	requireCanonicalWithTransactions(t, m, sideChain[1])
}

func TestRetainedSideBlockTransactionsSurviveLaterCommits(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	canonical := generateTransferBlocks(t, m, key, 3, 0x0a)
	insertAndValidateBlocks(t, m, side, canonical[0])
	updateForkChoiceTo(t, m, canonical[0])
	for _, block := range canonical[1:] {
		insertAndValidateBlocks(t, m, block)
		updateForkChoiceTo(t, m, block)
	}

	updateForkChoiceTo(t, m, side)
	requireCanonicalWithTransactions(t, m, side)
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		for _, block := range canonical {
			requireStoredTransactions(t, tx, block)
		}
		return nil
	}))
}

func TestRetainedBlockAboveCommittedSequenceKeepsTransactions(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	chainA := generateTransferBlocks(t, m, key, 2, 0x0a)
	chainB := generateTransferBlocks(t, m, key, 2, 0x0b)
	insertAndValidateBlocks(t, m, chainA[0], chainB[0])
	updateForkChoiceTo(t, m, chainA[0])
	updateForkChoiceTo(t, m, chainB[0])
	insertAndValidateBlocks(t, m, chainB[1])
	updateForkChoiceTo(t, m, chainA[0])
	insertAndValidateBlocks(t, m, chainA[1])
	updateForkChoiceTo(t, m, chainB[1])

	requireCanonicalWithTransactions(t, m, chainB[0])
	requireCanonicalWithTransactions(t, m, chainB[1])
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		requireStoredTransactions(t, tx, chainA[0])
		return nil
	}))
}

func TestRetainedSideBlockVisibleThroughInsertsAndForkChoices(t *testing.T) {
	m, key, _ := newMetricsTester(t)
	m.ExecModule.SetPublishedSD(m.Notifications.Events.LatestSD)
	side := generateTransferBlocks(t, m, key, 1, 0x0b)[0]
	canonical := generateTransferBlocks(t, m, key, 32, 0x0a)
	insertAndValidateBlocks(t, m, side, canonical[0])
	updateForkChoiceTo(t, m, canonical[0])

	hash := side.Hash()
	var misses, failures atomic.Int64
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		for {
			select {
			case <-stop:
				return
			default:
			}
			has, err := m.ExecModule.HasBlock(context.Background(), &hash, nil)
			switch {
			case err != nil:
				failures.Add(1)
			case !has:
				misses.Add(1)
			}
		}
	}()
	for _, block := range canonical[1:] {
		insertAndValidateBlocks(t, m, block)
		updateForkChoiceTo(t, m, block)
	}
	close(stop)
	<-done
	require.Zero(t, failures.Load())
	require.Zero(t, misses.Load())
}

func generateTransferBlocks(t *testing.T, m *execmoduletester.ExecModuleTester, key *ecdsa.PrivateKey, n int, to byte) []*types.Block {
	t.Helper()
	chain, err := m.GenerateChain(n, sendTo(t, m, key, common.Address{to}, 1))
	require.NoError(t, err)
	return chain.Blocks
}

func insertAndValidateBlocks(t *testing.T, m *execmoduletester.ExecModuleTester, blocks ...*types.Block) {
	t.Helper()
	for _, block := range blocks {
		_, err := m.InsertBlocks(t.Context(), []*types.Block{block})
		require.NoError(t, err)
		result, err := m.ValidateChain(t.Context(), block.Header())
		require.NoError(t, err)
		require.Equal(t, execmodule.ExecutionStatusSuccess, result.ValidationStatus)
	}
}

func updateForkChoiceTo(t *testing.T, m *execmoduletester.ExecModuleTester, head *types.Block) {
	t.Helper()
	result, err := m.UpdateForkChoice(t.Context(), head.Header())
	require.NoError(t, err)
	require.Equal(t, execmodule.ExecutionStatusSuccess, result.Status)
	m.ExecModule.WaitIdle(t.Context())
}

func requireCanonicalWithTransactions(t *testing.T, m *execmoduletester.ExecModuleTester, block *types.Block) {
	t.Helper()
	require.NoError(t, m.DB.View(t.Context(), func(tx kv.Tx) error {
		hash, ok, err := m.BlockReader.CanonicalHash(t.Context(), tx, block.NumberU64())
		require.NoError(t, err)
		require.True(t, ok)
		require.Equal(t, block.Hash(), hash)
		requireStoredTransactions(t, tx, block)
		return nil
	}))
}

func requireStoredTransactions(t *testing.T, tx kv.Tx, block *types.Block) {
	t.Helper()
	body, err := rawdb.ReadBodyWithTransactions(tx, block.Hash(), block.NumberU64())
	require.NoError(t, err)
	require.NotNil(t, body)
	require.Len(t, body.Transactions, len(block.Transactions()))
	for i, txn := range body.Transactions {
		require.Equal(t, block.Transactions()[i].Hash(), txn.Hash())
	}
}
