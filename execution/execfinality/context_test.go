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

package execfinality

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbutils"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func newFinalityTestDB(t *testing.T) kv.TemporalRwDB {
	t.Helper()
	return temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
}

func TestIrreversibleBlock(t *testing.T) {
	for _, tc := range []struct {
		name              string
		headBlockNum      uint64
		finalisedBlockNum uint64
		maxReorgDepth     uint64
		initialCycle      bool
		want              uint64
	}{
		{name: "tip uses finality", headBlockNum: 1_000, finalisedBlockNum: 100, maxReorgDepth: 96, want: 99},
		{name: "initial cycle uses reorg depth", headBlockNum: 1_000, finalisedBlockNum: 100, maxReorgDepth: 96, initialCycle: true, want: 903},
		{name: "missing finality uses reorg depth", headBlockNum: 1_000, maxReorgDepth: 96, want: 903},
		{name: "head inside reorg window", headBlockNum: 96, maxReorgDepth: 96, want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			require.Equal(t, tc.want, IrreversibleBlock(tc.headBlockNum, tc.finalisedBlockNum, tc.maxReorgDepth, tc.initialCycle))
		})
	}
}

func TestResolveUsesTransactionVisibleExecutionProgress(t *testing.T) {
	db := newFinalityTestDB(t)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	finalisedHash := common.Hash{0x01}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 1_000))
	require.NoError(t, rawdb.WriteHeaderNumber(tx, finalisedHash, 100))
	rawdb.WriteForkchoiceFinalized(tx, finalisedHash)
	block, err := Resolve(tx, 96, true)
	require.NoError(t, err)
	require.Equal(t, uint64(903), block)
	block, err = Resolve(tx, 96, false)
	require.NoError(t, err)
	require.Equal(t, uint64(99), block)
}

func TestResolveWithoutFinalisedBlockUsesMaxReorgDepth(t *testing.T) {
	db := newFinalityTestDB(t)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	finalisedHash := common.Hash{0x01}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 100))
	require.NoError(t, rawdb.WriteHeaderNumber(tx, finalisedHash, 1_000))
	rawdb.WriteForkchoiceFinalized(tx, finalisedHash)
	block, err := Resolve(tx, 96, false, WithoutFinalisedBlock())
	require.NoError(t, err)
	require.Equal(t, uint64(3), block)
}

func TestIrreversibleTxNumStopsBelowABlockWithAChangeset(t *testing.T) {
	db := newFinalityTestDB(t)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	for blockNum := uint64(0); blockNum <= 20; blockNum++ {
		require.NoError(t, rawdbv3.TxNums.Append(tx, blockNum, blockNum))
	}
	txNum, err := IrreversibleTxNum(t.Context(), tx, rawdbv3.TxNums, 12)
	require.NoError(t, err)
	require.Equal(t, uint64(12), txNum, "no changesets: the irreversible block's last txnum")

	require.NoError(t, tx.Put(kv.ChangeSets3, dbutils.BlockBodyKey(8, common.Hash{8}), []byte{1}))
	txNum, err = IrreversibleTxNum(t.Context(), tx, rawdbv3.TxNums, 12)
	require.NoError(t, err)
	require.Equal(t, uint64(7), txNum, "a block with a changeset can still be unwound")
}

// snapshotTxNums resolves a txnum from a full-range table, the way the block-snapshot
// backed index does for blocks chaindata no longer covers.
type snapshotTxNums struct{ maxTxNumByBlock map[uint64]uint64 }

func (s snapshotTxNums) MaxTxNum(_ context.Context, _ kv.Tx, _ kv.Cursor, blockNum uint64) (uint64, bool, error) {
	txNum, ok := s.maxTxNumByBlock[blockNum]
	return txNum, ok, nil
}

func (snapshotTxNums) BlockNumber(context.Context, kv.Tx, uint64) (uint64, bool, error) {
	return 0, false, nil
}

// pruned chaindata: genesis plus the downloaded-blocks window, far above the head a
// node re-executing from scratch has reached.
func txNumWindowDB(t *testing.T) kv.TemporalRwDB {
	t.Helper()
	db := newFinalityTestDB(t)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		// Genesis spans two txnums, so its max is 1.
		if err := rawdbv3.TxNums.Append(tx, 0, 1); err != nil {
			return err
		}
		for i := range uint64(3) {
			if err := rawdbv3.TxNums.Append(tx, 25_472_999+i, 3_630_627_978+i); err != nil {
				return err
			}
		}
		return nil
	}))
	return db
}

// Blocks downloaded to tip with execution far behind: the irreversible block is below the
// table's floor. The snapshot-backed reader names its txnum; chaindata alone must not answer
// with the latest txnum, which would open collation for every step.
func TestIrreversibleTxNumBelowTheTxNumWindow(t *testing.T) {
	const (
		irreversibleBlock = uint64(20_000_000)
		irreversibleTxNum = uint64(2_400_000_000)
	)
	db := txNumWindowDB(t)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		reader := rawdbv3.TxNums.WithCustomReadTxNumFunc(snapshotTxNums{map[uint64]uint64{irreversibleBlock: irreversibleTxNum}})
		txNum, err := IrreversibleTxNum(t.Context(), tx, reader, irreversibleBlock)
		require.NoError(t, err)
		require.Equal(t, irreversibleTxNum, txNum)

		txNum, err = IrreversibleTxNum(t.Context(), tx, rawdbv3.TxNums, irreversibleBlock)
		require.NoError(t, err)
		require.Zero(t, txNum, "an unknown block collates nothing")
		return nil
	}))
}
