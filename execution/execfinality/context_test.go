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
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

func newFinalityTestDB(t *testing.T) kv.TemporalRwDB {
	t.Helper()
	return temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
}

func TestContextBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name              string
		headBlockNum      uint64
		finalisedBlockNum uint64
		maxReorgDepth     uint64
		initialCycle      bool
		pruneTo           uint64
		retireTo          uint64
	}{
		{
			name:              "tip uses finality",
			headBlockNum:      1_000,
			finalisedBlockNum: 100,
			maxReorgDepth:     96,
			pruneTo:           100,
			retireTo:          100,
		},
		{
			name:              "initial cycle uses reorg depth",
			headBlockNum:      1_000,
			finalisedBlockNum: 100,
			maxReorgDepth:     96,
			initialCycle:      true,
			pruneTo:           904,
			retireTo:          904,
		},
		{
			name:          "missing finality uses reorg depth",
			headBlockNum:  1_000,
			maxReorgDepth: 96,
			pruneTo:       904,
			retireTo:      904,
		},
		{
			name:              "finalised genesis uses reorg depth",
			headBlockNum:      1_000,
			finalisedBlockNum: 0,
			maxReorgDepth:     96,
			pruneTo:           904,
			retireTo:          904,
		},
		{
			name:          "head inside reorg window",
			headBlockNum:  96,
			maxReorgDepth: 96,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var ctx kv.FinalityContext = NewContext(tc.headBlockNum, tc.finalisedBlockNum, tc.maxReorgDepth, tc.initialCycle, rawdbv3.TxNums)
			require.Equal(t, tc.pruneTo, ctx.PruneToBlockNum())
			require.Equal(t, tc.retireTo, ctx.RetireToBlockNum())
		})
	}
}

func TestResolveUsesTransactionVisibleExecutionProgress(t *testing.T) {
	db := newFinalityTestDB(t)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	const finalisedBlockNum = uint64(100)
	finalisedHash := common.Hash{0x01}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 1_000))
	require.NoError(t, rawdb.WriteHeaderNumber(tx, finalisedHash, finalisedBlockNum))
	rawdb.WriteForkchoiceFinalized(tx, finalisedHash)
	ctx, err := Resolve(tx, 96, true, rawdbv3.TxNums)
	require.NoError(t, err)
	require.Equal(t, uint64(904), ctx.PruneToBlockNum())
	require.Equal(t, uint64(904), ctx.RetireToBlockNum())
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
	ctx, err := Resolve(tx, 96, false, rawdbv3.TxNums, WithoutFinalisedBlock())
	require.NoError(t, err)
	require.Equal(t, uint64(4), ctx.PruneToBlockNum())
	require.Equal(t, uint64(4), ctx.RetireToBlockNum())
}

func TestContextReadyForCollationUsesTransactionVisibleTxNums(t *testing.T) {
	for _, tc := range []struct {
		name              string
		headBlockNum      uint64
		finalisedBlockNum uint64
		maxReorgDepth     uint64
		initialCycle      bool
		stepLastTxNum     uint64
		ready             bool
	}{
		{
			name:              "finality includes boundary",
			headBlockNum:      20,
			finalisedBlockNum: 12,
			maxReorgDepth:     5,
			stepLastTxNum:     12,
			ready:             true,
		},
		{
			name:              "finality excludes after boundary",
			headBlockNum:      20,
			finalisedBlockNum: 12,
			maxReorgDepth:     5,
			stepLastTxNum:     13,
		},
		{
			name:              "initial cycle includes before reorg window",
			headBlockNum:      20,
			finalisedBlockNum: 12,
			maxReorgDepth:     5,
			initialCycle:      true,
			stepLastTxNum:     14,
			ready:             true,
		},
		{
			name:              "initial cycle excludes reorg window",
			headBlockNum:      20,
			finalisedBlockNum: 12,
			maxReorgDepth:     5,
			initialCycle:      true,
			stepLastTxNum:     15,
		},
		{
			name:          "head inside reorg window",
			headBlockNum:  5,
			maxReorgDepth: 5,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newFinalityTestDB(t)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				for blockNum := uint64(0); blockNum <= tc.headBlockNum; blockNum++ {
					if err := rawdbv3.TxNums.Append(tx, blockNum, blockNum); err != nil {
						return err
					}
				}
				return nil
			}))
			ctx := NewContext(tc.headBlockNum, tc.finalisedBlockNum, tc.maxReorgDepth, tc.initialCycle, rawdbv3.TxNums)
			finalisedBlockNum, lastBlockInStep, lastBlockInDB, lastTxInDB, ready, err := ctx.ReadyForCollation(t.Context(), db, tc.stepLastTxNum)
			require.NoError(t, err)
			require.Equal(t, tc.finalisedBlockNum, finalisedBlockNum)
			require.Equal(t, tc.stepLastTxNum, lastBlockInStep)
			require.Equal(t, tc.headBlockNum, lastBlockInDB)
			require.Equal(t, tc.headBlockNum, lastTxInDB)
			require.Equal(t, tc.ready, ready)
		})
	}
}

// snapshotTxNums resolves a txnum from a full-range table, the way the block-snapshot
// backed index does for blocks chaindata no longer covers.
type snapshotTxNums struct{ maxTxNumByBlock map[uint64]uint64 }

func (snapshotTxNums) MaxTxNum(context.Context, kv.Tx, kv.Cursor, uint64) (uint64, bool, error) {
	return 0, false, nil
}

func (s snapshotTxNums) BlockNumber(_ context.Context, _ kv.Tx, txNum uint64) (uint64, bool, error) {
	for blockNum, maxTxNum := range s.maxTxNumByBlock {
		if txNum <= maxTxNum {
			return blockNum, true, nil
		}
	}
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

// Blocks downloaded to tip with execution far behind: the step ends below the table's
// floor, which the chaindata search answers with the floor rather than the real block.
// The snapshot-backed reader names it, so the step collates.
func TestContextReadyForCollationCollatesStepsBelowTheTxNumWindow(t *testing.T) {
	const (
		headBlockNum  = uint64(20_899_437)
		stepLastTxNum = uint64(2_400_000_000)
		stepLastBlock = uint64(20_000_000)
	)

	db := txNumWindowDB(t)
	reader := rawdbv3.TxNums.WithCustomReadTxNumFunc(snapshotTxNums{map[uint64]uint64{stepLastBlock: stepLastTxNum}})
	ctx := NewContext(headBlockNum, 25_837_750, 96, true, reader)

	_, lastBlockInStep, _, _, ready, err := ctx.ReadyForCollation(t.Context(), db, stepLastTxNum)
	require.NoError(t, err)
	require.Equal(t, stepLastBlock, lastBlockInStep, "must resolve the real block, not the table floor")
	require.True(t, ready, "a step below the reorg window must collate")
}

// The same shape one step later, with the step boundary inside the reorg window. Naming
// the real block is what keeps the gate working here: a shortcut past the table floor
// would open it for every step, since on this node every executed txnum is below it.
func TestContextReadyForCollationStillGatesStepsBelowTheTxNumWindow(t *testing.T) {
	const (
		headBlockNum  = uint64(20_899_437)
		stepLastTxNum = uint64(2_574_609_374)
		stepLastBlock = uint64(20_899_424)
	)

	db := txNumWindowDB(t)
	reader := rawdbv3.TxNums.WithCustomReadTxNumFunc(snapshotTxNums{map[uint64]uint64{stepLastBlock: stepLastTxNum}})
	ctx := NewContext(headBlockNum, 25_837_750, 96, true, reader)

	_, lastBlockInStep, _, _, ready, err := ctx.ReadyForCollation(t.Context(), db, stepLastTxNum)
	require.NoError(t, err)
	require.Equal(t, stepLastBlock, lastBlockInStep)
	require.False(t, ready, "a step inside the reorg window stays gated")
}

// A synced node also prunes MaxTxNum down to a recent window. There chaindata covers the
// step, so the default reader resolves it and nothing changes.
func TestContextReadyForCollationResolvesStepsInsideTheTxNumWindow(t *testing.T) {
	db := newFinalityTestDB(t)
	require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
		for _, e := range []struct{ blockNum, maxTxNum uint64 }{
			{0, 1}, {25_472_999, 3_630_627_978}, {25_473_000, 3_630_628_100}, {25_473_001, 3_630_628_300},
		} {
			if err := rawdbv3.TxNums.Append(tx, e.blockNum, e.maxTxNum); err != nil {
				return err
			}
		}
		return nil
	}))

	ctx := NewContext(25_473_001, 25_473_000, 96, false, rawdbv3.TxNums)
	_, lastBlockInStep, _, _, ready, err := ctx.ReadyForCollation(t.Context(), db, 3_630_628_100)
	require.NoError(t, err)
	require.Equal(t, uint64(25_473_000), lastBlockInStep)
	require.True(t, ready)

	_, lastBlockInStep, _, _, ready, err = ctx.ReadyForCollation(t.Context(), db, 3_630_628_300)
	require.NoError(t, err)
	require.Equal(t, uint64(25_473_001), lastBlockInStep)
	require.False(t, ready, "step above the finalised head stays gated")
}

// txRecordingIndex records the tx the reader is handed.
type txRecordingIndex struct{ tx kv.Tx }

func (txRecordingIndex) MaxTxNum(context.Context, kv.Tx, kv.Cursor, uint64) (uint64, bool, error) {
	return 0, false, nil
}

func (i *txRecordingIndex) BlockNumber(_ context.Context, tx kv.Tx, _ uint64) (uint64, bool, error) {
	i.tx = tx
	return 0, false, nil
}

// A snapshot-backed reader reads block files, which only a temporal tx pins a view for.
func TestContextReadyForCollationResolvesStepsOnATemporalTx(t *testing.T) {
	temporalDB := newFinalityTestDB(t)
	index := &txRecordingIndex{}
	ctx := NewContext(25_473_001, 25_473_000, 96, false, rawdbv3.TxNums.WithCustomReadTxNumFunc(index))

	_, _, _, _, _, err := ctx.ReadyForCollation(t.Context(), temporalDB, 3_630_628_100)
	require.NoError(t, err)
	require.Implements(t, (*kv.TemporalTx)(nil), index.tx)
}
