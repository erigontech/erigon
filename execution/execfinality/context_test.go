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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/dbfinality"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/mdbx/mdbxtest"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

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
			var ctx dbfinality.Context = NewContext(tc.headBlockNum, tc.finalisedBlockNum, tc.maxReorgDepth, tc.initialCycle)
			require.Equal(t, tc.pruneTo, ctx.PruneToBlockNum())
			require.Equal(t, tc.retireTo, ctx.RetireToBlockNum())
		})
	}
}

func TestResolveUsesTransactionVisibleExecutionProgress(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	const finalisedBlockNum = uint64(100)
	finalisedHash := common.Hash{0x01}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 1_000))
	require.NoError(t, rawdb.WriteHeaderNumber(tx, finalisedHash, finalisedBlockNum))
	rawdb.WriteForkchoiceFinalized(tx, finalisedHash)
	ctx, err := Resolve(tx, 96, true)
	require.NoError(t, err)
	require.Equal(t, uint64(904), ctx.PruneToBlockNum())
	require.Equal(t, uint64(904), ctx.RetireToBlockNum())
}

func TestResolveWithoutFinalisedBlockUsesMaxReorgDepth(t *testing.T) {
	db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	finalisedHash := common.Hash{0x01}
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 100))
	require.NoError(t, rawdb.WriteHeaderNumber(tx, finalisedHash, 1_000))
	rawdb.WriteForkchoiceFinalized(tx, finalisedHash)
	ctx, err := Resolve(tx, 96, false, WithoutFinalisedBlock())
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
			db := mdbxtest.NewTestDB(t, dbcfg.ChainDB)
			require.NoError(t, db.Update(t.Context(), func(tx kv.RwTx) error {
				for blockNum := uint64(0); blockNum <= tc.headBlockNum; blockNum++ {
					if err := rawdbv3.TxNums.Append(tx, blockNum, blockNum); err != nil {
						return err
					}
				}
				return nil
			}))
			ctx := NewContext(tc.headBlockNum, tc.finalisedBlockNum, tc.maxReorgDepth, tc.initialCycle)
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
