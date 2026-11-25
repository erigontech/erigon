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

package execmodule

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state/changeset"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/shards"
)

type buildRecordingDB struct {
	kv.TemporalRwDB
	irreversibleTxNums []uint64
}

func (db *buildRecordingDB) BuildFilesInBackground(irreversibleTxNum uint64) chan struct{} {
	db.irreversibleTxNums = append(db.irreversibleTxNums, irreversibleTxNum)
	done := make(chan struct{})
	close(done)
	return done
}

// A frozen batch longer than MaxReorgDepth flushes its changesets on commit, so the boundary handed to the file
// build must stay below them.
func TestProcessFrozenBlocksBuildsNoFilesOverBatchChangesets(t *testing.T) {
	const head, maxReorgDepth = 10, 2
	logger := log.New()
	db := &buildRecordingDB{TemporalRwDB: temporaltest.NewTestDB(t, datadir.New(t.TempDir()))}
	noop := func(bool, *stagedsync.StageState, stagedsync.Unwinder, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
		return nil
	}
	noopUnwind := func(*stagedsync.UnwindState, *stagedsync.StageState, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
		return nil
	}
	execute := func(_ bool, _ *stagedsync.StageState, _ stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
		for block := uint64(0); block <= head; block++ {
			if err := rawdbv3.TxNums.Append(tx, block, block*10+9); err != nil {
				return err
			}
			if block > 0 {
				sd.SavePastChangesetAccumulator(common.Hash{byte(block)}, block, &changeset.StateChangeSet{})
			}
		}
		return stages.SaveStageProgress(tx, stages.Execution, head)
	}
	sync := stagedsync.New(
		ethconfig.Sync{MaxReorgDepth: maxReorgDepth, AlwaysGenerateChangesets: true},
		[]*stagedsync.Stage{
			{ID: stages.Snapshots, Forward: noop, Unwind: noopUnwind},
			{ID: stages.Execution, Forward: execute, Unwind: noopUnwind},
		},
		nil, nil, logger, stages.ModeApplyingBlocks,
	)
	chainConfig := &chain.Config{ChainName: "test"}
	notifications := shards.NewNotifications(nil)
	dispatcher := NewDispatcher(chainConfig, freezeblocks.NewBlockReader(nil), notifications.Events, notifications.StateChangesConsumer, nil, logger)
	pe := NewPipelineExecutor(sync, db, pinTestBlockReader{}, chainConfig, nil, nil, nil, dispatcher, logger)
	hook := stageloop.NewHook(t.Context(), notifications, sync, chainConfig, logger, dispatcher, nil, nil, nil, pinTestBlockReader{})

	require.NoError(t, pe.ProcessFrozenBlocks(t.Context(), hook, false))

	require.NotEmpty(t, db.irreversibleTxNums)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		lowest, err := changeset.ReadLowestUnwindableBlock(tx)
		require.NoError(t, err)
		for _, txNum := range db.irreversibleTxNums {
			block, ok, err := rawdbv3.TxNums.FindBlockNum(t.Context(), tx, txNum)
			require.NoError(t, err)
			require.True(t, ok)
			require.Less(t, block, lowest, "files may hold txnum %d, a block that still has a changeset", txNum)
		}
		return nil
	}))
}
