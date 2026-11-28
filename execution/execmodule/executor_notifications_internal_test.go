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
	"errors"
	"fmt"
	"testing"

	"github.com/holiman/uint256"
	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/node/shards"
)

func TestProcessFrozenBlocksKeepsCommittedNotifications(t *testing.T) {
	for _, committedCycles := range []uint64{0, 1} {
		t.Run(fmt.Sprintf("%d committed cycles", committedCycles), func(t *testing.T) {
			stageErr := errors.New("startup execution failed")
			var notifications *shards.Notifications
			pe, hook, n := newPinTestExecutor(t, func(_ bool, _ *stagedsync.StageState, _ stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
				if sd == nil {
					return nil // Snapshot download precedes the execution loop.
				}
				blockNum, err := stages.GetStageProgress(tx, stages.Finish)
				if err != nil {
					return err
				}
				blockNum++
				header := &types.Header{Number: *uint256.NewInt(blockNum)}
				notifications.Accumulator.StartChange(header, [][]byte{{byte(blockNum)}}, false)
				notifications.RecentReceipts.Add(types.Receipts{{BlockNumber: &header.Number}}, nil, header)
				if err := stages.SaveStageProgress(tx, stages.Finish, blockNum); err != nil {
					return err
				}
				if blockNum > committedCycles {
					notifications.Accumulator.MarkIncomplete()
					return stageErr
				}
				return &stagedsync.ErrLoopExhausted{From: blockNum, To: blockNum}
			})
			notifications = n
			notifications.Accumulator.StartChange(&types.Header{}, [][]byte{{0}}, false)
			initial := notifications.Accumulator.Changes()[0].ToProto()

			require.ErrorIs(t, pe.ProcessFrozenBlocks(t.Context(), hook, false), stageErr)
			require.Equal(t, []uint64{committedCycles + 1}, notifications.RecentReceipts.Missing(1, committedCycles+2))
			require.NoError(t, pe.db.View(t.Context(), func(tx kv.Tx) error {
				progress, err := stages.GetStageProgress(tx, stages.Finish)
				require.Equal(t, committedCycles, progress)
				return err
			}))
			changes := notifications.Accumulator.Changes()
			require.Len(t, changes, int(committedCycles)+1, "only committed changes may remain pending")
			require.Equal(t, initial, changes[0].ToProto())
			if committedCycles > 0 {
				require.Equal(t, committedCycles, changes[1].BlockHeight)
				require.Equal(t, [][]byte{{1}}, changes[1].Txs)
			}
			batch := notifications.Accumulator.Batch(&types.Header{Number: *uint256.NewInt(committedCycles)})
			require.False(t, batch.StateChangesIncomplete, "the failed cycle's flag must also roll back")
		})
	}
}

func TestProcessFrozenBlocksKeepsIncompleteFlagWhenDomainsAhead(t *testing.T) {
	const snapshotBlock = uint64(100)
	var notifications *shards.Notifications
	pe, hook, n := newPinTestExecutor(t, func(_ bool, _ *stagedsync.StageState, _ stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, logger log.Logger) error {
		require.Nil(t, sd, "startup must stop after the snapshots stage")
		require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, snapshotBlock))
		notifications.Accumulator.MarkIncomplete()
		require.True(t, execctx.IsDomainAheadOfBlocks(t.Context(), tx, logger))
		return nil
	})
	notifications = n

	require.NoError(t, pe.ProcessFrozenBlocks(t.Context(), hook, false))
	require.NoError(t, pe.db.View(t.Context(), func(tx kv.Tx) error {
		progress, err := stages.GetStageProgress(tx, stages.Execution)
		require.Equal(t, snapshotBlock, progress, "snapshot progress must be committed")
		return err
	}))
	batch := notifications.Accumulator.Batch(&types.Header{Number: *uint256.NewInt(snapshotBlock)})
	require.True(t, batch.StateChangesIncomplete, "the committed snapshot reset must remain pending")
}

func TestProcessFrozenBlocksDoesNotRestorePublishedNotifications(t *testing.T) {
	header := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000}
	var notifications *shards.Notifications
	pe, hook, n := newPinTestExecutor(t, func(_ bool, _ *stagedsync.StageState, _ stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
		if sd == nil {
			return nil
		}
		notifications.Accumulator.StartChange(header, [][]byte{{1}}, false)
		require.NoError(t, rawdb.WriteHeader(tx, header))
		require.NoError(t, rawdb.WriteCanonicalHash(tx, header.Hash(), 1))
		require.NoError(t, rawdb.WriteHeadHeaderHash(tx, header.Hash()))
		return stages.SaveStageProgress(tx, stages.Finish, 1)
	})
	notifications = n
	capture := new(stateChangesCapture)
	pe.dispatcher.stateChangeConsumer = capture

	require.NoError(t, pe.ProcessFrozenBlocks(t.Context(), hook, false))
	require.NotNil(t, capture.batch)
	require.Len(t, capture.batch.ChangeBatch, 1)
	require.Equal(t, uint64(1), capture.batch.ChangeBatch[0].BlockHeight)
	require.Equal(t, [][]byte{{1}}, capture.batch.ChangeBatch[0].Txs)
	require.False(t, notifications.Accumulator.HasPendingChanges(), "published changes must stay cleared")
}

func TestProcessFrozenBlocksPublishesOnlyCommittedChangesAfterRejection(t *testing.T) {
	committed := &types.Header{Number: *uint256.NewInt(1), GasLimit: 30_000_000}
	rejected := &types.Header{Number: *uint256.NewInt(2), ParentHash: committed.Hash(), GasLimit: committed.GasLimit}
	var notifications *shards.Notifications
	pe, hook, n := newPinTestExecutor(t, func(_ bool, _ *stagedsync.StageState, _ stagedsync.Unwinder, sd *execctx.SharedDomains, tx kv.TemporalRwTx, _ log.Logger) error {
		if sd == nil {
			// Both headers are known before execution reaches the rejected block.
			for _, h := range []*types.Header{committed, rejected} {
				require.NoError(t, rawdb.WriteHeader(tx, h))
				require.NoError(t, rawdb.WriteCanonicalHash(tx, h.Hash(), h.Number.Uint64()))
				require.NoError(t, rawdbv3.TxNums.Append(tx, h.Number.Uint64(), 2*h.Number.Uint64()+1))
			}
			rawdb.WriteForkchoiceHead(tx, committed.Hash())
			return nil
		}
		finish, err := stages.GetStageProgress(tx, stages.Finish)
		require.NoError(t, err)
		if finish == 0 {
			notifications.Accumulator.StartChange(committed, [][]byte{{1}}, false)
			notifications.RecentReceipts.Add(types.Receipts{{BlockNumber: &committed.Number}}, nil, committed)
			require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 1))
			require.NoError(t, stages.SaveStageProgress(tx, stages.Finish, 1))
			return &stagedsync.ErrLoopExhausted{From: 1, To: 1}
		}
		notifications.Accumulator.StartChange(rejected, [][]byte{{2}}, false)
		notifications.Accumulator.MarkIncomplete()
		notifications.RecentReceipts.Add(types.Receipts{{BlockNumber: &rejected.Number}}, nil, rejected)
		return &stagedsync.RejectedBlockError{Number: 2, Hash: rejected.Hash(), Err: rules.ErrInvalidBlock}
	})
	notifications = n
	pe.blockReader = startupBlockReader{BlockReader: freezeblocks.NewBlockReader(nil)}
	capture := new(stateChangesCapture)
	pe.dispatcher.stateChangeConsumer = capture

	require.NoError(t, pe.ProcessFrozenBlocks(t.Context(), hook, false))
	require.Equal(t, 1, capture.calls)
	require.Len(t, capture.batch.ChangeBatch, 1)
	require.Equal(t, uint64(1), capture.batch.ChangeBatch[0].BlockHeight)
	require.Equal(t, [][]byte{{1}}, capture.batch.ChangeBatch[0].Txs)
	require.False(t, capture.batch.StateChangesIncomplete)
	require.Equal(t, []uint64{2}, notifications.RecentReceipts.Missing(1, 3))
	require.False(t, notifications.Accumulator.HasPendingChanges())
}
