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
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/temporal/temporaltest"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stageloop"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/shards"
)

type pinTestBlockReader struct {
	dbservices.FullBlockReader
}

func (pinTestBlockReader) FrozenBlocks() uint64 { return 0 }

// newPinTestExecutor wires a PipelineExecutor around a single-stage Sync whose
// Snapshots-stage Forward func is controlled by the test, so ProcessFrozenBlocks'
// control flow around the download-pin handoff can be exercised without a real
// snapshot download or block execution.
func newPinTestExecutor(t *testing.T, forward stagedsync.ExecFunc) (*PipelineExecutor, *stageloop.Hook, *shards.Notifications) {
	t.Helper()
	logger := log.New()
	db := temporaltest.NewTestDB(t, datadir.New(t.TempDir()))
	chainConfig := &chain.Config{ChainName: "test"}
	notifications := shards.NewNotifications(nil)

	noopUnwind := func(*stagedsync.UnwindState, *stagedsync.StageState, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
		return nil
	}
	sync := stagedsync.New(
		ethconfig.Sync{},
		[]*stagedsync.Stage{{ID: stages.Snapshots, Forward: forward, Unwind: noopUnwind}},
		nil, nil, logger, stages.ModeApplyingBlocks,
	)

	dispatcher := NewDispatcher(chainConfig, notifications.Events, notifications.StateChangesConsumer, logger)
	pe := NewPipelineExecutor(sync, db, pinTestBlockReader{}, chainConfig, nil, nil, nil, dispatcher, logger)
	hook := stageloop.NewHook(t.Context(), notifications, sync, chainConfig, logger, dispatcher, nil, nil, nil, pinTestBlockReader{})
	return pe, hook, notifications
}

// pinSurvived drains the pin via ClearSnapshotDownloadPin: true means a
// terminal sample was still there (the pin outlived ProcessFrozenBlocks).
func pinSurvived(n *shards.Notifications) bool {
	return n.ClearSnapshotDownloadPin()
}

// A Bor node's startup pipeline only downloads snapshots, and execution runs
// elsewhere: the download pin still has a handoff to bridge there, so
// ProcessFrozenBlocks must not touch it on this path.
func TestProcessFrozenBlocksOnlySnapDownloadKeepsPin(t *testing.T) {
	pe, hook, notifications := newPinTestExecutor(t, func(bool, *stagedsync.StageState, stagedsync.Unwinder, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
		return nil
	})
	notifications.SetSnapshotDownloadProgress(1, 1, 100)

	require.NoError(t, pe.ProcessFrozenBlocks(t.Context(), hook, true))

	require.True(t, pinSurvived(notifications), "onlySnapDownload must not touch the download pin")
}

// lastPublished drains the subscription channel and returns the most recent
// reply, or nil when nothing was published.
func lastPublished(ch chan *remoteproto.SyncingReply) (last *remoteproto.SyncingReply) {
	for {
		select {
		case reply := <-ch:
			last = reply
		default:
			return last
		}
	}
}

// requirePinDropped asserts the pin is gone and that the drop reached
// subscribers: the LAST published reply must reflect it, since BeforeRun
// publishes the still-pinned state earlier.
func requirePinDropped(t *testing.T, n *shards.Notifications, ch chan *remoteproto.SyncingReply) {
	t.Helper()
	require.False(t, n.ClearSnapshotDownloadPin(), "the pin must already be dropped")
	last := lastPublished(ch)
	require.NotNil(t, last, "the drop must be published to subscribers")
	require.Zero(t, last.CurrentBlock, "the last published reply must reflect the dropped pin")
}

// When execution runs and the pipeline fails, the pin's handoff never
// happens: it must be dropped and the drop published — even when the failure
// is a ctx cancellation, where a publish on the caller's ctx would fail.
func TestProcessFrozenBlocksClearsPinOnFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	pe, hook, notifications := newPinTestExecutor(t, func(bool, *stagedsync.StageState, stagedsync.Unwinder, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
		cancel()
		return nil
	})
	notifications.SetSnapshotDownloadProgress(1, 1, 100)
	ch, unsubscribe := notifications.Events.AddSyncStateSubscription()
	defer unsubscribe()

	require.Error(t, pe.ProcessFrozenBlocks(ctx, hook, false))

	requirePinDropped(t, notifications, ch)
}

// A failure inside the snapshots stage itself, after the pin was published
// (e.g. in the stage tail), must drop the pin too: the pipeline exits before
// executing anything, and nothing else would ever clear it.
func TestProcessFrozenBlocksClearsPinOnSnapshotsStageFailure(t *testing.T) {
	pe, hook, notifications := newPinTestExecutor(t, func(bool, *stagedsync.StageState, stagedsync.Unwinder, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
		return errors.New("stage tail failed after the pin")
	})
	notifications.SetSnapshotDownloadProgress(1, 1, 100)
	ch, unsubscribe := notifications.Events.AddSyncStateSubscription()
	defer unsubscribe()

	require.Error(t, pe.ProcessFrozenBlocks(t.Context(), hook, false))

	requirePinDropped(t, notifications, ch)
}
