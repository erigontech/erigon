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

// A Bor node's startup pipeline only downloads snapshots — Sync.RunSnapshots
// covers the snapshots stage — and execution runs elsewhere (polygon-sync).
// The download pin still has a handoff to bridge there, so ProcessFrozenBlocks
// must not touch it on this path.
func TestProcessFrozenBlocksOnlySnapDownloadKeepsPin(t *testing.T) {
	pe, hook, notifications := newPinTestExecutor(t, func(bool, *stagedsync.StageState, stagedsync.Unwinder, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
		return nil
	})
	notifications.SetSnapshotDownloadProgress(1, 1, 100)

	require.NoError(t, pe.ProcessFrozenBlocks(t.Context(), hook, true))

	require.True(t, pinSurvived(notifications), "onlySnapDownload must not touch the download pin")
}

// When execution does run (onlySnapDownload=false) and the pipeline fails
// before completing, the pin's handoff never happens: it must be dropped so
// eth_syncing does not keep reporting a stuck node as nearly synced.
func TestProcessFrozenBlocksClearsPinOnFailure(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	// Cancelling right after the snapshots stage (RunSnapshots) succeeds forces
	// the pipeline to fail on its next context-aware operation — after the pin's
	// defer is registered, without needing a real execution failure to reach it.
	pe, hook, notifications := newPinTestExecutor(t, func(bool, *stagedsync.StageState, stagedsync.Unwinder, *execctx.SharedDomains, kv.TemporalRwTx, log.Logger) error {
		cancel()
		return nil
	})
	notifications.SetSnapshotDownloadProgress(1, 1, 100)
	ch, unsubscribe := notifications.Events.AddSyncStateSubscription()
	defer unsubscribe()

	require.Error(t, pe.ProcessFrozenBlocks(ctx, hook, false))

	require.False(t, pinSurvived(notifications), "a pipeline failure past the handoff must drop the pin")
	select {
	case <-ch:
	default:
		t.Fatal("dropping the pin must publish the sync-state change to subscribers")
	}
}
