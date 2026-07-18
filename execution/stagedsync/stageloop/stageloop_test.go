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

package stageloop

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/shards"
)

type frozenBlocksStub uint64

func (f frozenBlocksStub) FrozenBlocks() uint64 { return uint64(f) }

func drainSyncState(ch chan *remoteproto.SyncingReply) []*remoteproto.SyncingReply {
	var got []*remoteproto.SyncingReply
	for {
		select {
		case reply := <-ch:
			got = append(got, reply)
		default:
			return got
		}
	}
}

func TestHookUpdateHeadEmitsSyncStateOnlyOnChange(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 100))

	notifications := shards.NewNotifications(nil)
	notifications.NewLastBlockSeen(200)
	ch, unsubscribe := notifications.Events.AddSyncStateSubscription()
	defer unsubscribe()

	hook := NewHook(t.Context(), notifications, nil, nil, log.New(), nil, nil, nil, nil, frozenBlocksStub(0))

	require.NoError(t, hook.UpdateHead(tx, 0, false))
	emitted := drainSyncState(ch)
	require.Len(t, emitted, 1)
	require.True(t, emitted[0].Syncing)
	require.Equal(t, uint64(100), emitted[0].CurrentBlock)
	require.Equal(t, uint64(200), emitted[0].LastNewBlockSeen)

	require.NoError(t, hook.UpdateHead(tx, 0, false))
	require.Empty(t, drainSyncState(ch), "unchanged sync state must not be re-emitted")

	notifications.NewLastBlockSeen(300)
	require.NoError(t, hook.UpdateHead(tx, 0, false))
	emitted = drainSyncState(ch)
	require.Len(t, emitted, 1)
	require.Equal(t, uint64(300), emitted[0].LastNewBlockSeen)
}

func TestHookUpdateHeadEmitsSyncedTransition(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	notifications := shards.NewNotifications(nil)
	notifications.NewLastBlockSeen(200)
	ch, unsubscribe := notifications.Events.AddSyncStateSubscription()
	defer unsubscribe()

	hook := NewHook(t.Context(), notifications, nil, nil, log.New(), nil, nil, nil, nil, frozenBlocksStub(0))

	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 100))
	require.NoError(t, hook.UpdateHead(tx, 0, false))
	require.Len(t, drainSyncState(ch), 1)

	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 198))
	require.NoError(t, hook.UpdateHead(tx, 0, true))
	emitted := drainSyncState(ch)
	require.Len(t, emitted, 1)
	require.False(t, emitted[0].Syncing)
}

func TestHookUpdateHeadDoesNotReEmitWhileSynced(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	notifications := shards.NewNotifications(nil)
	notifications.NewLastBlockSeen(200)
	ch, unsubscribe := notifications.Events.AddSyncStateSubscription()
	defer unsubscribe()

	hook := NewHook(t.Context(), notifications, nil, nil, log.New(), nil, nil, nil, nil, frozenBlocksStub(0))

	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 199))
	require.NoError(t, hook.UpdateHead(tx, 0, true))
	require.Len(t, drainSyncState(ch), 1)

	for blockNum := uint64(201); blockNum <= 205; blockNum++ {
		notifications.NewLastBlockSeen(blockNum)
		require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, blockNum-1))
		require.NoError(t, hook.UpdateHead(tx, 0, true))
	}
	require.Empty(t, drainSyncState(ch), "block progress while synced must not re-emit sync state")

	notifications.NewLastBlockSeen(500)
	require.NoError(t, hook.UpdateHead(tx, 0, false))
	emitted := drainSyncState(ch)
	require.Len(t, emitted, 1, "falling behind again must re-emit")
	require.True(t, emitted[0].Syncing)
}

func TestHookUpdateHeadNilSafe(t *testing.T) {
	var hook *Hook
	require.NoError(t, hook.UpdateHead(nil, 0, false))

	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	require.NoError(t, db.View(t.Context(), func(tx kv.Tx) error {
		return NewHook(t.Context(), nil, nil, nil, log.New(), nil, nil, nil, nil, nil).UpdateHead(tx, 0, false)
	}))
}
