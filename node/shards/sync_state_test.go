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

package shards

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func buildReplyForTest(t *testing.T, lastNewBlockSeen, frozenBlocks, executionProgress uint64) *remoteproto.SyncingReply {
	t.Helper()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, executionProgress))

	n := NewNotifications(nil)
	n.NewLastBlockSeen(lastNewBlockSeen)
	reply, err := n.BuildSyncingReply(tx, frozenBlocks)
	require.NoError(t, err)
	return reply
}

func TestBuildSyncingReplySyncedWithinReorgRange(t *testing.T) {
	reply := buildReplyForTest(t, 105, 0, 100)
	require.False(t, reply.Syncing)
	require.Empty(t, reply.Stages)
	require.Equal(t, uint64(100), reply.CurrentBlock)
	require.Equal(t, uint64(105), reply.LastNewBlockSeen)
}

func TestBuildSyncingReplySyncingReportsAllStages(t *testing.T) {
	reply := buildReplyForTest(t, 200, 0, 100)
	require.True(t, reply.Syncing)
	require.Len(t, reply.Stages, len(stages.AllStages))
	for _, stage := range reply.Stages {
		if stage.StageName == string(stages.Execution) {
			require.Equal(t, uint64(100), stage.BlockNumber)
		}
	}
	require.Equal(t, uint64(100), reply.CurrentBlock)
	require.Equal(t, uint64(200), reply.LastNewBlockSeen)
}

func TestBuildSyncingReplyUnknownHighestBlockOmitsStages(t *testing.T) {
	reply := buildReplyForTest(t, 0, 0, 0)
	require.True(t, reply.Syncing)
	require.Empty(t, reply.Stages)
	require.Equal(t, uint64(0), reply.LastNewBlockSeen)
}

func TestBuildSyncingReplyFrozenBlocksRaiseHighestBlock(t *testing.T) {
	reply := buildReplyForTest(t, 10, 500, 100)
	require.True(t, reply.Syncing)
	require.Equal(t, uint64(500), reply.LastNewBlockSeen)
	require.Equal(t, uint64(500), reply.FrozenBlocks)
}

func drainSyncStateEvents(ch chan *remoteproto.SyncingReply) []*remoteproto.SyncingReply {
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

// PublishSyncState builds and publishes atomically and is the single dedup
// point for every sync-state producer, so two producers observing the same
// state must yield one notification.
func TestPublishSyncStateDedupsAcrossPublishers(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	n := NewNotifications(nil)
	ch, unsubscribe := n.Events.AddSyncStateSubscription()
	defer unsubscribe()

	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 100))
	n.NewLastBlockSeen(100)
	require.NoError(t, n.PublishSyncState(tx, 0))
	require.NoError(t, n.PublishSyncState(tx, 0))
	require.Len(t, drainSyncStateEvents(ch), 1, "identical state published twice must notify once")

	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 105))
	n.NewLastBlockSeen(105)
	require.NoError(t, n.PublishSyncState(tx, 0))
	require.Empty(t, drainSyncStateEvents(ch), "block progress while synced must not notify")

	n.NewLastBlockSeen(500)
	require.NoError(t, n.PublishSyncState(tx, 0))
	got := drainSyncStateEvents(ch)
	require.Len(t, got, 1)
	require.True(t, got[0].Syncing)
}

// The seed and the events come from the same lock-ordered sequence: a state
// published before the subscription is the seed, one published after arrives
// on the channel — never both, never neither.
func TestSubscribeSyncStateSeedsWithLastPublishedState(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	n := NewNotifications(nil)
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 100))
	n.NewLastBlockSeen(500)
	require.NoError(t, n.PublishSyncState(tx, 0))

	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 105))
	ch, seed, unsubscribe, err := n.SubscribeSyncState(tx, 0)
	require.NoError(t, err)
	defer unsubscribe()

	require.NotNil(t, seed)
	require.True(t, seed.Syncing)
	require.Equal(t, uint64(100), seed.CurrentBlock, "the published state is the seed, not a fresh build")
	require.Empty(t, drainSyncStateEvents(ch), "the state published before subscribing is the seed, not an event")

	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 498))
	n.NewLastBlockSeen(498)
	require.NoError(t, n.PublishSyncState(tx, 0))
	got := drainSyncStateEvents(ch)
	require.Len(t, got, 1)
	require.False(t, got[0].Syncing)
}

// Before the first publish there is no last state to seed from, so the seed
// is built from the tx inside the same critical section that registers the
// subscription: any publish is then ordered entirely before (impossible, none
// happened) or entirely after it, and lands on the channel.
func TestSubscribeSyncStateBeforeFirstPublishBuildsSeed(t *testing.T) {
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	defer tx.Rollback()

	n := NewNotifications(nil)
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, 100))
	n.NewLastBlockSeen(500)

	ch, seed, unsubscribe, err := n.SubscribeSyncState(tx, 0)
	require.NoError(t, err)
	defer unsubscribe()

	require.NotNil(t, seed)
	require.True(t, seed.Syncing)
	require.Equal(t, uint64(100), seed.CurrentBlock)
	require.Empty(t, drainSyncStateEvents(ch))

	require.NoError(t, n.PublishSyncState(tx, 0))
	require.Len(t, drainSyncStateEvents(ch), 1, "a publish after subscribing must arrive as an event even when it equals the built seed")
}
