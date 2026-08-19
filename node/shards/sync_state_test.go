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

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/memdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

func newSyncStateFixture(t *testing.T, executionProgress uint64) (*Notifications, kv.RwTx) {
	t.Helper()
	db := memdb.NewTestDB(t, dbcfg.ChainDB)
	tx, err := db.BeginRw(t.Context())
	require.NoError(t, err)
	t.Cleanup(tx.Rollback)
	require.NoError(t, stages.SaveStageProgress(tx, stages.Execution, executionProgress))
	return NewNotifications(nil), tx
}

func buildReplyForTest(t *testing.T, lastNewBlockSeen, frozenBlocks, executionProgress uint64) *remoteproto.SyncingReply {
	t.Helper()
	n, tx := newSyncStateFixture(t, executionProgress)
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

func buildReplyWithDownloadForTest(t *testing.T, done, total, targetBlock, executionProgress uint64) *remoteproto.SyncingReply {
	t.Helper()
	n, tx := newSyncStateFixture(t, executionProgress)
	n.SetSnapshotDownloading(done, total, targetBlock)
	reply, err := n.BuildSyncingReply(tx, 0)
	require.NoError(t, err)
	return reply
}

// During snapshot download the byte-completion ratio is mapped onto the
// block-based currentBlock/highestBlock so dashboards show smooth 0→100%
// progress: currentBlock = ratio * blocks_to_be_downloaded.
func TestBuildSyncingReplySnapshotDownloadMapsRatioToBlocks(t *testing.T) {
	reply := buildReplyWithDownloadForTest(t, 250, 1000, 20_000_000, 0)
	require.True(t, reply.Syncing)
	require.Empty(t, reply.Stages)
	require.Equal(t, uint64(5_000_000), reply.CurrentBlock)
	require.Equal(t, uint64(20_000_000), reply.LastNewBlockSeen)
}

// After the download completes progress is pinned at the commitment block to
// bridge the handoff to execution: currentBlock must report that block, not drop
// to 0 while the Execution stage counter has not yet been updated.
func TestBuildSyncingReplySnapshotDownloadHandoffPinsCommitmentBlock(t *testing.T) {
	n, tx := newSyncStateFixture(t, 0)
	n.SetSnapshotDownloadHandoff(20_000_000)

	reply, err := n.BuildSyncingReply(tx, 0)
	require.NoError(t, err)
	require.True(t, reply.Syncing)
	require.Empty(t, reply.Stages)
	require.Equal(t, uint64(20_000_000), reply.CurrentBlock)
	require.Equal(t, uint64(20_000_000), reply.LastNewBlockSeen)
}

// The snapshots being downloaded only cover targetBlock, so an FCU arriving
// mid-download must raise the reported highest block without scaling the byte
// ratio onto it: doing so claims blocks no snapshot holds and makes
// currentBlock step backwards once execution starts.
func TestBuildSyncingReplySnapshotDownloadDoesNotScaleToLiveHead(t *testing.T) {
	const targetBlock, liveHead = 20_000_000, 21_000_000
	n, tx := newSyncStateFixture(t, 0)
	n.NewLastBlockSeen(liveHead)
	n.SetSnapshotDownloading(500, 1000, targetBlock)

	reply, err := n.BuildSyncingReply(tx, 0)
	require.NoError(t, err)
	require.True(t, reply.Syncing)
	require.Equal(t, uint64(targetBlock/2), reply.CurrentBlock)
	require.Equal(t, uint64(liveHead), reply.LastNewBlockSeen)
}

// The downloader recomputes its byte total every cycle and it grows as torrent
// metadata arrives, so consecutive samples differ in both fields. A reader must
// never combine the total of one sample with the completed bytes of the next:
// that overshoots the target, i.e. currentBlock > highestBlock.
func TestBuildSyncingReplySnapshotDownloadProgressIsPublishedAtomically(t *testing.T) {
	n, tx := newSyncStateFixture(t, 0)
	const target = 20_000_000

	stop, writerDone := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stop:
				return
			default:
			}
			n.SetSnapshotDownloading(99, 100, target)
			n.SetSnapshotDownloading(150, 200, target)
		}
	}()
	defer func() {
		close(stop)
		<-writerDone
	}()

	for range 20_000 {
		reply, err := n.BuildSyncingReply(tx, 0)
		require.NoError(t, err)
		require.LessOrEqual(t, reply.CurrentBlock, reply.LastNewBlockSeen)
	}
}

// Only the handoff pin is dropped: an in-flight sample is the last honest
// progress after a failed download and must survive. The report tells the caller
// whether the reply changed, so the transition can be published to subscribers.
func TestClearSnapshotDownloadPin(t *testing.T) {
	n := NewNotifications(nil)

	require.False(t, n.ClearSnapshotDownloadPin(), "no sample to drop")

	n.SetSnapshotDownloading(400, 1000, 20_000_000)
	require.False(t, n.ClearSnapshotDownloadPin())
	require.NotNil(t, n.snapDownload.Load())

	n.SetSnapshotDownloadHandoff(20_000_000)
	require.True(t, n.ClearSnapshotDownloadPin())
	require.Nil(t, n.snapDownload.Load())
	require.False(t, n.ClearSnapshotDownloadPin(), "already dropped")
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

// A node that already has committed execution progress and downloads more
// snapshots (an upgrade, caplin enabled later) must never report a position
// below what it committed; below that floor the byte ratio is the progress.
func TestBuildSyncingReplySnapshotDownloadKeepsCommittedProgress(t *testing.T) {
	reply := buildReplyWithDownloadForTest(t, 250, 1000, 20_000_000, 6_000_000)
	require.True(t, reply.Syncing)
	require.Equal(t, uint64(6_000_000), reply.CurrentBlock)

	reply = buildReplyWithDownloadForTest(t, 250, 1000, 20_000_000, 100)
	require.Equal(t, uint64(5_000_000), reply.CurrentBlock)
}

// The handoff pin ends where the reply is built: execution progress reaching the
// commitment block means the handoff is over, so the reply switches back to the
// stage shape and the state is latched off without any owner signalling the end.
func TestBuildSyncingReplyHandoffEndsOnceExecutionReachesCommitmentBlock(t *testing.T) {
	n, tx := newSyncStateFixture(t, 19_000_000)
	n.NewLastBlockSeen(20_000_000)
	n.SetSnapshotDownloadHandoff(19_000_000)

	reply, err := n.BuildSyncingReply(tx, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(19_000_000), reply.CurrentBlock)
	require.Len(t, reply.Stages, len(stages.AllStages))
	require.Nil(t, n.snapDownload.Load(), "handoff latched off")
}

// Committed progress below the commitment block is the pre-download position of
// an upgraded node: the stage bumps Execution to the commitment block in its own
// tx, so the publish that sets the pin still reads the old value. Dropping the
// pin there reports that old position, i.e. the backwards jump the pin prevents.
func TestBuildSyncingReplyHandoffSurvivesProgressBelowCommitmentBlock(t *testing.T) {
	n, tx := newSyncStateFixture(t, 6_000_000)
	n.NewLastBlockSeen(20_000_000)
	n.SetSnapshotDownloadHandoff(19_000_000)

	reply, err := n.BuildSyncingReply(tx, 0)
	require.NoError(t, err)
	require.Equal(t, uint64(19_000_000), reply.CurrentBlock)
	require.Equal(t, uint64(20_000_000), reply.LastNewBlockSeen)
	require.Empty(t, reply.Stages)
	require.NotNil(t, n.snapDownload.Load(), "handoff still pinned")
}
