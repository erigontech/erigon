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
