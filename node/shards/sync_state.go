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
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// syncedReorgRange is the distance from the highest seen block within which
// the node is considered synced: closer than a reorg's depth means there is
// no meaningful catching-up left to report.
const syncedReorgRange = 8

// BuildSyncingReply computes the sync status served by eth_syncing and
// published on the SYNCING event stream. While the highest block is still
// unknown (e.g. snapshots are downloading) it reports syncing with no stage
// detail.
func (n *Notifications) BuildSyncingReply(tx kv.Getter, frozenBlocks uint64) (*remoteproto.SyncingReply, error) {
	highestBlock := max(n.LastNewBlockSeen.Load(), frozenBlocks)

	currentBlock, err := stages.GetStageProgress(tx, stages.Execution)
	if err != nil {
		return nil, err
	}

	reply := &remoteproto.SyncingReply{
		CurrentBlock:     currentBlock,
		FrozenBlocks:     frozenBlocks,
		LastNewBlockSeen: highestBlock,
		Syncing:          true,
	}

	if highestBlock == 0 {
		return reply, nil
	}

	var distance uint64
	if highestBlock > currentBlock {
		distance = highestBlock - currentBlock
	} else {
		distance = currentBlock - highestBlock
	}
	if distance < syncedReorgRange {
		reply.Syncing = false
		return reply, nil
	}

	reply.Stages = make([]*remoteproto.SyncingReply_StageProgress, len(stages.AllStages))
	for i, stage := range stages.AllStages {
		progress, err := stages.GetStageProgress(tx, stage)
		if err != nil {
			return nil, err
		}
		reply.Stages[i] = &remoteproto.SyncingReply_StageProgress{StageName: string(stage), BlockNumber: progress}
	}
	return reply, nil
}
