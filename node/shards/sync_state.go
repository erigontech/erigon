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
	"google.golang.org/protobuf/proto"

	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

// syncedReorgRange is the distance from the highest seen block within which
// the node is considered synced: closer than a reorg's depth means there is
// no meaningful catching-up left to report.
const syncedReorgRange = 8

// PublishSyncState builds the current sync status and notifies subscribers if
// it changed. Building under the same lock as the compare-and-publish keeps
// events ordered by construction time, so a state built from an older tx can
// never overwrite a fresher one. Once synced, block numbers keep advancing on
// every cycle but subscribers only care about the flag flipping back —
// comparing full replies would republish on every block forever.
func (n *Notifications) PublishSyncState(tx kv.Getter, frozenBlocks uint64) error {
	n.syncStateLock.Lock()
	defer n.syncStateLock.Unlock()
	reply, err := n.BuildSyncingReply(tx, frozenBlocks)
	if err != nil {
		return err
	}
	n.repinStartingBlock(reply)
	stillSynced := n.lastSyncState != nil && !n.lastSyncState.Syncing && !reply.Syncing
	if stillSynced || proto.Equal(n.lastSyncState, reply) {
		return nil
	}
	n.lastSyncState = reply
	n.Events.OnNewSyncState(reply)
	return nil
}

// SubscribeSyncState registers a sync-state subscription and returns a seed:
// the last published state, or one built from tx before the first publish.
// Registering and seeding under the publish lock totally orders the seed
// against the event stream: a state published before subscribing is in the
// seed, one published after arrives on the channel. The built seed does not
// become the dedup baseline — other subscribers never saw it as an event.
func (n *Notifications) SubscribeSyncState(tx kv.Getter, frozenBlocks uint64) (chan *remoteproto.SyncingReply, *remoteproto.SyncingReply, func(), error) {
	n.syncStateLock.Lock()
	defer n.syncStateLock.Unlock()
	ch, clean := n.Events.AddSyncStateSubscription()
	seed := n.lastSyncState
	if seed == nil {
		var err error
		seed, err = n.BuildSyncingReply(tx, frozenBlocks)
		if err != nil {
			clean()
			return nil, nil, nil, err
		}
	}
	return ch, seed, clean, nil
}

// repinStartingBlock moves the block the current sync session started from: up
// when a new session begins, down when an unwind takes execution below it. Only
// the publish path may move it, since polls read unordered tx views.
func (n *Notifications) repinStartingBlock(reply *remoteproto.SyncingReply) {
	if reply.Syncing && (n.lastSyncState == nil || !n.lastSyncState.Syncing) {
		n.startingBlock.Store(reply.CurrentBlock)
	}
	n.withStartingBlock(reply)
	n.startingBlock.Store(reply.StartingBlock)
}

// withStartingBlock reports the pin clamped to the current block: an unwind can
// take execution below the pin, and a starting block above the current one
// makes the progress ratio negative.
func (n *Notifications) withStartingBlock(reply *remoteproto.SyncingReply) *remoteproto.SyncingReply {
	reply.StartingBlock = min(n.startingBlock.Load(), reply.CurrentBlock)
	return reply
}

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
		LastNewBlockSeen: max(currentBlock, highestBlock),
		Syncing:          true,
	}

	if snap := n.snapDownload.Load(); snap != nil {
		switch snap.phase {
		case snapHandoff:
			// Stop reporting the pin once this tx sees execution at the commitment
			// block, but leave the state alone: the observing tx can be the pipeline's
			// own rw tx, ahead of the committed view every poller reads. Ending the
			// handoff belongs to the pipeline exit that owns it.
			if currentBlock < snap.commitBlock {
				reply.CurrentBlock = snap.commitBlock
				reply.LastNewBlockSeen = max(snap.commitBlock, highestBlock)
				return n.withStartingBlock(reply), nil
			}
		case snapDownloading:
			// Map the byte-completion ratio onto the block-based fields so dashboards
			// show smooth progress: currentBlock = ratio * blocks_to_be_downloaded.
			// The ratio scales the snapshots' target, not the live head, which an FCU
			// can push past what the snapshots actually cover. Committed progress is
			// the floor: a node downloading further snapshots must not report a
			// position below the one it already reached.
			ratio := float64(snap.done) / float64(snap.total)
			reply.CurrentBlock = max(currentBlock, uint64(ratio*float64(snap.targetBlock)))
			reply.LastNewBlockSeen = max(reply.CurrentBlock, snap.targetBlock, highestBlock)
			return n.withStartingBlock(reply), nil
		}
	}

	if highestBlock == 0 {
		return n.withStartingBlock(reply), nil
	}

	distance := max(highestBlock, currentBlock) - min(highestBlock, currentBlock)
	if distance < syncedReorgRange {
		reply.Syncing = false
		return n.withStartingBlock(reply), nil
	}

	reply.Stages = make([]*remoteproto.SyncingReply_StageProgress, len(stages.AllStages))
	for i, stage := range stages.AllStages {
		progress, err := stages.GetStageProgress(tx, stage)
		if err != nil {
			return nil, err
		}
		reply.Stages[i] = &remoteproto.SyncingReply_StageProgress{StageName: string(stage), BlockNumber: progress}
	}
	return n.withStartingBlock(reply), nil
}
