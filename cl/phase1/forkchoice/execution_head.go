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

package forkchoice

import (
	"context"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

// GetExecutionHead resolves only a current head; FULL also requires a verified envelope.
func (f *ForkChoiceStore) GetExecutionHead(head ForkChoiceNode) (common.Hash, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.executionHeadLocked(head)
}

func (f *ForkChoiceStore) executionHeadLocked(head ForkChoiceNode) (common.Hash, bool) {
	if head.Root == (common.Hash{}) || head.Root != f.headHash || head.PayloadStatus != f.headPayloadStatus {
		return common.Hash{}, false
	}
	if f.beaconCfg.GetCurrentStateVersion(f.headSlot / f.beaconCfg.SlotsPerEpoch).Before(clparams.GloasVersion) {
		hash, ok := f.eth2Roots.Get(head.Root)
		return hash, ok
	}
	if head.PayloadStatus == cltypes.PayloadStatusFull && !f.IsPayloadVerified(head.Root) {
		return common.Hash{}, false
	}
	block, ok := f.forkGraph.GetBlock(head.Root)
	if !ok || block == nil {
		if head.Root == f.forkGraph.AnchorRoot() {
			hash, ok := f.eth2Roots.Get(head.Root)
			return hash, ok
		}
		return common.Hash{}, false
	}
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	if bid == nil || bid.Message == nil {
		return common.Hash{}, false
	}
	if head.PayloadStatus != cltypes.PayloadStatusFull {
		return bid.Message.ParentBlockHash, true
	}
	hash, ok := f.eth2Roots.Get(head.Root)
	return hash, ok && hash == bid.Message.BlockHash
}

// UpdateExecutionHead sends a canonical decision without holding the store lock over EL work.
func (f *ForkChoiceStore) UpdateExecutionHead(ctx context.Context, head ForkChoiceNode) (common.Hash, error) {
	f.mu.RLock()
	hash, ready := f.executionHeadLocked(head)
	if !ready {
		f.mu.RUnlock()
		return common.Hash{}, nil
	}
	if f.engine == nil {
		f.mu.RUnlock()
		return hash, nil
	}
	finalized := f.getFinalizedExecutionHashLocked(f.finalizedCheckpoint.Load().(solid.Checkpoint).Root)
	safe := f.getFinalizedExecutionHashLocked(f.justifiedCheckpoint.Load().(solid.Checkpoint).Root)
	version := f.beaconCfg.GetCurrentStateVersion(f.headSlot / f.beaconCfg.SlotsPerEpoch)
	engine := f.engine
	f.mu.RUnlock()

	if version.AfterOrEqual(clparams.GloasVersion) {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, time.Second)
		defer cancel()
	}
	if _, err := engine.ForkChoiceUpdate(ctx, finalized, safe, hash, nil, version); err != nil {
		return common.Hash{}, err
	}
	if current, ready := f.GetExecutionHead(head); !ready || current != hash {
		return common.Hash{}, nil
	}
	return hash, nil
}
