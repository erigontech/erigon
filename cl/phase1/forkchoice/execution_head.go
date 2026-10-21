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
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// GetExecutionHead resolves the execution hash for a Gloas fork-choice node.
// The boolean means the hash was resolved, not that the EL verified or adopted it.
// Canonical FCU permits an unverified FULL head; preparation checks verification separately.
func (f *ForkChoiceStore) GetExecutionHead(head ForkChoiceNode) (common.Hash, bool) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	block, ok := f.forkGraph.GetBlock(head.Root)
	if !ok || block == nil || block.Block.Body.Version < clparams.GloasVersion {
		return common.Hash{}, false
	}
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	if bid == nil || bid.Message == nil {
		return common.Hash{}, false
	}
	if head.PayloadStatus == cltypes.PayloadStatusFull {
		return bid.Message.BlockHash, true
	}
	return bid.Message.ParentBlockHash, true
}
