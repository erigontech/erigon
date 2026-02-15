// Copyright 2024 The Erigon Authors
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

package machine

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
)

// TransitionState will call impl..ProcessSlots, then impl.VerifyBlockSignature, then ProcessBlock, then impl.VerifyTransition
func TransitionState(impl Interface, s abstract.BeaconState, block *cltypes.SignedBeaconBlock) error {
	currentBlock := block.Block
	if err := impl.ProcessSlots(s, currentBlock.Slot); err != nil {
		return err
	}

	if err := impl.VerifyBlockSignature(s, block); err != nil {
		return err
	}

	// Transition block
	if err := ProcessBlock(impl, s, block.Block); err != nil {
		return err
	}

	// perform validation
	if err := impl.VerifyTransition(s, currentBlock); err != nil {
		return err
	}

	// if validation is successful, transition
	s.SetPreviousStateRoot(currentBlock.StateRoot)
	return nil
}

// PayloadStateTransitionNoStore simulates execution payload delivery for Gloas/EIP-7732.
// In EIP-7732, execution payloads arrive via separate envelopes after the beacon block.
// This function mirrors the pyspec's payload_state_transition_no_store helper by:
// 1. Caching the state root into latest_block_header.state_root
// 2. Updating latest_block_hash to the committed header's block_hash
// 3. Clearing PreviousStateRoot so the next slot recomputes it after these mutations
// Callers should invoke this after TransitionState when simulating payload delivery.
func PayloadStateTransitionNoStore(s abstract.BeaconState, block *cltypes.BeaconBlock) {
	if s.Version() < clparams.GloasVersion || block.Body.SignedExecutionPayloadBid == nil {
		return
	}
	latestBlockHeader := s.LatestBlockHeader()
	if latestBlockHeader.Root == (common.Hash{}) {
		latestBlockHeader.Root = block.StateRoot
		s.SetLatestBlockHeader(&latestBlockHeader)
	}
	s.SetLatestBlockHash(block.Body.SignedExecutionPayloadBid.Message.BlockHash)
	s.SetPreviousStateRoot(common.Hash{})
}
