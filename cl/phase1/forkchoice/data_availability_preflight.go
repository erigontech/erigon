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
	"errors"
	"fmt"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/transition/impl/eth2"
	"github.com/erigontech/erigon/common"
)

type dataAvailabilityPreflightParent struct {
	state *state.CachingBeaconState
	slot  uint64
}

// PreflightDataAvailabilityBlocks validates downloaded blocks before network column acquisition.
func (f *ForkChoiceStore) PreflightDataAvailabilityBlocks(blocks []*cltypes.SignedBeaconBlock) (skipNonFinalized []bool, retryable bool, err error) {
	f.mu.RLock()
	defer f.mu.RUnlock()

	skipNonFinalized = make([]bool, len(blocks))
	batchParents := make(map[common.Hash]dataAvailabilityPreflightParent, len(blocks))
	skippedRoots := make(map[common.Hash]struct{})
	finalizedCheckpoint := f.FinalizedCheckpoint()
	finalizedSlot := finalizedCheckpoint.Epoch * f.beaconCfg.SlotsPerEpoch
	if anchorSlot := f.forkGraph.AnchorSlot(); finalizedSlot < anchorSlot {
		finalizedSlot = anchorSlot
	}
	currentSlot := f.Slot()

	for i, block := range blocks {
		blockRoot, hashErr := block.Block.HashSSZ()
		if hashErr != nil {
			return nil, false, fmt.Errorf("hash block: %w", hashErr)
		}
		root := common.Hash(blockRoot)
		if block.Block.Slot > currentSlot {
			return nil, false, fmt.Errorf("block slot %d is later than current slot %d", block.Block.Slot, currentSlot)
		}
		if !f.beaconCfg.ForkSchemaMatchesSlot(block.Block.Slot, block.Version()) {
			return nil, false, ErrForkSchemaSlotMismatch
		}
		if _, parentSkipped := skippedRoots[block.Block.ParentRoot]; parentSkipped {
			skipNonFinalized[i] = true
			skippedRoots[root] = struct{}{}
			continue
		}

		parent, parentInBatch := batchParents[block.Block.ParentRoot]
		if !parentInBatch {
			parentHeader, parentKnown := f.forkGraph.GetHeader(block.Block.ParentRoot)
			if !parentKnown {
				return nil, true, fmt.Errorf("%w: parent header %v is unavailable", ErrMissingSegment, block.Block.ParentRoot)
			}
			parent.slot = parentHeader.Slot
			if block.Block.Slot > finalizedSlot && f.Ancestor(block.Block.ParentRoot, finalizedSlot).Root != finalizedCheckpoint.Root {
				skipNonFinalized[i] = true
				skippedRoots[root] = struct{}{}
				continue
			}
			parent.state, err = f.forkGraph.GetState(block.Block.ParentRoot, false)
			if err != nil {
				return nil, true, fmt.Errorf("get parent state: %w", err)
			}
			if parent.state == nil {
				return nil, true, fmt.Errorf("%w: parent state %v is unavailable", ErrMissingSegment, block.Block.ParentRoot)
			}
		}
		if parent.slot >= block.Block.Slot {
			return nil, false, fmt.Errorf("parent slot %d is not earlier than block slot %d", parent.slot, block.Block.Slot)
		}
		valid, signatureErr := eth2.VerifyBlockSignature(parent.state, block)
		if signatureErr != nil {
			return nil, false, fmt.Errorf("verify block signature: %w", signatureErr)
		}
		if !valid {
			return nil, false, errors.New("invalid block signature")
		}
		stateEpoch := parent.state.Slot() / f.beaconCfg.SlotsPerEpoch
		blockEpoch := block.Block.Slot / f.beaconCfg.SlotsPerEpoch
		if parent.state.Version() >= clparams.FuluVersion && blockEpoch <= stateEpoch+f.beaconCfg.MinSeedLookahead {
			expectedProposer, proposerErr := parent.state.GetBeaconProposerIndexForSlot(block.Block.Slot)
			if proposerErr != nil {
				return nil, true, fmt.Errorf("compute proposer for slot %d: %w", block.Block.Slot, proposerErr)
			}
			if block.Block.ProposerIndex != expectedProposer {
				return nil, false, fmt.Errorf("unexpected proposer %d for slot %d, expected %d", block.Block.ProposerIndex, block.Block.Slot, expectedProposer)
			}
		}
		batchParents[root] = dataAvailabilityPreflightParent{state: parent.state, slot: block.Block.Slot}
	}
	return skipNonFinalized, false, nil
}
