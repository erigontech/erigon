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
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/common"
)

func (f *ForkChoiceStore) AttestationData(headRoot common.Hash, slot uint64) (solid.AttestationData, bool) {
	header, ok := f.forkGraph.GetHeader(headRoot)
	if !ok || header.Slot > slot {
		return solid.AttestationData{}, false
	}
	checkpoints, ok := f.finalityCheckpoints.Get(headRoot)
	if !ok {
		return solid.AttestationData{}, false
	}
	unrealized, ok := f.getUnrealizedJustification(headRoot)
	if !ok {
		return solid.AttestationData{}, false
	}

	epoch := slot / f.beaconCfg.SlotsPerEpoch
	headEpoch := header.Slot / f.beaconCfg.SlotsPerEpoch
	var source solid.Checkpoint
	switch {
	case headEpoch == epoch:
		source = checkpoints.currentJustifiedCheckpoint
	case headEpoch+1 == epoch:
		source = unrealized
	default:
		return solid.AttestationData{}, false
	}

	targetRoot := f.Ancestor(headRoot, epoch*f.beaconCfg.SlotsPerEpoch).Root
	if targetRoot == (common.Hash{}) {
		return solid.AttestationData{}, false
	}
	return solid.AttestationData{
		Slot:            slot,
		BeaconBlockRoot: headRoot,
		Source:          source,
		Target:          solid.Checkpoint{Epoch: epoch, Root: targetRoot},
	}, true
}
