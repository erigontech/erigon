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

package statechange

import (
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/monitor"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/utils"
)

// ProcessHistoricalRootsUpdate updates the historical root data structure by computing a new historical root batch when it is time to do so.
func ProcessHistoricalRootsUpdate(s abstract.BeaconState) error {
	defer monitor.ObserveElaspedTime(monitor.ProcessHistoricalRootsUpdateTime).End()
	nextEpoch := state.Epoch(s) + 1
	beaconConfig := s.BeaconConfig()
	blockRoots := s.BlockRoots()
	stateRoots := s.StateRoots()

	// Check if it's time to compute the historical root batch.
	if nextEpoch%(beaconConfig.SlotsPerHistoricalRoot/beaconConfig.SlotsPerEpoch) != 0 {
		return nil
	}

	// Compute historical root batch.
	blockRootsLeaf, err := blockRoots.HashSSZ()
	if err != nil {
		return err
	}
	stateRootsLeaf, err := stateRoots.HashSSZ()
	if err != nil {
		return err
	}

	// Add the historical summary or root to the s.
	if s.Version() >= clparams.CapellaVersion {
		s.AddHistoricalSummary(&cltypes.HistoricalSummary{
			BlockSummaryRoot: blockRootsLeaf,
			StateSummaryRoot: stateRootsLeaf,
		})
	} else {
		historicalRoot := utils.Sha256(blockRootsLeaf[:], stateRootsLeaf[:])
		s.AddHistoricalRoot(historicalRoot)
	}

	return nil
}
