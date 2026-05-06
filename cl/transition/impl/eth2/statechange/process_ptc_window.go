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
	"github.com/erigontech/erigon/cl/cltypes/solid"
)

// ProcessPtcWindow shifts the PTC window forward by one epoch and fills the
// newly opened epoch with fresh PTC assignments.
//
// Spec: process_ptc_window
//   - Shift out the first epoch of entries (indices [0, SLOTS_PER_EPOCH))
//   - Compute next_epoch = current_epoch + MIN_SEED_LOOKAHEAD + 1
//   - For each slot in next_epoch, call GetPTC and store the result in the
//     last SLOTS_PER_EPOCH positions of the window.
func ProcessPtcWindow(s abstract.BeaconState) error {
	cfg := s.BeaconConfig()
	slotsPerEpoch := cfg.SlotsPerEpoch

	ptcWindow := s.GetPtcWindow()
	totalSlots := ptcWindow.Length() // (2 + MIN_SEED_LOOKAHEAD) * SLOTS_PER_EPOCH
	lastEpochStart := totalSlots - int(slotsPerEpoch)

	// Build a new window, shifting entries forward by one epoch.
	newWindow := solid.NewUint64VectorOfVectors(totalSlots, int(cfg.PtcSize))
	for i := 0; i < lastEpochStart; i++ {
		newWindow.Set(i, ptcWindow.Get(i+int(slotsPerEpoch)))
	}

	// Fill the last epoch with PTC for next_epoch.
	currentEpoch := s.Slot() / slotsPerEpoch
	nextEpoch := currentEpoch + cfg.MinSeedLookahead + 1
	nextEpochStartSlot := nextEpoch * slotsPerEpoch

	for i := uint64(0); i < slotsPerEpoch; i++ {
		ptc, err := s.ComputePTC(nextEpochStartSlot + i)
		if err != nil {
			return err
		}
		vec := solid.NewUint64VectorSSZ(int(cfg.PtcSize))
		for j, idx := range ptc {
			vec.Set(j, idx)
		}
		newWindow.Set(lastEpochStart+int(i), vec)
	}

	s.SetPtcWindow(newWindow)
	return nil
}
