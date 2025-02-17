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
	"github.com/erigontech/erigon/cl/cltypes"
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
