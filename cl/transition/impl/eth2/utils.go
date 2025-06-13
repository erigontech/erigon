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

package eth2

import (
	"encoding/binary"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cl/abstract"
	"github.com/erigontech/erigon/cl/utils"
)

func computeSigningRootEpoch(epoch uint64, domain []byte) (common.Hash, error) {
	b := make([]byte, 32)
	binary.LittleEndian.PutUint64(b, epoch)
	return utils.Sha256(b, domain), nil
}

// transitionSlot is called each time there is a new slot to process
func transitionSlot(s abstract.BeaconState) error {
	slot := s.Slot()
	previousStateRoot := s.PreviousStateRoot()
	var err error
	if previousStateRoot == (common.Hash{}) {
		previousStateRoot, err = s.HashSSZ()
		if err != nil {
			return err
		}
	}

	beaconConfig := s.BeaconConfig()

	s.SetStateRootAt(int(slot%beaconConfig.SlotsPerHistoricalRoot), previousStateRoot)

	latestBlockHeader := s.LatestBlockHeader()
	if latestBlockHeader.Root == [32]byte{} {
		latestBlockHeader.Root = previousStateRoot
		s.SetLatestBlockHeader(&latestBlockHeader)
	}
	blockHeader := s.LatestBlockHeader()

	previousBlockRoot, err := (&blockHeader).HashSSZ()
	if err != nil {
		return err
	}
	s.SetBlockRootAt(int(slot%beaconConfig.SlotsPerHistoricalRoot), previousBlockRoot)
	return nil
}
