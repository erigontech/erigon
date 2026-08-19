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
	"fmt"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// requireDataColumnAvailability accepts only materialized PeerDAS columns. EL
// blob evidence alone is not enough for the CL to sample or serve those columns.
func (f *ForkChoiceStore) requireDataColumnAvailability(block *cltypes.SignedBeaconBlock, blockRoot common.Hash) error {
	if f.peerDas == nil {
		return fmt.Errorf("%w: peer DAS is not configured", ErrEIP7594ColumnDataNotAvailable)
	}
	available, err := f.peerDas.IsDataAvailable(block.Block.Slot, blockRoot)
	if err != nil {
		return fmt.Errorf("%w: failed to check data column availability: %w", ErrEIP7594ColumnDataNotAvailable, err)
	}
	if available {
		return nil
	}
	if err := f.peerDas.SyncColumnDataLater(block); err != nil {
		log.Warn("failed to schedule data column sync", "slot", block.Block.Slot, "blockRoot", blockRoot, "err", err)
	}
	return ErrEIP7594ColumnDataNotAvailable
}
