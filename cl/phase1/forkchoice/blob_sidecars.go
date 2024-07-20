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

package forkchoice

import (
	"github.com/erigontech/erigon/cl/cltypes"
)

func (f *ForkChoiceStore) AddPreverifiedBlobSidecar(blobSidecar *cltypes.BlobSidecar) error {
	blockRoot, err := blobSidecar.SignedBlockHeader.Header.HashSSZ()
	if err != nil {
		return err
	}

	// operation is not thread safe from here.
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, sidecar := range f.hotSidecars[blockRoot] {
		if sidecar.SignedBlockHeader.Header.Slot == blobSidecar.SignedBlockHeader.Header.Slot &&
			sidecar.SignedBlockHeader.Header.ProposerIndex == blobSidecar.SignedBlockHeader.Header.ProposerIndex &&
			sidecar.Index == blobSidecar.Index {
			return nil // ignore if we already have it
		}
	}
	f.hotSidecars[blockRoot] = append(f.hotSidecars[blockRoot], blobSidecar)

	blobsMaxAge := 4 // a slot can live for up to 4 slots in the pool of hot sidecars.
	currentSlot := f.highestSeen.Load()
	var pruneSlot uint64
	if currentSlot > uint64(blobsMaxAge) {
		pruneSlot = currentSlot - uint64(blobsMaxAge)
	}
	// also clean up all old blobs that may have been accumulating
	for blockRoot := range f.hotSidecars {
		if len(f.hotSidecars) == 0 || f.hotSidecars[blockRoot][0].SignedBlockHeader.Header.Slot < pruneSlot {
			delete(f.hotSidecars, blockRoot)
		}
	}

	return nil
}
