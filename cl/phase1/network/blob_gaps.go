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

package network

import (
	"slices"

	"github.com/erigontech/erigon/cl/cltypes"
)

// incompleteAfterAttempt re-reads the store for each block the pass just tried to recover
// and returns the slots whose sidecars are still missing or only partly there. A partial
// set counts: DumpBlobSidecarsRange needs every commitment's sidecar to dump the range.
func (b *BlobHistoryDownloader) incompleteAfterAttempt(batch []*cltypes.SignedBeaconBlock) ([]uint64, error) {
	var incomplete []uint64
	for _, block := range batch {
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		stored, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
		if err != nil {
			return nil, err
		}
		want := 0
		if c := block.Block.Body.GetBlobKzgCommitments(); c != nil {
			want = c.Len()
		}
		if int(stored) != want {
			incomplete = append(incomplete, block.Block.Slot)
		}
	}
	return incomplete, nil
}

func (b *BlobHistoryDownloader) resetBlobGaps() {
	b.gapsMu.Lock()
	defer b.gapsMu.Unlock()
	b.gapSlots = nil
}

func (b *BlobHistoryDownloader) recordBlobGaps(slots []uint64) {
	if len(slots) == 0 {
		return
	}
	b.gapsMu.Lock()
	defer b.gapsMu.Unlock()
	if b.gapSlots == nil {
		b.gapSlots = map[uint64]struct{}{}
	}
	for _, slot := range slots {
		b.gapSlots[slot] = struct{}{}
	}
}

// BlobGapSlots returns the slots the last completed pass could not fill, sorted. It is the
// work list for an out-of-band repair: these slots are past what peers will serve.
func (b *BlobHistoryDownloader) BlobGapSlots() []uint64 {
	b.gapsMu.RLock()
	defer b.gapsMu.RUnlock()
	slots := make([]uint64, 0, len(b.gapSlots))
	for slot := range b.gapSlots {
		slots = append(slots, slot)
	}
	slices.Sort(slots)
	return slots
}

func (b *BlobHistoryDownloader) blobGapSummary() (count int, lowest, highest uint64) {
	slots := b.BlobGapSlots()
	if len(slots) == 0 {
		return 0, 0, 0
	}
	return len(slots), slots[0], slots[len(slots)-1]
}
