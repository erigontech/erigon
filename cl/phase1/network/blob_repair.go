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
	"errors"
	"time"

	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
)

// maxBlobRepairsPerPass bounds how many gap slots one pass will fetch. A backlog of
// thousands against a per-request timeout would otherwise occupy a single pass for hours;
// the remainder is carried to the next one so progress stays steady and visible.
const maxBlobRepairsPerPass = 256

// blobRepairInterval paces the drain. Each tick fetches at most maxBlobRepairsPerPass
// slots, so this bounds the load a repair puts on the endpoints it reads from.
const blobRepairInterval = 60 * time.Second

// drainBlobGaps repairs at most limit slots, oldest first, and reports how many it filled
// and how many it tried. A slot no endpoint can serve is attempted and not filled; it does
// not stop the drain.
func drainBlobGaps(slots []uint64, limit int, repair func(slot uint64) bool) (filled, attempted int) {
	if limit <= 0 {
		return 0, 0
	}
	for _, slot := range slots {
		if attempted == limit {
			break
		}
		attempted++
		if repair(slot) {
			filled++
		}
	}
	return filled, attempted
}

func (b *BlobHistoryDownloader) tryBeginRepair() bool {
	return b.repairing.CompareAndSwap(false, true)
}

func (b *BlobHistoryDownloader) endRepair() { b.repairing.Store(false) }

// repairBlobGapsFromRemote drains the recorded gaps through the configured beacon-API
// endpoints. Sidecars are verified against the block before they reach the store, so an
// endpoint cannot poison it.
func (b *BlobHistoryDownloader) repairBlobGapsFromRemote() {
	if !b.remoteBlobs.enabled() {
		return
	}
	if !b.tryBeginRepair() {
		return
	}
	defer b.endRepair()
	slots := b.BlobGapSlots()
	if len(slots) == 0 {
		return
	}

	filled, attempted := drainBlobGaps(slots, maxBlobRepairsPerPass, b.repairBlobGap)
	remaining := len(slots) - filled
	b.logger.Info("[BlobRepair] Fetched blob sidecars peers no longer serve",
		"filled", filled, "attempted", attempted, "remaining", remaining)
}

// repairBlobGap fetches and stores one slot's sidecars, reporting whether the store now
// holds the full set.
func (b *BlobHistoryDownloader) repairBlobGap(slot uint64) bool {
	tx, err := b.indiciesDB.BeginRo(b.ctx)
	if err != nil {
		b.logger.Debug("[BlobRepair] begin tx", "slot", slot, "err", err)
		return false
	}
	defer tx.Rollback()

	block, err := b.blockReader.ReadBeaconBlockBodyBySlot(b.ctx, tx, slot)
	if err != nil || block == nil {
		b.logger.Debug("[BlobRepair] block unavailable", "slot", slot, "err", err)
		return false
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		b.logger.Debug("[BlobRepair] hash block", "slot", slot, "err", err)
		return false
	}

	sidecars, err := b.remoteBlobs.fetch(b.ctx, blockRoot)
	if err != nil || len(sidecars) == 0 {
		return false
	}

	identifiers, err := BlobsIdentifiersFromBlocks([]*cltypes.SignedBeaconBlock{block}, b.beaconCfg)
	if err != nil {
		b.logger.Debug("[BlobRepair] build identifiers", "slot", slot, "err", err)
		return false
	}
	if _, _, err := blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(
		b.ctx, b.blobStorage, identifiers, sidecars,
		func(header *cltypes.SignedBeaconBlockHeader) error {
			if header.Header.Slot != slot {
				return errors.New("sidecar header slot does not match the requested slot")
			}
			if header.Signature != block.Signature {
				return errors.New("sidecar header signature does not match the stored block")
			}
			return nil
		},
	); err != nil {
		b.logger.Warn("[BlobRepair] rejected fetched sidecars", "slot", slot, "err", err)
		return false
	}

	stored, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
	if err != nil {
		return false
	}
	want := 0
	if c := block.Block.Body.GetBlobKzgCommitments(); c != nil {
		want = c.Len()
	}
	return int(stored) == want
}

// repairLoop drains gaps on its own schedule. downloadOnce holds run()'s goroutine for as
// long as a full walk takes - hours on an archive node - so a repair driven from that
// loop's select would never fire while there was anything to repair.
func (b *BlobHistoryDownloader) repairLoop() {
	if !b.remoteBlobs.enabled() {
		return
	}
	ticker := time.NewTicker(blobRepairInterval)
	defer ticker.Stop()
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-ticker.C:
			b.repairBlobGapsFromRemote()
		}
	}
}
