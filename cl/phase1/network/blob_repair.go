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

	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"

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

// repairCooldown spaces out retries per slot. A gap no endpoint can serve fails the same
// way every tick, so without this the drain re-attempts the oldest slots forever and never
// reaches the ones further up that could be filled.
type repairCooldown struct {
	skip    map[uint64]int
	streak  map[uint64]int
	maxSkip int
}

func newRepairCooldown() *repairCooldown {
	return &repairCooldown{skip: map[uint64]int{}, streak: map[uint64]int{}, maxSkip: 60}
}

// ready consumes one tick of cooldown for slot and reports whether it may be attempted.
func (c *repairCooldown) ready(slot uint64) bool {
	if left := c.skip[slot]; left > 0 {
		c.skip[slot] = left - 1
		return false
	}
	return true
}

func (c *repairCooldown) failed(slot uint64) {
	c.streak[slot]++
	shift := min(c.streak[slot]-1, 30)
	c.skip[slot] = min(1<<shift, c.maxSkip)
}

func (c *repairCooldown) filled(slot uint64) {
	delete(c.skip, slot)
	delete(c.streak, slot)
}

// drainBlobGaps repairs at most limit slots, oldest first, and reports how many it filled
// and how many it tried. A slot no endpoint can serve is attempted and not filled; it does
// not stop the drain.
func drainBlobGaps(slots []uint64, limit int, cool *repairCooldown, repair func(slot uint64) bool) (filled, attempted, skipped int) {
	if limit <= 0 {
		return 0, 0, 0
	}
	for _, slot := range slots {
		if attempted == limit {
			break
		}
		if cool != nil && !cool.ready(slot) {
			skipped++
			continue
		}
		attempted++
		if repair(slot) {
			filled++
			if cool != nil {
				cool.filled(slot)
			}
			continue
		}
		if cool != nil {
			cool.failed(slot)
		}
	}
	return filled, attempted, skipped
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
		// The column walk hands over gaps at its own pace - days, on an archive node -
		// so an empty set means scan, not idle.
		b.seedBlobGapsFromStore()
		slots = b.BlobGapSlots()
		if len(slots) == 0 {
			return
		}
	}

	filled, attempted, skipped := drainBlobGaps(slots, maxBlobRepairsPerPass, b.cooldown, b.repairBlobGap)
	remaining := len(slots) - filled
	b.logger.Info("[BlobRepair] Fetched blob sidecars peers no longer serve",
		"filled", filled, "attempted", attempted, "coolingOff", skipped, "remaining", remaining)
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
	// Not block.HashSSZ(): a block read out of a beaconblocks segment has no execution
	// payload, so hashing it yields a root that never existed on chain. The blob store and
	// the beacon API are both keyed by the canonical root.
	blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
	if err != nil || blockRoot == (common.Hash{}) {
		b.logger.Debug("[BlobRepair] no canonical root", "slot", slot, "err", err)
		return false
	}

	sidecars, err := b.remoteBlobs.fetch(b.ctx, blockRoot)
	if err != nil {
		b.logger.Debug("[BlobRepair] fetch failed", "slot", slot, "blockRoot", blockRoot, "err", err)
		return false
	}
	if len(sidecars) == 0 {
		b.logger.Debug("[BlobRepair] no endpoint had sidecars", "slot", slot, "blockRoot", blockRoot)
		return false
	}

	identifiers, err := BlobsIdentifiersFromBlocks([]*cltypes.SignedBeaconBlock{block}, b.beaconCfg)
	if err != nil {
		b.logger.Debug("[BlobRepair] build identifiers", "slot", slot, "err", err)
		return false
	}
	inserted, total, err := blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(
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
	)
	if err != nil {
		b.logger.Warn("[BlobRepair] rejected fetched sidecars", "slot", slot, "err", err)
		return false
	}

	stored, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
	if err != nil {
		b.logger.Debug("[BlobRepair] count after insert", "slot", slot, "err", err)
		return false
	}
	want := 0
	if c := block.Block.Body.GetBlobKzgCommitments(); c != nil {
		want = c.Len()
	}
	if int(stored) != want {
		// Fetched and accepted, yet the store still disagrees with the block. Left silent
		// once, which made 101 successful fetches on a gnosis node look like misses.
		b.logger.Warn("[BlobRepair] insert did not complete the slot",
			"slot", slot, "blockRoot", blockRoot, "stored", stored, "want", want,
			"fetched", len(sidecars), "inserted", inserted, "identifiers", total)
		return false
	}
	b.logger.Debug("[BlobRepair] filled slot", "slot", slot, "sidecars", len(sidecars))
	return true
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

// seedBlobGapsFromStore populates the gap set by reading the store directly, between the
// frozen frontier and the head the downloader is walking from.
func (b *BlobHistoryDownloader) seedBlobGapsFromStore() {
	from, to := b.sn.FrozenBlobs(), b.headSlot.Load()
	if from == 0 || to <= from {
		return
	}
	tx, err := b.indiciesDB.BeginRo(b.ctx)
	if err != nil {
		b.logger.Debug("[BlobRepair] scan: begin tx", "err", err)
		return
	}
	defer tx.Rollback()

	started := time.Now()
	gaps := scanRangeForGaps(from, to, func(slot uint64) (int, int, error) {
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil || blockRoot == (common.Hash{}) {
			return 0, 0, errors.New("no canonical root")
		}
		block, err := b.blockReader.ReadBeaconBlockBodyBySlot(b.ctx, tx, slot)
		if err != nil || block == nil {
			return 0, 0, errors.New("no block")
		}
		want := 0
		if c := block.Block.Body.GetBlobKzgCommitments(); c != nil {
			want = c.Len()
		}
		stored, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
		if err != nil {
			return 0, 0, err
		}
		return int(stored), want, nil
	})
	b.recordBlobGaps(gaps)
	b.logger.Info("[BlobRepair] Scanned the blob store for gaps",
		"from", from, "to", to, "gaps", len(gaps), "took", time.Since(started))
}
