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

package das

import (
	"context"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// maxBackfillQueued bounds the column sync queue while a backfill adds to it, so the backfill
// neither crowds out the roots gossip queues nor asks peers for a whole range at once.
const maxBackfillQueued = 32

type slotRange struct{ from, to uint64 }

// backfillColumnBlock is the part of a block the column sync needs, so a backfill does not keep
// whole blocks queued.
type backfillColumnBlock struct {
	version     clparams.StateVersion
	slot        uint64
	root        common.Hash
	commitments *solid.ListSSZ[*cltypes.KZGCommitment]
	signature   common.Bytes96
}

func (b *backfillColumnBlock) Version() clparams.StateVersion {
	return b.version
}

func (b *backfillColumnBlock) GetSlot() uint64 {
	return b.slot
}

func (b *backfillColumnBlock) BlockHashSSZ() ([32]byte, error) {
	return b.root, nil
}

func (b *backfillColumnBlock) GetBlobKzgCommitments() *solid.ListSSZ[*cltypes.KZGCommitment] {
	return b.commitments
}

func (b *backfillColumnBlock) BlockSignature() common.Bytes96 {
	return b.signature
}

// BackfillCustodyColumns queues the custody columns that the stored blocks between fromSlot and
// toSlot miss. It returns at once; a request that comes while another one waits is merged into it.
func (d *peerdas) BackfillCustodyColumns(fromSlot, toSlot uint64) {
	if toSlot < fromSlot {
		return
	}
	d.backfillMu.Lock()
	if d.backfillPending == nil {
		d.backfillPending = &slotRange{from: fromSlot, to: toSlot}
	} else {
		d.backfillPending.from = min(d.backfillPending.from, fromSlot)
		d.backfillPending.to = max(d.backfillPending.to, toSlot)
	}
	d.backfillMu.Unlock()
	select {
	case d.backfills <- struct{}{}:
	default: // the worker is woken already
	}
}

// takeBackfillRequest returns the waiting request, if any.
func (d *peerdas) takeBackfillRequest() (slotRange, bool) {
	d.backfillMu.Lock()
	defer d.backfillMu.Unlock()
	if d.backfillPending == nil {
		return slotRange{}, false
	}
	r := *d.backfillPending
	d.backfillPending = nil
	return r, true
}

func (d *peerdas) custodyBackfillWorker(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-d.backfills:
			if r, ok := d.takeBackfillRequest(); ok {
				d.backfillCustodyColumns(ctx, r.from, r.to)
			}
		}
	}
}

// backfillCustodyColumns walks the canonical chain from toSlot down to fromSlot, within the column
// serve range, and queues the blocks that commit to blobs and miss their columns for the column
// sync worker. It waits while the queue holds maxBackfillQueued roots below the maximum backoff:
// roots at the maximum backoff are retried once an epoch and may never complete, so they do not
// hold the walk. backfillSideBranchBlocks queues the stored blocks that are not canonical at that
// moment, before the wait for the canonical chain and again after it, since a canonical root can
// be reorged out during the wait.
func (d *peerdas) backfillCustodyColumns(ctx context.Context, fromSlot, toSlot uint64) {
	if d.indiciesDB == nil || d.blockReader == nil {
		return
	}
	currentSlot := d.ethClock.GetCurrentSlot()
	if currentSlot/d.beaconConfig.SlotsPerEpoch < d.beaconConfig.FuluForkEpoch {
		return // no block has columns before Fulu
	}
	fromSlot = max(fromSlot, d.beaconConfig.DataColumnSidecarServeRangeStartSlot(currentSlot))
	d.backfillSideBranchBlocks(ctx, fromSlot, toSlot)
	if !d.waitForCanonicalSlot(ctx, toSlot) {
		return
	}
	d.backfillSideBranchBlocks(ctx, fromSlot, toSlot)
	queued := 0
	for slot := toSlot; slot >= fromSlot; slot-- {
		for d.blocksToCheckSync.activeLen() >= maxBackfillQueued {
			select {
			case <-ctx.Done():
				return
			case <-time.After(d.deferredColumnSyncTick()):
			}
		}
		if ctx.Err() != nil {
			return
		}
		// A slot skipped on an error keeps its custody gap: nothing revisits it, so it is logged.
		block, err := d.backfillBlock(ctx, slot)
		if err != nil {
			log.Warn("[custodyBackfill] failed to read the block, skipping it", "slot", slot, "err", err)
		} else if block != nil {
			available, err := d.IsDataAvailable(slot, block.root)
			switch {
			case err != nil:
				log.Warn("[custodyBackfill] failed to check the stored columns, skipping the block", "slot", slot, "blockRoot", block.root, "err", err)
			case !available:
				d.blocksToCheckSync.add(block.root, block, time.Now())
				queued++
			}
		}
		if slot == 0 {
			break
		}
	}
	if queued > 0 {
		log.Info("[custodyBackfill] queued blocks missing custody columns", "from", fromSlot, "to", toSlot, "blocks", queued)
	}
}

// waitForCanonicalSlot waits until the canonical chain reaches slot: forward sync stores its blocks
// before the fork choice stage marks them canonical. After two epochs it stops waiting and lets
// the backfill walk the chain as marked; slots without a canonical root are skipped. It reports
// false only when ctx ends.
func (d *peerdas) waitForCanonicalSlot(ctx context.Context, slot uint64) bool {
	deadline := time.Now().Add(2 * time.Duration(d.beaconConfig.SlotsPerEpoch) * d.slotDuration())
	for time.Now().Before(deadline) {
		var headSlot uint64
		if err := d.indiciesDB.View(ctx, func(tx kv.Tx) error {
			var err error
			headSlot, _, err = beacon_indicies.ReadCanonicalHead(tx)
			return err
		}); err == nil && headSlot >= slot {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-time.After(d.deferredColumnSyncTick()):
		}
	}
	return ctx.Err() == nil
}

// backfillBlock returns the canonical block at slot, read without its execution payload, if
// newBackfillColumnBlock accepts it. Its root comes from the canonical index, since such a body
// does not hash to it.
func (d *peerdas) backfillBlock(ctx context.Context, slot uint64) (*backfillColumnBlock, error) {
	var block *backfillColumnBlock
	err := d.indiciesDB.View(ctx, func(tx kv.Tx) error {
		root, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil || root == (common.Hash{}) {
			return err
		}
		body, err := d.blockReader.ReadBeaconBlockBodyBySlot(ctx, tx, slot)
		if err != nil || body == nil {
			return err
		}
		block, err = newBackfillColumnBlock(tx, slot, root, body)
		return err
	})
	return block, err
}

// newBackfillColumnBlock returns the column sync entry of block, or nil if it has no columns to
// fetch: it predates Fulu or commits to no blobs, or it is a Gloas block whose envelope was not
// processed (a Gloas block commits to blobs in its bid, but its columns exist only if the payload
// was revealed).
func newBackfillColumnBlock(tx kv.Tx, slot uint64, root common.Hash, block *cltypes.SignedBeaconBlock) (*backfillColumnBlock, error) {
	if block.Version() < clparams.FuluVersion {
		return nil, nil
	}
	commitments := block.GetBlobKzgCommitments()
	if commitments == nil || commitments.Len() == 0 {
		return nil, nil
	}
	if block.Version() >= clparams.GloasVersion {
		// The envelope writes the execution block hash of a Gloas block.
		payloadHash, err := beacon_indicies.ReadExecutionBlockHash(tx, root)
		if err != nil || payloadHash == (common.Hash{}) {
			return nil, err
		}
	}
	return &backfillColumnBlock{version: block.Version(), slot: slot, root: root, commitments: commitments, signature: block.BlockSignature()}, nil
}

// backfillSideBranchBlocks queues, newest slot first, the stored blocks that are not canonical at
// that moment, are held by fork choice, commit to blobs and miss their columns. This includes
// blocks that the fork choice stage has not yet marked canonical. It never waits for room and ends
// once the queue holds half of maxBackfillQueued, so these roots never take more than half of the
// room the canonical walk has.
func (d *peerdas) backfillSideBranchBlocks(ctx context.Context, fromSlot, toSlot uint64) {
	if d.forkChoice == nil {
		return
	}
	sideQuotaReached := func() bool { return d.blocksToCheckSync.activeLen() >= maxBackfillQueued/2 }
	queued := 0
	for slot := toSlot; slot >= fromSlot && ctx.Err() == nil && !sideQuotaReached(); slot-- {
		for _, block := range d.sideBranchBlocks(ctx, slot) {
			if sideQuotaReached() {
				break
			}
			available, err := d.IsDataAvailable(slot, block.root)
			switch {
			case err != nil:
				log.Warn("[custodyBackfill] failed to check the stored columns, skipping the side-branch block", "slot", slot, "blockRoot", block.root, "err", err)
			case !available:
				d.blocksToCheckSync.add(block.root, block, time.Now())
				queued++
			}
		}
		if slot == 0 {
			break
		}
	}
	if queued > 0 {
		log.Debug("[custodyBackfill] queued stored non-canonical blocks missing custody columns", "from", fromSlot, "to", toSlot, "blocks", queued)
	}
}

// sideBranchBlocks returns the stored blocks at slot, other than the canonical one, that
// sideBranchBlock accepts. An unreadable root is logged and skipped.
func (d *peerdas) sideBranchBlocks(ctx context.Context, slot uint64) []*backfillColumnBlock {
	var blocks []*backfillColumnBlock
	err := d.indiciesDB.View(ctx, func(tx kv.Tx) error {
		canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, slot)
		if err != nil {
			return err
		}
		roots, err := beacon_indicies.ReadBlockRootsAtSlot(tx, slot)
		if err != nil {
			return err
		}
		for _, root := range roots {
			if root == canonicalRoot {
				continue
			}
			block, err := d.sideBranchBlock(tx, root)
			if err != nil {
				log.Warn("[custodyBackfill] failed to read a side-branch block, skipping it", "slot", slot, "blockRoot", root, "err", err)
			} else if block != nil {
				blocks = append(blocks, block)
			}
		}
		return nil
	})
	if err != nil {
		log.Warn("[custodyBackfill] failed to read the side-branch blocks, skipping the slot", "slot", slot, "err", err)
	}
	return blocks
}

// sideBranchBlock returns the entry for the block with the given root if fork choice holds it and
// newBackfillColumnBlock accepts it, or nil. The block comes from fork choice, but its signature
// from the stored header: fork choice may hold a block whose signature was not yet checked, and
// the stored header is written once the block is accepted.
func (d *peerdas) sideBranchBlock(tx kv.Tx, root common.Hash) (*backfillColumnBlock, error) {
	block, ok := d.forkChoice.GetBlock(root)
	if !ok {
		return nil, nil
	}
	entry, err := newBackfillColumnBlock(tx, block.Block.Slot, root, block)
	if err != nil || entry == nil {
		return nil, err
	}
	header, _, err := beacon_indicies.ReadSignedHeaderByBlockRoot(tx, root)
	if err != nil || header == nil {
		return nil, err
	}
	entry.signature = header.Signature
	return entry, nil
}
