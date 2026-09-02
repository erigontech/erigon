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

package network

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

const (
	blobDownloaderInterval      = 12 * time.Second
	blobLogInterval             = 30 * time.Second
	blobBackfillWarningInterval = 4 * time.Minute
	blocksBatchSize             = uint64(8)
	blobRetryShardBits          = 10
	maxBlobRetryRanges          = 1 << blobRetryShardBits
	blobRetryShardShift         = 64 - blobRetryShardBits
	// bounds a fulu block's column recovery; columns past the custody window are
	// unfetchable and would otherwise block forever.
	blobColumnBackfillTimeout = 30 * time.Second
)

type blobRetryRange struct {
	start          uint64
	end            uint64
	cursor         uint64
	intervals      *btree.BTreeG[blobRetryInterval]
	intervalCursor uint64
	work           uint64
}

type blobRetryInterval struct {
	start  uint64
	end    uint64
	cursor uint64
}

// SyncedChecker is an interface to check if the forkchoice is synced
type SyncedChecker interface {
	Synced() bool
}

// PeerDasGetter is an interface to get the PeerDas instance
type PeerDasGetter interface {
	GetPeerDas() das.PeerDas
}

type blobPeerClient interface {
	Peers() (uint64, error)
	blobRequester
}

type blobSnapshotReader interface {
	FrozenBlobs() uint64
}

// BlobHistoryDownloader downloads blob history backwards from a head slot
type BlobHistoryDownloader struct {
	ctx context.Context

	beaconCfg   *clparams.BeaconChainConfig
	rpc         blobPeerClient
	indiciesDB  kv.RoDB
	blobStorage blob_storage.BlobStorage
	blockReader freezeblocks.BeaconSnapshotReader
	sn          blobSnapshotReader

	syncedChecker SyncedChecker
	peerDasGetter PeerDasGetter

	// headSlot is the slot we start downloading from (currentSlot + 1)
	headSlot atomic.Uint64
	// highestBackfilledSlot is the highest slot we've successfully backfilled to
	highestBackfilledSlot  atomic.Uint64
	nextBackfillTargetSlot uint64
	denebStartSlot         uint64
	retryRanges            []blobRetryRange
	retryRangeCursor       uint64
	// archiveBlobs indicates whether to archive all blobs or just recent ones
	archiveBlobs bool
	// immediateBlobsBackfilling indicates whether to backfill blobs immediately
	immediateBlobsBackfilling bool
	// columnBackfillTimeout bounds each fulu block's PeerDAS column recovery
	columnBackfillTimeout time.Duration
	verifyBlobSidecars    func([]*cltypes.BlobSidecar, clparams.StateVersion, func(*cltypes.SignedBeaconBlockHeader) error) error

	running           atomic.Bool
	backfillCompleted atomic.Bool
	logger            log.Logger

	// notifyBlobBackfilled is called when blob backfilling completeness changes.
	notifyBlobBackfilled func(bool)

	mu sync.RWMutex
}

// NewBlobHistoryDownloader creates a new BlobHistoryDownloader
func NewBlobHistoryDownloader(
	ctx context.Context,
	beaconCfg *clparams.BeaconChainConfig,
	rpc *rpc.BeaconRpcP2P,
	indiciesDB kv.RoDB,
	blobStorage blob_storage.BlobStorage,
	blockReader freezeblocks.BeaconSnapshotReader,
	sn *freezeblocks.CaplinSnapshots,
	syncedChecker SyncedChecker,
	peerDasGetter PeerDasGetter,
	archiveBlobs bool,
	immediateBlobsBackfilling bool,
	logger log.Logger,
) *BlobHistoryDownloader {
	targetSlot := beaconCfg.DenebForkEpoch * beaconCfg.SlotsPerEpoch
	return &BlobHistoryDownloader{
		ctx:                       ctx,
		beaconCfg:                 beaconCfg,
		rpc:                       rpc,
		indiciesDB:                indiciesDB,
		blobStorage:               blobStorage,
		blockReader:               blockReader,
		sn:                        sn,
		syncedChecker:             syncedChecker,
		peerDasGetter:             peerDasGetter,
		nextBackfillTargetSlot:    targetSlot,
		denebStartSlot:            targetSlot,
		archiveBlobs:              archiveBlobs,
		immediateBlobsBackfilling: immediateBlobsBackfilling,
		columnBackfillTimeout:     blobColumnBackfillTimeout,
		verifyBlobSidecars:        blob_storage.VerifyBlobSidecars,
		logger:                    logger,
	}
}

// SetHeadSlot sets the head slot to download from (should be currentSlot + 1)
func (b *BlobHistoryDownloader) SetHeadSlot(slot uint64) {
	b.headSlot.Store(slot)
}

// SetNotifyBlobBackfilled sets the callback for blob backfilling completeness changes.
func (b *BlobHistoryDownloader) SetNotifyBlobBackfilled(notify func(bool)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.notifyBlobBackfilled = notify
}

func (b *BlobHistoryDownloader) setBackfillCompleted(completed bool) {
	if b.backfillCompleted.Swap(completed) == completed {
		return
	}
	b.mu.RLock()
	notify := b.notifyBlobBackfilled
	b.mu.RUnlock()
	if notify != nil {
		notify(completed)
	}
}

// HeadSlot returns the current head slot
func (b *BlobHistoryDownloader) HeadSlot() uint64 {
	return b.headSlot.Load()
}

// HighestBackfilledSlot returns the highest slot that has been backfilled
func (b *BlobHistoryDownloader) HighestBackfilledSlot() uint64 {
	return b.highestBackfilledSlot.Load()
}

// Running returns whether the downloader is currently running
func (b *BlobHistoryDownloader) Running() bool {
	return b.running.Load()
}

// Start begins the blob history download loop, querying every 12 seconds
func (b *BlobHistoryDownloader) Start() {
	if !b.archiveBlobs && !b.immediateBlobsBackfilling {
		return // nothing to do
	}
	if b.running.Swap(true) {
		return // already running
	}

	go b.run()
}

func (b *BlobHistoryDownloader) run() {
	defer b.running.Store(false)

	// Do an initial download immediately
	if err := b.downloadOnce(true); err != nil {
		b.logger.Error("[BlobHistoryDownloader] Error downloading blobs", "err", err)
	}

	downloadTimer := time.NewTimer(blobDownloaderInterval)
	defer downloadTimer.Stop()

	warningTimer := time.NewTimer(blobBackfillWarningInterval)
	defer warningTimer.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-downloadTimer.C:
			if err := b.downloadOnce(false); err != nil {
				b.logger.Error("[BlobHistoryDownloader] Error downloading blobs", "err", err)
			}
			downloadTimer.Reset(blobDownloaderInterval)
		case <-warningTimer.C:
			if !b.backfillCompleted.Load() {
				b.logger.Warn("[BlobHistoryDownloader] Blob backfilling is not finished, some blobs might be unavailable", "currentSlot", b.headSlot.Load(), "highestBackfilled", b.highestBackfilledSlot.Load())
			}
			warningTimer.Reset(blobBackfillWarningInterval)
		}
	}
}

// downloadOnce performs a single download pass
func (b *BlobHistoryDownloader) downloadOnce(shouldLog bool) error {
	currentSlot := b.headSlot.Load()
	if currentSlot == 0 {
		return nil // not initialized yet
	}
	startSlot := currentSlot

	if !b.peersAvailable() {
		return nil
	}

	logInterval := time.NewTicker(blobLogInterval)
	defer logInterval.Stop()

	prevLogSlot := currentSlot
	prevTime := time.Now()
	retryPending := true
	targetSlot := b.nextBackfillTargetSlot
	retryFloor := uint64(0)
	// in case of non-archive mode, we only backfill the last relevant epochs
	if !b.archiveBlobs {
		retentionFloor := currentSlot - min(currentSlot, b.beaconCfg.MinSlotsForBlobsSidecarsRequest())
		targetSlot = max(targetSlot, retentionFloor)
		retryFloor = retentionFloor
	}

	if shouldLog {
		b.logger.Info("[BlobHistoryDownloader] Downloading blobs backwards", "slot", currentSlot)
	}

	for currentSlot >= targetSlot {
		firstUnfrozenSlot := max(targetSlot, b.sn.FrozenBlobs())
		if currentSlot < firstUnfrozenSlot {
			break
		}
		if !b.syncedChecker.Synced() {
			if err := common.Sleep(b.ctx, 5*time.Second); err != nil {
				return nil
			}
			continue
		}
		if retryPending {
			if err := b.retryFailedRecoveries(retryFloor); err != nil {
				return err
			}
			if b.ctx.Err() != nil {
				return nil
			}
			retryPending = false
			firstUnfrozenSlot = max(targetSlot, b.sn.FrozenBlobs())
			if currentSlot < firstUnfrozenSlot {
				break
			}
		}

		batch, visited, err := b.collectIncompleteBlocks(currentSlot, firstUnfrozenSlot)
		if err != nil {
			return err
		}
		if len(batch) > 0 {
			select {
			case <-b.ctx.Done():
				return b.ctx.Err()
			case <-logInterval.C:
				if shouldLog {
					slotSec := float64(prevLogSlot-currentSlot) / time.Since(prevTime).Seconds()
					prevLogSlot = currentSlot
					prevTime = time.Now()
					progress := 0.0
					if startSlot > targetSlot {
						progress = float64(startSlot-currentSlot) / float64(startSlot-targetSlot) * 100
					}
					b.logger.Info("[BlobHistoryDownloader] Downloading blobs backwards",
						"slot", currentSlot, "to", targetSlot,
						"slots/sec", fmt.Sprintf("%.2f", slotSec),
						"progress", fmt.Sprintf("%.1f%%", progress),
						"eta", utils.ETA(currentSlot-targetSlot, slotSec))
				}
			default:
			}
			if !b.peersAvailable() {
				return nil
			}
			if b.processBatch(batch) {
				b.highestBackfilledSlot.Store(currentSlot)
			}
			if b.ctx.Err() != nil {
				return nil
			}
		}

		// Always advance so an uncompletable batch can't rebuild at the same slot forever.
		// step>=1 guarantees progress; stop once the distance left to the floor is below one
		// step. The loop guard keeps currentSlot>=targetSlot, so neither subtraction underflows.
		step := max(visited, 1)
		if currentSlot-targetSlot < step {
			break
		}
		currentSlot -= step
	}

	if shouldLog {
		b.logger.Info("[BlobHistoryDownloader] Blob history download finished successfully")
	}
	b.nextBackfillTargetSlot = max(b.denebStartSlot, startSlot-min(startSlot, b.beaconCfg.SlotsPerEpoch*2))

	if len(b.retryRanges) != 0 {
		b.setBackfillCompleted(false)
		return nil
	}
	b.setBackfillCompleted(true)
	return nil
}

func (b *BlobHistoryDownloader) retryFailedRecoveries(retryFloor uint64) error {
	if len(b.retryRanges) == 0 {
		return nil
	}
	firstUnfrozenSlot := max(retryFloor, b.sn.FrozenBlobs())
	b.trimRetryRanges(firstUnfrozenSlot)
	attemptedSlots := make(map[uint64]struct{}, blocksBatchSize)
	for range blocksBatchSize {
		var slot uint64
		found := false
		for range len(b.retryRanges) {
			if len(b.retryRanges) == 0 {
				break
			}
			rangeIndex := int(b.retryRangeCursor % uint64(len(b.retryRanges)))
			retryRange := &b.retryRanges[rangeIndex]
			slot = retryRange.nextSlot()
			b.retryRangeCursor++
			if _, attempted := attemptedSlots[slot]; attempted {
				continue
			}
			attemptedSlots[slot] = struct{}{}
			found = true
			break
		}
		if !found {
			break
		}

		block, err := b.readRetryBlock(slot)
		if err != nil {
			if b.ctx.Err() != nil {
				return nil
			}
			b.logger.Warn("[BlobHistoryDownloader] Failed to read retry block", "slot", slot, "err", err)
			continue
		}
		if block == nil || block.Version() < clparams.DenebVersion || block.GetBlobKzgCommitments() == nil {
			b.resolveRetrySlot(slot)
			continue
		}
		complete, err := b.retryBlock(block)
		if err != nil {
			if b.ctx.Err() != nil {
				return nil
			}
			b.logger.Warn("[BlobHistoryDownloader] Failed to retry blob recovery", "slot", slot, "err", err)
			continue
		}
		if complete {
			b.resolveRetrySlot(slot)
		}
		if b.ctx.Err() != nil {
			return nil
		}
	}
	return nil
}

func (b *BlobHistoryDownloader) readRetryBlock(slot uint64) (*cltypes.SignedBeaconBlock, error) {
	tx, err := b.indiciesDB.BeginRo(b.ctx)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	return b.blockReader.ReadBeaconBlockBodyBySlot(b.ctx, tx, slot)
}

func (b *BlobHistoryDownloader) retryBlock(block *cltypes.SignedBeaconBlock) (bool, error) {
	if block.Version() < clparams.FuluVersion {
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return false, err
		}
		_, complete, err := b.storedBlobSidecarsComplete(b.ctx, block, blockRoot)
		if err != nil {
			b.logger.Warn("[BlobHistoryDownloader] Failed to read stored blob sidecars during retry", "slot", block.GetSlot(), "err", err)
			return false, nil
		}
		if complete {
			return true, nil
		}
	}
	return b.processBatch([]*cltypes.SignedBeaconBlock{block}), nil
}

func (b *BlobHistoryDownloader) addRetrySlot(slot uint64) {
	b.setBackfillCompleted(false)
	shard := slot >> blobRetryShardShift
	index := sort.Search(len(b.retryRanges), func(i int) bool {
		return b.retryRanges[i].start>>blobRetryShardShift >= shard
	})
	if index < len(b.retryRanges) && b.retryRanges[index].start>>blobRetryShardShift == shard {
		b.retryRanges[index].add(slot)
		return
	}
	b.retryRanges = append(b.retryRanges, blobRetryRange{})
	copy(b.retryRanges[index+1:], b.retryRanges[index:])
	b.retryRanges[index] = blobRetryRange{start: slot, end: slot, cursor: slot}
}

func (r *blobRetryRange) nextSlot() uint64 {
	if r.intervals == nil {
		slot := r.cursor
		if slot == r.start {
			r.cursor = r.end
		} else {
			r.cursor--
		}
		return slot
	}
	interval, found := retryIntervalAtOrBefore(r.intervals, r.intervalCursor)
	var slot uint64
	if found && r.intervalCursor <= interval.end {
		slot = r.intervalCursor
	} else if interval, found = retryIntervalAtOrAfter(r.intervals, r.intervalCursor); found {
		slot = interval.start
	} else {
		interval, _ = r.intervals.Min()
		slot = interval.start
	}
	if slot == ^uint64(0) {
		r.intervalCursor = 0
	} else {
		r.intervalCursor = slot + 1
	}
	r.cursor = slot
	return slot
}

func (r blobRetryRange) contains(slot uint64) bool {
	if slot < r.start || slot > r.end {
		return false
	}
	if r.intervals == nil {
		return true
	}
	interval, found := retryIntervalAtOrBefore(r.intervals, slot)
	return found && slot <= interval.end
}

func (r blobRetryRange) workCount() uint64 {
	if r.intervals == nil {
		width := r.end - r.start
		if width == ^uint64(0) {
			return width
		}
		return width + 1
	}
	return r.work
}

func (r *blobRetryRange) add(slot uint64) {
	if r.intervals == nil {
		if r.start <= slot && slot <= r.end {
			return
		}
		if r.end != ^uint64(0) && slot == r.end+1 {
			r.end = slot
			return
		}
		if slot != ^uint64(0) && slot+1 == r.start {
			r.start = slot
			return
		}
		r.intervals = newBlobRetryIntervalTree()
		r.intervals.ReplaceOrInsert(blobRetryInterval{start: r.start, end: r.end, cursor: r.cursor})
		r.intervals.ReplaceOrInsert(blobRetryInterval{start: slot, end: slot, cursor: slot})
		r.work = r.workCountWithoutCache() + 1
		r.refreshBounds()
		return
	}
	previous, hasPrevious := retryIntervalAtOrBefore(r.intervals, slot)
	if hasPrevious && slot <= previous.end {
		return
	}
	next, hasNext := retryIntervalAtOrAfter(r.intervals, slot)
	mergePrevious := hasPrevious && previous.end != ^uint64(0) && previous.end+1 == slot
	mergeNext := hasNext && slot != ^uint64(0) && slot+1 == next.start
	switch {
	case mergePrevious && mergeNext:
		r.intervals.Delete(previous)
		r.intervals.Delete(next)
		previous.end = next.end
		r.intervals.ReplaceOrInsert(previous)
	case mergePrevious:
		r.intervals.Delete(previous)
		previous.end = slot
		r.intervals.ReplaceOrInsert(previous)
	case mergeNext:
		r.intervals.Delete(next)
		next.start = slot
		r.intervals.ReplaceOrInsert(next)
	default:
		r.intervals.ReplaceOrInsert(blobRetryInterval{start: slot, end: slot, cursor: slot})
	}
	if r.work != ^uint64(0) {
		r.work++
	}
	r.refreshBounds()
}

func newBlobRetryIntervalTree() *btree.BTreeG[blobRetryInterval] {
	return btree.NewG(8, func(left, right blobRetryInterval) bool { return left.start < right.start })
}

func retryIntervalAtOrBefore(tree *btree.BTreeG[blobRetryInterval], slot uint64) (blobRetryInterval, bool) {
	var interval blobRetryInterval
	found := false
	tree.DescendLessOrEqual(blobRetryInterval{start: slot}, func(item blobRetryInterval) bool {
		interval = item
		found = true
		return false
	})
	return interval, found
}

func retryIntervalAtOrAfter(tree *btree.BTreeG[blobRetryInterval], slot uint64) (blobRetryInterval, bool) {
	var interval blobRetryInterval
	found := false
	tree.AscendGreaterOrEqual(blobRetryInterval{start: slot}, func(item blobRetryInterval) bool {
		interval = item
		found = true
		return false
	})
	return interval, found
}

func (r *blobRetryRange) refreshBounds() {
	first, _ := r.intervals.Min()
	last, _ := r.intervals.Max()
	r.start = first.start
	r.end = last.end
}

func (r blobRetryRange) workCountWithoutCache() uint64 {
	width := r.end - r.start
	if width == ^uint64(0) {
		return width
	}
	return width + 1
}

func (r blobRetryRange) intervalCount() int {
	if r.intervals == nil {
		return 1
	}
	return r.intervals.Len()
}

func (b *BlobHistoryDownloader) addRetryBlocks(blocks []*cltypes.SignedBeaconBlock) {
	for _, block := range blocks {
		b.addRetrySlot(block.GetSlot())
	}
}

func (b *BlobHistoryDownloader) resolveRetrySlot(slot uint64) {
	for i := range b.retryRanges {
		retryRange := &b.retryRanges[i]
		if !retryRange.contains(slot) {
			continue
		}
		if retryRange.remove(slot) {
			last := len(b.retryRanges) - 1
			copy(b.retryRanges[i:], b.retryRanges[i+1:])
			b.retryRanges[last] = blobRetryRange{}
			b.retryRanges = b.retryRanges[:last]
		}
		return
	}
}

func (r *blobRetryRange) remove(slot uint64) bool {
	if r.intervals == nil {
		if r.start == r.end {
			return true
		}
		switch {
		case slot == r.start:
			r.start++
		case slot == r.end:
			r.end--
		default:
			left := blobRetryInterval{start: r.start, end: slot - 1, cursor: r.cursor}
			if left.cursor < left.start || left.cursor > left.end {
				left.cursor = left.end
			}
			right := blobRetryInterval{start: slot + 1, end: r.end, cursor: r.end}
			r.intervals = newBlobRetryIntervalTree()
			r.intervals.ReplaceOrInsert(left)
			r.intervals.ReplaceOrInsert(right)
			r.work = r.workCountWithoutCache() - 1
			r.refreshBounds()
			return false
		}
		if r.cursor < r.start || r.cursor > r.end {
			r.cursor = r.end
		}
		return false
	}
	interval, found := retryIntervalAtOrBefore(r.intervals, slot)
	if !found || slot > interval.end {
		return false
	}
	r.intervals.Delete(interval)
	switch {
	case interval.start == interval.end:
	case slot == interval.start:
		interval.start++
		if interval.cursor < interval.start || interval.cursor > interval.end {
			interval.cursor = interval.end
		}
		r.intervals.ReplaceOrInsert(interval)
	case slot == interval.end:
		interval.end--
		if interval.cursor > interval.end {
			interval.cursor = interval.end
		}
		r.intervals.ReplaceOrInsert(interval)
	default:
		left := blobRetryInterval{start: interval.start, end: slot - 1, cursor: interval.cursor}
		if left.cursor < left.start || left.cursor > left.end {
			left.cursor = left.end
		}
		right := blobRetryInterval{start: slot + 1, end: interval.end, cursor: interval.end}
		r.intervals.ReplaceOrInsert(left)
		r.intervals.ReplaceOrInsert(right)
	}
	if r.work > 0 {
		r.work--
	}
	if r.intervals.Len() == 0 {
		return true
	}
	if r.intervals.Len() == 1 {
		interval, _ := r.intervals.Min()
		*r = blobRetryRange{start: interval.start, end: interval.end, cursor: interval.cursor}
		return false
	}
	r.refreshBounds()
	return false
}

func (b *BlobHistoryDownloader) trimRetryRanges(firstUnfrozenSlot uint64) {
	kept := b.retryRanges[:0]
	for _, retryRange := range b.retryRanges {
		if retryRange.end < firstUnfrozenSlot {
			continue
		}
		if retryRange.intervals != nil {
			retryRange.trimBefore(firstUnfrozenSlot)
			if retryRange.intervals != nil && retryRange.intervals.Len() == 0 {
				continue
			}
			kept = append(kept, retryRange)
			continue
		}
		if retryRange.start < firstUnfrozenSlot {
			retryRange.start = firstUnfrozenSlot
		}
		if retryRange.cursor < retryRange.start || retryRange.cursor > retryRange.end {
			retryRange.cursor = retryRange.end
		}
		kept = append(kept, retryRange)
	}
	clear(b.retryRanges[len(kept):])
	b.retryRanges = kept
}

func (r *blobRetryRange) trimBefore(floor uint64) {
	if floor == 0 || r.intervals == nil {
		return
	}
	remove := make([]blobRetryInterval, 0)
	r.intervals.AscendLessThan(blobRetryInterval{start: floor}, func(interval blobRetryInterval) bool {
		remove = append(remove, interval)
		return true
	})
	for _, interval := range remove {
		r.intervals.Delete(interval)
		removedEnd := min(interval.end, floor-1)
		removed := removedEnd - interval.start + 1
		if removed > r.work {
			r.work = 0
		} else {
			r.work -= removed
		}
		if interval.end >= floor {
			interval.start = floor
			if interval.cursor < interval.start || interval.cursor > interval.end {
				interval.cursor = interval.end
			}
			r.intervals.ReplaceOrInsert(interval)
		}
	}
	if r.intervals.Len() == 1 {
		interval, _ := r.intervals.Min()
		*r = blobRetryRange{start: interval.start, end: interval.end, cursor: interval.cursor}
		return
	}
	if r.intervals.Len() > 0 {
		r.refreshBounds()
	}
}

func (b *BlobHistoryDownloader) peersAvailable() bool {
	peers, err := b.rpc.Peers()
	if err != nil {
		b.logger.Warn("[BlobHistoryDownloader] Failed to get peer count", "err", err)
		return false
	}
	if peers == 0 {
		b.logger.Debug("[BlobHistoryDownloader] Skipping iteration because no peers are available")
		return false
	}
	return true
}

// collectIncompleteBlocks scans backwards from currentSlot for Deneb+ blocks still
// missing blobs. Its read tx is released before the caller's network download.
func (b *BlobHistoryDownloader) collectIncompleteBlocks(currentSlot, targetSlot uint64) (batch []*cltypes.SignedBeaconBlock, visited uint64, err error) {
	tx, err := b.indiciesDB.BeginRo(b.ctx)
	if err != nil {
		return nil, 0, err
	}
	defer tx.Rollback()

	batch = make([]*cltypes.SignedBeaconBlock, 0, blocksBatchSize)
	for ; visited < blocksBatchSize; visited++ {
		if currentSlot < visited || currentSlot-visited < targetSlot {
			break
		}
		block, err := b.blockReader.ReadBeaconBlockBodyBySlot(b.ctx, tx, currentSlot-visited)
		if err != nil {
			return nil, 0, err
		}
		if block == nil {
			continue
		}
		if block.Version() < clparams.DenebVersion {
			break
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return nil, 0, err
		}
		commitments := block.Block.Body.GetBlobKzgCommitments()
		if commitments == nil {
			// For GLOAS, nil means SignedExecutionPayloadBid is absent — unexpected for a valid block.
			// For pre-GLOAS this should not happen on Deneb+.
			b.logger.Warn("[BlobHistoryDownloader] skipping block with nil kzg commitments", "slot", block.Block.Slot, "version", block.Version())
			continue
		}
		blobsCount, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
		if err != nil {
			return nil, 0, err
		}
		if commitments.Len() == int(blobsCount) {
			continue
		}
		batch = append(batch, block)
	}
	return batch, visited, nil
}

func (b *BlobHistoryDownloader) processBatch(batch []*cltypes.SignedBeaconBlock) bool {
	fuluBlocks := make([]*cltypes.SignedBeaconBlock, 0, len(batch))
	denebBlocks := make([]*cltypes.SignedBeaconBlock, 0, len(batch))
	for _, block := range batch {
		if block.Version() >= clparams.FuluVersion {
			fuluBlocks = append(fuluBlocks, block)
		} else {
			denebBlocks = append(denebBlocks, block)
		}
	}
	complete := true
	if len(denebBlocks) > 0 {
		complete = b.recoverDenebBlobs(denebBlocks)
	}
	if b.ctx.Err() != nil {
		return false
	}
	if len(fuluBlocks) > 0 {
		complete = b.recoverFuluColumns(fuluBlocks) && complete
	}
	return complete
}

func (b *BlobHistoryDownloader) recoverDenebBlobs(blocks []*cltypes.SignedBeaconBlock) bool {
	requestedBlocks := make([]*cltypes.SignedBeaconBlock, 0, len(blocks))
	for _, block := range blocks {
		if block.GetBlobKzgCommitments().Len() == 0 {
			continue
		}
		requestedBlocks = append(requestedBlocks, block)
	}
	if len(requestedBlocks) == 0 {
		return true
	}
	req, err := BlobsIdentifiersFromBlocks(requestedBlocks, b.beaconCfg)
	if err != nil {
		b.logger.Debug("[BlobHistoryDownloader] Error generating blob identifiers", "err", err)
		for _, block := range requestedBlocks {
			b.addRetrySlot(block.GetSlot())
		}
		return false
	}
	batch, err := newDenebRecoveryBatch(requestedBlocks, req)
	if err != nil {
		for _, block := range requestedBlocks {
			b.addRetrySlot(block.GetSlot())
		}
		return false
	}
	if !b.peersAvailable() {
		b.addRetryBlocks(requestedBlocks)
		return false
	}
	_, err = requestBlobsForBackfill(b.ctx, b.rpc, batch.remaining, func(ctx context.Context, candidate *PeerAndSidecars) (bool, bool, error) {
		progress, err := batch.validate(candidate.requested, candidate.Responses)
		if err != nil {
			return false, false, err
		}
		stored, err := batch.store(ctx, b.blobStorage)
		if err != nil {
			return progress > 0 || batch.hasCompleteUnstoredGroup(), false, err
		}
		return progress > 0 || stored > 0, batch.complete(), nil
	})
	if err != nil {
		b.logger.Debug("[BlobHistoryDownloader] Error requesting blobs", "err", err)
		b.addRetryBlocks(requestedBlocks)
	}
	requestFailed := err != nil
	complete := !requestFailed
	for _, block := range blocks {
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return false
		}
		if requestFailed {
			group := batch.groups[blockRoot]
			if group == nil || !group.stored {
				continue
			}
		}
		_, blockComplete, err := b.storedBlobSidecarsComplete(b.ctx, block, blockRoot)
		if err != nil || !blockComplete {
			b.addRetrySlot(block.GetSlot())
			complete = false
		}
	}
	return complete
}

type blobIdentifierKey struct {
	root  common.Hash
	index uint64
}

type requestedBlobBlock struct {
	block    *cltypes.SignedBeaconBlock
	ids      []*cltypes.BlobIdentifier
	sidecars map[uint64]*cltypes.BlobSidecar
	stored   bool
}

type denebRecoveryBatch struct {
	groups   map[common.Hash]*requestedBlobBlock
	order    []common.Hash
	verifier func([]*cltypes.BlobSidecar, func(*cltypes.SignedBeaconBlockHeader) error) error
}

func newDenebRecoveryBatch(blocks []*cltypes.SignedBeaconBlock, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*denebRecoveryBatch, error) {
	batch := &denebRecoveryBatch{
		groups: make(map[common.Hash]*requestedBlobBlock, len(blocks)),
		order:  make([]common.Hash, 0, len(blocks)),
		verifier: func(sidecars []*cltypes.BlobSidecar, verifySignatureFn func(*cltypes.SignedBeaconBlockHeader) error) error {
			return blob_storage.VerifyBlobSidecars(sidecars, clparams.DenebVersion, verifySignatureFn)
		},
	}
	blocksByRoot := make(map[common.Hash]*cltypes.SignedBeaconBlock, len(blocks))
	for _, block := range blocks {
		root, err := block.Block.HashSSZ()
		if err != nil {
			return nil, err
		}
		blocksByRoot[root] = block
	}
	for i := range req.Len() {
		id := req.Get(i)
		group := batch.groups[id.BlockRoot]
		if group == nil {
			group = &requestedBlobBlock{block: blocksByRoot[id.BlockRoot], sidecars: make(map[uint64]*cltypes.BlobSidecar)}
			batch.groups[id.BlockRoot] = group
			batch.order = append(batch.order, id.BlockRoot)
		}
		group.ids = append(group.ids, id)
	}
	return batch, nil
}

func (b *denebRecoveryBatch) validate(request *solid.ListSSZ[*cltypes.BlobIdentifier], sidecars []*cltypes.BlobSidecar) (int, error) {
	requested := make(map[blobIdentifierKey]struct{}, request.Len())
	for i := range request.Len() {
		id := request.Get(i)
		requested[blobIdentifierKey{root: id.BlockRoot, index: id.Index}] = struct{}{}
	}
	seen := make(map[blobIdentifierKey]struct{}, len(sidecars))
	newSidecars := make([]*cltypes.BlobSidecar, 0, len(sidecars))
	for _, sidecar := range sidecars {
		if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil {
			return 0, errors.New("blob response contains incomplete sidecar")
		}
		root, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil {
			return 0, err
		}
		key := blobIdentifierKey{root: root, index: sidecar.Index}
		if _, ok := requested[key]; !ok {
			return 0, fmt.Errorf("unrequested blob sidecar %x:%d", root, sidecar.Index)
		}
		group := b.groups[root]
		if group == nil || group.block == nil {
			return 0, fmt.Errorf("blob response block %x is not in batch", root)
		}
		if _, duplicate := seen[key]; duplicate {
			return 0, fmt.Errorf("duplicate blob sidecar %x:%d", root, sidecar.Index)
		}
		seen[key] = struct{}{}
		if group.sidecars[sidecar.Index] == nil {
			newSidecars = append(newSidecars, sidecar)
		}
	}
	if len(sidecars) == 0 {
		return 0, errors.New("empty blob response")
	}
	if len(newSidecars) == 0 {
		return 0, nil
	}
	if err := b.verifier(newSidecars, func(header *cltypes.SignedBeaconBlockHeader) error {
		root, err := header.Header.HashSSZ()
		if err != nil {
			return err
		}
		group := b.groups[root]
		if group == nil || group.block.Signature != header.Signature {
			return errors.New("signature mismatch between blob and stored block")
		}
		return nil
	}); err != nil {
		return 0, err
	}
	for _, sidecar := range newSidecars {
		root, _ := sidecar.SignedBlockHeader.Header.HashSSZ()
		b.groups[root].sidecars[sidecar.Index] = sidecar
	}
	return len(newSidecars), nil
}

func (b *denebRecoveryBatch) store(ctx context.Context, storage blob_storage.BlobStorage) (int, error) {
	stored := 0
	for _, root := range b.order {
		group := b.groups[root]
		if group.stored || len(group.sidecars) != len(group.ids) {
			continue
		}
		ordered := make([]*cltypes.BlobSidecar, 0, len(group.ids))
		for _, id := range group.ids {
			ordered = append(ordered, group.sidecars[id.Index])
		}
		if err := storage.WriteBlobSidecars(ctx, root, ordered); err != nil {
			return stored, err
		}
		group.stored = true
		stored++
	}
	return stored, nil
}

func (b *denebRecoveryBatch) hasCompleteUnstoredGroup() bool {
	for _, group := range b.groups {
		if !group.stored && len(group.sidecars) == len(group.ids) {
			return true
		}
	}
	return false
}

func (b *denebRecoveryBatch) complete() bool {
	for _, group := range b.groups {
		if !group.stored {
			return false
		}
	}
	return true
}

func (b *denebRecoveryBatch) remaining() *solid.ListSSZ[*cltypes.BlobIdentifier] {
	remaining := solid.NewStaticListSSZ[*cltypes.BlobIdentifier](0, 40)
	for _, root := range b.order {
		group := b.groups[root]
		if group.stored {
			continue
		}
		for _, id := range group.ids {
			if group.sidecars[id.Index] == nil {
				remaining.Append(id)
			}
		}
	}
	return remaining
}

// recoverFuluColumns recovers blobs from PeerDAS columns, bounding each attempt so
// columns no peer still serves can't block the backfill indefinitely.
func (b *BlobHistoryDownloader) recoverFuluColumns(blocks []*cltypes.SignedBeaconBlock) bool {
	completeBatch := true
	for i, block := range blocks {
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			b.addRetryBlocks(blocks[i:])
			b.logger.Warn("[BlobHistoryDownloader] Failed to hash recovered block", "err", err, "slot", block.GetSlot())
			return false
		}
		commitments := block.GetBlobKzgCommitments()
		if commitments == nil {
			b.addRetryBlocks(blocks[i:])
			b.logger.Warn("[BlobHistoryDownloader] Blob recovery completed without commitments", "slot", block.GetSlot())
			return false
		}
		if commitments.Len() == 0 {
			continue
		}
		ctx, cancel := context.WithTimeout(b.ctx, b.columnBackfillTimeout)
		_, complete, err := b.storedBlobSidecarsComplete(ctx, block, blockRoot)
		if err != nil {
			cancel()
			b.addRetryBlocks(blocks[i:])
			b.logger.Warn("[BlobHistoryDownloader] Failed to read stored blobs", "err", err, "slot", block.GetSlot())
			return false
		}
		if complete {
			cancel()
			continue
		}
		if !b.peersAvailable() {
			cancel()
			b.addRetryBlocks(blocks[i:])
			return false
		}
		peerDas := b.peerDasGetter.GetPeerDas()
		err = peerDas.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block})
		if err != nil {
			cancel()
			b.addRetryBlocks(blocks[i:])
			b.logger.Warn("[BlobHistoryDownloader] Error recovering blobs from block", "err", err, "slot", block.GetSlot())
			return false
		}
		stored, complete, err := b.waitForStoredBlobSidecars(ctx, block, blockRoot)
		cancel()
		if err != nil || !complete {
			b.addRetrySlot(block.GetSlot())
			completeBatch = false
			b.logger.Warn("[BlobHistoryDownloader] Blob recovery did not satisfy durable postcondition", "err", err, "slot", block.GetSlot(), "stored", stored, "expected", commitments.Len())
			if b.ctx.Err() != nil {
				return false
			}
		}
	}
	return completeBatch
}

func (b *BlobHistoryDownloader) waitForStoredBlobSidecars(ctx context.Context, block *cltypes.SignedBeaconBlock, blockRoot common.Hash) (uint32, bool, error) {
	ticker := time.NewTicker(requestBlobRetryInterval)
	defer ticker.Stop()
	for {
		stored, complete, err := b.storedBlobSidecarsComplete(ctx, block, blockRoot)
		if err != nil || complete {
			return stored, complete, err
		}
		select {
		case <-ctx.Done():
			return stored, false, ctx.Err()
		case <-ticker.C:
		}
	}
}

func (b *BlobHistoryDownloader) storedBlobSidecarsComplete(ctx context.Context, block *cltypes.SignedBeaconBlock, blockRoot common.Hash) (uint32, bool, error) {
	commitments := block.GetBlobKzgCommitments()
	if commitments == nil {
		return 0, false, nil
	}
	if commitments.Len() == 0 {
		return 0, true, nil
	}
	stored, err := b.blobStorage.KzgCommitmentsCount(ctx, blockRoot)
	if err != nil || int(stored) != commitments.Len() {
		return stored, false, err
	}
	sidecars, found, err := b.blobStorage.ReadBlobSidecars(ctx, block.GetSlot(), blockRoot)
	if err != nil || !found || len(sidecars) != commitments.Len() {
		return stored, false, err
	}
	seen := make([]bool, commitments.Len())
	for _, sidecar := range sidecars {
		if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil || sidecar.Index >= uint64(commitments.Len()) || seen[sidecar.Index] {
			return stored, false, nil
		}
		root, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil || root != blockRoot || sidecar.SignedBlockHeader.Header.Slot != block.GetSlot() || sidecar.SignedBlockHeader.Signature != block.Signature || sidecar.KzgCommitment != common.Bytes48(*commitments.Get(int(sidecar.Index))) {
			return stored, false, nil
		}
		seen[sidecar.Index] = true
	}
	verify := b.verifyBlobSidecars
	if verify == nil {
		verify = blob_storage.VerifyBlobSidecars
	}
	if err := verify(sidecars, block.Version(), nil); err != nil {
		return stored, false, nil
	}
	return stored, true, nil
}
