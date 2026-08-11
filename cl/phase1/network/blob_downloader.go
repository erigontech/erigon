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
	"cmp"
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/crypto/kzg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

const (
	blobDownloaderInterval      = 12 * time.Second
	blobLogInterval             = 30 * time.Second
	blobBackfillWarningInterval = 4 * time.Minute
	blocksBatchSize             = uint64(8)
	// bounds a fulu block's column recovery; columns past the custody window are
	// unfetchable and would otherwise block forever.
	blobColumnBackfillTimeout = 30 * time.Second
)

// SyncedChecker is an interface to check if the forkchoice is synced
type SyncedChecker interface {
	Synced() bool
}

// PeerDasGetter is an interface to get the PeerDas instance
type PeerDasGetter interface {
	GetPeerDas() das.PeerDas
}

type forcedBlobRecoverer interface {
	ForceScheduleRecover(context.Context, uint64, common.Hash, uint64) error
}

type blobSnapshotReader interface {
	FrozenBlobs() uint64
}

type blobRequestFn func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier]) (*PeerAndSidecars, error)

// BlobHistoryDownloader downloads blob history backwards from a head slot
type BlobHistoryDownloader struct {
	ctx context.Context

	beaconCfg    *clparams.BeaconChainConfig
	rpc          BlobPeerClient
	indiciesDB   kv.RoDB
	blobStorage  blob_storage.BlobStorage
	blockReader  freezeblocks.BeaconSnapshotReader
	sn           blobSnapshotReader
	requestBlobs blobRequestFn

	syncedChecker SyncedChecker
	peerDasGetter PeerDasGetter

	// headSlot is the inclusive upper bound of the range to download.
	headSlot atomic.Uint64
	// highestBackfilledSlot is the highest slot we've successfully backfilled to
	highestBackfilledSlot atomic.Uint64
	// targetSlot is the slot we're trying to reach (Deneb fork epoch start)
	targetSlot uint64
	// archiveBlobs indicates whether to archive all blobs or just recent ones
	archiveBlobs bool
	// immediateBlobsBackfilling indicates whether to backfill blobs immediately
	immediateBlobsBackfilling bool
	// columnBackfillTimeout bounds each fulu block's PeerDAS column recovery
	columnBackfillTimeout time.Duration

	running           atomic.Bool
	backfillCompleted atomic.Bool
	completedRanges   []backfillRange
	headRoot          common.Hash
	logger            log.Logger

	// notifyBlobBackfilled is called when blob backfilling is complete
	notifyBlobBackfilled func()

	mu sync.RWMutex
}

// NewBlobHistoryDownloader creates a new BlobHistoryDownloader
func NewBlobHistoryDownloader(
	ctx context.Context,
	beaconCfg *clparams.BeaconChainConfig,
	rpc BlobPeerClient,
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
	targetSlot, _ := denebStartSlot(beaconCfg)
	return &BlobHistoryDownloader{
		ctx:                       ctx,
		beaconCfg:                 beaconCfg,
		rpc:                       rpc,
		indiciesDB:                indiciesDB,
		blobStorage:               blobStorage,
		blockReader:               blockReader,
		sn:                        sn,
		requestBlobs:              RequestBlobsFrantically,
		syncedChecker:             syncedChecker,
		peerDasGetter:             peerDasGetter,
		targetSlot:                targetSlot,
		archiveBlobs:              archiveBlobs,
		immediateBlobsBackfilling: immediateBlobsBackfilling,
		columnBackfillTimeout:     blobColumnBackfillTimeout,
		logger:                    logger,
	}
}

// SetHead sets the inclusive upper bound and preserves completion only through safeThrough.
func (b *BlobHistoryDownloader) SetHead(slot uint64, root common.Hash, safeThrough uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.headSlot.Store(slot)
	b.trimCompletedRanges(safeThrough)
	b.headRoot = root
	if !b.completedRangesContain(b.backfillRanges(slot)) {
		b.backfillCompleted.Store(false)
	}
}

// InvalidateCompletionAbove conservatively trims completed coverage before a canonical-head change is committed.
func (b *BlobHistoryDownloader) InvalidateCompletionAbove(safeThrough uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.trimCompletedRanges(safeThrough)
	if !b.completedRangesContain(b.backfillRanges(b.headSlot.Load())) {
		b.backfillCompleted.Store(false)
	}
}

// SetNotifyBlobBackfilled sets the callback for when blob backfilling is complete
func (b *BlobHistoryDownloader) SetNotifyBlobBackfilled(notify func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.notifyBlobBackfilled = notify
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

func (b *BlobHistoryDownloader) BlobBackfillPending(slot uint64) bool {
	if !b.archiveBlobs && !b.immediateBlobsBackfilling {
		return false
	}
	headSlot := b.headSlot.Load()
	if headSlot == 0 {
		denebSlot, denebScheduled := forkStartSlot(b.beaconCfg.DenebForkEpoch, b.beaconCfg.SlotsPerEpoch)
		return denebScheduled && slot >= denebSlot
	}
	if headSlot != 0 && slot > headSlot {
		denebStart, scheduled := denebStartSlot(b.beaconCfg)
		return scheduled && slot >= denebStart
	}
	if !b.slotWithinBackfillRange(slot, headSlot) {
		return false
	}
	b.mu.RLock()
	completed := b.completedSlot(slot)
	b.mu.RUnlock()
	return !completed
}

func denebStartSlot(beaconCfg *clparams.BeaconChainConfig) (uint64, bool) {
	return forkStartSlot(beaconCfg.DenebForkEpoch, beaconCfg.SlotsPerEpoch)
}

func forkStartSlot(forkEpoch, slotsPerEpoch uint64) (uint64, bool) {
	if forkEpoch == math.MaxUint64 || slotsPerEpoch == 0 || forkEpoch > math.MaxUint64/slotsPerEpoch {
		return math.MaxUint64, false
	}
	return forkEpoch * slotsPerEpoch, true
}

func epochRetentionFloor(head, forkEpoch, retentionEpochs, slotsPerEpoch uint64) (uint64, bool) {
	if _, ok := forkStartSlot(forkEpoch, slotsPerEpoch); !ok {
		return math.MaxUint64, false
	}
	currentEpoch := head / slotsPerEpoch
	earliestEpoch := currentEpoch - min(currentEpoch, retentionEpochs)
	earliestEpoch = max(earliestEpoch, forkEpoch)
	return earliestEpoch * slotsPerEpoch, true
}

type backfillRange struct{ start, end uint64 }

func (b *BlobHistoryDownloader) backfillRanges(head uint64) []backfillRange {
	denebStart, denebScheduled := denebStartSlot(b.beaconCfg)
	if b.archiveBlobs && b.targetSlot != 0 {
		denebStart, denebScheduled = b.targetSlot, true
	}
	if !denebScheduled || head < denebStart {
		return nil
	}
	fuluStart, fuluScheduled := forkStartSlot(b.beaconCfg.FuluForkEpoch, b.beaconCfg.SlotsPerEpoch)
	ranges := make([]backfillRange, 0, 2)
	if !fuluScheduled || denebStart < fuluStart {
		denebFloor := denebStart
		if b.archiveBlobs && b.targetSlot != 0 {
			denebFloor = b.targetSlot
		} else if !b.archiveBlobs {
			denebFloor, _ = epochRetentionFloor(head, b.beaconCfg.DenebForkEpoch, b.beaconCfg.MinEpochsForBlobSidecarsRequests, b.beaconCfg.SlotsPerEpoch)
		}
		if !fuluScheduled || denebFloor < fuluStart {
			end := head
			if fuluScheduled && fuluStart > 0 {
				end = min(end, fuluStart-1)
			}
			if denebFloor <= end {
				ranges = append(ranges, backfillRange{denebFloor, end})
			}
		}
	}
	if fuluScheduled && head >= fuluStart {
		fuluFloor, _ := epochRetentionFloor(head, b.beaconCfg.FuluForkEpoch, b.beaconCfg.MinEpochsForDataColumnSidecarsRequests, b.beaconCfg.SlotsPerEpoch)
		ranges = append(ranges, backfillRange{fuluFloor, head})
	}
	return ranges
}

func (b *BlobHistoryDownloader) slotWithinBackfillRange(slot, head uint64) bool {
	for _, r := range b.backfillRanges(head) {
		if slot >= r.start && slot <= r.end {
			return true
		}
	}
	return false
}

func (b *BlobHistoryDownloader) completedRangeContains(start, end uint64) bool {
	for _, completed := range b.completedRanges {
		if start >= completed.start && end <= completed.end {
			return true
		}
	}
	return false
}

func (b *BlobHistoryDownloader) completedSlot(slot uint64) bool {
	return b.completedRangeContains(slot, slot)
}

func (b *BlobHistoryDownloader) completedRangesContain(ranges []backfillRange) bool {
	for _, r := range ranges {
		if !b.completedRangeContains(r.start, r.end) {
			return false
		}
	}
	return len(ranges) > 0
}

func (b *BlobHistoryDownloader) trimCompletedRanges(safeThrough uint64) {
	kept := b.completedRanges[:0]
	for _, r := range b.completedRanges {
		if r.start > safeThrough {
			break
		}
		r.end = min(r.end, safeThrough)
		kept = append(kept, r)
	}
	b.completedRanges = kept
}

func (b *BlobHistoryDownloader) addCompletedRanges(ranges []backfillRange) {
	all := append(append(make([]backfillRange, 0, len(b.completedRanges)+len(ranges)), b.completedRanges...), ranges...)
	slices.SortFunc(all, func(a, c backfillRange) int { return cmp.Compare(a.start, c.start) })
	b.completedRanges = b.completedRanges[:0]
	for _, r := range all {
		last := len(b.completedRanges) - 1
		if last < 0 || !intervalsTouch(r.start, r.end, b.completedRanges[last].start, b.completedRanges[last].end) {
			b.completedRanges = append(b.completedRanges, r)
			continue
		}
		b.completedRanges[last].end = max(b.completedRanges[last].end, r.end)
	}
}

func (b *BlobHistoryDownloader) incompleteRanges(desired []backfillRange) []backfillRange {
	pending := make([]backfillRange, 0, len(desired))
	for _, want := range desired {
		cursor := want.start
		coveredThroughEnd := false
		for _, done := range b.completedRanges {
			if done.end < cursor || done.start > want.end {
				continue
			}
			if done.start > cursor {
				pending = append(pending, backfillRange{cursor, done.start - 1})
			}
			if done.end >= want.end {
				coveredThroughEnd = true
				break
			}
			cursor = done.end + 1
		}
		if !coveredThroughEnd && cursor <= want.end {
			pending = append(pending, backfillRange{cursor, want.end})
		}
	}
	return pending
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
	b.mu.RLock()
	headSlot := b.headSlot.Load()
	headRoot := b.headRoot
	desiredRanges := b.backfillRanges(headSlot)
	if len(desiredRanges) == 0 {
		b.mu.RUnlock()
		return nil
	}
	pendingRanges := b.incompleteRanges(desiredRanges)
	if len(pendingRanges) == 0 {
		b.mu.RUnlock()
		b.backfillCompleted.Store(true)
		return nil
	}
	b.mu.RUnlock()
	if headSlot == 0 {
		return nil // not initialized yet
	}
	b.backfillCompleted.Store(false)

	// Check peer count before proceeding
	peers, err := b.rpc.Peers()
	if err != nil {
		b.logger.Warn("[BlobHistoryDownloader] Failed to get peer count", "err", err)
		return nil
	}
	if peers == 0 {
		b.logger.Warn("[BlobHistoryDownloader] Skipping iteration because no peers are available")
		return nil
	}

	logInterval := time.NewTicker(blobLogInterval)
	defer logInterval.Stop()

	prevLogSlot := pendingRanges[len(pendingRanges)-1].end
	prevTime := time.Now()

	if shouldLog {
		b.logger.Info("[BlobHistoryDownloader] Downloading blobs backwards", "slot", prevLogSlot)
	}

	var passErr error
	for _, work := range slices.Backward(pendingRanges) {
		currentSlot := work.end
		targetSlot := work.start
		for currentSlot >= targetSlot {
			firstUnfrozenSlot := max(targetSlot, b.sn.FrozenBlobs())
			if currentSlot < firstUnfrozenSlot {
				break
			}
			if !b.syncedChecker.Synced() {
				time.Sleep(5 * time.Second)
				continue
			}

			batch, visited, err := b.collectIncompleteBlocks(currentSlot, firstUnfrozenSlot, headSlot)
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
						b.logger.Info("[BlobHistoryDownloader] Downloading blobs backwards",
							"slot", currentSlot, "to", targetSlot,
							"slots/sec", fmt.Sprintf("%.2f", slotSec),
							"eta", utils.ETA(currentSlot-targetSlot, slotSec))
					}
				default:
				}
				if err := b.processBatch(batch); err != nil {
					passErr = errors.Join(passErr, err)
				} else {
					b.highestBackfilledSlot.Store(currentSlot)
				}
			}

			step := max(visited, 1)
			if currentSlot-targetSlot < step {
				break
			}
			currentSlot -= step
		}
	}
	if passErr != nil {
		b.backfillCompleted.Store(false)
		return passErr
	}

	if shouldLog {
		b.logger.Info("[BlobHistoryDownloader] Blob history download finished successfully")
	}

	b.mu.Lock()
	if b.headSlot.Load() != headSlot || b.headRoot != headRoot {
		b.mu.Unlock()
		return nil
	}
	b.addCompletedRanges(pendingRanges)
	complete := b.completedRangesContain(desiredRanges)
	b.backfillCompleted.Store(complete)
	notify := b.notifyBlobBackfilled
	b.mu.Unlock()
	if complete && notify != nil {
		notify()
	}

	return nil
}

func intervalsTouch(firstStart, firstEnd, secondStart, secondEnd uint64) bool {
	firstBeforeSecond := firstEnd < secondStart && (firstEnd == math.MaxUint64 || secondStart-firstEnd > 1)
	secondBeforeFirst := secondEnd < firstStart && (secondEnd == math.MaxUint64 || firstStart-secondEnd > 1)
	return !firstBeforeSecond && !secondBeforeFirst
}

// collectIncompleteBlocks scans backwards from currentSlot for Deneb+ blocks still
// missing blobs. Its read tx is released before the caller's network download.
func (b *BlobHistoryDownloader) collectIncompleteBlocks(currentSlot, targetSlot, rangeHead uint64) (batch []*cltypes.SignedBeaconBlock, visited uint64, err error) {
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
		if !b.slotWithinBackfillRange(currentSlot-visited, rangeHead) {
			continue
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
		complete, err := b.actualBlobSetComplete(block, blockRoot)
		if err != nil {
			return nil, 0, err
		}
		if complete {
			continue
		}
		batch = append(batch, block)
	}
	return batch, visited, nil
}

func (b *BlobHistoryDownloader) actualBlobSetComplete(block *cltypes.SignedBeaconBlock, blockRoot common.Hash) (bool, error) {
	commitments := block.Block.Body.GetBlobKzgCommitments()
	if commitments == nil {
		return false, nil
	}
	if commitments.Len() == 0 {
		return true, nil
	}
	sidecars, complete, err := b.blobStorage.ReadBlobSidecars(b.ctx, block.GetSlot(), blockRoot)
	if err != nil || !complete || len(sidecars) != commitments.Len() {
		return false, err
	}
	for index, sidecar := range sidecars {
		if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil ||
			sidecar.Index != uint64(index) || sidecar.SignedBlockHeader.Header.Slot != block.GetSlot() ||
			commitments.Get(index) == nil || sidecar.KzgCommitment != common.Bytes48(*commitments.Get(index)) {
			return false, nil
		}
		sidecarRoot, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil {
			return false, err
		}
		if sidecarRoot != blockRoot {
			return false, nil
		}
	}
	return true, nil
}

func (b *BlobHistoryDownloader) processBatch(batch []*cltypes.SignedBeaconBlock) error {
	fuluBlocks := make([]*cltypes.SignedBeaconBlock, 0, len(batch))
	denebBlocks := make([]*cltypes.SignedBeaconBlock, 0, len(batch))
	for _, block := range batch {
		if block.Version() >= clparams.FuluVersion {
			fuluBlocks = append(fuluBlocks, block)
		} else {
			denebBlocks = append(denebBlocks, block)
		}
	}
	var denebErr error
	if len(denebBlocks) > 0 {
		denebErr = b.recoverDenebBlobs(denebBlocks)
	}
	var fuluErr error
	if len(fuluBlocks) > 0 {
		fuluErr = b.recoverFuluColumns(fuluBlocks)
	}
	return errors.Join(denebErr, fuluErr)
}

func (b *BlobHistoryDownloader) recoverDenebBlobs(blocks []*cltypes.SignedBeaconBlock) error {
	req, err := BlobsIdentifiersFromBlocks(blocks, b.beaconCfg)
	if err != nil {
		return fmt.Errorf("generate blob identifiers: %w", err)
	}
	batch, err := newDenebRecoveryBatch(blocks, req)
	if err != nil {
		return err
	}
	remaining := req
	for remaining.Len() > 0 {
		blobs, err := b.requestBlobs(b.ctx, b.rpc, remaining)
		if err != nil {
			return fmt.Errorf("request blobs: %w", err)
		}
		if blobs == nil {
			return errors.New("request blobs: empty result")
		}
		progress, err := batch.validate(remaining, blobs.Responses)
		if err != nil {
			return err
		}
		if progress == 0 {
			return errors.New("request blobs: response made no progress")
		}
		if err := batch.store(b.ctx, b.blobStorage); err != nil {
			return err
		}
		remaining = batch.remaining()
	}
	return nil
}

// recoverFuluColumns bounds each PeerDAS recovery attempt independently.
func (b *BlobHistoryDownloader) recoverFuluColumns(blocks []*cltypes.SignedBeaconBlock) error {
	peerDas := b.peerDasGetter.GetPeerDas()
	var recoveryErr error
	for _, block := range blocks {
		// [Modified in Gloas:EIP7732] Use ColumnSyncableSignedBlock interface
		ctx, cancel := context.WithTimeout(b.ctx, b.columnBackfillTimeout)
		err := peerDas.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block})
		cancel()
		if err != nil {
			b.logger.Warn("[BlobHistoryDownloader] Error recovering blobs from block", "err", err, "slot", block.GetSlot())
			recoveryErr = errors.Join(recoveryErr, err)
			continue
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			recoveryErr = errors.Join(recoveryErr, err)
			continue
		}
		complete, err := b.actualBlobSetComplete(block, blockRoot)
		if err != nil {
			recoveryErr = errors.Join(recoveryErr, err)
			continue
		}
		if !complete {
			forced, ok := peerDas.(forcedBlobRecoverer)
			if !ok {
				recoveryErr = errors.Join(recoveryErr, fmt.Errorf("incomplete Fulu blob recovery at slot %d", block.GetSlot()))
				continue
			}
			ctx, cancel := context.WithTimeout(b.ctx, b.columnBackfillTimeout)
			err = forced.ForceScheduleRecover(ctx, block.GetSlot(), blockRoot, uint64(block.Block.Body.BlobKzgCommitments.Len()))
			cancel()
			if err != nil {
				recoveryErr = errors.Join(recoveryErr, err)
				continue
			}
			complete, err = b.actualBlobSetComplete(block, blockRoot)
			if err != nil {
				recoveryErr = errors.Join(recoveryErr, err)
				continue
			}
			if !complete {
				recoveryErr = errors.Join(recoveryErr, fmt.Errorf("incomplete Fulu blob recovery at slot %d", block.GetSlot()))
			}
		}
	}
	return recoveryErr
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
	groups map[common.Hash]*requestedBlobBlock
	order  []common.Hash
}

func newDenebRecoveryBatch(blocks []*cltypes.SignedBeaconBlock, req *solid.ListSSZ[*cltypes.BlobIdentifier]) (*denebRecoveryBatch, error) {
	batch := &denebRecoveryBatch{
		groups: make(map[common.Hash]*requestedBlobBlock, len(blocks)),
		order:  make([]common.Hash, 0, len(blocks)),
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

func (b *denebRecoveryBatch) validate(req *solid.ListSSZ[*cltypes.BlobIdentifier], sidecars []*cltypes.BlobSidecar) (int, error) {
	requested := make(map[blobIdentifierKey]struct{}, req.Len())
	for i := range req.Len() {
		id := req.Get(i)
		requested[blobIdentifierKey{root: id.BlockRoot, index: id.Index}] = struct{}{}
	}
	progress := 0
	for _, sidecar := range sidecars {
		if sidecar == nil || sidecar.SignedBlockHeader == nil || sidecar.SignedBlockHeader.Header == nil {
			return progress, errors.New("blob response contains incomplete sidecar")
		}
		root, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil {
			return progress, err
		}
		key := blobIdentifierKey{root: root, index: sidecar.Index}
		if _, ok := requested[key]; !ok {
			return progress, fmt.Errorf("unrequested blob sidecar %x:%d", root, sidecar.Index)
		}
		group := b.groups[root]
		if group == nil || group.block == nil {
			return progress, fmt.Errorf("blob response block %x is not in batch", root)
		}
		if _, duplicate := group.sidecars[sidecar.Index]; duplicate {
			return progress, fmt.Errorf("duplicate blob sidecar %x:%d", root, sidecar.Index)
		}
		if sidecar.SignedBlockHeader.Signature != group.block.Signature {
			return progress, errors.New("signature mismatch between blob and stored block")
		}
		if !cltypes.VerifyCommitmentInclusionProof(sidecar.KzgCommitment, sidecar.CommitmentInclusionProof, sidecar.Index, clparams.DenebVersion, sidecar.SignedBlockHeader.Header.BodyRoot) {
			return progress, errors.New("could not verify blob's inclusion proof")
		}
		if err := kzg.Ctx().VerifyBlobKZGProof((*goethkzg.Blob)(&sidecar.Blob), goethkzg.KZGCommitment(sidecar.KzgCommitment), goethkzg.KZGProof(sidecar.KzgProof)); err != nil {
			return progress, errors.New("sidecar is wrong")
		}
		group.sidecars[sidecar.Index] = sidecar
		progress++
	}
	return progress, nil
}

func (b *denebRecoveryBatch) store(ctx context.Context, storage blob_storage.BlobStorage) error {
	for _, root := range b.order {
		group := b.groups[root]
		if group.stored || len(group.sidecars) != len(group.ids) {
			continue
		}
		orderedSidecars := make([]*cltypes.BlobSidecar, 0, len(group.ids))
		for _, id := range group.ids {
			orderedSidecars = append(orderedSidecars, group.sidecars[id.Index])
		}
		if err := storage.WriteBlobSidecars(ctx, root, orderedSidecars); err != nil {
			return fmt.Errorf("store blobs: %w", err)
		}
		group.stored = true
	}
	return nil
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
