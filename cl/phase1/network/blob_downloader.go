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
	"sort"
	"sync"
	"sync/atomic"
	"time"

	goethkzg "github.com/crate-crypto/go-eth-kzg"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
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

type blobBackfillRequester interface {
	SendBlobsSidecarByIdentifierReqForBackfill(context.Context, *solid.ListSSZ[*cltypes.BlobIdentifier]) ([]*cltypes.BlobSidecar, string, error)
}

type blobSnapshotReader interface {
	FrozenBlobs() uint64
}

type blobRequestFn func(context.Context, BlobPeerClient, *solid.ListSSZ[*cltypes.BlobIdentifier], func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error)

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
	columnBackfillTimeout     time.Duration

	running           atomic.Bool
	backfillCompleted atomic.Bool
	completedRanges   []backfillRange
	headRoot          common.Hash
	headGeneration    uint64
	activePasses      map[*backfillPass]struct{}
	transitionCeiling uint64
	transitionActive  bool
	completionAudit   uint64
	auditInitialized  bool
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
	downloader := &BlobHistoryDownloader{
		ctx:         ctx,
		beaconCfg:   beaconCfg,
		rpc:         rpc,
		indiciesDB:  indiciesDB,
		blobStorage: blobStorage,
		blockReader: blockReader,
		requestBlobs: func(ctx context.Context, client BlobPeerClient, req *solid.ListSSZ[*cltypes.BlobIdentifier], validate func([]*cltypes.BlobSidecar) error) (*PeerAndSidecars, error) {
			if backfillClient, ok := client.(blobBackfillRequester); ok {
				return requestBlobsFranticallyValidated(ctx, req, backfillClient.SendBlobsSidecarByIdentifierReqForBackfill, blobPeerRejecterFor(client), validate)
			}
			return requestBlobsFranticallyValidated(ctx, req, client.SendBlobsSidecarByIdentifierReq, blobPeerRejecterFor(client), validate)
		},
		syncedChecker:             syncedChecker,
		peerDasGetter:             peerDasGetter,
		targetSlot:                targetSlot,
		archiveBlobs:              archiveBlobs,
		immediateBlobsBackfilling: immediateBlobsBackfilling,
		columnBackfillTimeout:     blobColumnBackfillTimeout,
		logger:                    logger,
	}
	if sn != nil {
		downloader.sn = sn
	}
	return downloader
}

// SetHead sets the inclusive upper bound and preserves completion only through safeThrough.
func (b *BlobHistoryDownloader) SetHead(slot uint64, root common.Hash, safeThrough uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.constrainActivePasses(safeThrough)
	b.headGeneration++
	b.headSlot.Store(slot)
	b.trimCompletedRanges(safeThrough)
	b.headRoot = root
	b.transitionActive = false
	unfrozenRanges := b.unfrozenBackfillRanges(slot, b.frozenBlobs())
	if len(unfrozenRanges) > 0 && !b.completedRangesContain(unfrozenRanges) {
		b.backfillCompleted.Store(false)
	}
}

// SetHeadSlot initializes the scan head without changing canonical completion metadata.
func (b *BlobHistoryDownloader) SetHeadSlot(slot uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.headGeneration++
	b.headSlot.Store(slot)
}

// InvalidateCompletionAbove conservatively trims completed coverage before a canonical-head change is committed.
func (b *BlobHistoryDownloader) InvalidateCompletionAbove(safeThrough uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.transitionActive {
		b.transitionCeiling = min(b.transitionCeiling, safeThrough)
	} else {
		b.transitionCeiling = safeThrough
		b.transitionActive = true
	}
	b.constrainActivePasses(b.transitionCeiling)
	b.headGeneration++
	b.trimCompletedRanges(b.transitionCeiling)
	unfrozenRanges := b.unfrozenBackfillRanges(b.headSlot.Load(), b.frozenBlobs())
	if len(unfrozenRanges) > 0 && !b.completedRangesContain(unfrozenRanges) {
		b.backfillCompleted.Store(false)
	}
}

// AbortHeadUpdate ends a failed canonical-head transition without restoring invalidated completion.
func (b *BlobHistoryDownloader) AbortHeadUpdate() {
	b.mu.Lock()
	b.transitionActive = false
	b.mu.Unlock()
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

// BlobBackfillPending reports whether a slot remains outside durable completed coverage.
func (b *BlobHistoryDownloader) BlobBackfillPending(slot uint64) bool {
	if !b.archiveBlobs && !b.immediateBlobsBackfilling {
		return false
	}
	if slot < b.frozenBlobs() {
		return false
	}
	b.mu.RLock()
	defer b.mu.RUnlock()
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
	if b.transitionActive && slot > b.transitionCeiling {
		return true
	}
	completed := b.completedSlot(slot)
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

type backfillPass struct {
	safeThrough uint64
	headSlot    uint64
	generation  uint64
}

func (b *BlobHistoryDownloader) constrainActivePasses(safeThrough uint64) {
	for pass := range b.activePasses {
		pass.safeThrough = min(pass.safeThrough, safeThrough)
	}
}

func (b *BlobHistoryDownloader) addPassCompletedRanges(pass *backfillPass, ranges []backfillRange) {
	firstUnfrozenSlot := b.frozenBlobs()
	b.trimCompletedBefore(firstUnfrozenSlot)
	accepted := make([]backfillRange, 0, len(ranges))
	for _, completed := range ranges {
		if completed.start > pass.safeThrough || completed.end < firstUnfrozenSlot {
			continue
		}
		completed.start = max(completed.start, firstUnfrozenSlot)
		completed.end = min(completed.end, pass.safeThrough)
		if completed.start > completed.end {
			continue
		}
		accepted = append(accepted, completed)
	}
	b.addCompletedRanges(accepted)
}

func (b *BlobHistoryDownloader) constrainPassForCurrentHead(pass *backfillPass) {
	if pass.generation == b.headGeneration {
		return
	}
	pass.safeThrough = min(pass.safeThrough, pass.headSlot, b.headSlot.Load())
}

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

func (b *BlobHistoryDownloader) frozenBlobs() uint64 {
	if b.sn == nil {
		return 0
	}
	return b.sn.FrozenBlobs()
}

func (b *BlobHistoryDownloader) unfrozenBackfillRanges(head, firstUnfrozenSlot uint64) []backfillRange {
	ranges := b.backfillRanges(head)
	unfrozen := ranges[:0]
	for _, candidate := range ranges {
		candidate.start = max(candidate.start, firstUnfrozenSlot)
		if candidate.start <= candidate.end {
			unfrozen = append(unfrozen, candidate)
		}
	}
	return unfrozen
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
	index, found := slices.BinarySearchFunc(b.completedRanges, start, func(completed backfillRange, slot uint64) int {
		switch {
		case completed.end < slot:
			return -1
		case completed.start > slot:
			return 1
		default:
			return 0
		}
	})
	if !found {
		return false
	}
	return end <= b.completedRanges[index].end
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

func (b *BlobHistoryDownloader) trimCompletedBefore(firstUnfrozenSlot uint64) {
	first := sort.Search(len(b.completedRanges), func(i int) bool {
		return b.completedRanges[i].end >= firstUnfrozenSlot
	})
	if first == 0 {
		if len(b.completedRanges) > 0 {
			b.completedRanges[0].start = max(b.completedRanges[0].start, firstUnfrozenSlot)
		}
		return
	}
	if first == len(b.completedRanges) {
		b.completedRanges = nil
		return
	}
	kept := append([]backfillRange(nil), b.completedRanges[first:]...)
	kept[0].start = max(kept[0].start, firstUnfrozenSlot)
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

func (b *BlobHistoryDownloader) completedAuditRanges(desired []backfillRange, firstUnfrozenSlot uint64) []backfillRange {
	auditable := make([]backfillRange, 0, len(b.completedRanges))
	for _, completed := range b.completedRanges {
		for _, wanted := range desired {
			start := max(completed.start, wanted.start, firstUnfrozenSlot)
			end := min(completed.end, wanted.end)
			if start <= end {
				auditable = append(auditable, backfillRange{start: start, end: end})
			}
		}
	}
	return auditable
}

func (b *BlobHistoryDownloader) nextCompletionAuditRange(desired []backfillRange) backfillRange {
	rangeIndex := len(desired) - 1
	if b.auditInitialized {
		for i, candidate := range desired {
			if b.completionAudit >= candidate.start && b.completionAudit <= candidate.end {
				rangeIndex = i
				break
			}
		}
	} else {
		b.auditInitialized = true
		b.completionAudit = desired[rangeIndex].end
	}
	selected := desired[rangeIndex]
	end := b.completionAudit
	if end < selected.start || end > selected.end {
		end = selected.end
	}
	start := selected.start
	if end-selected.start >= blocksBatchSize-1 {
		start = end - (blocksBatchSize - 1)
	}
	switch {
	case start > selected.start:
		b.completionAudit = start - 1
	case rangeIndex > 0:
		b.completionAudit = desired[rangeIndex-1].end
	default:
		b.completionAudit = desired[len(desired)-1].end
	}
	return backfillRange{start: start, end: end}
}

func (b *BlobHistoryDownloader) removeCompletedRange(removed backfillRange) {
	kept := make([]backfillRange, 0, len(b.completedRanges)+1)
	for _, completed := range b.completedRanges {
		if removed.end < completed.start || removed.start > completed.end {
			kept = append(kept, completed)
			continue
		}
		if completed.start < removed.start {
			kept = append(kept, backfillRange{start: completed.start, end: removed.start - 1})
		}
		if removed.end < completed.end {
			kept = append(kept, backfillRange{start: removed.end + 1, end: completed.end})
		}
	}
	b.completedRanges = kept
}

func (b *BlobHistoryDownloader) revalidateCompletedRange(headSlot, generation uint64, audit backfillRange) error {
	batch, _, _, err := b.collectIncompleteBlocks(audit.end, audit.start, headSlot)
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation != b.headGeneration {
		return nil
	}
	if err != nil {
		b.removeCompletedRange(audit)
		b.backfillCompleted.Store(false)
		return err
	}
	for _, block := range batch {
		b.removeCompletedRange(backfillRange{start: block.GetSlot(), end: block.GetSlot()})
	}
	if len(batch) == 0 {
		return nil
	}
	b.backfillCompleted.Store(false)
	return nil
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
	b.mu.Lock()
	headSlot := b.headSlot.Load()
	headGeneration := b.headGeneration
	firstUnfrozenSlot := b.frozenBlobs()
	b.trimCompletedBefore(firstUnfrozenSlot)
	desiredRanges := b.unfrozenBackfillRanges(headSlot, firstUnfrozenSlot)
	if len(desiredRanges) == 0 {
		b.backfillCompleted.Store(true)
		b.mu.Unlock()
		return nil
	}
	auditableRanges := b.completedAuditRanges(desiredRanges, firstUnfrozenSlot)
	var audit *backfillRange
	if len(auditableRanges) > 0 {
		next := b.nextCompletionAuditRange(auditableRanges)
		audit = &next
	}
	b.mu.Unlock()
	if audit != nil {
		if err := b.revalidateCompletedRange(headSlot, headGeneration, *audit); err != nil {
			return err
		}
	}

	b.mu.Lock()
	if headGeneration != b.headGeneration {
		headSlot = b.headSlot.Load()
		headGeneration = b.headGeneration
		firstUnfrozenSlot = b.frozenBlobs()
		b.trimCompletedBefore(firstUnfrozenSlot)
		desiredRanges = b.unfrozenBackfillRanges(headSlot, firstUnfrozenSlot)
		if len(desiredRanges) == 0 {
			b.backfillCompleted.Store(true)
			b.mu.Unlock()
			return nil
		}
	}
	pendingRanges := b.incompleteRanges(desiredRanges)
	if len(pendingRanges) == 0 {
		b.backfillCompleted.Store(true)
		b.mu.Unlock()
		return nil
	}
	passCeiling := uint64(math.MaxUint64)
	if b.transitionActive {
		passCeiling = b.transitionCeiling
	}
	pass := &backfillPass{safeThrough: passCeiling, headSlot: headSlot, generation: headGeneration}
	if b.activePasses == nil {
		b.activePasses = make(map[*backfillPass]struct{})
	}
	b.activePasses[pass] = struct{}{}
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.activePasses, pass)
		b.mu.Unlock()
	}()
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
	partiallyCompleted := make([]backfillRange, 0)
	defer func() {
		if len(partiallyCompleted) == 0 {
			return
		}
		b.mu.Lock()
		b.constrainPassForCurrentHead(pass)
		b.addPassCompletedRanges(pass, partiallyCompleted)
		b.mu.Unlock()
	}()
	for _, work := range slices.Backward(pendingRanges) {
		currentSlot := work.end
		targetSlot := work.start
		for currentSlot >= targetSlot {
			firstUnfrozenSlot := max(targetSlot, b.frozenBlobs())
			if currentSlot < firstUnfrozenSlot {
				break
			}
			if !b.syncedChecker.Synced() {
				timer := time.NewTimer(5 * time.Second)
				select {
				case <-b.ctx.Done():
					if !timer.Stop() {
						<-timer.C
					}
					return b.ctx.Err()
				case <-timer.C:
				}
				continue
			}

			batch, alreadyCompleted, visited, err := b.collectIncompleteBlocks(currentSlot, firstUnfrozenSlot, headSlot)
			partiallyCompleted = append(partiallyCompleted, alreadyCompleted...)
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
				for _, block := range batch {
					root, err := block.Block.HashSSZ()
					if err != nil {
						passErr = errors.Join(passErr, err)
						continue
					}
					complete, err := b.actualBlobSetComplete(block, root)
					if err != nil {
						passErr = errors.Join(passErr, err)
						continue
					}
					if complete {
						partiallyCompleted = append(partiallyCompleted, backfillRange{start: block.GetSlot(), end: block.GetSlot()})
					}
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
	b.constrainPassForCurrentHead(pass)
	b.addPassCompletedRanges(pass, pendingRanges)
	currentDesiredRanges := b.unfrozenBackfillRanges(b.headSlot.Load(), b.frozenBlobs())
	complete := len(currentDesiredRanges) == 0 || b.completedRangesContain(currentDesiredRanges)
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
func (b *BlobHistoryDownloader) collectIncompleteBlocks(currentSlot, targetSlot, rangeHead uint64) (batch []*cltypes.SignedBeaconBlock, completed []backfillRange, visited uint64, err error) {
	tx, err := b.indiciesDB.BeginRo(b.ctx)
	if err != nil {
		return nil, nil, 0, err
	}
	defer tx.Rollback()

	batch = make([]*cltypes.SignedBeaconBlock, 0, blocksBatchSize)
	for ; visited < blocksBatchSize; visited++ {
		if currentSlot < visited || currentSlot-visited < targetSlot {
			break
		}
		block, err := b.blockReader.ReadBeaconBlockBodyBySlot(b.ctx, tx, currentSlot-visited)
		if err != nil {
			return batch, completed, visited, err
		}
		if block == nil {
			canonicalRoot, err := beacon_indicies.ReadCanonicalBlockRoot(tx, currentSlot-visited)
			if err != nil {
				return batch, completed, visited, err
			}
			if canonicalRoot != (common.Hash{}) {
				return batch, completed, visited, fmt.Errorf("canonical block body is unavailable at slot %d", currentSlot-visited)
			}
			continue
		}
		if block.Block == nil {
			return batch, completed, visited, fmt.Errorf("canonical block is incomplete at slot %d", currentSlot-visited)
		}
		if block.Block.Body == nil {
			return batch, completed, visited, fmt.Errorf("canonical block body is incomplete at slot %d", currentSlot-visited)
		}
		if block.Version() < clparams.DenebVersion {
			break
		}
		if !b.slotWithinBackfillRange(currentSlot-visited, rangeHead) {
			continue
		}
		commitments := block.Block.Body.GetBlobKzgCommitments()
		if commitments == nil {
			return batch, completed, visited, fmt.Errorf("canonical block at slot %d has nil kzg commitments", block.Block.Slot)
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return batch, completed, visited, err
		}
		complete, err := b.actualBlobSetComplete(block, blockRoot)
		if err != nil {
			return batch, completed, visited, err
		}
		if complete {
			completed = append(completed, backfillRange{start: block.GetSlot(), end: block.GetSlot()})
			continue
		}
		batch = append(batch, block)
	}
	return batch, completed, visited, nil
}

func (b *BlobHistoryDownloader) actualBlobSetComplete(block *cltypes.SignedBeaconBlock, blockRoot common.Hash) (bool, error) {
	commitments := blobCommitments(block)
	if commitments == nil {
		return false, nil
	}
	if commitments.Len() == 0 {
		return true, nil
	}
	count, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
	if err != nil || count != uint32(commitments.Len()) {
		return false, err
	}
	sidecars, complete, err := b.blobStorage.ReadBlobSidecars(b.ctx, block.GetSlot(), blockRoot)
	if errors.Is(err, blob_storage.ErrBlobSidecarCorrupt) {
		if removeErr := b.blobStorage.RemoveBlobSidecars(b.ctx, block.GetSlot(), blockRoot); removeErr != nil {
			return false, errors.Join(err, removeErr)
		}
		return false, nil
	}
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
		if sidecar.SignedBlockHeader.Signature != block.Signature {
			return false, nil
		}
		if err := kzg.Ctx().VerifyBlobKZGProof((*goethkzg.Blob)(&sidecar.Blob), goethkzg.KZGCommitment(sidecar.KzgCommitment), goethkzg.KZGProof(sidecar.KzgProof)); err != nil {
			return false, nil
		}
		if block.Version() < clparams.GloasVersion && !cltypes.VerifyCommitmentInclusionProof(sidecar.KzgCommitment, sidecar.CommitmentInclusionProof, sidecar.Index, clparams.DenebVersion, sidecar.SignedBlockHeader.Header.BodyRoot) {
			return false, nil
		}
	}
	return true, nil
}

func blobCommitments(block *cltypes.SignedBeaconBlock) *solid.ListSSZ[*cltypes.KZGCommitment] {
	if block == nil || block.Block == nil || block.Block.Body == nil {
		return nil
	}
	return block.Block.Body.GetBlobKzgCommitments()
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
		blobs, err := b.requestBlobs(b.ctx, b.rpc, remaining, batch.validateCandidate)
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

func (b *BlobHistoryDownloader) recoverFuluColumns(blocks []*cltypes.SignedBeaconBlock) error {
	peerDas := b.peerDasGetter.GetPeerDas()
	var recoveryErr error
	for _, block := range blocks {
		commitments := blobCommitments(block)
		if commitments == nil {
			recoveryErr = errors.Join(recoveryErr, errors.New("cannot recover Fulu block without kzg commitments"))
			continue
		}
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
			err = forced.ForceScheduleRecover(ctx, block.GetSlot(), blockRoot, uint64(commitments.Len()))
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
	seen := make(map[blobIdentifierKey]struct{}, len(sidecars))
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
		if _, duplicate := group.sidecars[sidecar.Index]; duplicate {
			return 0, fmt.Errorf("duplicate blob sidecar %x:%d", root, sidecar.Index)
		}
		if _, duplicate := seen[key]; duplicate {
			return 0, fmt.Errorf("duplicate blob sidecar %x:%d", root, sidecar.Index)
		}
		if sidecar.SignedBlockHeader.Signature != group.block.Signature {
			return 0, errors.New("signature mismatch between blob and stored block")
		}
		seen[key] = struct{}{}
	}
	if err := blob_storage.VerifyBlobSidecars(sidecars, nil); err != nil {
		return 0, err
	}
	for _, sidecar := range sidecars {
		root, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil {
			return 0, err
		}
		group := b.groups[root]
		group.sidecars[sidecar.Index] = sidecar
	}
	return len(sidecars), nil
}

func (b *denebRecoveryBatch) validateCandidate(sidecars []*cltypes.BlobSidecar) error {
	for _, sidecar := range sidecars {
		root, err := sidecar.SignedBlockHeader.Header.HashSSZ()
		if err != nil {
			return err
		}
		group := b.groups[root]
		if group == nil || group.block == nil {
			return fmt.Errorf("blob response block %x is not in batch", root)
		}
		if sidecar.SignedBlockHeader.Signature != group.block.Signature {
			return errors.New("signature mismatch between blob and stored block")
		}
	}
	return nil
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
