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
	"sync"
	"sync/atomic"
	"time"

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

	// notifyBlobBackfilled is called when blob backfilling is complete
	notifyBlobBackfilled func()

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
	finalScanSlot := currentSlot

	targetSlot := b.nextBackfillTargetSlot
	// in case of non-archive mode, we only backfill the last relevant epochs
	if !b.archiveBlobs {
		retentionFloor := currentSlot - min(currentSlot, b.beaconCfg.MinSlotsForBlobsSidecarsRequest())
		targetSlot = max(targetSlot, retentionFloor)
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

		batch, visited, err := b.collectIncompleteBlocks(currentSlot, firstUnfrozenSlot)
		if err != nil {
			return err
		}
		scanned := max(visited, 1)
		finalScanSlot = currentSlot - (scanned - 1)

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
	b.nextBackfillTargetSlot = max(b.denebStartSlot, finalScanSlot-min(finalScanSlot, b.beaconCfg.SlotsPerEpoch*2))

	b.backfillCompleted.Store(true)

	b.mu.RLock()
	notify := b.notifyBlobBackfilled
	b.mu.RUnlock()
	if notify != nil {
		notify()
	}

	return nil
}

func (b *BlobHistoryDownloader) peersAvailable() bool {
	peers, err := b.rpc.Peers()
	if err != nil {
		b.logger.Warn("[BlobHistoryDownloader] Failed to get peer count", "err", err)
		return false
	}
	if peers == 0 {
		b.logger.Warn("[BlobHistoryDownloader] Skipping iteration because no peers are available")
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
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return false
		}
		if block.GetBlobKzgCommitments().Len() > 0 {
			requestedBlocks = append(requestedBlocks, block)
			continue
		}
		stored, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
		if err != nil {
			return false
		}
		if stored > 0 {
			if err := b.blobStorage.RemoveBlobSidecars(b.ctx, block.GetSlot(), blockRoot); err != nil {
				return false
			}
		}
	}
	if len(requestedBlocks) == 0 {
		return true
	}
	req, err := BlobsIdentifiersFromBlocks(requestedBlocks, b.beaconCfg)
	if err != nil {
		b.logger.Debug("[BlobHistoryDownloader] Error generating blob identifiers", "err", err)
		return false
	}
	batch, err := newDenebRecoveryBatch(requestedBlocks, req)
	if err != nil {
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
		return false
	}
	for _, block := range blocks {
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return false
		}
		_, complete, err := b.storedBlobSidecarsComplete(b.ctx, block, blockRoot)
		if err != nil || !complete {
			return false
		}
	}
	return true
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
	for _, block := range blocks {
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			b.logger.Warn("[BlobHistoryDownloader] Failed to hash recovered block", "err", err, "slot", block.GetSlot())
			return false
		}
		commitments := block.GetBlobKzgCommitments()
		if commitments == nil {
			b.logger.Warn("[BlobHistoryDownloader] Blob recovery completed without commitments", "slot", block.GetSlot())
			return false
		}
		ctx, cancel := context.WithTimeout(b.ctx, b.columnBackfillTimeout)
		stored, complete, err := b.storedBlobSidecarsComplete(ctx, block, blockRoot)
		if err != nil {
			cancel()
			b.logger.Warn("[BlobHistoryDownloader] Failed to read stored blobs", "err", err, "slot", block.GetSlot())
			return false
		}
		if commitments.Len() == 0 {
			if stored > 0 {
				if err := b.blobStorage.RemoveBlobSidecars(ctx, block.GetSlot(), blockRoot); err != nil {
					cancel()
					b.logger.Warn("[BlobHistoryDownloader] Failed to clear incomplete blob storage", "err", err, "slot", block.GetSlot())
					return false
				}
			}
			cancel()
			continue
		}
		if complete {
			cancel()
			continue
		}
		if stored > 0 {
			if err := b.blobStorage.RemoveBlobSidecars(ctx, block.GetSlot(), blockRoot); err != nil {
				cancel()
				b.logger.Warn("[BlobHistoryDownloader] Failed to clear incomplete blob storage", "err", err, "slot", block.GetSlot())
				return false
			}
		}
		peerDas := b.peerDasGetter.GetPeerDas()
		err = peerDas.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block})
		if err != nil {
			cancel()
			b.logger.Warn("[BlobHistoryDownloader] Error recovering blobs from block", "err", err, "slot", block.GetSlot())
			return false
		}
		stored, complete, err = b.waitForStoredBlobSidecars(ctx, block, blockRoot)
		cancel()
		if err != nil || !complete {
			b.logger.Warn("[BlobHistoryDownloader] Blob recovery did not satisfy durable postcondition", "err", err, "slot", block.GetSlot(), "stored", stored, "expected", commitments.Len())
			return false
		}
	}
	return true
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
	stored, err := b.blobStorage.KzgCommitmentsCount(ctx, blockRoot)
	if commitments.Len() == 0 {
		return stored, stored == 0, err
	}
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
