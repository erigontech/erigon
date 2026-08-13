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
	highestBackfilledSlot atomic.Uint64
	// nextBackfillTargetSlot is the oldest slot the next pass must reach.
	nextBackfillTargetSlot uint64
	denebStartSlot         uint64
	// archiveBlobs indicates whether to archive all blobs or just recent ones
	archiveBlobs bool
	// immediateBlobsBackfilling indicates whether to backfill blobs immediately
	immediateBlobsBackfilling bool
	// columnBackfillTimeout bounds each fulu block's PeerDAS column recovery
	columnBackfillTimeout time.Duration

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

	targetSlot := b.nextBackfillTargetSlot
	// in case of non-archive mode, we only backfill the last relevant epochs
	if !b.archiveBlobs {
		targetSlot = currentSlot - min(currentSlot, b.beaconCfg.MinSlotsForBlobsSidecarsRequest())
	}

	if shouldLog {
		b.logger.Info("[BlobHistoryDownloader] Downloading blobs backwards", "slot", currentSlot)
	}

	for currentSlot >= targetSlot {
		firstUnfrozenSlot := max(targetSlot, b.sn.FrozenBlobs())
		if currentSlot < firstUnfrozenSlot {
			break
		}
		if !b.peersAvailable() {
			return nil
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
			if !b.processBatch(batch) {
				return nil
			}
			b.highestBackfilledSlot.Store(currentSlot)
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
		blobsCount, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
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
		if commitments.Len() == int(blobsCount) {
			continue
		}
		batch = append(batch, block)
	}
	return batch, visited, nil
}

// processBatch recovers each block's blobs: Deneb by-root, Fulu from PeerDAS columns.
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
	if len(denebBlocks) > 0 {
		if !b.recoverDenebBlobs(denebBlocks) {
			return false
		}
	}
	if len(fuluBlocks) > 0 {
		if !b.recoverFuluColumns(fuluBlocks) {
			return false
		}
	}
	return true
}

func (b *BlobHistoryDownloader) recoverDenebBlobs(blocks []*cltypes.SignedBeaconBlock) bool {
	req, err := BlobsIdentifiersFromBlocks(blocks, b.beaconCfg)
	if err != nil {
		b.logger.Debug("[BlobHistoryDownloader] Error generating blob identifiers", "err", err)
		return false
	}
	if req.Len() == 0 {
		return true
	}
	batch, err := newDenebRecoveryBatch(blocks, req)
	if err != nil {
		return false
	}
	_, err = requestBlobsForBackfill(b.ctx, b.rpc, batch.remaining, func(ctx context.Context, candidate *PeerAndSidecars) (bool, bool, error) {
		progress, err := batch.validate(candidate.Responses)
		if err != nil {
			return false, false, err
		}
		if err := batch.store(ctx, b.blobStorage); err != nil {
			return false, false, err
		}
		return progress > 0, batch.remaining().Len() == 0, nil
	})
	if err != nil {
		b.logger.Debug("[BlobHistoryDownloader] Error requesting blobs", "err", err)
		return false
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
		groups:   make(map[common.Hash]*requestedBlobBlock, len(blocks)),
		order:    make([]common.Hash, 0, len(blocks)),
		verifier: blob_storage.VerifyBlobSidecars,
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

func (b *denebRecoveryBatch) validate(sidecars []*cltypes.BlobSidecar) (int, error) {
	remaining := b.remaining()
	requested := make(map[blobIdentifierKey]struct{}, remaining.Len())
	for i := range remaining.Len() {
		id := remaining.Get(i)
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
		if _, duplicate := seen[key]; duplicate {
			return 0, fmt.Errorf("duplicate blob sidecar %x:%d", root, sidecar.Index)
		}
		seen[key] = struct{}{}
	}
	if len(sidecars) == 0 {
		return 0, errors.New("empty blob response")
	}
	if err := b.verifier(sidecars, func(header *cltypes.SignedBeaconBlockHeader) error {
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
	for _, sidecar := range sidecars {
		root, _ := sidecar.SignedBlockHeader.Header.HashSSZ()
		b.groups[root].sidecars[sidecar.Index] = sidecar
	}
	return len(sidecars), nil
}

func (b *denebRecoveryBatch) store(ctx context.Context, storage blob_storage.BlobStorage) error {
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
			return err
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

// recoverFuluColumns recovers blobs from PeerDAS columns, bounding each attempt so
// columns no peer still serves can't block the backfill indefinitely.
func (b *BlobHistoryDownloader) recoverFuluColumns(blocks []*cltypes.SignedBeaconBlock) bool {
	peerDas := b.peerDasGetter.GetPeerDas()
	for _, block := range blocks {
		// [Modified in Gloas:EIP7732] Use ColumnSyncableSignedBlock interface
		ctx, cancel := context.WithTimeout(b.ctx, b.columnBackfillTimeout)
		err := peerDas.DownloadColumnsAndRecoverBlobs(ctx, []cltypes.ColumnSyncableSignedBlock{block})
		cancel()
		if err != nil {
			b.logger.Warn("[BlobHistoryDownloader] Error recovering blobs from block", "err", err, "slot", block.GetSlot())
			return false
		}
		commitments := block.GetBlobKzgCommitments()
		if commitments == nil {
			b.logger.Warn("[BlobHistoryDownloader] Blob recovery completed without commitments", "slot", block.GetSlot())
			return false
		}
		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			b.logger.Warn("[BlobHistoryDownloader] Failed to hash recovered block", "err", err, "slot", block.GetSlot())
			return false
		}
		stored, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
		if err != nil || int(stored) != commitments.Len() {
			b.logger.Warn("[BlobHistoryDownloader] Blob recovery did not satisfy durable postcondition", "err", err, "slot", block.GetSlot(), "stored", stored, "expected", commitments.Len())
			return false
		}
	}
	return true
}
