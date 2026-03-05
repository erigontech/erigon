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
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

const (
	blobDownloaderInterval      = 12 * time.Second
	blobLogInterval             = 30 * time.Second
	blobBackfillWarningInterval = 4 * time.Minute
	blocksBatchSize             = uint64(8)
	maxIterations               = uint64(32)
	minPeersForBlobDownload     = 16
)

// SyncedChecker is an interface to check if the forkchoice is synced
type SyncedChecker interface {
	Synced() bool
}

// PeerDasGetter is an interface to get the PeerDas instance
type PeerDasGetter interface {
	GetPeerDas() das.PeerDas
}

// BlobHistoryDownloader downloads blob history backwards from a head slot
type BlobHistoryDownloader struct {
	ctx context.Context

	beaconCfg   *clparams.BeaconChainConfig
	rpc         *rpc.BeaconRpcP2P
	indiciesDB  kv.RoDB
	blobStorage blob_storage.BlobStorage
	blockReader freezeblocks.BeaconSnapshotReader
	sn          *freezeblocks.CaplinSnapshots

	syncedChecker SyncedChecker
	peerDasGetter PeerDasGetter

	// headSlot is the slot we start downloading from (currentSlot + 1)
	headSlot atomic.Uint64
	// highestBackfilledSlot is the highest slot we've successfully backfilled to
	highestBackfilledSlot atomic.Uint64
	// targetSlot is the slot we're trying to reach (Deneb fork epoch start)
	targetSlot uint64
	// archiveBlobs indicates whether to archive all blobs or just recent ones
	archiveBlobs bool
	// immediateBlobsBackfilling indicates whether to backfill blobs immediately
	immediateBlobsBackfilling bool

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
		targetSlot:                targetSlot,
		archiveBlobs:              archiveBlobs,
		immediateBlobsBackfilling: immediateBlobsBackfilling,
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

	// Check peer count before proceeding
	peers, err := b.rpc.Peers()
	if err != nil {
		b.logger.Warn("[BlobHistoryDownloader] Failed to get peer count", "err", err)
		return nil
	}
	if peers < minPeersForBlobDownload {
		b.logger.Warn("[BlobHistoryDownloader] Skipping iteration due to low peer count", "peers", peers, "required", minPeersForBlobDownload)
		return nil
	}

	tx, err := b.indiciesDB.BeginRo(b.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	logInterval := time.NewTicker(blobLogInterval)
	defer logInterval.Stop()

	prevLogSlot := currentSlot
	prevTime := time.Now()

	targetSlot := b.targetSlot
	// in case of non-archive mode, we only backfill the last relevant epochs
	if !b.archiveBlobs {
		targetSlot = currentSlot - min(currentSlot, b.beaconCfg.MinSlotsForBlobsSidecarsRequest())
	}

	defer func() {
		// set target slot back in case it was modified
		b.targetSlot = currentSlot - b.beaconCfg.SlotsPerEpoch*2
	}()

	if shouldLog {
		b.logger.Info("[BlobHistoryDownloader] Downloading blobs backwards", "slot", currentSlot)
	}

	for currentSlot >= targetSlot {
		if currentSlot <= b.sn.FrozenBlobs() {
			break
		}
		if !b.syncedChecker.Synced() {
			time.Sleep(5 * time.Second)
			continue
		}

		batch := make([]*cltypes.SignedBlindedBeaconBlock, 0, blocksBatchSize)
		visited := uint64(0)
		for ; visited < blocksBatchSize; visited++ {
			if visited >= maxIterations {
				break
			}
			if currentSlot-visited < targetSlot {
				break
			}
			block, err := b.blockReader.ReadBlindedBlockBySlot(b.ctx, tx, currentSlot-visited)
			if err != nil {
				return err
			}
			if block == nil {
				continue
			}
			if block.Version() < clparams.DenebVersion {
				break
			}
			blockRoot, err := block.Block.HashSSZ()
			if err != nil {
				return err
			}
			blobsCount, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
			if err != nil {
				return err
			}

			if block.Block.Body.BlobKzgCommitments.Len() == int(blobsCount) {
				continue
			}
			batch = append(batch, block)
		}
		if len(batch) == 0 {
			currentSlot -= visited
			continue
		}

		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		case <-logInterval.C:
			if !shouldLog {
				continue
			}
			blkSec := float64(prevLogSlot-currentSlot) / time.Since(prevTime).Seconds()
			blkSecStr := fmt.Sprintf("%.1f", blkSec)
			prevLogSlot = currentSlot
			prevTime = time.Now()

			b.logger.Info("[BlobHistoryDownloader] Downloading blobs backwards", "slot", currentSlot, "blks/sec", blkSecStr)
		default:
		}

		// Generate the request
		fuluBlocks := []*cltypes.SignedBlindedBeaconBlock{}
		denebBlocks := []*cltypes.SignedBlindedBeaconBlock{}
		for _, block := range batch {
			if block.Version() >= clparams.FuluVersion {
				fuluBlocks = append(fuluBlocks, block)
			} else {
				denebBlocks = append(denebBlocks, block)
			}
		}

		if len(denebBlocks) > 0 {
			req, err := BlobsIdentifiersFromBlindedBlocks(batch, b.beaconCfg)
			if err != nil {
				b.logger.Debug("[BlobHistoryDownloader] Error generating blob identifiers", "err", err)
				continue
			}
			// Request the blobs
			blobs, err := RequestBlobsFrantically(b.ctx, b.rpc, req)
			if err != nil {
				b.logger.Debug("[BlobHistoryDownloader] Error requesting blobs", "err", err)
				continue
			}
			_, _, err = blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(b.ctx, b.blobStorage, req, blobs.Responses, func(header *cltypes.SignedBeaconBlockHeader) error {
				// The block is preverified so just check that the signature is correct against the block
				for _, block := range batch {
					if block.Block.Slot != header.Header.Slot {
						continue
					}
					if block.Signature != header.Signature {
						return errors.New("signature mismatch between blob and stored block")
					}
					return nil
				}
				return errors.New("block not in batch")
			})
			if err != nil {
				b.rpc.BanPeer(blobs.Peer)
				b.logger.Warn("[BlobHistoryDownloader] Error verifying blobs", "err", err)
				continue
			}
		}
		if len(fuluBlocks) > 0 {
			peerDas := b.peerDasGetter.GetPeerDas()
			for _, block := range fuluBlocks {
				if err := peerDas.DownloadColumnsAndRecoverBlobs(b.ctx, []*cltypes.SignedBlindedBeaconBlock{block}); err != nil {
					b.logger.Warn("[BlobHistoryDownloader] Error recovering blobs from block", "err", err, "slot", block.Block.Slot)
				}
			}
		}

		// Update highest backfilled slot
		b.highestBackfilledSlot.Store(currentSlot)
	}

	if shouldLog {
		b.logger.Info("[BlobHistoryDownloader] Blob history download finished successfully")
	}

	b.backfillCompleted.Store(true)

	b.mu.RLock()
	notify := b.notifyBlobBackfilled
	b.mu.RUnlock()
	if notify != nil {
		notify()
	}

	return nil
}
