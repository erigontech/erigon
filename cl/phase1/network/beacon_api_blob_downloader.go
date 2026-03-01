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
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/das"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

const (
	beaconApiBlobDownloaderInterval = 500 * time.Millisecond
	beaconApiBlobLogInterval        = 30 * time.Second
	beaconApiBlobTimeout            = 60 * time.Second
)

// BeaconApiBlobDownloader downloads blob history using a remote beacon API via data column sidecars
type BeaconApiBlobDownloader struct {
	ctx context.Context

	beaconCfg     *clparams.BeaconChainConfig
	apiUrl        string
	indiciesDB    kv.RoDB
	blobStorage   blob_storage.BlobStorage
	columnStorage blob_storage.DataColumnStorage
	blockReader   freezeblocks.BeaconSnapshotReader
	sn            *freezeblocks.CaplinSnapshots
	peerDas       das.PeerDas

	syncedChecker SyncedChecker

	// headSlot is the slot we start downloading from (currentSlot + 1)
	headSlot atomic.Uint64
	// highestBackfilledSlot is the highest slot we've successfully backfilled to
	highestBackfilledSlot atomic.Uint64
	// targetSlot is the slot we're trying to reach (Deneb fork epoch start)
	targetSlot uint64
	// archiveBlobs indicates whether to archive all blobs or just recent ones
	archiveBlobs bool

	running              atomic.Bool
	backfillCompleted    atomic.Bool
	notifyBlobBackfilled func()
	logger               log.Logger

	httpClient *http.Client
}

// NewBeaconApiBlobDownloader creates a new BeaconApiBlobDownloader
func NewBeaconApiBlobDownloader(
	ctx context.Context,
	beaconCfg *clparams.BeaconChainConfig,
	apiUrl string,
	indiciesDB kv.RoDB,
	blobStorage blob_storage.BlobStorage,
	columnStorage blob_storage.DataColumnStorage,
	blockReader freezeblocks.BeaconSnapshotReader,
	sn *freezeblocks.CaplinSnapshots,
	syncedChecker SyncedChecker,
	peerDas das.PeerDas,
	archiveBlobs bool,
	logger log.Logger,
) *BeaconApiBlobDownloader {
	targetSlot := beaconCfg.DenebForkEpoch * beaconCfg.SlotsPerEpoch
	return &BeaconApiBlobDownloader{
		ctx:           ctx,
		beaconCfg:     beaconCfg,
		apiUrl:        apiUrl,
		indiciesDB:    indiciesDB,
		blobStorage:   blobStorage,
		columnStorage: columnStorage,
		blockReader:   blockReader,
		sn:            sn,
		syncedChecker: syncedChecker,
		peerDas:       peerDas,
		targetSlot:    targetSlot,
		archiveBlobs:  archiveBlobs,
		logger:        logger,
		httpClient: &http.Client{
			Timeout: beaconApiBlobTimeout,
		},
	}
}

// SetHeadSlot sets the head slot to download from (should be currentSlot + 1)
func (b *BeaconApiBlobDownloader) SetHeadSlot(slot uint64) {
	b.headSlot.Store(slot)
}

// SetNotifyBlobBackfilled sets the callback to notify when blob backfilling is complete
func (b *BeaconApiBlobDownloader) SetNotifyBlobBackfilled(notify func()) {
	b.notifyBlobBackfilled = notify
}

// HeadSlot returns the current head slot
func (b *BeaconApiBlobDownloader) HeadSlot() uint64 {
	return b.headSlot.Load()
}

// HighestBackfilledSlot returns the highest slot that has been backfilled
func (b *BeaconApiBlobDownloader) HighestBackfilledSlot() uint64 {
	return b.highestBackfilledSlot.Load()
}

// Running returns whether the downloader is currently running
func (b *BeaconApiBlobDownloader) Running() bool {
	return b.running.Load()
}

// Start begins the blob history download loop using the beacon API
func (b *BeaconApiBlobDownloader) Start() {
	if !b.archiveBlobs {
		return // nothing to do
	}
	if b.running.Swap(true) {
		return // already running
	}

	go b.run()
}

func (b *BeaconApiBlobDownloader) run() {
	defer b.running.Store(false)

	b.logger.Info("[BeaconApiBlobDownloader] Starting blob backfill via beacon API (using data column sidecars)", "url", b.apiUrl)

	// Do an initial download immediately
	if err := b.downloadOnce(true); err != nil {
		b.logger.Error("[BeaconApiBlobDownloader] Error downloading blobs", "err", err)
	}

	downloadTimer := time.NewTimer(beaconApiBlobDownloaderInterval)
	defer downloadTimer.Stop()

	for {
		select {
		case <-b.ctx.Done():
			return
		case <-downloadTimer.C:
			if err := b.downloadOnce(false); err != nil {
				b.logger.Debug("[BeaconApiBlobDownloader] Error downloading blobs", "err", err)
			}
			downloadTimer.Reset(beaconApiBlobDownloaderInterval)
		}
	}
}

// downloadOnce performs a single download pass
func (b *BeaconApiBlobDownloader) downloadOnce(shouldLog bool) error {
	currentSlot := b.headSlot.Load()
	if currentSlot == 0 {
		return nil // not initialized yet
	}

	tx, err := b.indiciesDB.BeginRo(b.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	logInterval := time.NewTicker(beaconApiBlobLogInterval)
	defer logInterval.Stop()

	prevLogSlot := currentSlot
	prevTime := time.Now()

	targetSlot := b.targetSlot
	// in case of non-archive mode, we only backfill the last relevant epochs
	if !b.archiveBlobs {
		targetSlot = currentSlot - min(currentSlot, b.beaconCfg.MinSlotsForBlobsSidecarsRequest())
	}

	if shouldLog {
		b.logger.Info("[BeaconApiBlobDownloader] Downloading data columns backwards via beacon API", "slot", currentSlot, "target", targetSlot)
	}

	processed := 0
	for currentSlot >= targetSlot {
		if currentSlot <= b.sn.FrozenBlobs() {
			break
		}
		if !b.syncedChecker.Synced() {
			time.Sleep(5 * time.Second)
			continue
		}

		// Read the block to get the expected blob count
		block, err := b.blockReader.ReadBlindedBlockBySlot(b.ctx, tx, currentSlot)
		if err != nil {
			return err
		}
		if block == nil {
			currentSlot--
			continue
		}
		if block.Version() < clparams.DenebVersion {
			break
		}

		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			return err
		}

		// Check if we already have blobs for this block
		blobsCount, err := b.blobStorage.KzgCommitmentsCount(b.ctx, blockRoot)
		if err != nil {
			return err
		}

		expectedCount := block.Block.Body.BlobKzgCommitments.Len()
		if expectedCount == 0 || int(blobsCount) == expectedCount {
			// No blobs needed or already have them
			currentSlot--
			continue
		}

		// Check if we have enough columns to recover
		if b.peerDas != nil && b.peerDas.IsColumnOverHalf(currentSlot, blockRoot) {
			// We have enough columns, try to schedule recovery
			if err := b.peerDas.TryScheduleRecover(currentSlot, blockRoot); err != nil {
				b.logger.Debug("[BeaconApiBlobDownloader] Failed to schedule blob recovery", "slot", currentSlot, "err", err)
			}
			currentSlot--
			continue
		}

		// Fetch data column sidecars from beacon API
		columns, err := b.fetchDataColumnSidecars(currentSlot)
		if err != nil {
			b.logger.Debug("[BeaconApiBlobDownloader] Failed to fetch data columns from API", "slot", currentSlot, "err", err)
			currentSlot--
			continue
		}

		if len(columns) > 0 {
			// Store the columns
			storedCount := 0
			for _, column := range columns {
				if err := b.columnStorage.WriteColumnSidecars(b.ctx, blockRoot, int64(column.Index), column); err != nil {
					b.logger.Debug("[BeaconApiBlobDownloader] Failed to store column sidecar", "slot", currentSlot, "index", column.Index, "err", err)
					continue
				}
				storedCount++
			}

			if storedCount > 0 {
				processed++
				// Try to schedule blob recovery after storing columns
				if b.peerDas != nil {
					if err := b.peerDas.TryScheduleRecover(currentSlot, blockRoot); err != nil {
						b.logger.Debug("[BeaconApiBlobDownloader] Failed to schedule blob recovery", "slot", currentSlot, "err", err)
					}
				}
			}
		}

		select {
		case <-b.ctx.Done():
			return b.ctx.Err()
		case <-logInterval.C:
			if shouldLog {
				blkSec := float64(prevLogSlot-currentSlot) / time.Since(prevTime).Seconds()
				blkSecStr := fmt.Sprintf("%.1f", blkSec)
				prevLogSlot = currentSlot
				prevTime = time.Now()

				b.logger.Info("[BeaconApiBlobDownloader] Downloading data columns backwards via beacon API", "slot", currentSlot, "blks/sec", blkSecStr, "processed", processed)
			}
		default:
		}

		// Update highest backfilled slot
		b.highestBackfilledSlot.Store(currentSlot)
		currentSlot--
	}

	if shouldLog {
		b.logger.Info("[BeaconApiBlobDownloader] Data column history download finished successfully", "processed", processed)
	}

	b.backfillCompleted.Store(true)
	if b.notifyBlobBackfilled != nil {
		b.notifyBlobBackfilled()
	}
	return nil
}

// fetchDataColumnSidecars fetches data column sidecars from the beacon API for a given slot using SSZ encoding
func (b *BeaconApiBlobDownloader) fetchDataColumnSidecars(slot uint64) ([]*cltypes.DataColumnSidecar, error) {
	url := fmt.Sprintf("%s/eth/v1/beacon/data_column_sidecars/%d", b.apiUrl, slot)

	ctx, cancel := context.WithTimeout(b.ctx, beaconApiBlobTimeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	// Request SSZ encoding for lower bandwidth and faster decoding
	req.Header.Set("Accept", "application/octet-stream")

	resp, err := b.httpClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		// No columns for this slot
		return nil, nil
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("beacon API returned status %d: %s", resp.StatusCode, string(body))
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if len(body) == 0 {
		return nil, nil
	}

	// Decode SSZ list of data column sidecars
	columns, err := b.decodeDataColumnSidecarsSSZ(body)
	if err != nil {
		return nil, fmt.Errorf("failed to decode SSZ data column sidecars: %w", err)
	}

	return columns, nil
}

// decodeDataColumnSidecarsSSZ decodes a SSZ-encoded list of data column sidecars
func (b *BeaconApiBlobDownloader) decodeDataColumnSidecarsSSZ(data []byte) ([]*cltypes.DataColumnSidecar, error) {
	if len(data) == 0 {
		return nil, nil
	}

	// Use the solid library to decode the dynamic list
	list := solid.NewDynamicListSSZ[*cltypes.DataColumnSidecar](int(b.beaconCfg.NumberOfColumns))
	if err := list.DecodeSSZ(data, int(clparams.DenebVersion)); err != nil {
		return nil, err
	}

	// Convert to slice
	columns := make([]*cltypes.DataColumnSidecar, 0, list.Len())
	for i := 0; i < list.Len(); i++ {
		columns = append(columns, list.Get(i))
	}

	return columns, nil
}

// blockRootFromColumn extracts the block root from a data column sidecar
func blockRootFromColumn(column *cltypes.DataColumnSidecar) (common.Hash, error) {
	if column.SignedBlockHeader == nil || column.SignedBlockHeader.Header == nil {
		return common.Hash{}, fmt.Errorf("column has no signed block header")
	}
	return column.SignedBlockHeader.Header.HashSSZ()
}
