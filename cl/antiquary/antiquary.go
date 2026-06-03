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

package antiquary

import (
	"context"
	"math"

	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cl/beacon/synced_data"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	state_accessors "github.com/erigontech/erigon/cl/persistence/state"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

const (
	safetyMargin             = 20_000 // We retire snapshots 10k blocks after the finalized head
	antiquaryIndexBatchSlots = snaptype.CaplinMergeLimit / 2
)

var (
	mxAntiquaryIndexBatchSeconds = metrics.GetOrCreateSummary(`caplin_antiquary_batch_seconds{phase="index"}`)
	mxAntiquaryPruneBatchSeconds = metrics.GetOrCreateSummary(`caplin_antiquary_batch_seconds{phase="prune"}`)
	mxAntiquaryIndexBatchSlots   = metrics.GetOrCreateGauge(`caplin_antiquary_batch_items{phase="index"}`)
	mxAntiquaryPruneBatchBlocks  = metrics.GetOrCreateGauge(`caplin_antiquary_batch_items{phase="prune"}`)
	mxAntiquaryPrunedBlocks      = metrics.GetOrCreateCounter("caplin_antiquary_pruned_blocks_total")
)

// Antiquary is where the snapshots go, aka old history, it is what keep track of the oldest records.
type Antiquary struct {
	mainDB                         kv.RwDB                  // this is the main DB
	blobStorage                    blob_storage.BlobStorage // this is the blob storage
	dirs                           datadir.Dirs
	downloader                     downloader.Client
	logger                         log.Logger
	sn                             *freezeblocks.CaplinSnapshots
	stateSn                        *snapshotsync.CaplinStateSnapshots
	snReader                       freezeblocks.BeaconSnapshotReader
	snBuildSema                    *semaphore.Weighted // semaphore for building only one type (blocks, caplin, v3) at a time
	ctx                            context.Context
	backfilled                     *atomic.Bool
	blobBackfilled                 *atomic.Bool
	cfg                            *clparams.BeaconChainConfig
	states, blocks, blobs, snapgen bool

	validatorsTable *state_accessors.StaticValidatorTable
	genesisState    *state.CachingBeaconState
	syncedData      synced_data.SyncedData
	// set to nil
	currentState *state.CachingBeaconState
	balances32   []byte
}

func NewAntiquary(ctx context.Context, blobStorage blob_storage.BlobStorage, genesisState *state.CachingBeaconState, validatorsTable *state_accessors.StaticValidatorTable, cfg *clparams.BeaconChainConfig, dirs datadir.Dirs, downloaderClient downloader.Client, mainDB kv.RwDB, stateSn *snapshotsync.CaplinStateSnapshots, sn *freezeblocks.CaplinSnapshots, reader freezeblocks.BeaconSnapshotReader, syncedData synced_data.SyncedData, logger log.Logger, states, blocks, blobs, snapgen bool, snBuildSema *semaphore.Weighted) *Antiquary {
	backfilled := &atomic.Bool{}
	blobBackfilled := &atomic.Bool{}
	backfilled.Store(false)
	blobBackfilled.Store(false)
	return &Antiquary{
		mainDB:          mainDB,
		blobStorage:     blobStorage,
		dirs:            dirs,
		downloader:      downloaderClient,
		logger:          logger,
		sn:              sn,
		ctx:             ctx,
		backfilled:      backfilled,
		blobBackfilled:  blobBackfilled,
		cfg:             cfg,
		states:          states,
		snReader:        reader,
		snBuildSema:     snBuildSema,
		validatorsTable: validatorsTable,
		genesisState:    genesisState,
		blocks:          blocks,
		blobs:           blobs,
		snapgen:         snapgen,
		stateSn:         stateSn,
		syncedData:      syncedData,
	}
}

// Antiquate is the function that starts transactions seeding and shit, very cool but very shit too as a name.
func (a *Antiquary) Loop() error {
	if !a.blocks {
		return nil // Just skip if we don't have a downloader
	}
	// Skip if we don't support backfilling for the current network
	if !clparams.SupportBackfilling(a.cfg.DepositNetworkID) {
		return nil
	}
	if a.downloader != nil {
		reCheckTicker := time.NewTicker(3 * time.Second)
		defer reCheckTicker.Stop()

		// We need to make sure we 100% finish the download process.
		// 1) Define some time completionEpoch window
		completionEpoch := 2 * time.Minute
		// 2) Define a progress counter
		progress := time.Now()

		// Fist part of the antiquate is to download caplin snapshots
		for !time.Now().Add(completionEpoch).Before(progress) && !a.backfilled.Load() {
			select {
			case <-reCheckTicker.C:
				// We were waiting here previously for torrents to be completed, but they should be already
				// completed when added.
				progress = time.Now() // reset the progress if we are not completed
			case <-a.ctx.Done():
			}
		}
	}

	if err := a.sn.BuildMissingIndices(a.ctx, a.logger); err != nil {
		return err
	}
	var from uint64
	if err := a.mainDB.View(a.ctx, func(tx kv.Tx) error {
		var err error
		from, err = beacon_indicies.ReadLastBeaconSnapshot(tx)
		return err
	}); err != nil {
		return err
	}

	logInterval := time.NewTicker(30 * time.Second)
	if err := a.sn.OpenFolder(); err != nil {
		return err
	}
	if a.stateSn != nil {
		if err := a.stateSn.OpenFolder(); err != nil {
			return err
		}
	}

	defer logInterval.Stop()
	available := a.sn.BlocksAvailable()
	if from > available {
		a.logger.Warn("[Antiquary] Snapshot progress is ahead of visible snapshots", "progress", from, "available", available)
		from = clampBeaconSnapshotProgress(from, available)
	}
	indexedTo := from
	for {
		frozenSlots := a.sn.BlocksAvailable()
		if indexedTo >= frozenSlots {
			break
		}
		a.logger.Info("[Antiquary] Stopping Caplin to process historical indicies", "from", indexedTo, "to", frozenSlots)
		err := indexBeaconSnapshots(a.ctx, a.mainDB, indexedTo, frozenSlots, antiquaryIndexBatchSlots, a.sn.ReadHeader, func(slot uint64) {
			select {
			case <-logInterval.C:
				a.logger.Info("[Antiquary] Processed snapshots", "progress", slot, "target", frozenSlots)
			case <-a.ctx.Done():
			default:
			}
		})
		if err != nil {
			return err
		}
		indexedTo = frozenSlots
	}

	if a.stateSn != nil {
		if err := a.stateSn.OpenFolder(); err != nil {
			return err
		}
	}
	stateBlocksAvailable := uint64(0)
	if a.stateSn != nil {
		stateBlocksAvailable = a.stateSn.BlocksAvailable()
	}
	log.Info("[Caplin] Stat", "blocks-static", a.sn.BlocksAvailable(), "states-static", stateBlocksAvailable, "blobs-static", a.sn.FrozenBlobs(),
		"state-history-enabled", a.states, "block-history-enabled", a.blocks, "blob-history-enabled", a.blobs, "snapgen", a.snapgen)

	if err := pruneBeaconBlocksAndWriteProgress(a.ctx, a.mainDB, indexedTo, indexedTo, snaptype.CaplinMergeLimit); err != nil {
		return err
	}

	a.logger.Info("[Antiquary] Restarting Caplin")

	if a.states {
		go a.loopStates(a.ctx)
	}
	// Check for snapshots retirement every 3 minutes
	retirementTicker := time.NewTicker(12 * time.Second)
	defer retirementTicker.Stop()
	for {
		select {
		case <-retirementTicker.C:
			if !a.backfilled.Load() {
				continue
			}

			if err := a.antiquate(); err != nil {
				log.Warn("[Antiquary] Failed to antiquate", "err", err)
			}
			if a.cfg.DenebForkEpoch == math.MaxUint64 {
				continue
			}
			if !a.blobBackfilled.Load() {
				continue
			}
			if err := a.antiquateBlobs(); err != nil {
				log.Error("[Antiquary] Failed to antiquate blobs", "err", err)
			}
		case <-a.ctx.Done():
		}
	}
}

type readBeaconSnapshotHeaderFunc func(slot uint64, tx kv.Tx) (*cltypes.SignedBeaconBlockHeader, uint64, common.Hash, error)

func clampBeaconSnapshotProgress(progress, available uint64) uint64 {
	if progress > available {
		return available
	}
	return progress
}

func indexBeaconSnapshots(ctx context.Context, db kv.RwDB, from, to, batchSize uint64, readHeader readBeaconSnapshotHeaderFunc, onProgress func(slot uint64)) error {
	if batchSize == 0 {
		batchSize = antiquaryIndexBatchSlots
	}
	for batchFrom := from; batchFrom < to; batchFrom = nextBatchEnd(batchFrom, to, batchSize) {
		batchTo := nextBatchEnd(batchFrom, to, batchSize)
		start := time.Now()
		err := db.Update(ctx, func(tx kv.RwTx) error {
			if err := indexBeaconSnapshotBatch(ctx, tx, batchFrom, batchTo, readHeader, onProgress); err != nil {
				return err
			}
			if err := beacon_indicies.WriteLastBeaconSnapshot(tx, batchTo); err != nil {
				return err
			}
			return nil
		})
		mxAntiquaryIndexBatchSeconds.ObserveDuration(start)
		if err != nil {
			return err
		}
		mxAntiquaryIndexBatchSlots.SetUint64(batchTo - batchFrom)
	}
	return nil
}

func nextBatchEnd(from, to, batchSize uint64) uint64 {
	batchTo := from + batchSize
	if batchTo < from || batchTo > to {
		return to
	}
	return batchTo
}

func indexBeaconSnapshotBatch(ctx context.Context, tx kv.RwTx, from, to uint64, readHeader readBeaconSnapshotHeaderFunc, onProgress func(slot uint64)) error {
	for slot := from; slot < to; slot++ {
		header, elBlockNumber, elBlockHash, err := readHeader(slot, tx)
		if err != nil {
			return err
		}
		if header == nil {
			continue
		}
		blockRoot, err := header.Header.HashSSZ()
		if err != nil {
			return err
		}
		if err := beacon_indicies.MarkRootCanonical(ctx, tx, header.Header.Slot, blockRoot); err != nil {
			return err
		}
		if err := beacon_indicies.WriteHeaderSlot(tx, blockRoot, header.Header.Slot); err != nil {
			return err
		}
		if err := beacon_indicies.WriteStateRoot(tx, blockRoot, header.Header.Root); err != nil {
			return err
		}
		if err := beacon_indicies.WriteParentBlockRoot(ctx, tx, blockRoot, header.Header.ParentRoot); err != nil {
			return err
		}
		if err := beacon_indicies.WriteExecutionBlockNumber(tx, blockRoot, elBlockNumber); err != nil {
			return err
		}
		if err := beacon_indicies.WriteExecutionBlockHash(tx, blockRoot, elBlockHash); err != nil {
			return err
		}
		if onProgress != nil {
			onProgress(slot)
		}
	}
	return nil
}

func pruneBeaconBlocksAndWriteProgress(ctx context.Context, db kv.RwDB, pruneTo, progress, batchLimit uint64) error {
	if batchLimit == 0 {
		batchLimit = snaptype.CaplinMergeLimit
	}
	for {
		hasMore := false
		pruned := 0
		start := time.Now()
		if err := db.Update(ctx, func(tx kv.RwTx) error {
			if pruneTo != 0 {
				deleted, more, err := beacon_indicies.PruneBlocksLimit(ctx, tx, pruneTo, int(batchLimit))
				if err != nil {
					return err
				}
				pruned = deleted
				hasMore = more
			}
			if hasMore {
				return nil
			}
			return beacon_indicies.WriteLastBeaconSnapshot(tx, progress)
		}); err != nil {
			mxAntiquaryPruneBatchSeconds.ObserveDuration(start)
			return err
		}
		mxAntiquaryPruneBatchSeconds.ObserveDuration(start)
		mxAntiquaryPruneBatchBlocks.SetInt(pruned)
		if pruned != 0 {
			mxAntiquaryPrunedBlocks.AddInt(pruned)
		}
		if !hasMore {
			return nil
		}
	}
}

// weight for the semaphore to build only one type of snapshots at a time
// for now all of them have the same weight
//const caplinSnapshotBuildSemaWeight int64 = 1

// Antiquate will antiquate a specific block range (aka. retire snapshots), this should be ran in the background.
func (a *Antiquary) antiquate() error {
	if !a.snapgen {
		return nil
	}

	var from, to uint64
	var err error

	if err := a.mainDB.View(a.ctx, func(roTx kv.Tx) error {
		// read the last beacon snapshots
		from = a.sn.BlocksAvailable() + 1
		// read the finalized head
		to, err = beacon_indicies.ReadHighestFinalized(roTx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	from = (from / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit
	to = min(to, to-safetyMargin) // We don't want to retire snapshots that are too close to the finalized head
	to = (to / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit

	if from >= to || to-from < snaptype.CaplinMergeLimit {
		return nil
	}
	// if a.snBuildSema != nil {
	// 	if !a.snBuildSema.TryAcquire(caplinSnapshotBuildSemaWeight) {
	// 		return nil
	// 	}
	// 	defer a.snBuildSema.TryAcquire(caplinSnapshotBuildSemaWeight)
	// }

	a.logger.Info("[Antiquary] Antiquating", "from", from, "to", to)
	if err := freezeblocks.DumpBeaconBlocks(a.ctx, a.mainDB, from, to, a.sn.Salt, a.dirs, 1, log.LvlDebug, a.logger); err != nil {
		return err
	}
	if err := a.sn.OpenFolder(); err != nil {
		return err
	}
	if err := pruneBeaconBlocksAndWriteProgress(a.ctx, a.mainDB, to, to-1, snaptype.CaplinMergeLimit); err != nil {
		return err
	}
	if err := a.sn.OpenFolder(); err != nil {
		return err
	}

	paths := a.sn.SegFileNames(from, to)
	if a.downloader != nil {
		// Notify bittorent to seed the new snapshots
		if err := a.downloader.Seed(a.ctx, paths); err != nil {
			a.logger.Warn("[Antiquary] Failed to add items to bittorent", "err", err)
		}
	}

	return nil
}

func (a *Antiquary) NotifyBackfilled() {
	// we set up the range for [lowestRawSlot, finalized]
	a.backfilled.Store(true) // this is the lowest slot not in snapshots
}

func (a *Antiquary) NotifyBlobBackfilled() {
	a.blobBackfilled.Store(true)
}

func (a *Antiquary) antiquateBlobs() error {
	if !a.snapgen {
		return nil
	}
	// if a.snBuildSema != nil {
	// 	if !a.snBuildSema.TryAcquire(caplinSnapshotBuildSemaWeight) {
	// 		return nil
	// 	}
	// 	defer a.snBuildSema.TryAcquire(caplinSnapshotBuildSemaWeight)
	// }
	roTx, err := a.mainDB.BeginRo(a.ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	// perform blob antiquation if it is time to.
	currentBlobsProgress := a.sn.FrozenBlobs()
	// We should NEVER get ahead of the block snapshots.
	if currentBlobsProgress >= a.sn.BlocksAvailable() {
		return nil
	}
	minimunBlobsProgress := ((a.cfg.DenebForkEpoch * a.cfg.SlotsPerEpoch) / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit
	currentBlobsProgress = max(currentBlobsProgress, minimunBlobsProgress)
	// read the finalized head
	to := a.sn.BlocksAvailable()
	if to <= currentBlobsProgress || to-currentBlobsProgress < snaptype.CaplinMergeLimit {
		return nil
	}
	roTx.Rollback()
	a.logger.Info("[Antiquary] Antiquating blobs", "from", currentBlobsProgress, "to", to)
	blobCountFn := func(slot uint64) (uint64, error) {
		block, err := a.snReader.ReadBeaconBlockBodyBySlot(a.ctx, nil, slot)
		if err != nil {
			return 0, err
		}
		if block == nil {
			return 0, nil
		}
		commitments := block.Block.Body.GetBlobKzgCommitments()
		if commitments == nil {
			return 0, nil
		}
		return uint64(commitments.Len()), nil
	}

	// now, we need to retire the blobs
	if err := freezeblocks.DumpBlobsSidecar(a.ctx, a.blobStorage, a.mainDB, currentBlobsProgress, to, a.sn.Salt, a.dirs, 1, blobCountFn, log.LvlDebug, a.logger); err != nil {
		return err
	}
	to = (to / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit
	a.logger.Info("[Antiquary] Finished Antiquating blobs", "from", currentBlobsProgress, "to", to)
	if err := a.sn.OpenFolder(); err != nil {
		return err
	}

	paths := a.sn.SegFileNames(currentBlobsProgress, to)
	if a.downloader != nil {
		// Notify bittorent to seed the new snapshots
		if err := a.downloader.Seed(a.ctx, paths); err != nil {
			a.logger.Warn("[Antiquary] Failed to add items to bittorent", "err", err)
		}
	}

	roTx, err = a.mainDB.BeginRo(a.ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	// now prune blobs from the database
	for i := currentBlobsProgress; i < to; i++ {
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(roTx, i)
		if err != nil {
			return err
		}
		a.blobStorage.RemoveBlobSidecars(a.ctx, i, blockRoot)
	}
	return nil
}
