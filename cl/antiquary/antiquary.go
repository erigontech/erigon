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
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/dbservices"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
	"github.com/erigontech/erigon/db/snaptype"
	"github.com/erigontech/erigon/diagnostics/metrics"
)

const (
	safetyMargin             = 20_000 // We retire snapshots 20k blocks after the finalized head
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
	downloader                     dbservices.DownloaderClient
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
	// maxSlotsPerCommit bounds how many slots the antiquary accumulates into one
	// mdbx transaction. A very large commit overflows libmdbx's gc_fill_returned
	// while serializing the transaction's retired-page list; bounding it avoids that.
	maxSlotsPerCommit uint64

	statePruneStartIdx   int
	statePruneDisabled   bool
	statePruneTimeout    time.Duration
	statePruneBoundaryFn func(table string) uint64
}

func NewAntiquary(ctx context.Context, blobStorage blob_storage.BlobStorage, genesisState *state.CachingBeaconState, validatorsTable *state_accessors.StaticValidatorTable, cfg *clparams.BeaconChainConfig, dirs datadir.Dirs, downloaderClient dbservices.DownloaderClient, mainDB kv.RwDB, stateSn *snapshotsync.CaplinStateSnapshots, sn *freezeblocks.CaplinSnapshots, reader freezeblocks.BeaconSnapshotReader, syncedData synced_data.SyncedData, logger log.Logger, states, blocks, blobs, snapgen bool, snBuildSema *semaphore.Weighted) *Antiquary {
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

		maxSlotsPerCommit:  stateAntiquaryMaxSlotsPerCommit,
		statePruneDisabled: dbg.EnvBool("CAPLIN_STATE_PRUNE_DISABLE", false),
		statePruneTimeout:  dbg.EnvDuration("CAPLIN_STATE_PRUNE_TIMEOUT", 0),
	}
}

// Antiquate is the function that starts transactions seeding and shit, very cool but very shit too as a name.
func (s *Antiquary) Loop() error {
	if !s.blocks {
		return nil // Just skip if we don't have a downloader
	}
	// Skip if we don't support backfilling for the current network
	if !clparams.SupportBackfilling(s.cfg.DepositNetworkID) {
		return nil
	}
	if s.downloader != nil {
		reCheckTicker := time.NewTicker(3 * time.Second)
		defer reCheckTicker.Stop()

		// We need to make sure we 100% finish the download process.
		// 1) Define some time completionEpoch window
		completionEpoch := 2 * time.Minute
		// 2) Define a progress counter
		progress := time.Now()

		// Fist part of the antiquate is to download caplin snapshots
		for !time.Now().Add(completionEpoch).Before(progress) && !s.backfilled.Load() {
			select {
			case <-reCheckTicker.C:
				// We were waiting here previously for torrents to be completed, but they should be already
				// completed when added.
				progress = time.Now() // reset the progress if we are not completed
			case <-s.ctx.Done():
				return nil
			}
		}
	}

	if err := s.sn.BuildMissingIndices(s.ctx, s.logger); err != nil {
		return err
	}
	logInterval := time.NewTicker(30 * time.Second)
	if err := s.sn.OpenFolder(); err != nil {
		return err
	}
	if s.stateSn != nil {
		if err := s.stateSn.OpenFolder(); err != nil {
			return err
		}
	}

	defer logInterval.Stop()
	indexedTo, err := rebuildBeaconSnapshotIndex(s.ctx, s.mainDB, s.sn.BlocksAvailable, s.sn.ReadHeader,
		antiquaryIndexBatchSlots, func(slot uint64) {
			select {
			case <-logInterval.C:
				s.logger.Info("[Antiquary] Processed snapshots", "progress", slot)
			case <-s.ctx.Done():
			default:
			}
		}, s.logger)
	if err != nil {
		return err
	}

	if s.stateSn != nil {
		if err := s.stateSn.OpenFolder(); err != nil {
			return err
		}
	}
	stateBlocksAvailable := uint64(0)
	if s.stateSn != nil {
		stateBlocksAvailable = s.stateSn.BlocksAvailable()
	}
	log.Info("[Caplin] Stat", "blocks-static", s.sn.BlocksAvailable(), "states-static", stateBlocksAvailable, "blobs-static", s.sn.FrozenBlobs(),
		"state-history-enabled", s.states, "block-history-enabled", s.blocks, "blob-history-enabled", s.blobs, "snapgen", s.snapgen)

	if err := pruneBeaconBlocksAndWriteProgress(s.ctx, s.mainDB, indexedTo, indexedTo, snaptype.CaplinMergeLimit); err != nil {
		return err
	}

	s.logger.Info("[Antiquary] Restarting Caplin")

	if s.states {
		go s.loopStates(s.ctx)
	}
	return s.retirementLoop()
}

func (s *Antiquary) retirementLoop() error {
	blocks := &retirementStep{
		run:     s.antiquate,
		onError: func(err error) { log.Warn("[Antiquary] Failed to antiquate", "err", err) },
	}
	blobs := &retirementStep{
		run:     s.antiquateBlobs,
		onError: func(err error) { log.Error("[Antiquary] Failed to antiquate blobs", "err", err) },
	}

	retirementTicker := time.NewTicker(12 * time.Second)
	defer retirementTicker.Stop()
	for {
		select {
		case <-retirementTicker.C:
			s.retirementTick(blocks, blobs)
		case <-s.ctx.Done():
			return nil
		}
	}
}

func (s *Antiquary) retirementTick(blocks, blobs *retirementStep) {
	if !s.backfilled.Load() {
		return
	}
	blocks.attempt(s.shuttingDown)

	if s.cfg.DenebForkEpoch == math.MaxUint64 {
		return
	}
	if !s.blobBackfilled.Load() {
		return
	}
	blobs.attempt(s.shuttingDown)
}

func (s *Antiquary) shuttingDown() bool { return s.ctx.Err() != nil }

type readBeaconSnapshotHeaderFunc func(slot uint64, tx kv.Tx) (*cltypes.SignedBeaconBlockHeader, uint64, common.Hash, error)

func clampBeaconSnapshotProgress(progress, available uint64) uint64 {
	if progress > available {
		return available
	}
	return progress
}

// rebuildBeaconSnapshotIndex indexes the visible snapshot range, resuming from the persisted
// cursor, which is the first slot not yet indexed.
func rebuildBeaconSnapshotIndex(ctx context.Context, db kv.RwDB, blocksAvailable func() uint64, readHeader readBeaconSnapshotHeaderFunc, batchSize uint64, onProgress func(slot uint64), logger log.Logger) (uint64, error) {
	var from uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		var err error
		from, err = beacon_indicies.ReadLastBeaconSnapshot(tx)
		return err
	}); err != nil {
		return 0, err
	}
	if available := blocksAvailable(); from > 0 && from-1 > available {
		logger.Warn("[Antiquary] Snapshot progress is ahead of visible snapshots", "progress", from, "available", available)
		from = clampBeaconSnapshotProgress(from, available)
	}
	indexedTo := uint64(0)
	if from > 0 {
		indexedTo = from - 1
	}
	for {
		tip := blocksAvailable()
		if tip == 0 || from > tip {
			return indexedTo, nil
		}
		logger.Info("[Antiquary] Stopping Caplin to process historical indicies", "from", from, "to", tip)
		// tip is the inclusive last readable slot and indexBeaconSnapshots takes an exclusive
		// bound, so the tip needs tip+1 to be indexed at all. It is served from the snapshot path,
		// which does not consult the canonical index, so leaving it out gives readers a block with
		// no root for as long as the tip stays put.
		if err := indexBeaconSnapshots(ctx, db, from, tip+1, batchSize, readHeader, onProgress); err != nil {
			return indexedTo, err
		}
		from, indexedTo = tip+1, tip
	}
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
// const caplinSnapshotBuildSemaWeight int64 = 1

// Antiquate will antiquate a specific block range (aka. retire snapshots), this should be ran in the background.
func (s *Antiquary) antiquate() error {
	if !s.snapgen {
		return nil
	}

	var from, to uint64

	if err := s.mainDB.View(s.ctx, func(roTx kv.Tx) error {
		// read the last beacon snapshots
		from = s.sn.BlocksAvailable() + 1
		// read the finalized head
		highest, err := beacon_indicies.ReadHighestFinalized(roTx)
		if err != nil {
			return err
		}
		to = highest
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
	// if s.snBuildSema != nil {
	// 	if !s.snBuildSema.TryAcquire(caplinSnapshotBuildSemaWeight) {
	// 		return nil
	// 	}
	// 	defer s.snBuildSema.TryAcquire(caplinSnapshotBuildSemaWeight)
	// }

	s.logger.Info("[Antiquary] Antiquating", "from", from, "to", to)
	if err := freezeblocks.DumpBeaconBlocks(s.ctx, s.mainDB, from, to, s.sn.Salt, s.dirs, 1, log.LvlDebug, s.logger); err != nil {
		return err
	}
	if err := s.sn.OpenFolder(); err != nil {
		return err
	}
	if err := pruneBeaconBlocksAndWriteProgress(s.ctx, s.mainDB, to, to-1, snaptype.CaplinMergeLimit); err != nil {
		return err
	}
	if err := s.sn.OpenFolder(); err != nil {
		return err
	}

	paths := s.sn.SegFileNames(from, to)
	if s.downloader != nil {
		// Notify bittorent to seed the new snapshots
		if err := s.downloader.Seed(s.ctx, paths); err != nil {
			s.logger.Warn("[Antiquary] Failed to add items to bittorent", "err", err)
		}
	}

	return nil
}

func (s *Antiquary) NotifyBackfilled() {
	// we set up the range for [lowestRawSlot, finalized]
	s.backfilled.Store(true) // this is the lowest slot not in snapshots
}

func (s *Antiquary) NotifyBlobBackfilled(completed bool) {
	s.blobBackfilled.Store(completed)
}

const caplinSnapshotBuildSemaWeight int64 = 1

// blobCompressWorkers picks the compression parallelism for a dump, with a func to release the
// shared build limiter.
//
// One worker keeps steady-state retirement from competing with execution. Catching up may use the
// full estimate, but only while holding the limiter that admits one kind of snapshot build at a
// time, since EL retirement sizes its own workers from the same estimate of the host. A limiter it
// cannot take drops the dump to one worker rather than skipping it: the blob gate stays shut until
// the whole range lands, so retiring slowly beats not retiring.
func (s *Antiquary) blobCompressWorkers(from, to uint64) (int, func()) {
	noop := func() {}
	if !isBlobBacklog(from, to) {
		return 1, noop
	}
	if s.snBuildSema == nil {
		return estimate.CompressSnapshot.Workers(), noop
	}
	if !s.snBuildSema.TryAcquire(caplinSnapshotBuildSemaWeight) {
		return 1, noop
	}
	return estimate.CompressSnapshot.Workers(), func() {
		s.snBuildSema.Release(caplinSnapshotBuildSemaWeight)
	}
}

// isBlobBacklog reports whether the pending range is a catch-up rather than the single chunk
// retired at the tip, which is what decides how many workers the compression may use.
//
// The span is the one DumpBlobsSidecar counts chunks with: it breaks once `toSlot-i` drops below a
// merge limit, so two chunks are compressed exactly when the span reaches two of them.
func isBlobBacklog(from, to uint64) bool {
	return to >= from && to-from >= 2*snaptype.CaplinMergeLimit
}

func (s *Antiquary) antiquateBlobs() error {
	if !s.snapgen {
		return nil
	}
	// if s.snBuildSema != nil {
	// 	if !s.snBuildSema.TryAcquire(caplinSnapshotBuildSemaWeight) {
	// 		return nil
	// 	}
	// 	defer s.snBuildSema.TryAcquire(caplinSnapshotBuildSemaWeight)
	// }
	roTx, err := s.mainDB.BeginRo(s.ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	// perform blob antiquation if it is time to.
	currentBlobsProgress := s.sn.FrozenBlobs()
	// We should NEVER get ahead of the block snapshots.
	if currentBlobsProgress >= s.sn.BlocksAvailable() {
		return nil
	}
	minimunBlobsProgress := ((s.cfg.DenebForkEpoch * s.cfg.SlotsPerEpoch) / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit
	currentBlobsProgress = max(currentBlobsProgress, minimunBlobsProgress)
	// read the finalized head
	to := s.sn.BlocksAvailable()
	if to <= currentBlobsProgress || to-currentBlobsProgress < snaptype.CaplinMergeLimit {
		return nil
	}
	roTx.Rollback()
	s.logger.Info("[Antiquary] Antiquating blobs", "from", currentBlobsProgress, "to", to)
	blobCountFn := func(slot uint64) (uint64, error) {
		block, err := s.snReader.ReadBeaconBlockBodyBySlot(s.ctx, nil, slot)
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
	// The build slot is held for the compression only: opening the folder, seeding and pruning
	// below draw nothing from the build budget, and EL retirement blocks on the same slot.
	if err := func() error {
		compressWorkers, releaseBuildSlot := s.blobCompressWorkers(currentBlobsProgress, to)
		defer releaseBuildSlot()
		return freezeblocks.DumpBlobsSidecar(s.ctx, s.blobStorage, s.mainDB, currentBlobsProgress, to, s.sn.Salt, s.dirs, compressWorkers, blobCountFn, log.LvlDebug, s.logger)
	}(); err != nil {
		return err
	}
	to = (to / snaptype.CaplinMergeLimit) * snaptype.CaplinMergeLimit
	s.logger.Info("[Antiquary] Finished Antiquating blobs", "from", currentBlobsProgress, "to", to)
	if err := s.sn.OpenFolder(); err != nil {
		return err
	}

	paths := s.sn.SegFileNames(currentBlobsProgress, to)
	if s.downloader != nil {
		// Notify bittorent to seed the new snapshots
		if err := s.downloader.Seed(s.ctx, paths); err != nil {
			s.logger.Warn("[Antiquary] Failed to add items to bittorent", "err", err)
		}
	}

	roTx, err = s.mainDB.BeginRo(s.ctx)
	if err != nil {
		return err
	}
	defer roTx.Rollback()
	// now prune blobs from the database
	var removeFailures uint64
	var firstFailedSlot uint64
	var firstRemoveErr error
	for i := currentBlobsProgress; i < to; i++ {
		blockRoot, err := beacon_indicies.ReadCanonicalBlockRoot(roTx, i)
		if err != nil {
			return err
		}
		if err := s.blobStorage.RemoveBlobSidecars(s.ctx, i, blockRoot); err != nil {
			if s.ctx.Err() != nil {
				return s.ctx.Err()
			}
			removeFailures++
			if firstRemoveErr == nil {
				firstFailedSlot, firstRemoveErr = i, err
			}
		}
	}
	if removeFailures > 0 {
		// The loop spans at least CaplinMergeLimit slots and a storage fault hits every
		// one of them, so the failures are aggregated rather than warned per slot.
		s.logger.Warn("[Antiquary] Failed to remove blob sidecars", "slots", removeFailures, "firstSlot", firstFailedSlot, "err", firstRemoveErr)
	}
	return nil
}
