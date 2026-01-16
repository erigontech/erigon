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

package stages

import (
	"context"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/execution_client/block_collector"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type StageHistoryReconstructionCfg struct {
	beaconCfg                *clparams.BeaconChainConfig
	downloader               *network.BackwardBeaconDownloader
	sn                       *freezeblocks.CaplinSnapshots
	startingRoot             common.Hash
	caplinConfig             clparams.CaplinConfig
	waitForAllRoutines       bool
	startingSlot             uint64
	tmpdir                   string
	indiciesDB               kv.RwDB
	engine                   execution_client.ExecutionEngine
	antiquary                *antiquary.Antiquary
	logger                   log.Logger
	executionBlocksCollector block_collector.BlockCollector
	backfillingThrottling    time.Duration
	blockReader              freezeblocks.BeaconSnapshotReader
	blobStorage              blob_storage.BlobStorage
	forkchoiceStore          forkchoice.ForkChoiceStorage
	blobDownloader           *network.BlobHistoryDownloader
}

const logIntervalTime = 30 * time.Second

func StageHistoryReconstruction(downloader *network.BackwardBeaconDownloader, antiquary *antiquary.Antiquary, sn *freezeblocks.CaplinSnapshots, indiciesDB kv.RwDB, engine execution_client.ExecutionEngine, beaconCfg *clparams.BeaconChainConfig, caplinConfig clparams.CaplinConfig, waitForAllRoutines bool, startingRoot common.Hash, startinSlot uint64, tmpdir string, backfillingThrottling time.Duration, executionBlocksCollector block_collector.BlockCollector, blockReader freezeblocks.BeaconSnapshotReader, blobStorage blob_storage.BlobStorage, logger log.Logger, forkchoiceStore forkchoice.ForkChoiceStorage, blobDownloader *network.BlobHistoryDownloader) StageHistoryReconstructionCfg {
	return StageHistoryReconstructionCfg{
		beaconCfg:                beaconCfg,
		downloader:               downloader,
		startingRoot:             startingRoot,
		tmpdir:                   tmpdir,
		startingSlot:             startinSlot,
		waitForAllRoutines:       waitForAllRoutines,
		logger:                   logger,
		caplinConfig:             caplinConfig,
		indiciesDB:               indiciesDB,
		antiquary:                antiquary,
		engine:                   engine,
		sn:                       sn,
		backfillingThrottling:    backfillingThrottling,
		executionBlocksCollector: executionBlocksCollector,
		blockReader:              blockReader,
		blobStorage:              blobStorage,
		forkchoiceStore:          forkchoiceStore,
		blobDownloader:           blobDownloader,
	}
}

// SpawnStageBeaconsForward spawn the beacon forward stage
func SpawnStageHistoryDownload(cfg StageHistoryReconstructionCfg, ctx context.Context, logger log.Logger) error {
	// Wait for execution engine to be ready.
	blockRoot := cfg.startingRoot
	currentSlot := cfg.startingSlot

	if !clparams.SupportBackfilling(cfg.beaconCfg.DepositNetworkID) {
		cfg.caplinConfig.ArchiveBlocks = false // disable backfilling if not on a supported network
	}

	var hasFinishedDownloadingElBlocks atomic.Bool

	// Start the procedure
	logger.Info("Starting downloading History", "from", currentSlot)
	// Setup slot and block root
	cfg.downloader.SetSlotToDownload(currentSlot)
	cfg.downloader.SetExpectedRoot(blockRoot)
	cfg.downloader.SetBlockChecker(cfg.executionBlocksCollector)

	var initialBeaconBlock *cltypes.SignedBeaconBlock

	var currEth1Progress atomic.Int64

	destinationSlotForEL := uint64(math.MaxUint64)
	if cfg.engine != nil && cfg.engine.SupportInsertion() && cfg.beaconCfg.DenebForkEpoch != math.MaxUint64 {
		destinationSlotForEL = cfg.beaconCfg.BellatrixForkEpoch * cfg.beaconCfg.SlotsPerEpoch
	}
	// Set up onNewBlock callback
	cfg.downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlock) (finished bool, err error) {
		tx, err := cfg.indiciesDB.BeginRw(ctx)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
		// handle the case where the block is a CL block including an execution payload
		if blk.Version() >= clparams.BellatrixVersion {
			currEth1Progress.Store(int64(blk.Block.Body.ExecutionPayload.BlockNumber))
		}

		if initialBeaconBlock == nil {
			initialBeaconBlock = blk
		}

		slot := blk.Block.Slot
		isInCLSnapshots := cfg.sn.SegmentsMax() > blk.Block.Slot
		// Skip blocks that are already in the snapshots
		if !isInCLSnapshots {
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, blk, true); err != nil {
				return false, err
			}
		}
		// we need to backfill an equivalent number of blobs to the blocks
		hasDownloadEnoughForImmediateBlobsBackfilling := true
		if cfg.caplinConfig.ImmediateBlobsBackfilling {
			// download twice the number of blocks needed for good measure
			blocksToDownload := cfg.beaconCfg.MinSlotsForBlobsSidecarsRequest() * 2
			hasDownloadEnoughForImmediateBlobsBackfilling = cfg.startingSlot < blocksToDownload || slot > cfg.startingSlot-blocksToDownload
		}

		if cfg.engine != nil && cfg.engine.SupportInsertion() && blk.Version() >= clparams.BellatrixVersion {
			frozenBlocksInEL := cfg.engine.FrozenBlocks(ctx)

			payload := blk.Block.Body.ExecutionPayload
			hasELBlock := frozenBlocksInEL > blk.Block.Body.ExecutionPayload.BlockNumber
			if !hasELBlock {
				hasELBlock, err = cfg.engine.HasBlock(ctx, payload.BlockHash)
				if err != nil {
					return false, fmt.Errorf("error retrieving whether execution payload is present: %s", err)
				}
			}

			if !hasELBlock {
				if err := cfg.executionBlocksCollector.AddBlock(blk.Block); err != nil {
					return false, fmt.Errorf("error adding block to execution blocks collector: %s", err)
				}
				if currEth1Progress.Load()%100 == 0 {
					return false, tx.Commit()
				}
			}
			if hasELBlock && !cfg.caplinConfig.ArchiveBlocks {
				return hasDownloadEnoughForImmediateBlobsBackfilling, tx.Commit()
			}
			hasFinishedDownloadingElBlocks.Store(hasELBlock)
		} else {
			hasFinishedDownloadingElBlocks.Store(true)
		}
		isInElSnapshots := true
		if blk.Version() >= clparams.BellatrixVersion && cfg.engine != nil && cfg.engine.SupportInsertion() {
			frozenBlocksInEL := cfg.engine.FrozenBlocks(ctx)
			isInElSnapshots = frozenBlocksInEL > blk.Block.Body.ExecutionPayload.BlockNumber
			if cfg.engine.HasGapInSnapshots(ctx) && frozenBlocksInEL > 0 {
				destinationSlotForEL = frozenBlocksInEL - 1
			}
		}

		if slot == 0 || (isInCLSnapshots && isInElSnapshots) {
			return true, tx.Commit()
		}
		return hasDownloadEnoughForImmediateBlobsBackfilling &&
				(!cfg.caplinConfig.ArchiveBlocks || slot <= cfg.sn.SegmentsMax()) &&
				(slot <= destinationSlotForEL || isInElSnapshots),
			tx.Commit()
	})

	finishCh := make(chan struct{})
	// Start logging thread

	isBackfilling := atomic.Bool{}

	go func() {
		startTimeLoop := time.Now()
		initialProgress := cfg.downloader.Progress()
		logInterval := time.NewTicker(logIntervalTime)
		defer logInterval.Stop()
		for {
			select {
			case <-logInterval.C:
				if cfg.engine != nil && cfg.engine.SupportInsertion() {
					if ready, err := cfg.engine.Ready(ctx); !ready {
						if err != nil {
							log.Warn("could not log progress", "err", err)
						}
						continue
					}

				}
				logArgs := []any{}
				currProgress := cfg.downloader.Progress()
				speed := math.Abs(float64(currProgress)-float64(initialProgress)) / time.Since(startTimeLoop).Seconds()
				if speed > 1000.0 { // to avoid spamming logs on fast syncs
					initialProgress = currProgress
					startTimeLoop = time.Now()
					continue
				}

				if speed == 0 || initialBeaconBlock == nil {
					continue
				}

				if cfg.sn != nil && cfg.sn.SegmentsMax() == 0 {
					cfg.sn.OpenFolder()
				}

				highestBlockSeen := initialBeaconBlock.Block.Slot
				lowestBlockToReach := cfg.sn.SegmentsMax()

				logArgs = append(logArgs,
					"slot", currProgress,
					"blockNumber", currEth1Progress.Load(),
					"blk/sec", fmt.Sprintf("%.1f", speed),
					"snapshots", cfg.sn.SegmentsMax(),
				)

				isDownloadingForBeacon := (hasFinishedDownloadingElBlocks.Load() || cfg.caplinConfig.ArchiveBlocks) && clparams.SupportBackfilling(cfg.beaconCfg.DepositNetworkID)

				if cfg.engine != nil && cfg.engine.SupportInsertion() {
					logArgs = append(logArgs, "frozenBlocks", cfg.engine.FrozenBlocks(ctx))
					if !isDownloadingForBeacon {
						// If we are not backfilling, we are in the EL phase
						highestBlockSeen = initialBeaconBlock.Block.Body.ExecutionPayload.BlockNumber

						h, err := cfg.engine.CurrentHeader(ctx)
						if err != nil || h == nil {
							log.Debug("could not log progress", "err", err)
							lowestBlockToReach = cfg.engine.FrozenBlocks(ctx)
						} else {
							lowestBlockToReach = h.Number.Uint64()
						}
					}
				}

				logMsg := "Node is still syncing... downloading past blocks"
				if isBackfilling.Load() {
					logMsg = "Node has finished syncing... full history is being downloaded for archiving purposes"
				}
				// Log the progress for debugging
				logger.Debug(logMsg, logArgs...)

				if !isDownloadingForBeacon {
					toprocess := highestBlockSeen - lowestBlockToReach
					processed := highestBlockSeen - uint64(currEth1Progress.Load())
					remaining := float64(toprocess - processed)
					log.Info("Downloading Execution History", "progress",
						fmt.Sprintf("%d/%d", processed, toprocess),
						"ETA", (time.Duration(remaining/speed) * time.Second).String(),
						"blk/sec", fmt.Sprintf("%.1f", speed))
				} else {
					log.Info("Downloading Beacon History", "progress",
						fmt.Sprintf("%d/%d", highestBlockSeen-currProgress, highestBlockSeen-lowestBlockToReach),
						"blk/sec", fmt.Sprintf("%.1f", speed))
				}
				// More UX-friendly logging
			case <-finishCh:
				return
			case <-ctx.Done():
				return
			}
		}
	}()

	go func() {
		for !cfg.downloader.Finished() {
			if err := cfg.downloader.RequestMore(ctx); err != nil {
				log.Debug("closing backfilling routine", "err", err)
				return
			}
		}
		cfg.antiquary.NotifyBackfilled()
		if cfg.caplinConfig.ArchiveBlocks {
			cfg.logger.Info("Full backfilling finished")
		}

		close(finishCh)
		if cfg.blobDownloader != nil {
			cfg.blobDownloader.SetHeadSlot(cfg.startingSlot + 1)
			cfg.blobDownloader.SetNotifyBlobBackfilled(cfg.antiquary.NotifyBlobBackfilled)
			cfg.blobDownloader.Start()
		}
	}()
	// We block until we are done with the EL side of the backfilling with 2000 blocks of safety margin.
	for !cfg.downloader.Finished() && (cfg.engine == nil || cfg.downloader.Progress() > destinationSlotForEL) {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}
	cfg.downloader.SetThrottle(cfg.backfillingThrottling) // throttle to 0.6 second for backfilling
	cfg.downloader.SetNeverSkip(false)
	isBackfilling.Store(true)

	cfg.logger.Info("Ready to insert history, waiting for sync cycle to finish")

	return nil
}
