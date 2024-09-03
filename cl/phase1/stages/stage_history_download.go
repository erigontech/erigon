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
	"errors"
	"fmt"
	"math"
	"sync/atomic"
	"time"

	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/memdb"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/execution_client/block_collector"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
)

type StageHistoryReconstructionCfg struct {
	beaconCfg                *clparams.BeaconChainConfig
	downloader               *network.BackwardBeaconDownloader
	sn                       *freezeblocks.CaplinSnapshots
	startingRoot             libcommon.Hash
	backfilling              bool
	blobsBackfilling         bool
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
}

const logIntervalTime = 30 * time.Second

func StageHistoryReconstruction(downloader *network.BackwardBeaconDownloader, antiquary *antiquary.Antiquary, sn *freezeblocks.CaplinSnapshots, indiciesDB kv.RwDB, engine execution_client.ExecutionEngine, beaconCfg *clparams.BeaconChainConfig, backfilling, blobsBackfilling, waitForAllRoutines bool, startingRoot libcommon.Hash, startinSlot uint64, tmpdir string, backfillingThrottling time.Duration, executionBlocksCollector block_collector.BlockCollector, blockReader freezeblocks.BeaconSnapshotReader, blobStorage blob_storage.BlobStorage, logger log.Logger) StageHistoryReconstructionCfg {
	return StageHistoryReconstructionCfg{
		beaconCfg:                beaconCfg,
		downloader:               downloader,
		startingRoot:             startingRoot,
		tmpdir:                   tmpdir,
		startingSlot:             startinSlot,
		waitForAllRoutines:       waitForAllRoutines,
		logger:                   logger,
		backfilling:              backfilling,
		indiciesDB:               indiciesDB,
		antiquary:                antiquary,
		engine:                   engine,
		sn:                       sn,
		backfillingThrottling:    backfillingThrottling,
		executionBlocksCollector: executionBlocksCollector,
		blockReader:              blockReader,
		blobsBackfilling:         blobsBackfilling,
		blobStorage:              blobStorage,
	}
}

// SpawnStageBeaconsForward spawn the beacon forward stage
func SpawnStageHistoryDownload(cfg StageHistoryReconstructionCfg, ctx context.Context, logger log.Logger) error {
	// Wait for execution engine to be ready.
	blockRoot := cfg.startingRoot
	currentSlot := cfg.startingSlot

	if !clparams.SupportBackfilling(cfg.beaconCfg.DepositNetworkID) {
		cfg.backfilling = false // disable backfilling if not on a supported network
	}

	// Start the procedure
	logger.Info("Starting downloading History", "from", currentSlot)
	// Setup slot and block root
	cfg.downloader.SetSlotToDownload(currentSlot)
	cfg.downloader.SetExpectedRoot(blockRoot)

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
		if blk.Version() >= clparams.BellatrixVersion {
			currEth1Progress.Store(int64(blk.Block.Body.ExecutionPayload.BlockNumber))
		}

		slot := blk.Block.Slot
		isInCLSnapshots := cfg.sn.SegmentsMax() > blk.Block.Slot
		if !isInCLSnapshots {
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, blk, true); err != nil {
				return false, err
			}
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
			if hasELBlock && !cfg.backfilling {
				return true, tx.Commit()
			}
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
		return (!cfg.backfilling || slot <= cfg.sn.SegmentsMax()) && (slot <= destinationSlotForEL || isInElSnapshots), tx.Commit()
	})
	prevProgress := cfg.downloader.Progress()

	finishCh := make(chan struct{})
	// Start logging thread

	isBackfilling := atomic.Bool{}

	go func() {
		logInterval := time.NewTicker(logIntervalTime)
		defer logInterval.Stop()
		for {
			select {
			case <-logInterval.C:
				logTime := logIntervalTime

				if cfg.engine != nil && cfg.engine.SupportInsertion() {
					if ready, err := cfg.engine.Ready(ctx); !ready {
						if err != nil {
							log.Warn("could not log progress", "err", err)
						}
						continue
					}

				}
				logArgs := []interface{}{}
				currProgress := cfg.downloader.Progress()
				blockProgress := float64(prevProgress - currProgress)
				ratio := float64(logTime / time.Second)
				speed := blockProgress / ratio
				prevProgress = currProgress

				if speed == 0 {
					continue
				}
				logArgs = append(logArgs,
					"slot", currProgress,
					"blockNumber", currEth1Progress.Load(),
					"frozenBlocks", cfg.engine.FrozenBlocks(ctx),
					"blk/sec", fmt.Sprintf("%.1f", speed),
					"snapshots", cfg.sn.SegmentsMax(),
				)
				logMsg := "Node is still syncing... downloading past blocks"
				if isBackfilling.Load() {
					logMsg = "Node has finished syncing... full history is being downloaded for archiving purposes"
				}
				logger.Info(logMsg, logArgs...)
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
		if cfg.backfilling {
			cfg.logger.Info("Full backfilling finished")
		} else {
			cfg.logger.Info("Missing blocks download finished (note: this does not mean that the history is complete, only that the missing blocks need for sync have been downloaded)")
		}

		close(finishCh)
		if cfg.blobsBackfilling {
			go func() {
				if err := downloadBlobHistoryWorker(cfg, ctx, true, logger); err != nil {
					logger.Error("Error downloading blobs", "err", err)
				}
				// set a timer every 15 minutes as a failsafe
				ticker := time.NewTicker(15 * time.Minute)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return
					case <-ticker.C:
						if err := downloadBlobHistoryWorker(cfg, ctx, false, logger); err != nil {
							logger.Error("Error downloading blobs", "err", err)
						}
					}
				}
			}()
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
	// If i do not give it a database, erigon lib starts to cry uncontrollably
	db2 := memdb.New(cfg.tmpdir)
	defer db2.Close()
	tx2, err := db2.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx2.Rollback()
	isBackfilling.Store(true)

	cfg.logger.Info("Ready to insert history, waiting for sync cycle to finish")

	return nil
}

// downloadBlobHistoryWorker is a worker that downloads the blob history by using the already downloaded beacon blocks
func downloadBlobHistoryWorker(cfg StageHistoryReconstructionCfg, ctx context.Context, shouldLog bool, logger log.Logger) error {
	currentSlot := cfg.startingSlot + 1
	blocksBatchSize := uint64(8) // requests 8 blocks worth of blobs at a time
	tx, err := cfg.indiciesDB.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	logInterval := time.NewTicker(logIntervalTime)

	rpc := cfg.downloader.RPC()
	prevLogSlot := currentSlot
	prevTime := time.Now()
	targetSlot := cfg.beaconCfg.DenebForkEpoch * cfg.beaconCfg.SlotsPerEpoch

	for currentSlot >= targetSlot {
		if currentSlot <= cfg.sn.FrozenBlobs() {
			break
		}

		batch := make([]*cltypes.SignedBlindedBeaconBlock, 0, blocksBatchSize)
		visited := uint64(0)
		maxIterations := uint64(32)
		for ; visited < blocksBatchSize; visited++ {
			if visited >= maxIterations {
				break
			}
			if currentSlot-visited < targetSlot {
				break
			}
			block, err := cfg.blockReader.ReadBlindedBlockBySlot(ctx, tx, currentSlot-visited)
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
			blobsCount, err := cfg.blobStorage.KzgCommitmentsCount(ctx, blockRoot)
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
		case <-ctx.Done():
			return ctx.Err()
		case <-logInterval.C:
			if !shouldLog {
				continue
			}
			blkSec := float64(prevLogSlot-currentSlot) / time.Since(prevTime).Seconds()
			blkSecStr := fmt.Sprintf("%.1f", blkSec)
			// round to 1 decimal place  and convert to string
			prevLogSlot = currentSlot
			prevTime = time.Now()

			logger.Info("Downloading blobs backwards", "slot", currentSlot, "blks/sec", blkSecStr)
		default:
		}
		// Generate the request
		req, err := network.BlobsIdentifiersFromBlindedBlocks(batch)
		if err != nil {
			cfg.logger.Debug("Error generating blob identifiers", "err", err)
			continue
		}
		// Request the blobs
		blobs, err := network.RequestBlobsFrantically(ctx, rpc, req)
		if err != nil {
			cfg.logger.Debug("Error requesting blobs", "err", err)
			continue
		}
		_, _, err = blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, cfg.blobStorage, req, blobs.Responses, func(header *cltypes.SignedBeaconBlockHeader) error {
			// The block is preverified so just check that the signature is correct against the block
			for _, block := range batch {
				if block.Block.Slot != header.Header.Slot {
					continue
				}
				if block.Signature != header.Signature {
					return errors.New("signature mismatch beetwen blob and stored block")
				}
				return nil
			}
			return errors.New("block not in batch")
		})
		if err != nil {
			rpc.BanPeer(blobs.Peer)
			cfg.logger.Warn("Error verifying blobs", "err", err)
			continue
		}
	}
	if shouldLog {
		logger.Info("Blob history download finished successfully")
	}
	cfg.antiquary.NotifyBlobBackfilled()
	return nil
}
