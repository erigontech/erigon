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

	"github.com/erigontech/erigon/cl/antiquary"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/execution_client/block_collector"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	"github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

type StageHistoryReconstructionCfg struct {
	beaconCfg                *clparams.BeaconChainConfig
	downloader               historyDownloader
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

type historyDownloader interface {
	SetSlotToDownload(uint64)
	SetExpectedRoot(common.Hash)
	SetBlockChecker(network.BlockChecker)
	SetOnNewBlock(network.OnNewBlock)
	Finished() bool
	Progress() uint64
	RequestMore(context.Context) error
	SkippedFullBlocks() []network.SkippedFullBlock
	HasEnvelopeRecoverySource() bool
	RecoverSkippedEnvelopes(context.Context, []network.SkippedFullBlock, map[common.Hash]*cltypes.SignedBeaconBlock) network.EnvelopeRecoveryResult
	SetThrottle(time.Duration)
	SetNeverSkip(bool)
}

const logIntervalTime = 30 * time.Second

const (
	skippedEnvelopeRecoveryRetryInterval  = 10 * time.Second
	skippedEnvelopeRecoveryBatchSize      = 2
	skippedEnvelopeRecoveryBatchTimeout   = 5 * time.Second
	skippedEnvelopeRecoveryAttemptTimeout = 2 * time.Minute
)

var errSkippedEnvelopeRecoveryIncomplete = errors.New("skipped envelope recovery incomplete")

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

// elBackfillFinished reports whether the EL history backfill reached its floor:
// the beacon slot, or for a snapshot gap the EL block number (compared to elBlock).
func elBackfillFinished(slot, elBlock, destinationSlot, destinationBlock uint64) bool {
	if destinationSlot != math.MaxUint64 && slot <= destinationSlot {
		return true
	}
	if destinationBlock != math.MaxUint64 && elBlock != 0 && elBlock <= destinationBlock {
		return true
	}
	return false
}

// clampProgress derives (processed, total) for a backwards download, guarding the
// unsigned subtractions against underflow when the floor and current counters
// drift past the frozen highestBlockSeen. total grows to at least processed so a
// backfill continuing below the floor estimate keeps advancing while the display
// stays within 100%.
func clampProgress(highestBlockSeen, floor, current uint64) (processed, total uint64) {
	current = min(current, highestBlockSeen)
	floor = min(floor, highestBlockSeen)
	processed = highestBlockSeen - current
	total = max(highestBlockSeen-floor, processed)
	return
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
	// initialEth1Progress holds the EL block number of the first (highest) beacon block seen.
	// Used for logging; kept separate to avoid accessing ExecutionPayload on GLOAS blocks.
	var initialEth1Progress atomic.Int64

	destinationSlotForEL := uint64(math.MaxUint64)
	if cfg.engine != nil && cfg.engine.SupportInsertion() && cfg.beaconCfg.DenebForkEpoch != math.MaxUint64 {
		destinationSlotForEL = cfg.beaconCfg.BellatrixForkEpoch * cfg.beaconCfg.SlotsPerEpoch
	}
	// EL block-number floor for snapshot-gap backfill, kept separate from the
	// beacon-slot destinationSlotForEL since the units must not be mixed.
	destinationBlockForEL := uint64(math.MaxUint64)
	// Set up onNewBlock callback
	// [Modified in Gloas:EIP7732] envelope is non-nil for GLOAS FULL blocks, nil for EMPTY or pre-GLOAS.
	cfg.downloader.SetOnNewBlock(func(blk *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (finished bool, err error) {
		tx, err := cfg.indiciesDB.BeginRw(ctx)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()

		// Track EL block number for progress logging and batch-commit cadence.
		if blk.Version() >= clparams.GloasVersion {
			if envelope != nil {
				currEth1Progress.Store(int64(envelope.Message.Payload.BlockNumber))
			}
		} else if blk.Version() >= clparams.BellatrixVersion {
			currEth1Progress.Store(int64(blk.Block.Body.ExecutionPayload.BlockNumber))
		}

		if initialBeaconBlock == nil {
			initialBeaconBlock = blk
			// Record initial EL block number for the logging goroutine.
			if blk.Version() >= clparams.GloasVersion {
				if envelope != nil {
					initialEth1Progress.Store(int64(envelope.Message.Payload.BlockNumber))
				}
			} else if blk.Version() >= clparams.BellatrixVersion {
				initialEth1Progress.Store(int64(blk.Block.Body.ExecutionPayload.BlockNumber))
			}
		}

		slot := blk.Block.Slot
		isInCLSnapshots := cfg.sn.SegmentsMax() > blk.Block.Slot
		// Skip blocks that are already in the snapshots
		if !isInCLSnapshots {
			if err := beacon_indicies.WriteBeaconBlockAndIndicies(ctx, tx, blk, true); err != nil {
				return false, err
			}
			// [New in Gloas:EIP7732] WriteBeaconBlockAndIndicies skips EL indices for GLOAS blocks
			// because the payload is in the envelope, not the block body. Write them here now
			// that we have the envelope.
			if blk.Version() >= clparams.GloasVersion && envelope != nil {
				blockRoot, hashErr := blk.Block.HashSSZ()
				if hashErr != nil {
					return false, hashErr
				}
				if err := beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, common.Hash(blockRoot), envelope.Message); err != nil {
					return false, err
				}
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

			// [New in Gloas:EIP7732] EMPTY blocks carry no EL payload; skip EL insertion.
			isGloasEmpty := blk.Version() >= clparams.GloasVersion && envelope == nil
			if !isGloasEmpty {
				var payloadBlockHash common.Hash
				var payloadBlockNumber uint64
				if blk.Version() >= clparams.GloasVersion {
					payloadBlockHash = envelope.Message.Payload.BlockHash
					payloadBlockNumber = envelope.Message.Payload.BlockNumber
				} else {
					payloadBlockHash = blk.Block.Body.ExecutionPayload.BlockHash
					payloadBlockNumber = blk.Block.Body.ExecutionPayload.BlockNumber
				}

				hasELBlock := frozenBlocksInEL > payloadBlockNumber
				if !hasELBlock {
					hasELBlock, err = cfg.engine.HasBlock(ctx, payloadBlockHash)
					if err != nil {
						return false, fmt.Errorf("error retrieving whether execution payload is present: %s", err)
					}
				}

				if !hasELBlock {
					if blk.Version() >= clparams.GloasVersion {
						if err := cfg.executionBlocksCollector.AddGloasBlock(blk.Block, envelope); err != nil {
							return false, fmt.Errorf("error adding gloas block to execution blocks collector: %s", err)
						}
					} else {
						if err := cfg.executionBlocksCollector.AddBlock(blk.Block); err != nil {
							return false, fmt.Errorf("error adding block to execution blocks collector: %s", err)
						}
					}
					if currEth1Progress.Load()%100 == 0 {
						return false, tx.Commit()
					}
				}
				if hasELBlock && !cfg.caplinConfig.ArchiveBlocks {
					return hasDownloadEnoughForImmediateBlobsBackfilling, tx.Commit()
				}
				hasFinishedDownloadingElBlocks.Store(hasELBlock)
			}
			// For GLOAS EMPTY blocks, hasFinishedDownloadingElBlocks is left unchanged.
		} else {
			hasFinishedDownloadingElBlocks.Store(true)
		}

		isInElSnapshots := true
		if blk.Version() >= clparams.BellatrixVersion && cfg.engine != nil && cfg.engine.SupportInsertion() {
			frozenBlocksInEL := cfg.engine.FrozenBlocks(ctx)
			if blk.Version() >= clparams.GloasVersion {
				if envelope != nil {
					isInElSnapshots = frozenBlocksInEL > envelope.Message.Payload.BlockNumber
				} else {
					// GLOAS EMPTY: no EL block for this slot; EL chain did not advance.
					// Keep isInElSnapshots=false so we continue backwards to find the next FULL block.
					isInElSnapshots = false
				}
			} else {
				isInElSnapshots = frozenBlocksInEL > blk.Block.Body.ExecutionPayload.BlockNumber
			}
			if cfg.engine.HasGapInSnapshots(ctx) && frozenBlocksInEL > 0 {
				destinationBlockForEL = frozenBlocksInEL - 1
			}
		}

		if slot == 0 || (isInCLSnapshots && isInElSnapshots) {
			return true, tx.Commit()
		}
		return hasDownloadEnoughForImmediateBlobsBackfilling &&
				(!cfg.caplinConfig.ArchiveBlocks || slot <= cfg.sn.SegmentsMax()) &&
				(elBackfillFinished(slot, uint64(currEth1Progress.Load()), destinationSlotForEL, destinationBlockForEL) || isInElSnapshots),
			tx.Commit()
	})

	finishCh := make(chan struct{})
	workerResultCh := make(chan error, 1)
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

				logArgs = append(
					logArgs,
					"slot", currProgress,
					"blockNumber", currEth1Progress.Load(),
					"blk/sec", fmt.Sprintf("%.1f", speed),
					"snapshots", cfg.sn.SegmentsMax(),
				)

				isDownloadingForBeacon := (hasFinishedDownloadingElBlocks.Load() || cfg.caplinConfig.ArchiveBlocks) && clparams.SupportBackfilling(cfg.beaconCfg.DepositNetworkID)

				if cfg.engine != nil && cfg.engine.SupportInsertion() {
					logArgs = append(logArgs, "frozenBlocks", cfg.engine.FrozenBlocks(ctx))
					if !isDownloadingForBeacon {
						// If we are not backfilling, we are in the EL phase.
						// [Modified in Gloas:EIP7732] Use initialEth1Progress to avoid nil ExecutionPayload access.
						highestBlockSeen = uint64(initialEth1Progress.Load())

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
					// Genesis block (0) is never collected, so the lowest reachable EL block is 1.
					processed, toprocess := clampProgress(highestBlockSeen, max(lowestBlockToReach, 1), uint64(currEth1Progress.Load()))
					log.Info("Downloading Execution History", "progress",
						fmt.Sprintf("%d/%d", processed, toprocess),
						"ETA", utils.ETA(toprocess-processed, speed),
						"blk/sec", fmt.Sprintf("%.1f", speed))
				} else {
					processed, toprocess := clampProgress(highestBlockSeen, lowestBlockToReach, currProgress)
					log.Info("Downloading Beacon History", "progress",
						fmt.Sprintf("%d/%d", processed, toprocess),
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
		defer close(finishCh)
		publicResultSent := false
		sendPublicResult := func(err error) {
			if !publicResultSent {
				workerResultCh <- err
				publicResultSent = true
			}
		}

		for !cfg.downloader.Finished() {
			if cfg.engine != nil && cfg.downloader.Progress() <= destinationSlotForEL {
				sendPublicResult(nil)
			}
			if err := cfg.downloader.RequestMore(ctx); err != nil {
				if !errors.Is(err, context.Canceled) {
					log.Warn("closing backfilling routine", "err", err)
				}
				sendPublicResult(err)
				return
			}
		}

		// Recover FULL blocks whose envelopes were skipped during backward download.
		if skipped := cfg.downloader.SkippedFullBlocks(); len(skipped) > 0 {
			if !recoverSkippedEnvelopesWithRetries(ctx, cfg, skipped) {
				workerErr := ctx.Err()
				if workerErr == nil {
					workerErr = errSkippedEnvelopeRecoveryIncomplete
				}
				sendPublicResult(workerErr)
				return
			}
		}

		cfg.antiquary.NotifyBackfilled()
		if cfg.caplinConfig.ArchiveBlocks {
			cfg.logger.Info("Full backfilling finished")
		}

		if cfg.blobDownloader != nil {
			cfg.blobDownloader.SetHeadSlot(cfg.startingSlot + 1)
			cfg.blobDownloader.SetNotifyBlobBackfilled(cfg.antiquary.NotifyBlobBackfilled)
			cfg.blobDownloader.Start()
		}
		sendPublicResult(nil)
	}()
	// We block until we are done with the EL side of the backfilling with 2000 blocks of safety margin.
	select {
	case workerErr := <-workerResultCh:
		if workerErr != nil {
			return workerErr
		}
	case <-ctx.Done():
		return ctx.Err()
	}
	cfg.downloader.SetThrottle(cfg.backfillingThrottling) // throttle to 0.6 second for backfilling
	cfg.downloader.SetNeverSkip(false)
	isBackfilling.Store(true)

	cfg.logger.Info("Ready to insert history, waiting for sync cycle to finish")

	return nil
}

func recoverSkippedEnvelopesWithRetries(ctx context.Context, cfg StageHistoryReconstructionCfg, skipped []network.SkippedFullBlock) bool {
	return recoverSkippedEnvelopesWithRetryPolicy(ctx, cfg, skipped,
		func(attemptCtx context.Context, pending []network.SkippedFullBlock) []network.SkippedFullBlock {
			return recoverSkippedEnvelopes(attemptCtx, cfg, pending)
		}, skippedEnvelopeRecoveryRetryInterval)
}

func recoverSkippedEnvelopesWithRetryPolicy(ctx context.Context, cfg StageHistoryReconstructionCfg, skipped []network.SkippedFullBlock, recoverAttempt func(context.Context, []network.SkippedFullBlock) []network.SkippedFullBlock, retryInterval time.Duration) bool {
	if cfg.downloader == nil || !cfg.downloader.HasEnvelopeRecoverySource() {
		log.Warn("[BackwardBeaconDownloader] envelope recovery unavailable", "remaining", len(skipped))
		return false
	}
	return recoverSkippedEnvelopesUntilComplete(ctx, skipped, recoverAttempt, retryInterval)
}

func recoverSkippedEnvelopesUntilComplete(ctx context.Context, skipped []network.SkippedFullBlock, recoverAttempt func(context.Context, []network.SkippedFullBlock) []network.SkippedFullBlock, retryInterval time.Duration) bool {
	pending := skipped
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			log.Warn("[BackwardBeaconDownloader] envelope recovery canceled", "remaining", len(pending), "err", ctx.Err())
			return false
		}
		attemptCtx, cancel := context.WithTimeout(ctx, skippedEnvelopeRecoveryAttemptTimeout)
		pending = recoverAttempt(attemptCtx, pending)
		cancel()
		if len(pending) == 0 {
			return true
		}

		log.Warn("[BackwardBeaconDownloader] envelope recovery incomplete, retrying",
			"attempt", attempt,
			"recovered", len(skipped)-len(pending), "total", len(skipped), "remaining", len(pending))

		select {
		case <-ctx.Done():
			log.Warn("[BackwardBeaconDownloader] envelope recovery canceled", "remaining", len(pending), "err", ctx.Err())
			return false
		case <-time.After(retryInterval):
		}
	}
}

// recoverSkippedEnvelopes attempts to fetch execution payload envelopes for
// GLOAS FULL blocks that were skipped during backward download.
func recoverSkippedEnvelopes(ctx context.Context, cfg StageHistoryReconstructionCfg, skipped []network.SkippedFullBlock) []network.SkippedFullBlock {
	log.Info("[BackwardBeaconDownloader] recovering skipped GLOAS envelopes", "count", len(skipped))

	remaining := recoverSkippedEnvelopeBatches(ctx, skipped, skippedEnvelopeRecoveryBatchSize, skippedEnvelopeRecoveryBatchTimeout,
		func(fetchCtx, persistCtx context.Context, batch []network.SkippedFullBlock) []network.SkippedFullBlock {
			return recoverSkippedEnvelopeBatch(fetchCtx, persistCtx, cfg, batch)
		})
	log.Info("[BackwardBeaconDownloader] envelope recovery complete",
		"recovered", len(skipped)-len(remaining), "total", len(skipped))
	return remaining
}

func recoverSkippedEnvelopeBatches(ctx context.Context, skipped []network.SkippedFullBlock, batchSize int, batchTimeout time.Duration, recoverBatch func(context.Context, context.Context, []network.SkippedFullBlock) []network.SkippedFullBlock) []network.SkippedFullBlock {
	remaining := make([]network.SkippedFullBlock, 0, len(skipped))
	for start := 0; start < len(skipped); start += batchSize {
		end := min(start+batchSize, len(skipped))
		batch := skipped[start:end]
		if ctx.Err() != nil {
			return rotateTimedOutEnvelopeRecovery(skipped, start, remaining)
		}
		batchCtx, cancel := context.WithTimeout(ctx, batchTimeout)
		remaining = append(remaining, recoverBatch(batchCtx, ctx, batch)...)
		cancel()
	}
	return remaining
}

func rotateTimedOutEnvelopeRecovery(skipped []network.SkippedFullBlock, unattemptedStart int, failed []network.SkippedFullBlock) []network.SkippedFullBlock {
	pending := make([]network.SkippedFullBlock, 0, len(skipped)-unattemptedStart+len(failed))
	pending = append(pending, skipped[unattemptedStart:]...)
	pending = append(pending, failed...)
	return pending
}

func recoverSkippedEnvelopeBatch(fetchCtx, persistCtx context.Context, cfg StageHistoryReconstructionCfg, batch []network.SkippedFullBlock) []network.SkippedFullBlock {
	if cfg.indiciesDB == nil || cfg.blockReader == nil {
		return append([]network.SkippedFullBlock(nil), batch...)
	}
	blocks := readSkippedEnvelopeBlocks(persistCtx, cfg, batch)
	recovery := cfg.downloader.RecoverSkippedEnvelopes(fetchCtx, batch, blocks)
	tx, err := cfg.indiciesDB.BeginRo(persistCtx)
	if err != nil {
		return append([]network.SkippedFullBlock(nil), batch...)
	}
	defer tx.Rollback()

	return unresolvedSkippedEnvelopes(batch, recovery, func(s network.SkippedFullBlock, env *cltypes.SignedExecutionPayloadEnvelope) bool {
		block, err := cfg.blockReader.ReadBlockByRoot(persistCtx, tx, common.Hash(s.Root))
		if err != nil || block == nil || block.Block == nil || block.Block.Body == nil {
			log.Warn("[BackwardBeaconDownloader] skipped block unavailable during recovery", "slot", s.Slot, "root", common.Hash(s.Root), "err", err)
			return false
		}
		root, err := block.Block.HashSSZ()
		if err != nil || root != s.Root {
			return false
		}
		if err := network.ValidateFetchedEnvelope(cfg.beaconCfg, block, common.Hash(s.Root), env); err != nil {
			log.Warn("[BackwardBeaconDownloader] recovered envelope does not match block", "slot", s.Slot, "root", common.Hash(s.Root), "err", err)
			return false
		}
		return recoverSkippedEnvelope(persistCtx, cfg, s, block, env)
	})
}

func unresolvedSkippedEnvelopes(batch []network.SkippedFullBlock, recovery network.EnvelopeRecoveryResult, persist func(network.SkippedFullBlock, *cltypes.SignedExecutionPayloadEnvelope) bool) []network.SkippedFullBlock {
	remaining := make([]network.SkippedFullBlock, 0, len(batch))
	for _, item := range batch {
		root := common.Hash(item.Root)
		envelope := recovery.Envelopes[root]
		if envelope == nil || !persist(item, envelope) {
			remaining = append(remaining, item)
		}
	}
	return remaining
}

func readSkippedEnvelopeBlocks(ctx context.Context, cfg StageHistoryReconstructionCfg, batch []network.SkippedFullBlock) map[common.Hash]*cltypes.SignedBeaconBlock {
	blocks := make(map[common.Hash]*cltypes.SignedBeaconBlock, len(batch))
	tx, err := cfg.indiciesDB.BeginRo(ctx)
	if err != nil {
		return blocks
	}
	defer tx.Rollback()
	for _, item := range batch {
		root := common.Hash(item.Root)
		block, err := cfg.blockReader.ReadBlockByRoot(ctx, tx, root)
		if err != nil || block == nil || block.Block == nil || block.Block.Body == nil {
			continue
		}
		decodedRoot, err := block.Block.HashSSZ()
		if err == nil && decodedRoot == item.Root {
			blocks[root] = block
		}
	}
	return blocks
}

func recoverSkippedEnvelope(ctx context.Context, cfg StageHistoryReconstructionCfg, s network.SkippedFullBlock, block *cltypes.SignedBeaconBlock, env *cltypes.SignedExecutionPayloadEnvelope) bool {
	if cfg.executionBlocksCollector != nil {
		if err := cfg.executionBlocksCollector.AddGloasBlock(block.Block, env); err != nil {
			log.Warn("[BackwardBeaconDownloader] envelope recovery: add block failed", "err", err)
			return false
		}
	}

	tx, err := cfg.indiciesDB.BeginRw(ctx)
	if err != nil {
		log.Warn("[BackwardBeaconDownloader] envelope recovery: begin tx failed", "err", err)
		return false
	}
	defer tx.Rollback()

	if err := beacon_indicies.WriteExecutionPayloadEnvelopeIndicies(tx, common.Hash(s.Root), env.Message); err != nil {
		log.Warn("[BackwardBeaconDownloader] envelope recovery: write indices failed", "err", err)
		return false
	}
	if err := tx.Commit(); err != nil {
		log.Warn("[BackwardBeaconDownloader] envelope recovery: commit failed", "err", err)
		return false
	}
	return true
}
