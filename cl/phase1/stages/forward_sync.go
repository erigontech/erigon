package stages

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
)

func shouldProcessBlobs(blocks []*cltypes.SignedBeaconBlock) bool {
	for _, block := range blocks {
		if block.Version() >= clparams.DenebVersion && block.Block.Body.BlobKzgCommitments.Len() > 0 {
			return true
		}
	}
	return false
}

func downloadAndProcessEip4844DA(ctx context.Context, logger log.Logger, cfg *Cfg, highestSlotProcessed uint64, highestBlockRootProcessed common.Hash, blocks []*cltypes.SignedBeaconBlock) (highestBlobSlotProcessed uint64, highestBlobBlockRootProcessed common.Hash, err error) {
	var (
		ids   *solid.ListSSZ[*cltypes.BlobIdentifier]
		blobs *network2.PeerAndSidecars
	)

	// Do the DA now, first of all see what blobs to retrieve
	ids, err = network2.BlobsIdentifiersFromBlocks(blocks)
	if err != nil {
		err = fmt.Errorf("failed to get blob identifiers: %w", err)
		return
	}
	if ids.Len() == 0 { // no blobs, no DA.
		return highestSlotProcessed, highestBlockRootProcessed, nil
	}
	blobs, err = network2.RequestBlobsFrantically(ctx, cfg.rpc, ids)
	if err != nil {
		err = fmt.Errorf("failed to get blobs: %w", err)
		return
	}
	var highestProcessed, inserted uint64
	if highestProcessed, inserted, err = blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, cfg.blobStore, ids, blobs.Responses, nil); err != nil {
		cfg.rpc.BanPeer(blobs.Peer)
		err = fmt.Errorf("failed to verify blobs: %w", err)
		return
	}
	if inserted == uint64(ids.Len()) {
		return highestSlotProcessed, highestBlockRootProcessed, nil
	}

	return highestProcessed - 1, highestBlockRootProcessed, err
}

func processDownloadedBlockBatches(ctx context.Context, cfg *Cfg, highestBlockProcessed uint64, highestBlockRoot common.Hash, shouldInsert bool, blocks []*cltypes.SignedBeaconBlock) (newHighestBlockProcessed uint64, newHighestBlockRoot common.Hash, err error) {
	// Pre-process the block batch to ensure that the order is correct
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Block.Slot < blocks[j].Block.Slot
	})

	var (
		blockRoot common.Hash
		st        *state.CachingBeaconState
	)
	newHighestBlockProcessed = highestBlockProcessed
	newHighestBlockRoot = highestBlockRoot
	for _, block := range blocks {
		blockRoot, err = block.Block.HashSSZ()
		if err != nil {
			err = fmt.Errorf("failed to hash block: %w", err)
			return
		}

		if err = processBlock(ctx, cfg, cfg.indiciesDB, block, false, true, false); err != nil {
			err = fmt.Errorf("bad blocks segment received: %w", err)
			return
		}
		// Do some block post-processing

		st, err = cfg.forkChoice.GetStateAtBlockRoot(blockRoot, false)
		if err == nil && block.Block.Slot%(cfg.beaconCfg.SlotsPerEpoch*2) == 0 && st != nil {
			if err = cfg.forkChoice.DumpBeaconStateOnDisk(st); err != nil {
				err = fmt.Errorf("failed to dump state: %w", err)
				return
			}
		}
		// End of block post-processing

		// Update the highest block processed if necessary
		if newHighestBlockProcessed < block.Block.Slot {
			newHighestBlockProcessed = block.Block.Slot
			newHighestBlockRoot = blockRoot
		}
		if block.Version() < clparams.BellatrixVersion || !shouldInsert {
			continue
		}
		if err = cfg.blockCollector.AddBlock(block.Block); err != nil {
			err = fmt.Errorf("failed to add block to collector: %w", err)
			return
		}

		//currentSlot.Store(block.Block.Slot)

	}
	return
}

func forwardSync(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	var (
		shouldInsert        = cfg.executionClient != nil && cfg.executionClient.SupportInsertion()
		finalizedCheckpoint = cfg.forkChoice.FinalizedCheckpoint()
		secsPerLog          = 30
		logTicker           = time.NewTicker(time.Duration(secsPerLog) * time.Second)
		downloader          = network2.NewForwardBeaconDownloader(ctx, cfg.rpc)
		currentSlot         atomic.Uint64
	)
	// Initialize the slot to download
	currentSlot.Store(finalizedCheckpoint.Epoch() * cfg.beaconCfg.SlotsPerEpoch)
	// Always start from the current finalized checkpoint
	downloader.SetHighestProcessedRoot(finalizedCheckpoint.BlockRoot())
	downloader.SetHighestProcessedSlot(currentSlot.Load())
	downloader.SetProcessFunction(func(initialHighestSlotProcessed uint64, initialHighestBlockRootProcessed common.Hash, blocks []*cltypes.SignedBeaconBlock) (newHighestSlotProcessed uint64, newHighestBlockRootProcessed common.Hash, err error) {
		highestSlotProcessed, highestBlockRootProcessed, err := processDownloadedBlockBatches(ctx, cfg, initialHighestSlotProcessed, initialHighestBlockRootProcessed, shouldInsert, blocks)
		if err != nil {
			return initialHighestSlotProcessed, initialHighestBlockRootProcessed, err
		}

		// exit if we are pre-eip4844
		if !shouldProcessBlobs(blocks) {
			currentSlot.Store(highestSlotProcessed)
			return highestSlotProcessed, highestBlockRootProcessed, nil
		}

		highestBlobSlotProcessed, highestBlobRootProcessed, err := downloadAndProcessEip4844DA(ctx, logger, cfg, initialHighestSlotProcessed, initialHighestBlockRootProcessed, blocks)
		if err != nil {
			return initialHighestSlotProcessed, initialHighestBlockRootProcessed, err
		}
		if highestBlobSlotProcessed <= initialHighestSlotProcessed {
			return initialHighestSlotProcessed, initialHighestBlockRootProcessed, nil
		}
		currentSlot.Store(highestBlobSlotProcessed)
		return highestBlobSlotProcessed, highestBlobRootProcessed, nil
	})
	chainTipSlot := cfg.ethClock.GetCurrentSlot()
	logger.Info("[Caplin] Forward Sync", "from", currentSlot.Load(), "to", chainTipSlot)
	prevProgress := currentSlot.Load()
	// Run the log loop
	for downloader.GetHighestProcessedSlot() < chainTipSlot {
		downloader.RequestMore(ctx)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-logTicker.C:
			progressMade := chainTipSlot - currentSlot.Load()
			distFromChainTip := time.Duration(progressMade*cfg.beaconCfg.SecondsPerSlot) * time.Second
			timeProgress := currentSlot.Load() - prevProgress
			estimatedTimeRemaining := 999 * time.Hour
			if timeProgress > 0 {
				estimatedTimeRemaining = time.Duration(float64(progressMade)/(float64(currentSlot.Load()-prevProgress)/float64(secsPerLog))) * time.Second
			}
			prevProgress = currentSlot.Load()
			logger.Info("[Caplin] Forward Sync", "progress", currentSlot.Load(), "distance-from-chain-tip", distFromChainTip, "estimated-time-remaining", estimatedTimeRemaining)
		default:
		}
	}

	return nil
}
