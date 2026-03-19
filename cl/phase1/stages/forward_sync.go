package stages

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
)

// ErrForwardSyncStale is returned when forward sync makes no progress for an extended period.
// The stage transition function should skip ForwardSync and go directly to ChainTipSync.
var ErrForwardSyncStale = errors.New("forward sync stale")

// shouldProcessBlobs checks if any block in the given list of blocks
// has a version greater than or equal to DenebVersion and contains BlobKzgCommitments.
func shouldProcessBlobs(blocks []*cltypes.SignedBeaconBlock, cfg *Cfg) bool {
	if !cfg.caplinConfig.ArchiveBlobs && !cfg.caplinConfig.ImmediateBlobsBackfilling {
		return false
	}
	blobsExist := false
	highestSlot := blocks[0].Block.Slot
	for _, block := range blocks {
		// Check if block version is greater than or equal to DenebVersion and contains BlobKzgCommitments
		if block.Version() >= clparams.DenebVersion {
			if c := block.Block.Body.GetBlobKzgCommitments(); c != nil && c.Len() > 0 {
				blobsExist = true
			}
		}
		if block.Block.Slot > highestSlot {
			highestSlot = block.Block.Slot
		}
	}
	// Check if the requested blocks are too old to request blobs
	// https://github.com/ethereum/consensus-specs/blob/dev/specs/deneb/p2p-interface.md#the-reqresp-domain

	// this is bad
	// highestEpoch := highestSlot / cfg.beaconCfg.SlotsPerEpoch
	// currentEpoch := cfg.ethClock.GetCurrentEpoch()
	// minEpochDist := uint64(0)
	// if currentEpoch > cfg.beaconCfg.MinEpochsForBlobSidecarsRequests {
	// 	minEpochDist = currentEpoch - cfg.beaconCfg.MinEpochsForBlobSidecarsRequests
	// }
	// finalizedEpoch := currentEpoch - 2
	// if highestEpoch < max(cfg.beaconCfg.DenebForkEpoch, minEpochDist, finalizedEpoch) {
	// 	return false
	// }

	return blobsExist
}

// downloadAndProcessEip4844DA handles downloading and processing of EIP-4844 data availability blobs.
// It takes highest slot processed, and a list of signed beacon blocks as input.
// It returns the highest blob slot processed and an error if any.
func downloadAndProcessEip4844DA(ctx context.Context, logger log.Logger, cfg *Cfg, highestSlotProcessed uint64, blocks []*cltypes.SignedBeaconBlock) (highestBlobSlotProcessed uint64, err error) {
	var (
		ids   *solid.ListSSZ[*cltypes.BlobIdentifier]
		blobs *network2.PeerAndSidecars
	)

	// Retrieve blob identifiers from the given blocks
	ids, err = network2.BlobsIdentifiersFromBlocks(blocks, cfg.beaconCfg)
	if err != nil {
		// Return an error if blob identifiers could not be retrieved
		err = fmt.Errorf("failed to get blob identifiers: %w", err)
		return
	}

	// If there are no blobs to retrieve, return the highest slot processed
	if ids.Len() == 0 {
		return highestSlotProcessed, nil
	}

	// Request blobs from the network
	blobs, err = network2.RequestBlobsFrantically(ctx, cfg.rpc, ids)
	if errors.Is(err, network2.ErrTimeout) {
		log.Warn("Blob request timeout", "from", blocks[0].Block.Slot, "to", blocks[len(blocks)-1].Block.Slot)
		return highestSlotProcessed, nil
	}
	if err != nil {
		// Return an error if blobs could not be retrieved
		err = fmt.Errorf("failed to get blobs: %w", err)
		return
	}

	var highestProcessed, inserted uint64
	// Verify and insert blobs into the blob store
	if highestProcessed, inserted, err = blob_storage.VerifyAgainstIdentifiersAndInsertIntoTheBlobStore(ctx, cfg.blobStore, ids, blobs.Responses, nil); err != nil {
		// Ban the peer if verification fails
		cfg.rpc.BanPeer(blobs.Peer)
		// Return an error if blobs could not be verified
		err = fmt.Errorf("failed to verify blobs: %w", err)
		return
	}
	// If all blobs were inserted successfully, return the highest processed slot
	if inserted == uint64(ids.Len()) {
		return highestProcessed, nil
	}

	// If not all blobs were inserted, return the highest processed slot minus one
	return highestProcessed - 1, err
}

// processDownloadedBlockBatches processes a batch of downloaded blocks.
// It takes the highest block processed, a flag to determine if insertion is needed, a list of signed beacon blocks,
// and a map of beacon block root -> envelope for GLOAS FULL blocks.
// It returns the new highest block processed and an error if any.
func processDownloadedBlockBatches(ctx context.Context, logger log.Logger, cfg *Cfg, highestBlockProcessed uint64, shouldInsert bool, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (newHighestBlockProcessed uint64, err error) {
	// Pre-process the block batch to ensure that the blocks are sorted by slot in ascending order
	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Block.Slot < blocks[j].Block.Slot
	})

	// // [GLOAS] Before processing blocks, fetch missing parent envelopes so that
	// // latestBlockHash is up-to-date when we validate subsequent bids.
	// // DISABLED: with trimAtMissingEnvelope in beacon_downloader.go, this path should
	// // never trigger. It also has a bug: fetchAndApplyEnvelopes only calls
	// // OnExecutionPayload but not AddGloasBlock, which would create an EL chain gap.
	// if missingRoots := findMissingEnvelopeRoots(cfg, blocks); len(missingRoots) > 0 {
	// 	logger.Debug("[Caplin] forward sync: fetching missing parent envelopes", "count", len(missingRoots))
	// 	fetchAndApplyEnvelopes(ctx, cfg, missingRoots)
	// }

	var blockRoot common.Hash
	newHighestBlockProcessed = highestBlockProcessed
	// Iterate over each block in the sorted list
	for _, block := range blocks {
		// Compute the hash of the current block
		blockRoot, err = block.Block.HashSSZ()
		if err != nil {
			// Return an error if block hashing fails
			err = fmt.Errorf("failed to hash block: %w", err)
			return
		}

		var hasSignedHeaderInDB bool

		if err = cfg.indiciesDB.View(ctx, func(tx kv.Tx) error {
			_, hasSignedHeaderInDB, err = beacon_indicies.ReadSignedHeaderByBlockRoot(ctx, tx, blockRoot)
			return err
		}); err != nil {
			err = fmt.Errorf("failed to read signed header: %w", err)
			return
		}

		// Process the block
		if err = processBlock(ctx, cfg, cfg.indiciesDB, block, false, true, false); err != nil {
			if errors.Is(err, forkchoice.ErrEIP4844DataNotAvailable) || errors.Is(err, forkchoice.ErrEIP7594ColumnDataNotAvailable) {
				// Return an error if EIP-4844 data is not available
				logger.Trace("[Caplin] forward sync EIP-4844 data not available", "blockSlot", block.Block.Slot)
				if newHighestBlockProcessed == 0 {
					return 0, nil
				}
				return newHighestBlockProcessed - 1, nil
			}
			if errors.Is(err, forkchoice.ErrMissingSegment) {
				// Parent state not available — likely peer returned incomplete chain.
				// Do NOT advance progress: return the initial highestBlockProcessed so the
				// downloader retries from the same position with a (potentially different) peer.
				// The 2-minute stale timeout in forwardSync will hand off to ChainTipSync
				// if retries never succeed (e.g. all peers lack these blocks).
				logger.Debug("[Caplin] forward sync missing segment, will retry", "blockSlot", block.Block.Slot)
				return highestBlockProcessed, nil
			}
			// Return an error if block processing fails
			err = fmt.Errorf("bad blocks segment received: %w", err)
			return
		}

		// Update the highest block processed if the current block's slot is higher
		if newHighestBlockProcessed < block.Block.Slot {
			newHighestBlockProcessed = block.Block.Slot
		}

		// [New in Gloas:EIP7732] GLOAS blocks carry no execution payload in the body;
		// the payload is delivered separately via a SignedExecutionPayloadEnvelope.
		if block.Version() >= clparams.GloasVersion {
			if env, ok := envelopes[blockRoot]; ok {
				// FULL block: update forkchoice with the envelope (updates eth2Roots, persists to disk).
				// checkBlobData=false and validatePayload=false because we trust the chain during forward sync.
				if fceErr := cfg.forkChoice.OnExecutionPayload(ctx, env, false, false); fceErr != nil {
					logger.Warn("[Caplin] forward sync: failed to process GLOAS envelope", "slot", block.Block.Slot, "err", fceErr)
				} else if shouldInsert {
					if err = cfg.blockCollector.AddGloasBlock(block.Block, env); err != nil {
						err = fmt.Errorf("failed to add gloas block to collector: %w", err)
						return
					}
				}
			} else {
				bid := block.Block.Body.GetSignedExecutionPayloadBid()
				if bid != nil && bid.Message != nil {
					logger.Debug("[Caplin] forward sync: GLOAS block without envelope (treated as EMPTY)", "slot", block.Block.Slot, "bidBlockHash", bid.Message.BlockHash)
				}
			}
			// [Modified in Gloas:EIP7732] Dump state and save head state for restart.
			// DumpBeaconStateOnDisk uses pre-envelope state (block_state) so that the
			// filename computed by BlockRoot() is correct.
			// saveHeadStateOnDiskIfNeeded uses post-envelope state (execution_payload_state)
			// so that a restart loads the full state with correct LatestBlockHash and
			// LatestBlockHeader.Root — preventing "parent block hash mismatch" errors.
			if !hasSignedHeaderInDB && block.Block.Slot%(cfg.beaconCfg.SlotsPerEpoch*2) == 0 {
				var st *state.CachingBeaconState
				st, err = cfg.forkChoice.GetStateAtBlockRoot(blockRoot, false)
				if err == nil && st != nil {
					if err = cfg.forkChoice.DumpBeaconStateOnDisk(st); err != nil {
						err = fmt.Errorf("failed to dump state: %w", err)
						return
					}
				}
				// Use post-envelope state for the restart checkpoint (latest.ssz_snappy).
				var fullSt *state.CachingBeaconState
				fullSt, err = cfg.forkChoice.GetFullStateAtBlockRoot(blockRoot)
				if err != nil || fullSt == nil {
					// Fallback to pre-envelope state if envelope is not available (EMPTY block).
					fullSt = st
					err = nil
				}
				if fullSt != nil {
					if err = saveHeadStateOnDiskIfNeeded(cfg, fullSt); err != nil {
						err = fmt.Errorf("failed to save head state: %w", err)
						return
					}
				}
			}
			// EMPTY block: OnBlock already correctly inherits the parent EL hash in eth2Roots.
			continue
		}

		// Pre-GLOAS: dump state after block processing (envelope is part of the beacon block).
		if !hasSignedHeaderInDB && block.Block.Slot%(cfg.beaconCfg.SlotsPerEpoch*2) == 0 {
			var st *state.CachingBeaconState
			st, err = cfg.forkChoice.GetStateAtBlockRoot(blockRoot, false)
			if err == nil && st != nil {
				if err = cfg.forkChoice.DumpBeaconStateOnDisk(st); err != nil {
					err = fmt.Errorf("failed to dump state: %w", err)
					return
				}
				if err = saveHeadStateOnDiskIfNeeded(cfg, st); err != nil {
					err = fmt.Errorf("failed to save head state: %w", err)
					return
				}
			}
		}

		// If block version is less than BellatrixVersion or shouldInsert is false, skip insertion
		if block.Version() < clparams.BellatrixVersion || !shouldInsert {
			continue
		}
		// Add the block to the block collector
		if err = cfg.blockCollector.AddBlock(block.Block); err != nil {
			// Return an error if adding the block to the collector fails
			err = fmt.Errorf("failed to add block to collector: %w", err)
			return
		}
	}
	return
}

// forwardSync (MAIN ROUTINE FOR ForwardSync) performs the forward synchronization of beacon blocks.
func forwardSync(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	var (
		shouldInsert  = cfg.executionClient != nil && cfg.executionClient.SupportInsertion() // Check if the execution client supports insertion
		secsPerLog    = 30                                                                   // Interval in seconds for logging progress
		logTicker     = time.NewTicker(time.Duration(secsPerLog) * time.Second)              // Ticker for logging progress
		downloader    = network2.NewForwardBeaconDownloader(ctx, cfg.rpc)                    // Initialize a new forward beacon downloader
		currentSlot   atomic.Uint64                                                          // Atomic variable to track the current slot
		startSlot     = cfg.forkChoice.HighestSeen()
		maxReorgRange = uint64(300) // if node falls too much out of sync, we allow a maximum reorg range of 300 slots
	)
	// Start forwardsync a little bit behind the highest seen slot (account for potential reorgs)
	if startSlot < maxReorgRange {
		startSlot = 0
	} else {
		startSlot = startSlot - maxReorgRange
	}

	finalizedSlot := cfg.forkChoice.FinalizedCheckpoint().Epoch * cfg.beaconCfg.SlotsPerEpoch
	startSlot = max(startSlot, finalizedSlot, cfg.forkChoice.AnchorSlot()) // we cap how low we go with the finalized slot and anchor slot

	// Initialize the slot to download from the finalized checkpoint
	currentSlot.Store(startSlot)

	// Always start from the current finalized checkpoint
	downloader.SetHighestProcessedSlot(currentSlot.Load())

	// Set the function to process downloaded blocks
	downloader.SetProcessFunction(func(initialHighestSlotProcessed uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (newHighestSlotProcessed uint64, err error) {
		highestSlotProcessed, err := processDownloadedBlockBatches(ctx, logger, cfg, initialHighestSlotProcessed, shouldInsert, blocks, envelopes)
		if err != nil {
			logger.Warn("[Caplin] Failed to process block batch", "err", err)
			return initialHighestSlotProcessed, err
		}
		currentSlot.Store(highestSlotProcessed)
		return highestSlotProcessed, nil
	})

	// Get the current slot of the chain tip
	chainTipSlot := cfg.ethClock.GetCurrentSlot()
	logger.Info("[Caplin] Forward Sync", "from", currentSlot.Load(), "to", chainTipSlot)
	prevProgress := currentSlot.Load()

	// Stale progress detection: if no progress for this duration, exit and let ChainTipSync take over.
	const staleTimeout = 2 * time.Minute
	lastProgressTime := time.Now()
	lastProgressSlot := currentSlot.Load()

	// Run the log loop until the highest processed slot reaches the chain tip slot
	for downloader.GetHighestProcessedSlot() < chainTipSlot {
		downloader.RequestMore(ctx)

		// Detect stale progress: if no new slots processed for staleTimeout, exit.
		// ChainTipSync will take over using gossip, which works better near the chain tip.
		if cur := currentSlot.Load(); cur > lastProgressSlot {
			lastProgressSlot = cur
			lastProgressTime = time.Now()
		} else if time.Since(lastProgressTime) > staleTimeout {
			logger.Info("[Caplin] Forward sync: no progress, handing off to ChainTipSync",
				"progress", currentSlot.Load(), "targetSlot", chainTipSlot,
				"stale", time.Since(lastProgressTime).Round(time.Second))
			return ErrForwardSyncStale
		}

		select {
		case <-ctx.Done():
			// Return if the context is done
			return ctx.Err()
		case <-logTicker.C:
			// Log progress at regular intervals
			progressMade := chainTipSlot - currentSlot.Load()
			distFromChainTip := time.Duration(progressMade*cfg.beaconCfg.SecondsPerSlot) * time.Second
			timeProgress := currentSlot.Load() - prevProgress
			estimatedTimeRemaining := 999 * time.Hour
			if timeProgress > 0 {
				estimatedTimeRemaining = time.Duration(float64(progressMade)/(float64(currentSlot.Load()-prevProgress)/float64(secsPerLog))) * time.Second
			}
			if distFromChainTip < 0 || estimatedTimeRemaining < 0 {
				continue
			}
			prevProgress = currentSlot.Load()
			logger.Info("[Caplin] Forward Sync", "progress", currentSlot.Load(), "distance-from-chain-tip", distFromChainTip, "estimated-time-remaining", estimatedTimeRemaining)
		default:
		}
	}

	return nil
}
