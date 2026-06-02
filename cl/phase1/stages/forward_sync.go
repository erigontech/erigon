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
	"github.com/erigontech/erigon/cl/fork"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/persistence/blob_storage"
	"github.com/erigontech/erigon/cl/phase1/core/checkpoint_sync"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	network2 "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/cl/utils/bls"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
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
				logger.Trace("[Caplin] forward sync data not available", "blockSlot", block.Block.Slot, "err", err)
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
			if errors.Is(err, forkchoice.ErrParentEnvelopePending) {
				// Parent is FULL but its envelope is not yet on disk.  This is
				// normal during forward sync: the envelope either arrives in this
				// batch (for a later block) or in a subsequent batch.  Return
				// whatever progress was made so far (do NOT ban the peer).
				logger.Debug("[Caplin] forward sync: parent envelope pending, will retry", "blockSlot", block.Block.Slot)
				if newHighestBlockProcessed > 0 {
					return newHighestBlockProcessed, nil
				}
				return highestBlockProcessed, nil
			}
			if errors.Is(err, forkchoice.ErrNotFinalizedDescendant) {
				// Block is on a different fork than our finalized chain.
				// Common on devnets with stalled finality where the beacon API
				// may return blocks from different forks at different slots.
				// Skip this block but still advance progress so the downloader
				// doesn't retry the same slot range indefinitely.
				logger.Debug("[Caplin] forward sync: block not on finalized chain, skipping", "blockSlot", block.Block.Slot)
				if newHighestBlockProcessed < block.Block.Slot {
					newHighestBlockProcessed = block.Block.Slot
				}
				err = nil
				continue
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
				if fceErr := cfg.forkChoice.OnExecutionPayload(ctx, env, false, false); fceErr != nil {
					logger.Warn("[Caplin] forward sync: failed to process GLOAS envelope", "slot", block.Block.Slot, "err", fceErr)
				} else if shouldInsert {
					if err = cfg.blockCollector.AddGloasBlock(block.Block, env); err != nil {
						err = fmt.Errorf("failed to add gloas block to collector: %w", err)
						return
					}
				}
			}
			// Dump state periodically for restart checkpoints.
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
			continue
		}

		// Pre-GLOAS: dump state periodically for restart checkpoints.
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
		// Add the block to the block collector (pre-GLOAS only)
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
		downloader    = network2.NewForwardBeaconDownloader(ctx, cfg.rpc, cfg.beaconCfg)     // Initialize a new forward beacon downloader
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

	// [New in Gloas:EIP7732] Before starting forward sync, ensure the anchor's envelope
	// is on disk if the anchor was a FULL block. This must happen before any blocks are
	// processed (via forward sync OR gossip), because blocks built on a FULL parent need
	// the parent's envelope on disk so ProcessParentExecutionPayload can resolve it.
	if err := ensureAnchorEnvelopeOnce(ctx, cfg); err != nil {
		logger.Warn("[Caplin] Failed to fetch anchor envelope (will retry)", "err", err)
	}

	// Set HTTP fallback URL for when P2P blocks_by_range fails.
	if urls := clparams.ConfigurableCheckpointsURLs; len(urls) > 0 {
		downloader.SetHTTPFallbackURL(urls[0])
	}

	// Initialize the slot to download from the finalized checkpoint
	currentSlot.Store(startSlot)

	// Always start from the current finalized checkpoint
	downloader.SetHighestProcessedSlot(currentSlot.Load())
	downloader.SetMinSlot(startSlot)

	// Set the function to process downloaded blocks
	downloader.SetProcessFunction(func(initialHighestSlotProcessed uint64, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) (newHighestSlotProcessed uint64, err error) {
		highestSlotProcessed, err := processDownloadedBlockBatches(ctx, logger, cfg, initialHighestSlotProcessed, shouldInsert, blocks, envelopes)
		if err != nil {
			logger.Warn("[Caplin] Failed to process block batch", "err", err)
			return initialHighestSlotProcessed, err
		}
		currentSlot.Store(highestSlotProcessed)
		// Update advertised status so peers don't disconnect us for being too far behind.
		// Use the actual head root (not the finalized root) so headRoot corresponds to
		// headSlot. Peers like Lighthouse penalize us when headRoot maps to a slot far
		// below headSlot ("useless peer").
		if highestSlotProcessed > initialHighestSlotProcessed {
			fc := cfg.forkChoice.FinalizedCheckpoint()
			headRoot := cfg.forkChoice.HighestSeenRoot()
			if headRoot == (common.Hash{}) {
				headRoot = fc.Root
			}
			if err2 := cfg.rpc.SetStatus(fc.Root, fc.Epoch, headRoot, highestSlotProcessed); err2 != nil {
				logger.Debug("Could not update status during forward sync", "err", err2)
			}
		}
		return highestSlotProcessed, nil
	})

	// Get the current slot of the chain tip
	chainTipSlot := cfg.ethClock.GetCurrentSlot()
	logger.Info("[Caplin] Forward Sync", "from", currentSlot.Load(), "to", chainTipSlot)
	prevProgress := currentSlot.Load()

	// Stale progress detection: if no progress for this duration, exit and let ChainTipSync take over.
	// Use a longer timeout for pre-GLOAS chains where range-request stalls are less common.
	staleTimeout := 2 * time.Minute
	if cfg.beaconCfg.GetCurrentStateVersion(cfg.ethClock.GetCurrentEpoch()) < clparams.GloasVersion {
		staleTimeout = 5 * time.Minute
	}
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
			// Before handing off to ChainTipSync, try to set the head state so that
			// gossip beacon_block messages won't be ignored (Syncing() becomes false).
			setHeadStateFromForkChoice(cfg, logger)
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

// setHeadStateFromForkChoice retrieves the current head state from the fork choice store
// and sets it on syncedData. This clears the Syncing() flag so gossip beacon_block messages
// are accepted by ChainTipSync instead of being ignored.
func setHeadStateFromForkChoice(cfg *Cfg, logger log.Logger) {
	headRoot, headSlot, err := cfg.forkChoice.GetHead(nil)
	if err != nil {
		return
	}
	headState, err := cfg.forkChoice.GetStateAtBlockRoot(headRoot, false)
	if err != nil || headState == nil {
		return
	}
	if err := cfg.syncedData.OnHeadStateWithBlockRoot(headState, headRoot); err != nil {
		logger.Debug("Could not set head state from fork choice", "err", err)
		return
	}
	logger.Info("[Caplin] Head state set from forward sync", "slot", headSlot, "root", headRoot)
}

// ensureAnchorEnvelopeOnce proactively fetches the anchor block's execution payload
// envelope from P2P if it is not already on disk. Called once at the start of
// forwardSync, before any blocks are processed.
// [New in Gloas:EIP7732]
func ensureAnchorEnvelopeOnce(ctx context.Context, cfg *Cfg) error {
	anchorSlot := cfg.forkChoice.AnchorSlot()
	epoch := anchorSlot / cfg.beaconCfg.SlotsPerEpoch
	if cfg.beaconCfg.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
		return nil // Pre-GLOAS anchor, nothing to do
	}

	// Find the anchor block root via the header stored in the fork graph
	anchorHeader, ok := cfg.forkChoice.GetHeader(cfg.forkChoice.FinalizedCheckpoint().Root)
	if !ok {
		// Try to find by iterating — the anchor root should be the finalized root
		log.Debug("[ensureAnchorEnvelopeOnce] anchor header not found, skipping")
		return nil
	}
	_ = anchorHeader

	// Get anchor block root from state
	anchorRoot := cfg.forkChoice.FinalizedCheckpoint().Root
	if cfg.forkChoice.IsPayloadVerified(anchorRoot) {
		return nil
	}

	// Optimistically fetch the anchor envelope. If the anchor was EMPTY, the envelope
	// won't exist on the network and the timeout will expire harmlessly.
	// We use the state's LatestExecutionPayloadBid (not the block) since the anchor
	// block is not stored in the fork graph under the single-state model.
	anchorState, err := cfg.forkChoice.GetStateAtBlockRoot(anchorRoot, true)
	if err != nil || anchorState == nil {
		log.Debug("[ensureAnchorEnvelopeOnce] anchor state not available, skipping", "err", err)
		return nil
	}
	bid := anchorState.GetLatestExecutionPayloadBid()
	if bid == nil {
		return nil
	}

	log.Info("[Caplin] Proactively fetching anchor envelope for GLOAS checkpoint sync",
		"anchorSlot", anchorSlot, "anchorRoot", common.Hash(anchorRoot),
		"bidBlockHash", bid.BlockHash)

	var env *cltypes.SignedExecutionPayloadEnvelope
	if cfg.forkChoice.HasEnvelope(anchorRoot) {
		env, err = cfg.forkChoice.ReadEnvelopeFromDisk(anchorRoot)
		if err != nil {
			return fmt.Errorf("failed to read anchor envelope: %w", err)
		}
	} else {
		// Try HTTP API first (checkpoint sync endpoint), then fall back to P2P.
		// HTTP is more reliable on devnets with few peers.
		if httpEnv := checkpoint_sync.FetchFinalizedEnvelope(ctx, cfg.beaconCfg, cfg.caplinConfig); httpEnv != nil {
			log.Info("[Caplin] Anchor envelope fetched via HTTP checkpoint sync", "anchorSlot", anchorSlot)
			env = httpEnv
		} else {
			// Fall back to P2P
			log.Info("[Caplin] HTTP envelope fetch returned nil, trying P2P...", "anchorSlot", anchorSlot)
			envMap, err := network2.RequestEnvelopesFrantically(ctx, cfg.rpc, [][32]byte{anchorRoot})
			if err != nil {
				return fmt.Errorf("failed to request anchor envelope: %w", err)
			}
			env = envMap[common.Hash(anchorRoot)]
		}
	}
	if env == nil {
		log.Info("[Caplin] Anchor envelope not received (block may be EMPTY)", "anchorSlot", anchorSlot)
		return nil // Not fatal — if EMPTY, envelope doesn't exist
	}
	if err := validateAnchorEnvelope(cfg.beaconCfg, anchorState, anchorRoot, bid, env); err != nil {
		return fmt.Errorf("invalid anchor envelope: %w", err)
	}

	// Use StoreAnchorEnvelope instead of OnExecutionPayload because the checkpoint
	// sync state already incorporates the envelope's effects.
	// We only need to persist the envelope to disk so forward sync can resolve parent
	// execution payloads for subsequent blocks.
	if err := cfg.forkChoice.StoreAnchorEnvelope(anchorRoot, env); err != nil {
		return fmt.Errorf("failed to store anchor envelope: %w", err)
	}
	if cfg.executionClient != nil {
		status, err := validateAnchorPayloadWithEL(ctx, cfg, bid, env)
		if err != nil {
			log.Warn("[Caplin] Anchor envelope EL validation failed", "anchorSlot", anchorSlot, "anchorRoot", common.Hash(anchorRoot), "status", status, "err", err)
		}
		switch status {
		case execution_client.PayloadStatusValidated:
			cfg.forkChoice.MarkPayloadVerified(anchorRoot, env.Message.Payload.BlockHash, nil)
		case execution_client.PayloadStatusInvalidated:
			cfg.forkChoice.MarkPayloadInvalid(anchorRoot, env.Message.Payload.BlockHash, nil)
			return fmt.Errorf("anchor execution payload invalidated by EL")
		}
	}

	log.Info("[Caplin] Anchor envelope stored successfully (checkpoint sync)",
		"anchorSlot", anchorSlot, "anchorRoot", common.Hash(anchorRoot))
	return nil
}

func validateAnchorEnvelope(beaconCfg *clparams.BeaconChainConfig, anchorState *state.CachingBeaconState, anchorRoot common.Hash, bid *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) error {
	if bid == nil {
		return errors.New("nil execution payload bid")
	}
	if env == nil || env.Message == nil || env.Message.Payload == nil {
		return errors.New("nil execution payload envelope")
	}
	envelope := env.Message
	payload := envelope.Payload
	if envelope.BeaconBlockRoot != anchorRoot {
		return fmt.Errorf("beacon block root mismatch: envelope=%v anchor=%v", envelope.BeaconBlockRoot, anchorRoot)
	}
	if envelope.ParentBeaconBlockRoot != bid.ParentBlockRoot {
		return fmt.Errorf("parent beacon block root mismatch: envelope=%v bid=%v", envelope.ParentBeaconBlockRoot, bid.ParentBlockRoot)
	}
	if envelope.BuilderIndex != bid.BuilderIndex {
		return fmt.Errorf("builder index mismatch: envelope=%d bid=%d", envelope.BuilderIndex, bid.BuilderIndex)
	}
	if payload.BlockHash != bid.BlockHash {
		return fmt.Errorf("block hash mismatch: envelope=%v bid=%v", payload.BlockHash, bid.BlockHash)
	}
	if payload.ParentHash != bid.ParentBlockHash {
		return fmt.Errorf("parent block hash mismatch: envelope=%v bid=%v", payload.ParentHash, bid.ParentBlockHash)
	}
	if payload.PrevRandao != bid.PrevRandao {
		return fmt.Errorf("prev randao mismatch: envelope=%v bid=%v", payload.PrevRandao, bid.PrevRandao)
	}
	if payload.FeeRecipient != bid.FeeRecipient {
		return fmt.Errorf("fee recipient mismatch: envelope=%v bid=%v", payload.FeeRecipient, bid.FeeRecipient)
	}
	if payload.GasLimit != bid.GasLimit {
		return fmt.Errorf("gas limit mismatch: envelope=%d bid=%d", payload.GasLimit, bid.GasLimit)
	}
	if payload.SlotNumber != bid.Slot {
		return fmt.Errorf("slot mismatch: envelope=%d bid=%d", payload.SlotNumber, bid.Slot)
	}
	if envelope.ExecutionRequests == nil {
		return errors.New("nil execution requests")
	}
	requestsRoot, err := envelope.ExecutionRequests.HashSSZ()
	if err != nil {
		return fmt.Errorf("execution requests root: %w", err)
	}
	if requestsRoot != bid.ExecutionRequestsRoot {
		return fmt.Errorf("execution requests root mismatch: envelope=%v bid=%v", requestsRoot, bid.ExecutionRequestsRoot)
	}
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(beaconCfg, envelope.ExecutionRequests))
	header, err := payload.RlpHeader(&envelope.ParentBeaconBlockRoot, requestsHash)
	if err != nil {
		return fmt.Errorf("payload header: %w", err)
	}
	if header.Hash() != payload.BlockHash {
		return fmt.Errorf("payload block hash mismatch: header=%v payload=%v", header.Hash(), payload.BlockHash)
	}
	if err := verifyAnchorEnvelopeSignature(beaconCfg, anchorState, env, bid.Slot); err != nil {
		return err
	}
	return nil
}

func verifyAnchorEnvelopeSignature(beaconCfg *clparams.BeaconChainConfig, anchorState *state.CachingBeaconState, env *cltypes.SignedExecutionPayloadEnvelope, slot uint64) error {
	var pk [48]byte
	if env.Message.BuilderIndex == clparams.BuilderIndexSelfBuild {
		proposerIndex := anchorState.LatestBlockHeader().ProposerIndex
		validator, err := anchorState.ValidatorForValidatorIndex(int(proposerIndex))
		if err != nil {
			return fmt.Errorf("proposer validator: %w", err)
		}
		pk = validator.PublicKey()
	} else {
		builders := anchorState.GetBuilders()
		if builders == nil {
			return errors.New("builders not found")
		}
		if env.Message.BuilderIndex >= uint64(builders.Len()) {
			return fmt.Errorf("builder index %d out of range", env.Message.BuilderIndex)
		}
		builder := builders.Get(int(env.Message.BuilderIndex))
		if builder == nil {
			return errors.New("builder not found")
		}
		pk = builder.Pubkey
	}
	epoch := state.GetEpochAtSlot(beaconCfg, slot)
	domain, err := anchorState.GetDomain(beaconCfg.DomainBeaconBuilder, epoch)
	if err != nil {
		return fmt.Errorf("builder domain: %w", err)
	}
	signingRoot, err := fork.ComputeSigningRoot(env.Message, domain)
	if err != nil {
		return fmt.Errorf("builder signing root: %w", err)
	}
	valid, err := bls.Verify(env.Signature[:], signingRoot[:], pk[:])
	if err != nil {
		return fmt.Errorf("builder signature: %w", err)
	}
	if !valid {
		return errors.New("invalid builder signature")
	}
	return nil
}

func validateAnchorPayloadWithEL(ctx context.Context, cfg *Cfg, bid *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) (execution_client.PayloadStatus, error) {
	versionedHashes, executionRequestsList, err := buildAnchorNewPayloadArgs(cfg.beaconCfg, bid, env)
	if err != nil {
		return execution_client.PayloadStatusNone, err
	}
	return cfg.executionClient.NewPayload(ctx, env.Message.Payload, &bid.ParentBlockRoot, versionedHashes, executionRequestsList)
}

func buildAnchorNewPayloadArgs(beaconCfg *clparams.BeaconChainConfig, bid *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) ([]common.Hash, []hexutil.Bytes, error) {
	versionedHashes := make([]common.Hash, 0, bid.BlobKzgCommitments.Len())
	if err := solid.RangeErr[*cltypes.KZGCommitment](&bid.BlobKzgCommitments, func(_ int, k *cltypes.KZGCommitment, _ int) error {
		versionedHash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(*k))
		if err != nil {
			return err
		}
		versionedHashes = append(versionedHashes, versionedHash)
		return nil
	}); err != nil {
		return nil, nil, fmt.Errorf("failed to compute versioned hashes: %w", err)
	}
	return versionedHashes, cltypes.GetExecutionRequestsList(beaconCfg, env.Message.ExecutionRequests), nil
}
