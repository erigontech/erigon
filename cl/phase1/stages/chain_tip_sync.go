package stages

import (
	"context"
	"sort"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	network "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
)

// waitForExecutionEngineToBeFinished checks if the execution engine is ready within a specified timeout.
// It periodically checks the readiness of the execution client and returns true if the client is ready before
// the timeout occurs. If the context is canceled or a timeout occurs, it returns false with the corresponding error.
func waitForExecutionEngineToBeFinished(ctx context.Context, cfg *Cfg) (ready bool, err error) {
	// If no execution client is set, then we can skip this step
	if cfg.executionClient == nil {
		return true, nil
	}

	// Setup the timers
	readyTimeout := time.NewTimer(10 * time.Second)
	readyInterval := time.NewTicker(50 * time.Millisecond)

	// Ensure the timers are stopped to release resources
	defer readyTimeout.Stop()
	defer readyInterval.Stop()

	// Loop to check the readiness status
	for {
		select {
		case <-ctx.Done():
			// Context canceled or timed out
			return false, ctx.Err()
		case <-readyTimeout.C:
			// Timeout reached without the execution engine being ready
			return false, nil
		case <-readyInterval.C:
			// Check the readiness of the execution engine
			ready, err := cfg.executionClient.Ready(ctx)
			if err != nil {
				return false, err
			}
			if !ready {
				// If not ready, continue checking in the next interval
				continue
			}
			// Execution engine is ready
			return true, nil
		}
	}
}

// fetchBlocksFromReqResp retrieves blocks starting from a specified block number and continues for a given count.
// It sends a request to fetch the blocks, verifies the associated blobs, and inserts them into the blob store.
// It returns a PeeredObject containing the blocks and the peer ID, or an error if something goes wrong.
func fetchBlocksFromReqResp(ctx context.Context, cfg *Cfg, from uint64, count uint64) (*peers.PeeredObject[[]*cltypes.SignedBeaconBlock], error) {
	// spam requests to fetch blocks by range from the execution client
	blocks, pid, err := cfg.rpc.SendBeaconBlocksByRangeReq(ctx, from, count)
	for err != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		blocks, pid, err = cfg.rpc.SendBeaconBlocksByRangeReq(ctx, from, count)
	}

	// If no blocks are returned, return nil without error
	if len(blocks) == 0 {
		return nil, nil
	}

	sort.Slice(blocks, func(i, j int) bool {
		return blocks[i].Block.Slot < blocks[j].Block.Slot
	})

	// Return the blocks and the peer ID wrapped in a PeeredObject
	return &peers.PeeredObject[[]*cltypes.SignedBeaconBlock]{
		Data: blocks,
		Peer: pid,
	}, nil
}

// startFetchingBlocksMissedByGossipAfterSomeTime starts fetching blocks that might have been missed by gossip after a delay.
// It periodically fetches blocks from the highest seen block up to the current slot and sends the results or errors to the provided channels.
func startFetchingBlocksMissedByGossipAfterSomeTime(ctx context.Context, cfg *Cfg, args Args, respCh chan<- *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], errCh chan error) {
	// Wait for half the duration of SecondsPerSlot or until the context is done
	select {
	case <-time.After((time.Duration(cfg.beaconCfg.SecondsPerSlot) * time.Second) / 2):
	case <-ctx.Done():
		return
	}

	// Continuously fetch and process blocks
	for {
		// Calculate the range of blocks to fetch
		from := cfg.forkChoice.HighestSeen() - 2
		currentSlot := cfg.ethClock.GetCurrentSlot()
		count := (currentSlot - from) + 4

		// Stop fetching if the highest seen block is greater than or equal to the target slot
		if cfg.forkChoice.HighestSeen() >= args.targetSlot {
			return
		}

		// Fetch blocks from the specified range
		blocks, err := fetchBlocksFromReqResp(ctx, cfg, from, count)
		if err != nil {
			// Send error to the error channel and return
			errCh <- err
			return
		}

		// Send fetched blocks to the response channel or handle context cancellation
		select {
		case respCh <- blocks:
		case <-ctx.Done():
			return
		case <-time.After(time.Second): // Take a short pause before the next iteration
		}
	}
}

// listenToIncomingBlocksUntilANewBlockIsReceived listens for incoming blocks until a new block with a slot greater than or equal to the target slot is received.
// It processes blocks, checks their validity, and publishes them. It also handles context cancellation and logs progress periodically.
func listenToIncomingBlocksUntilANewBlockIsReceived(ctx context.Context, logger log.Logger, cfg *Cfg, args Args, respCh <-chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], errCh chan error) error {
	// Timer to log progress every 30 seconds
	logTicker := time.NewTicker(30 * time.Second)
	defer logTicker.Stop()

	// Timer to check block presence every 20 milliseconds
	presenceTicker := time.NewTicker(20 * time.Millisecond)
	defer presenceTicker.Stop()

	// Map to keep track of seen block roots
	seenBlockRoots := make(map[common.Hash]struct{})
	// [GLOAS] Track the latest envelope fetch goroutine so we can wait for it before returning.
	var envelopeDone chan struct{}
MainLoop:
	for {
		select {
		case <-presenceTicker.C:
			// Check if the highest seen block is greater than or equal to the target slot
			if cfg.forkChoice.HighestSeen() >= args.targetSlot {
				break MainLoop
			}
		case <-ctx.Done():
			// Handle context cancellation
			return ctx.Err()
		case err := <-errCh:
			// Handle errors received on the error channel
			return err
		case blocks := <-respCh:
			if blocks == nil {
				continue
			}
			// [GLOAS] Fetch missing execution payload envelopes for confirmed FULL parents.
			// Runs concurrently so block processing is not blocked; we wait via envelopeDone after MainLoop.
			if fullRoots := findMissingEnvelopeRoots(cfg, blocks.Data); len(fullRoots) > 0 {
				envelopeDone = make(chan struct{})
				go func(done chan struct{}) {
					defer close(done)
					fetchAndApplyEnvelopes(ctx, cfg, fullRoots)
				}(envelopeDone)
			}
			// Handle blocks received on the response channel
			for _, block := range blocks.Data {
				// Check if the parent block is known
				if _, ok := cfg.forkChoice.GetHeader(block.Block.ParentRoot); !ok {
					time.Sleep(time.Millisecond)
					continue
				}

				// Calculate the block root and check if the block is already known
				blockRoot, _ := block.Block.HashSSZ() // Ignoring error as block would not process if HashSSZ failed
				if _, ok := cfg.forkChoice.GetHeader(blockRoot); ok {
					// Check if the block slot is greater than or equal to the target slot
					if block.Block.Slot >= args.targetSlot {
						break MainLoop
					}
					continue
				}

				// Check if the block root has already been seen
				if _, ok := seenBlockRoots[blockRoot]; ok {
					continue
				}

				// Process the block - DA can be downloaded later if we are behind (see blobHistoryDownloader)
				if err := processBlock(ctx, cfg, cfg.indiciesDB, block, true, true, false); err != nil {
					log.Debug("bad blocks segment received", "err", err, "blockSlot", block.Block.Slot)
					continue
				}

				// Mark the block root as seen
				seenBlockRoots[blockRoot] = struct{}{}

				// Check if the block slot is greater than or equal to the target slot
				if block.Block.Slot >= args.targetSlot {
					break MainLoop
				}
			}
		case <-logTicker.C:
			// Log progress periodically
			logger.Info("[Caplin] Progress", "progress", cfg.forkChoice.HighestSeen(), "from", args.seenSlot, "to", args.targetSlot)
		}
	}
	if envelopeDone != nil {
		<-envelopeDone
	}
	return nil
}

// findMissingEnvelopeRoots identifies blocks whose parent was FULL but missing its execution payload envelope.
// Each block's bid.ParentBlockHash is compared against the parent block's bid.BlockHash to determine FULL status.
func findMissingEnvelopeRoots(cfg *Cfg, blocks []*cltypes.SignedBeaconBlock) [][32]byte {
	var missing [][32]byte
	for _, block := range blocks {
		epoch := block.Block.Slot / cfg.beaconCfg.SlotsPerEpoch
		if cfg.beaconCfg.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
			continue
		}
		parentRoot := block.Block.ParentRoot
		if cfg.forkChoice.HasEnvelope(common.Hash(parentRoot)) {
			continue
		}
		bid := block.Block.Body.GetSignedExecutionPayloadBid()
		if bid == nil || bid.Message == nil {
			log.Warn("[chainTipSync] GLOAS block missing bid", "slot", block.Block.Slot)
			continue
		}
		parentBlock, ok := cfg.forkChoice.GetBlock(parentRoot)
		if !ok {
			log.Debug("[chainTipSync] parent block not in fork choice (possible reorg)", "slot", block.Block.Slot, "parentRoot", common.Hash(parentRoot))
			continue
		}
		parentBid := parentBlock.Block.Body.GetSignedExecutionPayloadBid()
		if parentBid == nil || parentBid.Message == nil {
			continue // normal at GLOAS transition boundary (parent is pre-GLOAS)
		}
		if bid.Message.ParentBlockHash == parentBid.Message.BlockHash {
			missing = append(missing, parentRoot)
		}
	}
	return missing
}

// fetchAndApplyEnvelopes fetches missing execution payload envelopes from peers and applies them.
func fetchAndApplyEnvelopes(ctx context.Context, cfg *Cfg, roots [][32]byte) {
	envelopes, err := network.RequestEnvelopesFrantically(ctx, cfg.rpc, roots)
	if err != nil {
		log.Debug("[chainTipSync] failed to request GLOAS envelopes", "err", err)
		return
	}
	for _, env := range envelopes {
		if err := cfg.forkChoice.OnExecutionPayload(ctx, env, false, false); err != nil {
			log.Debug("[chainTipSync] failed to apply GLOAS envelope", "beaconBlockRoot", env.Message.BeaconBlockRoot, "err", err)
		}
	}
}

// recoverMissingEnvelopes walks backwards from the current fork choice head
// and fetches execution payload envelopes for FULL GLOAS blocks that are missing them.
// It stops when it finds a parent that already has its envelope, since prior blocks are
// expected to be covered. Fetching runs in a background goroutine to avoid blocking the stage.
func recoverMissingEnvelopes(ctx context.Context, cfg *Cfg) {
	headRoot, headSlot, err := cfg.forkChoice.GetHead(nil)
	if err != nil {
		log.Debug("[chainTipSync] envelope recovery: failed to get head", "err", err)
		return
	}

	epoch := headSlot / cfg.beaconCfg.SlotsPerEpoch
	if cfg.beaconCfg.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
		return
	}

	childBlock, ok := cfg.forkChoice.GetBlock(headRoot)
	if !ok {
		log.Debug("[chainTipSync] envelope recovery: head block not in fork graph", "headRoot", headRoot)
		return
	}

	var missingRoots [][32]byte
	for {
		parentRoot := childBlock.Block.ParentRoot
		parentBlock, ok := cfg.forkChoice.GetBlock(parentRoot)
		if !ok {
			break
		}

		parentEpoch := parentBlock.Block.Slot / cfg.beaconCfg.SlotsPerEpoch
		if cfg.beaconCfg.GetCurrentStateVersion(parentEpoch) < clparams.GloasVersion {
			break
		}

		childBid := childBlock.Block.Body.GetSignedExecutionPayloadBid()
		parentBid := parentBlock.Block.Body.GetSignedExecutionPayloadBid()
		if childBid == nil || childBid.Message == nil || parentBid == nil || parentBid.Message == nil {
			childBlock = parentBlock
			continue
		}

		if childBid.Message.ParentBlockHash == parentBid.Message.BlockHash {
			// Parent is FULL.
			if cfg.forkChoice.HasEnvelope(common.Hash(parentRoot)) {
				// Envelope already present; everything further back is covered.
				break
			}
			missingRoots = append(missingRoots, parentRoot)
		}
		// Whether FULL (missing) or EMPTY, continue walking backwards.
		childBlock = parentBlock
	}

	if len(missingRoots) > 0 {
		log.Debug("[chainTipSync] envelope recovery: fetching missing envelopes", "count", len(missingRoots))
		go fetchAndApplyEnvelopes(ctx, cfg, missingRoots)
	}
}

// chainTipSync synchronizes the chain tip by fetching blocks from the highest seen block up to the target slot by listening to incoming blocks.
// or by fetching blocks that might have been missed by gossip after a delay.
func chainTipSync(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	// [GLOAS] When caught up on blocks, check for missing execution payload envelopes.
	// The gossip service has a 30-second window to deliver envelopes; this is the fallback
	// for when gossip permanently failed but the CL block arrived on time.
	if args.seenSlot >= args.targetSlot {
		recoverMissingEnvelopes(ctx, cfg)
		return nil
	}

	totalRequest := args.targetSlot - args.seenSlot
	log.Debug("[chainTipSync] totalRequest", "totalRequest", totalRequest, "seenSlot", args.seenSlot, "targetSlot", args.targetSlot)
	// If the execution engine is not ready, wait for it to be ready.
	ready, err := waitForExecutionEngineToBeFinished(ctx, cfg)
	if err != nil {
		log.Warn("[chainTipSync] error waiting for execution engine to be ready", "err", err)
		return err
	}
	if !ready {
		log.Debug("[chainTipSync] execution engine is not ready yet")
		return nil
	}

	log.Debug("[chainTipSync] execution engine is ready")

	if cfg.executionClient != nil && cfg.executionClient.SupportInsertion() {
		if err := cfg.blockCollector.Flush(context.Background()); err != nil {
			return err
		}
	}

	logger.Debug("waiting for blocks...",
		"seenSlot", args.seenSlot,
		"targetSlot", args.targetSlot,
		"requestedSlots", totalRequest,
	)
	respCh := make(chan *peers.PeeredObject[[]*cltypes.SignedBeaconBlock], 1024)
	errCh := make(chan error)

	// 25 seconds is a good timeout for this
	ctx, cn := context.WithTimeout(ctx, 25*time.Second)
	defer cn()

	go startFetchingBlocksMissedByGossipAfterSomeTime(ctx, cfg, args, respCh, errCh)

	return listenToIncomingBlocksUntilANewBlockIsReceived(ctx, logger, cfg, args, respCh, errCh)
}
