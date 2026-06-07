package stages

import (
	"context"
	"errors"
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
	blocks, pid, err := cfg.rpc.SendBeaconBlocksByRangeReq(ctx, from, count)
	for err != nil {
		// Respect context cancellation to avoid infinite loops.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if errors.Is(err, peers.ErrNoPeers) {
			// Back off when no peers are available to avoid CPU-burning tight loops.
			log.Debug("[Caplin] no peers available, backing off before retrying block request", "from", from, "count", count)
			select {
			case <-time.After(2 * time.Second):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
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
		highestSeen := cfg.forkChoice.HighestSeen()
		var from uint64
		if highestSeen >= 2 {
			from = highestSeen - 2
		}
		currentSlot := cfg.ethClock.GetCurrentSlot()
		var count uint64
		if currentSlot >= from {
			count = (currentSlot - from) + 4
		} else {
			count = 4
		}

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

			// [GLOAS] Batch-determine and fetch parent envelopes before processing blocks.
			envelopeRoots := determineParentEnvelopeRoots(cfg, blocks.Data)
			envelopes := fetchParentEnvelopes(ctx, cfg, envelopeRoots)

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

				// [GLOAS] Apply parent's envelope before processBlock so that
				// latestBlockHash is up-to-date for bid validation.
				if block.Version() >= clparams.GloasVersion && len(envelopes) > 0 {
					parentRoot := block.Block.ParentRoot
					if env, ok := envelopes[common.Hash(parentRoot)]; ok {
						if envErr := cfg.forkChoice.OnExecutionPayload(ctx, env, false, true); envErr != nil {
							log.Debug("[chainTipSync] failed to apply parent envelope", "slot", block.Block.Slot, "err", envErr)
						}
					}
				}

				// Process the block - DA can be downloaded later if we are behind (see blobHistoryDownloader)
				if err := processBlock(ctx, cfg, cfg.indiciesDB, block, true, true, false); err != nil {
					log.Debug("bad blocks segment received", "err", err, "blockSlot", block.Block.Slot)
					seenBlockRoots[blockRoot] = struct{}{}
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
	return nil
}

// fetchAndApplyEnvelopes fetches missing execution payload envelopes from peers and applies them.
func fetchAndApplyEnvelopes(ctx context.Context, cfg *Cfg, roots [][32]byte) {
	envelopes, err := network.RequestEnvelopesFrantically(ctx, cfg.rpc, roots)
	if err != nil {
		log.Debug("[chainTipSync] failed to request GLOAS envelopes", "err", err)
		return
	}
	for _, env := range envelopes {
		// validatePayload=true so recovered envelopes are sent to EL via NewPayload.
		// If EL is behind, errELBehind will queue them as pending EL payloads.
		if err := cfg.forkChoice.OnExecutionPayload(ctx, env, true, true); err != nil {
			log.Debug("[chainTipSync] failed to apply recovered GLOAS envelope", "beaconBlockRoot", env.Message.BeaconBlockRoot, "err", err)
		}
	}
}

// determineParentEnvelopeRoots identifies parent blocks that were FULL but missing their
// execution payload envelope. It checks parents in fork choice AND within the current batch
// using the bid chain: if child.bid.ParentBlockHash == parent.bid.BlockHash, parent was FULL.
func determineParentEnvelopeRoots(cfg *Cfg, blocks []*cltypes.SignedBeaconBlock) [][32]byte {
	batchBlockByRoot := make(map[common.Hash]*cltypes.SignedBeaconBlock)
	for _, b := range blocks {
		if r, err := b.Block.HashSSZ(); err == nil {
			batchBlockByRoot[common.Hash(r)] = b
		}
	}

	var roots [][32]byte
	seen := make(map[[32]byte]struct{})
	for _, block := range blocks {
		if block.Version() < clparams.GloasVersion {
			continue
		}
		bid := block.Block.Body.GetSignedExecutionPayloadBid()
		if bid == nil || bid.Message == nil {
			continue
		}
		parentRoot := block.Block.ParentRoot
		if cfg.forkChoice.HasEnvelope(common.Hash(parentRoot)) {
			continue
		}
		if _, ok := seen[parentRoot]; ok {
			continue
		}
		// Look up parent bid from fork choice or current batch
		var parentBlock *cltypes.SignedBeaconBlock
		if pb, ok := cfg.forkChoice.GetBlock(common.Hash(parentRoot)); ok {
			parentBlock = pb
		} else if pb, ok := batchBlockByRoot[common.Hash(parentRoot)]; ok {
			parentBlock = pb
		}
		if parentBlock == nil {
			continue
		}
		parentBid := parentBlock.Block.Body.GetSignedExecutionPayloadBid()
		if parentBid == nil || parentBid.Message == nil {
			continue
		}
		if bid.Message.ParentBlockHash == parentBid.Message.BlockHash {
			roots = append(roots, parentRoot)
			seen[parentRoot] = struct{}{}
		}
	}
	return roots
}

// fetchParentEnvelopes batch-fetches execution payload envelopes for the given roots.
// It retries until all envelopes are obtained or the context is cancelled.
func fetchParentEnvelopes(ctx context.Context, cfg *Cfg, roots [][32]byte) map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope {
	if len(roots) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	envelopes := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope)
	remaining := make([][32]byte, len(roots))
	copy(remaining, roots)

	const maxAttempts = 10
	for attempt := 0; attempt < maxAttempts && len(remaining) > 0; attempt++ {
		if ctx.Err() != nil {
			return envelopes
		}
		result, err := network.RequestEnvelopesFrantically(ctx, cfg.rpc, remaining)
		if err != nil {
			log.Debug("[chainTipSync] envelope fetch attempt failed", "err", err, "attempt", attempt+1, "remaining", len(remaining))
			continue
		}
		for k, v := range result {
			envelopes[k] = v
		}
		// Recalculate remaining
		var stillMissing [][32]byte
		for _, root := range remaining {
			if _, ok := envelopes[common.Hash(root)]; !ok {
				stillMissing = append(stillMissing, root)
			}
		}
		remaining = stillMissing
	}
	if len(remaining) > 0 {
		log.Debug("[chainTipSync] some parent envelopes still missing after retries", "missing", len(remaining))
	}
	return envelopes
}

// recoverMissingEnvelopes walks backwards from the highest-seen block root
// and fetches execution payload envelopes for FULL GLOAS blocks that are missing them.
// It walks all the way back to the finalized slot (or the GLOAS boundary) so that
// non-contiguous gaps are recovered — a nearer FULL-with-envelope does not guarantee
// that all earlier parents also have their envelopes.
//
// Uses HighestSeenRoot (O(1) gossip-tracked tip) instead of GetHead to avoid
// running the full fork choice traversal, which is expensive for large trees.
func recoverMissingEnvelopes(ctx context.Context, cfg *Cfg) {
	headRoot := cfg.forkChoice.HighestSeenRoot()
	if headRoot == (common.Hash{}) {
		return
	}

	childBlock, ok := cfg.forkChoice.GetBlock(headRoot)
	if !ok {
		log.Debug("[chainTipSync] envelope recovery: head block not in fork graph", "headRoot", headRoot)
		return
	}

	epoch := childBlock.Block.Slot / cfg.beaconCfg.SlotsPerEpoch
	if cfg.beaconCfg.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
		return
	}

	var missingRoots [][32]byte

	// Also check the head block itself — if its envelope is missing, fork choice
	// can only offer EMPTY, causing the next builder to build on the wrong path.
	if !cfg.forkChoice.HasEnvelope(headRoot) {
		missingRoots = append(missingRoots, [32]byte(headRoot))
	}

	finalizedSlot := cfg.forkChoice.FinalizedSlot()

	for {
		parentRoot := childBlock.Block.ParentRoot
		parentBlock, ok := cfg.forkChoice.GetBlock(parentRoot)
		if !ok {
			break
		}

		if parentBlock.Block.Slot <= finalizedSlot {
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
			// Parent is FULL — check whether its envelope is present.
			if !cfg.forkChoice.HasEnvelope(common.Hash(parentRoot)) {
				missingRoots = append(missingRoots, parentRoot)
			}
		}
		childBlock = parentBlock
	}

	if len(missingRoots) > 0 {
		log.Info("[chainTipSync] envelope recovery: fetching missing envelopes", "count", len(missingRoots))
		fetchAndApplyEnvelopes(ctx, cfg, missingRoots)
	}
}

// pollForEnvelope polls HasEnvelope until the envelope arrives or the timeout expires.
func pollForEnvelope(ctx context.Context, cfg *Cfg, headRoot common.Hash, timeout time.Duration) {
	pollCtx, pollCancel := context.WithTimeout(ctx, timeout)
	defer pollCancel()

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-pollCtx.Done():
			return
		case <-ticker.C:
			if cfg.forkChoice.HasEnvelope(headRoot) {
				return
			}
		}
	}
}

// chainTipSync synchronizes the chain tip by fetching blocks from the highest seen block up to the target slot by listening to incoming blocks.
// or by fetching blocks that might have been missed by gossip after a delay.
func chainTipSync(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	// [GLOAS] Recover any execution payload envelopes that were missed by gossip.
	// This runs every cycle (not just when caught-up) because seenSlot < targetSlot
	// is almost always true — by the time ChainTipSync finishes a slot, the next one
	// has arrived. Recovery only needs P2P (cfg.rpc) and fork choice — no local EL
	// insertion — so it must run regardless of SupportInsertion().
	recoverMissingEnvelopes(ctx, cfg)

	// Flush any collected blocks to the execution engine before ForkChoice.
	// DrainPendingELPayloads and blockCollector require local insertion support.
	if cfg.executionClient != nil && cfg.executionClient.SupportInsertion() {
		// [New in Gloas:EIP7732] Drain execution blocks whose CL transition succeeded
		// but whose EL newPayload failed (EL was behind).  Adding them to the collector
		// before Flush ensures they are inserted into EL in block-number order, filling
		// the gap that would otherwise permanently break the EL chain.
		for _, p := range cfg.forkChoice.DrainPendingELPayloads() {
			if p.Envelope != nil && p.Envelope.Message != nil && p.Envelope.Message.Payload != nil {
				if addErr := cfg.blockCollector.AddGloasBlock(p.Block.Block, p.Envelope); addErr != nil {
					log.Warn("[chainTipSync] failed to add pending EL payload to collector", "err", addErr)
				}
			}
		}
		// Flush must run under a bounded context. Using context.Background()
		// here can deadlock with the EL exec stage: blockCollector.Flush
		// blocks waiting for the EL to accept the inserted blocks, but the
		// EL stage waits for Caplin to issue a forkchoice update before it
		// advances — and Caplin can't issue one because the clstages thread
		// is stuck inside this Flush. Use a bounded deadline so a stalled
		// EL surfaces as a Warn and the clstages loop can move on, eventually
		// driving an FCU that unblocks the EL.
		flushCtx, flushCancel := context.WithTimeout(ctx, 30*time.Second)
		if err := cfg.blockCollector.Flush(flushCtx); err != nil {
			log.Warn("[chainTipSync] blockCollector.Flush failed (EL may still be catching up)", "err", err)
		}
		flushCancel()
	}

	if args.seenSlot >= args.targetSlot {
		// [GLOAS] Wait for the head's execution payload envelope before proceeding to ForkChoice.
		// The block was already processed by gossip during SleepForSlot, but the envelope
		// may still be in-flight. Without this, FCU sends the parent's execution hash.
		headEpoch := args.targetSlot / cfg.beaconCfg.SlotsPerEpoch
		if cfg.beaconCfg.GetCurrentStateVersion(headEpoch) >= clparams.GloasVersion {
			headRoot := cfg.forkChoice.HighestSeenRoot()
			if headRoot != (common.Hash{}) && !cfg.forkChoice.HasEnvelope(headRoot) {
				pollForEnvelope(ctx, cfg, headRoot, 2*time.Second)
			}
			// NOTE: recoverMissingEnvelopes runs unconditionally above (before
			// SupportInsertion check), so it covers every cycle.
		}
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
