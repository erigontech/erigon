package stages

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/cltypes/solid"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/phase1/forkchoice"
	network "github.com/erigontech/erigon/cl/phase1/network"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/utils"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/hexutil"
	"github.com/erigontech/erigon/common/log/v3"
)

const (
	maxGloasVerificationSweepPerCycle = 32
	maxGloasAncestorVisitsPerCycle    = 32
	maxPendingGloasPayloadsPerCycle   = 32
	gloasPayloadRetryBudget           = 2 * time.Second
)

func gloasVersionedHashes(blobCommitments *solid.ListSSZ[*cltypes.KZGCommitment]) ([]common.Hash, error) {
	if blobCommitments == nil || blobCommitments.Len() == 0 {
		return []common.Hash{}, nil
	}
	versionedHashes := make([]common.Hash, 0, blobCommitments.Len())
	if err := solid.RangeErr[*cltypes.KZGCommitment](blobCommitments, func(_ int, k *cltypes.KZGCommitment, _ int) error {
		versionedHash, err := utils.KzgCommitmentToVersionedHash(common.Bytes48(*k))
		if err != nil {
			return err
		}
		versionedHashes = append(versionedHashes, versionedHash)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("failed to compute versioned hashes: %w", err)
	}
	return versionedHashes, nil
}

func gloasExecutionRequestsList(beaconCfg *clparams.BeaconChainConfig, requests *cltypes.ExecutionRequests) []hexutil.Bytes {
	if requests == nil {
		return nil
	}
	return cltypes.GetExecutionRequestsList(beaconCfg, requests)
}

func validPendingGloasPayload(p forkchoice.PendingELPayload) bool {
	return p.Block != nil && p.Block.Block != nil && p.Envelope != nil && p.Envelope.Message != nil && p.Envelope.Message.Payload != nil
}

func gloasEnvelopePayloadHash(envelope *cltypes.SignedExecutionPayloadEnvelope) (common.Hash, bool) {
	if envelope == nil || envelope.Message == nil || envelope.Message.Payload == nil {
		return common.Hash{}, false
	}
	return envelope.Message.Payload.BlockHash, true
}

func canValidateGloasPayloads(cfg *Cfg) bool {
	return cfg.executionClient != nil
}

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

	slices.SortFunc(blocks, func(a, b *cltypes.SignedBeaconBlock) int {
		return cmp.Compare(a.Block.Slot, b.Block.Slot)
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
						if envErr := cfg.forkChoice.OnExecutionPayload(ctx, env, false, canValidateGloasPayloads(cfg)); envErr != nil {
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
		if err := cfg.forkChoice.OnExecutionPayload(ctx, env, true, canValidateGloasPayloads(cfg)); err != nil {
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
		maps.Copy(envelopes, result)
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

// recoverMissingEnvelopes incrementally scans from the selected head for missing FULL-block envelopes.
func recoverMissingEnvelopes(ctx context.Context, cfg *Cfg) {
	headRoot, err := gloasVerificationHeadRoot(cfg.forkChoice)
	if err != nil || headRoot == (common.Hash{}) {
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

	missingRoots := make([][32]byte, 0, maxGloasAncestorVisitsPerCycle)

	finalizedSlot := cfg.forkChoice.FinalizedSlot()
	var directExtensionParent common.Hash
	if cfg.gloasEnvelopeRecoveryHead != (common.Hash{}) && cfg.gloasEnvelopeRecoveryHead != headRoot {
		newHeadBlock, ok := cfg.forkChoice.GetBlock(headRoot)
		directExtension := ok && common.Hash(newHeadBlock.Block.ParentRoot) == cfg.gloasEnvelopeRecoveryHead
		if directExtension {
			directExtensionParent = cfg.gloasEnvelopeRecoveryHead
			oldHeadBlock, oldOK := cfg.forkChoice.GetBlock(cfg.gloasEnvelopeRecoveryHead)
			newBid := newHeadBlock.Block.Body.GetSignedExecutionPayloadBid()
			if oldOK && oldHeadBlock != nil && newBid != nil && newBid.Message != nil {
				oldBid := oldHeadBlock.Block.Body.GetSignedExecutionPayloadBid()
				if oldBid != nil && oldBid.Message != nil && newBid.Message.ParentBlockHash == oldBid.Message.BlockHash && !cfg.forkChoice.HasEnvelope(cfg.gloasEnvelopeRecoveryHead) {
					missingRoots = append(missingRoots, [32]byte(cfg.gloasEnvelopeRecoveryHead))
				}
			}
		}
		if !directExtension {
			cfg.gloasEnvelopeRecoveryCursor = common.Hash{}
		}
	}
	cfg.gloasEnvelopeRecoveryHead = headRoot
	scanRoot := cfg.gloasEnvelopeRecoveryCursor
	if scanRoot == (common.Hash{}) {
		if directExtensionParent != (common.Hash{}) {
			scanRoot = directExtensionParent
			if cursorBlock, ok := cfg.forkChoice.GetBlock(scanRoot); ok {
				childBlock = cursorBlock
			}
		} else {
			scanRoot = headRoot
		}
	} else if cursorBlock, cursorOK := cfg.forkChoice.GetBlock(scanRoot); cursorOK {
		childBlock = cursorBlock
	} else {
		scanRoot = headRoot
	}

	completedScan := false
	for visited := 1; visited < maxGloasAncestorVisitsPerCycle; visited++ {
		parentRoot := childBlock.Block.ParentRoot
		parentBlock, ok := cfg.forkChoice.GetBlock(parentRoot)
		if !ok {
			completedScan = true
			break
		}

		if parentBlock.Block.Slot <= finalizedSlot {
			completedScan = true
			break
		}

		parentEpoch := parentBlock.Block.Slot / cfg.beaconCfg.SlotsPerEpoch
		if cfg.beaconCfg.GetCurrentStateVersion(parentEpoch) < clparams.GloasVersion {
			completedScan = true
			break
		}

		childBid := childBlock.Block.Body.GetSignedExecutionPayloadBid()
		parentBid := parentBlock.Block.Body.GetSignedExecutionPayloadBid()
		childBlock = parentBlock
		scanRoot = common.Hash(parentRoot)
		if childBid == nil || childBid.Message == nil || parentBid == nil || parentBid.Message == nil {
			continue
		}

		if childBid.Message.ParentBlockHash == parentBid.Message.BlockHash {
			// Parent is FULL — check whether its envelope is present.
			if !cfg.forkChoice.HasEnvelope(common.Hash(parentRoot)) {
				missingRoots = append(missingRoots, parentRoot)
			}
		}
	}
	if len(missingRoots) > 0 {
		log.Info("[chainTipSync] envelope recovery: fetching missing envelopes", "count", len(missingRoots))
		fetchAndApplyEnvelopes(ctx, cfg, missingRoots)
	}
	advanceGloasEnvelopeRecoveryCursor(cfg, scanRoot, completedScan)
}

func advanceGloasEnvelopeRecoveryCursor(cfg *Cfg, scanRoot common.Hash, completedScan bool) {
	if completedScan {
		cfg.gloasEnvelopeRecoveryCursor = common.Hash{}
	} else {
		cfg.gloasEnvelopeRecoveryCursor = scanRoot
	}
}

type selectedHeadEnvelopeStore interface {
	HasEnvelope(common.Hash) bool
	OnExecutionPayload(context.Context, *cltypes.SignedExecutionPayloadEnvelope, bool, bool) error
}

type envelopeRequestFunc func(context.Context, [][32]byte) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error)

type selectedHeadEnvelopeRequestHooks struct {
	done  func()
	retry func()
}

func waitForSelectedHeadEnvelope(
	ctx context.Context,
	store selectedHeadEnvelopeStore,
	requestEnvelopes envelopeRequestFunc,
	headRoot common.Hash,
	timeout time.Duration,
	requestFromPeer bool,
	validatePayload bool,
	hooks selectedHeadEnvelopeRequestHooks,
) {
	pollCtx, pollCancel := context.WithTimeout(ctx, timeout)
	defer pollCancel()
	if store.HasEnvelope(headRoot) {
		if hooks.done != nil {
			hooks.done()
		}
		return
	}
	if requestFromPeer {
		go func() {
			if hooks.done != nil {
				defer hooks.done()
			}
			envelopes, err := requestEnvelopes(pollCtx, [][32]byte{headRoot})
			if err != nil {
				if hooks.retry != nil && (pollCtx.Err() != nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded)) {
					hooks.retry()
				}
				log.Debug("[chainTipSync] failed to request selected head envelope", "headRoot", headRoot, "err", err)
				return
			}
			envelope := envelopes[headRoot]
			if envelope == nil {
				return
			}
			if pollCtx.Err() != nil {
				if hooks.retry != nil {
					hooks.retry()
				}
				return
			}
			if err := store.OnExecutionPayload(pollCtx, envelope, true, validatePayload); err != nil {
				if hooks.retry != nil {
					hooks.retry()
				}
				log.Debug("[chainTipSync] failed to apply selected head envelope", "headRoot", headRoot, "err", err)
			}
		}()
	}

	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-pollCtx.Done():
			return
		case <-ticker.C:
			if store.HasEnvelope(headRoot) {
				return
			}
		}
	}
}

type selectedHeadEnvelopeRequestClaim struct {
	root common.Hash
	id   uint64
}

func observeSelectedHeadEnvelopeRequest(cfg *Cfg, headRoot common.Hash) {
	cfg.gloasHeadEnvelopeRequestMu.Lock()
	defer cfg.gloasHeadEnvelopeRequestMu.Unlock()
	if cfg.gloasHeadEnvelopeRequestHead != headRoot {
		cfg.gloasHeadEnvelopeRequestHead = headRoot
		cfg.gloasHeadEnvelopeAttempted = false
	}
}

func claimSelectedHeadEnvelopeRequest(cfg *Cfg, headRoot common.Hash) (selectedHeadEnvelopeRequestClaim, bool, bool) {
	cfg.gloasHeadEnvelopeRequestMu.Lock()
	defer cfg.gloasHeadEnvelopeRequestMu.Unlock()
	if cfg.gloasHeadEnvelopeRequestHead != headRoot {
		cfg.gloasHeadEnvelopeRequestHead = headRoot
		cfg.gloasHeadEnvelopeAttempted = false
	}
	if _, ok := cfg.gloasHeadEnvelopeRequests[headRoot]; ok {
		return selectedHeadEnvelopeRequestClaim{}, false, true
	}
	if cfg.gloasHeadEnvelopeAttempted {
		return selectedHeadEnvelopeRequestClaim{}, false, false
	}
	if cfg.gloasHeadEnvelopeRequests == nil {
		cfg.gloasHeadEnvelopeRequests = make(map[common.Hash]uint64)
	}
	cfg.gloasHeadEnvelopeAttempted = true
	cfg.gloasHeadEnvelopeRequestID++
	id := cfg.gloasHeadEnvelopeRequestID
	cfg.gloasHeadEnvelopeRequests[headRoot] = id
	return selectedHeadEnvelopeRequestClaim{root: headRoot, id: id}, true, true
}

func releaseSelectedHeadEnvelopeRequest(cfg *Cfg, claim selectedHeadEnvelopeRequestClaim) {
	cfg.gloasHeadEnvelopeRequestMu.Lock()
	defer cfg.gloasHeadEnvelopeRequestMu.Unlock()
	if cfg.gloasHeadEnvelopeRequests[claim.root] == claim.id {
		delete(cfg.gloasHeadEnvelopeRequests, claim.root)
	}
}

func retrySelectedHeadEnvelopeRequest(cfg *Cfg, claim selectedHeadEnvelopeRequestClaim) {
	cfg.gloasHeadEnvelopeRequestMu.Lock()
	defer cfg.gloasHeadEnvelopeRequestMu.Unlock()
	if cfg.gloasHeadEnvelopeRequestHead == claim.root && cfg.gloasHeadEnvelopeRequests[claim.root] == claim.id {
		cfg.gloasHeadEnvelopeAttempted = false
	}
}

func waitForClaimedSelectedHeadEnvelope(
	ctx context.Context,
	cfg *Cfg,
	store selectedHeadEnvelopeStore,
	requestEnvelopes envelopeRequestFunc,
	headRoot common.Hash,
	timeout time.Duration,
	validatePayload bool,
) {
	claim, requestFromPeer, waitForEnvelope := claimSelectedHeadEnvelopeRequest(cfg, headRoot)
	if !waitForEnvelope {
		return
	}
	hooks := selectedHeadEnvelopeRequestHooks{}
	if requestFromPeer {
		hooks.done = func() { releaseSelectedHeadEnvelopeRequest(cfg, claim) }
		hooks.retry = func() { retrySelectedHeadEnvelopeRequest(cfg, claim) }
	}
	waitForSelectedHeadEnvelope(ctx, store, requestEnvelopes, headRoot, timeout, requestFromPeer, validatePayload, hooks)
}

func shouldRecoverMissingEnvelopes(beaconCfg *clparams.BeaconChainConfig, targetSlot uint64) bool {
	if beaconCfg == nil || beaconCfg.SlotsPerEpoch == 0 {
		return false
	}
	return beaconCfg.GetCurrentStateVersion(targetSlot/beaconCfg.SlotsPerEpoch) >= clparams.GloasVersion
}

func blockSupportsExecutionPayloadEnvelope(block *cltypes.SignedBeaconBlock) bool {
	return block != nil && block.Block != nil && block.Version() >= clparams.GloasVersion
}

type gloasPayloadValidator interface {
	NewPayloadWithAdmission(context.Context, *cltypes.Eth1Block, *common.Hash, []common.Hash, []hexutil.Bytes) (execution_client.PayloadStatus, error)
}

func buildGloasNewPayloadArgs(cfg *Cfg, block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) ([]common.Hash, []hexutil.Bytes, error) {
	if block == nil || block.Block == nil {
		return nil, nil, errors.New("missing beacon block")
	}
	if envelope == nil || envelope.Message == nil || envelope.Message.Payload == nil {
		return nil, nil, errors.New("missing execution payload envelope")
	}

	committedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if committedBid == nil || committedBid.Message == nil {
		return nil, nil, errors.New("missing execution payload bid")
	}

	versionedHashes, err := gloasVersionedHashes(&committedBid.Message.BlobKzgCommitments)
	if err != nil {
		return nil, nil, err
	}
	return versionedHashes, gloasExecutionRequestsList(cfg.beaconCfg, envelope.Message.ExecutionRequests), nil
}

func retryGloasPayloadWithEL(ctx context.Context, cfg *Cfg, block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (execution_client.PayloadStatus, error) {
	versionedHashes, executionRequestsList, err := buildGloasNewPayloadArgs(cfg, block, envelope)
	if err != nil {
		return execution_client.PayloadStatusNone, err
	}
	parentRoot := block.Block.ParentRoot
	return cfg.gloasPayloadValidator.NewPayloadWithAdmission(ctx, envelope.Message.Payload, &parentRoot, versionedHashes, executionRequestsList)
}

func isGloasPayloadKnownInvalid(cfg *Cfg, envelope *cltypes.SignedExecutionPayloadEnvelope) bool {
	executionBlockHash, ok := gloasEnvelopePayloadHash(envelope)
	if !ok {
		return false
	}
	status, ok := cfg.forkChoice.GetRecentExecutionPayloadStatus(executionBlockHash)
	return ok && status == execution_client.PayloadStatusInvalidated
}

func drainPendingGloasPayloads(ctx context.Context, cfg *Cfg) {
	pending := cfg.forkChoice.DrainPendingELPayloadsLimit(maxPendingGloasPayloadsPerCycle)
	for i, p := range pending {
		if ctx.Err() != nil {
			for _, deferred := range pending[i:] {
				cfg.forkChoice.RequeuePendingELPayload(deferred)
			}
			return
		}
		if !validPendingGloasPayload(p) {
			continue
		}
		status, err := retryGloasPayloadWithEL(ctx, cfg, p.Block, p.Envelope)
		if err != nil {
			log.Warn("[chainTipSync] pending GLOAS NewPayload failed", "slot", p.Block.Block.Slot, "status", status, "err", err)
		}
		beaconRoot := p.Envelope.Message.BeaconBlockRoot
		execHash := p.Envelope.Message.Payload.BlockHash
		switch status {
		case execution_client.PayloadStatusValidated:
			cfg.forkChoice.MarkPayloadVerified(beaconRoot, execHash)
		case execution_client.PayloadStatusNone, execution_client.PayloadStatusNotValidated:
			cfg.forkChoice.RequeuePendingELPayload(p)
		case execution_client.PayloadStatusInvalidated:
			cfg.forkChoice.MarkPayloadInvalid(beaconRoot, execHash)
			log.Warn("[chainTipSync] pending GLOAS payload invalidated by EL", "slot", p.Block.Block.Slot, "blockRoot", beaconRoot)
		}
	}
}

type gloasHeadReader interface {
	GetHead(*state.CachingBeaconState) (common.Hash, uint64, error)
}

func gloasVerificationHeadRoot(forkChoice gloasHeadReader) (common.Hash, error) {
	headRoot, _, err := forkChoice.GetHead(nil)
	return headRoot, err
}

func continueGloasVerificationAfterItemFailure(ctx context.Context, completeBatch *bool) bool {
	if ctx.Err() == nil {
		return true
	}
	*completeBatch = false
	return false
}

type gloasVerificationItem struct {
	root  common.Hash
	block *cltypes.SignedBeaconBlock
}

func processImmediateGloasVerificationItems(selectedHead, immediateHead *gloasVerificationItem, process func(gloasVerificationItem) bool, completeBatch *bool) {
	for _, item := range []*gloasVerificationItem{selectedHead, immediateHead} {
		if item != nil && !process(*item) {
			*completeBatch = false
			return
		}
	}
}

func verifyUnverifiedGloasPayloads(ctx context.Context, cfg *Cfg) {
	headRoot, err := gloasVerificationHeadRoot(cfg.forkChoice)
	if err != nil {
		log.Debug("[chainTipSync] failed to select GLOAS verification head", "err", err)
		return
	}
	if headRoot == (common.Hash{}) {
		return
	}

	finalizedSlot := cfg.forkChoice.FinalizedSlot()
	var blocks []gloasVerificationItem
	var immediateHead *gloasVerificationItem

	root := cfg.gloasVerificationCursor
	var directExtensionParentRoot common.Hash
	if cfg.gloasVerificationHead != (common.Hash{}) && cfg.gloasVerificationHead != headRoot {
		newHeadBlock, ok := cfg.forkChoice.GetBlock(headRoot)
		directExtension := ok && common.Hash(newHeadBlock.Block.ParentRoot) == cfg.gloasVerificationHead
		if directExtension {
			directExtensionParentRoot = cfg.gloasVerificationHead
			oldHeadBlock, oldOK := cfg.forkChoice.GetBlock(cfg.gloasVerificationHead)
			if oldOK && oldHeadBlock != nil && cfg.forkChoice.HasEnvelope(cfg.gloasVerificationHead) && !cfg.forkChoice.IsPayloadVerified(cfg.gloasVerificationHead) {
				immediateHead = &gloasVerificationItem{root: cfg.gloasVerificationHead, block: oldHeadBlock}
			}
		}
		if !directExtension {
			root = headRoot
			cfg.gloasVerificationCursor = common.Hash{}
		}
	}
	cfg.gloasVerificationHead = headRoot
	visitLimit := maxGloasAncestorVisitsPerCycle
	if immediateHead != nil {
		visitLimit--
	}
	var selectedHead *gloasVerificationItem
	if root == (common.Hash{}) {
		if directExtensionParentRoot != (common.Hash{}) {
			oldHeadBlock, ok := cfg.forkChoice.GetBlock(directExtensionParentRoot)
			if ok && oldHeadBlock != nil {
				root = common.Hash(oldHeadBlock.Block.ParentRoot)
			} else {
				root = headRoot
			}
		} else {
			root = headRoot
		}
	} else if _, ok := cfg.forkChoice.GetBlock(root); !ok {
		root = headRoot
	} else if root != headRoot {
		headBlock, headOK := cfg.forkChoice.GetBlock(headRoot)
		if headOK && headBlock != nil && cfg.forkChoice.HasEnvelope(headRoot) && !cfg.forkChoice.IsPayloadVerified(headRoot) {
			selectedHead = &gloasVerificationItem{root: headRoot, block: headBlock}
		}
		visitLimit--
	}
	completedScan := false
	for visited := 0; root != (common.Hash{}) && visited < visitLimit; visited++ {
		block, ok := cfg.forkChoice.GetBlock(root)
		if !ok || block == nil {
			completedScan = true
			break
		}
		if block.Block.Slot <= finalizedSlot {
			completedScan = true
			break
		}
		epoch := block.Block.Slot / cfg.beaconCfg.SlotsPerEpoch
		if cfg.beaconCfg.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
			completedScan = true
			break
		}
		if cfg.forkChoice.HasEnvelope(root) && !cfg.forkChoice.IsPayloadVerified(root) {
			blocks = append(blocks, gloasVerificationItem{root: root, block: block})
		}
		root = common.Hash(block.Block.ParentRoot)
	}
	swept := 0
	completeBatch := true
	processItem := func(item gloasVerificationItem) bool {
		if ctx.Err() != nil {
			return false
		}
		if cfg.forkChoice.IsPayloadVerified(item.root) {
			return true
		}
		envelope, err := cfg.forkChoice.ReadEnvelopeFromDisk(item.root)
		if err != nil {
			log.Debug("[chainTipSync] failed to read GLOAS envelope for verification sweep", "slot", item.block.Block.Slot, "blockRoot", item.root, "err", err)
			return continueGloasVerificationAfterItemFailure(ctx, &completeBatch)
		}
		execHash, ok := gloasEnvelopePayloadHash(envelope)
		if !ok {
			log.Warn("[chainTipSync] missing GLOAS envelope payload during verification sweep", "slot", item.block.Block.Slot, "blockRoot", item.root)
			return continueGloasVerificationAfterItemFailure(ctx, &completeBatch)
		}
		if isGloasPayloadKnownInvalid(cfg, envelope) {
			cfg.forkChoice.MarkPayloadInvalid(item.root, execHash)
			return true
		}
		status, err := retryGloasPayloadWithEL(ctx, cfg, item.block, envelope)
		if err != nil {
			log.Warn("[chainTipSync] GLOAS verification sweep NewPayload failed", "slot", item.block.Block.Slot, "blockRoot", item.root, "status", status, "err", err)
		}
		switch status {
		case execution_client.PayloadStatusValidated:
			cfg.forkChoice.MarkPayloadVerified(item.root, execHash)
		case execution_client.PayloadStatusNone, execution_client.PayloadStatusNotValidated:
			cfg.forkChoice.RequeuePendingELPayload(forkchoice.PendingELPayload{Block: item.block, Envelope: envelope})
		case execution_client.PayloadStatusInvalidated:
			cfg.forkChoice.MarkPayloadInvalid(item.root, execHash)
			log.Warn("[chainTipSync] GLOAS verification sweep found invalid payload", "slot", item.block.Block.Slot, "blockRoot", item.root)
		}
		swept++
		return true
	}
	processImmediateGloasVerificationItems(selectedHead, immediateHead, processItem, &completeBatch)
	for _, item := range slices.Backward(blocks) {
		if !processItem(item) {
			completeBatch = false
			break
		}
	}
	if completeBatch {
		if completedScan || root == (common.Hash{}) {
			cfg.gloasVerificationCursor = common.Hash{}
		} else {
			cfg.gloasVerificationCursor = root
		}
	}
	if swept > 0 || len(blocks) >= maxGloasVerificationSweepPerCycle {
		log.Info("[chainTipSync] GLOAS verification sweep", "swept", swept, "queued", len(blocks), "limit", maxGloasVerificationSweepPerCycle)
	}
}

func retryUnverifiedAnchorPayload(ctx context.Context, cfg *Cfg) {
	if ctx.Err() != nil {
		return
	}
	anchorSlot := cfg.forkChoice.AnchorSlot()
	epoch := anchorSlot / cfg.beaconCfg.SlotsPerEpoch
	if cfg.beaconCfg.GetCurrentStateVersion(epoch) < clparams.GloasVersion {
		return
	}
	anchorRoot := cfg.forkChoice.AnchorRoot()
	if anchorRoot == (common.Hash{}) || cfg.forkChoice.IsPayloadVerified(anchorRoot) || !cfg.forkChoice.HasEnvelope(anchorRoot) {
		return
	}
	if status, ok := cfg.forkChoice.GetRecentExecutionPayloadStatusByRoot(anchorRoot); ok && status == execution_client.PayloadStatusInvalidated {
		return
	}
	anchorState, err := cfg.forkChoice.GetStateAtBlockRoot(anchorRoot, true)
	if err != nil || anchorState == nil {
		log.Debug("[chainTipSync] anchor state not available for payload retry", "anchorRoot", anchorRoot, "err", err)
		return
	}
	bid := anchorState.GetLatestExecutionPayloadBid()
	if bid == nil {
		return
	}
	envelope, err := cfg.forkChoice.ReadEnvelopeFromDisk(anchorRoot)
	if err != nil {
		log.Debug("[chainTipSync] failed to read anchor envelope for payload retry", "anchorRoot", anchorRoot, "err", err)
		return
	}
	if err := validateAnchorEnvelope(cfg.beaconCfg, anchorState, anchorRoot, bid, envelope); err != nil {
		log.Warn("[chainTipSync] invalid anchor envelope during payload retry", "anchorRoot", anchorRoot, "err", err)
		return
	}
	status, err := validateAnchorPayloadWithEL(ctx, cfg, bid, envelope)
	if err != nil {
		log.Warn("[chainTipSync] anchor payload NewPayload retry failed", "anchorRoot", anchorRoot, "status", status, "err", err)
	}
	execHash := envelope.Message.Payload.BlockHash
	switch status {
	case execution_client.PayloadStatusValidated:
		cfg.forkChoice.MarkPayloadVerified(anchorRoot, execHash)
	case execution_client.PayloadStatusInvalidated:
		cfg.forkChoice.MarkPayloadInvalid(anchorRoot, execHash)
		log.Warn("[chainTipSync] anchor payload invalidated by EL", "anchorRoot", anchorRoot)
	}
}

func runGloasPayloadRetryPhases(ctx context.Context, budget time.Duration, offset uint32, phases ...func(context.Context)) {
	if len(phases) == 0 {
		return
	}
	parentCtx, cancelParent := context.WithTimeout(ctx, budget)
	defer cancelParent()
	for i := range phases {
		if parentCtx.Err() != nil {
			return
		}
		phases[(int(offset)+i)%len(phases)](parentCtx)
	}
}

// chainTipSync synchronizes the chain tip by fetching blocks from the highest seen block up to the target slot by listening to incoming blocks.
// or by fetching blocks that might have been missed by gossip after a delay.
func chainTipSync(ctx context.Context, logger log.Logger, cfg *Cfg, args Args) error {
	if shouldRecoverMissingEnvelopes(cfg.beaconCfg, args.targetSlot) {
		recoverMissingEnvelopes(ctx, cfg)
	}

	if canValidateGloasPayloads(cfg) {
		offset := cfg.gloasPayloadRetryOffset.Add(1) - 1
		runGloasPayloadRetryPhases(ctx, gloasPayloadRetryBudget, offset,
			func(retryCtx context.Context) {
				cfg.forkChoice.RetryPendingExecutionPayloadEnvelopes(retryCtx, maxPendingGloasPayloadsPerCycle)
			},
			func(retryCtx context.Context) {
				drainPendingGloasPayloads(retryCtx, cfg)
			},
			func(retryCtx context.Context) {
				retryUnverifiedAnchorPayload(retryCtx, cfg)
			},
		)
		if cfg.executionClient.SupportInsertion() {
			if err := cfg.blockCollector.Flush(context.Background()); err != nil {
				log.Warn("[chainTipSync] blockCollector.Flush failed (EL may still be catching up)", "err", err)
			}
		}
	}

	if args.seenSlot >= args.targetSlot {
		// [GLOAS] Wait for the head's execution payload envelope before proceeding to ForkChoice.
		// The block was already processed by gossip during SleepForSlot, but the envelope
		// may still be in-flight. Without this, FCU sends the parent's execution hash.
		headEpoch := args.targetSlot / cfg.beaconCfg.SlotsPerEpoch
		if cfg.beaconCfg.GetCurrentStateVersion(headEpoch) >= clparams.GloasVersion {
			headRoot, _, err := cfg.forkChoice.GetHead(nil)
			if err != nil {
				return err
			}
			headBlock, _ := cfg.forkChoice.GetBlock(headRoot)
			observeSelectedHeadEnvelopeRequest(cfg, headRoot)
			if headRoot != (common.Hash{}) && blockSupportsExecutionPayloadEnvelope(headBlock) && !cfg.forkChoice.HasEnvelope(headRoot) {
				waitForClaimedSelectedHeadEnvelope(ctx, cfg, cfg.forkChoice, func(requestCtx context.Context, roots [][32]byte) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
					return network.RequestEnvelopesFrantically(requestCtx, cfg.rpc, roots)
				}, headRoot, 2*time.Second, canValidateGloasPayloads(cfg))
			}
			if canValidateGloasPayloads(cfg) {
				verifyCtx, cancelVerify := context.WithTimeout(ctx, gloasPayloadRetryBudget)
				verifyUnverifiedGloasPayloads(verifyCtx, cfg)
				cancelVerify()
			}
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

	logger.Debug(
		"waiting for blocks...",
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
