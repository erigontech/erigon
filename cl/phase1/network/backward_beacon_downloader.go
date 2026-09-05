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

package network

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cl/cltypes"
	"github.com/erigontech/erigon/cl/persistence/base_encoding"
	"github.com/erigontech/erigon/cl/persistence/beacon_indicies"
	"github.com/erigontech/erigon/cl/phase1/core/state"
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/cl/transition"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/common/math"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/snapshotsync/freezeblocks"
)

// Whether the reverse downloader arrived at expected height or condition.
// [Modified in Gloas:EIP7732] envelope is non-nil for GLOAS FULL blocks, nil for EMPTY or pre-GLOAS.
type OnNewBlock func(blk *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) (finished bool, err error)

// BlockChecker is an interface for checking if a block exists
type BlockChecker interface {
	HasBlock(blockNumber uint64) bool
}

type BeaconBlockBodyReader interface {
	ReadBlockByRoot(ctx context.Context, tx kv.Tx, root common.Hash) (*cltypes.SignedBeaconBlock, error)
}

type BackwardBeaconDownloader struct {
	ctx                    context.Context
	slotToDownload         atomic.Uint64
	expectedRoot           common.Hash
	rpc                    *rpc.BeaconRpcP2P
	requestBlocksByRange   func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, string, error)
	requestEnvelopes       func(context.Context, [][32]byte, ...*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error)
	engine                 execution_client.ExecutionEngine
	onNewBlock             OnNewBlock
	finished               atomic.Bool
	reqInterval            *time.Ticker
	db                     kv.RwDB
	sn                     *freezeblocks.CaplinSnapshots
	neverSkip              bool
	blockChecker           BlockChecker
	blockReader            BeaconBlockBodyReader
	beaconCfg              *clparams.BeaconChainConfig
	currentSlot            func() uint64
	validateGloasSuccessor func(*cltypes.SignedBeaconBlock) error
	validateGloasEnvelope  func(*cltypes.SignedBeaconBlock, *cltypes.SignedExecutionPayloadEnvelope) error
	// [New in Gloas:EIP7732] highest block from the previous batch, used as lookahead
	// to determine FULL/EMPTY status of the highest block in the current batch.
	prevBatchTopBlock      *cltypes.SignedBeaconBlock
	gloasSuccessorRoot     common.Hash
	gloasSuccessorNext     uint64
	gloasSuccessorFailures int
	httpFallbackURL        string
	httpPreferred          atomic.Bool // set after first HTTP success; skips P2P probing

	consecutiveEnvelopeFailures int

	mu sync.Mutex
}

var (
	errExecutionPayloadEnvelopeNotFound   = errors.New("execution payload envelope not found")
	errCanonicalGloasSuccessorUnavailable = errors.New("canonical GLOAS successor source is not configured")
	errInvalidCanonicalGloasSuccessor     = errors.New("canonical GLOAS successor response is invalid")
	errDisconnectedGloasSuccessorRange    = errors.New("canonical GLOAS successor range is disconnected")
)

const (
	maxConsecutiveEnvelopeFailures = 3
	maxGloasSuccessorFailures      = 3
)

func NewBackwardBeaconDownloader(ctx context.Context, rpc *rpc.BeaconRpcP2P, sn *freezeblocks.CaplinSnapshots, engine execution_client.ExecutionEngine, db kv.RwDB, beaconCfg *clparams.BeaconChainConfig) *BackwardBeaconDownloader {
	b := &BackwardBeaconDownloader{
		ctx:       ctx,
		rpc:       rpc,
		db:        db,
		neverSkip: true,
		engine:    engine,
		sn:        sn,
		beaconCfg: beaconCfg,
		validateGloasEnvelope: func(block *cltypes.SignedBeaconBlock, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
			return ValidateDownloadedGloasEnvelope(beaconCfg, block, envelope)
		},
		reqInterval: time.NewTicker(200 * time.Millisecond),
	}
	if rpc != nil {
		b.requestBlocksByRange = rpc.SendBeaconBlocksByRangeReq
		b.requestEnvelopes = func(ctx context.Context, roots [][32]byte, blocks ...*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, error) {
			return RequestEnvelopesFrantically(ctx, rpc, roots, blocks...)
		}
	}
	return b
}

// NewGloasSuccessorValidator validates a direct successor from an immutable checkpoint state.
func NewGloasSuccessorValidator(anchorState *state.CachingBeaconState, anchorRoot common.Hash) func(*cltypes.SignedBeaconBlock) error {
	return func(block *cltypes.SignedBeaconBlock) error {
		if anchorState == nil {
			return errors.New("nil GLOAS anchor state")
		}
		if block == nil || block.Block == nil || block.Block.Body == nil {
			return errors.New("missing GLOAS successor block")
		}
		if block.Block.ParentRoot != anchorRoot {
			return fmt.Errorf("GLOAS successor parent root mismatch: expected %v, received %v", anchorRoot, block.Block.ParentRoot)
		}
		validationState, err := anchorState.Copy()
		if err != nil {
			return fmt.Errorf("copy GLOAS anchor state: %w", err)
		}
		if err := transition.TransitionState(validationState, block, nil, true); err != nil {
			return fmt.Errorf("transition GLOAS successor: %w", err)
		}
		return nil
	}
}

func ValidateGloasEnvelopeAgainstBid(beaconCfg *clparams.BeaconChainConfig, blockRoot common.Hash, bid *cltypes.ExecutionPayloadBid, env *cltypes.SignedExecutionPayloadEnvelope) error {
	if beaconCfg == nil {
		return errors.New("nil beacon chain config")
	}
	if bid == nil {
		return errors.New("nil execution payload bid")
	}
	if env == nil || env.Message == nil || env.Message.Payload == nil {
		return errors.New("nil execution payload envelope")
	}
	envelope := env.Message
	payload := envelope.Payload
	if envelope.BeaconBlockRoot != blockRoot {
		return fmt.Errorf("beacon block root mismatch: envelope=%v block=%v", envelope.BeaconBlockRoot, blockRoot)
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
	header, err := payload.RlpHeader(&envelope.ParentBeaconBlockRoot, requestsHash, nil)
	if err != nil {
		return fmt.Errorf("payload header: %w", err)
	}
	if header.Hash() != payload.BlockHash {
		return fmt.Errorf("payload block hash mismatch: header=%v payload=%v", header.Hash(), payload.BlockHash)
	}
	return nil
}

func ValidateDownloadedGloasEnvelope(beaconCfg *clparams.BeaconChainConfig, block *cltypes.SignedBeaconBlock, env *cltypes.SignedExecutionPayloadEnvelope) error {
	if block == nil || block.Block == nil || block.Block.Body == nil {
		return errors.New("nil Gloas beacon block")
	}
	signedBid := block.Block.Body.GetSignedExecutionPayloadBid()
	if signedBid == nil || signedBid.Message == nil {
		return errors.New("nil signed execution payload bid")
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return fmt.Errorf("beacon block root: %w", err)
	}
	return ValidateGloasEnvelopeAgainstBid(beaconCfg, blockRoot, signedBid.Message, env)
}

// SetThrottle sets the throttle.
func (b *BackwardBeaconDownloader) SetThrottle(throttle time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.reqInterval.Reset(throttle)
}

// SetSlotToDownload sets slot to download.
func (b *BackwardBeaconDownloader) SetSlotToDownload(slot uint64) {
	b.slotToDownload.Store(slot)
}

// SetCurrentSlotSampler limits successor searches to slots that can already exist.
func (b *BackwardBeaconDownloader) SetCurrentSlotSampler(currentSlot func() uint64) {
	b.currentSlot = currentSlot
}

func (b *BackwardBeaconDownloader) SetGloasSuccessorValidator(validate func(*cltypes.SignedBeaconBlock) error) {
	b.validateGloasSuccessor = validate
}

// SetExpectedRoot sets the expected root we expect to download.
func (b *BackwardBeaconDownloader) SetExpectedRoot(root common.Hash) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.setExpectedRoot(root)
}

func (b *BackwardBeaconDownloader) setExpectedRoot(root common.Hash) {
	if b.expectedRoot != root {
		b.consecutiveEnvelopeFailures = 0
	}
	b.expectedRoot = root
}

// SetExpectedRoot sets the expected root we expect to download.
func (b *BackwardBeaconDownloader) SetNeverSkip(neverSkip bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.neverSkip = neverSkip
}

// SetBlockChecker sets the block checker for skipping already downloaded blocks
func (b *BackwardBeaconDownloader) SetBlockChecker(checker BlockChecker) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blockChecker = checker
}

func (b *BackwardBeaconDownloader) SetBlockReader(reader BeaconBlockBodyReader) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.blockReader = reader
}

// SetHTTPFallbackURL sets the beacon API base URL for HTTP-based block fetching
// when P2P blocks_by_range requests fail. Derived from the checkpoint sync URL.
func (b *BackwardBeaconDownloader) SetHTTPFallbackURL(checkpointSyncURL string) {
	if checkpointSyncURL == "" {
		return
	}
	before, _, found := strings.Cut(checkpointSyncURL, "/eth/")
	if !found {
		// URL is already a base URL without path (e.g. https://beacon.example.io).
		b.httpFallbackURL = strings.TrimRight(checkpointSyncURL, "/")
		return
	}
	b.httpFallbackURL = before
}

// SetShouldStopAtFn sets the stop condition.
func (b *BackwardBeaconDownloader) SetOnNewBlock(onNewBlock OnNewBlock) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.onNewBlock = onNewBlock
}

func (b *BackwardBeaconDownloader) RPC() *rpc.BeaconRpcP2P {
	return b.rpc
}

// HighestProcessedRoot returns the highest processed block root so far.
func (b *BackwardBeaconDownloader) Finished() bool { return b.finished.Load() }

// Progress current progress.
func (b *BackwardBeaconDownloader) Progress() uint64 {
	// Skip if it is not downloading or limit was reached
	return b.slotToDownload.Load()
}

// Peers returns the current number of peers connected to the BackwardBeaconDownloader.
func (b *BackwardBeaconDownloader) Peers() (uint64, error) {
	return b.rpc.Peers()
}

// RequestMore downloads a range of blocks in a backward manner.
// It requests blocks, processes them in reverse order via the onNewBlock callback,
// and rejects blocks whose root hash doesn't match the expected root.
func (b *BackwardBeaconDownloader) RequestMore(ctx context.Context) error {
	responses, err := b.fetchBlockRange(ctx)
	if err != nil {
		return err
	}

	if err := b.processResponses(ctx, responses); err != nil {
		canSkip := errors.Is(err, errCanonicalGloasSuccessorUnavailable) || errors.Is(err, errInvalidCanonicalGloasSuccessor)
		if !canSkip || !b.neverSkip {
			return err
		}
		expectedRoot, slotToDownload := b.expectedRoot, b.slotToDownload.Load()
		if skipErr := b.trySkipToExistingBlock(ctx); skipErr != nil {
			return skipErr
		}
		if b.expectedRoot == expectedRoot && b.slotToDownload.Load() == slotToDownload {
			return err
		}
		return nil
	}

	if !b.neverSkip {
		return nil
	}

	return b.trySkipToExistingBlock(ctx)
}

// fetchBlockRange requests a range of blocks from peers and waits for a response.
// Falls back to the beacon API when P2P is unavailable and an HTTP URL is configured.
func (b *BackwardBeaconDownloader) fetchBlockRange(ctx context.Context) ([]*cltypes.SignedBeaconBlock, error) {
	const count = uint64(64)
	start, underflow := math.SafeSub(b.slotToDownload.Load(), count-1)
	if underflow {
		start = 0
	}

	// Fast path: when HTTP has been working, skip P2P probing entirely.
	if b.httpPreferred.Load() && b.httpFallbackURL != "" {
		blocks, err := fetchBlocksFromBeaconAPI(ctx, b.httpFallbackURL, start, count, b.beaconCfg)
		if err == nil && len(blocks) > 0 {
			log.Debug("[BackwardBeaconDownloader] fetched blocks from beacon API", "fromSlot", start, "count", len(blocks))
			return blocks, nil
		}
		// HTTP failed — fall back to P2P probing.
		b.httpPreferred.Store(false)
	}

	// Buffered channel prevents goroutine leaks
	received := make(chan []*cltypes.SignedBeaconBlock, 1)
	var requestSent atomic.Bool

	p2pDeadline := time.NewTimer(10 * time.Second)
	defer p2pDeadline.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()

		case <-b.reqInterval.C:
			if requestSent.Swap(true) {
				continue // request already in flight
			}
			go b.sendBlockRequest(ctx, start, count, received, &requestSent)

		case responses := <-received:
			return responses, nil

		case <-p2pDeadline.C:
			if b.httpFallbackURL == "" {
				p2pDeadline.Reset(10 * time.Second)
				continue
			}
			blocks, err := fetchBlocksFromBeaconAPI(ctx, b.httpFallbackURL, start, count, b.beaconCfg)
			if err == nil && len(blocks) > 0 {
				log.Debug("[BackwardBeaconDownloader] P2P failed, fetched blocks from beacon API", "fromSlot", start, "count", len(blocks))
				b.httpPreferred.Store(true)
				return blocks, nil
			}
			if err != nil {
				log.Debug("[BackwardBeaconDownloader] HTTP fallback also failed", "err", err)
			}
			p2pDeadline.Reset(10 * time.Second)
		}
	}
}

// sendBlockRequest sends a block range request and writes the result to the channel.
func (b *BackwardBeaconDownloader) sendBlockRequest(
	ctx context.Context,
	start, count uint64,
	received chan<- []*cltypes.SignedBeaconBlock,
	requestSent *atomic.Bool,
) {
	if b.requestBlocksByRange == nil {
		requestSent.Store(false)
		return
	}
	blocks, peerId, err := b.requestBlocksByRange(ctx, start, count)
	if err != nil {
		// Don't ban when the error is due to no peers being available.
		if !errors.Is(err, peers.ErrNoPeers) && b.rpc != nil {
			b.rpc.BanPeer(peerId)
		} else {
			log.Debug("[Caplin] no peers available for backward beacon block request", "start", start, "count", count)
		}
		requestSent.Store(false)
		return
	}
	if len(blocks) == 0 {
		if b.rpc != nil {
			b.rpc.BanPeer(peerId)
		}
		requestSent.Store(false)
		return
	}

	select {
	case received <- blocks:
	default:
		// Response already received, discard
	}
}

// processResponses processes downloaded blocks in reverse order.
func (b *BackwardBeaconDownloader) processResponses(ctx context.Context, responses []*cltypes.SignedBeaconBlock) error {
	// [New in Gloas:EIP7732] Fetch envelopes for GLOAS FULL blocks before processing.
	log.Debug("[BackwardBeaconDownloader] processResponses start", "blocks", len(responses), "slotToDownload", b.slotToDownload.Load(), "expectedRoot", b.expectedRoot)
	expectedBlock := blockWithRoot(responses, b.expectedRoot)
	if expectedBlock != nil && expectedBlock.Version() >= clparams.GloasVersion {
		if b.prevBatchTopBlock != nil && !isDirectSuccessor(expectedBlock, b.prevBatchTopBlock) {
			b.clearGloasSuccessor()
		}
		if b.prevBatchTopBlock == nil {
			successor, err := b.fetchGloasSuccessor(ctx, expectedBlock)
			if err != nil {
				if errors.Is(err, errCanonicalGloasSuccessorUnavailable) || errors.Is(err, errInvalidCanonicalGloasSuccessor) {
					return err
				}
				b.httpPreferred.Store(false)
				log.Debug("[BackwardBeaconDownloader] initial GLOAS successor fetch failed", "root", b.expectedRoot, "err", err)
				return nil
			}
			if successor == nil {
				return nil
			}
			b.prevBatchTopBlock = successor
		}
	}
	canonicalResponses := canonicalBackwardResponses(responses, b.expectedRoot)
	envelopes, fullRootSet := b.fetchGloasEnvelopes(ctx, canonicalResponses)
	log.Debug("[BackwardBeaconDownloader] envelopes fetched", "count", len(envelopes), "fullRoots", len(fullRootSet))

	matched := false
	for _, block := range slices.Backward(responses) {
		if b.finished.Load() {
			return nil
		}

		blockRoot, err := block.Block.HashSSZ()
		if err != nil {
			log.Debug("Could not compute block root", "err", err)
			continue
		}

		if blockRoot != b.expectedRoot {
			log.Trace("[BackwardBeaconDownloader] root mismatch", "slot", block.Block.Slot, "got", common.Hash(blockRoot), "expected", b.expectedRoot)
			continue
		}
		matched = true

		var envelope *cltypes.SignedExecutionPayloadEnvelope
		if envelopes != nil {
			envelope = envelopes[common.Hash(blockRoot)]
		}

		if _, isFull := fullRootSet[common.Hash(blockRoot)]; isFull && envelope == nil {
			b.httpPreferred.Store(false)
			log.Warn("[BackwardBeaconDownloader] GLOAS FULL block envelope missing, will retry",
				"slot", block.Block.Slot, "consecutiveFailures", b.consecutiveEnvelopeFailures)
			return b.requiredEnvelopeRetryError(block.Block.Slot, blockRoot)
		}

		finished, err := b.onNewBlock(block, envelope)
		if err != nil {
			b.httpPreferred.Store(false)
			log.Warn("Error processing block", "err", err)
			return nil
		}
		b.setExpectedRoot(block.Block.ParentRoot)
		b.prevBatchTopBlock = block
		if block.Block.Slot == 0 {
			b.finished.Store(true)
			return nil
		}
		b.slotToDownload.Store(block.Block.Slot - 1)
		b.finished.Store(finished)
	}

	if !matched {
		log.Debug("[BackwardBeaconDownloader] no root match in batch", "expectedRoot", b.expectedRoot, "responses", len(responses))
	}

	// When slot-based fetching found no match, the expected block may be on the
	// finalized chain while the beacon API serves HEAD-chain blocks by slot.
	// Fall back to fetching by root hash which works regardless of fork choice.
	if !matched && !b.finished.Load() && b.httpFallbackURL != "" {
		block, err := fetchBlockFromBeaconAPIByRoot(ctx, b.httpFallbackURL, b.expectedRoot, b.beaconCfg)
		if err != nil {
			log.Debug("[BackwardBeaconDownloader] root-based HTTP fallback failed", "root", b.expectedRoot, "err", err)
		} else if block != nil {
			blockRoot, err := block.Block.HashSSZ()
			if err == nil && blockRoot == b.expectedRoot {
				log.Debug("[BackwardBeaconDownloader] block matched via root lookup", "slot", block.Block.Slot, "root", common.Hash(blockRoot))

				var envelope *cltypes.SignedExecutionPayloadEnvelope
				if block.Version() >= clparams.GloasVersion {
					if b.prevBatchTopBlock != nil && !isDirectSuccessor(block, b.prevBatchTopBlock) {
						b.clearGloasSuccessor()
					}
					if b.prevBatchTopBlock == nil {
						successor, successorErr := b.fetchGloasSuccessor(ctx, block)
						if successorErr != nil {
							if errors.Is(successorErr, errCanonicalGloasSuccessorUnavailable) || errors.Is(successorErr, errInvalidCanonicalGloasSuccessor) {
								return successorErr
							}
							b.httpPreferred.Store(false)
							log.Debug("[BackwardBeaconDownloader] root-fetched GLOAS successor fetch failed", "root", b.expectedRoot, "err", successorErr)
							return nil
						}
						if successor == nil {
							return nil
						}
						b.prevBatchTopBlock = successor
					}
					if len(determineGloasFullRoots([]*cltypes.SignedBeaconBlock{block}, b.prevBatchTopBlock)) > 0 {
						env, fetchErr := b.fetchSingleEnvelope(ctx, block)
						if fetchErr != nil {
							b.consecutiveEnvelopeFailures++
							b.httpPreferred.Store(false)
							log.Warn("[BackwardBeaconDownloader] GLOAS envelope fetch failed for root-fetched block, will retry",
								"slot", block.Block.Slot, "consecutiveFailures", b.consecutiveEnvelopeFailures, "err", fetchErr)
							return b.requiredEnvelopeRetryError(block.Block.Slot, blockRoot)
						}
						envelope = env
					}
					b.consecutiveEnvelopeFailures = 0
				}

				finished, err := b.onNewBlock(block, envelope)
				if err != nil {
					b.httpPreferred.Store(false)
					log.Warn("Error processing root-fetched block", "err", err)
					return nil
				}
				b.setExpectedRoot(block.Block.ParentRoot)
				b.prevBatchTopBlock = block
				if block.Block.Slot == 0 {
					b.finished.Store(true)
					return nil
				}
				b.slotToDownload.Store(block.Block.Slot - 1)
				b.finished.Store(finished)
			}
		}
	}

	return nil
}

func blockWithRoot(blocks []*cltypes.SignedBeaconBlock, expectedRoot common.Hash) *cltypes.SignedBeaconBlock {
	for _, block := range blocks {
		if block == nil || block.Block == nil {
			continue
		}
		root, err := block.Block.HashSSZ()
		if err == nil && root == expectedRoot {
			return block
		}
	}
	return nil
}

func isDirectSuccessor(block, successor *cltypes.SignedBeaconBlock) bool {
	if block == nil || block.Block == nil || successor == nil || successor.Block == nil {
		return false
	}
	root, err := block.Block.HashSSZ()
	return err == nil && successor.Block.ParentRoot == root
}

func (b *BackwardBeaconDownloader) clearGloasSuccessor() {
	b.prevBatchTopBlock = nil
	b.gloasSuccessorRoot = common.Hash{}
	b.gloasSuccessorNext = 0
	b.gloasSuccessorFailures = 0
}

func canonicalBackwardResponses(responses []*cltypes.SignedBeaconBlock, expectedRoot common.Hash) []*cltypes.SignedBeaconBlock {
	byRoot := make(map[common.Hash]*cltypes.SignedBeaconBlock, len(responses))
	for _, block := range responses {
		if block == nil || block.Block == nil {
			continue
		}
		root, err := block.Block.HashSSZ()
		if err == nil {
			byRoot[root] = block
		}
	}

	canonical := make([]*cltypes.SignedBeaconBlock, 0, len(responses))
	for block := byRoot[expectedRoot]; block != nil; block = byRoot[block.Block.ParentRoot] {
		canonical = append(canonical, block)
	}
	slices.Reverse(canonical)
	return canonical
}

const backwardGloasSuccessorSearchBatch = uint64(64)

func (b *BackwardBeaconDownloader) fetchGloasSuccessor(ctx context.Context, block *cltypes.SignedBeaconBlock) (*cltypes.SignedBeaconBlock, error) {
	if block == nil || block.Block == nil {
		return nil, errors.New("cannot fetch successor for nil block")
	}
	if b.validateGloasSuccessor == nil {
		return nil, nil
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return nil, fmt.Errorf("block root: %w", err)
	}
	if b.gloasSuccessorRoot != blockRoot {
		b.gloasSuccessorRoot = blockRoot
		b.gloasSuccessorNext = saturatingIncrement(block.Block.Slot)
		b.gloasSuccessorFailures = 0
	}
	start := b.gloasSuccessorNext
	if start == block.Block.Slot {
		return nil, nil
	}
	count := backwardGloasSuccessorSearchBatch
	completedRange := false
	if b.currentSlot != nil {
		currentSlot := b.currentSlot()
		if start >= currentSlot {
			start = saturatingIncrement(block.Block.Slot)
			b.gloasSuccessorNext = start
			if start >= currentSlot {
				return nil, nil
			}
		}
		count = min(count, currentSlot-start)
		completedRange = true
	}
	successor, err := b.fetchGloasSuccessorRange(ctx, start, count, blockRoot)
	if err != nil {
		if errors.Is(err, errDisconnectedGloasSuccessorRange) {
			b.gloasSuccessorFailures++
			if b.gloasSuccessorFailures >= maxGloasSuccessorFailures {
				return nil, fmt.Errorf("%w after %d attempts: %w", errInvalidCanonicalGloasSuccessor, b.gloasSuccessorFailures, err)
			}
		}
		return nil, err
	}
	if successor == nil {
		if completedRange {
			b.gloasSuccessorNext = nextSlotAfterRange(start, count)
		}
		return nil, nil
	}
	b.gloasSuccessorRoot = common.Hash{}
	b.gloasSuccessorNext = 0
	b.gloasSuccessorFailures = 0
	return successor, nil
}

func (b *BackwardBeaconDownloader) fetchGloasSuccessorRange(ctx context.Context, start, count uint64, parentRoot common.Hash) (*cltypes.SignedBeaconBlock, error) {
	if count == 0 {
		return nil, nil
	}
	if b.httpFallbackURL == "" {
		return nil, errCanonicalGloasSuccessorUnavailable
	}
	blocks, err := fetchBlocksFromBeaconAPI(ctx, b.httpFallbackURL, start, count, b.beaconCfg)
	if err != nil {
		return nil, err
	}
	successor, err := linkedGloasSuccessor(blocks, start, count, parentRoot)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", errDisconnectedGloasSuccessorRange, err)
	}
	if successor == nil {
		return nil, nil
	}
	if err := b.validateGloasSuccessor(successor); err != nil {
		return nil, fmt.Errorf("%w: %w", errInvalidCanonicalGloasSuccessor, err)
	}
	return successor, nil
}

func linkedGloasSuccessor(blocks []*cltypes.SignedBeaconBlock, start, count uint64, parentRoot common.Hash) (*cltypes.SignedBeaconBlock, error) {
	for _, block := range blocks {
		if block == nil || block.Block == nil {
			return nil, errors.New("successor response contains a nil block")
		}
		if block.Block.Slot < start || block.Block.Slot >= nextSlotAfterRange(start, count) {
			return nil, fmt.Errorf("successor slot %d is outside requested range [%d,%d)", block.Block.Slot, start, nextSlotAfterRange(start, count))
		}
	}
	return connectedGloasSuccessor(blocks, parentRoot)
}

func connectedGloasSuccessor(blocks []*cltypes.SignedBeaconBlock, parentRoot common.Hash) (*cltypes.SignedBeaconBlock, error) {
	if len(blocks) == 0 {
		return nil, nil
	}
	ordered := append([]*cltypes.SignedBeaconBlock(nil), blocks...)
	slices.SortFunc(ordered, func(left, right *cltypes.SignedBeaconBlock) int {
		return cmp.Compare(left.Block.Slot, right.Block.Slot)
	})
	expectedParent := parentRoot
	for _, block := range ordered {
		if block.Block.ParentRoot != expectedParent {
			return nil, fmt.Errorf("successor chain parent root mismatch: expected %v, received %v", expectedParent, block.Block.ParentRoot)
		}
		root, err := block.Block.HashSSZ()
		if err != nil {
			return nil, fmt.Errorf("successor block root: %w", err)
		}
		expectedParent = root
	}
	return ordered[0], nil
}

// determineGloasFullRoots derives payload status from each block's next canonical child.
func determineGloasFullRoots(responses []*cltypes.SignedBeaconBlock, prevBatchTopBlock *cltypes.SignedBeaconBlock) [][32]byte {
	var fullRoots [][32]byte
	for i, block := range responses {
		if block.Version() < clparams.GloasVersion {
			continue
		}
		bid := block.Block.Body.GetSignedExecutionPayloadBid()
		if bid == nil || bid.Message == nil {
			continue
		}
		// Determine the lookahead block (next higher slot in the chain).
		var lookahead *cltypes.SignedBeaconBlock
		if i+1 < len(responses) {
			lookahead = responses[i+1]
		} else {
			lookahead = prevBatchTopBlock
		}
		if lookahead == nil {
			// No lookahead for the highest block in the first batch: request optimistically.
			root, err := block.Block.HashSSZ()
			if err == nil {
				fullRoots = append(fullRoots, root)
			}
			continue
		}
		nextBid := lookahead.Block.Body.GetSignedExecutionPayloadBid()
		if nextBid != nil && nextBid.Message != nil && nextBid.Message.ParentBlockHash == bid.Message.BlockHash {
			root, err := block.Block.HashSSZ()
			if err == nil {
				fullRoots = append(fullRoots, root)
			}
		}
	}
	return fullRoots
}

// fetchGloasEnvelopes preserves a proven FULL status when its envelope remains unavailable.
func (b *BackwardBeaconDownloader) fetchGloasEnvelopes(ctx context.Context, responses []*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, map[common.Hash]struct{}) {
	if len(responses) == 0 {
		return nil, nil
	}

	fullRoots := determineGloasFullRoots(responses, b.prevBatchTopBlock)

	// Build a set for O(1) lookup by callers.
	fullRootSet := make(map[common.Hash]struct{}, len(fullRoots))
	for _, r := range fullRoots {
		fullRootSet[common.Hash(r)] = struct{}{}
	}
	if len(fullRoots) == 0 {
		return nil, fullRootSet
	}

	// When HTTP has been working, skip the slow P2P envelope fetch entirely.
	if b.httpPreferred.Load() && b.httpFallbackURL != "" {
		envelopes := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, len(fullRoots))
		result := fetchEnvelopesFromBeaconAPI(ctx, b.httpFallbackURL, responses, fullRoots, envelopes, b.beaconCfg)
		b.discardInvalidGloasEnvelopes(responses, envelopes)
		if result.fetched > 0 {
			log.Debug("[BackwardBeaconDownloader] fetched envelopes from beacon API", "count", result.fetched)
		}
		b.recordRequiredEnvelopeAttempt(fullRootSet, envelopes)
		return envelopes, fullRootSet
	}

	var envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	var err error
	if b.requestEnvelopes != nil {
		envelopes, err = b.requestEnvelopes(ctx, fullRoots, responses...)
	} else {
		err = errors.New("no P2P envelope source configured")
	}
	if err != nil {
		log.Debug("[BackwardBeaconDownloader] failed to fetch GLOAS envelopes via P2P", "err", err)
	}
	b.discardInvalidGloasEnvelopes(responses, envelopes)
	// Fill in missing envelopes from the beacon API when an HTTP URL is configured.
	if b.httpFallbackURL != "" && len(envelopes) < len(fullRoots) {
		if envelopes == nil {
			envelopes = make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, len(fullRoots))
		}
		result := fetchEnvelopesFromBeaconAPI(ctx, b.httpFallbackURL, responses, fullRoots, envelopes, b.beaconCfg)
		b.discardInvalidGloasEnvelopes(responses, envelopes)
		if result.fetched > 0 {
			log.Debug("[BackwardBeaconDownloader] fetched envelopes from beacon API", "count", result.fetched)
		}
	}

	b.recordRequiredEnvelopeAttempt(fullRootSet, envelopes)

	return envelopes, fullRootSet
}

func (b *BackwardBeaconDownloader) requiredEnvelopeRetryError(slot uint64, root common.Hash) error {
	if b.consecutiveEnvelopeFailures < maxConsecutiveEnvelopeFailures {
		return nil
	}
	return fmt.Errorf("required GLOAS envelope unavailable after %d attempts: slot %d root %x",
		b.consecutiveEnvelopeFailures, slot, root)
}

func (b *BackwardBeaconDownloader) discardInvalidGloasEnvelopes(blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) {
	if b.validateGloasEnvelope == nil {
		return
	}
	for _, block := range blocks {
		if block == nil || block.Block == nil {
			continue
		}
		root, err := block.Block.HashSSZ()
		if err != nil {
			continue
		}
		envelope := envelopes[root]
		if envelope == nil {
			continue
		}
		if err := b.validateGloasEnvelope(block, envelope); err != nil {
			delete(envelopes, root)
			log.Debug("[BackwardBeaconDownloader] discarded invalid GLOAS envelope", "root", common.Hash(root), "err", err)
		}
	}
}

func (b *BackwardBeaconDownloader) recordRequiredEnvelopeAttempt(fullRootSet map[common.Hash]struct{}, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) {
	if _, required := fullRootSet[b.expectedRoot]; !required {
		return
	}
	if envelopes[b.expectedRoot] == nil {
		b.consecutiveEnvelopeFailures++
		return
	}
	b.consecutiveEnvelopeFailures = 0
}

// trySkipToExistingBlock attempts to skip ahead if the expected block already exists in the database.
func (b *BackwardBeaconDownloader) trySkipToExistingBlock(ctx context.Context) error {
	tx, err := b.db.BeginRw(b.ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	elFrozenBlocks := uint64(math.MaxUint64)
	if b.engine != nil && b.engine.SupportInsertion() {
		elFrozenBlocks = b.engine.FrozenBlocks(ctx)
	}

	clFrozenBlocks := uint64(0)
	if b.sn != nil {
		clFrozenBlocks = b.sn.SegmentsMax()
	}

	refreshTicker := time.NewTicker(5 * time.Second)
	defer refreshTicker.Stop()

	expectedRoot := b.expectedRoot
	slotToDownload := b.slotToDownload.Load()
	prevBatchTopBlock := b.prevBatchTopBlock
	didSkip := false
	for {
		// Periodically refresh frozen block counts
		select {
		case <-refreshTicker.C:
			if b.sn != nil {
				clFrozenBlocks = b.sn.SegmentsMax()
			}
			if b.engine != nil && b.engine.SupportInsertion() {
				elFrozenBlocks = b.engine.FrozenBlocks(ctx)
			}
		default:
		}

		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, expectedRoot)
		if err != nil {
			return err
		}
		if slot == nil || *slot == 0 {
			break
		}

		var skippedBlock *cltypes.SignedBeaconBlock
		isGloas := b.isGloasSlot(*slot)
		if isGloas {
			if b.blockReader == nil {
				break
			}
			skippedBlock, err = b.blockReader.ReadBlockByRoot(b.ctx, tx, expectedRoot)
			if err != nil {
				return err
			}
			if skippedBlock == nil || skippedBlock.Block == nil || skippedBlock.Block.Body == nil || skippedBlock.Version() < clparams.GloasVersion {
				break
			}
			skippedRoot, hashErr := skippedBlock.Block.HashSSZ()
			if hashErr != nil || skippedRoot != expectedRoot {
				break
			}
		}
		canSkip := b.canSkipRoot(ctx, tx, elFrozenBlocks, clFrozenBlocks, *slot, expectedRoot)
		if !canSkip && isGloas && *slot > clFrozenBlocks && prevBatchTopBlock != nil &&
			prevBatchTopBlock.Block != nil && prevBatchTopBlock.Block.Body != nil && prevBatchTopBlock.Block.ParentRoot == expectedRoot {
			bid := skippedBlock.Block.Body.GetSignedExecutionPayloadBid()
			successorBid := prevBatchTopBlock.Block.Body.GetSignedExecutionPayloadBid()
			canSkip = bid != nil && bid.Message != nil && successorBid != nil && successorBid.Message != nil &&
				successorBid.Message.ParentBlockHash != bid.Message.BlockHash
		}
		if !canSkip {
			break
		}

		parentRoot, err := beacon_indicies.ReadParentBlockRoot(b.ctx, tx, expectedRoot)
		if err != nil {
			return err
		}
		if err := beacon_indicies.MarkRootCanonical(b.ctx, tx, *slot, expectedRoot); err != nil {
			return err
		}
		slotToDownload = *slot - 1
		expectedRoot = parentRoot
		didSkip = true
		if skippedBlock != nil {
			prevBatchTopBlock = skippedBlock
		} else {
			prevBatchTopBlock = nil
		}

		// Clean up non-canonical slots
		newSlot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, expectedRoot)
		if err != nil {
			return err
		}
		if newSlot == nil || *newSlot == 0 {
			continue
		}
		for i := *newSlot + 1; i < *slot; i++ {
			if err := tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(i)); err != nil {
				return err
			}
		}
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	if didSkip {
		b.setExpectedRoot(expectedRoot)
		b.slotToDownload.Store(slotToDownload)
		b.prevBatchTopBlock = prevBatchTopBlock
		b.gloasSuccessorRoot = common.Hash{}
		b.gloasSuccessorNext = 0
	}
	return nil
}

func (b *BackwardBeaconDownloader) canSkipRoot(ctx context.Context, tx kv.Tx, elFrozenBlocks, clFrozenBlocks, slot uint64, blockRoot common.Hash) bool {
	if slot <= clFrozenBlocks {
		return false
	}

	var blockHash common.Hash
	isGloas := b.isGloasSlot(slot)
	if isGloas {
		var err error
		blockHash, err = beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		if err != nil {
			log.Warn("Failed to read execution block hash", "err", err)
			return false
		}
		if blockHash == (common.Hash{}) {
			return false
		}
	}

	if b.engine == nil || !b.engine.SupportInsertion() {
		return true
	}

	if !isGloas {
		var err error
		blockHash, err = beacon_indicies.ReadExecutionBlockHash(tx, blockRoot)
		if err != nil {
			log.Warn("Failed to read execution block hash", "err", err)
			return false
		}
		if blockHash == (common.Hash{}) {
			return false
		}
	}

	blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, blockRoot)
	if err != nil {
		log.Warn("Failed to read execution block number", "err", err)
	}
	if err != nil || blockNumber == nil {
		return false
	}

	// Check if block is already in the collector
	if b.blockChecker != nil && b.blockChecker.HasBlock(*blockNumber) {
		return true
	}

	if *blockNumber < elFrozenBlocks {
		return true
	}

	has, err := b.engine.HasBlock(ctx, blockHash)
	return err == nil && has
}

func (b *BackwardBeaconDownloader) isGloasSlot(slot uint64) bool {
	return b.beaconCfg != nil && b.beaconCfg.SlotsPerEpoch != 0 && b.beaconCfg.GetCurrentStateVersion(slot/b.beaconCfg.SlotsPerEpoch) >= clparams.GloasVersion
}

// fetchBlockFromBeaconAPIByRoot fetches a single beacon block by its root hash.
// This is needed when the beacon API's HEAD chain diverges from the finalized chain;
// slot-based queries return HEAD-chain blocks, but root-based queries work regardless.
func fetchBlockFromBeaconAPIByRoot(ctx context.Context, baseURL string, root common.Hash, beaconCfg *clparams.BeaconChainConfig) (*cltypes.SignedBeaconBlock, error) {
	client := &http.Client{Timeout: 10 * time.Second}
	reqURL := fmt.Sprintf("%s/eth/v2/beacon/blocks/0x%x", baseURL, root)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := readBeaconAPIResponseBody(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("block fetch by root: status %d", resp.StatusCode)
	}

	version, err := httpConsensusVersion(resp.Header.Get("Eth-Consensus-Version"))
	if err != nil {
		return nil, err
	}
	block := cltypes.NewSignedBeaconBlock(beaconCfg, version)
	if err := block.DecodeSSZStrict(body, int(version)); err != nil {
		return nil, fmt.Errorf("block decode by root: %w", err)
	}
	if block.Block == nil {
		return nil, errors.New("block fetched by root has no message")
	}
	if err := validateHTTPBlockVersion(beaconCfg, block.Block.Slot, version); err != nil {
		return nil, err
	}
	decodedRoot, err := block.Block.HashSSZ()
	if err != nil {
		return nil, fmt.Errorf("block root: %w", err)
	}
	if decodedRoot != root {
		return nil, fmt.Errorf("block root mismatch: requested %v, received %v", root, decodedRoot)
	}
	return block, nil
}

// fetchSingleEnvelope fetches the execution payload envelope for a single GLOAS block.
func (b *BackwardBeaconDownloader) fetchSingleEnvelope(ctx context.Context, block *cltypes.SignedBeaconBlock) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	var p2pErr error
	p2pAttempted := false
	if !b.httpPreferred.Load() {
		p2pAttempted = true
		if envelope, err := b.fetchSingleEnvelopeP2P(ctx, block); err == nil {
			return envelope, nil
		} else {
			p2pErr = err
		}
	}
	envelope, httpErr := b.fetchSingleEnvelopeHTTP(ctx, block)
	if httpErr == nil {
		return envelope, nil
	}
	if !p2pAttempted {
		if envelope, err := b.fetchSingleEnvelopeP2P(ctx, block); err == nil {
			return envelope, nil
		} else {
			p2pErr = err
		}
	}
	return nil, errors.Join(httpErr, p2pErr)
}

func (b *BackwardBeaconDownloader) fetchSingleEnvelopeP2P(ctx context.Context, block *cltypes.SignedBeaconBlock) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if b.requestEnvelopes == nil {
		return nil, errors.New("no P2P envelope source configured")
	}
	if block == nil || block.Block == nil {
		return nil, errors.New("cannot fetch envelope for nil block")
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return nil, fmt.Errorf("block root: %w", err)
	}
	envelopes, err := b.requestEnvelopes(ctx, [][32]byte{blockRoot}, block)
	if err != nil {
		return nil, err
	}
	if envelope := envelopes[blockRoot]; envelope != nil && envelope.Message != nil && envelope.Message.BeaconBlockRoot == blockRoot {
		if b.validateGloasEnvelope != nil {
			if err := b.validateGloasEnvelope(block, envelope); err != nil {
				return nil, fmt.Errorf("invalid P2P GLOAS envelope: %w", err)
			}
		}
		return envelope, nil
	}
	return nil, errExecutionPayloadEnvelopeNotFound
}

func (b *BackwardBeaconDownloader) fetchSingleEnvelopeHTTP(ctx context.Context, block *cltypes.SignedBeaconBlock) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if b.httpFallbackURL == "" {
		return nil, fmt.Errorf("no HTTP fallback URL configured")
	}
	if block == nil || block.Block == nil {
		return nil, errors.New("cannot fetch envelope for nil block")
	}
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return nil, fmt.Errorf("block root: %w", err)
	}

	client := &http.Client{Timeout: 10 * time.Second}
	reqURL := fmt.Sprintf("%s/eth/v1/beacon/execution_payload_envelopes/0x%x", b.httpFallbackURL, blockRoot)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := readBeaconAPIResponseBody(resp)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, errExecutionPayloadEnvelopeNotFound
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("envelope fetch: HTTP %d", resp.StatusCode)
	}
	version, err := httpConsensusVersion(resp.Header.Get("Eth-Consensus-Version"))
	if err != nil {
		return nil, err
	}
	if version != clparams.GloasVersion {
		return nil, fmt.Errorf("envelope version mismatch: expected %s, received %s", clparams.GloasVersion, version)
	}
	if err := validateHTTPBlockVersion(b.beaconCfg, block.Block.Slot, version); err != nil {
		return nil, err
	}

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(b.beaconCfg),
	}
	if err := envelope.DecodeSSZStrict(body, int(clparams.GloasVersion)); err != nil {
		return nil, fmt.Errorf("envelope decode: %w", err)
	}
	if envelope.Message == nil || envelope.Message.BeaconBlockRoot != blockRoot {
		return nil, fmt.Errorf("envelope block root mismatch: requested %v", blockRoot)
	}
	if b.validateGloasEnvelope != nil {
		if err := b.validateGloasEnvelope(block, envelope); err != nil {
			return nil, fmt.Errorf("invalid HTTP GLOAS envelope: %w", err)
		}
	}
	return envelope, nil
}
