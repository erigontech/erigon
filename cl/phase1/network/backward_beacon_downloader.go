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
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"math"
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
	"github.com/erigontech/erigon/cl/phase1/execution_client"
	"github.com/erigontech/erigon/cl/rpc"
	"github.com/erigontech/erigon/cl/sentinel/peers"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
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

type BackwardBeaconDownloader struct {
	ctx            context.Context
	slotToDownload atomic.Uint64
	expectedRoot   common.Hash
	rpc            *rpc.BeaconRpcP2P
	engine         execution_client.ExecutionEngine
	onNewBlock     OnNewBlock
	finished       atomic.Bool
	reqInterval    *time.Ticker
	db             kv.RwDB
	sn             *freezeblocks.CaplinSnapshots
	neverSkip      bool
	blockChecker   BlockChecker
	beaconCfg      *clparams.BeaconChainConfig
	// [New in Gloas:EIP7732] highest block from the previous batch, used as lookahead
	// to determine FULL/EMPTY status of the highest block in the current batch.
	prevBatchTopBlock            *cltypes.SignedBeaconBlock
	httpFallbackURL              string      // beacon API base URL for HTTP fallback when P2P fails
	httpPreferred                atomic.Bool // set after first HTTP success; skips P2P probing
	consecutiveLookaheadFailures uint8
	lookaheadSearchOffset        uint64
	lookaheadRescan              bool
	lookaheadAnchorRoot          common.Hash

	// Count consecutive batches where envelope fetch returned 0 for all FULL roots.
	// After enough failures, skip envelope requirements and process blocks as EMPTY.
	consecutiveEnvelopeFailures int
	envelopesSkipped            bool // set when we give up on envelopes

	// FULL blocks that were processed without envelopes due to envelopesSkipped.
	// Collected for post-download recovery.
	skippedFullBlocks []SkippedFullBlock

	mu sync.Mutex
}

const (
	gloasLookaheadWindow      = uint64(64)
	maxSkippedFullBlocks      = 65536
	maxBeaconAPIResponseBytes = 64 << 20
)

// SkippedFullBlock records a GLOAS FULL block whose envelope was unavailable during backward download.
type SkippedFullBlock struct {
	Slot uint64
	Root [32]byte
}

func NewBackwardBeaconDownloader(ctx context.Context, rpc *rpc.BeaconRpcP2P, sn *freezeblocks.CaplinSnapshots, engine execution_client.ExecutionEngine, db kv.RwDB, beaconCfg *clparams.BeaconChainConfig) *BackwardBeaconDownloader {
	return &BackwardBeaconDownloader{
		ctx:         ctx,
		rpc:         rpc,
		db:          db,
		reqInterval: time.NewTicker(200 * time.Millisecond),
		neverSkip:   true,
		engine:      engine,
		sn:          sn,
		beaconCfg:   beaconCfg,
	}
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

// SetExpectedRoot sets the expected root we expect to download.
func (b *BackwardBeaconDownloader) SetExpectedRoot(root common.Hash) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
		return err
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
	start := b.slotToDownload.Load() - count + 1
	if start > b.slotToDownload.Load() { // overflow check
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
	blocks, peerId, err := b.rpc.SendBeaconBlocksByRangeReq(ctx, start, count)
	if err != nil {
		// Don't ban when the error is due to no peers being available.
		if !errors.Is(err, peers.ErrNoPeers) {
			b.rpc.BanPeer(peerId)
		} else {
			log.Debug("[Caplin] no peers available for backward beacon block request", "start", start, "count", count)
		}
		requestSent.Store(false)
		return
	}
	if blocks == nil || len(blocks) == 0 {
		b.rpc.BanPeer(peerId)
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
	if err := b.prepareFirstBatchLookahead(ctx, responses); err != nil {
		log.Warn("[BackwardBeaconDownloader] GLOAS lookahead unavailable", "err", err)
		b.waitBeforeLookaheadRetry(ctx)
		return nil
	}

	// [New in Gloas:EIP7732] Fetch envelopes for GLOAS FULL blocks before processing.
	log.Debug("[BackwardBeaconDownloader] processResponses start", "blocks", len(responses), "slotToDownload", b.slotToDownload.Load(), "expectedRoot", b.expectedRoot)
	envelopes, fullRootSet, knownRootSet := b.fetchGloasEnvelopes(ctx, responses)
	log.Debug("[BackwardBeaconDownloader] envelopes fetched", "count", len(envelopes), "fullRoots", len(fullRootSet))

	// Track whether any block was successfully processed. Only update
	// prevBatchTopBlock when we advance, to avoid corrupting the lookahead
	// when a retry causes the same batch to be re-fetched.
	advanced := false
	matched := false
	for _, block := range slices.Backward(responses) {
		if b.finished.Load() {
			return nil
		}
		if block == nil || block.Block == nil || block.Block.Body == nil {
			log.Debug("[BackwardBeaconDownloader] ignoring incomplete block response")
			continue
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
		if block.Version() >= clparams.GloasVersion {
			if _, known := knownRootSet[common.Hash(blockRoot)]; !known {
				log.Warn("[BackwardBeaconDownloader] GLOAS block availability unknown, will retry", "slot", block.Block.Slot)
				return nil
			}
			if envelope != nil {
				if err := ValidateFetchedEnvelope(b.beaconCfg, block, common.Hash(blockRoot), envelope); err != nil {
					log.Warn("[BackwardBeaconDownloader] GLOAS envelope does not match block, will retry", "slot", block.Block.Slot, "err", err)
					return nil
				}
			}
		}

		// A FULL block whose envelope could not be fetched must not be treated as
		// EMPTY — unless we've exhausted retries (envelopesSkipped is set when
		// consecutive batches fail envelope fetch entirely).
		if _, isFull := fullRootSet[common.Hash(blockRoot)]; isFull && envelope == nil && (!b.envelopesSkipped || !b.canTrackSkippedFullBlock(block)) {
			log.Warn("[BackwardBeaconDownloader] GLOAS FULL block envelope missing, will retry",
				"slot", block.Block.Slot, "consecutiveFailures", b.consecutiveEnvelopeFailures)
			return nil
		}

		finished, err := b.onNewBlock(block, envelope)
		b.finished.Store(finished)
		if err != nil {
			log.Warn("Error processing block", "err", err)
			continue
		}

		// Record FULL blocks passing through without envelope for post-download recovery.
		if _, isFull := fullRootSet[common.Hash(blockRoot)]; isFull && envelope == nil {
			b.skippedFullBlocks = append(b.skippedFullBlocks, SkippedFullBlock{Slot: block.Block.Slot, Root: blockRoot})
		}

		advanced = true
		b.prevBatchTopBlock = block
		b.expectedRoot = block.Block.ParentRoot
		if block.Block.Slot == 0 {
			b.finished.Store(true)
			b.prevBatchTopBlock = firstCompleteBlock(responses)
			return nil
		}
		b.slotToDownload.Store(block.Block.Slot - 1)
	}

	// Update prevBatchTopBlock only when at least one block was processed,
	// so retries preserve the correct lookahead for FULL/EMPTY determination.
	if !matched {
		log.Debug("[BackwardBeaconDownloader] no root match in batch", "expectedRoot", b.expectedRoot, "responses", len(responses), "advanced", advanced)
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
				isFull := false
				if block.Version() >= clparams.GloasVersion {
					lookahead := b.prevBatchTopBlock
					if _, known := gloasBlockAvailability(block, lookahead); !known {
						lookahead, err = b.fetchGloasLookahead(ctx, block, common.Hash(blockRoot))
						if err != nil {
							log.Warn("[BackwardBeaconDownloader] root-fetched GLOAS lookahead unavailable, will retry", "slot", block.Block.Slot, "err", err)
							return nil
						}
					}
					full, known := gloasBlockAvailability(block, lookahead)
					if !known {
						log.Warn("[BackwardBeaconDownloader] root-fetched GLOAS block availability unknown, will retry", "slot", block.Block.Slot)
						return nil
					}
					isFull = full
					if full {
						env, fetchErr := b.fetchSingleEnvelope(ctx, block)
						if fetchErr == nil && env != nil {
							if err := ValidateFetchedEnvelope(b.beaconCfg, block, common.Hash(blockRoot), env); err == nil {
								envelope = env
							} else {
								log.Warn("[BackwardBeaconDownloader] root-fetched envelope does not match block", "slot", block.Block.Slot, "err", err)
							}
						}
						b.recordEnvelopeFetchResult(1, btoi(envelope != nil))
						if envelope == nil && (!b.envelopesSkipped || !b.canTrackSkippedFullBlock(block)) {
							log.Warn("[BackwardBeaconDownloader] root-fetched FULL block envelope unavailable, will retry",
								"slot", block.Block.Slot, "err", fetchErr, "consecutiveFailures", b.consecutiveEnvelopeFailures)
							return nil
						}
					}
				}

				finished, err := b.onNewBlock(block, envelope)
				b.finished.Store(finished)
				if err != nil {
					log.Warn("Error processing root-fetched block", "err", err)
				} else {
					if isFull && envelope == nil {
						b.skippedFullBlocks = append(b.skippedFullBlocks, SkippedFullBlock{Slot: block.Block.Slot, Root: blockRoot})
					}
					b.prevBatchTopBlock = block
					b.expectedRoot = block.Block.ParentRoot
					if block.Block.Slot == 0 {
						b.finished.Store(true)
						return nil
					}
					b.slotToDownload.Store(block.Block.Slot - 1)
				}
			}
		}
	}

	return nil
}

func btoi(value bool) int {
	if value {
		return 1
	}
	return 0
}

func firstCompleteBlock(responses []*cltypes.SignedBeaconBlock) *cltypes.SignedBeaconBlock {
	for _, block := range responses {
		if block != nil && block.Block != nil && block.Block.Body != nil {
			return block
		}
	}
	return nil
}

func lastCompleteBlock(responses []*cltypes.SignedBeaconBlock) *cltypes.SignedBeaconBlock {
	for _, block := range slices.Backward(responses) {
		if block != nil && block.Block != nil && block.Block.Body != nil {
			return block
		}
	}
	return nil
}

func blockByRoot(responses []*cltypes.SignedBeaconBlock, expectedRoot common.Hash) *cltypes.SignedBeaconBlock {
	for _, block := range responses {
		if block == nil || block.Block == nil || block.Block.Body == nil {
			continue
		}
		root, err := block.Block.HashSSZ()
		if err == nil && root == expectedRoot {
			return block
		}
	}
	return nil
}

func (b *BackwardBeaconDownloader) prepareFirstBatchLookahead(ctx context.Context, responses []*cltypes.SignedBeaconBlock) error {
	if b.prevBatchTopBlock != nil || len(responses) == 0 {
		return nil
	}

	anchor := blockByRoot(responses, b.expectedRoot)
	if anchor == nil {
		return nil
	}
	if anchor.Version() < clparams.GloasVersion {
		return nil
	}

	anchorRoot, err := anchor.Block.HashSSZ()
	if err != nil {
		return fmt.Errorf("hash highest block: %w", err)
	}
	if lookahead := selectGloasLookahead(anchor, anchorRoot, responses); lookahead != nil {
		b.prevBatchTopBlock = lookahead
		b.consecutiveLookaheadFailures = 0
		b.lookaheadSearchOffset = 0
		b.lookaheadRescan = false
		return nil
	}

	lookahead, err := b.fetchGloasLookahead(ctx, anchor, anchorRoot)
	if err != nil {
		return err
	}
	b.prevBatchTopBlock = lookahead
	b.consecutiveLookaheadFailures = 0
	b.lookaheadSearchOffset = 0
	b.lookaheadRescan = false
	return nil
}

func (b *BackwardBeaconDownloader) fetchGloasLookahead(
	ctx context.Context,
	anchor *cltypes.SignedBeaconBlock,
	anchorRoot common.Hash,
) (*cltypes.SignedBeaconBlock, error) {
	if b.lookaheadAnchorRoot != anchorRoot {
		b.lookaheadAnchorRoot = anchorRoot
		b.lookaheadSearchOffset = 0
		b.lookaheadRescan = false
	}
	if anchor.Block.Slot == math.MaxUint64 {
		return nil, errors.New("cannot fetch lookahead after max slot")
	}

	if b.lookaheadSearchOffset > math.MaxUint64-anchor.Block.Slot-1 {
		return nil, errors.New("GLOAS lookahead search overflow")
	}
	offset := b.lookaheadSearchOffset
	if b.lookaheadRescan {
		offset = 0
	}
	start := anchor.Block.Slot + 1 + offset
	sources := make([]gloasLookaheadFetcher, 0, 2)
	if b.httpFallbackURL != "" {
		sources = append(sources, func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, error) {
			return fetchBlocksFromBeaconAPI(ctx, b.httpFallbackURL, start, count, b.beaconCfg)
		})
	}
	if b.rpc != nil {
		sources = append(sources, func(ctx context.Context, start, count uint64) ([]*cltypes.SignedBeaconBlock, error) {
			blocks, _, err := b.rpc.SendBeaconBlocksByRangeReq(ctx, start, count)
			return blocks, err
		})
	}
	if len(sources) == 0 {
		return nil, errors.New("no GLOAS lookahead source configured")
	}
	lookahead, err := fetchGloasLookaheadFromSources(ctx, anchor, anchorRoot, start, sources...)
	if lookahead != nil {
		return lookahead, nil
	}
	b.advanceLookaheadSearch()
	return nil, err
}

type gloasLookaheadFetcher func(context.Context, uint64, uint64) ([]*cltypes.SignedBeaconBlock, error)

func fetchGloasLookaheadFromSources(ctx context.Context, anchor *cltypes.SignedBeaconBlock, anchorRoot common.Hash, start uint64, sources ...gloasLookaheadFetcher) (*cltypes.SignedBeaconBlock, error) {
	errs := make([]error, 0, len(sources))
	for _, fetch := range sources {
		candidates, err := fetch(ctx, start, gloasLookaheadWindow)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		if lookahead := selectGloasLookahead(anchor, anchorRoot, candidates); lookahead != nil {
			return lookahead, nil
		}
		errs = append(errs, errors.New("lookahead source returned no direct child"))
	}
	return nil, errors.Join(errs...)
}

func (b *BackwardBeaconDownloader) advanceLookaheadSearch() {
	if b.lookaheadRescan {
		b.lookaheadRescan = false
		b.lookaheadSearchOffset += gloasLookaheadWindow
		return
	}
	if b.lookaheadSearchOffset == 0 {
		b.lookaheadSearchOffset = gloasLookaheadWindow
		return
	}
	b.lookaheadRescan = true
}

func selectGloasLookahead(
	anchor *cltypes.SignedBeaconBlock,
	anchorRoot common.Hash,
	candidates []*cltypes.SignedBeaconBlock,
) *cltypes.SignedBeaconBlock {
	var selected *cltypes.SignedBeaconBlock
	for _, candidate := range candidates {
		if candidate == nil || candidate.Block == nil || candidate.Block.Body == nil {
			continue
		}
		if candidate.Block.Slot <= anchor.Block.Slot || candidate.Block.ParentRoot != anchorRoot {
			continue
		}
		bid := candidate.Block.Body.GetSignedExecutionPayloadBid()
		if bid == nil || bid.Message == nil {
			continue
		}
		if selected == nil || candidate.Block.Slot < selected.Block.Slot {
			selected = candidate
		}
	}
	return selected
}

func (b *BackwardBeaconDownloader) waitBeforeLookaheadRetry(ctx context.Context) {
	if b.consecutiveLookaheadFailures < 6 {
		b.consecutiveLookaheadFailures++
	}
	delay := time.Second << (b.consecutiveLookaheadFailures - 1)
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

func determineGloasFullRoots(responses []*cltypes.SignedBeaconBlock, prevBatchTopBlock *cltypes.SignedBeaconBlock) [][32]byte {
	anchor := lastCompleteBlock(responses)
	if anchor == nil {
		return nil
	}
	expectedRoot, err := anchor.Block.HashSSZ()
	if err != nil {
		return nil
	}
	fullRoots, _ := determineGloasAvailability(responses, prevBatchTopBlock, expectedRoot)
	return fullRoots
}

func determineGloasAvailability(
	responses []*cltypes.SignedBeaconBlock,
	lookahead *cltypes.SignedBeaconBlock,
	expectedRoot common.Hash,
) ([][32]byte, map[common.Hash]struct{}) {
	var fullRoots [][32]byte
	knownRoots := make(map[common.Hash]struct{})
	for _, block := range slices.Backward(responses) {
		if block == nil || block.Block == nil || block.Block.Body == nil {
			continue
		}
		root, err := block.Block.HashSSZ()
		if err != nil || root != expectedRoot {
			continue
		}
		if block.Version() < clparams.GloasVersion {
			lookahead = block
			expectedRoot = block.Block.ParentRoot
			continue
		}
		full, known := gloasBlockAvailability(block, lookahead)
		if known {
			knownRoots[common.Hash(root)] = struct{}{}
			if full {
				fullRoots = append(fullRoots, root)
			}
		}
		lookahead = block
		expectedRoot = block.Block.ParentRoot
	}
	return fullRoots, knownRoots
}

func gloasBlockAvailability(block, lookahead *cltypes.SignedBeaconBlock) (bool, bool) {
	if block == nil || block.Block == nil || block.Block.Body == nil || lookahead == nil || lookahead.Block == nil || lookahead.Block.Body == nil {
		return false, false
	}
	root, err := block.Block.HashSSZ()
	if err != nil || lookahead.Block.Slot <= block.Block.Slot || lookahead.Block.ParentRoot != root {
		return false, false
	}
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	nextBid := lookahead.Block.Body.GetSignedExecutionPayloadBid()
	if bid == nil || bid.Message == nil || nextBid == nil || nextBid.Message == nil {
		return false, false
	}
	return nextBid.Message.ParentBlockHash == bid.Message.BlockHash, true
}

// fetchGloasEnvelopes determines which GLOAS blocks in the batch are FULL and fetches their envelopes.
// It returns the envelopes map and a set of block roots that were determined FULL by lookahead.
// Callers must check: if a root is in fullRootSet but missing from envelopes, the fetch failed
// and the block must NOT be treated as EMPTY.
func (b *BackwardBeaconDownloader) fetchGloasEnvelopes(ctx context.Context, responses []*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, map[common.Hash]struct{}, map[common.Hash]struct{}) {
	if len(responses) == 0 {
		return nil, nil, nil
	}

	fullRoots, knownRootSet := determineGloasAvailability(responses, b.prevBatchTopBlock, b.expectedRoot)

	// Build a set for O(1) lookup by callers.
	fullRootSet := make(map[common.Hash]struct{}, len(fullRoots))
	for _, r := range fullRoots {
		fullRootSet[common.Hash(r)] = struct{}{}
	}

	if len(fullRoots) == 0 {
		return nil, fullRootSet, knownRootSet
	}

	var envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope
	if b.httpPreferred.Load() && b.httpFallbackURL != "" {
		envelopes = validateAndFetchMissingEnvelopes(ctx, b.httpFallbackURL, responses, fullRoots, nil, b.beaconCfg)
	} else {
		var err error
		envelopes, err = RequestEnvelopesFrantically(ctx, b.rpc, fullRoots)
		if err != nil {
			log.Debug("[BackwardBeaconDownloader] failed to fetch GLOAS envelopes via P2P", "err", err)
		}
		envelopes = validateAndFetchMissingEnvelopes(ctx, b.httpFallbackURL, responses, fullRoots, envelopes, b.beaconCfg)
	}

	b.recordEnvelopeFetchResult(len(fullRoots), len(envelopes))

	return envelopes, fullRootSet, knownRootSet
}

func validateFetchedEnvelopes(beaconCfg *clparams.BeaconChainConfig, blocks []*cltypes.SignedBeaconBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope {
	valid := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, len(envelopes))
	for _, block := range blocks {
		if block == nil || block.Block == nil || block.Block.Body == nil {
			continue
		}
		root, err := block.Block.HashSSZ()
		if err != nil {
			continue
		}
		envelope := envelopes[common.Hash(root)]
		if ValidateFetchedEnvelope(beaconCfg, block, common.Hash(root), envelope) == nil {
			valid[common.Hash(root)] = envelope
		}
	}
	return valid
}

func (b *BackwardBeaconDownloader) recordEnvelopeFetchResult(requested, received int) {
	if received < requested {
		b.consecutiveEnvelopeFailures++
		const maxConsecutiveFailures = 3
		if b.consecutiveEnvelopeFailures >= maxConsecutiveFailures && !b.envelopesSkipped {
			b.envelopesSkipped = true
			log.Warn("[BackwardBeaconDownloader] too many consecutive envelope failures, treating FULL blocks as EMPTY",
				"consecutiveFailures", b.consecutiveEnvelopeFailures)
		}
		return
	}
	b.consecutiveEnvelopeFailures = 0
	b.envelopesSkipped = false
}

// SkippedFullBlocks returns FULL blocks that were processed without envelopes
// due to consecutive fetch failures during backward download.
func (b *BackwardBeaconDownloader) SkippedFullBlocks() []SkippedFullBlock {
	return b.skippedFullBlocks
}

func (b *BackwardBeaconDownloader) canTrackSkippedFullBlock(block *cltypes.SignedBeaconBlock) bool {
	return block != nil && len(b.skippedFullBlocks) < maxSkippedFullBlocks
}

func ValidateFetchedEnvelope(beaconCfg *clparams.BeaconChainConfig, block *cltypes.SignedBeaconBlock, blockRoot common.Hash, envelope *cltypes.SignedExecutionPayloadEnvelope) error {
	if block == nil || block.Block == nil || block.Block.Body == nil || envelope == nil || envelope.Message == nil || envelope.Message.Payload == nil || envelope.Message.ExecutionRequests == nil {
		return errors.New("incomplete block or envelope")
	}
	if envelope.Message.BeaconBlockRoot != blockRoot {
		return fmt.Errorf("envelope beacon root %v != block root %v", envelope.Message.BeaconBlockRoot, blockRoot)
	}
	payload := envelope.Message.Payload
	if payload.SlotNumber != block.Block.Slot {
		return fmt.Errorf("envelope slot %d != block slot %d", envelope.Message.Payload.SlotNumber, block.Block.Slot)
	}
	bid := block.Block.Body.GetSignedExecutionPayloadBid()
	if bid == nil || bid.Message == nil {
		return errors.New("block missing execution payload bid")
	}
	committed := bid.Message
	if envelope.Message.ParentBeaconBlockRoot != committed.ParentBlockRoot || envelope.Message.BuilderIndex != committed.BuilderIndex {
		return errors.New("envelope metadata does not match committed bid")
	}
	if payload.BlockHash != committed.BlockHash || payload.ParentHash != committed.ParentBlockHash || payload.PrevRandao != committed.PrevRandao || payload.FeeRecipient != committed.FeeRecipient || payload.GasLimit != committed.GasLimit || payload.SlotNumber != committed.Slot {
		return errors.New("envelope payload does not match committed bid")
	}
	requestsRoot, err := envelope.Message.ExecutionRequests.HashSSZ()
	if err != nil {
		return fmt.Errorf("hash execution requests: %w", err)
	}
	if requestsRoot != committed.ExecutionRequestsRoot {
		return errors.New("envelope execution requests do not match committed bid")
	}
	requestsHash := cltypes.ComputeExecutionRequestHash(cltypes.GetExecutionRequestsList(beaconCfg, envelope.Message.ExecutionRequests))
	header, err := payload.RlpHeader(&envelope.Message.ParentBeaconBlockRoot, requestsHash)
	if err != nil {
		return fmt.Errorf("build execution payload header: %w", err)
	}
	if header.Hash() != payload.BlockHash {
		return errors.New("execution payload block hash does not match payload contents")
	}
	return nil
}

// RecoverSkippedEnvelopes retries fetching envelopes for blocks that were
// skipped during backward download. Returns a map of successfully fetched
// envelopes keyed by beacon block root.
func (b *BackwardBeaconDownloader) RecoverSkippedEnvelopes(ctx context.Context, skipped []SkippedFullBlock) map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope {
	if len(skipped) == 0 {
		return nil
	}

	roots := make([][32]byte, len(skipped))
	for i, s := range skipped {
		roots[i] = s.Root
	}

	envelopes := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, len(roots))
	if b.httpFallbackURL != "" {
		b.fetchSkippedEnvelopesFromBeaconAPI(ctx, skipped, envelopes)
	}
	missingRoots := make([][32]byte, 0, len(roots)-len(envelopes))
	for _, root := range roots {
		if _, ok := envelopes[common.Hash(root)]; !ok {
			missingRoots = append(missingRoots, root)
		}
	}
	if b.rpc != nil && len(missingRoots) > 0 && ctx.Err() == nil {
		var err error
		p2pEnvelopes, err := RequestEnvelopesFrantically(ctx, b.rpc, missingRoots)
		if err != nil {
			log.Debug("[BackwardBeaconDownloader] envelope recovery: P2P failed", "err", err)
		}
		maps.Copy(envelopes, p2pEnvelopes)
	}

	return envelopes
}

func (b *BackwardBeaconDownloader) fetchSkippedEnvelopesFromBeaconAPI(ctx context.Context, skipped []SkippedFullBlock, envelopes map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope) {
	for _, item := range skipped {
		root := common.Hash(item.Root)
		if _, ok := envelopes[root]; ok {
			continue
		}
		block := &cltypes.SignedBeaconBlock{Block: &cltypes.BeaconBlock{Slot: item.Slot}}
		envelope, err := b.fetchSingleEnvelope(ctx, block)
		if err != nil || envelope == nil || envelope.Message == nil || envelope.Message.BeaconBlockRoot != root {
			continue
		}
		envelopes[root] = envelope
	}
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

		slot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, b.expectedRoot)
		if err != nil {
			return err
		}
		if slot == nil || *slot == 0 {
			break
		}

		if !b.canSkipSlot(ctx, tx, elFrozenBlocks, clFrozenBlocks, *slot) {
			break
		}

		b.slotToDownload.Store(*slot - 1)
		if err := beacon_indicies.MarkRootCanonical(b.ctx, tx, *slot, b.expectedRoot); err != nil {
			return err
		}

		b.expectedRoot, err = beacon_indicies.ReadParentBlockRoot(b.ctx, tx, b.expectedRoot)
		if err != nil {
			return err
		}

		// Clean up non-canonical slots
		newSlot, err := beacon_indicies.ReadBlockSlotByBlockRoot(tx, b.expectedRoot)
		if err != nil {
			return err
		}
		if newSlot == nil || *newSlot == 0 {
			continue
		}
		for i := *newSlot + 1; i < *slot; i++ {
			tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(i))
		}
	}

	return tx.Commit()
}

// canSkipSlot checks if we can skip to an existing block at the given slot.
func (b *BackwardBeaconDownloader) canSkipSlot(ctx context.Context, tx kv.Tx, elFrozenBlocks, clFrozenBlocks, slot uint64) bool {
	if slot <= clFrozenBlocks {
		return false
	}

	if b.engine == nil || !b.engine.SupportInsertion() {
		return true
	}

	blockHash, err := beacon_indicies.ReadExecutionBlockHash(tx, b.expectedRoot)
	if err != nil {
		log.Warn("Failed to read execution block hash", "err", err)
		return false
	}
	if blockHash == (common.Hash{}) {
		// [New in Gloas:EIP7732] GLOAS EMPTY blocks have no execution hash (no payload delivered).
		// If this slot is in the GLOAS era, no EL processing is needed, so we can skip.
		epoch := slot / b.beaconCfg.SlotsPerEpoch
		return b.beaconCfg.GetCurrentStateVersion(epoch) >= clparams.GloasVersion
	}

	blockNumber, err := beacon_indicies.ReadExecutionBlockNumber(tx, b.expectedRoot)
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
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("block fetch by root: status %d", resp.StatusCode)
	}
	body, err := readBoundedBeaconAPIResponse(resp.Body, maxBeaconAPIResponseBytes)
	if err != nil {
		return nil, err
	}

	version := httpConsensusVersion(resp.Header.Get("Eth-Consensus-Version"))
	block := cltypes.NewSignedBeaconBlock(beaconCfg, version)
	if err := block.DecodeSSZ(body, int(version)); err != nil {
		return nil, fmt.Errorf("block decode by root: %w", err)
	}
	return block, nil
}

// fetchSingleEnvelope fetches the execution payload envelope for a single GLOAS block.
// Returns (envelope, nil) on success, (nil, nil) when the beacon API confirms the slot
// has no envelope (HTTP 404 = genuinely EMPTY), or (nil, err) on fetch failure.
func (b *BackwardBeaconDownloader) fetchSingleEnvelope(ctx context.Context, block *cltypes.SignedBeaconBlock) (*cltypes.SignedExecutionPayloadEnvelope, error) {
	if b.httpFallbackURL == "" {
		return nil, fmt.Errorf("no HTTP fallback URL configured")
	}

	client := &http.Client{Timeout: 10 * time.Second}
	reqURL := fmt.Sprintf("%s/eth/v1/beacon/execution_payload_envelope/%d", b.httpFallbackURL, block.Block.Slot)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("envelope fetch: HTTP %d", resp.StatusCode)
	}
	body, err := readBoundedBeaconAPIResponse(resp.Body, maxBeaconAPIResponseBytes)
	if err != nil {
		return nil, err
	}

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelope(b.beaconCfg),
	}
	if err := envelope.DecodeSSZ(body, int(clparams.GloasVersion)); err != nil {
		return nil, fmt.Errorf("envelope decode: %w", err)
	}
	return envelope, nil
}

func readBoundedBeaconAPIResponse(body io.Reader, limit int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(body, limit+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > limit {
		return nil, fmt.Errorf("beacon API response exceeds %d bytes", limit)
	}
	return data, nil
}
