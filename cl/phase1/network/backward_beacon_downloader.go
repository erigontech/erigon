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
	prevBatchTopBlock    *cltypes.SignedBeaconBlock
	httpFallbackURL      string      // beacon API base URL for HTTP fallback when P2P fails
	httpPreferred        atomic.Bool // set after first HTTP success; skips P2P probing
	deferredEnvelopeRoot atomic.Pointer[common.Hash]

	// Confirmed FULL blocks processed without envelopes are collected for recovery.
	skippedFullBlocks []SkippedFullBlock

	mu sync.Mutex
}

// SkippedFullBlock records a GLOAS FULL block whose envelope was unavailable during backward download.
type SkippedFullBlock struct {
	Block *cltypes.SignedBeaconBlock
	Root  [32]byte
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

// SetInitialBlockEnvelopeDeferred delegates the starting anchor's envelope to checkpoint recovery.
func (b *BackwardBeaconDownloader) SetInitialBlockEnvelopeDeferred(beaconBlockRoot common.Hash) {
	b.deferredEnvelopeRoot.Store(&beaconBlockRoot)
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
	if len(blocks) == 0 {
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
	// [New in Gloas:EIP7732] Fetch envelopes for GLOAS FULL blocks before processing.
	log.Debug("[BackwardBeaconDownloader] processResponses start", "blocks", len(responses), "slotToDownload", b.slotToDownload.Load(), "expectedRoot", b.expectedRoot)
	envelopes, fetchRootSet, fullRootSet := b.fetchGloasEnvelopes(ctx, responses)
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

		_, isFull := fullRootSet[common.Hash(blockRoot)]
		_, isUnknown := fetchRootSet[common.Hash(blockRoot)]
		isUnknown = isUnknown && !isFull
		if isUnknown && envelope == nil {
			log.Debug("[BackwardBeaconDownloader] unseeded GLOAS envelope unavailable, preserving cursor", "slot", block.Block.Slot)
			return nil
		}

		finished, err := b.onNewBlock(block, envelope)
		b.finished.Store(finished)
		if err != nil {
			log.Warn("Error processing block", "err", err)
			continue
		}

		// Record FULL blocks passing through without envelope for post-download recovery.
		if isFull && envelope == nil {
			b.skippedFullBlocks = append(b.skippedFullBlocks, SkippedFullBlock{Block: block, Root: blockRoot})
		}

		advanced = true
		b.prevBatchTopBlock = block
		b.expectedRoot = block.Block.ParentRoot
		if block.Block.Slot == 0 {
			b.finished.Store(true)
			return nil
		}
		b.slotToDownload.Store(block.Block.Slot - 1)
	}

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
				fetchRoots, fullRoots := determineGloasEnvelopeRootsWithDeferredRoot([]*cltypes.SignedBeaconBlock{block}, b.prevBatchTopBlock, blockRoot, b.deferredEnvelopeRoot.Load())
				shouldFetch := slices.Contains(fetchRoots, blockRoot)
				isFull := slices.Contains(fullRoots, blockRoot)
				if shouldFetch {
					env, fetchErr := b.fetchSingleEnvelope(ctx, block)
					if fetchErr != nil {
						log.Debug("[BackwardBeaconDownloader] GLOAS envelope unavailable for root-fetched block",
							"slot", block.Block.Slot, "confirmedFull", isFull, "err", fetchErr)
					}
					envelope = env
				}
				if shouldFetch && !isFull && envelope == nil {
					log.Debug("[BackwardBeaconDownloader] unseeded GLOAS envelope unavailable for root-fetched block, preserving cursor", "slot", block.Block.Slot)
					return nil
				}

				finished, err := b.onNewBlock(block, envelope)
				b.finished.Store(finished)
				if err != nil {
					log.Warn("Error processing root-fetched block", "err", err)
				} else {
					if isFull && envelope == nil {
						b.skippedFullBlocks = append(b.skippedFullBlocks, SkippedFullBlock{Block: block, Root: blockRoot})
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

// determineGloasEnvelopeRoots separates optimistic fetches from lookahead-confirmed FULL roots.
func determineGloasEnvelopeRoots(responses []*cltypes.SignedBeaconBlock, prevBatchTopBlock *cltypes.SignedBeaconBlock) (fetchRoots, fullRoots [][32]byte) {
	if len(responses) == 0 {
		return nil, nil
	}
	expectedRoot, err := responses[len(responses)-1].Block.HashSSZ()
	if err != nil {
		return nil, nil
	}
	return determineGloasEnvelopeRootsWithDeferredRoot(responses, prevBatchTopBlock, expectedRoot, nil)
}

func determineGloasEnvelopeRootsWithDeferredRoot(responses []*cltypes.SignedBeaconBlock, prevBatchTopBlock *cltypes.SignedBeaconBlock, expectedRoot common.Hash, deferredEnvelopeRoot *common.Hash) (fetchRoots, fullRoots [][32]byte) {
	blocksByRoot := make(map[common.Hash]*cltypes.SignedBeaconBlock, len(responses))
	for _, block := range responses {
		root, err := block.Block.HashSSZ()
		if err == nil {
			blocksByRoot[root] = block
		}
	}

	block := blocksByRoot[expectedRoot]
	var child *cltypes.SignedBeaconBlock
	if prevBatchTopBlock != nil && prevBatchTopBlock.Block.ParentRoot == expectedRoot {
		child = prevBatchTopBlock
	}
	for block != nil {
		root := expectedRoot
		if block.Version() < clparams.GloasVersion {
			child = block
			expectedRoot = block.Block.ParentRoot
			block = blocksByRoot[expectedRoot]
			continue
		}
		bid := block.Block.Body.GetSignedExecutionPayloadBid()
		if bid != nil && bid.Message != nil {
			if child == nil {
				if deferredEnvelopeRoot == nil || *deferredEnvelopeRoot != root {
					fetchRoots = append(fetchRoots, root)
				}
			} else {
				childBid := child.Block.Body.GetSignedExecutionPayloadBid()
				if childBid != nil && childBid.Message != nil && childBid.Message.ParentBlockHash == bid.Message.BlockHash {
					fetchRoots = append(fetchRoots, root)
					fullRoots = append(fullRoots, root)
				}
			}
		}
		child = block
		expectedRoot = block.Block.ParentRoot
		block = blocksByRoot[expectedRoot]
	}
	return fetchRoots, fullRoots
}

func determineGloasFullRoots(responses []*cltypes.SignedBeaconBlock, prevBatchTopBlock *cltypes.SignedBeaconBlock) [][32]byte {
	_, fullRoots := determineGloasEnvelopeRoots(responses, prevBatchTopBlock)
	return fullRoots
}

// fetchGloasEnvelopes determines which GLOAS blocks in the batch are FULL and fetches their envelopes.
// It returns envelopes, attempted roots, and roots confirmed FULL by lookahead.
func (b *BackwardBeaconDownloader) fetchGloasEnvelopes(ctx context.Context, responses []*cltypes.SignedBeaconBlock) (map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, map[common.Hash]struct{}, map[common.Hash]struct{}) {
	if len(responses) == 0 {
		return nil, nil, nil
	}

	fetchRoots, fullRoots := determineGloasEnvelopeRootsWithDeferredRoot(responses, b.prevBatchTopBlock, b.expectedRoot, b.deferredEnvelopeRoot.Load())
	fetchRootSet := make(map[common.Hash]struct{}, len(fetchRoots))
	for _, r := range fetchRoots {
		fetchRootSet[common.Hash(r)] = struct{}{}
	}

	// Build a set for O(1) lookup by callers.
	fullRootSet := make(map[common.Hash]struct{}, len(fullRoots))
	for _, r := range fullRoots {
		fullRootSet[common.Hash(r)] = struct{}{}
	}

	if len(fetchRoots) == 0 {
		return nil, fetchRootSet, fullRootSet
	}

	// When HTTP has been working, skip the slow P2P envelope fetch entirely.
	if b.httpPreferred.Load() && b.httpFallbackURL != "" {
		envelopes := make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, len(fetchRoots))
		fetched := fetchEnvelopesFromBeaconAPI(ctx, b.httpFallbackURL, responses, fetchRoots, envelopes, b.beaconCfg)
		if fetched > 0 {
			log.Debug("[BackwardBeaconDownloader] fetched envelopes from beacon API", "count", fetched)
		}
		return envelopes, fetchRootSet, fullRootSet
	}

	envelopes, err := RequestEnvelopesFrantically(ctx, b.rpc, fetchRoots)
	if err != nil {
		log.Debug("[BackwardBeaconDownloader] failed to fetch GLOAS envelopes via P2P", "err", err)
	}
	// Fill in missing envelopes from the beacon API when an HTTP URL is configured.
	if b.httpFallbackURL != "" && len(envelopes) < len(fetchRoots) {
		if envelopes == nil {
			envelopes = make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, len(fetchRoots))
		}
		fetched := fetchEnvelopesFromBeaconAPI(ctx, b.httpFallbackURL, responses, fetchRoots, envelopes, b.beaconCfg)
		if fetched > 0 {
			log.Debug("[BackwardBeaconDownloader] fetched envelopes from beacon API", "count", fetched)
		}
	}

	return envelopes, fetchRootSet, fullRootSet
}

// SkippedFullBlocks returns confirmed FULL blocks awaiting envelope recovery.
func (b *BackwardBeaconDownloader) SkippedFullBlocks() []SkippedFullBlock {
	return b.skippedFullBlocks
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

	envelopes, err := RequestEnvelopesFrantically(ctx, b.rpc, roots)
	if err != nil {
		log.Debug("[BackwardBeaconDownloader] envelope recovery: P2P failed", "err", err)
	}
	if envelopes == nil {
		envelopes = make(map[common.Hash]*cltypes.SignedExecutionPayloadEnvelope, len(roots))
	}

	// HTTP fallback for roots still missing after P2P.
	if b.httpFallbackURL != "" && len(envelopes) < len(skipped) {
		blocks := make([]*cltypes.SignedBeaconBlock, len(skipped))
		for i, s := range skipped {
			blocks[i] = s.Block
		}
		fetchEnvelopesFromBeaconAPI(ctx, b.httpFallbackURL, blocks, roots, envelopes, b.beaconCfg)
	}

	return envelopes
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
			if err := tx.Delete(kv.CanonicalBlockRoots, base_encoding.Encode64ToBytes4(i)); err != nil {
				return err
			}
		}
	}

	return tx.Commit()
}

// canSkipSlot checks if we can skip to an existing block at the given slot.
func (b *BackwardBeaconDownloader) canSkipSlot(ctx context.Context, tx kv.Tx, elFrozenBlocks, clFrozenBlocks, slot uint64) bool {
	if slot <= clFrozenBlocks {
		return false
	}
	if b.beaconCfg != nil && b.beaconCfg.SlotsPerEpoch > 0 &&
		b.beaconCfg.GetCurrentStateVersion(slot/b.beaconCfg.SlotsPerEpoch) >= clparams.GloasVersion {
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
		return false
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
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("block fetch by root: status %d", resp.StatusCode)
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
	blockRoot, err := block.Block.HashSSZ()
	if err != nil {
		return nil, fmt.Errorf("block root: %w", err)
	}
	reqURL := fmt.Sprintf("%s/eth/v1/beacon/execution_payload_envelope/0x%x", b.httpFallbackURL, blockRoot)
	req, err := http.NewRequestWithContext(ctx, "GET", reqURL, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", "application/octet-stream")

	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	body, err := readEnvelopeHTTPBody(resp.Body)
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil // genuinely EMPTY: beacon API confirms no envelope
	}
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("envelope fetch: HTTP %d", resp.StatusCode)
	}
	version, err := cltypes.ParseExecutionPayloadEnvelopeVersion(resp.Header.Get("Eth-Consensus-Version"))
	if err != nil {
		return nil, fmt.Errorf("envelope consensus version: %w", err)
	}

	envelope := &cltypes.SignedExecutionPayloadEnvelope{
		Message: cltypes.NewExecutionPayloadEnvelopeWithVersion(b.beaconCfg, version),
	}
	if err := envelope.DecodeSSZStrict(body, int(version)); err != nil {
		return nil, fmt.Errorf("envelope decode: %w", err)
	}
	if err := envelope.ValidateForConfig(b.beaconCfg); err != nil {
		return nil, fmt.Errorf("envelope validation: %w", err)
	}
	if envelope.Message.BeaconBlockRoot != blockRoot {
		return nil, fmt.Errorf("envelope block root %x does not match requested block root %x", envelope.Message.BeaconBlockRoot, blockRoot)
	}
	return envelope, nil
}
