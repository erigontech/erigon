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

package sync

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/hashicorp/golang-lru/v2/simplelru"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/shards"
)

// If there are no waypoints from Heimdall (including in our local database), we won't be able to rely on the last received waypoint
// to determine the root of the canonical chain builder tree. However, we heuristically know the maximum expected height of such a tree.
// Therefore, given a descendant (tip), we can select a block that lags behind it by this constant value and consider it as the root.
// If we happen to choose an older root, it's not a problem since this only temporarily affects the tree size.
//
// Waypoints may be absent in case if it's an early stage of the chain's evolution, starting from the genesis block.
// The current constant value is chosen based on observed metrics in production as twice the doubled value of the maximum observed waypoint length.
const maxFinalizationHeight = 512
const downloadRequestsCacheSize = 1024
const maxBlockBatchDownloadSize = 256

const heimdallSyncRetryIntervalOnTip = 200 * time.Millisecond
const heimdallSyncRetryIntervalOnStartup = 30 * time.Second

var (
	futureMilestoneDelay  = 1 * time.Second // amount of time to wait before putting a future milestone back in the event queue
	errAlreadyProcessed   = errors.New("already processed")
	errKnownBadBlock      = errors.New("known bad block")
	errKnowBadParentBlock = errors.New("known bad parent block")
)

type heimdallSynchronizer interface {
	IsCatchingUp(ctx context.Context) (bool, error)
	SynchronizeCheckpoints(ctx context.Context) (latest *heimdall.Checkpoint, ok bool, err error)
	SynchronizeMilestones(ctx context.Context) (latest *heimdall.Milestone, ok bool, err error)
	SynchronizeSpans(ctx context.Context, blockNum uint64) error
	WaitUntilHeimdallIsSynced(ctx context.Context, retryInterval time.Duration) error
	Ready(ctx context.Context) <-chan error
}

type bridgeSynchronizer interface {
	Unwind(ctx context.Context, blockNum uint64) error
	ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error
	Ready(ctx context.Context) <-chan error
}

type wiggleCalculator interface {
	CalculateWiggle(ctx context.Context, header *types.Header) (time.Duration, error)
}

type EngineAPISwitcher interface {
	SetConsuming(consuming bool)
}

func NewSync(
	config *ethconfig.Config,
	logger log.Logger,
	store Store,
	execution ExecutionClient,
	milestoneVerifier WaypointHeadersVerifier,
	blocksVerifier BlocksVerifier,
	p2pService p2pService,
	blockDownloader *BlockDownloader,
	ccBuilderFactory CanonicalChainBuilderFactory,
	heimdallSync heimdallSynchronizer,
	bridgeSync bridgeSynchronizer,
	tipEvents *TipEvents,
	notifications *shards.Notifications,
	wiggleCalculator wiggleCalculator,
	engineAPISwitcher EngineAPISwitcher,
) *Sync {
	badBlocksLru, err := simplelru.NewLRU[common.Hash, struct{}](1024, nil)
	if err != nil {
		panic(err)
	}
	blockRequestsCache, err := lru.NewARC[common.Hash, struct{}](downloadRequestsCacheSize)
	if err != nil {
		panic(err)
	}
	return &Sync{
		config:             config,
		logger:             logger,
		store:              store,
		execution:          execution,
		milestoneVerifier:  milestoneVerifier,
		blocksVerifier:     blocksVerifier,
		p2pService:         p2pService,
		blockDownloader:    blockDownloader,
		ccBuilderFactory:   ccBuilderFactory,
		heimdallSync:       heimdallSync,
		bridgeSync:         bridgeSync,
		tipEvents:          tipEvents,
		badBlocks:          badBlocksLru,
		notifications:      notifications,
		wiggleCalculator:   wiggleCalculator,
		engineAPISwitcher:  engineAPISwitcher,
		blockRequestsCache: blockRequestsCache,
	}
}

type Sync struct {
	config             *ethconfig.Config
	logger             log.Logger
	store              Store
	execution          ExecutionClient
	milestoneVerifier  WaypointHeadersVerifier
	blocksVerifier     BlocksVerifier
	p2pService         p2pService
	blockDownloader    *BlockDownloader
	ccBuilderFactory   CanonicalChainBuilderFactory
	heimdallSync       heimdallSynchronizer
	bridgeSync         bridgeSynchronizer
	tipEvents          *TipEvents
	badBlocks          *simplelru.LRU[common.Hash, struct{}]
	notifications      *shards.Notifications
	wiggleCalculator   wiggleCalculator
	engineAPISwitcher  EngineAPISwitcher
	blockRequestsCache *lru.ARCCache[common.Hash, struct{}]
}

func (s *Sync) commitExecution(ctx context.Context, newTip *types.Header, finalizedHeader *types.Header) error {
	if err := s.store.Flush(ctx); err != nil {
		return err
	}

	blockNum := newTip.Number.Uint64()

	age := common.PrettyAge(time.Unix(int64(newTip.Time), 0))
	s.logger.Info(syncLogPrefix("update fork choice"), "block", blockNum, "hash", newTip.Hash(), "age", age)
	fcStartTime := time.Now()

	latestValidHash, err := s.execution.UpdateForkChoice(ctx, newTip, finalizedHeader)
	if err != nil {
		s.logger.Error("failed to update fork choice", "latestValidHash", latestValidHash, "err", err)
		return err
	}

	s.logger.Info(syncLogPrefix("update fork choice done"), "in", time.Since(fcStartTime))
	return nil
}

func (s *Sync) handleMilestoneTipMismatch(ctx context.Context, ccb *CanonicalChainBuilder, event EventNewMilestone) error {
	// the milestone doesn't correspond to the tip of the chain
	// unwind to the previous verified milestone
	// and download the blocks of the new milestone
	rootNum := ccb.Root().Number.Uint64()
	rootHash := ccb.Root().Hash()
	tipNum := ccb.Tip().Number.Uint64()
	tipHash := ccb.Tip().Hash()
	distanceToRoot := event.EndBlock().Uint64() - rootNum + 1
	s.logger.Info(
		syncLogPrefix("local chain tip does not match the milestone, unwinding to the previous verified root"),
		"rootNum", rootNum,
		"rootHash", rootHash,
		"tipNum", tipNum,
		"tipHash", tipHash,
		"milestoneId", event.Id,
		"milestoneStart", event.StartBlock(),
		"milestoneEnd", event.EndBlock(),
		"milestoneRootHash", event.RootHash(),
		"distanceToRoot", distanceToRoot,
	)

	feed, err := s.p2pService.FetchBlocksBackwards(
		ctx,
		event.RootHash(),
		ccb.HeaderReader(),
		p2p.WithChainLengthLimit(distanceToRoot),
		p2p.WithBlocksBatchSize(min(distanceToRoot, maxBlockBatchDownloadSize)),
	)
	if err != nil {
		s.logger.Warn(syncLogPrefix("failed to fetch blocks backwards during milestone mismatch"), "err", err)
		return nil // in case of p2p download err do not terminate the process
	}

	blocks := make([]*types.Block, 0, distanceToRoot)
	var batch []*types.Block
	for batch, err = feed.Next(ctx); err == nil && len(batch) > 0; batch, err = feed.Next(ctx) {
		blocks = append(blocks, batch...)
	}
	if err != nil {
		s.logger.Warn(syncLogPrefix("failed to get next block batch during milestone mismatch"), "err", err)
		return nil // in case of p2p download err do not terminate the process
	}

	// wait for any possibly unprocessed previous block inserts to finish
	if err := s.store.Flush(ctx); err != nil {
		return err
	}

	if err := s.bridgeSync.Unwind(ctx, rootNum); err != nil {
		return err
	}

	if s.config.PolygonPosSingleSlotFinality {
		for i := range blocks {
			if blocks[i].Number().Uint64() > s.config.PolygonPosSingleSlotFinalityBlockAt {
				blocks = blocks[:i]
				break
			}
		}
	}

	if err := s.store.InsertBlocks(ctx, blocks); err != nil {
		return err
	}

	newTip := blocks[len(blocks)-1].HeaderNoCopy()
	if err := s.commitExecution(ctx, newTip, newTip); err != nil {
		// note: if we face a failure during execution of finalized waypoints blocks, it means that
		// we're wrong and the blocks are not considered as bad blocks, so we should terminate
		return s.handleWaypointExecutionErr(ctx, ccb.Root(), err)
	}

	s.logger.Info(
		syncLogPrefix("resetting ccb to new tip after handling milestone mismatch"),
		"num", newTip.Number.Uint64(),
		"hash", newTip.Hash(),
	)
	ccb.Reset(newTip)
	return nil
}

func (s *Sync) applyNewMilestoneOnTip(ctx context.Context, event EventNewMilestone, ccb *CanonicalChainBuilder) error {
	milestone := event
	if milestone.EndBlock().Uint64() <= ccb.Root().Number.Uint64() {
		return nil
	}

	// milestone is ahead of our current tip
	if milestone.EndBlock().Uint64() > ccb.Tip().Number.Uint64() {
		s.logger.Debug(syncLogPrefix("putting milestone event back in the queue because our tip is behind the milestone"),
			"milestoneId", milestone.RawId(),
			"milestoneStart", milestone.StartBlock().Uint64(),
			"milestoneEnd", milestone.EndBlock().Uint64(),
			"milestoneRootHash", milestone.RootHash(),
			"tipBlockNumber", ccb.Tip().Number.Uint64(),
		)
		// put the milestone back in the queue, so it can be processed at a later time
		go func() {
			time.Sleep(futureMilestoneDelay)
			s.tipEvents.events.PushEvent(Event{Type: EventTypeNewMilestone, newMilestone: event})
		}()
		return nil
	}

	s.logger.Info(
		syncLogPrefix("applying new milestone event"),
		"milestoneId", milestone.RawId(),
		"milestoneStart", milestone.StartBlock().Uint64(),
		"milestoneEnd", milestone.EndBlock().Uint64(),
		"milestoneRootHash", milestone.RootHash(),
	)

	milestoneHeaders := ccb.HeadersInRange(milestone.StartBlock().Uint64(), milestone.Length())
	if err := s.milestoneVerifier(milestone, milestoneHeaders); err != nil {
		return s.handleMilestoneTipMismatch(ctx, ccb, milestone)
	}

	return ccb.PruneRoot(milestone.EndBlock().Uint64())
}

func (s *Sync) applyNewBlockChainOnTip(ctx context.Context, blockChain []*types.Block, ccb *CanonicalChainBuilder, source EventSource, peerId *p2p.PeerId) error {
	if err := s.blocksVerifier(blockChain); err != nil {
		s.logger.Debug(
			syncLogPrefix("applyNewBlockOnTip: invalid new block event from peer, penalizing and ignoring"),
			"err", err,
		)

		if err = s.p2pService.Penalize(ctx, peerId); err != nil {
			s.logger.Debug(syncLogPrefix("applyNewBlockOnTip: issue with penalizing peer"), "err", err)
		}

		return nil
	}

	headerChain := make([]*types.Header, len(blockChain))
	for i, block := range blockChain {
		headerChain[i] = block.HeaderNoCopy()
	}

	// wait until heimdall is synchronized before proceeding
	err := s.heimdallSync.WaitUntilHeimdallIsSynced(ctx, heimdallSyncRetryIntervalOnTip)
	if err != nil {
		return err
	}
	// make sure spans are synchronized
	// math.MaxUint64 is used because post VeBlop/Rio hard fork
	// spans could be overlapping, and the blocknum for the tip
	// of the headerChain might still be in the range of the last span
	// in the store, but we may still be processing a new span in the meantime
	err = s.heimdallSync.SynchronizeSpans(ctx, math.MaxUint64)
	if err != nil {
		return err
	}

	oldTip := ccb.Tip()
	newConnectedHeaders, err := ccb.Connect(ctx, headerChain)
	if err != nil {
		// IMPORTANT: we just log the error and do not return
		// to process the possibility of a partially connected header chain
		s.logger.Debug(
			syncLogPrefix("applyNewBlockOnTip: couldn't connect header chain to the local chain tip"),
			"partiallyConnected", len(newConnectedHeaders),
			"err", err,
		)
	}
	if len(newConnectedHeaders) == 0 {
		return nil
	}

	go func() {
		for i := range newConnectedHeaders {
			select {
			case <-ctx.Done():
				return
			default:
				wiggle, err := s.wiggleCalculator.CalculateWiggle(ctx, newConnectedHeaders[i])
				if err != nil {
					s.logger.Error(
						syncLogPrefix("failed update wiggle metrics"),
						"err", err,
					)
					continue
				}

				UpdateWiggleDuration(wiggle)
			}
		}
	}()

	newTip := ccb.Tip()
	firstNewConnectedHeader := newConnectedHeaders[0]
	if newTip != oldTip && oldTip.Hash() != firstNewConnectedHeader.ParentHash {
		if err := s.handleBridgeOnForkChange(ctx, ccb, oldTip); err != nil {
			return err
		}
	}

	newBlocksStartIdx := firstNewConnectedHeader.Number.Uint64() - blockChain[0].NumberU64()
	newBlocksEndIdx := newBlocksStartIdx + uint64(len(newConnectedHeaders))
	newConnectedBlocks := blockChain[newBlocksStartIdx:newBlocksEndIdx]
	newBlock := newConnectedBlocks[len(newConnectedBlocks)-1]
	if len(newConnectedBlocks) > 1 {
		s.logger.Info(
			syncLogPrefix("inserting multiple connected blocks"),
			"amount", len(newConnectedBlocks),
			"start", newConnectedBlocks[0].NumberU64(),
			"end", newBlock.NumberU64(),
		)
	}
	if err := s.store.InsertBlocks(ctx, newConnectedBlocks); err != nil {
		return err
	}

	if source == EventSourceBlockProducer {
		go s.publishNewBlock(ctx, newBlock)
		go s.p2pService.PublishNewBlockHashes(newBlock)
	}

	if source == EventSourceP2PNewBlock {
		// https://github.com/ethereum/devp2p/blob/master/caps/eth.md#block-propagation
		// devp2p spec: when a NewBlock announcement message is received from a peer, the client first verifies the
		// basic header validity of the block, checking whether the proof-of-work value is valid (replace PoW
		// with Bor rules that we do as part of CanonicalChainBuilder.Connect).
		// It then sends the block to a small fraction of connected peers (usually the square root of the total
		// number of peers) using the NewBlock message.
		// note, below is non-blocking
		go s.publishNewBlock(ctx, newBlock)
	}

	if newTip == oldTip {
		lastConnectedNum := newConnectedHeaders[len(newConnectedHeaders)-1].Number.Uint64()
		if tipNum := newTip.Number.Uint64(); lastConnectedNum > tipNum {
			return s.handleBridgeOnBlocksInsertAheadOfTip(ctx, tipNum, lastConnectedNum)
		}

		return nil
	}

	if err := s.commitExecution(ctx, newTip, ccb.Root()); err != nil {
		if errors.Is(err, ErrForkChoiceUpdateBadBlock) {
			return s.handleBadBlockErr(ctx, ccb, newBlock.Hash(), source, peerId, firstNewConnectedHeader, oldTip, err)
		}

		return err
	}

	if source == EventSourceP2PNewBlock {
		// https://github.com/ethereum/devp2p/blob/master/caps/eth.md#block-propagation
		// devp2p spec: After the header validity check, the client imports the block into its local chain by executing
		// all transactions contained in the block, computing the block's 'post state'. The block's state-root hash
		// must match the computed post state root. Once the block is fully processed, and considered valid,
		// the client sends a NewBlockHashes message about the block to all peers which it didn't notify earlier.
		// Those peers may request the full block later if they fail to receive it via NewBlock from anyone else.
		// Including hashes that the sending node later refuses to honour with a proceeding GetBlockHeaders
		// message is considered bad form, and may reduce the reputation of the sending node.
		// note, below is non-blocking
		s.p2pService.PublishNewBlockHashes(newBlock)
	}
	return nil
}

// apply some checks on new block header. (i.e. bad block , or too old block, or already contained in ccb)
func (s *Sync) checkNewBlockHeader(ctx context.Context, newBlockHeader *types.Header, ccb *CanonicalChainBuilder, eventSource EventSource, peerId *p2p.PeerId) error {
	newBlockHeaderNum := newBlockHeader.Number.Uint64()
	newBlockHeaderHash := newBlockHeader.Hash()
	rootNum := ccb.Root().Number.Uint64()
	if newBlockHeaderNum <= rootNum || ccb.ContainsHash(newBlockHeaderHash) {
		return errAlreadyProcessed
	}

	if s.badBlocks.Contains(newBlockHeaderHash) {
		s.logger.Warn(syncLogPrefix("bad block received from peer"),
			"blockHash", newBlockHeaderHash,
			"blockNum", newBlockHeaderNum,
			"peerId", peerId,
		)
		s.maybePenalizePeerOnBadBlockEvent(ctx, eventSource, peerId)
		return errKnownBadBlock
	}

	if s.badBlocks.Contains(newBlockHeader.ParentHash) {
		s.logger.Warn(syncLogPrefix("block with bad parent received from peer"),
			"blockHash", newBlockHeaderHash,
			"blockNum", newBlockHeaderNum,
			"parentHash", newBlockHeader.ParentHash,
			"peerId", peerId,
		)
		s.badBlocks.Add(newBlockHeaderHash, struct{}{})
		s.maybePenalizePeerOnBadBlockEvent(ctx, eventSource, peerId)
		return errKnowBadParentBlock
	}
	return nil
}

func (s *Sync) applyNewBlockOnTip(ctx context.Context, event EventNewBlock, ccb *CanonicalChainBuilder) error {
	newBlockHeader := event.NewBlock.HeaderNoCopy()
	newBlockHeaderHash := newBlockHeader.Hash()
	newBlockHeaderNum := newBlockHeader.Number.Uint64()
	if err := s.checkNewBlockHeader(ctx, newBlockHeader, ccb, event.Source, event.PeerId); err != nil {
		return nil
	}
	s.logger.Debug(
		syncLogPrefix("applying new block event"),
		"blockNum", newBlockHeaderNum,
		"blockHash", newBlockHeaderHash,
		"parentBlockHash", newBlockHeader.ParentHash,
		"source", event.Source,
		"peerId", event.PeerId,
	)
	if ccb.ContainsHash(newBlockHeader.ParentHash) {
		return s.applyNewBlockChainOnTip(ctx, []*types.Block{event.NewBlock}, ccb, event.Source, event.PeerId)
	} else {
		s.asyncBackwardDownloadBlockBatches(ctx, newBlockHeaderHash, newBlockHeaderNum, event.PeerId, event.Source, ccb)
		return nil
	}
}

func (s *Sync) applyNewBlockBatchOnTip(ctx context.Context, event EventNewBlockBatch, ccb *CanonicalChainBuilder) error {
	var err error
	defer func() {
		if err == nil || errors.Is(err, errAlreadyProcessed) {
			close(event.Processed)
			return
		}
		select {
		case <-ctx.Done():
			return
		case event.Processed <- err:
		}
	}()
	numBlocks := len(event.NewBlocks)
	if numBlocks == 0 {
		s.logger.Debug(syncLogPrefix("applying new empty block batch event"))
		return nil
	} else {
		s.logger.Debug(syncLogPrefix("applying new block batch event"), "startBlock", event.NewBlocks[0].Number().Uint64(), "endBlock", event.NewBlocks[numBlocks-1].Number().Uint64())
	}
	blockChain := event.NewBlocks
	newBlockHeader := blockChain[len(blockChain)-1].HeaderNoCopy()
	err = s.checkNewBlockHeader(ctx, newBlockHeader, ccb, event.Source, event.PeerId)
	if err != nil {
		if errors.Is(err, errKnownBadBlock) || errors.Is(err, errKnowBadParentBlock) {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case event.Processed <- err:
			}
		}
		return nil
	}
	err = s.applyNewBlockChainOnTip(ctx, blockChain, ccb, event.Source, event.PeerId)
	if err != nil {
		return err
	}
	return nil
}

func (s *Sync) applyNewBlockHashesOnTip(ctx context.Context, event EventNewBlockHashes, ccb *CanonicalChainBuilder) error {
	for _, hashOrNum := range event.NewBlockHashes {
		if (hashOrNum.Number <= ccb.Root().Number.Uint64()) || ccb.ContainsHash(hashOrNum.Hash) {
			continue
		}

		if s.badBlocks.Contains(hashOrNum.Hash) {
			// note: we do not penalize peer for bad blocks on new block hash events since they have
			// not necessarily been executed by the peer but just propagated as per the devp2p spec
			s.logger.Warn(syncLogPrefix("bad block hash received from peer"),
				"blockHash", hashOrNum.Hash,
				"blockNum", hashOrNum.Number,
				"peerId", event.PeerId,
			)
			continue
		}

		s.asyncBackwardDownloadBlockBatches(ctx, hashOrNum.Hash, hashOrNum.Number, event.PeerId, EventSourceP2PNewBlockHashes, ccb)
	}
	return nil
}

func (s *Sync) asyncBackwardDownloadBlockBatches(
	ctx context.Context,
	fromHash common.Hash,
	fromNum uint64,
	fromPeerId *p2p.PeerId,
	eventSource EventSource,
	ccb *CanonicalChainBuilder,
) {
	if s.blockRequestsCache.Contains(fromHash) { // we've already seen this download request before
		s.logger.Debug(
			syncLogPrefix("ignoring duplicate backward download"),
			"blockNum", fromNum,
			"blockHash", fromHash,
			"source", eventSource,
		)
		return
	}
	// we need to do a backward download. so schedule the download in a goroutine and have it  push an `EventNewBlockBatch` which can be processed later,
	// so that we don't block the event processing loop
	root := ccb.Root()
	rootNum := root.Number.Uint64()
	s.logger.Debug(
		syncLogPrefix("block parent hash not in ccb, fetching blocks backwards to root"),
		"rootNum", rootNum,
		"rootHash", root.Hash(),
		"blockNum", fromNum,
		"blockHash", fromHash,
		"amount", fromNum-rootNum+1,
	)
	s.blockRequestsCache.Add(fromHash, struct{}{})
	go func() {
		err := s.backwardDownloadBlockBatches(ctx, fromHash, fromNum, fromPeerId, eventSource, ccb)
		if err != nil {
			s.logger.Warn(
				syncLogPrefix("failed to backward download blocks"),
				"blockNum", fromNum,
				"blockHash", fromHash,
				"source", eventSource,
				"err", err,
			)
			s.blockRequestsCache.Remove(fromHash)
		}
	}()
}

func (s *Sync) backwardDownloadBlockBatches(
	ctx context.Context,
	fromHash common.Hash,
	fromNum uint64,
	fromPeerId *p2p.PeerId,
	source EventSource,
	ccb *CanonicalChainBuilder,
) error {
	rootNum := ccb.Root().Number.Uint64()
	amount := fromNum - rootNum + 1
	feed, err := s.p2pService.FetchBlocksBackwards(
		ctx,
		fromHash,
		ccb.HeaderReader(),
		p2p.WithChainLengthLimit(amount),
		p2p.WithBlocksBatchSize(min(amount, maxBlockBatchDownloadSize)),
		p2p.WithPeerId(fromPeerId),
	)
	if err != nil {
		return err
	}
	var blocks []*types.Block
	for blocks, err = feed.Next(ctx); err == nil && len(blocks) > 0; blocks, err = feed.Next(ctx) {
		processedC := make(chan error)
		s.tipEvents.events.PushEvent(Event{
			Type: EventTypeNewBlockBatch,
			newBlockBatch: EventNewBlockBatch{
				NewBlocks: blocks,
				PeerId:    fromPeerId,
				Source:    source,
				Processed: processedC,
			},
		})
		s.logger.Debug(
			syncLogPrefix("downloaded block batch, waiting to be processed"),
			"fromNum", blocks[0].NumberU64(),
			"toNum", blocks[len(blocks)-1].NumberU64(),
			"fromHash", blocks[0].Hash(),
			"toHash", blocks[len(blocks)-1].Hash(),
			"peerId", fromPeerId,
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-processedC:
			if err != nil {
				return err
			}
			s.logger.Debug(
				syncLogPrefix("block batch processed"),
				"fromNum", blocks[0].NumberU64(),
				"toNum", blocks[len(blocks)-1].NumberU64(),
				"fromHash", blocks[0].Hash(),
				"toHash", blocks[len(blocks)-1].Hash(),
				"peerId", fromPeerId,
			)
		}
	}
	return err
}

func (s *Sync) publishNewBlock(ctx context.Context, block *types.Block) {
	td, err := s.execution.GetTd(ctx, block.NumberU64(), block.Hash())
	if err != nil {
		s.logger.Warn(syncLogPrefix("could not fetch td when publishing new block"),
			"blockNum", block.NumberU64(),
			"blockHash", block.Hash(),
			"err", err,
		)

		return
	}

	s.p2pService.PublishNewBlock(block, td)
}

func (s *Sync) handleBridgeOnForkChange(ctx context.Context, ccb *CanonicalChainBuilder, oldTip *types.Header) error {
	// forks have changed, we need to unwind unwindable data
	newTip := ccb.Tip()
	s.logger.Info(
		syncLogPrefix("handling bridge on fork change"),
		"oldNum", oldTip.Number.Uint64(),
		"oldHash", oldTip.Hash(),
		"newNum", newTip.Number.Uint64(),
		"newHash", newTip.Hash(),
	)

	// Find unwind point
	lca, ok := ccb.LowestCommonAncestor(newTip.Hash(), oldTip.Hash())
	if !ok {
		return errors.New("could not find lowest common ancestor of old and new tip")
	}

	// wait for any possibly unprocessed previous block inserts to finish
	if err := s.store.Flush(ctx); err != nil {
		return err
	}

	return s.reorganiseBridge(ctx, ccb, lca)
}

func (s *Sync) reorganiseBridge(ctx context.Context, ccb *CanonicalChainBuilder, forksLca *types.Header) error {
	newTip := ccb.Tip()
	newTipNum := ccb.Tip().Number.Uint64()
	unwindPoint := forksLca.Number.Uint64()
	s.logger.Debug(
		syncLogPrefix("reorganise bridge"),
		"newTip", newTipNum,
		"newTipHash", newTip.Hash(),
		"unwindPointNum", unwindPoint,
		"unwindPointHash", forksLca.Hash(),
	)

	if newTipNum < unwindPoint { // defensive check against underflow & unexpected newTipNum and unwindPoint
		return fmt.Errorf("unexpected newTipNum <= unwindPoint: %d < %d", newTipNum, unwindPoint)
	}

	// 1. Do the unwind from the old tip (on the old canonical fork) to the unwindPoint
	if err := s.bridgeSync.Unwind(ctx, unwindPoint); err != nil {
		return err
	}

	// 2. Replay the new canonical blocks from the unwindPoint+1 to the new tip (on the new canonical fork). Note,
	//    that there may be a case where the newTip == unwindPoint in which case the below will be a no-op.
	if newTipNum == unwindPoint {
		return nil
	}

	start := unwindPoint + 1
	amount := newTipNum - start + 1
	canonicalHeaders := ccb.HeadersInRange(start, amount)
	canonicalBlocks := make([]*types.Block, len(canonicalHeaders))
	for i, header := range canonicalHeaders {
		canonicalBlocks[i] = types.NewBlockWithHeader(header)
	}

	return s.bridgeSync.ProcessNewBlocks(ctx, canonicalBlocks)
}

func (s *Sync) handleBridgeOnBlocksInsertAheadOfTip(ctx context.Context, tipNum, lastInsertedNum uint64) error {
	// this is a hack that should disappear when changing the bridge to not track blocks (future work)
	// make sure the bridge does not go past the tip (it may happen when we insert blocks from another fork that
	// has a higher block number than the canonical tip but lower difficulty) - this is to prevent the bridge
	// from recording incorrect bor txn hashes
	s.logger.Info(
		syncLogPrefix("unwinding bridge due to inserting headers past the tip"),
		"tip", tipNum,
		"lastInsertedNum", lastInsertedNum,
	)

	// wait for the insert blocks flush
	if err := s.store.Flush(ctx); err != nil {
		return err
	}

	return s.bridgeSync.Unwind(ctx, tipNum)
}

func (s *Sync) handleBadBlockErr(
	ctx context.Context,
	ccb *CanonicalChainBuilder,
	newBlockHash common.Hash,
	eventSource EventSource,
	peerId *p2p.PeerId,
	firstNewConnectedHeader *types.Header,
	oldTip *types.Header,
	badBlockErr error,
) error {
	badTip := ccb.Tip()
	badTipHash := badTip.Hash()
	oldTipNum := oldTip.Number.Uint64()
	oldTipHash := oldTip.Hash()
	s.logger.Warn(
		syncLogPrefix("handling bad block after execution"),
		"peerId", peerId,
		"badTipNum", badTip.Number.Uint64(),
		"badTipHash", badTipHash,
		"oldTipNum", oldTipNum,
		"oldTipHash", oldTipHash,
		"firstNewConnectedNum", firstNewConnectedHeader.Number.Uint64(),
		"firstNewConnectedHash", firstNewConnectedHeader.Hash(),
		"err", badBlockErr,
	)

	// 1. Mark block as bad and penalize peer
	s.badBlocks.Add(newBlockHash, struct{}{})
	s.maybePenalizePeerOnBadBlockEvent(ctx, eventSource, peerId)

	// 2. Find unwind point
	lca, ok := ccb.LowestCommonAncestor(oldTipHash, badTip.Hash())
	if !ok {
		return errors.New("could not find lowest common ancestor of old and new tip")
	}

	// 3. Prune newly inserted nodes in the tree => should roll back the ccb to the old tip
	if err := ccb.PruneNode(firstNewConnectedHeader.Hash()); err != nil {
		return err
	}

	newTip := ccb.Tip()
	newTipNum := newTip.Number.Uint64()
	newTipHash := newTip.Hash()
	if oldTipHash != newTipHash { // defensive check for unexpected behaviour
		return fmt.Errorf(
			"old tip hash does not match new tip hash (%d,%s) vs (%d, %s)",
			oldTipNum, oldTipHash, newTipNum, newTipHash,
		)
	}

	// 4. Update bridge
	return s.reorganiseBridge(ctx, ccb, lca)
}

func (s *Sync) maybePenalizePeerOnBadBlockEvent(ctx context.Context, eventSource EventSource, peerId *p2p.PeerId) {
	if eventSource == EventSourceP2PNewBlockHashes {
		// note: we do not penalize peer for bad blocks on new block hash events since they have
		// not necessarily been executed by the peer but just propagated as per the devp2p spec
		return
	}

	s.logger.Debug(syncLogPrefix("penalizing peer for bad block"), "peerId", peerId)
	if err := s.p2pService.Penalize(ctx, peerId); err != nil {
		s.logger.Debug(syncLogPrefix("issue with penalizing peer for bad block"), "peerId", peerId, "err", err)
	}
}

//
// TODO (subsequent PRs) - unit test initial sync + on new event cases
//

func (s *Sync) Run(ctx context.Context) error {
	// we have to check if the heimdall we are connected to is synchonised with the chain
	// to prevent getting empty list of checkpoints/milestones during the sync
	catchingUp, err := s.heimdallSync.IsCatchingUp(ctx)
	if err != nil {
		return err
	}

	if catchingUp {
		s.logger.Warn(syncLogPrefix("your heimdalld process is behind, please check its logs and <HEIMDALL_HOST>:1317/status api"))
		err = s.heimdallSync.WaitUntilHeimdallIsSynced(ctx, heimdallSyncRetryIntervalOnStartup)
		if err != nil {
			return err
		}
	}

	s.logger.Info(syncLogPrefix("waiting for execution client"))
	if err := <-s.bridgeSync.Ready(ctx); err != nil {
		return err
	}

	if err := <-s.heimdallSync.Ready(ctx); err != nil {
		return err
	}

	if err := s.execution.Prepare(ctx); err != nil {
		return err
	}

	if err := s.store.Prepare(ctx); err != nil {
		return err
	}

	s.logger.Info(syncLogPrefix("running sync component"))
	result, err := s.syncToTip(ctx)
	if err != nil {
		return err
	}

	if s.config.PolygonPosSingleSlotFinality {
		if result.latestTip.Number.Uint64() >= s.config.PolygonPosSingleSlotFinalityBlockAt {
			s.engineAPISwitcher.SetConsuming(true)
			return nil
		}
	}

	ccBuilder, err := s.initialiseCcb(ctx, result)
	if err != nil {
		return err
	}

	inactivityDuration := 30 * time.Second
	lastProcessedEventTime := time.Now()
	inactivityTicker := time.NewTicker(inactivityDuration)
	defer inactivityTicker.Stop()
	for {
		select {
		case event := <-s.tipEvents.Events():
			if s.config.PolygonPosSingleSlotFinality {
				block, err := s.execution.CurrentHeader(ctx)
				if err != nil {
					return err
				}

				if block.Number.Uint64() >= s.config.PolygonPosSingleSlotFinalityBlockAt {
					s.engineAPISwitcher.SetConsuming(true)
					return nil
				}
			}

			switch event.Type {
			case EventTypeNewMilestone:
				if err = s.applyNewMilestoneOnTip(ctx, event.AsNewMilestone(), ccBuilder); err != nil {
					return err
				}
			case EventTypeNewBlock:
				if err = s.applyNewBlockOnTip(ctx, event.AsNewBlock(), ccBuilder); err != nil {
					return err
				}
			case EventTypeNewBlockBatch:
				if err = s.applyNewBlockBatchOnTip(ctx, event.AsNewBlockBatch(), ccBuilder); err != nil {
					return err
				}
			case EventTypeNewBlockHashes:
				if err = s.applyNewBlockHashesOnTip(ctx, event.AsNewBlockHashes(), ccBuilder); err != nil {
					return err
				}
			default:
				panic(fmt.Sprintf("unexpected event type: %v", event.Type))
			}

			lastProcessedEventTime = time.Now()
		case <-inactivityTicker.C:
			if time.Since(lastProcessedEventTime) < inactivityDuration {
				continue
			}

			s.logger.Info(syncLogPrefix("waiting for chain tip events..."))
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// initialiseCcb populates the canonical chain builder with the latest finalized root header and with latest known
// canonical chain tip.
func (s *Sync) initialiseCcb(ctx context.Context, result syncToTipResult) (*CanonicalChainBuilder, error) {
	tip := result.latestTip

	tipNum := tip.Number.Uint64()
	rootNum := uint64(0)

	if tipNum > maxFinalizationHeight {
		rootNum = tipNum - maxFinalizationHeight
	}

	if result.latestWaypoint != nil {
		rootNum = result.latestWaypoint.EndBlock().Uint64()
		if result.latestWaypoint.EndBlock().Uint64() > tipNum {
			return nil, fmt.Errorf("unexpected rootNum > tipNum: %d > %d", rootNum, tipNum)
		}
	}

	s.logger.Debug(syncLogPrefix("initialising canonical chain builder"), "rootNum", rootNum, "tipNum", tipNum)

	var root *types.Header
	var err error
	if rootNum == tipNum {
		root = tip
	} else {
		root, err = s.execution.GetHeader(ctx, rootNum)
	}
	if err != nil {
		return nil, err
	}

	ccb := s.ccBuilderFactory(root)
	for blockNum := rootNum + 1; blockNum <= tipNum; blockNum++ {
		header, err := s.execution.GetHeader(ctx, blockNum)
		if err != nil {
			return nil, err
		}

		_, err = ccb.Connect(ctx, []*types.Header{header})
		if err != nil {
			return nil, err
		}
	}

	return ccb, nil
}

type syncToTipResult struct {
	latestTip      *types.Header
	latestWaypoint heimdall.Waypoint
}

func (s *Sync) syncToTip(ctx context.Context) (syncToTipResult, error) {
	latestTipOnStart, err := s.execution.CurrentHeader(ctx)
	if err != nil {
		return syncToTipResult{}, err
	}

	finalisedTip := syncToTipResult{
		latestTip: latestTipOnStart,
	}

	// we need to synchronize spans because syncing from checkpoints and milestones below has a dependency on spans
	// during pruning, and if the span store is not up to date then this can result in an error
	if err := s.heimdallSync.SynchronizeSpans(ctx, math.MaxUint64); err != nil {
		return syncToTipResult{}, err
	}

	s.logger.Info(syncLogPrefix("spans synchronized"))

	startTime := time.Now()
	result, ok, err := s.syncToTipUsingCheckpoints(ctx, finalisedTip.latestTip)
	if err != nil {
		return syncToTipResult{}, err
	}

	if ok {
		blocks := result.latestTip.Number.Uint64() - finalisedTip.latestTip.Number.Uint64()
		s.logger.Info(
			syncLogPrefix("checkpoint sync finished"),
			"tip", result.latestTip.Number.Uint64(),
			"time", common.PrettyAge(startTime),
			"blocks", blocks,
			"blk/sec", uint64(float64(blocks)/time.Since(startTime).Seconds()),
		)

		finalisedTip = result
	}

	startTime = time.Now()
	result, ok, err = s.syncToTipUsingMilestones(ctx, finalisedTip.latestTip)
	if err != nil {
		return syncToTipResult{}, err
	}

	if ok {
		blocks := result.latestTip.Number.Uint64() - finalisedTip.latestTip.Number.Uint64()
		s.logger.Info(
			syncLogPrefix("milestone sync finished"),
			"tip", result.latestTip.Number.Uint64(),
			"time", common.PrettyAge(startTime),
			"blocks", blocks,
			"blk/sec", uint64(float64(blocks)/time.Since(startTime).Seconds()),
		)
		finalisedTip = result
	}

	if finalisedTip.latestTip != nil {
		if err := s.heimdallSync.SynchronizeSpans(ctx, finalisedTip.latestTip.Number.Uint64()); err != nil {
			return syncToTipResult{}, err
		}
	}

	return finalisedTip, nil
}

func (s *Sync) syncToTipUsingCheckpoints(ctx context.Context, tip *types.Header) (syncToTipResult, bool, error) {
	syncCheckpoints := func(ctx context.Context) (heimdall.Waypoint, bool, error) {
		return s.heimdallSync.SynchronizeCheckpoints(ctx)
	}
	return s.sync(ctx, tip, syncCheckpoints, s.blockDownloader.DownloadBlocksUsingCheckpoints)
}

func (s *Sync) syncToTipUsingMilestones(ctx context.Context, tip *types.Header) (syncToTipResult, bool, error) {
	syncMilestones := func(ctx context.Context) (heimdall.Waypoint, bool, error) {
		return s.heimdallSync.SynchronizeMilestones(ctx)
	}
	return s.sync(ctx, tip, syncMilestones, s.blockDownloader.DownloadBlocksUsingMilestones)
}

type waypointSyncFunc func(ctx context.Context) (heimdall.Waypoint, bool, error)
type blockDownloadFunc func(ctx context.Context, startBlockNum uint64, endBlockNum *uint64) (*types.Header, error)

func (s *Sync) sync(
	ctx context.Context,
	tip *types.Header,
	waypointSync waypointSyncFunc,
	blockDownload blockDownloadFunc,
) (syncToTipResult, bool, error) {
	var waypoint heimdall.Waypoint
	var err error
	var ok bool

	var syncTo *uint64

	if s.config.PolygonPosSingleSlotFinality {
		syncTo = &s.config.PolygonPosSingleSlotFinalityBlockAt
	}

	for {
		waypoint, ok, err = waypointSync(ctx)
		if err != nil {
			return syncToTipResult{}, false, err
		}
		if !ok {
			return syncToTipResult{}, false, nil
		}

		// notify about latest waypoint end block so that eth_syncing API doesn't flicker on initial sync
		s.notifications.NewLastBlockSeen(waypoint.EndBlock().Uint64())

		newTip, err := blockDownload(ctx, tip.Number.Uint64()+1, syncTo)
		if err != nil {
			return syncToTipResult{}, false, err
		}

		if newTip == nil {
			// we've reached the tip
			break
		}

		if err := s.commitExecution(ctx, newTip, newTip); err != nil {
			if errors.Is(err, ErrForkChoiceUpdateTooFarBehind) {
				s.logger.Warn(
					syncLogPrefix("ufc skipped during sync to tip - likely due to domain ahead of blocks"),
					"err", err,
				)
			} else {
				// note: if we face a failure during execution of finalized waypoints blocks, it means that
				// we're wrong and the blocks are not considered as bad blocks, so we should terminate
				err = s.handleWaypointExecutionErr(ctx, tip, err)
				return syncToTipResult{}, false, err
			}
		}

		tip = newTip

		if s.config.PolygonPosSingleSlotFinality {
			if newTip.Number.Uint64() >= s.config.PolygonPosSingleSlotFinalityBlockAt {
				break
			}
		}
	}

	return syncToTipResult{latestTip: tip, latestWaypoint: waypoint}, true, nil
}

func (s *Sync) handleWaypointExecutionErr(ctx context.Context, lastCorrectTip *types.Header, execErr error) error {
	s.logger.Error(
		syncLogPrefix("waypoint execution err"),
		"lastCorrectTipNum", lastCorrectTip.Number.Uint64(),
		"lastCorrectTipHash", lastCorrectTip.Hash(),
		"execErr", execErr,
	)

	if !errors.Is(execErr, ErrForkChoiceUpdateBadBlock) {
		return execErr
	}

	execErr = fmt.Errorf("unexpected bad block at finalized waypoint: %w", execErr)
	// if it is a bad block try to unwind the bridge to the last known tip so we leave it in a good state
	if bridgeUnwindErr := s.bridgeSync.Unwind(ctx, lastCorrectTip.Number.Uint64()); bridgeUnwindErr != nil {
		return fmt.Errorf("%w: %w", bridgeUnwindErr, execErr)
	}

	return execErr
}

func (s *Sync) ignoreFetchBlocksErrOnTipEvent(err error) bool {
	return errors.Is(err, &p2p.ErrIncompleteHeaders{}) ||
		errors.Is(err, &p2p.ErrNonSequentialHeaderNumbers{}) ||
		errors.Is(err, &p2p.ErrNonSequentialHeaderHashes{}) ||
		errors.Is(err, &p2p.ErrMissingHeaderHash{}) ||
		errors.Is(err, &p2p.ErrUnexpectedHeaderHash{}) ||
		errors.Is(err, &p2p.ErrTooManyHeaders{}) ||
		errors.Is(err, &p2p.ErrMissingBodies{}) ||
		errors.Is(err, &p2p.ErrTooManyBodies{}) ||
		errors.Is(err, p2p.ErrPeerNotFound) ||
		errors.Is(err, context.DeadlineExceeded)
}
