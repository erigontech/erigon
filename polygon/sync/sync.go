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
	"time"

	"github.com/hashicorp/golang-lru/v2/simplelru"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/p2p"
)

type heimdallSynchronizer interface {
	SynchronizeCheckpoints(ctx context.Context) (latest *heimdall.Checkpoint, err error)
	SynchronizeMilestones(ctx context.Context) (latest *heimdall.Milestone, err error)
	SynchronizeSpans(ctx context.Context, blockNum uint64) error
}

type bridgeSynchronizer interface {
	Synchronize(ctx context.Context, blockNum uint64) error
	Unwind(ctx context.Context, blockNum uint64) error
	ProcessNewBlocks(ctx context.Context, blocks []*types.Block) error
}

type Sync struct {
	store             Store
	execution         ExecutionClient
	milestoneVerifier WaypointHeadersVerifier
	blocksVerifier    BlocksVerifier
	p2pService        p2p.Service
	blockDownloader   BlockDownloader
	ccBuilderFactory  CanonicalChainBuilderFactory
	heimdallSync      heimdallSynchronizer
	bridgeSync        bridgeSynchronizer
	events            <-chan Event
	badBlocks         *simplelru.LRU[common.Hash, struct{}]
	logger            log.Logger
}

func NewSync(
	store Store,
	execution ExecutionClient,
	milestoneVerifier WaypointHeadersVerifier,
	blocksVerifier BlocksVerifier,
	p2pService p2p.Service,
	blockDownloader BlockDownloader,
	ccBuilderFactory CanonicalChainBuilderFactory,
	heimdallSync heimdallSynchronizer,
	bridgeSync bridgeSynchronizer,
	events <-chan Event,
	logger log.Logger,
) *Sync {
	badBlocksLru, err := simplelru.NewLRU[common.Hash, struct{}](1024, nil)
	if err != nil {
		panic(err)
	}

	return &Sync{
		store:             store,
		execution:         execution,
		milestoneVerifier: milestoneVerifier,
		blocksVerifier:    blocksVerifier,
		p2pService:        p2pService,
		blockDownloader:   blockDownloader,
		ccBuilderFactory:  ccBuilderFactory,
		heimdallSync:      heimdallSync,
		bridgeSync:        bridgeSync,
		events:            events,
		badBlocks:         badBlocksLru,
		logger:            logger,
	}
}

func (s *Sync) commitExecution(ctx context.Context, newTip *types.Header, finalizedHeader *types.Header) (common.Hash, error) {
	if err := s.store.Flush(ctx); err != nil {
		return common.Hash{}, err
	}

	blockNum := newTip.Number.Uint64()

	if err := s.heimdallSync.SynchronizeSpans(ctx, blockNum); err != nil {
		return common.Hash{}, err
	}

	if err := s.bridgeSync.Synchronize(ctx, blockNum); err != nil {
		return common.Hash{}, err
	}

	age := common.PrettyAge(time.Unix(int64(newTip.Time), 0))
	s.logger.Info(syncLogPrefix("update fork choice"), "block", blockNum, "hash", newTip.Hash(), "age", age)
	fcStartTime := time.Now()

	latestValidHash, err := s.execution.UpdateForkChoice(ctx, newTip, finalizedHeader)
	if err != nil {
		s.logger.Error("failed to update fork choice", "latestValidHash", latestValidHash, "err", err)
		return latestValidHash, err
	}

	s.logger.Info(syncLogPrefix("update fork choice done"), "in", time.Since(fcStartTime))
	return latestValidHash, nil
}

func (s *Sync) handleMilestoneTipMismatch(
	ctx context.Context,
	ccBuilder CanonicalChainBuilder,
	milestone EventNewMilestone,
) error {
	// the milestone doesn't correspond to the tip of the chain
	// unwind to the previous verified milestone
	// and download the blocks of the new milestone
	rootNum := ccBuilder.Root().Number.Uint64()

	s.logger.Debug(
		syncLogPrefix("local chain tip does not match the milestone, unwinding to the previous verified root"),
		"rootNum", rootNum,
		"milestoneId", milestone.Id,
		"milestoneStart", milestone.StartBlock(),
		"milestoneEnd", milestone.EndBlock(),
		"milestoneRootHash", milestone.RootHash(),
	)

	if err := s.bridgeSync.Unwind(ctx, rootNum); err != nil {
		return err
	}

	newTip, err := s.blockDownloader.DownloadBlocksUsingMilestones(ctx, rootNum+1)
	if err != nil {
		return err
	}
	if newTip == nil {
		err = errors.New("unexpected empty headers from p2p since new milestone")
		return fmt.Errorf(
			"%w: rootNum=%d, milestoneId=%d, milestoneStart=%d, milestoneEnd=%d, milestoneRootHash=%s",
			err, rootNum, milestone.Id, milestone.StartBlock(), milestone.EndBlock(), milestone.RootHash(),
		)
	}

	if _, err := s.commitExecution(ctx, newTip, newTip); err != nil {
		// note: if we face a failure during execution of finalized waypoints blocks, it means that
		// we're wrong and the blocks are not considered as bad blocks, so we should terminate
		return s.handleWaypointExecutionErr(ctx, ccBuilder.Root(), err)
	}

	ccBuilder.Reset(newTip)
	return nil
}

func (s *Sync) applyNewMilestoneOnTip(
	ctx context.Context,
	event EventNewMilestone,
	ccBuilder CanonicalChainBuilder,
) error {
	milestone := event
	if milestone.EndBlock().Uint64() <= ccBuilder.Root().Number.Uint64() {
		return nil
	}

	s.logger.Debug(
		syncLogPrefix("applying new milestone event"),
		"milestoneId", milestone.RawId(),
		"milestoneStart", milestone.StartBlock().Uint64(),
		"milestoneEnd", milestone.EndBlock().Uint64(),
		"milestoneRootHash", milestone.RootHash(),
	)

	milestoneHeaders := ccBuilder.HeadersInRange(milestone.StartBlock().Uint64(), milestone.Length())
	if err := s.milestoneVerifier(milestone, milestoneHeaders); err != nil {
		return s.handleMilestoneTipMismatch(ctx, ccBuilder, milestone)
	}

	return ccBuilder.PruneRoot(milestone.EndBlock().Uint64())
}

func (s *Sync) applyNewBlockOnTip(
	ctx context.Context,
	event EventNewBlock,
	ccBuilder CanonicalChainBuilder,
) error {
	newBlockHeader := event.NewBlock.Header()
	newBlockHeaderNum := newBlockHeader.Number.Uint64()
	newBlockHeaderHash := newBlockHeader.Hash()
	rootNum := ccBuilder.Root().Number.Uint64()
	if newBlockHeaderNum <= rootNum || ccBuilder.ContainsHash(newBlockHeaderHash) {
		return nil
	}

	if s.badBlocks.Contains(newBlockHeaderHash) {
		s.logger.Warn(syncLogPrefix("bad block received from peer"),
			"blockHash", newBlockHeaderHash,
			"blockNum", newBlockHeaderNum,
			"peerId", event.PeerId,
		)
		s.maybePenalizePeerOnBadBlockEvent(ctx, event)
		return nil
	}

	if s.badBlocks.Contains(newBlockHeader.ParentHash) {
		s.logger.Warn(syncLogPrefix("block with bad parent received from peer"),
			"blockHash", newBlockHeaderHash,
			"blockNum", newBlockHeaderNum,
			"parentHash", newBlockHeader.ParentHash,
			"peerId", event.PeerId,
		)
		s.badBlocks.Add(newBlockHeaderHash, struct{}{})
		s.maybePenalizePeerOnBadBlockEvent(ctx, event)
		return nil
	}

	s.logger.Debug(
		syncLogPrefix("applying new block event"),
		"blockNum", newBlockHeaderNum,
		"blockHash", newBlockHeaderHash,
		"source", event.Source,
		"parentBlockHash", newBlockHeader.ParentHash,
	)

	var blockChain []*types.Block
	if ccBuilder.ContainsHash(newBlockHeader.ParentHash) {
		blockChain = []*types.Block{event.NewBlock}
	} else {
		amount := newBlockHeaderNum - rootNum + 1
		s.logger.Debug(
			syncLogPrefix("block parent hash not in ccb, fetching blocks backwards to root"),
			"rootNum", rootNum,
			"blockNum", newBlockHeaderNum,
			"blockHash", newBlockHeaderHash,
			"amount", amount,
		)

		if amount > 1024 {
			// should not ever need to request more than 1024 blocks here in order to backward connect
			// - if we do then we are missing milestones and need to investigate why
			// - additionally 1024 blocks should be enough to connect a new block at tip even without milestones
			// since we do not expect to see such large re-organisations
			// - if we ever do get a block from a peer for which 1024 blocks back is not enough to connect it
			// then we shall drop it as the canonical chain builder will fail to connect it and move on
			// useful read: https://forum.polygon.technology/t/proposal-improved-ux-with-milestones-for-polygon-pos/11534
			s.logger.Warn(syncLogPrefix("canonical chain builder root is too far"), "amount", amount)
			amount = 1024
		}

		opts := []p2p.FetcherOption{p2p.WithMaxRetries(0), p2p.WithResponseTimeout(time.Second)}
		blocks, err := s.p2pService.FetchBlocksBackwardsByHash(ctx, newBlockHeaderHash, amount, event.PeerId, opts...)
		if err != nil {
			if s.ignoreFetchBlocksErrOnTipEvent(err) {
				s.logger.Debug(
					syncLogPrefix("applyNewBlockOnTip: failed to fetch complete blocks, ignoring event"),
					"err", err,
					"peerId", event.PeerId,
					"lastBlockNum", newBlockHeaderNum,
				)

				return nil
			}

			return err
		}

		blockChain = blocks.Data
	}

	if err := s.blocksVerifier(blockChain); err != nil {
		s.logger.Debug(
			syncLogPrefix("applyNewBlockOnTip: invalid new block event from peer, penalizing and ignoring"),
			"err", err,
		)

		if err = s.p2pService.Penalize(ctx, event.PeerId); err != nil {
			s.logger.Debug(syncLogPrefix("applyNewBlockOnTip: issue with penalizing peer"), "err", err)
		}

		return nil
	}

	headerChain := make([]*types.Header, len(blockChain))
	for i, block := range blockChain {
		headerChain[i] = block.HeaderNoCopy()
	}

	oldTip := ccBuilder.Tip()
	newConnectedHeaders, err := ccBuilder.Connect(ctx, headerChain)
	if err != nil {
		s.logger.Debug(
			syncLogPrefix("applyNewBlockOnTip: couldn't connect a header to the local chain tip, ignoring"),
			"err", err,
		)

		return nil
	}
	if len(newConnectedHeaders) == 0 {
		return nil
	}

	newTip := ccBuilder.Tip()
	firstConnectedHeader := newConnectedHeaders[0]
	if newTip != oldTip && oldTip.Hash() != firstConnectedHeader.ParentHash {
		if err := s.handleTipForkChangeUnwinds(ctx, ccBuilder, oldTip); err != nil {
			return err
		}
	}

	// len(newConnectedHeaders) is always <= len(blockChain)
	newConnectedBlocks := blockChain[len(blockChain)-len(newConnectedHeaders):]
	if err := s.store.InsertBlocks(ctx, newConnectedBlocks); err != nil {
		return err
	}

	if event.Source == EventSourceP2PNewBlock {
		// https://github.com/ethereum/devp2p/blob/master/caps/eth.md#block-propagation
		// devp2p spec: when a NewBlock announcement message is received from a peer, the client first verifies the
		// basic header validity of the block, checking whether the proof-of-work value is valid (replace PoW
		// with Bor rules that we do as part of CanonicalChainBuilder.Connect).
		// It then sends the block to a small fraction of connected peers (usually the square root of the total
		// number of peers) using the NewBlock message.
		// note, below is non-blocking
		go s.publishNewBlock(ctx, event.NewBlock)
	}

	if newTip == oldTip {
		lastConnectedNum := newConnectedHeaders[len(newConnectedHeaders)-1].Number.Uint64()
		if tipNum := newTip.Number.Uint64(); lastConnectedNum > tipNum {
			return s.handleInsertBlocksAfterUnchangedTip(ctx, tipNum, lastConnectedNum)
		}

		return nil
	}

	if latestValidHash, err := s.commitExecution(ctx, newTip, ccBuilder.Root()); err != nil {
		if errors.Is(err, ErrForkChoiceUpdateBadBlock) {
			return s.handleBadBlockErr(ctx, ccBuilder, event, latestValidHash, err)
		}

		return err
	}

	if event.Source == EventSourceP2PNewBlock {
		// https://github.com/ethereum/devp2p/blob/master/caps/eth.md#block-propagation
		// devp2p spec: After the header validity check, the client imports the block into its local chain by executing
		// all transactions contained in the block, computing the block's 'post state'. The block's state-root hash
		// must match the computed post state root. Once the block is fully processed, and considered valid,
		// the client sends a NewBlockHashes message about the block to all peers which it didn't notify earlier.
		// Those peers may request the full block later if they fail to receive it via NewBlock from anyone else.
		// Including hashes that the sending node later refuses to honour with a proceeding GetBlockHeaders
		// message is considered bad form, and may reduce the reputation of the sending node.
		// note, below is non-blocking
		s.p2pService.PublishNewBlockHashes(event.NewBlock)
	}

	return nil
}

func (s *Sync) applyNewBlockHashesOnTip(
	ctx context.Context,
	event EventNewBlockHashes,
	ccBuilder CanonicalChainBuilder,
) error {
	for _, hashOrNum := range event.NewBlockHashes {
		if (hashOrNum.Number <= ccBuilder.Root().Number.Uint64()) || ccBuilder.ContainsHash(hashOrNum.Hash) {
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
			return nil
		}

		s.logger.Debug(
			syncLogPrefix("applying new block hash event"),
			"blockNum", hashOrNum.Number,
			"blockHash", hashOrNum.Hash,
		)

		fetchOpts := []p2p.FetcherOption{p2p.WithMaxRetries(0), p2p.WithResponseTimeout(time.Second)}
		newBlocks, err := s.p2pService.FetchBlocksBackwardsByHash(ctx, hashOrNum.Hash, 1, event.PeerId, fetchOpts...)
		if err != nil {
			if s.ignoreFetchBlocksErrOnTipEvent(err) {
				s.logger.Debug(
					syncLogPrefix("applyNewBlockHashesOnTip: failed to fetch complete blocks, ignoring event"),
					"err", err,
					"peerId", event.PeerId,
					"lastBlockNum", hashOrNum.Number,
				)

				continue
			}

			return err
		}

		newBlockEvent := EventNewBlock{
			NewBlock: newBlocks.Data[0],
			PeerId:   event.PeerId,
			Source:   EventSourceP2PNewBlockHashes,
		}

		err = s.applyNewBlockOnTip(ctx, newBlockEvent, ccBuilder)
		if err != nil {
			return err
		}
	}
	return nil
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

func (s *Sync) handleTipForkChangeUnwinds(ctx context.Context, ccb CanonicalChainBuilder, oldTip *types.Header) error {
	// forks have changed, we need to unwind unwindable data
	newTip := ccb.Tip()
	newTipNum := newTip.Number.Uint64()
	newTipHash := newTip.Hash()
	oldTipNum := oldTip.Number.Uint64()
	oldTipHash := oldTip.Hash()
	s.logger.Debug(
		syncLogPrefix("fork change"),
		"oldNum", oldTipNum,
		"oldHash", oldTipHash,
		"newNum", newTipNum,
		"newHash", newTipHash,
	)

	lca, ok := ccb.LowestCommonAncestor(newTipHash, oldTipHash)
	if !ok {
		return fmt.Errorf("could not find lowest common ancestor of old and new tip")
	}

	lcaNum := lca.Number.Uint64()
	start := lcaNum + 1
	if newTipNum < start { // defensive check against underflow & unexpected lcaNum
		return fmt.Errorf("unexpected newTipNum < start: %d < %d", oldTipNum, start)
	}

	amount := newTipNum - start + 1
	canonicalHeaders := ccb.HeadersInRange(start, amount)
	if uint64(len(canonicalHeaders)) != amount {
		return fmt.Errorf("expected %d canonical headers", amount)
	}

	if err := s.bridgeSync.Unwind(ctx, lcaNum); err != nil {
		return err
	}

	canonicalBlocksToReplay := make([]*types.Block, len(canonicalHeaders))
	for i, header := range canonicalHeaders {
		canonicalBlocksToReplay[i] = types.NewBlockWithHeader(header)
	}

	if err := s.bridgeSync.ProcessNewBlocks(ctx, canonicalBlocksToReplay); err != nil {
		return err
	}

	return s.bridgeSync.Synchronize(ctx, oldTipNum)
}

func (s *Sync) handleInsertBlocksAfterUnchangedTip(ctx context.Context, tipNum, lastInsertedNum uint64) error {
	// this is a hack that should disappear when changing the bridge to not track blocks (future work)
	// make sure the bridge does not go past the tip (it may happen when we insert blocks from another fork that
	// has a higher block number than the canonical tip but lower difficulty) - this is to prevent the bridge
	// from recording incorrect bor txn hashes
	s.logger.Debug(
		syncLogPrefix("unwinding back bridge due to inserting headers past the tip"),
		"tip", tipNum,
		"lastInsertedNum", lastInsertedNum,
	)

	// wait for the insert blocks flush
	if err := s.store.Flush(ctx); err != nil {
		return err
	}

	// wait for the bridge processing
	if err := s.bridgeSync.Synchronize(ctx, lastInsertedNum); err != nil {
		return err
	}

	return s.bridgeSync.Unwind(ctx, tipNum)
}

func (s *Sync) handleBadBlockErr(
	ctx context.Context,
	ccb CanonicalChainBuilder,
	event EventNewBlock,
	latestValidHash common.Hash,
	badBlockErr error,
) error {
	s.logger.Warn(
		syncLogPrefix("bad block after execution"),
		"peerId", event.PeerId,
		"latestValidHash", latestValidHash,
		"err", badBlockErr,
	)

	s.badBlocks.Add(event.NewBlock.Hash(), struct{}{})
	s.maybePenalizePeerOnBadBlockEvent(ctx, event)
	latestValidHeader, ok := ccb.HeaderByHash(latestValidHash)
	if !ok {
		return fmt.Errorf("unexpected latestValidHash not in canonical builder: %s", latestValidHash)
	}

	badTipNum := ccb.Tip().Number.Uint64()
	start := latestValidHeader.Number.Uint64() + 1
	if badTipNum < start { // defensive check against underflow & unexpected badTipNum and latestValidHeader
		return fmt.Errorf("unexpected badTipNum < start: %d < %d", badTipNum, start)
	}

	amount := badTipNum - start + 1
	badHeaders := ccb.HeadersInRange(start, amount)
	if uint64(len(badHeaders)) != amount {
		return fmt.Errorf("expected %d bad headers after bad block err", amount)
	}

	if err := ccb.PruneNode(badHeaders[0].Hash()); err != nil {
		return err
	}

	tipPostPrune := ccb.Tip()
	return s.bridgeSync.Unwind(ctx, tipPostPrune.Number.Uint64())
}

func (s *Sync) maybePenalizePeerOnBadBlockEvent(ctx context.Context, event EventNewBlock) {
	if event.Source == EventSourceP2PNewBlockHashes {
		// note: we do not penalize peer for bad blocks on new block hash events since they have
		// not necessarily been executed by the peer but just propagated as per the devp2p spec
		return
	}

	if err := s.p2pService.Penalize(ctx, event.PeerId); err != nil {
		s.logger.Debug(syncLogPrefix("issue with penalizing peer for bad block"), "peerId", event.PeerId, "err", err)
	}
}

//
// TODO (subsequent PRs) - unit test initial sync + on new event cases
//

func (s *Sync) Run(ctx context.Context) error {
	s.logger.Debug(syncLogPrefix("running sync component"))

	result, err := s.syncToTip(ctx)
	if err != nil {
		return err
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
		case event := <-s.events:
			switch event.Type {
			case EventTypeNewMilestone:
				if err = s.applyNewMilestoneOnTip(ctx, event.AsNewMilestone(), ccBuilder); err != nil {
					return err
				}
			case EventTypeNewBlock:
				if err = s.applyNewBlockOnTip(ctx, event.AsNewBlock(), ccBuilder); err != nil {
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
func (s *Sync) initialiseCcb(ctx context.Context, result syncToTipResult) (CanonicalChainBuilder, error) {
	tip := result.latestTip
	tipNum := tip.Number.Uint64()
	rootNum := result.latestWaypoint.EndBlock().Uint64()
	if rootNum > tipNum {
		return nil, fmt.Errorf("unexpected rootNum > tipNum: %d > %d", rootNum, tipNum)
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
	startTime := time.Now()
	latestTipOnStart, err := s.execution.CurrentHeader(ctx)
	if err != nil {
		return syncToTipResult{}, err
	}

	result, err := s.syncToTipUsingCheckpoints(ctx, latestTipOnStart)
	if err != nil {
		return syncToTipResult{}, err
	}

	result, err = s.syncToTipUsingMilestones(ctx, result.latestTip)
	if err != nil {
		return syncToTipResult{}, err
	}

	blocks := result.latestTip.Number.Uint64() - latestTipOnStart.Number.Uint64()
	s.logger.Info(
		syncLogPrefix("sync to tip finished"),
		"time", common.PrettyAge(startTime),
		"blocks", blocks,
		"blk/sec", uint64(float64(blocks)/time.Since(startTime).Seconds()),
	)

	return result, nil
}

func (s *Sync) syncToTipUsingCheckpoints(ctx context.Context, tip *types.Header) (syncToTipResult, error) {
	return s.sync(ctx, tip, func(ctx context.Context, startBlockNum uint64) (syncToTipResult, error) {
		latestCheckpoint, err := s.heimdallSync.SynchronizeCheckpoints(ctx)
		if err != nil {
			return syncToTipResult{}, err
		}

		tip, err := s.blockDownloader.DownloadBlocksUsingCheckpoints(ctx, startBlockNum)
		if err != nil {
			return syncToTipResult{}, err
		}

		return syncToTipResult{latestTip: tip, latestWaypoint: latestCheckpoint}, nil
	})
}

func (s *Sync) syncToTipUsingMilestones(ctx context.Context, tip *types.Header) (syncToTipResult, error) {
	return s.sync(ctx, tip, func(ctx context.Context, startBlockNum uint64) (syncToTipResult, error) {
		latestMilestone, err := s.heimdallSync.SynchronizeMilestones(ctx)
		if err != nil {
			return syncToTipResult{}, err
		}

		tip, err := s.blockDownloader.DownloadBlocksUsingMilestones(ctx, startBlockNum)
		if err != nil {
			return syncToTipResult{}, err
		}

		return syncToTipResult{latestTip: tip, latestWaypoint: latestMilestone}, nil
	})
}

type tipDownloaderFunc func(ctx context.Context, startBlockNum uint64) (syncToTipResult, error)

func (s *Sync) sync(ctx context.Context, tip *types.Header, tipDownloader tipDownloaderFunc) (syncToTipResult, error) {
	var latestWaypoint heimdall.Waypoint
	for {
		newResult, err := tipDownloader(ctx, tip.Number.Uint64()+1)
		if err != nil {
			return syncToTipResult{}, err
		}

		latestWaypoint = newResult.latestWaypoint

		if newResult.latestTip == nil {
			// we've reached the tip
			break
		}

		newTip := newResult.latestTip
		if _, err := s.commitExecution(ctx, newTip, newTip); err != nil {
			// note: if we face a failure during execution of finalized waypoints blocks, it means that
			// we're wrong and the blocks are not considered as bad blocks, so we should terminate
			err = s.handleWaypointExecutionErr(ctx, tip, err)
			return syncToTipResult{}, err
		}

		tip = newTip
	}

	return syncToTipResult{latestTip: tip, latestWaypoint: latestWaypoint}, nil
}

func (s *Sync) handleWaypointExecutionErr(ctx context.Context, lastCorrectTip *types.Header, execErr error) error {
	if !errors.Is(execErr, ErrForkChoiceUpdateBadBlock) {
		return execErr
	}

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
