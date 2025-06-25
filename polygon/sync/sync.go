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
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/protocols/eth"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/p2p"
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

type heimdallSynchronizer interface {
	IsCatchingUp(ctx context.Context) (bool, error)
	SynchronizeCheckpoints(ctx context.Context) (latest *heimdall.Checkpoint, ok bool, err error)
	SynchronizeMilestones(ctx context.Context) (latest *heimdall.Milestone, ok bool, err error)
	SynchronizeSpans(ctx context.Context, blockNum uint64) error
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
	events <-chan Event,
	notifications *shards.Notifications,
	wiggleCalculator wiggleCalculator,
	engineAPISwitcher EngineAPISwitcher,
) *Sync {
	badBlocksLru, err := simplelru.NewLRU[common.Hash, struct{}](1024, nil)
	if err != nil {
		panic(err)
	}

	return &Sync{
		config:            config,
		logger:            logger,
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
		notifications:     notifications,
		wiggleCalculator:  wiggleCalculator,
		engineAPISwitcher: engineAPISwitcher,
	}
}

type Sync struct {
	config            *ethconfig.Config
	logger            log.Logger
	store             Store
	execution         ExecutionClient
	milestoneVerifier WaypointHeadersVerifier
	blocksVerifier    BlocksVerifier
	p2pService        p2pService
	blockDownloader   *BlockDownloader
	ccBuilderFactory  CanonicalChainBuilderFactory
	heimdallSync      heimdallSynchronizer
	bridgeSync        bridgeSynchronizer
	events            <-chan Event
	badBlocks         *simplelru.LRU[common.Hash, struct{}]
	notifications     *shards.Notifications
	wiggleCalculator  wiggleCalculator
	engineAPISwitcher EngineAPISwitcher
}

func (s *Sync) commitExecution(ctx context.Context, newTip *types.Header, finalizedHeader *types.Header) error {
	if err := s.store.Flush(ctx); err != nil {
		return err
	}

	blockNum := newTip.Number.Uint64()
	if err := s.heimdallSync.SynchronizeSpans(ctx, blockNum); err != nil {
		return err
	}

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
	)

	// wait for any possibly unprocessed previous block inserts to finish
	if err := s.store.Flush(ctx); err != nil {
		return err
	}

	if err := s.bridgeSync.Unwind(ctx, rootNum); err != nil {
		return err
	}

	var syncTo *uint64

	if s.config.PolygonPosSingleSlotFinality {
		syncTo = &s.config.PolygonPosSingleSlotFinalityBlockAt
	}

	newTip, err := s.blockDownloader.DownloadBlocksUsingMilestones(ctx, rootNum+1, syncTo)
	if err != nil {
		return err
	}
	if newTip == nil {
		err = errors.New("unexpected empty headers from p2p since new milestone")
		return fmt.Errorf(
			"%w: rootNum=%d, milestoneId=%d, milestoneStart=%d, milestoneEnd=%d, milestoneRootHash=%s",
			err, rootNum, event.Id, event.StartBlock(), event.EndBlock(), event.RootHash(),
		)
	}

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

func (s *Sync) applyNewBlockOnTip(ctx context.Context, event EventNewBlock, ccb *CanonicalChainBuilder) error {
	newBlockHeader := event.NewBlock.HeaderNoCopy()
	newBlockHeaderNum := newBlockHeader.Number.Uint64()
	newBlockHeaderHash := newBlockHeader.Hash()
	rootNum := ccb.Root().Number.Uint64()
	if newBlockHeaderNum <= rootNum || ccb.ContainsHash(newBlockHeaderHash) {
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
	if ccb.ContainsHash(newBlockHeader.ParentHash) {
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

		opts := []p2p.FetcherOption{p2p.WithMaxRetries(0), p2p.WithResponseTimeout(5 * time.Second)}

		// This used to be limited to 1024 blocks (eth.MaxHeadersServe) however for the heimdall v1-v2 migration
		// this limit on backward downloading does not holde so it has been adjusted to recieve several pages
		// of 1024 blocks until the gap is filled.  For this one off case the gap was ~15,000 blocks.  If this
		// ever grows substantially this will need to be revisited:
		// 1. If we need to page we should requests from may peers
		// 2. We need to do something about memory at the moment this is unconstrained

		fetchHeaderHash := newBlockHeaderHash
		for amount > 0 {
			fetchAmount := amount

			if fetchAmount > eth.MaxHeadersServe {
				fetchAmount = eth.MaxHeadersServe
			}

			blocks, err := s.p2pService.FetchBlocksBackwardsByHash(ctx, fetchHeaderHash, fetchAmount, event.PeerId, opts...)
			if err != nil || len(blocks.Data) == 0 {
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

			blockChain = append(blocks.Data, blockChain...)
			fetchHeaderHash = blocks.Data[0].ParentHash()
			amount -= uint64(len(blocks.Data))
		}
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
	if len(newConnectedBlocks) > 1 {
		s.logger.Info(
			syncLogPrefix("inserting multiple connected blocks"),
			"amount", len(newConnectedBlocks),
			"start", newConnectedBlocks[0].NumberU64(),
			"end", newConnectedBlocks[len(newConnectedBlocks)-1].NumberU64(),
		)
	}
	if err := s.store.InsertBlocks(ctx, newConnectedBlocks); err != nil {
		return err
	}

	if event.Source == EventSourceBlockProducer {
		go s.publishNewBlock(ctx, event.NewBlock)
		go s.p2pService.PublishNewBlockHashes(event.NewBlock)
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
			return s.handleBridgeOnBlocksInsertAheadOfTip(ctx, tipNum, lastConnectedNum)
		}

		return nil
	}

	if err := s.commitExecution(ctx, newTip, ccb.Root()); err != nil {
		if errors.Is(err, ErrForkChoiceUpdateBadBlock) {
			return s.handleBadBlockErr(ctx, ccb, event, firstNewConnectedHeader, oldTip, err)
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

		err = s.applyNewBlockOnTip(ctx, newBlockEvent, ccb)
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
	event EventNewBlock,
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
		"peerId", event.PeerId,
		"badTipNum", badTip.Number.Uint64(),
		"badTipHash", badTipHash,
		"oldTipNum", oldTipNum,
		"oldTipHash", oldTipHash,
		"firstNewConnectedNum", firstNewConnectedHeader.Number.Uint64(),
		"firstNewConnectedHash", firstNewConnectedHeader.Hash(),
		"err", badBlockErr,
	)

	// 1. Mark block as bad and penalize peer
	s.badBlocks.Add(event.NewBlock.Hash(), struct{}{})
	s.maybePenalizePeerOnBadBlockEvent(ctx, event)

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

func (s *Sync) maybePenalizePeerOnBadBlockEvent(ctx context.Context, event EventNewBlock) {
	if event.Source == EventSourceP2PNewBlockHashes {
		// note: we do not penalize peer for bad blocks on new block hash events since they have
		// not necessarily been executed by the peer but just propagated as per the devp2p spec
		return
	}

	s.logger.Debug(syncLogPrefix("penalizing peer for bad block"), "peerId", event.PeerId)
	if err := s.p2pService.Penalize(ctx, event.PeerId); err != nil {
		s.logger.Debug(syncLogPrefix("issue with penalizing peer for bad block"), "peerId", event.PeerId, "err", err)
	}
}

//
// TODO (subsequent PRs) - unit test initial sync + on new event cases
//

func (s *Sync) Run(ctx context.Context) error {
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

	// This is set to disable heimdall checks casuing a pause during transition
	// it can be removed post July 2025 where behavior can revert to previous
	// behavior of waitiing for heimdall to sync
	const HiemdalV1V2Transition = true

	for {
		// we have to check if the heimdall we are connected to is synchonised with the chain
		// to prevent getting empty list of checkpoints/milestones during the sync

		catchingUp, err := s.heimdallSync.IsCatchingUp(ctx)
		if err != nil {
			return err
		}

		if !catchingUp {
			break
		}

		s.logger.Warn(syncLogPrefix("your heimdalld process is behind, please check its logs and <HEIMDALL_HOST>:1317/status api"))

		if HiemdalV1V2Transition {
			break
		}

		if err := libcommon.Sleep(ctx, 30*time.Second); err != nil {
			return err
		}
	}

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
		case event := <-s.events:
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

	if result.latestTip != nil {
		if err := s.heimdallSync.SynchronizeSpans(ctx, result.latestTip.Number.Uint64()); err != nil {
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
			if errors.Is(err, ErrUfcTooFarBehind) {
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
