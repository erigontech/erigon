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
		logger:            logger,
	}
}

func (s *Sync) commitExecution(ctx context.Context, newTip *types.Header, finalizedHeader *types.Header) error {
	if err := s.store.Flush(ctx); err != nil {
		return err
	}

	blockNum := newTip.Number.Uint64()

	if err := s.heimdallSync.SynchronizeSpans(ctx, blockNum); err != nil {
		return err
	}

	if err := s.bridgeSync.Synchronize(ctx, blockNum); err != nil {
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

	if err = s.commitExecution(ctx, newTip, newTip); err != nil {
		return err
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
		"id", milestone.RawId(),
		"start", milestone.StartBlock().Uint64(),
		"end", milestone.EndBlock().Uint64(),
		"root", milestone.RootHash(),
	)

	milestoneHeaders := ccBuilder.HeadersInRange(milestone.StartBlock().Uint64(), milestone.Length())
	if err := s.milestoneVerifier(milestone, milestoneHeaders); err != nil {
		return s.handleMilestoneTipMismatch(ctx, ccBuilder, milestone)
	}

	return ccBuilder.Prune(milestone.EndBlock().Uint64())
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
		// forks have changed, we need to unwind unwindable data
		blockNum := max(1, firstConnectedHeader.Number.Uint64()) - 1
		if err := s.bridgeSync.Unwind(ctx, blockNum); err != nil {
			return err
		}
	}

	// len(newConnectedHeaders) is always <= len(blockChain)
	newConnectedBlocks := blockChain[len(blockChain)-len(newConnectedHeaders):]
	if err := s.store.InsertBlocks(ctx, newConnectedBlocks); err != nil {
		return err
	}

	if newTip == oldTip {
		return nil
	}

	return s.commitExecution(ctx, newTip, ccBuilder.Root())
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

		s.logger.Debug(
			syncLogPrefix("applying new block hash"),
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
			}
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

		tip = newResult.latestTip
		if err = s.commitExecution(ctx, tip, tip); err != nil {
			return syncToTipResult{}, err
		}
	}

	return syncToTipResult{latestTip: tip, latestWaypoint: latestWaypoint}, nil
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
