// Copyright 2026 The Erigon Authors
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

package bscsync

import (
	"context"
	"errors"
	"fmt"
	"time"

	lru "github.com/hashicorp/golang-lru/v2"
	"github.com/hashicorp/golang-lru/v2/simplelru"

	"github.com/erigontech/erigon/bsc/parlia"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/execmodule"
	"github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/types"
)

const (
	downloadRequestsCacheSize = 1024
	maxBlockBatchDownloadSize = 256
	tipRetryInterval          = time.Second
	tipFallbackDistance       = 64
	tipStallTimeout           = 30 * time.Second
)

var (
	// errFallBack ends the tip loop so that the driver catches up again.
	errFallBack           = errors.New("fall back to catch-up")
	errAlreadyProcessed   = errors.New("already processed")
	errKnownBadBlock      = errors.New("known bad block")
	errKnowBadParentBlock = errors.New("known bad parent block")
)

type peerPenalizer interface {
	Penalize(ctx context.Context, peerId *p2p.PeerId) error
}

// backwardFetchFunc downloads the chain that ends at hash, down to a block whose parent hr
// knows, and passes it to consume in ascending batches.
type backwardFetchFunc func(ctx context.Context, hash common.Hash, hr p2p.BbdHeaderReader, limit uint64, peerId *p2p.PeerId, consume func([]*types.Block) error) error

type backwardDownloader interface {
	FetchBlocksBackwards(ctx context.Context, hash common.Hash, hr p2p.BbdHeaderReader, opts ...p2p.BbdOption) (p2p.BbdResultFeed, error)
}

func fetchBackwardsWith(d backwardDownloader) backwardFetchFunc {
	return func(ctx context.Context, hash common.Hash, hr p2p.BbdHeaderReader, limit uint64, peerId *p2p.PeerId, consume func([]*types.Block) error) error {
		feed, err := d.FetchBlocksBackwards(
			ctx,
			hash,
			hr,
			p2p.WithChainLengthLimit(limit),
			p2p.WithBlocksBatchSize(min(limit, maxBlockBatchDownloadSize)),
			p2p.WithPeerId(peerId),
		)
		if err != nil {
			return err
		}
		for {
			batch, err := feed.Next(ctx)
			if err != nil {
				return err
			}
			if len(batch.Blocks) == 0 {
				return nil
			}
			if err := consume(batch.Blocks); err != nil {
				return err
			}
		}
	}
}

// tipSync follows the chain tip from pushed blocks. It is a port of the Astrid tip loop:
// connect blocks to the canonical chain builder, download missing parents backwards, insert
// the blocks and move the head with one forkchoice update per batch of queued events.
type tipSync struct {
	logger         log.Logger
	chainRW        chainReaderWriter
	peers          peerPenalizer
	fetchBackwards backwardFetchFunc
	tipEvents      *TipEvents
	db             parlia.Chain
	verifier       HeaderVerifier

	ccb                *CanonicalChainBuilder
	downloadCtx        context.Context
	committed          *types.Header
	lastProgress       time.Time
	inserted           int
	tipSource          EventSource
	tipPeer            *p2p.PeerId
	badBlocks          *simplelru.LRU[common.Hash, struct{}]
	blockRequestsCache *lru.Cache[common.Hash, struct{}]
}

func newTipSync(
	logger log.Logger,
	chainRW chainReaderWriter,
	peers peerPenalizer,
	fetchBackwards backwardFetchFunc,
	tipEvents *TipEvents,
	db parlia.Chain,
	verifier HeaderVerifier,
) *tipSync {
	badBlocks, err := simplelru.NewLRU[common.Hash, struct{}](1024, nil)
	if err != nil {
		panic(err)
	}
	blockRequestsCache, err := lru.New[common.Hash, struct{}](downloadRequestsCacheSize)
	if err != nil {
		panic(err)
	}
	return &tipSync{
		logger:             logger,
		chainRW:            chainRW,
		peers:              peers,
		fetchBackwards:     fetchBackwards,
		tipEvents:          tipEvents,
		db:                 db,
		verifier:           verifier,
		badBlocks:          badBlocks,
		blockRequestsCache: blockRequestsCache,
	}
}

// run follows the tip from head until ctx is cancelled or the loop has to fall back to
// catch-up, which it reports with errFallBack.
func (s *tipSync) run(ctx context.Context, head *types.Header) error {
	ccb, err := InitialiseCCB(ctx, s.db, s.verifier, head)
	if err != nil {
		return err
	}
	s.ccb = ccb
	s.committed = head
	s.lastProgress = time.Now()
	s.blockRequestsCache.Purge()
	downloadCtx, cancelDownloads := context.WithCancel(ctx)
	defer cancelDownloads()
	s.downloadCtx = downloadCtx

	for {
		waitCtx, cancel := context.WithTimeout(ctx, tipRetryInterval)
		events, err := s.tipEvents.Next(waitCtx)
		cancel()
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		for i := range events {
			if err := s.applyEvent(ctx, &events[i]); err != nil {
				return err
			}
		}
		if err := s.commit(ctx); err != nil {
			return err
		}
		if err := s.checkBehind(); err != nil {
			return err
		}
	}
}

// checkBehind reports errFallBack when the committed head is more than tipFallbackDistance
// blocks behind the announced head, or has not moved for tipStallTimeout although peers
// announce a higher head.
func (s *tipSync) checkBehind() error {
	announced, ok := s.tipEvents.AnnouncedHead()
	committed := s.committed.Number.Uint64()
	if !ok || announced <= committed {
		s.lastProgress = time.Now()
		return nil
	}
	if announced > committed+tipFallbackDistance {
		return fmt.Errorf("%w: head %d is %d blocks behind the announced head %d", errFallBack, committed, announced-committed, announced)
	}
	if stalled := time.Since(s.lastProgress); stalled > tipStallTimeout {
		return fmt.Errorf("%w: head %d has not moved for %s, announced head %d", errFallBack, committed, stalled.Round(time.Second), announced)
	}
	return nil
}

func (s *tipSync) applyEvent(ctx context.Context, event *Event) error {
	switch event.Type {
	case EventTypeNewBlock:
		return s.applyNewBlockOnTip(ctx, event.AsNewBlock())
	case EventTypeNewBlockBatch:
		return s.applyNewBlockBatchOnTip(ctx, event.AsNewBlockBatch())
	case EventTypeNewBlockHashes:
		s.applyNewBlockHashesOnTip(ctx, event.AsNewBlockHashes())
		return nil
	default:
		panic(fmt.Sprintf("unexpected event type: %v", event.Type))
	}
}

// commit moves the exec head to the chain builder's tip. Busy and TooFarAway leave the tip
// uncommitted, so the next round retries it.
func (s *tipSync) commit(ctx context.Context) error {
	tip := s.ccb.Tip()
	if tip.Hash() == s.committed.Hash() {
		return nil
	}
	var safe, finalized common.Hash
	var justifiedNum, finalizedNum uint64
	if h := s.ccb.Justified(); h != nil {
		safe, justifiedNum = h.Hash(), h.Number.Uint64()
	}
	if h := s.ccb.Finalized(); h != nil {
		finalized, finalizedNum = h.Hash(), h.Number.Uint64()
	}

	start := time.Now()
	status, valErr, _, err := s.chainRW.UpdateForkChoice(ctx, tip.Hash(), safe, finalized)
	if err != nil {
		return err
	}
	switch status {
	case execmodule.ExecutionStatusSuccess:
		s.logger.Info("[bsc] tip updated",
			"number", tip.Number.Uint64(),
			"hash", tip.Hash(),
			"age", time.Since(time.UnixMilli(int64(parlia.MilliTimestamp(tip)))).Round(time.Millisecond),
			"justified", justifiedNum,
			"finalized", finalizedNum,
			"inserted", s.inserted,
			"in", time.Since(start).Round(time.Millisecond),
		)
		s.committed = tip
		s.lastProgress = time.Now()
		s.inserted = 0
		return s.ccb.PruneToFinalized()
	case execmodule.ExecutionStatusBusy, execmodule.ExecutionStatusTooFarAway:
		s.logger.Debug("[bsc] forkchoice update deferred", "block", tip.Number.Uint64(), "hash", tip.Hash(), "status", status)
		return nil
	case execmodule.ExecutionStatusBadBlock:
		return s.handleBadBlock(ctx, tip, valErr)
	default:
		msg := ""
		if valErr != nil {
			msg = *valErr
		}
		s.logger.Warn("[bsc] forkchoice rejected tip", "block", tip.Number.Uint64(), "hash", tip.Hash(), "status", status, "err", msg)
		return fmt.Errorf("%w: forkchoice status %s", errFallBack, status)
	}
}

func (s *tipSync) applyNewBlockChainOnTip(ctx context.Context, blockChain []*types.Block, source EventSource, peerId *p2p.PeerId) error {
	if err := verifyChain(nil, blockChain); err != nil {
		s.logger.Debug("[bsc] invalid new block event from peer, penalizing and ignoring", "err", err)
		s.penalize(ctx, peerId)
		return nil
	}

	headerChain := make([]*types.Header, len(blockChain))
	for i, block := range blockChain {
		headerChain[i] = block.HeaderNoCopy()
	}

	oldTip := s.ccb.Tip()
	newConnectedHeaders, err := s.ccb.Connect(ctx, headerChain)
	if err != nil {
		s.logger.Debug(
			"[bsc] couldn't connect header chain to the local chain tip",
			"partiallyConnected", len(newConnectedHeaders),
			"err", err,
		)
		s.maybePenalizePeerOnBadBlockEvent(ctx, source, peerId)
	}
	if len(newConnectedHeaders) == 0 {
		return nil
	}

	newBlocksStartIdx := newConnectedHeaders[0].Number.Uint64() - blockChain[0].NumberU64()
	newBlocksEndIdx := newBlocksStartIdx + uint64(len(newConnectedHeaders))
	if err := s.chainRW.InsertBlocks(ctx, blockChain[newBlocksStartIdx:newBlocksEndIdx]); err != nil {
		return err
	}
	s.inserted += len(newConnectedHeaders)

	if s.ccb.Tip() != oldTip {
		s.tipSource = source
		s.tipPeer = peerId
	}
	return nil
}

// checkNewBlockHeader rejects headers at or below the root, already connected headers and
// known bad blocks or their children.
func (s *tipSync) checkNewBlockHeader(ctx context.Context, newBlockHeader *types.Header, eventSource EventSource, peerId *p2p.PeerId) error {
	newBlockHeaderNum := newBlockHeader.Number.Uint64()
	newBlockHeaderHash := newBlockHeader.Hash()
	rootNum := s.ccb.Root().Number.Uint64()
	if newBlockHeaderNum <= rootNum || s.ccb.ContainsHash(newBlockHeaderHash) {
		return errAlreadyProcessed
	}

	if s.badBlocks.Contains(newBlockHeaderHash) {
		s.logger.Warn("[bsc] bad block received from peer",
			"blockHash", newBlockHeaderHash,
			"blockNum", newBlockHeaderNum,
			"peerId", peerId,
		)
		s.maybePenalizePeerOnBadBlockEvent(ctx, eventSource, peerId)
		return errKnownBadBlock
	}

	if s.badBlocks.Contains(newBlockHeader.ParentHash) {
		s.logger.Warn("[bsc] block with bad parent received from peer",
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

func (s *tipSync) applyNewBlockOnTip(ctx context.Context, event EventNewBlock) error {
	newBlockHeader := event.NewBlock.HeaderNoCopy()
	if err := s.checkNewBlockHeader(ctx, newBlockHeader, event.Source, event.PeerId); err != nil {
		return nil
	}
	if s.ccb.ContainsHash(newBlockHeader.ParentHash) {
		return s.applyNewBlockChainOnTip(ctx, []*types.Block{event.NewBlock}, event.Source, event.PeerId)
	}
	s.asyncBackwardDownloadBlockBatches(newBlockHeader.Hash(), newBlockHeader.Number.Uint64(), event.PeerId, event.Source)
	return nil
}

func (s *tipSync) applyNewBlockBatchOnTip(ctx context.Context, event EventNewBlockBatch) error {
	var err error
	defer func() {
		if err == nil || errors.Is(err, errAlreadyProcessed) {
			close(event.Processed)
			return
		}
		event.Processed <- err
	}()
	if len(event.NewBlocks) == 0 {
		return nil
	}
	newBlockHeader := event.NewBlocks[len(event.NewBlocks)-1].HeaderNoCopy()
	if err = s.checkNewBlockHeader(ctx, newBlockHeader, event.Source, event.PeerId); err != nil {
		return nil
	}
	err = s.applyNewBlockChainOnTip(ctx, event.NewBlocks, event.Source, event.PeerId)
	return err
}

func (s *tipSync) applyNewBlockHashesOnTip(ctx context.Context, event EventNewBlockHashes) {
	for _, hashOrNum := range event.NewBlockHashes {
		if (hashOrNum.Number <= s.ccb.Root().Number.Uint64()) || s.ccb.ContainsHash(hashOrNum.Hash) {
			continue
		}

		if s.badBlocks.Contains(hashOrNum.Hash) {
			// we do not penalize peers for bad blocks on new block hash events since they have
			// not necessarily been executed by the peer but just propagated as per the devp2p spec
			s.logger.Warn("[bsc] bad block hash received from peer",
				"blockHash", hashOrNum.Hash,
				"blockNum", hashOrNum.Number,
				"peerId", event.PeerId,
			)
			continue
		}

		s.asyncBackwardDownloadBlockBatches(hashOrNum.Hash, hashOrNum.Number, event.PeerId, EventSourceP2PNewBlockHashes)
	}
}

func (s *tipSync) asyncBackwardDownloadBlockBatches(fromHash common.Hash, fromNum uint64, fromPeerId *p2p.PeerId, eventSource EventSource) {
	if s.blockRequestsCache.Contains(fromHash) {
		return
	}
	root := s.ccb.Root()
	rootNum := root.Number.Uint64()
	s.logger.Debug(
		"[bsc] block parent hash not in ccb, fetching blocks backwards to root",
		"rootNum", rootNum,
		"rootHash", root.Hash(),
		"blockNum", fromNum,
		"blockHash", fromHash,
		"amount", fromNum-rootNum+1,
	)
	s.blockRequestsCache.Add(fromHash, struct{}{})
	hr := s.ccb.HeaderReader()
	ctx := s.downloadCtx
	go func() {
		err := s.fetchBackwards(ctx, fromHash, hr, fromNum-rootNum+1, fromPeerId, func(blocks []*types.Block) error {
			processedC := make(chan error, 1)
			s.tipEvents.PushEvent(Event{
				Type: EventTypeNewBlockBatch,
				newBlockBatch: EventNewBlockBatch{
					NewBlocks: blocks,
					PeerId:    fromPeerId,
					Source:    eventSource,
					Processed: processedC,
				},
			})
			select {
			case <-ctx.Done():
				return ctx.Err()
			case err := <-processedC:
				return err
			}
		})
		if err != nil {
			s.logger.Debug(
				"[bsc] failed to backward download blocks",
				"blockNum", fromNum,
				"blockHash", fromHash,
				"source", eventSource,
				"err", err,
			)
			s.blockRequestsCache.Remove(fromHash)
		}
	}()
}

// handleBadBlock marks the rejected tip as bad, penalizes the peer that sent it and prunes the
// uncommitted branch back to the last committed block.
func (s *tipSync) handleBadBlock(ctx context.Context, badTip *types.Header, valErr *string) error {
	msg := ""
	if valErr != nil {
		msg = *valErr
	}
	s.logger.Warn(
		"[bsc] handling bad block after execution",
		"peerId", s.tipPeer,
		"badTipNum", badTip.Number.Uint64(),
		"badTipHash", badTip.Hash(),
		"committedNum", s.committed.Number.Uint64(),
		"committedHash", s.committed.Hash(),
		"err", msg,
	)

	s.badBlocks.Add(badTip.Hash(), struct{}{})
	s.maybePenalizePeerOnBadBlockEvent(ctx, s.tipSource, s.tipPeer)

	lca, ok := s.ccb.LowestCommonAncestor(s.committed.Hash(), badTip.Hash())
	if !ok {
		return errors.New("could not find lowest common ancestor of committed and bad tip")
	}
	first := badTip
	for first.ParentHash != lca.Hash() {
		parent, _ := s.ccb.HeaderReader().HeaderByHash(ctx, first.ParentHash)
		if parent == nil {
			return fmt.Errorf("bad branch does not reach the common ancestor %d", lca.Number.Uint64())
		}
		first = parent
	}
	return s.ccb.PruneNode(first.Hash())
}

func (s *tipSync) maybePenalizePeerOnBadBlockEvent(ctx context.Context, eventSource EventSource, peerId *p2p.PeerId) {
	if eventSource == EventSourceP2PNewBlockHashes {
		// we do not penalize peers for bad blocks on new block hash events since they have
		// not necessarily been executed by the peer but just propagated as per the devp2p spec
		return
	}
	s.penalize(ctx, peerId)
}

func (s *tipSync) penalize(ctx context.Context, peerId *p2p.PeerId) {
	if peerId == nil {
		return
	}
	if err := s.peers.Penalize(ctx, peerId); err != nil {
		s.logger.Debug("[bsc] issue with penalizing peer", "peerId", peerId, "err", err)
	}
}
