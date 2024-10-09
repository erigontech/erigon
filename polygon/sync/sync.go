package sync

import (
	"context"
	"errors"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
)

type Sync struct {
	storage          Storage
	execution        ExecutionClient
	headersVerifier  AccumulatedHeadersVerifier
	blocksVerifier   BlocksVerifier
	p2pService       p2p.Service
	blockDownloader  BlockDownloader
	ccBuilderFactory func(root *types.Header, span *heimdall.Span) CanonicalChainBuilder
	spansCache       *SpansCache
	fetchLatestSpan  func(ctx context.Context) (*heimdall.Span, error)
	events           <-chan Event
	logger           log.Logger
}

func NewSync(
	storage Storage,
	execution ExecutionClient,
	headersVerifier AccumulatedHeadersVerifier,
	blocksVerifier BlocksVerifier,
	p2pService p2p.Service,
	blockDownloader BlockDownloader,
	ccBuilderFactory func(root *types.Header, span *heimdall.Span) CanonicalChainBuilder,
	spansCache *SpansCache,
	fetchLatestSpan func(ctx context.Context) (*heimdall.Span, error),
	events <-chan Event,
	logger log.Logger,
) *Sync {
	return &Sync{
		storage:          storage,
		execution:        execution,
		headersVerifier:  headersVerifier,
		blocksVerifier:   blocksVerifier,
		p2pService:       p2pService,
		blockDownloader:  blockDownloader,
		ccBuilderFactory: ccBuilderFactory,
		spansCache:       spansCache,
		fetchLatestSpan:  fetchLatestSpan,
		events:           events,
		logger:           logger,
	}
}

func (s *Sync) commitExecution(ctx context.Context, newTip *types.Header, finalizedHeader *types.Header) error {
	if err := s.storage.Flush(ctx); err != nil {
		return err
	}
	return s.execution.UpdateForkChoice(ctx, newTip, finalizedHeader)
}

func (s *Sync) onMilestoneEvent(
	ctx context.Context,
	event EventNewMilestone,
	ccBuilder CanonicalChainBuilder,
) error {
	milestone := event
	if milestone.EndBlock().Uint64() <= ccBuilder.Root().Number.Uint64() {
		return nil
	}

	milestoneHeaders := ccBuilder.HeadersInRange(milestone.StartBlock().Uint64(), milestone.Length())
	err := s.headersVerifier(milestone, milestoneHeaders)
	if err == nil {
		if err = ccBuilder.Prune(milestone.EndBlock().Uint64()); err != nil {
			return err
		}
	}

	s.logger.Debug(
		syncLogPrefix("onMilestoneEvent: local chain tip does not match the milestone, unwinding to the previous verified milestone"),
		"err", err,
	)

	// the milestone doesn't correspond to the tip of the chain
	// unwind to the previous verified milestone
	oldTip := ccBuilder.Root()
	oldTipNum := oldTip.Number.Uint64()
	if err = s.execution.UpdateForkChoice(ctx, oldTip, oldTip); err != nil {
		return err
	}

	newTip, err := s.blockDownloader.DownloadBlocksUsingMilestones(ctx, oldTipNum)
	if err != nil {
		return err
	}
	if newTip == nil {
		return errors.New("sync.Sync.onMilestoneEvent: unexpected to have no milestone headers since the last milestone after receiving a new milestone event")
	}

	if err = s.commitExecution(ctx, newTip, newTip); err != nil {
		return err
	}

	ccBuilder.Reset(newTip)

	return nil
}

func (s *Sync) onNewBlockEvent(
	ctx context.Context,
	event EventNewBlock,
	ccBuilder CanonicalChainBuilder,
) error {
	newBlockHeader := event.NewBlock.Header()
	newBlockHeaderNum := newBlockHeader.Number.Uint64()
	rootNum := ccBuilder.Root().Number.Uint64()
	if newBlockHeaderNum <= rootNum {
		return nil
	}

	var newBlocks []*types.Block
	var err error
	if ccBuilder.ContainsHash(newBlockHeader.ParentHash) {
		newBlocks = []*types.Block{event.NewBlock}
	} else {
		blocks, err := s.p2pService.FetchBlocks(ctx, rootNum, newBlockHeaderNum+1, event.PeerId)
		if err != nil {
			if (p2p.ErrIncompleteHeaders{}).Is(err) || (p2p.ErrMissingBodies{}).Is(err) {
				s.logger.Debug(
					syncLogPrefix("onNewBlockEvent: failed to fetch complete blocks, ignoring event"),
					"err", err,
					"peerId", event.PeerId,
					"lastBlockNum", newBlockHeaderNum,
				)

				return nil
			}

			return err
		}

		newBlocks = blocks.Data
	}

	if err := s.blocksVerifier(newBlocks); err != nil {
		s.logger.Debug(syncLogPrefix("onNewBlockEvent: invalid new block event from peer, penalizing and ignoring"), "err", err)

		if err = s.p2pService.Penalize(ctx, event.PeerId); err != nil {
			s.logger.Debug(syncLogPrefix("onNewBlockEvent: issue with penalizing peer"), "err", err)
		}

		return nil
	}

	newHeaders := make([]*types.Header, len(newBlocks))
	for i, block := range newBlocks {
		newHeaders[i] = block.HeaderNoCopy()
	}

	oldTip := ccBuilder.Tip()
	if err = ccBuilder.Connect(newHeaders); err != nil {
		s.logger.Debug(syncLogPrefix("onNewBlockEvent: couldn't connect a header to the local chain tip, ignoring"), "err", err)
		return nil
	}

	newTip := ccBuilder.Tip()
	if newTip != oldTip {
		if err = s.execution.InsertBlocks(ctx, newBlocks); err != nil {
			return err
		}

		if err = s.execution.UpdateForkChoice(ctx, newTip, ccBuilder.Root()); err != nil {
			return err
		}
	}

	return nil
}

func (s *Sync) onNewBlockHashesEvent(
	ctx context.Context,
	event EventNewBlockHashes,
	ccBuilder CanonicalChainBuilder,
) error {
	for _, headerHashNum := range event.NewBlockHashes {
		if (headerHashNum.Number <= ccBuilder.Root().Number.Uint64()) || ccBuilder.ContainsHash(headerHashNum.Hash) {
			continue
		}

		newBlocks, err := s.p2pService.FetchBlocks(ctx, headerHashNum.Number, headerHashNum.Number+1, event.PeerId)
		if err != nil {
			if (p2p.ErrIncompleteHeaders{}).Is(err) || (p2p.ErrMissingBodies{}).Is(err) {
				s.logger.Debug(
					syncLogPrefix("onNewBlockHashesEvent: failed to fetch complete blocks, ignoring event"),
					"err", err,
					"peerId", event.PeerId,
					"lastBlockNum", headerHashNum.Number,
				)

				continue
			}

			return err
		}

		newBlockEvent := EventNewBlock{
			NewBlock: newBlocks.Data[0],
			PeerId:   event.PeerId,
		}

		err = s.onNewBlockEvent(ctx, newBlockEvent, ccBuilder)
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

	tip, err := s.execution.CurrentHeader(ctx)
	if err != nil {
		return err
	}

	// loop until we converge at the latest checkpoint & milestone
	var prevTip *types.Header
	for tip != prevTip {
		prevTip = tip

		newTip, err := s.blockDownloader.DownloadBlocksUsingCheckpoints(ctx, tip.Number.Uint64()+1)
		if err != nil {
			return err
		}
		if newTip != nil {
			tip = newTip
		}

		newTip, err = s.blockDownloader.DownloadBlocksUsingMilestones(ctx, tip.Number.Uint64()+1)
		if err != nil {
			return err
		}
		if newTip != nil {
			tip = newTip
		}

		if err = s.commitExecution(ctx, tip, tip); err != nil {
			return err
		}
	}

	latestSpan, err := s.fetchLatestSpan(ctx)
	if err != nil {
		return err
	}
	s.spansCache.Add(latestSpan)

	ccBuilder := s.ccBuilderFactory(tip, latestSpan)

	for {
		select {
		case event := <-s.events:
			switch event.Type {
			case EventTypeNewMilestone:
				if err = s.onMilestoneEvent(ctx, event.AsNewMilestone(), ccBuilder); err != nil {
					return err
				}
			case EventTypeNewBlock:
				if err = s.onNewBlockEvent(ctx, event.AsNewBlock(), ccBuilder); err != nil {
					return err
				}
			case EventTypeNewBlockHashes:
				if err = s.onNewBlockHashesEvent(ctx, event.AsNewBlockHashes(), ccBuilder); err != nil {
					return err
				}
			case EventTypeNewSpan:
				s.spansCache.Add(event.AsNewSpan())
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
