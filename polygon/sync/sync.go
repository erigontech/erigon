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
	verify           AccumulatedHeadersVerifier
	p2pService       p2p.Service
	downloader       HeaderDownloader
	ccBuilderFactory func(root *types.Header, span *heimdall.Span) CanonicalChainBuilder
	spansCache       *SpansCache
	fetchLatestSpan  func(ctx context.Context) (*heimdall.Span, error)
	events           <-chan Event
	logger           log.Logger
}

func NewSync(
	storage Storage,
	execution ExecutionClient,
	verify AccumulatedHeadersVerifier,
	p2pService p2p.Service,
	downloader HeaderDownloader,
	ccBuilderFactory func(root *types.Header, span *heimdall.Span) CanonicalChainBuilder,
	spansCache *SpansCache,
	fetchLatestSpan func(ctx context.Context) (*heimdall.Span, error),
	events <-chan Event,
	logger log.Logger,
) *Sync {
	return &Sync{
		storage:          storage,
		execution:        execution,
		verify:           verify,
		p2pService:       p2pService,
		downloader:       downloader,
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
	err := s.verify(milestone, milestoneHeaders)
	if err == nil {
		if err = ccBuilder.Prune(milestone.EndBlock().Uint64()); err != nil {
			return err
		}
	}

	s.logger.Debug(
		"sync.Sync.onMilestoneEvent: local chain tip does not match the milestone, unwinding to the previous verified milestone",
		"err", err,
	)

	// the milestone doesn't correspond to the tip of the chain
	// unwind to the previous verified milestone
	oldTip := ccBuilder.Root()
	oldTipNum := oldTip.Number.Uint64()
	if err = s.execution.UpdateForkChoice(ctx, oldTip, oldTip); err != nil {
		return err
	}

	newTip, err := s.downloader.DownloadUsingMilestones(ctx, oldTipNum)
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

func (s *Sync) onNewHeaderEvent(
	ctx context.Context,
	event EventNewHeader,
	ccBuilder CanonicalChainBuilder,
) error {
	if event.NewHeader.Number.Uint64() <= ccBuilder.Root().Number.Uint64() {
		return nil
	}

	var newHeaders []*types.Header
	var err error
	if ccBuilder.ContainsHash(event.NewHeader.ParentHash) {
		newHeaders = []*types.Header{event.NewHeader}
	} else {
		newHeaders, err = s.p2pService.FetchHeaders(
			ctx,
			ccBuilder.Root().Number.Uint64(),
			event.NewHeader.Number.Uint64()+1,
			event.PeerId,
		)
		if err != nil {
			return err
		}
	}

	oldTip := ccBuilder.Tip()
	if err = ccBuilder.Connect(newHeaders); err != nil {
		s.logger.Debug("sync.Sync.onNewHeaderEvent: couldn't connect a header to the local chain tip, ignoring", "err", err)
		return nil
	}
	newTip := ccBuilder.Tip()

	if newTip != oldTip {
		if err = s.execution.InsertBlocks(ctx, newHeaders); err != nil {
			return err
		}

		if err = s.execution.UpdateForkChoice(ctx, newTip, ccBuilder.Root()); err != nil {
			return err
		}
	}

	return nil
}

func (s *Sync) onNewHeaderHashesEvent(
	ctx context.Context,
	event EventNewHeaderHashes,
	ccBuilder CanonicalChainBuilder,
) error {
	for _, headerHashNum := range event.NewHeaderHashes {
		if (headerHashNum.Number <= ccBuilder.Root().Number.Uint64()) || ccBuilder.ContainsHash(headerHashNum.Hash) {
			continue
		}

		newHeaders, err := s.p2pService.FetchHeaders(
			ctx,
			headerHashNum.Number,
			headerHashNum.Number+1,
			event.PeerId,
		)
		if err != nil {
			return err
		}

		newHeaderEvent := EventNewHeader{
			NewHeader: newHeaders[0],
			PeerId:    event.PeerId,
		}

		err = s.onNewHeaderEvent(ctx, newHeaderEvent, ccBuilder)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *Sync) Run(ctx context.Context) error {
	tip, err := s.execution.CurrentHeader(ctx)
	if err != nil {
		return err
	}

	if newTip, err := s.downloader.DownloadUsingCheckpoints(ctx, tip.Number.Uint64()); err != nil {
		return err
	} else if newTip != nil {
		tip = newTip
	}

	if newTip, err := s.downloader.DownloadUsingMilestones(ctx, tip.Number.Uint64()); err != nil {
		return err
	} else if newTip != nil {
		tip = newTip
	}

	if err = s.commitExecution(ctx, tip, tip); err != nil {
		return err
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
			case EventTypeNewHeader:
				if err = s.onNewHeaderEvent(ctx, event.AsNewHeader(), ccBuilder); err != nil {
					return err
				}
			case EventTypeNewHeaderHashes:
				if err = s.onNewHeaderHashesEvent(ctx, event.AsNewHeaderHashes(), ccBuilder); err != nil {
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
