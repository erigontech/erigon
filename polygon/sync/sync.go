package sync

import (
	"context"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/polygon/p2p"
)

type Sync struct {
	storage          Storage
	execution        ExecutionClient
	verify           AccumulatedHeadersVerifier
	p2pService       p2p.Service
	downloader       *HeaderDownloader
	ccBuilderFactory func(root *types.Header) CanonicalChainBuilder
	events           chan Event
	logger           log.Logger
}

func NewSync(
	storage Storage,
	execution ExecutionClient,
	verify AccumulatedHeadersVerifier,
	p2pService p2p.Service,
	downloader *HeaderDownloader,
	ccBuilderFactory func(root *types.Header) CanonicalChainBuilder,
	events chan Event,
	logger log.Logger,
) *Sync {
	return &Sync{
		storage:          storage,
		execution:        execution,
		verify:           verify,
		p2pService:       p2pService,
		downloader:       downloader,
		ccBuilderFactory: ccBuilderFactory,
		events:           events,
		logger:           logger,
	}
}

func (s *Sync) commitExecution(ctx context.Context, oldTip uint64) error {
	newTip, err := s.storage.TipHeader(ctx)
	if err != nil {
		return err
	}
	newTipNum := newTip.Number.Uint64()

	newHeaders, err := s.storage.GetHeadersInRange(ctx, oldTip, newTipNum+1)
	if err != nil {
		return err
	}

	if err = s.execution.InsertBlocks(newHeaders); err != nil {
		return err
	}
	return s.execution.UpdateForkChoice(newTip)
}

func (s *Sync) onMilestoneEvent(
	ctx context.Context,
	event Event,
	ccBuilder CanonicalChainBuilder,
) error {
	if event.Milestone.EndBlock().Uint64() <= ccBuilder.Root().Number.Uint64() {
		return nil
	}

	milestoneHeaders := ccBuilder.HeadersInRange(event.Milestone.StartBlock().Uint64(), event.Milestone.Length())
	err := s.verify(event.Milestone, milestoneHeaders)
	if err == nil {
		if err = ccBuilder.Prune(event.Milestone.EndBlock().Uint64()); err != nil {
			return err
		}
	}

	s.logger.Debug("sync.Sync.onMilestoneEvent: local chain tip does not match the milestone, unwinding to the previous verified milestone", "err", err)

	// the milestone doesn't correspond to the tip of the chain
	// unwind to the previous verified milestone
	oldTip := ccBuilder.Root()
	oldTipNum := oldTip.Number.Uint64()
	if err = s.execution.UpdateForkChoice(oldTip); err != nil {
		return err
	}

	if err = s.downloader.DownloadUsingMilestones(ctx, oldTipNum); err != nil {
		return err
	}

	if err = s.commitExecution(ctx, oldTipNum); err != nil {
		return err
	}

	root, err := s.storage.TipHeader(ctx)
	if err != nil {
		return err
	}
	ccBuilder.Reset(root)

	return nil
}

func (s *Sync) onNewHeaderEvent(
	ctx context.Context,
	event Event,
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
			event.PeerId)
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
		if err = s.execution.InsertBlocks(newHeaders); err != nil {
			return err
		}

		if err = s.execution.UpdateForkChoice(newTip); err != nil {
			return err
		}
	}

	return nil
}

func (s *Sync) Run(ctx context.Context) error {
	oldTip, err := s.storage.TipBlockNumber(ctx)
	if err != nil {
		return err
	}

	if err = s.downloader.DownloadUsingCheckpoints(ctx, oldTip); err != nil {
		return err
	}

	newTip, err := s.storage.TipBlockNumber(ctx)
	if err != nil {
		return err
	}
	if err = s.downloader.DownloadUsingMilestones(ctx, newTip); err != nil {
		return err
	}

	if err = s.commitExecution(ctx, oldTip); err != nil {
		return err
	}

	root, err := s.storage.TipHeader(ctx)
	if err != nil {
		return err
	}
	ccBuilder := s.ccBuilderFactory(root)

	for {
		select {
		case event := <-s.events:
			switch event.Type {
			case EventTypeMilestone:
				if err = s.onMilestoneEvent(ctx, event, ccBuilder); err != nil {
					return err
				}
			case EventTypeNewHeader:
				if err = s.onNewHeaderEvent(ctx, event, ccBuilder); err != nil {
					return err
				}
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
