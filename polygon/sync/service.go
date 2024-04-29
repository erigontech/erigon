package sync

import (
	"context"

	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/direct"
	executionproto "github.com/ledgerwatch/erigon-lib/gointerfaces/execution"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
)

type Service interface {
	Run(ctx context.Context) error
}

type service struct {
	sync *Sync

	p2pService p2p.Service
	store      Store
	events     *TipEvents
}

func NewService(
	logger log.Logger,
	chainConfig *chain.Config,
	sentryClient direct.SentryClient,
	maxPeers int,
	statusDataProvider *sentry.StatusDataProvider,
	heimdallUrl string,
	executionClient executionproto.ExecutionClient,
) Service {
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	execution := NewExecutionClient(executionClient)
	store := NewStore(logger, execution)
	headersVerifier := VerifyAccumulatedHeaders
	blocksVerifier := VerifyBlocks
	p2pService := p2p.NewService(maxPeers, logger, sentryClient, statusDataProvider.GetStatusData)
	heimdallClient := heimdall.NewHeimdallClient(heimdallUrl, logger)
	heimdallService := heimdall.NewHeimdall(heimdallClient, logger)
	blockDownloader := NewBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		headersVerifier,
		blocksVerifier,
		store,
	)
	spansCache := NewSpansCache()
	ccBuilderFactory := NewCanonicalChainBuilderFactory(chainConfig, borConfig, spansCache)
	events := NewTipEvents(logger, p2pService, heimdallService)
	sync := NewSync(
		store,
		execution,
		headersVerifier,
		blocksVerifier,
		p2pService,
		blockDownloader,
		ccBuilderFactory,
		spansCache,
		heimdallService.FetchLatestSpan,
		events.Events(),
		logger,
	)
	return &service{
		sync:       sync,
		p2pService: p2pService,
		store:      store,
		events:     events,
	}
}

func (s *service) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var serviceErr error

	go func() {
		s.p2pService.Run(ctx)
	}()

	go func() {
		err := s.store.Run(ctx)
		if (err != nil) && (ctx.Err() == nil) {
			serviceErr = err
			cancel()
		}
	}()

	go func() {
		err := s.events.Run(ctx)
		if (err != nil) && (ctx.Err() == nil) {
			serviceErr = err
			cancel()
		}
	}()

	go func() {
		err := s.sync.Run(ctx)
		if (err != nil) && (ctx.Err() == nil) {
			serviceErr = err
			cancel()
		}
	}()

	<-ctx.Done()

	if serviceErr != nil {
		return serviceErr
	}

	return ctx.Err()
}
