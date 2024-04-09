package sync

import (
	"context"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/direct"
	executionclient "github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
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
	storage    Storage
	events     *TipEvents
}

func NewService(
	logger log.Logger,
	chainConfig *chain.Config,
	sentryClient direct.SentryClient,
	maxPeers int,
	statusDataProvider *sentry.StatusDataProvider,
	heimdallUrl string,
	executionEngine executionclient.ExecutionEngine,
) Service {
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	execution := NewExecutionClient(executionEngine)
	storage := NewStorage(logger, execution, maxPeers)
	headersVerifier := VerifyAccumulatedHeaders
	blocksVerifier := VerifyBlocks
	p2pService := p2p.NewService(maxPeers, logger, sentryClient, statusDataProvider.GetStatusData)
	heimdallClient := heimdall.NewHeimdallClient(heimdallUrl, logger)
	heimdallService := heimdall.NewHeimdallNoStore(heimdallClient, logger)
	blockDownloader := NewBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		headersVerifier,
		blocksVerifier,
		storage,
	)
	spansCache := NewSpansCache()
	signaturesCache, err := lru.NewARC[common.Hash, common.Address](stagedsync.InMemorySignatures)
	if err != nil {
		panic(err)
	}
	difficultyCalculator := NewDifficultyCalculator(borConfig, spansCache, nil, signaturesCache)
	headerTimeValidator := NewHeaderTimeValidator(borConfig, spansCache, nil, signaturesCache)
	headerValidator := NewHeaderValidator(chainConfig, borConfig, headerTimeValidator)
	ccBuilderFactory := func(root *types.Header, span *heimdall.Span) CanonicalChainBuilder {
		if span == nil {
			panic("sync.Service: ccBuilderFactory - span is nil")
		}
		if spansCache.IsEmpty() {
			panic("sync.Service: ccBuilderFactory - spansCache is empty")
		}
		return NewCanonicalChainBuilder(
			root,
			difficultyCalculator,
			headerValidator,
			spansCache)
	}
	events := NewTipEvents(logger, p2pService, heimdallService)
	sync := NewSync(
		storage,
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
		storage:    storage,
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
		err := s.storage.Run(ctx)
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
