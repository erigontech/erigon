package sync

import (
	"context"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon/cl/phase1/execution_client"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	polygonP2P "github.com/ledgerwatch/erigon/polygon/p2p"
)

type Service interface {
	GetSync() *Sync
}

type service struct {
	sync *Sync

	p2pService polygonP2P.Service
}

func NewService(
	chainConfig *chain.Config,
	maxPeers int,
	borConfig *borcfg.BorConfig,
	heimdallURL string,
	engine execution_client.ExecutionEngine,
	sentryClient direct.SentryClient,
	logger log.Logger,
) Service {
	storage := NewStorage()
	execution := NewExecutionClient(engine)
	verify := VerifyAccumulatedHeaders
	p2pService := polygonP2P.NewService(maxPeers, logger, sentryClient)
	heimdallClient := heimdall.NewHeimdallClient(heimdallURL, logger)
	heimdallService := heimdall.NewHeimdallNoStore(heimdallClient, logger)
	downloader := NewHeaderDownloader(
		logger,
		p2pService,
		heimdallService,
		verify,
		storage,
	)
	spansCache := NewSpansCache()
	signaturesCache, err := lru.NewARC[libcommon.Hash, libcommon.Address](stagedsync.InMemorySignatures)
	if err != nil {
		panic(err)
	}
	difficultyCalculator := NewDifficultyCalculator(borConfig, spansCache, nil, signaturesCache)
	headerTimeValidator := NewHeaderTimeValidator(borConfig, spansCache, nil, signaturesCache)
	headerValidator := NewHeaderValidator(chainConfig, borConfig, headerTimeValidator)
	ccBuilderFactory := func(root *types.Header) CanonicalChainBuilder {
		return NewCanonicalChainBuilder(
			root,
			difficultyCalculator,
			headerValidator,
			spansCache)
	}
	events := NewSyncToTipEvents()
	sync := NewSync(
		storage,
		execution,
		verify,
		p2pService,
		downloader,
		ccBuilderFactory,
		events.Events(),
		logger,
	)
	return &service{
		sync:       sync,
		p2pService: p2pService,
	}
}

func (s *service) GetSync() *Sync {
	return s.sync
}

func (s *service) Run(ctx context.Context) {
	s.p2pService.Start(ctx)
	<-ctx.Done()
	s.p2pService.Stop()
}
