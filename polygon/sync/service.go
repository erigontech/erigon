package sync

import (
	"context"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/executionproto"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bridge"
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

	heimdallService heimdall.Service
	bridge          *bridge.Bridge
}

func NewService(
	logger log.Logger,
	chainConfig *chain.Config,
	dataDir string,
	tmpDir string,
	sentryClient direct.SentryClient,
	maxPeers int,
	statusDataProvider *sentry.StatusDataProvider,
	heimdallUrl string,
	executionClient executionproto.ExecutionClient,
	nodeCfg *nodecfg.Config,
) (Service, error) {
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	execution := NewExecutionClient(executionClient)
	store := NewStore(logger, execution)
	checkpointVerifier := VerifyCheckpointHeaders
	milestoneVerifier := VerifyMilestoneHeaders
	blocksVerifier := VerifyBlocks
	p2pService := p2p.NewService(maxPeers, logger, sentryClient, statusDataProvider.GetStatusData)
	heimdallClient := heimdall.NewHeimdallClient(heimdallUrl, logger)
	heimdallService := heimdall.NewHeimdall(heimdallClient, logger)
	heimdallServiceV2 := heimdall.NewService(
		heimdallUrl,
		dataDir,
		tmpDir,
		logger,
	)

	b, err := bridge.NewBridge(context.Background(), dataDir, logger, borConfig, heimdallClient.FetchStateSyncEvents, bor.GenesisContractStateReceiverABI())
	if err != nil {
		return nil, err
	}

	blockDownloader := NewBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		b,
		checkpointVerifier,
		milestoneVerifier,
		blocksVerifier,
		store,
	)
	spansCache := NewSpansCache()
	ccBuilderFactory := NewCanonicalChainBuilderFactory(chainConfig, borConfig, spansCache)
	events := NewTipEvents(logger, p2pService, heimdallService)
	sync := NewSync(
		store,
		execution,
		milestoneVerifier,
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

		heimdallService: heimdallServiceV2,
		bridge:          b,
	}, nil
}

func (s *service) Run(parentCtx context.Context) error {
	group, ctx := errgroup.WithContext(parentCtx)

	group.Go(func() error { s.p2pService.Run(ctx); return nil })
	group.Go(func() error { return s.store.Run(ctx) })
	group.Go(func() error { return s.events.Run(ctx) })
	// TODO: remove the check when heimdall.NewService is functional
	if s.heimdallService != nil {
		group.Go(func() error { return s.heimdallService.Run(ctx) })
	}
	group.Go(func() error { return s.bridge.Run(ctx) })
	group.Go(func() error { return s.sync.Run(ctx) })

	return group.Wait()
}
