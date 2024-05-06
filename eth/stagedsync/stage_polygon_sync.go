package stagedsync

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
	"github.com/ledgerwatch/erigon/polygon/sync"
	"github.com/ledgerwatch/erigon/turbo/services"
)

func NewPolygonSyncStageCfg(
	logger log.Logger,
	chainConfig *chain.Config,
	db kv.RwDB,
	heimdallClient heimdall.HeimdallClient,
	sentry direct.SentryClient,
	maxPeers int,
	statusDataProvider *sentry.StatusDataProvider,
	blockReader services.FullBlockReader,
	stopNode func() error,
) PolygonSyncStageCfg {
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	storage := newPolygonSyncStageStorage(logger, blockReader)
	p2pService := p2p.NewService(maxPeers, logger, sentry, statusDataProvider.GetStatusData)
	headersVerifier := sync.VerifyAccumulatedHeaders
	blocksVerifier := sync.VerifyBlocks
	heimdallService := heimdall.NewHeimdall(heimdallClient, logger, heimdall.WithStore(storage))
	blockDownloader := sync.NewBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		headersVerifier,
		blocksVerifier,
		storage,
	)
	spansCache := sync.NewSpansCache()
	events := sync.NewTipEvents(logger, p2pService, heimdallService)
	executionClient := &interruptingExecutionClient{}
	sync := sync.NewSync(
		storage,
		executionClient,
		headersVerifier,
		blocksVerifier,
		p2pService,
		blockDownloader,
		sync.NewCanonicalChainBuilderFactory(chainConfig, borConfig, spansCache),
		spansCache,
		heimdallService.FetchLatestSpan,
		events.Events(),
		logger,
	)
	syncService := newPolygonSyncStageService(logger, sync, events, p2pService, executionClient, stopNode)
	return PolygonSyncStageCfg{
		db:      db,
		storage: storage,
		service: syncService,
	}
}

type PolygonSyncStageCfg struct {
	db      kv.RwDB
	storage *polygonSyncStageStorage
	service *polygonSyncStageService
}

func SpawnPolygonSyncStage(ctx context.Context, tx kv.RwTx, cfg PolygonSyncStageCfg) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	cfg.storage.use(tx)
	if err := cfg.service.Run(ctx); err != nil {
		return err
	}

	if !useExternalTx {
		return nil
	}

	if err := tx.Commit(); err != nil {
		return err
	}

	return nil
}

func UnwindPolygonSyncStage() error {
	return nil
}

func PrunePolygonSyncStage() error {
	return nil
}

func newPolygonSyncStageService(
	logger log.Logger,
	sync *sync.Sync,
	events *sync.TipEvents,
	p2pService p2p.Service,
	executionClient *interruptingExecutionClient,
	stopNode func() error,
) *polygonSyncStageService {
	return &polygonSyncStageService{
		logger:          logger,
		sync:            sync,
		events:          events,
		p2pService:      p2pService,
		executionClient: executionClient,
		stopNode:        stopNode,
	}
}

type polygonSyncStageService struct {
	logger          log.Logger
	sync            *sync.Sync
	events          *sync.TipEvents
	p2pService      p2p.Service
	executionClient *interruptingExecutionClient
	stopNode        func() error
	bgComponentsRun bool
	errChan         chan error
}

func (s polygonSyncStageService) Run(ctx context.Context) error {
	select {
	case err := <-s.errChan:
		s.logger.Error("stopping node", "err", err)
		stopErr := s.stopNode()
		if stopErr != nil {
			return fmt.Errorf("%w: %w", stopErr, err)
		}
		return err
	default:
		// carry on
	}

	if !s.bgComponentsRun {
		s.runBgComponents(ctx)
	}

	ctx, cancel := context.WithCancelCause(ctx)
	s.executionClient.useInterrupt(cancel)

	err := s.sync.Run(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) && errors.Is(context.Cause(ctx), errUpdateForkChoiceInterrupt) {
			s.logger.Info("fork choice update called, stage done")
			return nil
		}

		return err
	}

	return nil
}

func (s polygonSyncStageService) runBgComponents(ctx context.Context) {
	s.bgComponentsRun = true

	go func() {
		eg := errgroup.Group{}

		eg.Go(func() error {
			return s.events.Run(ctx)
		})

		eg.Go(func() error {
			s.p2pService.Run(ctx)
			select {
			case <-ctx.Done():
				return nil
			default:
				return errors.New("p2p service stopped")
			}
		})

		if err := eg.Wait(); err != nil {
			s.errChan <- err
		}
	}()
}

func newPolygonSyncStageStorage(logger log.Logger, blockReader services.FullBlockReader) *polygonSyncStageStorage {
	return &polygonSyncStageStorage{
		logger:      logger,
		blockReader: blockReader,
	}
}

type polygonSyncStageStorage struct {
	logger      log.Logger
	blockReader services.FullBlockReader

	heimdall.Store
	tx kv.RwTx
}

func (s *polygonSyncStageStorage) InsertBlocks(_ context.Context, blocks []*types.Block) error {
	if s.tx == nil {
		return errors.New("missing tx")
	}

	for _, block := range blocks {
		height := block.NumberU64()
		header := block.Header()
		body := block.Body()

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height-1, s.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height-1, s.logger)

		parentTd := common.Big0
		if height > 0 {
			// Parent's total difficulty
			parentTd, err := rawdb.ReadTd(s.tx, header.ParentHash, height-1)
			if err != nil || parentTd == nil {
				return fmt.Errorf(
					"parent's total difficulty not found with hash %x and height %d: %v",
					header.ParentHash,
					height-1,
					err,
				)
			}
		}

		td := parentTd.Add(parentTd, header.Difficulty)
		if err := rawdb.WriteHeader(s.tx, header); err != nil {
			return fmt.Errorf("ethereumExecutionModule.InsertHeaders: writeHeader: %s", err)
		}

		if err := rawdb.WriteTd(s.tx, header.Hash(), height, td); err != nil {
			return fmt.Errorf("ethereumExecutionModule.InsertHeaders: writeTd: %s", err)
		}

		if _, err := rawdb.WriteRawBodyIfNotExists(s.tx, header.Hash(), height, body.RawBody()); err != nil {
			return fmt.Errorf("ethereumExecutionModule.InsertBlocks: writeBody: %s", err)
		}
	}

	return nil
}

func (s *polygonSyncStageStorage) Flush(context.Context) error {
	return nil
}

func (s *polygonSyncStageStorage) Run(context.Context) error {
	return nil
}

func (s *polygonSyncStageStorage) use(tx kv.RwTx) {
	s.tx = tx
	s.Store = heimdall.NewTxStore(s.blockReader, tx)
}

var errUpdateForkChoiceInterrupt = errors.New("update fork choice interrupt")

type interruptingExecutionClient struct {
	interrupt context.CancelCauseFunc
}

func (ec interruptingExecutionClient) InsertBlocks(context.Context, []*types.Block) error {
	panic("should not be used")
}

func (ec interruptingExecutionClient) UpdateForkChoice(context.Context, *types.Header, *types.Header) error {
	// we need to interrupt sync.Run when we reach an UpdateForkChoice call
	// so that the stage can exit and allow the loop to proceed to execution
	ec.interrupt(errUpdateForkChoiceInterrupt)
	return nil
}

func (ec interruptingExecutionClient) CurrentHeader(context.Context) (*types.Header, error) {
	// TODO need to change sync.Run to use some other func for getting current persisted header instead of canonical/executed?
	panic("should not be used")
}

func (ec interruptingExecutionClient) useInterrupt(cancel context.CancelCauseFunc) {
	ec.interrupt = cancel
}
