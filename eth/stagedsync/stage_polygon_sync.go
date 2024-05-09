package stagedsync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ledgerwatch/log/v3"
	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/polygon/p2p"
	polygonsync "github.com/ledgerwatch/erigon/polygon/sync"
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
	stateReceiverABI abi.ABI,
) PolygonSyncStageCfg {
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	storage := newPolygonSyncStageStorage(blockReader)
	p2pService := p2p.NewService(maxPeers, logger, sentry, statusDataProvider.GetStatusData)
	headersVerifier := polygonsync.VerifyAccumulatedHeaders
	blocksVerifier := polygonsync.VerifyBlocks
	heimdallService := heimdall.NewHeimdall(heimdallClient, logger, heimdall.WithStore(storage))
	blockDownloader := polygonsync.NewBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		headersVerifier,
		blocksVerifier,
		storage,
	)
	spansCache := polygonsync.NewSpansCache()
	events := polygonsync.NewTipEvents(logger, p2pService, heimdallService)
	executionEngine := newPolygonSyncStageExecutionEngine(
		logger,
		chainConfig,
		blockReader,
		heimdallClient,
		stateReceiverABI,
	)
	sync := polygonsync.NewSync(
		storage,
		executionEngine,
		headersVerifier,
		blocksVerifier,
		p2pService,
		blockDownloader,
		polygonsync.NewCanonicalChainBuilderFactory(chainConfig, borConfig, spansCache),
		spansCache,
		heimdallService.FetchLatestSpan,
		events.Events(),
		logger,
	)
	syncService := newPolygonSyncStageService(logger, sync, events, p2pService, storage, executionEngine, stopNode)
	return PolygonSyncStageCfg{
		db:      db,
		service: syncService,
	}
}

type PolygonSyncStageCfg struct {
	db      kv.RwDB
	service *polygonSyncStageService
}

func SpawnPolygonSyncStage(ctx context.Context, tx kv.RwTx, stageState *StageState, cfg PolygonSyncStageCfg) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err := cfg.service.Run(ctx, tx, stageState); err != nil {
		return err
	}

	if useExternalTx {
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
	sync *polygonsync.Sync,
	events *polygonsync.TipEvents,
	p2p p2p.Service,
	storage *polygonSyncStageStorage,
	execution *polygonSyncStageExecutionEngine,
	stopNode func() error,
) *polygonSyncStageService {
	return &polygonSyncStageService{
		logger:    logger,
		sync:      sync,
		events:    events,
		p2p:       p2p,
		storage:   storage,
		execution: execution,
		stopNode:  stopNode,
	}
}

type polygonSyncStageService struct {
	logger          log.Logger
	sync            *polygonsync.Sync
	events          *polygonsync.TipEvents
	p2p             p2p.Service
	storage         *polygonSyncStageStorage
	execution       *polygonSyncStageExecutionEngine
	stopNode        func() error
	bgComponentsRun bool
	bgComponentsErr chan error
}

func (s polygonSyncStageService) Run(ctx context.Context, tx kv.RwTx, stageState *StageState) error {
	appendLogPrefix := newAppendLogPrefix(stageState.LogPrefix())

	select {
	case err := <-s.bgComponentsErr:
		s.logger.Error(appendLogPrefix("stopping node"), "err", err)
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
	s.execution.useInterrupt(cancel)
	s.execution.useStageState(stageState)
	s.execution.useTx(tx)
	s.storage.useTx(tx)

	err := s.sync.Run(ctx)
	if err != nil {
		if errors.Is(err, context.Canceled) && errors.Is(context.Cause(ctx), errUpdateForkChoiceInterrupt) {
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
			s.p2p.Run(ctx)
			select {
			case <-ctx.Done():
				return nil
			default:
				return errors.New("p2p service stopped")
			}
		})

		if err := eg.Wait(); err != nil {
			s.bgComponentsErr <- err
		}
	}()
}

func newPolygonSyncStageStorage(blockReader services.FullBlockReader) *polygonSyncStageStorage {
	return &polygonSyncStageStorage{
		blockReader: blockReader,
	}
}

type polygonSyncStageStorage struct {
	heimdall.Store
	executionEngine *polygonSyncStageExecutionEngine
	blockReader     services.FullBlockReader

	// set by "use" functions
	tx kv.RwTx
}

func (s *polygonSyncStageStorage) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	return s.executionEngine.InsertBlocks(ctx, blocks)
}

func (s *polygonSyncStageStorage) Flush(context.Context) error {
	return nil
}

func (s *polygonSyncStageStorage) Run(context.Context) error {
	return nil
}

func (s *polygonSyncStageStorage) useTx(tx kv.RwTx) {
	s.tx = tx
	s.Store = heimdall.NewTxStore(s.blockReader, tx)
}

var errUpdateForkChoiceInterrupt = errors.New("update fork choice interrupt")

func newPolygonSyncStageExecutionEngine(
	logger log.Logger,
	chainConfig *chain.Config,
	blockReader services.FullBlockReader,
	heimdallClient heimdall.HeimdallClient,
	stateReceiverABI abi.ABI,
) *polygonSyncStageExecutionEngine {
	return &polygonSyncStageExecutionEngine{
		logger:                   logger,
		chainConfig:              *chainConfig,
		blockReader:              blockReader,
		heimdallClient:           heimdallClient,
		stateReceiverABI:         stateReceiverABI,
		initStateSyncEventIDOnce: sync.Once{},
	}
}

type polygonSyncStageExecutionEngine struct {
	logger                   log.Logger
	chainConfig              chain.Config
	blockReader              services.FullBlockReader
	heimdallClient           heimdall.HeimdallClient
	stateReceiverABI         abi.ABI
	lastStateSyncEventID     uint64
	initStateSyncEventIDOnce sync.Once

	// set by "use" functions
	interrupt       context.CancelCauseFunc
	tx              kv.RwTx
	stageState      *StageState
	appendLogPrefix func(string) string
}

func (e *polygonSyncStageExecutionEngine) InsertBlocks(_ context.Context, blocks []*types.Block) error {
	if err := e.checkDependencies(); err != nil {
		return err
	}

	for _, block := range blocks {
		height := block.NumberU64()
		header := block.Header()
		body := block.Body()

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height-1, e.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height-1, e.logger)

		parentTd := common.Big0
		if height > 0 {
			// Parent's total difficulty
			parentTd, err := rawdb.ReadTd(e.tx, header.ParentHash, height-1)
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
		if err := rawdb.WriteHeader(e.tx, header); err != nil {
			return fmt.Errorf("InsertHeaders: writeHeader: %s", err)
		}

		if err := rawdb.WriteTd(e.tx, header.Hash(), height, td); err != nil {
			return fmt.Errorf("InsertHeaders: writeTd: %s", err)
		}

		if _, err := rawdb.WriteRawBodyIfNotExists(e.tx, header.Hash(), height, body.RawBody()); err != nil {
			return fmt.Errorf("InsertBlocks: writeBody: %s", err)
		}
	}

	return nil
}

func (e *polygonSyncStageExecutionEngine) UpdateForkChoice(ctx context.Context, tip *types.Header, _ *types.Header) error {
	if err := e.checkDependencies(); err != nil {
		return err
	}

	// make sure all state sync events for the given tip are downloaded to mdbx
	// NOTE: remove this once we integrate the bridge component in sync.Run
	if err := e.downloadStateSyncEvents(ctx, tip); err != nil {
		return err
	}

	// update stage progress
	tipBlockNum := tip.Number.Uint64()
	if err := e.stageState.Update(e.tx, tipBlockNum); err != nil {
		return err
	}

	// we need to interrupt sync.Run when we reach an UpdateForkChoice call
	// so that the stage can exit and allow the loop to proceed to execution
	e.interrupt(errUpdateForkChoiceInterrupt)

	e.logger.Info(e.appendLogPrefix("stage progress updated"), "block", tipBlockNum)
	return nil
}

func (e *polygonSyncStageExecutionEngine) CurrentHeader(ctx context.Context) (*types.Header, error) {
	if err := e.checkDependencies(); err != nil {
		return nil, err
	}

	stageProgress, err := stages.GetStageProgress(e.tx, stages.PolygonSync)
	if err != nil {
		return nil, err
	}

	header, err := e.blockReader.HeaderByNumber(ctx, e.tx, stageProgress)
	if err != nil {
		return nil, err
	}

	return header, nil
}

func (e *polygonSyncStageExecutionEngine) useTx(tx kv.RwTx) {
	e.tx = tx
}

func (e *polygonSyncStageExecutionEngine) useInterrupt(cancel context.CancelCauseFunc) {
	e.interrupt = cancel
}

func (e *polygonSyncStageExecutionEngine) useStageState(stageState *StageState) {
	e.stageState = stageState
	e.appendLogPrefix = newAppendLogPrefix(stageState.LogPrefix())
}

func (e *polygonSyncStageExecutionEngine) checkDependencies() error {
	if e.interrupt == nil {
		return errors.New("missing interrupt")
	}

	if e.stageState == nil {
		return errors.New("missing stage state")
	}

	if e.appendLogPrefix == nil {
		return errors.New("missing appendLogPrefix")
	}

	return nil
}

func (e *polygonSyncStageExecutionEngine) downloadStateSyncEvents(ctx context.Context, tip *types.Header) (err error) {
	e.initStateSyncEventIDOnce.Do(func() {
		e.lastStateSyncEventID, _, err = e.blockReader.LastEventId(ctx, e.tx)
	})
	if err != nil {
		return err
	}

	borConfig := e.chainConfig.Bor.(*borcfg.BorConfig)
	// need to use latest sprint start block num
	tipBlockNum := tip.Number.Uint64()
	sprintLen := borConfig.CalculateSprintLength(tipBlockNum)
	sprintRemainder := tipBlockNum % sprintLen
	if tipBlockNum > sprintLen && sprintRemainder > 0 {
		tipBlockNum -= sprintRemainder
		tip = rawdb.ReadHeaderByNumber(e.tx, tipBlockNum)
	}

	e.logger.Info(
		e.appendLogPrefix("downloading state sync event"),
		"sprintStartBlockNum", tip.Number.Uint64(),
		"lastStateSyncEventID", e.lastStateSyncEventID,
	)

	var records int
	var duration time.Duration
	e.lastStateSyncEventID, records, duration, err = fetchAndWriteHeimdallStateSyncEvents(
		ctx,
		tip,
		e.lastStateSyncEventID,
		e.tx,
		borConfig,
		e.blockReader,
		e.heimdallClient,
		e.chainConfig.ChainID.String(),
		e.stateReceiverABI,
		e.stageState.LogPrefix(),
		e.logger,
	)
	if err != nil {
		return err
	}

	e.logger.Info(
		e.appendLogPrefix("finished downloading state sync events"),
		"records", records,
		"duration", duration,
	)

	return nil
}

func newAppendLogPrefix(logPrefix string) func(msg string) string {
	return func(msg string) string {
		return fmt.Sprintf("[%s] %s", logPrefix, msg)
	}
}
