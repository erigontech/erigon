package stagedsync

import (
	"context"
	"errors"
	"fmt"
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

const hashProgressKeySuffix = "_hash"

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
	dataStream := make(chan polygonSyncStageDataItem)
	storage := &polygonSyncStageStorage{
		db:          db,
		blockReader: blockReader,
		dataStream:  dataStream,
	}
	executionEngine := &polygonSyncStageExecutionEngine{
		db:          db,
		blockReader: blockReader,
		dataStream:  dataStream,
	}
	p2pService := p2p.NewService(maxPeers, logger, sentry, statusDataProvider.GetStatusData)
	checkpointVerifier := polygonsync.VerifyCheckpointHeaders
	milestoneVerifier := polygonsync.VerifyMilestoneHeaders
	blocksVerifier := polygonsync.VerifyBlocks
	heimdallService := heimdall.NewHeimdall(heimdallClient, logger, heimdall.WithStore(storage))
	blockDownloader := polygonsync.NewBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		checkpointVerifier,
		milestoneVerifier,
		blocksVerifier,
		storage,
	)
	spansCache := polygonsync.NewSpansCache()
	events := polygonsync.NewTipEvents(logger, p2pService, heimdallService)
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	sync := polygonsync.NewSync(
		storage,
		executionEngine,
		milestoneVerifier,
		blocksVerifier,
		p2pService,
		blockDownloader,
		polygonsync.NewCanonicalChainBuilderFactory(chainConfig, borConfig, spansCache),
		spansCache,
		heimdallService.FetchLatestSpan,
		events.Events(),
		logger,
	)
	syncService := &polygonSyncStageService{
		logger:           logger,
		chainConfig:      chainConfig,
		blockReader:      blockReader,
		sync:             sync,
		events:           events,
		p2p:              p2pService,
		heimdallClient:   heimdallClient,
		stateReceiverABI: stateReceiverABI,
		dataStream:       dataStream,
		stopNode:         stopNode,
	}
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

type polygonSyncStageDataItem struct {
	updateForkChoice *types.Header
	insertBlocks     []*types.Block
	span             *heimdall.Span
	milestone        *heimdall.Milestone
	checkpoint       *heimdall.Checkpoint
}

type polygonSyncStageService struct {
	logger           log.Logger
	chainConfig      *chain.Config
	blockReader      services.FullBlockReader
	sync             *polygonsync.Sync
	events           *polygonsync.TipEvents
	p2p              p2p.Service
	heimdallClient   heimdall.HeimdallClient
	stateReceiverABI abi.ABI
	dataStream       <-chan polygonSyncStageDataItem
	stopNode         func() error
	// internal
	appendLogPrefix          func(string) string
	stageState               *StageState
	lastStateSyncEventId     uint64
	lastStateSyncEventIdInit bool
	bgComponentsRun          bool
	bgComponentsErr          chan error
}

func (s *polygonSyncStageService) Run(ctx context.Context, tx kv.RwTx, stageState *StageState) error {
	s.appendLogPrefix = newAppendLogPrefix(stageState.LogPrefix())
	s.stageState = stageState
	s.logger.Info(s.appendLogPrefix("begin..."), "progress", stageState.BlockNumber)

	if !s.bgComponentsRun {
		s.runBgComponents(ctx)
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-s.bgComponentsErr:
			s.logger.Error(s.appendLogPrefix("stopping node"), "err", err)
			stopErr := s.stopNode()
			if stopErr != nil {
				return fmt.Errorf("%w: %w", stopErr, err)
			}
			return err
		case data := <-s.dataStream:
			var err error
			if data.updateForkChoice != nil {
				// exit stage upon update fork choice
				return s.handleUpdateForkChoice(ctx, tx, data.updateForkChoice)
			} else if len(data.insertBlocks) > 0 {
				err = s.handleInsertBlocks(tx, stageState, data.insertBlocks)
			} else if data.span != nil {
				err = s.handleSpan(ctx, tx, data.span)
			} else if data.checkpoint != nil {
				err = s.handleCheckpoint(ctx, tx, data.checkpoint)
			} else if data.milestone != nil {
				err = s.handleMilestone(ctx, tx, data.milestone)
			} else {
				err = errors.New("unrecognized data")
			}
			if err != nil {
				return err
			}
		}
	}
}

func (s *polygonSyncStageService) runBgComponents(ctx context.Context) {
	s.logger.Info(s.appendLogPrefix("running background components"))
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

		eg.Go(func() error {
			return s.sync.Run(ctx)
		})

		if err := eg.Wait(); err != nil {
			s.bgComponentsErr <- err
		}
	}()
}

func (s *polygonSyncStageService) handleInsertBlocks(tx kv.RwTx, stageState *StageState, blocks []*types.Block) error {
	for _, block := range blocks {
		height := block.NumberU64()
		header := block.Header()
		body := block.Body()

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height-1, s.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height-1, s.logger)

		parentTd := common.Big0
		if height > 0 {
			// Parent's total difficulty
			parentTd, err := rawdb.ReadTd(tx, header.ParentHash, height-1)
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
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return fmt.Errorf("InsertHeaders: writeHeader: %s", err)
		}

		if err := rawdb.WriteTd(tx, header.Hash(), height, td); err != nil {
			return fmt.Errorf("InsertHeaders: writeTd: %s", err)
		}

		if _, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), height, body.RawBody()); err != nil {
			return fmt.Errorf("InsertBlocks: writeBody: %s", err)
		}
	}

	if len(blocks) == 0 {
		return nil
	}

	// update stage progress
	tip := blocks[len(blocks)-1]
	tipBlockNum := tip.NumberU64()
	tipBlockHash := tip.Hash()

	if err := stageState.Update(tx, tipBlockNum); err != nil {
		return err
	}

	if err := saveStageHashProgress(tx, stageState.ID, tipBlockHash); err != nil {
		return err
	}

	return nil
}

func (s *polygonSyncStageService) handleUpdateForkChoice(ctx context.Context, tx kv.RwTx, tip *types.Header) error {
	s.logger.Info(s.appendLogPrefix("handle update fork choice"), "block", tip.Number.Uint64())

	// make sure all state sync events for the given tip are downloaded to mdbx
	// NOTE: remove this once we integrate the bridge component in sync.Run
	if err := s.downloadStateSyncEvents(ctx, tx, tip); err != nil {
		return err
	}

	return nil
}

func (s *polygonSyncStageService) downloadStateSyncEvents(ctx context.Context, tx kv.RwTx, tip *types.Header) (err error) {
	if !s.lastStateSyncEventIdInit {
		s.lastStateSyncEventId, _, err = s.blockReader.LastEventId(ctx, tx)
	}
	if err != nil {
		return err
	}

	borConfig := s.chainConfig.Bor.(*borcfg.BorConfig)
	// need to use latest sprint start block num
	tipBlockNum := tip.Number.Uint64()
	sprintLen := borConfig.CalculateSprintLength(tipBlockNum)
	sprintRemainder := tipBlockNum % sprintLen
	if tipBlockNum > sprintLen && sprintRemainder > 0 {
		tipBlockNum -= sprintRemainder
		tip = rawdb.ReadHeaderByNumber(tx, tipBlockNum)
	}

	s.logger.Info(
		s.appendLogPrefix("downloading state sync events"),
		"sprintStartBlockNum", tip.Number.Uint64(),
		"lastStateSyncEventId", s.lastStateSyncEventId,
	)

	var records int
	var duration time.Duration
	s.lastStateSyncEventId, records, duration, err = fetchAndWriteHeimdallStateSyncEvents(
		ctx,
		tip,
		s.lastStateSyncEventId,
		tx,
		borConfig,
		s.blockReader,
		s.heimdallClient,
		s.chainConfig.ChainID.String(),
		s.stateReceiverABI,
		s.stageState.LogPrefix(),
		s.logger,
	)
	if err != nil {
		return err
	}

	s.logger.Info(
		s.appendLogPrefix("finished downloading state sync events"),
		"records", records,
		"duration", duration,
	)

	return nil
}

func (s *polygonSyncStageService) handleSpan(ctx context.Context, tx kv.RwTx, sp *heimdall.Span) error {
	return heimdall.NewTxStore(s.blockReader, tx).PutSpan(ctx, sp)
}

func (s *polygonSyncStageService) handleCheckpoint(ctx context.Context, tx kv.RwTx, cp *heimdall.Checkpoint) error {
	return heimdall.NewTxStore(s.blockReader, tx).PutCheckpoint(ctx, cp.Id, cp)
}

func (s *polygonSyncStageService) handleMilestone(ctx context.Context, tx kv.RwTx, ms *heimdall.Milestone) error {
	return heimdall.NewTxStore(s.blockReader, tx).PutMilestone(ctx, ms.Id, ms)
}

type polygonSyncStageStorage struct {
	db          kv.RoDB
	blockReader services.FullBlockReader
	dataStream  chan<- polygonSyncStageDataItem
}

func (s *polygonSyncStageStorage) LastSpanId(ctx context.Context) (id heimdall.SpanId, ok bool, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		id, ok, err = heimdall.NewTxReadStore(s.blockReader, tx).LastSpanId(ctx)
		return err
	})
	return
}

func (s *polygonSyncStageStorage) GetSpan(ctx context.Context, id heimdall.SpanId) (sp *heimdall.Span, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		sp, err = heimdall.NewTxReadStore(s.blockReader, tx).GetSpan(ctx, id)
		return err
	})
	return
}

func (s *polygonSyncStageStorage) PutSpan(_ context.Context, span *heimdall.Span) error {
	s.dataStream <- polygonSyncStageDataItem{
		span: span,
	}

	return nil
}

func (s *polygonSyncStageStorage) LastMilestoneId(ctx context.Context) (id heimdall.MilestoneId, ok bool, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		id, ok, err = heimdall.NewTxReadStore(s.blockReader, tx).LastMilestoneId(ctx)
		return err
	})
	return
}

func (s *polygonSyncStageStorage) GetMilestone(ctx context.Context, id heimdall.MilestoneId) (ms *heimdall.Milestone, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		ms, err = heimdall.NewTxReadStore(s.blockReader, tx).GetMilestone(ctx, id)
		return err
	})
	return
}

func (s *polygonSyncStageStorage) PutMilestone(_ context.Context, _ heimdall.MilestoneId, ms *heimdall.Milestone) error {
	s.dataStream <- polygonSyncStageDataItem{
		milestone: ms,
	}

	return nil
}

func (s *polygonSyncStageStorage) LastCheckpointId(ctx context.Context) (id heimdall.CheckpointId, ok bool, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		id, ok, err = heimdall.NewTxReadStore(s.blockReader, tx).LastCheckpointId(ctx)
		return err
	})
	return
}

func (s *polygonSyncStageStorage) GetCheckpoint(ctx context.Context, id heimdall.CheckpointId) (cp *heimdall.Checkpoint, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		cp, err = heimdall.NewTxReadStore(s.blockReader, tx).GetCheckpoint(ctx, id)
		return err
	})
	return
}

func (s *polygonSyncStageStorage) PutCheckpoint(_ context.Context, _ heimdall.CheckpointId, cp *heimdall.Checkpoint) error {
	s.dataStream <- polygonSyncStageDataItem{
		checkpoint: cp,
	}

	return nil
}

func (s *polygonSyncStageStorage) InsertBlocks(_ context.Context, blocks []*types.Block) error {
	s.dataStream <- polygonSyncStageDataItem{
		insertBlocks: blocks,
	}

	return nil
}

func (s *polygonSyncStageStorage) Flush(context.Context) error {
	return nil
}

func (s *polygonSyncStageStorage) Run(context.Context) error {
	return nil
}

type polygonSyncStageExecutionEngine struct {
	db          kv.RoDB
	blockReader services.FullBlockReader
	dataStream  chan<- polygonSyncStageDataItem
}

func (e *polygonSyncStageExecutionEngine) InsertBlocks(_ context.Context, blocks []*types.Block) error {
	e.dataStream <- polygonSyncStageDataItem{
		insertBlocks: blocks,
	}

	return nil
}

func (e *polygonSyncStageExecutionEngine) UpdateForkChoice(_ context.Context, tip *types.Header, _ *types.Header) error {
	e.dataStream <- polygonSyncStageDataItem{
		updateForkChoice: tip,
	}

	return nil
}

func (e *polygonSyncStageExecutionEngine) CurrentHeader(ctx context.Context) (*types.Header, error) {
	tx, err := e.db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()

	stageBlockNum, err := stages.GetStageProgress(tx, stages.PolygonSync)
	if err != nil {
		return nil, err
	}

	snapshotBlockNum := e.blockReader.FrozenBlocks()
	if stageBlockNum < snapshotBlockNum {
		return e.blockReader.HeaderByNumber(ctx, tx, snapshotBlockNum)
	}

	stageHash, err := readStageHashProgress(tx, stages.PolygonSync)
	if err != nil {
		return nil, err
	}

	header := rawdb.ReadHeader(tx, stageHash, stageBlockNum)
	if header == nil {
		return nil, errors.New("header not found")
	}

	return header, nil
}

func saveStageHashProgress(db kv.Putter, stageId stages.SyncStage, progress common.Hash) error {
	return db.Put(kv.SyncStageProgress, hashProgressKey(stageId), progress[:])
}

func readStageHashProgress(db kv.Getter, stageId stages.SyncStage) (common.Hash, error) {
	hashBytes, err := db.GetOne(kv.SyncStageProgress, hashProgressKey(stageId))
	if err != nil {
		return common.Hash{}, err
	}

	return common.BytesToHash(hashBytes), nil
}

func hashProgressKey(stageId stages.SyncStage) []byte {
	return []byte(stageId + hashProgressKeySuffix)
}

func newAppendLogPrefix(logPrefix string) func(msg string) string {
	return func(msg string) string {
		return fmt.Sprintf("[%s] %s", logPrefix, msg)
	}
}
