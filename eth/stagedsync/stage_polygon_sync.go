// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/metrics"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/log/v3"
	"github.com/ledgerwatch/erigon/accounts/abi"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/p2p/sentry"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/polygon/bridge"
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
	blockLimit uint,
	polygonBridge bridge.Service,
) PolygonSyncStageCfg {
	dataStream := make(chan polygonSyncStageDataItem)
	executionEngine := &polygonSyncStageExecutionEngine{
		db:          db,
		blockReader: blockReader,
		dataStream:  dataStream,
	}
	heimdallStore := &polygonSyncStageHeimdallStore{
		checkpoints: &polygonSyncStageCheckpointStore{
			db:               db,
			checkpointReader: blockReader,
			dataStream:       dataStream,
		},
		milestones: &polygonSyncStageMilestoneStore{
			db:              db,
			milestoneReader: blockReader,
			dataStream:      dataStream,
		},
		spans: &polygonSyncStageSpanStore{
			db:         db,
			spanReader: blockReader,
			dataStream: dataStream,
		},
	}
	heimdallService := heimdall.NewService(heimdallClient, heimdallStore, logger)
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	p2pService := p2p.NewService(maxPeers, logger, sentry, statusDataProvider.GetStatusData)
	checkpointVerifier := polygonsync.VerifyCheckpointHeaders
	milestoneVerifier := polygonsync.VerifyMilestoneHeaders
	blocksVerifier := polygonsync.VerifyBlocks
	syncStore := &polygonSyncStageSyncStore{
		db:          db,
		blockReader: blockReader,
		dataStream:  dataStream,
	}
	blockDownloader := polygonsync.NewBlockDownloader(
		logger,
		p2pService,
		heimdallService,
		checkpointVerifier,
		milestoneVerifier,
		blocksVerifier,
		syncStore,
		blockLimit,
	)
	spansCache := polygonsync.NewSpansCache()
	events := polygonsync.NewTipEvents(logger, p2pService, heimdallService)
	sync := polygonsync.NewSync(
		syncStore,
		executionEngine,
		milestoneVerifier,
		blocksVerifier,
		p2pService,
		blockDownloader,
		polygonsync.NewCanonicalChainBuilderFactory(chainConfig, borConfig, spansCache),
		spansCache,
		heimdallService.FetchLatestSpans,
		events.Events(),
		logger,
	)
	syncService := &polygonSyncStageService{
		logger:           logger,
		chainConfig:      chainConfig,
		blockReader:      blockReader,
		bridge:           polygonBridge,
		sync:             sync,
		events:           events,
		p2p:              p2pService,
		heimdall:         heimdallService,
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

func SpawnPolygonSyncStage(
	ctx context.Context,
	tx kv.RwTx,
	stageState *StageState,
	unwinder Unwinder,
	cfg PolygonSyncStageCfg,
) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err := cfg.service.Run(ctx, tx, stageState, unwinder); err != nil {
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
	// TODO - headers, bodies (including txnums index), checkpoints, milestones, spans, state sync events
	return nil
}

func PrunePolygonSyncStage() error {
	// TODO - headers, bodies (including txnums index), checkpoints, milestones, spans, state sync events
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
	bridge           bridge.Service
	sync             *polygonsync.Sync
	events           *polygonsync.TipEvents
	p2p              p2p.Service
	heimdall         heimdall.Service
	heimdallClient   heimdall.HeimdallClient
	stateReceiverABI abi.ABI
	dataStream       <-chan polygonSyncStageDataItem
	stopNode         func() error
	// internal
	appendLogPrefix          func(string) string
	stageState               *StageState
	unwinder                 Unwinder
	cachedForkChoice         *types.Header
	lastStateSyncEventId     uint64
	lastStateSyncEventIdInit bool
	bgComponentsRun          bool
	bgComponentsErr          chan error
}

func (s *polygonSyncStageService) Run(ctx context.Context, tx kv.RwTx, stageState *StageState, unwinder Unwinder) error {
	s.appendLogPrefix = newAppendLogPrefix(stageState.LogPrefix())
	s.stageState = stageState
	s.unwinder = unwinder
	s.logger.Info(s.appendLogPrefix("begin..."), "progress", stageState.BlockNumber)

	s.runBgComponentsOnce(ctx)

	if s.cachedForkChoice != nil {
		err := s.handleUpdateForkChoice(tx, s.cachedForkChoice)
		if err != nil {
			return err
		}

		s.cachedForkChoice = nil
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err := <-s.bgComponentsErr:
			// call stop in separate goroutine to avoid deadlock due to "waitForStageLoopStop"
			go func() {
				s.logger.Error(s.appendLogPrefix("stopping node"), "err", err)
				err = s.stopNode()
				if err != nil {
					s.logger.Error(s.appendLogPrefix("could not stop node cleanly"), "err", err)
				}
			}()

			// use ErrStopped to exit the stage loop
			return fmt.Errorf("%w: %w", common.ErrStopped, err)
		case data := <-s.dataStream:
			var err error
			if data.updateForkChoice != nil {
				// exit stage upon update fork choice
				return s.handleUpdateForkChoice(tx, data.updateForkChoice)
			} else if len(data.insertBlocks) > 0 {
				err = s.handleInsertBlocks(ctx, tx, data.insertBlocks)
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

func (s *polygonSyncStageService) runBgComponentsOnce(ctx context.Context) {
	if s.bgComponentsRun {
		return
	}

	s.logger.Info(s.appendLogPrefix("running background components"))
	s.bgComponentsRun = true
	s.bgComponentsErr = make(chan error)

	go func() {
		eg, ctx := errgroup.WithContext(ctx)

		eg.Go(func() error {
			return s.events.Run(ctx)
		})

		eg.Go(func() error {
			return s.heimdall.Run(ctx)
		})

		if s.bridge != nil {
			eg.Go(func() error {
				return s.bridge.Run(ctx)
			})
		}

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

func (s *polygonSyncStageService) handleInsertBlocks(ctx context.Context, tx kv.RwTx, blocks []*types.Block) error {
	stateSyncEventsLogTicker := time.NewTicker(logInterval)
	defer stateSyncEventsLogTicker.Stop()

	for _, block := range blocks {
		height := block.NumberU64()
		header := block.Header()
		body := block.Body()

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height-1, s.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height-1, s.logger)

		var parentTd *big.Int
		var err error
		if height > 0 {
			// Parent's total difficulty
			parentTd, err = rawdb.ReadTd(tx, header.ParentHash, height-1)
			if err != nil || parentTd == nil {
				return fmt.Errorf(
					"parent's total difficulty not found with hash %x and height %d: %v",
					header.ParentHash,
					height-1,
					err,
				)
			}
		} else {
			parentTd = big.NewInt(0)
		}

		td := parentTd.Add(parentTd, header.Difficulty)
		if err := rawdb.WriteHeader(tx, header); err != nil {
			return err
		}

		if err := rawdb.WriteTd(tx, header.Hash(), height, td); err != nil {
			return err
		}

		if _, err := rawdb.WriteRawBodyIfNotExists(tx, header.Hash(), height, body.RawBody()); err != nil {
			return err
		}

		if err := s.downloadStateSyncEvents(ctx, tx, header, stateSyncEventsLogTicker); err != nil {
			return err
		}
	}

	if s.bridge != nil {
		err := s.bridge.ProcessNewBlocks(ctx, blocks)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *polygonSyncStageService) handleUpdateForkChoice(tx kv.RwTx, tip *types.Header) error {
	tipBlockNum := tip.Number.Uint64()
	tipHash := tip.Hash()

	s.logger.Info(s.appendLogPrefix("handle update fork choice"), "block", tipBlockNum, "hash", tipHash)

	logPrefix := s.stageState.LogPrefix()
	logTicker := time.NewTicker(logInterval)
	defer logTicker.Stop()

	newNodes, badNodes, err := fixCanonicalChain(logPrefix, logTicker, tipBlockNum, tipHash, tx, s.blockReader, s.logger)
	if err != nil {
		return err
	}

	if len(badNodes) > 0 {
		badNode := badNodes[len(badNodes)-1]
		s.cachedForkChoice = tip
		return s.unwinder.UnwindTo(badNode.number, ForkReset(badNode.hash), tx)
	}

	if len(newNodes) == 0 {
		return nil
	}

	if err := rawdb.AppendCanonicalTxNums(tx, newNodes[len(newNodes)-1].number); err != nil {
		return err
	}

	if err := rawdb.WriteHeadHeaderHash(tx, tipHash); err != nil {
		return err
	}

	if err := s.stageState.Update(tx, tipBlockNum); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(tx, stages.Headers, tipBlockNum); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(tx, stages.BlockHashes, tipBlockNum); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(tx, stages.Bodies, tipBlockNum); err != nil {
		return err
	}

	return nil
}

func (s *polygonSyncStageService) downloadStateSyncEvents(
	ctx context.Context,
	tx kv.RwTx,
	header *types.Header,
	logTicker *time.Ticker,
) error {
	var err error
	if !s.lastStateSyncEventIdInit {
		s.lastStateSyncEventId, _, err = s.blockReader.LastEventId(ctx, tx)
	}
	if err != nil {
		return err
	}

	s.lastStateSyncEventIdInit = true
	newStateSyncEventId, records, duration, err := fetchRequiredHeimdallStateSyncEventsIfNeeded(
		ctx,
		header,
		tx,
		s.chainConfig.Bor.(*borcfg.BorConfig),
		s.blockReader,
		s.heimdallClient,
		s.chainConfig.ChainID.String(),
		s.stateReceiverABI,
		s.stageState.LogPrefix(),
		s.logger,
		s.lastStateSyncEventId,
	)
	if err != nil {
		return err
	}

	if s.lastStateSyncEventId == newStateSyncEventId {
		return nil
	}

	select {
	case <-logTicker.C:
		s.logger.Info(
			s.appendLogPrefix("downloading state sync events progress"),
			"blockNum", header.Number,
			"records", records,
			"duration", duration,
		)
	default:
		// carry on
	}

	s.lastStateSyncEventId = newStateSyncEventId
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

type polygonSyncStageSyncStore struct {
	db          kv.RoDB
	blockReader services.FullBlockReader
	dataStream  chan<- polygonSyncStageDataItem
}

func (s *polygonSyncStageSyncStore) InsertBlocks(_ context.Context, blocks []*types.Block) error {
	s.dataStream <- polygonSyncStageDataItem{
		insertBlocks: blocks,
	}

	return nil
}

func (s *polygonSyncStageSyncStore) Flush(context.Context, *types.Header) error {
	return nil
}

func (s *polygonSyncStageSyncStore) Run(context.Context) error {
	return nil
}

type polygonSyncStageHeimdallStore struct {
	checkpoints *polygonSyncStageCheckpointStore
	milestones  *polygonSyncStageMilestoneStore
	spans       *polygonSyncStageSpanStore
}

func (s polygonSyncStageHeimdallStore) Checkpoints() heimdall.EntityStore[*heimdall.Checkpoint] {
	return s.checkpoints
}

func (s polygonSyncStageHeimdallStore) Milestones() heimdall.EntityStore[*heimdall.Milestone] {
	return s.milestones
}

func (s polygonSyncStageHeimdallStore) Spans() heimdall.EntityStore[*heimdall.Span] {
	return s.spans
}

func (s polygonSyncStageHeimdallStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageHeimdallStore) Close() {
	// no-op
}

type polygonSyncStageCheckpointStore struct {
	db               kv.RoDB
	checkpointReader services.BorCheckpointReader
	dataStream       chan<- polygonSyncStageDataItem
}

func (s polygonSyncStageCheckpointStore) GetLastEntityId(ctx context.Context) (id uint64, ok bool, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		id, ok, err = s.checkpointReader.LastCheckpointId(ctx, tx)
		return err
	})
	return id, ok, err
}

func (s polygonSyncStageCheckpointStore) GetLastEntity(ctx context.Context) (cp *heimdall.Checkpoint, err error) {
	id, ok, err := s.GetLastEntityId(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("last checkpoint not found")
	}

	return s.GetEntity(ctx, id)
}

func (s polygonSyncStageCheckpointStore) GetEntity(ctx context.Context, id uint64) (cp *heimdall.Checkpoint, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		var cpBytes []byte
		cpBytes, err = s.checkpointReader.Checkpoint(ctx, tx, id)
		if err != nil {
			return err
		}

		return json.Unmarshal(cpBytes, &cp)
	})

	return cp, err
}

func (s polygonSyncStageCheckpointStore) PutEntity(_ context.Context, id uint64, entity *heimdall.Checkpoint) error {
	entity.Id = heimdall.CheckpointId(id)
	s.dataStream <- polygonSyncStageDataItem{
		checkpoint: entity,
	}

	return nil
}

func (s polygonSyncStageCheckpointStore) RangeFromBlockNum(ctx context.Context, blockNum uint64) ([]*heimdall.Checkpoint, error) {
	makeCheckpoint := func() *heimdall.Checkpoint { return &heimdall.Checkpoint{} }
	return blockRangeEntitiesFromBlockNum[*heimdall.Checkpoint](ctx, s.db, kv.BorCheckpoints, makeCheckpoint, blockNum)
}

func (s polygonSyncStageCheckpointStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageCheckpointStore) Close() {
	// no-op
}

type polygonSyncStageMilestoneStore struct {
	db              kv.RoDB
	milestoneReader services.BorMilestoneReader
	dataStream      chan<- polygonSyncStageDataItem
}

func (s polygonSyncStageMilestoneStore) GetLastEntityId(ctx context.Context) (id uint64, ok bool, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		id, ok, err = s.milestoneReader.LastMilestoneId(ctx, tx)
		return err
	})
	return id, ok, err
}

func (s polygonSyncStageMilestoneStore) GetLastEntity(ctx context.Context) (*heimdall.Milestone, error) {
	id, ok, err := s.GetLastEntityId(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("last milestone not found")
	}

	return s.GetEntity(ctx, id)
}

func (s polygonSyncStageMilestoneStore) GetEntity(ctx context.Context, id uint64) (ms *heimdall.Milestone, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		var msBytes []byte
		msBytes, err = s.milestoneReader.Milestone(ctx, tx, id)
		if err != nil {
			return err
		}

		return json.Unmarshal(msBytes, &ms)
	})

	return ms, err
}

func (s polygonSyncStageMilestoneStore) PutEntity(_ context.Context, _ uint64, entity *heimdall.Milestone) error {
	s.dataStream <- polygonSyncStageDataItem{
		milestone: entity,
	}

	return nil
}

func (s polygonSyncStageMilestoneStore) RangeFromBlockNum(ctx context.Context, blockNum uint64) ([]*heimdall.Milestone, error) {
	makeMilestone := func() *heimdall.Milestone { return &heimdall.Milestone{} }
	return blockRangeEntitiesFromBlockNum(ctx, s.db, kv.BorMilestones, makeMilestone, blockNum)
}

func (s polygonSyncStageMilestoneStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageMilestoneStore) Close() {
	// no-op
}

type polygonSyncStageSpanStore struct {
	db         kv.RoDB
	spanReader services.BorSpanReader
	dataStream chan<- polygonSyncStageDataItem
}

func (s polygonSyncStageSpanStore) GetLastEntityId(ctx context.Context) (id uint64, ok bool, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		id, ok, err = s.spanReader.LastSpanId(ctx, tx)
		return err
	})
	return id, ok, err
}

func (s polygonSyncStageSpanStore) GetLastEntity(ctx context.Context) (*heimdall.Span, error) {
	id, ok, err := s.GetLastEntityId(ctx)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, errors.New("last span not found")
	}

	return s.GetEntity(ctx, id)
}

func (s polygonSyncStageSpanStore) GetEntity(ctx context.Context, id uint64) (span *heimdall.Span, err error) {
	err = s.db.View(ctx, func(tx kv.Tx) error {
		var spanBytes []byte
		spanBytes, err = s.spanReader.Span(ctx, tx, id)
		if err != nil {
			return err
		}

		return json.Unmarshal(spanBytes, &span)
	})

	return span, err
}

func (s polygonSyncStageSpanStore) PutEntity(_ context.Context, _ uint64, entity *heimdall.Span) error {
	s.dataStream <- polygonSyncStageDataItem{
		span: entity,
	}

	return nil
}

func (s polygonSyncStageSpanStore) RangeFromBlockNum(ctx context.Context, blockNum uint64) ([]*heimdall.Span, error) {
	makeSpan := func() *heimdall.Span { return &heimdall.Span{} }
	return blockRangeEntitiesFromBlockNum[*heimdall.Span](ctx, s.db, kv.BorSpans, makeSpan, blockNum)
}

func (s polygonSyncStageSpanStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageSpanStore) Close() {
	// no-op
}

type blockRangeComparator interface {
	CmpRange(blockNum uint64) int
}

func blockRangeEntitiesFromBlockNum[T blockRangeComparator](
	ctx context.Context,
	db kv.RoDB,
	table string,
	makeEntity func() T,
	blockNum uint64,
) ([]T, error) {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return nil, err
	}

	defer tx.Rollback()
	cur, err := tx.Cursor(table)
	if err != nil {
		return nil, err
	}

	defer cur.Close()
	var k, v []byte
	var entities []T
	for k, v, err = cur.Last(); err != nil && k != nil; _, v, err = cur.Prev() {
		entity := makeEntity()
		err = json.Unmarshal(v, entity)
		if err != nil {
			return nil, err
		}
		if entity.CmpRange(blockNum) == 1 {
			break
		}
		entities = append(entities, entity)
	}
	if err != nil {
		return nil, err
	}

	slices.Reverse(entities)
	return entities, nil
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

	hash := rawdb.ReadHeadHeaderHash(tx)
	header := rawdb.ReadHeader(tx, hash, stageBlockNum)
	if header == nil {
		return nil, errors.New("header not found")
	}

	return header, nil
}

func newAppendLogPrefix(logPrefix string) func(msg string) string {
	return func(msg string) string {
		return fmt.Sprintf("[%s] %s", logPrefix, msg)
	}
}
