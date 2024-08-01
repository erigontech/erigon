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
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"slices"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/generics"
	"github.com/erigontech/erigon-lib/common/metrics"
	"github.com/erigontech/erigon-lib/direct"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/accounts/abi"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/p2p"
	polygonsync "github.com/erigontech/erigon/polygon/sync"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var updateForkChoiceSuccessErr = errors.New("update fork choice success")

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
) PolygonSyncStageCfg {
	txActionStream := make(chan polygonSyncStageTxAction)
	executionEngine := &polygonSyncStageExecutionEngine{
		blockReader:      blockReader,
		txActionStream:   txActionStream,
		logger:           logger,
		heimdallClient:   heimdallClient,
		stateReceiverABI: stateReceiverABI,
		chainConfig:      chainConfig,
	}
	heimdallStore := &polygonSyncStageHeimdallStore{
		checkpoints: &polygonSyncStageCheckpointStore{
			checkpointReader: blockReader,
			txActionStream:   txActionStream,
		},
		milestones: &polygonSyncStageMilestoneStore{
			milestoneReader: blockReader,
			txActionStream:  txActionStream,
		},
		spans: &polygonSyncStageSpanStore{
			spanReader:     blockReader,
			txActionStream: txActionStream,
		},
	}
	heimdallService := heimdall.NewService(heimdallClient, heimdallStore, logger)
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	p2pService := p2p.NewService(maxPeers, logger, sentry, statusDataProvider.GetStatusData)
	checkpointVerifier := polygonsync.VerifyCheckpointHeaders
	milestoneVerifier := polygonsync.VerifyMilestoneHeaders
	blocksVerifier := polygonsync.VerifyBlocks
	syncStore := &polygonSyncStageSyncStore{
		executionEngine: executionEngine,
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
		heimdallService.LatestSpans,
		events.Events(),
		logger,
	)
	syncService := &polygonSyncStageService{
		logger:          logger,
		sync:            sync,
		events:          events,
		p2p:             p2pService,
		executionEngine: executionEngine,
		heimdall:        heimdallService,
		txActionStream:  txActionStream,
		stopNode:        stopNode,
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

type polygonSyncStageTxAction struct {
	apply func(tx kv.RwTx) error
}

type polygonSyncStageService struct {
	logger          log.Logger
	sync            *polygonsync.Sync
	events          *polygonsync.TipEvents
	p2p             p2p.Service
	executionEngine *polygonSyncStageExecutionEngine
	heimdall        heimdall.Service
	txActionStream  <-chan polygonSyncStageTxAction
	stopNode        func() error
	// internal
	appendLogPrefix func(string) string
	bgComponentsRun bool
	bgComponentsErr chan error
}

func (s *polygonSyncStageService) Run(ctx context.Context, tx kv.RwTx, stageState *StageState, unwinder Unwinder) error {
	s.appendLogPrefix = newAppendLogPrefix(stageState.LogPrefix())
	s.executionEngine.appendLogPrefix = s.appendLogPrefix
	s.executionEngine.stageState = stageState
	s.executionEngine.unwinder = unwinder
	s.logger.Info(s.appendLogPrefix("begin..."), "progress", stageState.BlockNumber)

	s.runBgComponentsOnce(ctx)

	if s.executionEngine.cachedForkChoice != nil {
		err := s.executionEngine.UpdateForkChoice(ctx, s.executionEngine.cachedForkChoice, nil)
		if err != nil {
			return err
		}

		s.executionEngine.cachedForkChoice = nil
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
		case txAction := <-s.txActionStream:
			err := txAction.apply(tx)
			if errors.Is(err, updateForkChoiceSuccessErr) {
				return nil
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

type polygonSyncStageSyncStore struct {
	executionEngine *polygonSyncStageExecutionEngine
}

func (s *polygonSyncStageSyncStore) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	return s.executionEngine.InsertBlocks(ctx, blocks)
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
	checkpointReader services.BorCheckpointReader
	txActionStream   chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageCheckpointStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	type response struct {
		id  uint64
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		id, ok, err := s.checkpointReader.LastCheckpointId(ctx, tx)
		responseStream <- response{id: id, ok: ok, err: err}
		return nil
	})
	if err != nil {
		return 0, false, err
	}

	return r.id, r.ok, r.err
}

func (s polygonSyncStageCheckpointStore) LastEntity(ctx context.Context) (*heimdall.Checkpoint, bool, error) {
	id, ok, err := s.LastEntityId(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	return s.Entity(ctx, id)
}

func (s polygonSyncStageCheckpointStore) Entity(ctx context.Context, id uint64) (*heimdall.Checkpoint, bool, error) {
	type response struct {
		v   []byte
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		v, err := s.checkpointReader.Checkpoint(ctx, tx, id)
		responseStream <- response{v: v, err: err}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if r.err != nil {
		if errors.Is(r.err, freezeblocks.ErrCheckpointNotFound) {
			return nil, false, nil
		}

		return nil, false, r.err
	}

	var c heimdall.Checkpoint
	err = json.Unmarshal(r.v, &c)
	return &c, true, err
}

func (s polygonSyncStageCheckpointStore) PutEntity(ctx context.Context, id uint64, entity *heimdall.Checkpoint) error {
	entity.Id = heimdall.CheckpointId(id)

	var k [8]byte
	binary.BigEndian.PutUint64(k[:], id)

	v, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		responseStream <- response{err: tx.Put(kv.BorCheckpoints, k[:], v)}
		return nil
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageCheckpointStore) RangeFromBlockNum(ctx context.Context, blockNum uint64) ([]*heimdall.Checkpoint, error) {
	type response struct {
		result []*heimdall.Checkpoint
		err    error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		makeEntity := func() *heimdall.Checkpoint { return &heimdall.Checkpoint{} }
		r, err := blockRangeEntitiesFromBlockNum(tx, kv.BorCheckpoints, makeEntity, blockNum)
		responseStream <- response{result: r, err: err}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return r.result, r.err
}

func (s polygonSyncStageCheckpointStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageCheckpointStore) Close() {
	// no-op
}

type polygonSyncStageMilestoneStore struct {
	milestoneReader services.BorMilestoneReader
	txActionStream  chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageMilestoneStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	type response struct {
		id  uint64
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		id, ok, err := s.milestoneReader.LastMilestoneId(ctx, tx)
		responseStream <- response{id: id, ok: ok, err: err}
		return nil
	})
	if err != nil {
		return 0, false, err
	}

	return r.id, r.ok, r.err
}

func (s polygonSyncStageMilestoneStore) LastEntity(ctx context.Context) (*heimdall.Milestone, bool, error) {
	id, ok, err := s.LastEntityId(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	return s.Entity(ctx, id)
}

func (s polygonSyncStageMilestoneStore) Entity(ctx context.Context, id uint64) (*heimdall.Milestone, bool, error) {
	type response struct {
		v   []byte
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		v, err := s.milestoneReader.Milestone(ctx, tx, id)
		responseStream <- response{v: v, err: err}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if r.err != nil {
		if errors.Is(r.err, freezeblocks.ErrMilestoneNotFound) {
			return nil, false, nil
		}

		return nil, false, r.err
	}

	var m heimdall.Milestone
	err = json.Unmarshal(r.v, &m)
	return &m, true, err
}

func (s polygonSyncStageMilestoneStore) PutEntity(ctx context.Context, id uint64, entity *heimdall.Milestone) error {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], id)

	v, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		responseStream <- response{err: tx.Put(kv.BorMilestones, k[:], v)}
		return nil
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageMilestoneStore) RangeFromBlockNum(ctx context.Context, blockNum uint64) ([]*heimdall.Milestone, error) {
	type response struct {
		result []*heimdall.Milestone
		err    error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		makeEntity := func() *heimdall.Milestone { return &heimdall.Milestone{} }
		r, err := blockRangeEntitiesFromBlockNum(tx, kv.BorMilestones, makeEntity, blockNum)
		responseStream <- response{result: r, err: err}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return r.result, r.err
}

func (s polygonSyncStageMilestoneStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageMilestoneStore) Close() {
	// no-op
}

type polygonSyncStageSpanStore struct {
	spanReader     services.BorSpanReader
	txActionStream chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageSpanStore) LastEntityId(ctx context.Context) (id uint64, ok bool, err error) {
	type response struct {
		id  uint64
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		id, ok, err := s.spanReader.LastSpanId(ctx, tx)
		responseStream <- response{id: id, ok: ok, err: err}
		return nil
	})
	if err != nil {
		return 0, false, err
	}

	return r.id, r.ok, r.err
}

func (s polygonSyncStageSpanStore) LastEntity(ctx context.Context) (*heimdall.Span, bool, error) {
	id, ok, err := s.LastEntityId(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	return s.Entity(ctx, id)
}

func (s polygonSyncStageSpanStore) Entity(ctx context.Context, id uint64) (*heimdall.Span, bool, error) {
	type response struct {
		v   []byte
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		v, err := s.spanReader.Span(ctx, tx, id)
		responseStream <- response{v: v, err: err}
		return nil
	})
	if err != nil {
		return nil, false, err
	}
	if r.err != nil {
		if errors.Is(r.err, freezeblocks.ErrSpanNotFound) {
			return nil, false, nil
		}

		return nil, false, r.err
	}

	var span heimdall.Span
	err = json.Unmarshal(r.v, &span)
	return &span, true, err
}

func (s polygonSyncStageSpanStore) PutEntity(ctx context.Context, id uint64, entity *heimdall.Span) error {
	var k [8]byte
	binary.BigEndian.PutUint64(k[:], id)

	v, err := json.Marshal(entity)
	if err != nil {
		return err
	}

	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		responseStream <- response{err: tx.Put(kv.BorSpans, k[:], v)}
		return nil
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageSpanStore) RangeFromBlockNum(ctx context.Context, blockNum uint64) ([]*heimdall.Span, error) {
	type response struct {
		result []*heimdall.Span
		err    error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		makeEntity := func() *heimdall.Span { return &heimdall.Span{} }
		r, err := blockRangeEntitiesFromBlockNum(tx, kv.BorSpans, makeEntity, blockNum)
		responseStream <- response{result: r, err: err}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return r.result, r.err
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

func blockRangeEntitiesFromBlockNum[T blockRangeComparator](tx kv.Tx, table string, makeEntity func() T, blockNum uint64) ([]T, error) {
	cur, err := tx.Cursor(table)
	if err != nil {
		return nil, err
	}

	defer cur.Close()
	var k, v []byte
	var entities []T
	for k, v, err = cur.Last(); err == nil && k != nil; k, v, err = cur.Prev() {
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
	blockReader      services.FullBlockReader
	txActionStream   chan<- polygonSyncStageTxAction
	logger           log.Logger
	heimdallClient   heimdall.HeimdallClient
	stateReceiverABI abi.ABI
	chainConfig      *chain.Config
	// internal
	appendLogPrefix          func(string) string
	stageState               *StageState
	unwinder                 Unwinder
	cachedForkChoice         *types.Header
	lastStateSyncEventIdInit bool
	lastStateSyncEventId     uint64
}

func (e *polygonSyncStageExecutionEngine) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, e.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		responseStream <- response{err: e.insertBlocks(blocks, tx)}
		return nil
	})
	if err != nil {
		return err
	}

	return r.err
}

func (e *polygonSyncStageExecutionEngine) insertBlocks(blocks []*types.Block, tx kv.RwTx) error {
	for _, block := range blocks {
		height := block.NumberU64()
		header := block.Header()
		body := block.Body()

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height-1, e.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height-1, e.logger)

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
	}

	return nil
}

func (e *polygonSyncStageExecutionEngine) UpdateForkChoice(ctx context.Context, tip *types.Header, _ *types.Header) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, e.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		err := e.updateForkChoice(ctx, tx, tip)
		responseStream <- response{err: err}
		if err == nil {
			return updateForkChoiceSuccessErr
		}
		return nil
	})
	if err != nil {
		return err
	}

	return r.err
}

func (e *polygonSyncStageExecutionEngine) updateForkChoice(ctx context.Context, tx kv.RwTx, tip *types.Header) error {
	tipBlockNum := tip.Number.Uint64()
	tipHash := tip.Hash()

	e.logger.Info(e.appendLogPrefix("update fork choice"), "block", tipBlockNum, "hash", tipHash)

	logPrefix := e.stageState.LogPrefix()
	logTicker := time.NewTicker(logInterval)
	defer logTicker.Stop()

	newNodes, badNodes, err := fixCanonicalChain(logPrefix, logTicker, tipBlockNum, tipHash, tx, e.blockReader, e.logger)
	if err != nil {
		return err
	}

	if len(badNodes) > 0 {
		badNode := badNodes[len(badNodes)-1]
		e.cachedForkChoice = tip
		return e.unwinder.UnwindTo(badNode.number, ForkReset(badNode.hash), tx)
	}

	if len(newNodes) == 0 {
		return nil
	}

	// TODO remove below for loop once the bridge is integrated in the stage
	borConfig := e.chainConfig.Bor.(*borcfg.BorConfig)
	for i := len(newNodes) - 1; i >= 0; {
		blockNum := newNodes[i].number
		blockHash := newNodes[i].hash
		if blockNum == 0 {
			break
		}

		sprintLen := borConfig.CalculateSprintLength(blockNum)
		blockPosInSprint := blockNum % sprintLen
		if blockPosInSprint > 0 {
			i -= int(blockPosInSprint)
			continue
		}

		header, err := e.blockReader.Header(ctx, tx, blockHash, blockNum)
		if err != nil {
			return err
		}

		err = e.downloadStateSyncEvents(ctx, tx, header, logTicker)
		if err != nil {
			return err
		}

		i -= int(sprintLen)
	}

	if err := rawdb.AppendCanonicalTxNums(tx, newNodes[len(newNodes)-1].number); err != nil {
		return err
	}

	if err := rawdb.WriteHeadHeaderHash(tx, tipHash); err != nil {
		return err
	}

	if err := e.stageState.Update(tx, tipBlockNum); err != nil {
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

func (e *polygonSyncStageExecutionEngine) CurrentHeader(ctx context.Context) (*types.Header, error) {
	type response struct {
		result *types.Header
		err    error
	}

	r, err := awaitTxAction(ctx, e.txActionStream, func(tx kv.RwTx, responseStream chan<- response) error {
		r, err := e.currentHeader(ctx, tx)
		responseStream <- response{result: r, err: err}
		return nil
	})
	if err != nil {
		return nil, err
	}

	return r.result, r.err
}

func (e *polygonSyncStageExecutionEngine) currentHeader(ctx context.Context, tx kv.Tx) (*types.Header, error) {
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

func (e *polygonSyncStageExecutionEngine) downloadStateSyncEvents(
	ctx context.Context,
	tx kv.RwTx,
	header *types.Header,
	logTicker *time.Ticker,
) error {
	e.logger.Trace(e.appendLogPrefix("download state sync event"), "block", header.Number.Uint64())

	var err error
	if !e.lastStateSyncEventIdInit {
		e.lastStateSyncEventId, _, err = e.blockReader.LastEventId(ctx, tx)
	}
	if err != nil {
		return err
	}

	e.lastStateSyncEventIdInit = true
	newStateSyncEventId, records, duration, err := fetchRequiredHeimdallStateSyncEventsIfNeeded(
		ctx,
		header,
		tx,
		e.chainConfig.Bor.(*borcfg.BorConfig),
		e.blockReader,
		e.heimdallClient,
		e.chainConfig.ChainID.String(),
		e.stateReceiverABI,
		e.stageState.LogPrefix(),
		e.logger,
		e.lastStateSyncEventId,
	)
	if err != nil {
		return err
	}

	if e.lastStateSyncEventId == newStateSyncEventId {
		return nil
	}

	select {
	case <-logTicker.C:
		e.logger.Info(
			e.appendLogPrefix("downloading state sync events progress"),
			"blockNum", header.Number,
			"records", records,
			"duration", duration,
		)
	default:
		// carry on
	}

	e.lastStateSyncEventId = newStateSyncEventId
	return nil
}

func awaitTxAction[T any](
	ctx context.Context,
	txActionStream chan<- polygonSyncStageTxAction,
	cb func(tx kv.RwTx, responseStream chan<- T) error,
) (T, error) {
	responseStream := make(chan T)
	txAction := polygonSyncStageTxAction{
		apply: func(tx kv.RwTx) error {
			return cb(tx, responseStream)
		},
	}

	select {
	case <-ctx.Done():
		return generics.Zero[T](), ctx.Err()
	case txActionStream <- txAction:
		// no-op
	}

	select {
	case <-ctx.Done():
		return generics.Zero[T](), ctx.Err()
	case resp := <-responseStream:
		return resp, nil
	}
}

func newAppendLogPrefix(logPrefix string) func(msg string) string {
	return func(msg string) string {
		return fmt.Sprintf("[%s] %s", logPrefix, msg)
	}
}
