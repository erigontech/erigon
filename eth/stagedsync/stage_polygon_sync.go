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
	"errors"
	"fmt"
	"math/big"
	"time"

	"golang.org/x/sync/errgroup"

	"github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/generics"
	"github.com/erigontech/erigon-lib/common/metrics"
	"github.com/erigontech/erigon-lib/downloader/snaptype"
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/blockio"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	"github.com/erigontech/erigon/polygon/bor/bordb"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/p2p"
	polygonsync "github.com/erigontech/erigon/polygon/sync"
	"github.com/erigontech/erigon/rlp"
	"github.com/erigontech/erigon/turbo/services"
)

var errBreakPolygonSyncStage = errors.New("break polygon sync stage")

func NewPolygonSyncStageCfg(
	logger log.Logger,
	chainConfig *chain.Config,
	db kv.RwDB,
	heimdallClient heimdall.HeimdallClient,
	heimdallStore heimdall.Store,
	bridgeStore bridge.Store,
	sentry sentryproto.SentryClient,
	maxPeers int,
	statusDataProvider *sentry.StatusDataProvider,
	blockReader services.FullBlockReader,
	stopNode func() error,
	blockLimit uint,
	userUnwindTypeOverrides []string,
) PolygonSyncStageCfg {
	// using a buffered channel to preserve order of tx actions,
	// do not expect to ever have more than 50 goroutines blocking on this channel
	// realistically 6: 4 scrapper goroutines in heimdall.Service, 1 in bridge.Service, 1 in sync.Sync,
	// but does not hurt to leave some extra buffer
	txActionStream := make(chan polygonSyncStageTxAction, 50)
	executionEngine := &polygonSyncStageExecutionEngine{
		blockReader:    blockReader,
		txActionStream: txActionStream,
		logger:         logger,
	}
	stageHeimdallStore := &polygonSyncStageHeimdallStore{
		checkpoints: &polygonSyncStageCheckpointStore{
			checkpointStore: heimdallStore.Checkpoints(),
			txActionStream:  txActionStream,
		},
		milestones: &polygonSyncStageMilestoneStore{
			milestoneStore: heimdallStore.Milestones(),
			txActionStream: txActionStream,
		},
		spans: &polygonSyncStageSpanStore{
			spanStore:      heimdallStore.Spans(),
			txActionStream: txActionStream,
		},
		spanBlockProducerSelections: &polygonSyncStageSbpsStore{
			spanStore:      heimdallStore.SpanBlockProducerSelections(),
			txActionStream: txActionStream,
		},
	}
	stageBridgeStore := &polygonSyncStageBridgeStore{
		eventStore:     bridgeStore,
		txActionStream: txActionStream,
	}
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	heimdallService := heimdall.NewService(borConfig, heimdallClient, stageHeimdallStore, logger)
	bridgeService := bridge.NewBridge(bridge.Config{
		Store:        stageBridgeStore,
		Logger:       logger,
		BorConfig:    borConfig,
		EventFetcher: heimdallClient})
	p2pService := p2p.NewService(maxPeers, logger, sentry, statusDataProvider.GetStatusData)
	checkpointVerifier := polygonsync.VerifyCheckpointHeaders
	milestoneVerifier := polygonsync.VerifyMilestoneHeaders
	blocksVerifier := polygonsync.VerifyBlocks
	syncStore := polygonsync.NewStore(logger, executionEngine, bridgeService)
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
	events := polygonsync.NewTipEvents(logger, p2pService, heimdallService)
	sync := polygonsync.NewSync(
		syncStore,
		executionEngine,
		milestoneVerifier,
		blocksVerifier,
		p2pService,
		blockDownloader,
		polygonsync.NewCanonicalChainBuilderFactory(chainConfig, borConfig, heimdallService),
		heimdallService,
		bridgeService,
		events.Events(),
		logger,
	)
	syncService := &polygonSyncStageService{
		logger:          logger,
		sync:            sync,
		syncStore:       syncStore,
		events:          events,
		p2p:             p2pService,
		executionEngine: executionEngine,
		heimdall:        heimdallService,
		bridge:          bridgeService,
		txActionStream:  txActionStream,
		stopNode:        stopNode,
	}

	unwindCfg := bordb.HeimdallUnwindCfg{
		// we keep finalized data, no point in unwinding it
		KeepEvents:                      true,
		KeepSpans:                       true,
		KeepSpanBlockProducerSelections: true,
		KeepCheckpoints:                 true,
		KeepMilestones:                  true,
		// below are handled via the Bridge.Unwind logic in Astrid
		KeepEventNums:            true,
		KeepEventProcessedBlocks: true,
		Astrid:                   true,
	}
	if len(userUnwindTypeOverrides) > 0 {
		unwindCfg.ApplyUserUnwindTypeOverrides(userUnwindTypeOverrides)
	}

	return PolygonSyncStageCfg{
		db:          db,
		service:     syncService,
		blockReader: blockReader,
		blockWriter: blockio.NewBlockWriter(),
		unwindCfg:   unwindCfg,
	}
}

type PolygonSyncStageCfg struct {
	db          kv.RwDB
	service     *polygonSyncStageService
	blockReader services.FullBlockReader
	blockWriter *blockio.BlockWriter
	unwindCfg   bordb.HeimdallUnwindCfg
}

func ForwardPolygonSyncStage(
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

	if !useExternalTx {
		return tx.Commit()
	}

	return nil
}

func UnwindPolygonSyncStage(ctx context.Context, tx kv.RwTx, u *UnwindState, cfg PolygonSyncStageCfg) error {
	u.UnwindPoint = max(u.UnwindPoint, cfg.blockReader.FrozenBlocks()) // protect from unwind behind files

	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}

		defer tx.Rollback()
	}

	// headers
	unwindBlock := u.Reason.Block != nil
	if err := rawdb.TruncateCanonicalHash(tx, u.UnwindPoint+1, unwindBlock); err != nil {
		return err
	}

	canonicalHash, ok, err := cfg.blockReader.CanonicalHash(ctx, tx, u.UnwindPoint)
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("canonical marker not found: %d", u.UnwindPoint)
	}

	if err = rawdb.WriteHeadHeaderHash(tx, canonicalHash); err != nil {
		return err
	}

	// bodies
	if err = cfg.blockWriter.MakeBodiesNonCanonical(tx, u.UnwindPoint+1); err != nil {
		return err
	}

	// heimdall
	if err = bordb.UnwindHeimdall(ctx, cfg.service.heimdallStore, cfg.service.bridgeStore, tx, u.UnwindPoint, cfg.unwindCfg); err != nil {
		return err
	}

	if err = u.Done(tx); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(tx, stages.Headers, u.UnwindPoint); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(tx, stages.Bodies, u.UnwindPoint); err != nil {
		return err
	}

	if err := stages.SaveStageProgress(tx, stages.BlockHashes, u.UnwindPoint); err != nil {
		return err
	}

	if !useExternalTx {
		return tx.Commit()
	}

	return nil
}

type polygonSyncStageTxAction struct {
	apply func(tx kv.RwTx) error
}

type polygonSyncStageService struct {
	logger          log.Logger
	sync            *polygonsync.Sync
	syncStore       polygonsync.Store
	events          *polygonsync.TipEvents
	p2p             p2p.Service
	executionEngine *polygonSyncStageExecutionEngine
	heimdall        heimdall.Service
	heimdallStore   heimdall.Store
	bridge          bridge.Service
	bridgeStore     bridge.Store
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
	s.logger.Info(s.appendLogPrefix("forward"), "progress", stageState.BlockNumber)

	s.runBgComponentsOnce(ctx)

	err := s.executionEngine.processCachedForkChoiceIfNeeded(ctx, tx)
	if errors.Is(err, errBreakPolygonSyncStage) {
		s.logger.Info(s.appendLogPrefix("leaving stage"), "reason", err)
		return nil
	}
	if err != nil {
		return err
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
			if errors.Is(err, errBreakPolygonSyncStage) {
				s.logger.Info(s.appendLogPrefix("leaving stage"), "reason", err)
				return nil
			}
			if errors.Is(err, context.Canceled) {
				// we return a different err and not context.Canceled because that will cancel the stage loop
				// instead we want the stage loop to return to this stage and re-processes
				return errors.New("txAction cancelled by requester")
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
			return s.bridge.Run(ctx)
		})

		eg.Go(func() error {
			return s.p2p.Run(ctx)
		})

		eg.Go(func() error {
			return s.syncStore.Run(ctx)
		})

		eg.Go(func() error {
			return s.sync.Run(ctx)
		})

		if err := eg.Wait(); err != nil {
			s.bgComponentsErr <- err
		}
	}()
}

type polygonSyncStageHeimdallStore struct {
	checkpoints                 *polygonSyncStageCheckpointStore
	milestones                  *polygonSyncStageMilestoneStore
	spans                       *polygonSyncStageSpanStore
	spanBlockProducerSelections *polygonSyncStageSbpsStore
}

func (s polygonSyncStageHeimdallStore) SpanBlockProducerSelections() heimdall.EntityStore[*heimdall.SpanBlockProducerSelection] {
	return s.spanBlockProducerSelections
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
	checkpointStore heimdall.EntityStore[*heimdall.Checkpoint]
	txActionStream  chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageCheckpointStore) SnapType() snaptype.Type {
	return s.checkpointStore.SnapType()
}

func (s polygonSyncStageCheckpointStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	type response struct {
		id  uint64
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, ok, err := s.checkpointStore.(txStore[*heimdall.Checkpoint]).WithTx(tx).LastEntityId(ctx)
		return respond(response{id: id, ok: ok, err: err})
	})
	if err != nil {
		return 0, false, err
	}

	return r.id, r.ok, r.err
}

func (s polygonSyncStageCheckpointStore) LastFrozenEntityId() uint64 {
	return s.checkpointStore.LastFrozenEntityId()
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
		v   *heimdall.Checkpoint
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		v, _, err := s.checkpointStore.(txStore[*heimdall.Checkpoint]).WithTx(tx).Entity(ctx, id)
		return respond(response{v: v, err: err})
	})
	if err != nil {
		return nil, false, err
	}
	if r.err != nil {
		if errors.Is(r.err, heimdall.ErrCheckpointNotFound) {
			return nil, false, nil
		}

		return nil, false, r.err
	}

	return r.v, true, err
}

type txStore[T heimdall.Entity] interface {
	WithTx(kv.Tx) heimdall.EntityStore[T]
}

func (s polygonSyncStageCheckpointStore) PutEntity(ctx context.Context, id uint64, entity *heimdall.Checkpoint) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		err := s.checkpointStore.(txStore[*heimdall.Checkpoint]).WithTx(tx).PutEntity(ctx, id, entity)
		return respond(response{err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		r, err := s.checkpointStore.(txStore[*heimdall.Checkpoint]).WithTx(tx).RangeFromBlockNum(ctx, blockNum)
		return respond(response{result: r, err: err})
	})
	if err != nil {
		return nil, err
	}

	return r.result, r.err
}

func (s *polygonSyncStageCheckpointStore) EntityIdFromBlockNum(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	panic("polygonSyncStageCheckpointStore.EntityIdFromBlockNum not supported")
}

func (s *polygonSyncStageCheckpointStore) DeleteToBlockNum(ctx context.Context, unwindPoint uint64, limit int) (int, error) {
	panic("polygonSyncStageCheckpointStore.DeleteToBlockNum not supported")
}

func (s *polygonSyncStageCheckpointStore) DeleteFromBlockNum(ctx context.Context, unwindPoint uint64) (int, error) {
	panic("polygonSyncStageCheckpointStore.DeletefromBlockNum not supported")
}

func (s polygonSyncStageCheckpointStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageCheckpointStore) Close() {
	// no-op
}

type polygonSyncStageMilestoneStore struct {
	milestoneStore heimdall.EntityStore[*heimdall.Milestone]
	txActionStream chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageMilestoneStore) SnapType() snaptype.Type {
	return s.milestoneStore.SnapType()
}

func (s polygonSyncStageMilestoneStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	type response struct {
		id  uint64
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, ok, err := s.milestoneStore.(txStore[*heimdall.Milestone]).WithTx(tx).LastEntityId(ctx)
		return respond(response{id: id, ok: ok, err: err})
	})
	if err != nil {
		return 0, false, err
	}

	return r.id, r.ok, r.err
}

func (s polygonSyncStageMilestoneStore) LastFrozenEntityId() uint64 {
	return s.milestoneStore.LastFrozenEntityId()
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
		v   *heimdall.Milestone
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		v, _, err := s.milestoneStore.(txStore[*heimdall.Milestone]).WithTx(tx).Entity(ctx, id)
		return respond(response{v: v, err: err})
	})
	if err != nil {
		return nil, false, err
	}
	if r.err != nil {
		if errors.Is(r.err, heimdall.ErrMilestoneNotFound) {
			return nil, false, nil
		}

		return nil, false, r.err
	}

	return r.v, true, err
}

func (s polygonSyncStageMilestoneStore) PutEntity(ctx context.Context, id uint64, entity *heimdall.Milestone) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		err := s.milestoneStore.(txStore[*heimdall.Milestone]).WithTx(tx).PutEntity(ctx, id, entity)
		return respond(response{err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		r, err := s.milestoneStore.(txStore[*heimdall.Milestone]).WithTx(tx).RangeFromBlockNum(ctx, blockNum)
		return respond(response{result: r, err: err})
	})
	if err != nil {
		return nil, err
	}

	return r.result, r.err
}

func (s *polygonSyncStageMilestoneStore) EntityIdFromBlockNum(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	panic("polygonSyncStageMilestoneStore.EntityIdFromBlockNum not supported")
}

func (s *polygonSyncStageMilestoneStore) DeleteToBlockNum(ctx context.Context, unwindPoint uint64, limit int) (int, error) {
	panic("polygonSyncStageMilestoneStore.DeleteToBlockNum not supported")
}

func (s *polygonSyncStageMilestoneStore) DeleteFromBlockNum(ctx context.Context, unwindPoint uint64) (int, error) {
	panic("polygonSyncStageMilestoneStore.DeletefromBlockNum not supported")
}

func (s polygonSyncStageMilestoneStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageMilestoneStore) Close() {
	// no-op
}

type polygonSyncStageSpanStore struct {
	spanStore      heimdall.EntityStore[*heimdall.Span]
	txActionStream chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageSpanStore) SnapType() snaptype.Type {
	return s.spanStore.SnapType()
}

func (s polygonSyncStageSpanStore) LastEntityId(ctx context.Context) (id uint64, ok bool, err error) {
	type response struct {
		id  uint64
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, ok, err := s.spanStore.(txStore[*heimdall.Span]).WithTx(tx).LastEntityId(ctx)
		return respond(response{id: id, ok: ok, err: err})
	})
	if err != nil {
		return 0, false, err
	}

	return r.id, r.ok, r.err
}

func (s polygonSyncStageSpanStore) LastFrozenEntityId() (id uint64) {
	return s.spanStore.LastFrozenEntityId()
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
		v   *heimdall.Span
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		v, _, err := s.spanStore.(txStore[*heimdall.Span]).WithTx(tx).Entity(ctx, id)
		return respond(response{v: v, err: err})
	})
	if err != nil {
		return nil, false, err
	}
	if r.err != nil {
		if errors.Is(r.err, heimdall.ErrSpanNotFound) {
			return nil, false, nil
		}

		return nil, false, r.err
	}

	return r.v, true, err
}

func (s polygonSyncStageSpanStore) PutEntity(ctx context.Context, id uint64, entity *heimdall.Span) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		err := s.spanStore.(txStore[*heimdall.Span]).WithTx(tx).PutEntity(ctx, id, entity)
		return respond(response{err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		r, err := s.spanStore.(txStore[*heimdall.Span]).WithTx(tx).RangeFromBlockNum(ctx, blockNum)
		return respond(response{result: r, err: err})
	})
	if err != nil {
		return nil, err
	}

	return r.result, r.err
}

func (s *polygonSyncStageSpanStore) EntityIdFromBlockNum(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	panic("polygonSyncStageSpanStore.EntityIdFromBlockNum not supported")
}

func (s *polygonSyncStageSpanStore) DeleteToBlockNum(ctx context.Context, unwindPoint uint64, limit int) (int, error) {
	panic("polygonSyncStageSpanStore.DeleteToBlockNum not supported")
}

func (s *polygonSyncStageSpanStore) DeleteFromBlockNum(ctx context.Context, unwindPoint uint64) (int, error) {
	panic("polygonSyncStageSpanStore.DeletefromBlockNum not supported")
}

func (s polygonSyncStageSpanStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageSpanStore) Close() {
	// no-op
}

// polygonSyncStageSbpsStore is the store for heimdall.SpanBlockProducerSelection
type polygonSyncStageSbpsStore struct {
	spanStore      heimdall.EntityStore[*heimdall.SpanBlockProducerSelection]
	txActionStream chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageSbpsStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	type response struct {
		id  uint64
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, ok, err := s.spanStore.(txStore[*heimdall.SpanBlockProducerSelection]).WithTx(tx).LastEntityId(ctx)
		return respond(response{id: id, ok: ok, err: err})
	})
	if err != nil {
		return 0, false, err
	}

	return r.id, r.ok, r.err
}

func (s polygonSyncStageSbpsStore) SnapType() snaptype.Type {
	return nil
}

func (s polygonSyncStageSbpsStore) LastFrozenEntityId() uint64 {
	return s.spanStore.LastFrozenEntityId()
}

func (s polygonSyncStageSbpsStore) LastEntity(ctx context.Context) (*heimdall.SpanBlockProducerSelection, bool, error) {
	id, ok, err := s.LastEntityId(ctx)
	if err != nil {
		return nil, false, err
	}
	if !ok {
		return nil, false, nil
	}

	return s.Entity(ctx, id)
}

func (s polygonSyncStageSbpsStore) Entity(ctx context.Context, id uint64) (*heimdall.SpanBlockProducerSelection, bool, error) {
	type response struct {
		v   *heimdall.SpanBlockProducerSelection
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		v, _, err := s.spanStore.(txStore[*heimdall.SpanBlockProducerSelection]).WithTx(tx).Entity(ctx, id)
		return respond(response{v: v, err: err})
	})
	if err != nil {
		return nil, false, err
	}
	if r.err != nil {
		if errors.Is(r.err, heimdall.ErrSpanNotFound) {
			return nil, false, nil
		}

		return nil, false, r.err
	}

	return r.v, true, err
}

func (s polygonSyncStageSbpsStore) PutEntity(ctx context.Context, id uint64, entity *heimdall.SpanBlockProducerSelection) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		err := s.spanStore.(txStore[*heimdall.SpanBlockProducerSelection]).WithTx(tx).PutEntity(ctx, id, entity)
		return respond(response{err: err})
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageSbpsStore) RangeFromBlockNum(ctx context.Context, blockNum uint64) ([]*heimdall.SpanBlockProducerSelection, error) {
	type response struct {
		result []*heimdall.SpanBlockProducerSelection
		err    error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		r, err := s.spanStore.(txStore[*heimdall.SpanBlockProducerSelection]).WithTx(tx).RangeFromBlockNum(ctx, blockNum)
		return respond(response{result: r, err: err})
	})
	if err != nil {
		return nil, err
	}

	return r.result, r.err
}

func (s *polygonSyncStageSbpsStore) EntityIdFromBlockNum(ctx context.Context, blockNum uint64) (uint64, bool, error) {
	panic("polygonSyncStageSbpsStore.EntityIdFromBlockNum not supported")
}

func (s *polygonSyncStageSbpsStore) DeleteToBlockNum(ctx context.Context, unwindPoint uint64, limit int) (int, error) {
	panic("polygonSyncStageSbpsStore.DeleteToBlockNum not supported")
}

func (s *polygonSyncStageSbpsStore) DeleteFromBlockNum(ctx context.Context, unwindPoint uint64) (int, error) {
	panic("polygonSyncStageSbpsStore.DeletefromBlockNum not supported")
}

func (s polygonSyncStageSbpsStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageSbpsStore) Close() {
	// no-op
}

type polygonSyncStageBridgeStore struct {
	eventStore     bridge.Store
	txActionStream chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageBridgeStore) LastEventId(ctx context.Context) (uint64, error) {
	type response struct {
		id  uint64
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, err := s.eventStore.(interface{ WithTx(kv.Tx) bridge.Store }).WithTx(tx).LastEventId(ctx)
		return respond(response{id: id, err: err})
	})
	if err != nil {
		return 0, err
	}

	return r.id, r.err
}

func (s polygonSyncStageBridgeStore) PutEvents(ctx context.Context, events []*heimdall.EventRecordWithTime) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: s.eventStore.(interface{ WithTx(kv.Tx) bridge.Store }).WithTx(tx).PutEvents(ctx, events)})
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageBridgeStore) LastProcessedEventId(ctx context.Context) (uint64, error) {
	type response struct {
		id  uint64
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, err := s.eventStore.(interface{ WithTx(kv.Tx) bridge.Store }).WithTx(tx).LastProcessedEventId(ctx)
		return respond(response{id: id, err: err})
	})
	if err != nil {
		return 0, err
	}
	if r.err != nil {
		return 0, r.err
	}
	if r.id == 0 {
		return s.eventStore.LastFrozenEventId(), nil
	}

	return r.id, nil
}

func (s polygonSyncStageBridgeStore) LastProcessedBlockInfo(ctx context.Context) (bridge.ProcessedBlockInfo, bool, error) {
	type response struct {
		info bridge.ProcessedBlockInfo
		ok   bool
		err  error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		info, ok, err := s.eventStore.(interface{ WithTx(kv.Tx) bridge.Store }).WithTx(tx).LastProcessedBlockInfo(ctx)
		return respond(response{info: info, ok: ok, err: err})
	})
	if err != nil {
		return bridge.ProcessedBlockInfo{}, false, err
	}

	return r.info, r.ok, r.err
}

func (s polygonSyncStageBridgeStore) PutProcessedBlockInfo(ctx context.Context, info bridge.ProcessedBlockInfo) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: s.eventStore.(interface{ WithTx(kv.Tx) bridge.Store }).
			WithTx(tx).PutProcessedBlockInfo(ctx, info)})
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageBridgeStore) LastFrozenEventId() uint64 {
	return s.eventStore.LastFrozenEventId()
}

func (s polygonSyncStageBridgeStore) LastFrozenEventBlockNum() uint64 {
	return s.eventStore.LastFrozenEventBlockNum()
}

func (s polygonSyncStageBridgeStore) LastEventIdWithinWindow(ctx context.Context, fromId uint64, toTime time.Time) (uint64, error) {
	type response struct {
		id  uint64
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, err := s.eventStore.(interface{ WithTx(kv.Tx) bridge.Store }).WithTx(tx).LastEventIdWithinWindow(ctx, fromId, toTime)
		return respond(response{id: id, err: err})
	})
	if err != nil {
		return 0, err
	}
	if r.err != nil {
		return 0, err
	}

	return r.id, nil
}

func (s polygonSyncStageBridgeStore) PutBlockNumToEventId(ctx context.Context, blockNumToEventId map[uint64]uint64) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: s.eventStore.(interface{ WithTx(kv.Tx) bridge.Store }).
			WithTx(tx).PutBlockNumToEventId(ctx, blockNumToEventId)})
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageBridgeStore) BorStartEventId(ctx context.Context, hash common.Hash, blockHeight uint64) (uint64, error) {
	panic("polygonSyncStageBridgeStore.BorStartEventId not supported")
}

func (s polygonSyncStageBridgeStore) EventsByBlock(ctx context.Context, hash common.Hash, blockNum uint64) ([]rlp.RawValue, error) {
	panic("polygonSyncStageBridgeStore.EventsByBlock not supported")
}

// Unwind delete unwindable bridge data.
// The blockNum parameter is exclusive, i.e. only data in the range (blockNum, last] is deleted.
func (s polygonSyncStageBridgeStore) Unwind(ctx context.Context, blockNum uint64) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: s.eventStore.(interface{ WithTx(kv.Tx) bridge.Store }).
			WithTx(tx).Unwind(ctx, blockNum)})
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageBridgeStore) PruneEvents(ctx context.Context, blocksTo uint64, blocksDeleteLimit int) (deleted int, err error) {
	type response struct {
		deleted int
		err     error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		deleted, err := s.eventStore.(interface{ WithTx(kv.Tx) bridge.Store }).
			WithTx(tx).PruneEvents(ctx, blocksTo, blocksDeleteLimit)
		return respond(response{deleted: deleted, err: err})
	})

	if err != nil {
		return 0, err
	}

	return r.deleted, r.err
}

func (s polygonSyncStageBridgeStore) Events(context.Context, uint64, uint64) ([][]byte, error) {
	// used for accessing events in execution
	// astrid stage integration intends to use the bridge only for scrapping
	// not for reading which remains the same in execution (via BlockReader)
	// astrid standalone mode introduces its own reader
	panic("polygonSyncStageBridgeStore.Events not supported")
}

func (s polygonSyncStageBridgeStore) BlockEventIdsRange(context.Context, uint64) (uint64, uint64, error) {
	// used for accessing events in execution
	// astrid stage integration intends to use the bridge only for scrapping
	// not for reading which remains the same in execution (via BlockReader)
	// astrid standalone mode introduces its own reader
	panic("polygonSyncStageBridgeStore.BlockEventIdsRange not supported")
}

func (s polygonSyncStageBridgeStore) EventTxnToBlockNum(context.Context, common.Hash) (uint64, bool, error) {
	// used in RPCs
	// astrid stage integration intends to use the bridge only for scrapping,
	// not for reading which remains the same in RPCs (via BlockReader)
	// astrid standalone mode introduces its own reader
	panic("polygonSyncStageBridgeStore.EventTxnToBlockNum not supported")
}

func (s polygonSyncStageBridgeStore) EventsByIdFromSnapshot(from uint64, to time.Time, limit int) ([]*heimdall.EventRecordWithTime, bool, error) {
	panic("polygonSyncStageBridgeStore.EventsByIdFromSnapshot not supported")
}

func (s polygonSyncStageBridgeStore) PutEventTxnToBlockNum(context.Context, map[common.Hash]uint64) error {
	// this is a no-op for the astrid stage integration mode because the BorTxLookup table is populated
	// in stage_txlookup.go as part of borTxnLookupTransform
	return nil
}

func (s polygonSyncStageBridgeStore) Prepare(context.Context) error {
	// no-op
	return nil
}

func (s polygonSyncStageBridgeStore) Close() {
	// no-op
}

type polygonSyncStageForkChoiceState int

const (
	forkChoiceCached polygonSyncStageForkChoiceState = iota
	forkChoiceAlreadyCanonicalConnected
	forkChoiceConnected
	forkChoiceExecuted
)

type polygonSyncStageForkChoiceResult struct {
	latestValidHash common.Hash
	validationErr   error
}

type polygonSyncStageForkChoice struct {
	tip       *types.Header
	finalized *types.Header
	state     polygonSyncStageForkChoiceState
	resultCh  chan polygonSyncStageForkChoiceResult
	newNodes  []chainNode // newNodes contains tip first and its new ancestors after it (oldest is last)
}

func (fc polygonSyncStageForkChoice) oldestNewAncestorBlockNum() uint64 {
	return fc.newNodes[len(fc.newNodes)-1].number
}

type chainNode struct {
	hash   common.Hash
	number uint64
}

type polygonSyncStageExecutionEngine struct {
	blockReader    services.FullBlockReader
	txActionStream chan<- polygonSyncStageTxAction
	logger         log.Logger
	// internal
	appendLogPrefix  func(string) string
	stageState       *StageState
	unwinder         Unwinder
	cachedForkChoice *polygonSyncStageForkChoice
}

func (e *polygonSyncStageExecutionEngine) Prepare(ctx context.Context) error {
	return <-e.blockReader.Snapshots().Ready(ctx)
}

func (e *polygonSyncStageExecutionEngine) GetTd(ctx context.Context, blockNum uint64, blockHash common.Hash) (*big.Int, error) {
	type response struct {
		td  *big.Int
		err error
	}

	r, err := awaitTxAction(ctx, e.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		td, err := rawdb.ReadTd(tx, blockHash, blockNum)
		return respond(response{td: td, err: err})
	})
	if err != nil {
		return nil, err
	}

	return r.td, r.err
}

func (e *polygonSyncStageExecutionEngine) GetHeader(ctx context.Context, blockNum uint64) (*types.Header, error) {
	type response struct {
		header *types.Header
		err    error
	}

	r, err := awaitTxAction(ctx, e.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		header, err := e.blockReader.HeaderByNumber(ctx, tx, blockNum)
		return respond(response{header: header, err: err})
	})
	if err != nil {
		return nil, err
	}

	return r.header, r.err
}

func (e *polygonSyncStageExecutionEngine) InsertBlocks(ctx context.Context, blocks []*types.Block) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, e.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: e.insertBlocks(tx, blocks)})
	})
	if err != nil {
		return err
	}

	return r.err
}

func (e *polygonSyncStageExecutionEngine) insertBlocks(tx kv.RwTx, blocks []*types.Block) error {
	for _, block := range blocks {
		height := block.NumberU64()
		header := block.Header()
		body := block.Body()

		e.logger.Trace(e.appendLogPrefix("inserting block"), "blockNum", height, "blockHash", header.Hash())

		metrics.UpdateBlockConsumerHeaderDownloadDelay(header.Time, height, e.logger)
		metrics.UpdateBlockConsumerBodyDownloadDelay(header.Time, height, e.logger)

		var parentTd *big.Int
		var err error
		if height > 0 {
			// Parent's total difficulty
			parentHeight := height - 1
			parentTd, err = rawdb.ReadTd(tx, header.ParentHash, parentHeight)
			if err != nil || parentTd == nil {
				return fmt.Errorf(
					"parent's total difficulty not found with hash %x and height %d: %v",
					header.ParentHash,
					parentHeight,
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

func (e *polygonSyncStageExecutionEngine) UpdateForkChoice(ctx context.Context, tip *types.Header, finalized *types.Header) (common.Hash, error) {
	resultCh := make(chan polygonSyncStageForkChoiceResult)

	_, err := awaitTxAction(ctx, e.txActionStream, func(tx kv.RwTx, respond func(_ any) error) error {
		// we set cached fork choice inside tx action to ensure only 1 goroutine ever updates this attribute
		e.cachedForkChoice = &polygonSyncStageForkChoice{
			tip:       tip,
			finalized: finalized,
			resultCh:  resultCh,
		}

		// we want to break the stage before processing the fork choice so that all data written so far gets commited
		if err := respond(nil); err != nil {
			return err
		}

		return fmt.Errorf("%w: update fork choice received", errBreakPolygonSyncStage)
	})
	if err != nil {
		return common.Hash{}, err
	}

	select {
	case <-ctx.Done():
		return common.Hash{}, ctx.Err()
	case result := <-resultCh:
		err := result.validationErr
		if err != nil {
			err = fmt.Errorf("%w: %w", polygonsync.ErrForkChoiceUpdateBadBlock, err)
		}
		return result.latestValidHash, err
	}
}

func (e *polygonSyncStageExecutionEngine) connectForkChoice(ctx context.Context, tx kv.RwTx) (err error) {
	tip := e.cachedForkChoice.tip
	e.logger.Debug(e.appendLogPrefix("connecting fork choice"), "blockNum", tip.Number, "blockHash", tip.Hash())

	newNodes, badNodes, err := e.connectTip(ctx, tx, tip)
	if err != nil {
		return err
	}
	if len(newNodes) == 0 {
		// empty new nodes means that the tip block hash is already part of the canonical chain
		e.cachedForkChoice.state = forkChoiceAlreadyCanonicalConnected
		return nil
	}

	e.cachedForkChoice.state = forkChoiceConnected
	e.cachedForkChoice.newNodes = newNodes

	if len(badNodes) > 0 {
		badNode := badNodes[len(badNodes)-1]
		unwindNumber := badNode.number - 1
		badHash := badNode.hash

		e.logger.Info(
			e.appendLogPrefix("new fork - unwinding"),
			"unwindNumber", unwindNumber,
			"badHash", badHash,
			"badNodes", len(badNodes),
			"cachedTipNumber", tip.Number,
			"cachedTipHash", tip.Hash(),
			"cachedNewNodes", len(newNodes),
		)

		if err = e.unwinder.UnwindTo(unwindNumber, ForkReset(badHash), tx); err != nil {
			return err
		}

		// we want to break the stage so that we let the unwind take place first
		return fmt.Errorf("%w: unwinding due to fork choice", errBreakPolygonSyncStage)
	}

	return nil
}

func (e *polygonSyncStageExecutionEngine) connectTip(
	ctx context.Context,
	tx kv.RwTx,
	tip *types.Header,
) (newNodes []chainNode, badNodes []chainNode, err error) {
	blockNum := tip.Number.Uint64()
	if blockNum == 0 {
		return nil, nil, nil
	}

	blockHash := tip.Hash()

	var emptyHash common.Hash
	var ch common.Hash
	for {
		ch, _, err = e.blockReader.CanonicalHash(ctx, tx, blockNum)
		if err != nil {
			return nil, nil, fmt.Errorf("connectTip reading canonical hash for %d: %w", blockNum, err)
		}
		if ch == blockHash {
			break
		}

		h, err := e.blockReader.Header(ctx, tx, blockHash, blockNum)
		if err != nil {
			return nil, nil, err
		}
		if h == nil {
			return nil, nil, fmt.Errorf("connectTip header is nil. blockNum %d, blockHash %x", blockNum, blockHash)
		}

		newNodes = append(newNodes, chainNode{
			hash:   blockHash,
			number: blockNum,
		})

		if ch != emptyHash {
			badNodes = append(badNodes, chainNode{
				hash:   ch,
				number: blockNum,
			})
		}

		blockHash = h.ParentHash
		blockNum--
	}

	return newNodes, badNodes, nil
}

func (e *polygonSyncStageExecutionEngine) executeForkChoice(tx kv.RwTx) error {
	tip := e.cachedForkChoice.tip
	e.logger.Debug(
		e.appendLogPrefix("executing fork choice"),
		"blockNum", tip.Number,
		"blockHash", tip.Hash(),
		"newNodes", len(e.cachedForkChoice.newNodes),
	)

	for i := len(e.cachedForkChoice.newNodes) - 1; i >= 0; i-- {
		newNode := e.cachedForkChoice.newNodes[i]
		if err := rawdb.WriteCanonicalHash(tx, newNode.hash, newNode.number); err != nil {
			return err
		}
	}

	if err := rawdb.AppendCanonicalTxNums(tx, e.cachedForkChoice.oldestNewAncestorBlockNum()); err != nil {
		return err
	}

	if err := rawdb.WriteHeadHeaderHash(tx, tip.Hash()); err != nil {
		return err
	}

	tipBlockNum := tip.Number.Uint64()

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

	e.cachedForkChoice.state = forkChoiceExecuted
	return fmt.Errorf("%w: executing fork choice", errBreakPolygonSyncStage)
}

func (e *polygonSyncStageExecutionEngine) answerAlreadyCanonicalForkChoice(ctx context.Context, tx kv.RwTx) error {
	tip := e.cachedForkChoice.tip
	e.logger.Debug(
		e.appendLogPrefix("received fork choice that is already canonical"),
		"blockNum", tip.Number,
		"blockHash", tip.Hash(),
	)

	// no reason to unwind/re-execute, only need to update head header hash
	if err := rawdb.WriteHeadHeaderHash(tx, tip.Hash()); err != nil {
		return err
	}

	lastValidHash := tip.Hash()
	return e.answerForkChoice(ctx, tx, lastValidHash)
}

func (e *polygonSyncStageExecutionEngine) answerExecutedForkChoice(ctx context.Context, tx kv.RwTx) error {
	// if head block was set then fork choice was successfully executed
	lastValidHash := rawdb.ReadHeadBlockHash(tx)
	return e.answerForkChoice(ctx, tx, lastValidHash)
}

func (e *polygonSyncStageExecutionEngine) answerForkChoice(
	ctx context.Context,
	tx kv.RwTx,
	latestValidHash common.Hash,
) error {
	var result polygonSyncStageForkChoiceResult
	tip := e.cachedForkChoice.tip
	finalized := e.cachedForkChoice.finalized
	canonicalFinalizedHash, err := rawdb.ReadCanonicalHash(tx, finalized.Number.Uint64())
	if err != nil {
		return err
	}

	if latestValidHash != tip.Hash() {
		result = polygonSyncStageForkChoiceResult{
			latestValidHash: latestValidHash,
			validationErr:   errors.New("headHash and blockHash mismatch"),
		}
	} else if canonicalFinalizedHash != finalized.Hash() {
		result = polygonSyncStageForkChoiceResult{
			latestValidHash: common.Hash{},
			validationErr:   errors.New("invalid fork choice"),
		}
	} else {
		result = polygonSyncStageForkChoiceResult{
			latestValidHash: latestValidHash,
		}
	}

	e.logger.Debug(
		e.appendLogPrefix("answering fork choice"),
		"latestValidHash", result.latestValidHash,
		"validationErr", result.validationErr,
	)

	if result.validationErr != nil {
		e.logger.Warn(e.appendLogPrefix("fork choice has failed, check execution stage logs"))
	}

	select {
	case e.cachedForkChoice.resultCh <- result:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (e *polygonSyncStageExecutionEngine) processCachedForkChoiceIfNeeded(ctx context.Context, tx kv.RwTx) error {
	if e.cachedForkChoice == nil {
		return nil
	}

	if e.cachedForkChoice.state == forkChoiceCached {
		if err := e.connectForkChoice(ctx, tx); err != nil {
			return err
		}
	}

	if e.cachedForkChoice.state == forkChoiceAlreadyCanonicalConnected {
		if err := e.answerAlreadyCanonicalForkChoice(ctx, tx); err != nil {
			return err
		}
	}

	if e.cachedForkChoice.state == forkChoiceConnected {
		if err := e.executeForkChoice(tx); err != nil {
			return err
		}
	}

	if e.cachedForkChoice.state == forkChoiceExecuted {
		if err := e.answerExecutedForkChoice(ctx, tx); err != nil {
			return err
		}
	}

	e.cachedForkChoice = nil
	return nil
}

func (e *polygonSyncStageExecutionEngine) CurrentHeader(ctx context.Context) (*types.Header, error) {
	type response struct {
		result *types.Header
		err    error
	}

	r, err := awaitTxAction(ctx, e.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		r, err := e.currentHeader(ctx, tx)
		return respond(response{result: r, err: err})
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

func awaitTxAction[T any](
	ctx context.Context,
	txActionStream chan<- polygonSyncStageTxAction,
	cb func(tx kv.RwTx, respond func(response T) error) error,
) (T, error) {
	responseStream := make(chan T)
	respondFunc := func(response T) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case responseStream <- response:
			return nil
		}
	}
	txAction := polygonSyncStageTxAction{
		apply: func(tx kv.RwTx) error {
			return cb(tx, respondFunc)
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
