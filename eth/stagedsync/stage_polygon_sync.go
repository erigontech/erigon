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
	"github.com/erigontech/erigon-lib/gointerfaces/sentryproto"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/dbutils"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/rawdb"
	"github.com/erigontech/erigon/core/rawdb/blockio"
	"github.com/erigontech/erigon/core/types"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/polygon/bor/borcfg"
	borsnaptype "github.com/erigontech/erigon/polygon/bor/snaptype"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/polygon/p2p"
	polygonsync "github.com/erigontech/erigon/polygon/sync"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
)

var errBreakPolygonSyncStage = errors.New("break polygon sync stage")

func NewPolygonSyncStageCfg(
	logger log.Logger,
	chainConfig *chain.Config,
	db kv.RwDB,
	heimdallClient heimdall.HeimdallClient,
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
		spanBlockProducerSelections: &polygonSyncStageSbpsStore{
			txActionStream: txActionStream,
		},
	}
	bridgeStore := &polygonSyncStageBridgeStore{
		eventReader:    blockReader,
		txActionStream: txActionStream,
	}
	borConfig := chainConfig.Bor.(*borcfg.BorConfig)
	heimdallReader := heimdall.NewReader(borConfig.CalculateSprintNumber, heimdallStore, logger)
	heimdallService := heimdall.NewService(borConfig.CalculateSprintNumber, heimdallClient, heimdallStore, logger, heimdallReader)
	bridgeService := bridge.NewBridge(bridgeStore, logger, borConfig, heimdallClient, nil)
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

	unwindCfg := HeimdallUnwindCfg{
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
	unwindCfg   HeimdallUnwindCfg
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
	if err = UnwindHeimdall(tx, u, cfg.unwindCfg); err != nil {
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

type HeimdallUnwindCfg struct {
	KeepEvents                      bool
	KeepEventNums                   bool
	KeepEventProcessedBlocks        bool
	KeepSpans                       bool
	KeepSpanBlockProducerSelections bool
	KeepCheckpoints                 bool
	KeepMilestones                  bool
	Astrid                          bool
}

func (cfg *HeimdallUnwindCfg) ApplyUserUnwindTypeOverrides(userUnwindTypeOverrides []string) {
	if len(userUnwindTypeOverrides) > 0 {
		return
	}

	// If a user has specified an unwind type override it means we need to unwind all the tables that fall
	// inside that type but NOT unwind the tables for the types that have not been specified in the overrides.
	// Our default config value unwinds everything.
	// If we initialise that and keep track of all the "unseen" unwind type overrides then we can flip our config
	// to not unwind the tables for the "unseen" types.
	const events = "events"
	const spans = "spans"
	const checkpoints = "checkpoints"
	const milestones = "milestones"
	unwindTypes := map[string]struct{}{
		events:      {},
		spans:       {},
		checkpoints: {},
		milestones:  {},
	}

	for _, unwindType := range userUnwindTypeOverrides {
		if _, exists := unwindTypes[unwindType]; !exists {
			panic("unknown unwindType override " + unwindType)
		}

		delete(unwindTypes, unwindType)
	}

	// our config unwinds everything by default
	defaultCfg := HeimdallUnwindCfg{}
	defaultCfg.Astrid = cfg.Astrid
	// flip the config for the unseen type overrides
	for unwindType := range unwindTypes {
		switch unwindType {
		case events:
			defaultCfg.KeepEvents = true
			defaultCfg.KeepEventNums = true
			defaultCfg.KeepEventProcessedBlocks = true
		case spans:
			defaultCfg.KeepSpans = true
			defaultCfg.KeepSpanBlockProducerSelections = true
		case checkpoints:
			defaultCfg.KeepCheckpoints = true
		case milestones:
			defaultCfg.KeepMilestones = true
		default:
			panic(fmt.Sprintf("missing override logic for unwindType %s, please add it", unwindType))
		}
	}

	*cfg = defaultCfg
}

func UnwindHeimdall(tx kv.RwTx, u *UnwindState, unwindCfg HeimdallUnwindCfg) error {
	if !unwindCfg.KeepEvents {
		if err := UnwindEvents(tx, u.UnwindPoint); err != nil {
			return err
		}
	}

	if !unwindCfg.KeepEventNums {
		if err := bridge.UnwindBlockNumToEventID(tx, u.UnwindPoint); err != nil {
			return err
		}
	}

	if !unwindCfg.KeepEventProcessedBlocks && unwindCfg.Astrid {
		if err := bridge.UnwindEventProcessedBlocks(tx, u.UnwindPoint); err != nil {
			return err
		}
	}

	if !unwindCfg.KeepSpans {
		if err := UnwindSpans(tx, u.UnwindPoint); err != nil {
			return err
		}
	}

	if !unwindCfg.KeepSpanBlockProducerSelections && unwindCfg.Astrid {
		if err := UnwindSpanBlockProducerSelections(tx, u.UnwindPoint); err != nil {
			return err
		}
	}

	if borsnaptype.CheckpointsEnabled() && !unwindCfg.KeepCheckpoints {
		if err := UnwindCheckpoints(tx, u.UnwindPoint); err != nil {
			return err
		}
	}

	if borsnaptype.MilestonesEnabled() && !unwindCfg.KeepMilestones {
		if err := UnwindMilestones(tx, u.UnwindPoint); err != nil {
			return err
		}
	}

	return nil
}

func UnwindEvents(tx kv.RwTx, unwindPoint uint64) error {
	eventNumsCursor, err := tx.Cursor(kv.BorEventNums)
	if err != nil {
		return err
	}
	defer eventNumsCursor.Close()

	var blockNumBuf [8]byte
	binary.BigEndian.PutUint64(blockNumBuf[:], unwindPoint+1)

	_, _, err = eventNumsCursor.Seek(blockNumBuf[:])
	if err != nil {
		return err
	}

	// keep last event ID of previous block with assigned events
	_, lastEventIdToKeep, err := eventNumsCursor.Prev()
	if err != nil {
		return err
	}

	var firstEventIdToRemove uint64
	if lastEventIdToKeep == nil {
		// there are no assigned events before the unwind block, remove all items from BorEvents
		firstEventIdToRemove = 0
	} else {
		firstEventIdToRemove = binary.BigEndian.Uint64(lastEventIdToKeep) + 1
	}

	from := make([]byte, 8)
	binary.BigEndian.PutUint64(from, firstEventIdToRemove)
	eventCursor, err := tx.RwCursor(kv.BorEvents)
	if err != nil {
		return err
	}
	defer eventCursor.Close()

	var k []byte
	for k, _, err = eventCursor.Seek(from); err == nil && k != nil; k, _, err = eventCursor.Next() {
		if err = eventCursor.DeleteCurrent(); err != nil {
			return err
		}
	}

	return err
}

func UnwindSpans(tx kv.RwTx, unwindPoint uint64) error {
	cursor, err := tx.RwCursor(kv.BorSpans)
	if err != nil {
		return err
	}

	defer cursor.Close()
	lastSpanToKeep := heimdall.SpanIdAt(unwindPoint)
	var spanIdBytes [8]byte
	binary.BigEndian.PutUint64(spanIdBytes[:], uint64(lastSpanToKeep+1))
	var k []byte
	for k, _, err = cursor.Seek(spanIdBytes[:]); err == nil && k != nil; k, _, err = cursor.Next() {
		if err = cursor.DeleteCurrent(); err != nil {
			return err
		}
	}

	return err
}

func UnwindSpanBlockProducerSelections(tx kv.RwTx, unwindPoint uint64) error {
	producerCursor, err := tx.RwCursor(kv.BorProducerSelections)
	if err != nil {
		return err
	}
	defer producerCursor.Close()

	lastSpanToKeep := heimdall.SpanIdAt(unwindPoint)
	var spanIdBytes [8]byte
	binary.BigEndian.PutUint64(spanIdBytes[:], uint64(lastSpanToKeep+1))
	var k []byte
	for k, _, err = producerCursor.Seek(spanIdBytes[:]); err == nil && k != nil; k, _, err = producerCursor.Next() {
		if err = producerCursor.DeleteCurrent(); err != nil {
			return err
		}
	}

	return err
}

func UnwindCheckpoints(tx kv.RwTx, unwindPoint uint64) error {
	cursor, err := tx.RwCursor(kv.BorCheckpoints)
	if err != nil {
		return err
	}

	defer cursor.Close()
	lastCheckpointToKeep, err := heimdall.CheckpointIdAt(tx, unwindPoint)
	if errors.Is(err, heimdall.ErrCheckpointNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	var checkpointIdBytes [8]byte
	binary.BigEndian.PutUint64(checkpointIdBytes[:], uint64(lastCheckpointToKeep+1))
	var k []byte
	for k, _, err = cursor.Seek(checkpointIdBytes[:]); err == nil && k != nil; k, _, err = cursor.Next() {
		if err = cursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	return err
}

func UnwindMilestones(tx kv.RwTx, unwindPoint uint64) error {
	cursor, err := tx.RwCursor(kv.BorMilestones)
	if err != nil {
		return err
	}

	defer cursor.Close()
	lastMilestoneToKeep, err := heimdall.MilestoneIdAt(tx, unwindPoint)
	if errors.Is(err, heimdall.ErrMilestoneNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	var milestoneIdBytes [8]byte
	binary.BigEndian.PutUint64(milestoneIdBytes[:], uint64(lastMilestoneToKeep+1))
	var k []byte
	for k, _, err = cursor.Seek(milestoneIdBytes[:]); err == nil && k != nil; k, _, err = cursor.Next() {
		if err = cursor.DeleteCurrent(); err != nil {
			return err
		}
	}
	return err
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
	bridge          bridge.Service
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
	checkpointReader services.BorCheckpointReader
	txActionStream   chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageCheckpointStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	type response struct {
		id  uint64
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, ok, err := s.checkpointReader.LastCheckpointId(ctx, tx)
		return respond(response{id: id, ok: ok, err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		v, err := s.checkpointReader.Checkpoint(ctx, tx, id)
		return respond(response{v: v, err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: tx.Put(kv.BorCheckpoints, k[:], v)})
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
		makeEntity := func() *heimdall.Checkpoint { return &heimdall.Checkpoint{} }
		r, err := blockRangeEntitiesFromBlockNum(tx, kv.BorCheckpoints, makeEntity, blockNum)
		return respond(response{result: r, err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, ok, err := s.milestoneReader.LastMilestoneId(ctx, tx)
		return respond(response{id: id, ok: ok, err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		v, err := s.milestoneReader.Milestone(ctx, tx, id)
		return respond(response{v: v, err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: tx.Put(kv.BorMilestones, k[:], v)})
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
		makeEntity := func() *heimdall.Milestone { return &heimdall.Milestone{} }
		r, err := blockRangeEntitiesFromBlockNum(tx, kv.BorMilestones, makeEntity, blockNum)
		return respond(response{result: r, err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, ok, err := s.spanReader.LastSpanId(ctx, tx)
		return respond(response{id: id, ok: ok, err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		v, err := s.spanReader.Span(ctx, tx, id)
		return respond(response{v: v, err: err})
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

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: tx.Put(kv.BorSpans, k[:], v)})
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
		makeEntity := func() *heimdall.Span { return &heimdall.Span{} }
		r, err := blockRangeEntitiesFromBlockNum(tx, kv.BorSpans, makeEntity, blockNum)
		return respond(response{result: r, err: err})
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

// polygonSyncStageSbpsStore is the store for heimdall.SpanBlockProducerSelection
type polygonSyncStageSbpsStore struct {
	txActionStream chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageSbpsStore) LastEntityId(ctx context.Context) (uint64, bool, error) {
	entity, ok, err := s.LastEntity(ctx)
	if err != nil || !ok {
		return 0, ok, err
	}

	return entity.RawId(), true, nil
}

func (s polygonSyncStageSbpsStore) LastEntity(ctx context.Context) (*heimdall.SpanBlockProducerSelection, bool, error) {
	type response struct {
		v   []byte
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		cursor, err := tx.Cursor(kv.BorProducerSelections)
		if err != nil {
			return respond(response{err: err})
		}

		defer cursor.Close()
		k, v, err := cursor.Last()
		if err != nil {
			return respond(response{v: nil, ok: false, err: err})
		}
		if k == nil {
			// not found
			return respond(response{v: nil, ok: false, err: nil})
		}

		return respond(response{v: v, ok: true, err: err})
	})
	if err != nil {
		return nil, false, err
	}
	if r.err != nil || !r.ok {
		return nil, r.ok, r.err
	}

	var selection heimdall.SpanBlockProducerSelection
	err = json.Unmarshal(r.v, &selection)
	return &selection, true, err
}

func (s polygonSyncStageSbpsStore) Entity(ctx context.Context, id uint64) (*heimdall.SpanBlockProducerSelection, bool, error) {
	type response struct {
		v   []byte
		ok  bool
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		k := make([]byte, dbutils.NumberLength)
		binary.BigEndian.PutUint64(k, id)

		v, err := tx.GetOne(kv.BorProducerSelections, k)
		if err != nil {
			return respond(response{v: nil, ok: false, err: err})
		}
		if v == nil {
			// not found
			return respond(response{v: nil, ok: false, err: nil})
		}

		return respond(response{v: v, ok: true, err: err})
	})
	if err != nil {
		return nil, false, err
	}
	if r.err != nil || !r.ok {
		return nil, r.ok, r.err
	}

	var selection heimdall.SpanBlockProducerSelection
	err = json.Unmarshal(r.v, &selection)
	return &selection, true, err
}

func (s polygonSyncStageSbpsStore) PutEntity(ctx context.Context, id uint64, entity *heimdall.SpanBlockProducerSelection) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		k := make([]byte, dbutils.NumberLength)
		binary.BigEndian.PutUint64(k, id)

		v, err := json.Marshal(entity)
		if err != nil {
			return respond(response{err: err})
		}

		return respond(response{err: tx.Put(kv.BorProducerSelections, k, v)})
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
		makeEntity := func() *heimdall.SpanBlockProducerSelection { return &heimdall.SpanBlockProducerSelection{} }
		r, err := blockRangeEntitiesFromBlockNum(tx, kv.BorProducerSelections, makeEntity, blockNum)
		return respond(response{result: r, err: err})
	})
	if err != nil {
		return nil, err
	}

	return r.result, r.err
}

func (s polygonSyncStageSbpsStore) Prepare(_ context.Context) error {
	return nil
}

func (s polygonSyncStageSbpsStore) Close() {
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

type polygonSyncStageBridgeStore struct {
	eventReader    services.BorEventReader
	txActionStream chan<- polygonSyncStageTxAction
}

func (s polygonSyncStageBridgeStore) LatestEventID(ctx context.Context) (uint64, error) {
	type response struct {
		id  uint64
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, _, err := s.eventReader.LastEventId(ctx, tx)
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
		return respond(response{err: bridge.PutEvents(tx, events)})
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageBridgeStore) LastProcessedEventID(ctx context.Context) (uint64, error) {
	type response struct {
		id  uint64
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, err := bridge.LastProcessedEventID(tx)
		return respond(response{id: id, err: err})
	})
	if err != nil {
		return 0, err
	}
	if r.err != nil {
		return 0, r.err
	}
	if r.id == 0 {
		return s.eventReader.LastFrozenEventId(), nil
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
		info, ok, err := bridge.LastProcessedBlockInfo(tx)
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
		return respond(response{err: bridge.PutProcessedBlockInfo(tx, info)})
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageBridgeStore) LastFrozenEventBlockNum() uint64 {
	return s.eventReader.LastFrozenEventBlockNum()
}

func (s polygonSyncStageBridgeStore) LastEventIDWithinWindow(ctx context.Context, fromID uint64, toTime time.Time) (uint64, error) {
	type response struct {
		id  uint64
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		id, err := bridge.LastEventIDWithinWindow(tx, fromID, toTime)
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

func (s polygonSyncStageBridgeStore) PutBlockNumToEventID(ctx context.Context, blockNumToEventId map[uint64]uint64) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: bridge.PutBlockNumToEventID(tx, blockNumToEventId)})
	})
	if err != nil {
		return err
	}

	return r.err
}

// Unwind delete unwindable bridge data.
// The blockNum parameter is exclusive, i.e. only data in the range (blockNum, last] is deleted.
func (s polygonSyncStageBridgeStore) Unwind(ctx context.Context, blockNum uint64) error {
	type response struct {
		err error
	}

	r, err := awaitTxAction(ctx, s.txActionStream, func(tx kv.RwTx, respond func(r response) error) error {
		return respond(response{err: bridge.Unwind(tx, blockNum)})
	})
	if err != nil {
		return err
	}

	return r.err
}

func (s polygonSyncStageBridgeStore) Events(context.Context, uint64, uint64) ([][]byte, error) {
	// used for accessing events in execution
	// astrid stage integration intends to use the bridge only for scrapping
	// not for reading which remains the same in execution (via BlockReader)
	// astrid standalone mode introduces its own reader
	panic("polygonSyncStageBridgeStore.Events not supported")
}

func (s polygonSyncStageBridgeStore) BlockEventIDsRange(context.Context, uint64) (uint64, uint64, error) {
	// used for accessing events in execution
	// astrid stage integration intends to use the bridge only for scrapping
	// not for reading which remains the same in execution (via BlockReader)
	// astrid standalone mode introduces its own reader
	panic("polygonSyncStageBridgeStore.BlockEventIDsRange not supported")
}

func (s polygonSyncStageBridgeStore) EventTxnToBlockNum(context.Context, common.Hash) (uint64, bool, error) {
	// used in RPCs
	// astrid stage integration intends to use the bridge only for scrapping,
	// not for reading which remains the same in RPCs (via BlockReader)
	// astrid standalone mode introduces its own reader
	panic("polygonSyncStageBridgeStore.EventTxnToBlockNum not supported")
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
		return result.latestValidHash, result.validationErr
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
