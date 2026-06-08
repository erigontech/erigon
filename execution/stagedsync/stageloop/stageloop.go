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

package stageloop

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/downloader"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/services"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/exec"
	execp2p "github.com/erigontech/erigon/execution/p2p"
	"github.com/erigontech/erigon/execution/protocol/rules"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/tracing"
	"github.com/erigontech/erigon/execution/tracing/tracers"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/vm"
	"github.com/erigontech/erigon/node/ethconfig"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/shards"
	"github.com/erigontech/erigon/p2p/protocols/eth"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
)

// NotificationSender abstracts notification dispatch so Hook can delegate to
// an implementation defined in another package (e.g. execmodule.Dispatcher)
// without creating a circular import.
type NotificationSender interface {
	Dispatch(ctx context.Context, tx kv.Tx, accumulator *shards.Accumulator, recentReceipts *shards.RecentReceipts, finishProgressBefore, finishProgressAfter uint64, prevUnwindPoint *uint64) error
}

type Hook struct {
	ctx                                 context.Context
	notifications                       *shards.Notifications
	sync                                *stagedsync.Sync
	chainConfig                         *chain.Config
	logger                              log.Logger
	dispatcher                          NotificationSender
	updateHead                          func(ctx context.Context)
	statusDataGetter                    sentry_multi_client.StatusGetter
	blockRangePublisher                 *execp2p.Publisher
	lastAnnouncedBlockRangeLatestNumber uint64
	lastAnnouncedBlockRangeTime         time.Time
}

func NewHook(
	ctx context.Context,
	notifications *shards.Notifications,
	sync *stagedsync.Sync,
	chainConfig *chain.Config,
	logger log.Logger,
	dispatcher NotificationSender,
	updateHead func(ctx context.Context),
	statusDataGetter sentry_multi_client.StatusGetter,
	blockRangePublisher *execp2p.Publisher,
) *Hook {
	return &Hook{
		ctx:                 ctx,
		notifications:       notifications,
		sync:                sync,
		chainConfig:         chainConfig,
		logger:              logger,
		dispatcher:          dispatcher,
		updateHead:          updateHead,
		statusDataGetter:    statusDataGetter,
		blockRangePublisher: blockRangePublisher,
	}
}

func (h *Hook) beforeRun(tx kv.Tx, inSync bool) error {
	notifications := h.notifications
	if notifications != nil && notifications.Accumulator != nil && inSync {
		stateVersion, err := rawdb.GetStateVersion(tx)
		if err != nil {
			h.logger.Error("problem reading plain state version", "err", err)
		}
		notifications.Accumulator.Reset(stateVersion)
	}
	return nil
}

func (h *Hook) LastNewBlockSeen(n uint64) {
	if h == nil || h.notifications == nil {
		return
	}
	h.notifications.NewLastBlockSeen(n)
}

func (h *Hook) BeforeRun(tx kv.Tx, inSync bool) error {
	if h == nil {
		return nil
	}
	return h.beforeRun(tx, inSync)
}

// SendNotifications dispatches all pending notifications (state changes,
// headers, logs, receipts) via the Dispatcher. The tx is the data source —
// either the SD's blockOverlay (pre-commit) or a committed DB tx.
//
// All call sites follow the same pattern:
//
//	hook.SendNotifications(tx, finishProgressBefore)
//	hook.UpdateHead(tx, finishProgressBefore, isSynced)
func (h *Hook) SendNotifications(tx kv.Tx, finishProgressBefore uint64) error {
	if h == nil {
		return nil
	}
	if h.notifications == nil || h.dispatcher == nil {
		return nil
	}
	finishStageAfterSync, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return err
	}
	return h.dispatcher.Dispatch(
		h.ctx, tx,
		h.notifications.Accumulator,
		h.notifications.RecentReceipts,
		finishProgressBefore,
		finishStageAfterSync,
		h.sync.PrevUnwindPoint(),
	)
}

// UpdateHead updates the sentry head status and announces the block range
// to P2P peers. Called after SendNotifications.
func (h *Hook) UpdateHead(tx kv.Tx, finishProgressBefore uint64, isSynced bool) error {
	if h == nil {
		return nil
	}
	if h.updateHead != nil {
		h.updateHead(h.ctx)
	}
	finishStageAfterSync, err := stages.GetStageProgress(tx, stages.Finish)
	if err != nil {
		return err
	}
	h.maybeAnnounceBlockRange(finishProgressBefore, finishStageAfterSync, isSynced)
	return nil
}

func (h *Hook) maybeAnnounceBlockRange(finishStageBeforeSync, finishStageAfterSync uint64, isSynced bool) {
	if h.blockRangePublisher == nil || h.statusDataGetter == nil {
		return
	}

	// we want to announce BlockRangeUpdate
	// - after initialization
	// - during sync: every 1 minute. A time limit is used to prevent excessive announcements in
	//									case of fast execution due to empty blocks or small SyncLoopBlockLimit
	// - after sync: every 32 blocks or on unwind of any depth
	hadUnwind := h.sync != nil && h.sync.PrevUnwindPoint() != nil && *h.sync.PrevUnwindPoint() < finishStageBeforeSync
	isInitialAnnouncement := h.lastAnnouncedBlockRangeLatestNumber == 0
	progressed := finishStageAfterSync >= h.lastAnnouncedBlockRangeLatestNumber+32
	timeElapsed := time.Since(h.lastAnnouncedBlockRangeTime)
	shouldPublishByTime := !isSynced && timeElapsed >= 1*time.Minute

	if !hadUnwind && !isInitialAnnouncement && !progressed && !shouldPublishByTime {
		return
	}

	status, err := h.statusDataGetter.GetStatusData(h.ctx)
	if err != nil {
		h.logger.Warn("[hook] block range update skipped; status data unavailable", "err", err)
		return
	}

	packet := eth.BlockRangeUpdatePacket{
		Earliest:   status.MinimumBlockHeight,
		Latest:     status.MaxBlockHeight,
		LatestHash: gointerfaces.ConvertH256ToHash(status.BestHash),
	}

	if err := packet.Validate(); err != nil {
		h.logger.Warn("[hook] block range update skipped", "err", err)
		return
	}

	h.logger.Debug("[hook] publishing block range update", "earliest", packet.Earliest, "latest", packet.Latest, "hadUnwind", hadUnwind, "isSynced", isSynced, "timeElapsed", timeElapsed)
	h.blockRangePublisher.PublishBlockRangeUpdate(packet)
	h.lastAnnouncedBlockRangeLatestNumber = packet.Latest
	h.lastAnnouncedBlockRangeTime = time.Now()
}

func addAndVerifyBlockStep(batch kv.RwTx, engine rules.Engine, chainReader rules.ChainReader, currentHeader *types.Header, currentBody *types.RawBody) error {
	currentHeight := currentHeader.Number.Uint64()
	currentHash := currentHeader.Hash()
	if chainReader != nil {
		if err := engine.VerifyHeader(chainReader, currentHeader, true); err != nil {
			log.Warn("Header Verification Failed", "number", currentHeight, "hash", currentHash, "reason", err)
			return fmt.Errorf("%w: %v", rules.ErrInvalidBlock, err)
		}
		if err := engine.VerifyUncles(chainReader, currentHeader, currentBody.Uncles); err != nil {
			log.Warn("Unlcles Verification Failed", "number", currentHeight, "hash", currentHash, "reason", err)
			return fmt.Errorf("%w: %v", rules.ErrInvalidBlock, err)
		}
	}
	// Prepare memory state for block execution
	if err := rawdb.WriteHeader(batch, currentHeader); err != nil {
		return err
	}
	prevHash, err := rawdb.ReadCanonicalHash(batch, currentHeight)
	if err != nil {
		return err
	}
	if err := rawdb.WriteCanonicalHash(batch, currentHash, currentHeight); err != nil {
		return err
	}
	if err := rawdb.WriteHeadHeaderHash(batch, currentHash); err != nil {
		return err
	}
	if _, err := rawdb.WriteRawBodyIfNotExists(batch, currentHash, currentHeight, currentBody); err != nil {
		return err
	}
	if prevHash != currentHash {
		if err := rawdbv3.TxNums.Truncate(batch, currentHeight); err != nil {
			return err
		}
		if err := rawdb.AppendCanonicalTxNums(batch, currentHeight); err != nil {
			return err
		}
	}

	if err := stages.SaveStageProgress(batch, stages.Headers, currentHeight); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(batch, stages.Bodies, currentHeight); err != nil {
		return err
	}
	return nil
}

func cleanupProgressIfNeeded(batch kv.RwTx, header *types.Header) error {
	// If we fail state root then we have wrong execution stage progress set (+1), we need to decrease by one!
	progress, err := stages.GetStageProgress(batch, stages.Execution)
	if err != nil {
		return err
	}
	if progress == header.Number.Uint64() && progress > 0 {
		progress--
		if err := stages.SaveStageProgress(batch, stages.Execution, progress); err != nil {
			return err
		}
	}
	return nil
}

func StateStep(ctx context.Context, chainReader rules.ChainReader, engine rules.Engine, sd *execctx.SharedDomains, tx kv.TemporalRwTx, stateSync *stagedsync.Sync, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things
	shouldUnwind := unwindPoint > 0
	// Construct side fork if we have one
	if shouldUnwind {
		// Run it through the unwind
		if err := stateSync.UnwindTo(unwindPoint, stagedsync.StagedUnwind, nil); err != nil {
			return err
		}
		if err = stateSync.RunUnwind(sd, tx); err != nil {
			return err
		}
	}
	lastNum := headersChain[len(headersChain)-1].Number.Uint64()
	if err := rawdb.TruncateCanonicalChain(ctx, tx, lastNum+1); err != nil {
		return err
	}
	// Once we unwound we can start constructing the chain (assumption: len(headersChain) == len(bodiesChain))
	for i := range headersChain {
		currentHeader := headersChain[i]
		currentBody := bodiesChain[i]
		if err := addAndVerifyBlockStep(tx, engine, chainReader, currentHeader, currentBody); err != nil {
			return err
		}
		// Run state sync
		hasMore, err := stateSync.RunNoInterrupt(sd, tx)
		if err != nil {
			// Don't lose the original error, like cancellation or common.ErrStopped.
			return errors.Join(err, cleanupProgressIfNeeded(tx, currentHeader))
		}
		if hasMore {
			// should not ever happen since we exec blocks 1 by 1
			if err := cleanupProgressIfNeeded(tx, currentHeader); err != nil {
				return err
			}
			return errors.New("unexpected state step has more work")
		}
	}
	return nil
}

func NewDefaultStages(ctx context.Context,
	db kv.TemporalRwDB,
	cfg *ethconfig.Config,
	controlServer *sentry_multi_client.MultiClient,
	notifications *shards.Notifications,
	snapDownloader downloader.Client,
	blockReader services.FullBlockReader,
	blockRetire services.BlockRetire,
	tracer *tracers.Tracer,
	afterSnapshotDownload func(ctx context.Context) error,
	readAheader *exec.BlockReadAheader,
) []*stagedsync.Stage {
	var tracingHooks *tracing.Hooks
	if tracer != nil {
		tracingHooks = tracer.Hooks
	}
	dirs := cfg.Dirs
	blockWriter := blockio.NewBlockWriter()
	snapCfg := stagedsync.StageSnapshotsCfg(db, controlServer.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, blockReader, notifications, cfg.InternalCL && cfg.CaplinConfig.ArchiveBlocks, cfg.CaplinConfig.ArchiveBlobs, cfg.CaplinConfig.ArchiveStates, cfg.Prune, afterSnapshotDownload, cfg.Snapshot.ManifestReady)
	snapCfg.SetLifecycleDrivenByStorage(cfg.Snapshot.LifecycleDrivenByStorage)
	snapCfg.SetInitialStateReady(cfg.Snapshot.InitialStateReady)
	snapCfg.SetRetirementPublishers(cfg.Snapshot.PublishRetirementStart, cfg.Snapshot.PublishRetirementDone)
	return stagedsync.DefaultStages(
		ctx,
		snapCfg,
		stagedsync.StageHeadersCfg(blockReader),
		stagedsync.StageBlockHashesCfg(dirs.Tmp, blockWriter),
		stagedsync.StageBodiesCfg(blockReader, blockWriter),
		stagedsync.StageSendersCfg(controlServer.ChainConfig, cfg.Sync, dbg.BadBlockHalt, dirs.Tmp, cfg.Prune, blockReader, readAheader),
		stagedsync.StageExecuteBlocksCfg(db, cfg.Prune, cfg.BatchSize, controlServer.ChainConfig, controlServer.Engine, &vm.Config{Tracer: tracingHooks}, notifications, cfg.StateStream, dbg.BadBlockHalt, dirs, blockReader, cfg.Genesis, cfg.Sync, cfg.ExperimentalBAL, readAheader),
		stagedsync.StageTxLookupCfg(cfg.Prune, dirs.Tmp, blockReader),
		stagedsync.StageFinishCfg(),
	)
}

func NewPipelineStages(ctx context.Context,
	db kv.TemporalRwDB,
	cfg *ethconfig.Config,
	controlServer *sentry_multi_client.MultiClient,
	notifications *shards.Notifications,
	snapDownloader downloader.Client,
	blockReader services.FullBlockReader,
	blockRetire services.BlockRetire,
	tracer *tracers.Tracer,
	afterSnapshotDownload func(ctx context.Context) error,
	readAheader *exec.BlockReadAheader,
) []*stagedsync.Stage {
	var tracingHooks *tracing.Hooks
	if tracer != nil {
		tracingHooks = tracer.Hooks
	}
	dirs := cfg.Dirs
	blockWriter := blockio.NewBlockWriter()

	var depositContract common.Address
	if cfg.Genesis != nil {
		depositContract = cfg.Genesis.Config.DepositContract
	}
	_ = depositContract

	snapCfg := stagedsync.StageSnapshotsCfg(db, controlServer.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, blockReader, notifications, cfg.InternalCL && cfg.CaplinConfig.ArchiveBlocks, cfg.CaplinConfig.ArchiveBlobs, cfg.CaplinConfig.ArchiveStates, cfg.Prune, afterSnapshotDownload, cfg.Snapshot.ManifestReady)
	snapCfg.SetLifecycleDrivenByStorage(cfg.Snapshot.LifecycleDrivenByStorage)
	snapCfg.SetInitialStateReady(cfg.Snapshot.InitialStateReady)
	snapCfg.SetRetirementPublishers(cfg.Snapshot.PublishRetirementStart, cfg.Snapshot.PublishRetirementDone)
	return stagedsync.PipelineStages(ctx,
		snapCfg,
		stagedsync.StageBlockHashesCfg(dirs.Tmp, blockWriter),
		stagedsync.StageSendersCfg(controlServer.ChainConfig, cfg.Sync, dbg.BadBlockHalt, dirs.Tmp, cfg.Prune, blockReader, readAheader),
		stagedsync.StageExecuteBlocksCfg(db, cfg.Prune, cfg.BatchSize, controlServer.ChainConfig, controlServer.Engine, &vm.Config{Tracer: tracingHooks}, notifications, cfg.StateStream, dbg.BadBlockHalt, dirs, blockReader, cfg.Genesis, cfg.Sync, cfg.ExperimentalBAL, readAheader),
		stagedsync.StageTxLookupCfg(cfg.Prune, dirs.Tmp, blockReader),
		stagedsync.StageFinishCfg(),
		stagedsync.StageWitnessProcessingCfg(controlServer.ChainConfig, controlServer.WitnessBuffer),
	)
}

func NewInMemoryExecution(
	ctx context.Context,
	db kv.TemporalRwDB,
	cfg *ethconfig.Config,
	controlServer *sentry_multi_client.MultiClient,
	notifications *shards.Notifications,
	blockReader services.FullBlockReader,
	blockWriter *blockio.BlockWriter,
	logger log.Logger,
	readAheader *exec.BlockReadAheader,
) *stagedsync.Sync {
	return stagedsync.New(
		cfg.Sync,
		stagedsync.StateStages(ctx,
			stagedsync.StageHeadersCfg(blockReader),
			stagedsync.StageBodiesCfg(blockReader, blockWriter),
			stagedsync.StageBlockHashesCfg(cfg.Dirs.Tmp, blockWriter),
			stagedsync.StageSendersCfg(controlServer.ChainConfig, cfg.Sync, true /* badBlockHalt */, cfg.Dirs.Tmp, cfg.Prune, blockReader, readAheader),
			stagedsync.StageExecuteBlocksCfg(db, cfg.Prune, cfg.BatchSize, controlServer.ChainConfig, controlServer.Engine, &vm.Config{}, notifications, cfg.StateStream, true, cfg.Dirs, blockReader, cfg.Genesis, cfg.Sync, cfg.ExperimentalBAL, readAheader),
		),
		stagedsync.StateUnwindOrder,
		nil, /* pruneOrder */
		logger,
		stages.ModeForkValidation,
	)
}
