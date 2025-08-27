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

package stages

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/common/metrics"
	proto_downloader "github.com/erigontech/erigon-lib/gointerfaces/downloaderproto"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/membatchwithdb"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/wrap"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/tracers"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/consensus/misc"
	"github.com/erigontech/erigon/execution/engineapi/engine_helpers"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/stages/headerdownload"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/sentry/sentry_multi_client"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/silkworm"
)

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db kv.RwDB,
	sync *stagedsync.Sync,
	hd *headerdownload.HeaderDownload,
	waitForDone chan struct{},
	loopMinTime time.Duration,
	logger log.Logger,
	blockReader services.FullBlockReader,
	hook *Hook,
) {
	defer close(waitForDone)

	if err := ProcessFrozenBlocks(ctx, db, blockReader, sync, hook); err != nil {
		if errors.Is(err, common.ErrStopped) || errors.Is(err, context.Canceled) {
			return
		}

		logger.Error("Staged Sync", "err", err)
		if recoveryErr := hd.RecoverFromDb(db); recoveryErr != nil {
			logger.Error("Failed to recover header sentriesClient", "err", recoveryErr)
		}
	}

	logger.Debug("[stageloop] Starting iteration")
	initialCycle := true
	for {
		start := time.Now()

		select {
		case <-hd.ShutdownCh:
			return
		default:
			// continue
		}

		hook.LastNewBlockSeen(hd.Progress())
		t := time.Now()
		// Estimate the current top height seen from the peer
		err := StageLoopIteration(ctx, db, wrap.NewTxContainer(nil, nil), sync, initialCycle, false, logger, blockReader, hook)

		if err != nil {
			if errors.Is(err, common.ErrStopped) || errors.Is(err, context.Canceled) {
				return
			}

			logger.Error("Staged Sync", "err", err)
			if recoveryErr := hd.RecoverFromDb(db); recoveryErr != nil {
				logger.Error("Failed to recover header sentriesClient", "err", recoveryErr)
			}
			time.Sleep(500 * time.Millisecond) // just to avoid too many similar error logs
			continue
		}
		if time.Since(t) < 5*time.Minute {
			initialCycle = false
		}
		if !initialCycle {
			hd.AfterInitialCycle()
		}

		if loopMinTime != 0 {
			waitTime := loopMinTime - time.Since(start)
			logger.Info("Wait time until next loop", "for", waitTime)
			c := time.After(waitTime)
			select {
			case <-ctx.Done():
				return
			case <-c:
			}
		}
	}
}

// ProcessFrozenBlocks - withuot global rwtx
func ProcessFrozenBlocks(ctx context.Context, db kv.RwDB, blockReader services.FullBlockReader, sync *stagedsync.Sync, hook *Hook) error {
	sawZeroBlocksTimes := 0
	initialCycle, firstCycle := true, true
	for {
		// run stages first time - it will download blocks
		if hook != nil {
			if err := db.View(ctx, func(tx kv.Tx) (err error) {
				err = hook.BeforeRun(tx, false)
				return err
			}); err != nil {
				return err
			}
		}

		more, err := sync.Run(db, wrap.NewTxContainer(nil, nil), initialCycle, firstCycle)
		if err != nil {
			return err
		}

		if hook != nil {
			if err := db.View(ctx, func(tx kv.Tx) (err error) {
				finishProgressBefore, _, _, err := stagesHeadersAndFinish(db, tx)
				if err != nil {
					return err
				}
				err = hook.AfterRun(tx, finishProgressBefore)
				return err
			}); err != nil {
				return err
			}
		}

		if err := sync.RunPrune(db, nil, initialCycle); err != nil {
			return err
		}
		firstCycle = false

		var finStageProgress uint64
		if blockReader.FrozenBlocks() > 0 {
			if err := db.View(ctx, func(tx kv.Tx) (err error) {
				finStageProgress, err = stages.GetStageProgress(tx, stages.Finish)
				if err != nil {
					return err
				}
				return nil
			}); err != nil {
				return err
			}
			if finStageProgress >= blockReader.FrozenBlocks() {
				break
			}
		} else {
			// having 0 frozen blocks - also may mean we didn't download them. so stages. 1 time is enough.
			// during testing we may have 0 frozen blocks and firstCycle expected to be false
			sawZeroBlocksTimes++
			if sawZeroBlocksTimes > 2 {
				break
			}
		}

		if !more {
			break
		}
	}
	return nil
}

func StageLoopIteration(ctx context.Context, db kv.RwDB, txc wrap.TxContainer, sync *stagedsync.Sync, initialCycle, firstCycle bool, logger log.Logger, blockReader services.FullBlockReader, hook *Hook) (err error) {
	// avoid crash because Erigon's core does many things
	defer dbg.RecoverPanicIntoError(logger, &err)

	hasMore := true
	for hasMore {
		hasMore, err = stageLoopIteration(ctx, db, txc, sync, initialCycle, firstCycle, logger, blockReader, hook)
		if err != nil {
			return err
		}
	}
	return nil
}

func stageLoopIteration(ctx context.Context, db kv.RwDB, txc wrap.TxContainer, sync *stagedsync.Sync, initialCycle, firstCycle bool, logger log.Logger, blockReader services.FullBlockReader, hook *Hook) (hasMore bool, err error) {
	externalTx := txc.Tx != nil
	finishProgressBefore, headersProgressBefore, gasUsed, err := stagesHeadersAndFinish(db, txc.Tx)
	if err != nil {
		return false, err
	}
	// Sync from scratch must be able Commit partial progress
	// In all other cases - process blocks batch in 1 RwTx
	// 2 corner-cases: when sync with --snapshots=false and when executed only blocks from snapshots (in this case all stages progress is equal and > 0, but node is not synced)
	isSynced := finishProgressBefore > 0 && finishProgressBefore > blockReader.FrozenBlocks() && finishProgressBefore == headersProgressBefore
	canRunCycleInOneTransaction := isSynced
	if externalTx {
		canRunCycleInOneTransaction = true
	}
	if firstCycle {
		canRunCycleInOneTransaction = false
	}
	if dbg.CommitEachStage {
		canRunCycleInOneTransaction = false
	}

	// Main steps:
	// - process new blocks
	// - commit(no_sync). NoSync - making data available for readers as-soon-as-possible. Can
	//       send notifications Now and do write to disks Later.
	// - Send Notifications: about new blocks, new receipts, state changes, etc...
	// - Prune(limited time)+Commit(sync). Write to disk happening here.

	if canRunCycleInOneTransaction && !externalTx {
		tx, err := db.BeginRwNosync(ctx)
		if err != nil {
			return false, err
		}
		defer tx.Rollback()
		txc.SetTx(tx)
	}

	if err = hook.BeforeRun(txc.Tx, isSynced); err != nil {
		return false, err
	}
	hasMore, err = sync.Run(db, txc, initialCycle, firstCycle)
	if err != nil {
		return false, err
	}
	logCtx := sync.PrintTimings()
	//var tableSizes []interface{}
	var commitTime time.Duration
	if canRunCycleInOneTransaction && !externalTx {
		//tableSizes = stagedsync.CollectDBMetrics(db, txc.Tx) // Need to do this before commit to access tx
		commitStart := time.Now()
		errTx := txc.Tx.Commit()
		txc = wrap.NewTxContainer(nil, txc.Doms)
		if errTx != nil {
			return false, errTx
		}
		commitTime = time.Since(commitStart)
	}

	// -- send notifications START
	if err = hook.AfterRun(txc.Tx, finishProgressBefore); err != nil {
		return false, err
	}
	if canRunCycleInOneTransaction && !externalTx && commitTime > 500*time.Millisecond {
		logger.Info("Commit cycle", "in", commitTime)
	}
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	if gasUsed > 0 {
		var mgasPerSec float64
		// this is a bit hacky
		for i, v := range logCtx {
			if stringVal, ok := v.(string); ok && stringVal == "Execution" {
				execTime := logCtx[i+1].(string)
				// convert from ..ms to duration
				execTimeDuration, err := time.ParseDuration(execTime)
				if err != nil {
					logger.Error("Failed to parse execution time", "err", err)
				} else {
					gasUsedMgas := float64(gasUsed) / 1e6
					mgasPerSec = gasUsedMgas / execTimeDuration.Seconds()
				}
			}
		}
		if mgasPerSec > 0 {
			metrics.ChainTipMgasPerSec.Add(mgasPerSec)
			logCtx = append(logCtx, "mgas/s", mgasPerSec)
		}
	}
	logCtx = append(logCtx, "alloc", common.ByteCount(m.Alloc), "sys", common.ByteCount(m.Sys))
	logger.Info("Timings", logCtx...)
	//if len(tableSizes) > 0 {
	//	logger.Info("Tables", tableSizes...)
	//}
	//}
	// -- send notifications END

	// -- Prune+commit(sync)
	if externalTx {
		err = sync.RunPrune(db, txc.Tx, initialCycle)
	} else {
		err = db.Update(ctx, func(tx kv.RwTx) error { return sync.RunPrune(db, tx, initialCycle) })
	}

	if err != nil {
		return false, err
	}

	return hasMore, nil
}

func stagesHeadersAndFinish(db kv.RoDB, tx kv.Tx) (head, fin uint64, gasUsed uint64, err error) {
	if tx != nil {
		if fin, err = stages.GetStageProgress(tx, stages.Finish); err != nil {
			return head, fin, gasUsed, err
		}
		if head, err = stages.GetStageProgress(tx, stages.Headers); err != nil {
			return head, fin, gasUsed, err
		}
		h := rawdb.ReadHeaderByNumber(tx, head)
		if h != nil {
			gasUsed = h.GasUsed
		}
		return head, fin, gasUsed, nil
	}
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		if fin, err = stages.GetStageProgress(tx, stages.Finish); err != nil {
			return err
		}
		if head, err = stages.GetStageProgress(tx, stages.Headers); err != nil {
			return err
		}
		h := rawdb.ReadHeaderByNumber(tx, head)
		if h != nil {
			gasUsed = h.GasUsed
		}
		return nil
	}); err != nil {
		return head, fin, gasUsed, err
	}
	return head, fin, gasUsed, nil
}

type Hook struct {
	ctx           context.Context
	notifications *shards.Notifications
	sync          *stagedsync.Sync
	chainConfig   *chain.Config
	logger        log.Logger
	blockReader   services.FullBlockReader
	updateHead    func(ctx context.Context)
	db            kv.RoDB
}

func NewHook(ctx context.Context, db kv.RoDB, notifications *shards.Notifications, sync *stagedsync.Sync, blockReader services.FullBlockReader, chainConfig *chain.Config, logger log.Logger, updateHead func(ctx context.Context)) *Hook {
	return &Hook{ctx: ctx, db: db, notifications: notifications, sync: sync, blockReader: blockReader, chainConfig: chainConfig, logger: logger, updateHead: updateHead}
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
	if tx == nil {
		return h.db.View(h.ctx, func(tx kv.Tx) error { return h.beforeRun(tx, inSync) })
	}
	return h.beforeRun(tx, inSync)
}
func (h *Hook) AfterRun(tx kv.Tx, finishProgressBefore uint64) error {
	if h == nil {
		return nil
	}
	if tx == nil {
		return h.db.View(h.ctx, func(tx kv.Tx) error { return h.afterRun(tx, finishProgressBefore) })
	}
	return h.afterRun(tx, finishProgressBefore)
}
func (h *Hook) afterRun(tx kv.Tx, finishProgressBefore uint64) error {
	// Update sentry status for peers to see our sync status
	if h.updateHead != nil {
		h.updateHead(h.ctx)
	}
	return h.sendNotifications(tx, finishProgressBefore)

}
func (h *Hook) sendNotifications(tx kv.Tx, finishStageBeforeSync uint64) error {
	if h.notifications == nil {
		return nil
	}

	// update the accumulator with a new plain state version so the cache can be notified that
	// state has moved on
	if h.notifications.Accumulator != nil {
		plainStateVersion, err := rawdb.GetStateVersion(tx)
		if err != nil {
			return err
		}

		h.notifications.Accumulator.SetStateID(plainStateVersion)
	}

	if h.notifications.Events != nil {
		finishStageAfterSync, err := stages.GetStageProgress(tx, stages.Finish)
		if err != nil {
			return err
		}

		unwindTo := h.sync.PrevUnwindPoint()

		var notifyFrom uint64
		var isUnwind bool
		if unwindTo != nil && *unwindTo != 0 && (*unwindTo) < finishStageBeforeSync {
			notifyFrom = *unwindTo
			isUnwind = true
		} else {
			heightSpan := finishStageAfterSync - finishStageBeforeSync
			if heightSpan > 1024 {
				heightSpan = 1024
			}
			notifyFrom = finishStageAfterSync - heightSpan
		}
		notifyFrom++
		notifyTo := finishStageAfterSync + 1 //[from, to)

		if err = stagedsync.NotifyNewHeaders(h.ctx, notifyFrom, notifyTo, h.notifications.Events, tx, h.logger); err != nil {
			return nil
		}
		h.notifications.RecentLogs.Notify(h.notifications.Events, notifyFrom, notifyTo, isUnwind)
	}

	currentHeader := rawdb.ReadCurrentHeader(tx)
	if (h.notifications.Accumulator != nil) && (currentHeader != nil) {
		if currentHeader.Number.Uint64() == 0 {
			h.notifications.Accumulator.StartChange(currentHeader, nil, false)
		}

		pendingBaseFee := misc.CalcBaseFee(h.chainConfig, currentHeader)
		pendingBlobFee := h.chainConfig.GetMinBlobGasPrice()
		if currentHeader.ExcessBlobGas != nil {
			nextBlockTime := currentHeader.Time + h.chainConfig.SecondsPerSlot()
			excessBlobGas := misc.CalcExcessBlobGas(h.chainConfig, currentHeader, nextBlockTime)
			f, err := misc.GetBlobGasPrice(h.chainConfig, excessBlobGas, nextBlockTime)
			if err != nil {
				return err
			}
			pendingBlobFee = f.Uint64()
		}

		var finalizedBlock uint64
		if fb := rawdb.ReadHeaderNumber(tx, rawdb.ReadForkchoiceFinalized(tx)); fb != nil {
			finalizedBlock = *fb
		}

		//h.logger.Debug("[hook] Sending state changes", "currentBlock", currentHeader.Number.Uint64(), "finalizedBlock", finalizedBlock)
		h.notifications.Accumulator.SendAndReset(h.ctx, h.notifications.StateChangesConsumer, pendingBaseFee.Uint64(), pendingBlobFee, currentHeader.GasLimit, finalizedBlock)
	}
	return nil
}

func MiningStep(ctx context.Context, db kv.RwDB, mining *stagedsync.Sync, tmpDir string, logger log.Logger) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	mb := membatchwithdb.NewMemoryBatch(tx, tmpDir, logger)
	defer mb.Close()

	txc := wrap.NewTxContainer(mb, nil)
	sd, err := state.NewSharedDomains(mb, logger)
	if err != nil {
		return err
	}
	defer sd.Close()
	txc.Doms = sd

	hasMore, err := mining.Run(nil, txc, false /* firstCycle */, false)
	if err != nil {
		return err
	}
	if hasMore {
		return errors.New("unexpected mining step has more work")
	}
	return nil
}

func addAndVerifyBlockStep(batch kv.RwTx, engine consensus.Engine, chainReader consensus.ChainReader, currentHeader *types.Header, currentBody *types.RawBody) error {
	currentHeight := currentHeader.Number.Uint64()
	currentHash := currentHeader.Hash()
	if chainReader != nil {
		if err := engine.VerifyHeader(chainReader, currentHeader, true); err != nil {
			log.Warn("Header Verification Failed", "number", currentHeight, "hash", currentHash, "reason", err)
			return fmt.Errorf("%w: %v", consensus.ErrInvalidBlock, err)
		}
		if err := engine.VerifyUncles(chainReader, currentHeader, currentBody.Uncles); err != nil {
			log.Warn("Unlcles Verification Failed", "number", currentHeight, "hash", currentHash, "reason", err)
			return fmt.Errorf("%w: %v", consensus.ErrInvalidBlock, err)
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

func StateStep(ctx context.Context, chainReader consensus.ChainReader, engine consensus.Engine, txc wrap.TxContainer, stateSync *stagedsync.Sync, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody, test bool) (err error) {
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
		if err = stateSync.RunUnwind(nil, txc); err != nil {
			return err
		}
	}
	if err := rawdb.TruncateCanonicalChain(ctx, txc.Tx, header.Number.Uint64()+1); err != nil {
		return err
	}
	// Once we unwound we can start constructing the chain (assumption: len(headersChain) == len(bodiesChain))
	for i := range headersChain {
		currentHeader := headersChain[i]
		currentBody := bodiesChain[i]

		if err := addAndVerifyBlockStep(txc.Tx, engine, chainReader, currentHeader, currentBody); err != nil {
			return err
		}
		// Run state sync
		if !test {
			hasMore, err := stateSync.RunNoInterrupt(nil, txc)
			if err != nil {
				if err := cleanupProgressIfNeeded(txc.Tx, currentHeader); err != nil {
					return err

				}
				return err
			}
			if hasMore {
				// should not ever happen since we exec blocks 1 by 1
				if err := cleanupProgressIfNeeded(txc.Tx, currentHeader); err != nil {
					return err
				}
				return errors.New("unexpected state step has more work")
			}
		}

	}

	// If we did not specify header we stop here
	if header == nil {
		return nil
	}
	// Prepare memory state for block execution
	if err := addAndVerifyBlockStep(txc.Tx, engine, chainReader, header, body); err != nil {
		return err
	}

	hasMore, err := stateSync.RunNoInterrupt(nil, txc)
	if err != nil {
		if !test {
			if err := cleanupProgressIfNeeded(txc.Tx, header); err != nil {
				return err
			}
		}
		return err
	}
	if hasMore {
		// should not ever happen since we exec blocks 1 by 1
		if !test {
			if err := cleanupProgressIfNeeded(txc.Tx, header); err != nil {
				return err
			}
		}
		return errors.New("unexpected state step has more work")
	}

	return nil
}

func SilkwormForExecutionStage(silkworm *silkworm.Silkworm, cfg *ethconfig.Config) *silkworm.Silkworm {
	if cfg.SilkwormExecution {
		return silkworm
	}
	return nil
}

func NewDefaultStages(ctx context.Context,
	db kv.TemporalRwDB,
	p2pCfg p2p.Config,
	cfg *ethconfig.Config,
	controlServer *sentry_multi_client.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	blockRetire services.BlockRetire,
	silkworm *silkworm.Silkworm,
	forkValidator *engine_helpers.ForkValidator,
	signatures *lru.ARCCache[common.Hash, common.Address],
	logger log.Logger,
	tracer *tracers.Tracer,
) []*stagedsync.Stage {
	var tracingHooks *tracing.Hooks
	if tracer != nil {
		tracingHooks = tracer.Hooks
	}
	dirs := cfg.Dirs
	blockWriter := blockio.NewBlockWriter()

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return stagedsync.DefaultStages(ctx,
		stagedsync.StageSnapshotsCfg(db, controlServer.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, blockReader, notifications, cfg.InternalCL && cfg.CaplinConfig.ArchiveBlocks, cfg.CaplinConfig.ArchiveBlobs, cfg.CaplinConfig.ArchiveStates, silkworm, cfg.Prune),
		stagedsync.StageHeadersCfg(db, controlServer.Hd, controlServer.Bd, controlServer.ChainConfig, cfg.Sync, controlServer.SendHeaderRequest, controlServer.PropagateNewBlockHashes, controlServer.Penalize, cfg.BatchSize, p2pCfg.NoDiscovery, blockReader, blockWriter, dirs.Tmp, notifications),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
		stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, controlServer.ChainConfig, blockReader, blockWriter),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, cfg.Sync, false, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd),
		stagedsync.StageExecuteBlocksCfg(db, cfg.Prune, cfg.BatchSize, controlServer.ChainConfig, controlServer.Engine, &vm.Config{Tracer: tracingHooks}, notifications, cfg.StateStream, false, dirs, blockReader, controlServer.Hd, cfg.Genesis, cfg.Sync, SilkwormForExecutionStage(silkworm, cfg)),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator), runInTestMode)
}

func NewPipelineStages(ctx context.Context,
	db kv.TemporalRwDB,
	cfg *ethconfig.Config,
	controlServer *sentry_multi_client.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	blockRetire services.BlockRetire,
	silkworm *silkworm.Silkworm,
	forkValidator *engine_helpers.ForkValidator,
	tracer *tracers.Tracer,
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

	return stagedsync.PipelineStages(ctx,
		stagedsync.StageSnapshotsCfg(db, controlServer.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, blockReader, notifications, cfg.InternalCL && cfg.CaplinConfig.ArchiveBlocks, cfg.CaplinConfig.ArchiveBlobs, cfg.CaplinConfig.ArchiveStates, silkworm, cfg.Prune),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, cfg.Sync, false, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd),
		stagedsync.StageExecuteBlocksCfg(db, cfg.Prune, cfg.BatchSize, controlServer.ChainConfig, controlServer.Engine, &vm.Config{Tracer: tracingHooks}, notifications, cfg.StateStream, false, dirs, blockReader, controlServer.Hd, cfg.Genesis, cfg.Sync, SilkwormForExecutionStage(silkworm, cfg)),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		stagedsync.StageWitnessProcessingCfg(db, controlServer.ChainConfig, controlServer.WitnessBuffer))
}

func NewInMemoryExecution(ctx context.Context, db kv.RwDB, cfg *ethconfig.Config, controlServer *sentry_multi_client.MultiClient,
	dirs datadir.Dirs, notifications *shards.Notifications, blockReader services.FullBlockReader, blockWriter *blockio.BlockWriter,
	silkworm *silkworm.Silkworm, logger log.Logger) *stagedsync.Sync {
	return stagedsync.New(
		cfg.Sync,
		stagedsync.StateStages(ctx, stagedsync.StageHeadersCfg(db, controlServer.Hd, controlServer.Bd, controlServer.ChainConfig, cfg.Sync, controlServer.SendHeaderRequest, controlServer.PropagateNewBlockHashes, controlServer.Penalize, cfg.BatchSize, false, blockReader, blockWriter, dirs.Tmp, nil),
			stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, controlServer.ChainConfig, blockReader, blockWriter), stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter), stagedsync.StageSendersCfg(db, controlServer.ChainConfig, cfg.Sync, true, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd),
			stagedsync.StageExecuteBlocksCfg(db, cfg.Prune, cfg.BatchSize, controlServer.ChainConfig, controlServer.Engine, &vm.Config{}, notifications, cfg.StateStream, true, cfg.Dirs, blockReader, controlServer.Hd, cfg.Genesis, cfg.Sync, SilkwormForExecutionStage(silkworm, cfg))),
		stagedsync.StateUnwindOrder,
		nil, /* pruneOrder */
		logger,
		stages.ModeForkValidation,
	)
}
