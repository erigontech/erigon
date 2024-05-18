package stages

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloaderproto"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/membatchwithdb"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon-lib/wrap"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/sentry/sentry_multi_client"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/finality"
	"github.com/ledgerwatch/erigon/polygon/bor/finality/flags"
	"github.com/ledgerwatch/erigon/polygon/heimdall"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/silkworm"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
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
	initialCycle := true

	for {
		start := time.Now()

		select {
		case <-hd.ShutdownCh:
			return
		default:
			// continue
		}

		// Estimate the current top height seen from the peer
		err := StageLoopIteration(ctx, db, wrap.TxContainer{}, sync, initialCycle, false, logger, blockReader, hook)

		if err != nil {
			if errors.Is(err, libcommon.ErrStopped) || errors.Is(err, context.Canceled) {
				return
			}

			logger.Error("Staged Sync", "err", err)
			if recoveryErr := hd.RecoverFromDb(db); recoveryErr != nil {
				logger.Error("Failed to recover header sentriesClient", "err", recoveryErr)
			}
			time.Sleep(500 * time.Millisecond) // just to avoid too much similar errors in logs
			continue
		}

		initialCycle = false
		hd.AfterInitialCycle()

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
func ProcessFrozenBlocks(ctx context.Context, db kv.RwDB, blockReader services.FullBlockReader, sync *stagedsync.Sync) error {
	sawZeroBlocksTimes := 0
	for {
		var finStageProgress uint64
		if blockReader.FrozenBlocks() > 0 {
			if err := db.View(ctx, func(tx kv.Tx) (err error) {
				finStageProgress, err = stages.GetStageProgress(tx, stages.Finish)
				return err
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

		log.Debug("[sync] processFrozenBlocks", "finStageProgress", finStageProgress, "frozenBlocks", blockReader.FrozenBlocks())

		more, err := sync.Run(db, wrap.TxContainer{}, true)
		if err != nil {
			return err
		}

		if err := sync.RunPrune(db, nil, true); err != nil {
			return err
		}

		if !more {
			break
		}
	}
	return nil
}

func StageLoopIteration(ctx context.Context, db kv.RwDB, txc wrap.TxContainer, sync *stagedsync.Sync, initialCycle bool, skipFrozenBlocks bool, logger log.Logger, blockReader services.FullBlockReader, hook *Hook) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	if !skipFrozenBlocks {
		if err := ProcessFrozenBlocks(ctx, db, blockReader, sync); err != nil {
			return err
		}
	}

	externalTx := txc.Tx != nil
	finishProgressBefore, borProgressBefore, headersProgressBefore, err := stagesHeadersAndFinish(db, txc.Tx)
	if err != nil {
		return err
	}
	// Sync from scratch must be able Commit partial progress
	// In all other cases - process blocks batch in 1 RwTx
	// 2 corner-cases: when sync with --snapshots=false and when executed only blocks from snapshots (in this case all stages progress is equal and > 0, but node is not synced)
	isSynced := finishProgressBefore > 0 && finishProgressBefore > blockReader.FrozenBlocks() && finishProgressBefore == headersProgressBefore
	if blockReader.BorSnapshots() != nil {
		isSynced = isSynced && borProgressBefore > blockReader.FrozenBorBlocks()
	}
	canRunCycleInOneTransaction := isSynced
	if externalTx {
		canRunCycleInOneTransaction = true
	}

	// Main steps:
	// - process new blocks
	// - commit(no_sync). NoSync - making data available for readers as-soon-as-possible. Can
	//       send notifications Now and do write to disks Later.
	// - Send Notifications: about new blocks, new receipts, state changes, etc...
	// - Prune(limited time)+Commit(sync). Write to disk happening here.

	if canRunCycleInOneTransaction && !externalTx {
		txc.Tx, err = db.BeginRwNosync(ctx)
		if err != nil {
			return err
		}
		defer txc.Tx.Rollback()
	}

	if hook != nil {
		if err = hook.BeforeRun(txc.Tx, isSynced); err != nil {
			return err
		}
	}
	_, err = sync.Run(db, txc, initialCycle)
	if err != nil {
		return err
	}
	logCtx := sync.PrintTimings()
	//var tableSizes []interface{}
	var commitTime time.Duration
	if canRunCycleInOneTransaction && !externalTx {
		//tableSizes = stagedsync.CollectDBMetrics(db, txc.Tx) // Need to do this before commit to access tx
		commitStart := time.Now()
		errTx := txc.Tx.Commit()
		txc.Tx = nil
		if errTx != nil {
			return errTx
		}
		commitTime = time.Since(commitStart)
	}

	// -- send notifications START
	if hook != nil {
		if err = hook.AfterRun(txc.Tx, finishProgressBefore); err != nil {
			return err
		}
	}
	if canRunCycleInOneTransaction && !externalTx && commitTime > 500*time.Millisecond {
		logger.Info("Commit cycle", "in", commitTime)
	}
	//if len(logCtx) > 0 { // No printing of timings or table sizes if there were no progress
	var m runtime.MemStats
	dbg.ReadMemStats(&m)
	logCtx = append(logCtx, "alloc", libcommon.ByteCount(m.Alloc), "sys", libcommon.ByteCount(m.Sys))
	logger.Info("Timings (slower than 50ms)", logCtx...)
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
		return err
	}

	return nil
}

func stagesHeadersAndFinish(db kv.RoDB, tx kv.Tx) (head, bor, fin uint64, err error) {
	if tx != nil {
		if fin, err = stages.GetStageProgress(tx, stages.Finish); err != nil {
			return head, bor, fin, err
		}
		if head, err = stages.GetStageProgress(tx, stages.Headers); err != nil {
			return head, bor, fin, err
		}
		if bor, err = stages.GetStageProgress(tx, stages.BorHeimdall); err != nil {
			return head, bor, fin, err
		}
		return head, bor, fin, nil
	}
	if err := db.View(context.Background(), func(tx kv.Tx) error {
		if fin, err = stages.GetStageProgress(tx, stages.Finish); err != nil {
			return err
		}
		if head, err = stages.GetStageProgress(tx, stages.Headers); err != nil {
			return err
		}
		if bor, err = stages.GetStageProgress(tx, stages.BorHeimdall); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return head, bor, fin, err
	}
	return head, bor, fin, nil
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
func (h *Hook) BeforeRun(tx kv.Tx, inSync bool) error {
	if tx == nil {
		return h.db.View(h.ctx, func(tx kv.Tx) error { return h.beforeRun(tx, inSync) })
	}
	return h.beforeRun(tx, inSync)
}
func (h *Hook) AfterRun(tx kv.Tx, finishProgressBefore uint64) error {
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
	if h.notifications != nil {
		return h.sendNotifications(h.notifications, tx, finishProgressBefore)
	}
	return nil
}
func (h *Hook) sendNotifications(notifications *shards.Notifications, tx kv.Tx, finishProgressBefore uint64) error {
	// update the accumulator with a new plain state version so the cache can be notified that
	// state has moved on
	if notifications.Accumulator != nil {
		plainStateVersion, err := rawdb.GetStateVersion(tx)
		if err != nil {
			return err
		}

		notifications.Accumulator.SetStateID(plainStateVersion)
	}

	if notifications.Events != nil {
		finishStageAfterSync, err := stages.GetStageProgress(tx, stages.Finish)
		if err != nil {
			return err
		}
		if err = stagedsync.NotifyNewHeaders(h.ctx, finishProgressBefore, finishStageAfterSync, h.sync.PrevUnwindPoint(), notifications.Events, tx, h.logger, h.blockReader); err != nil {
			return nil
		}
	}

	currentHeader := rawdb.ReadCurrentHeader(tx)
	if (notifications.Accumulator != nil) && (currentHeader != nil) {
		if currentHeader.Number.Uint64() == 0 {
			notifications.Accumulator.StartChange(0, currentHeader.Hash(), nil, false)
		}

		pendingBaseFee := misc.CalcBaseFee(h.chainConfig, currentHeader)
		pendingBlobFee := h.chainConfig.GetMinBlobGasPrice()
		if currentHeader.ExcessBlobGas != nil {
			excessBlobGas := misc.CalcExcessBlobGas(h.chainConfig, currentHeader)
			f, err := misc.GetBlobGasPrice(h.chainConfig, excessBlobGas)
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
		notifications.Accumulator.SendAndReset(h.ctx, notifications.StateChangesConsumer, pendingBaseFee.Uint64(), pendingBlobFee, currentHeader.GasLimit, finalizedBlock)
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

	var miningBatch kv.RwTx
	//if histV3 {
	//	sd := state.NewSharedDomains(tx)
	//	defer sd.Close()
	//	miningBatch = sd
	//} else {
	mb := membatchwithdb.NewMemoryBatch(tx, tmpDir, logger)
	defer mb.Rollback()
	miningBatch = mb
	//}
	txc := wrap.TxContainer{Tx: miningBatch}
	sd, err := state.NewSharedDomains(mb, logger)
	if err != nil {
		return err
	}
	defer sd.Close()
	txc.Doms = sd

	if _, err = mining.Run(nil, txc, false /* firstCycle */); err != nil {
		return err
	}
	tx.Rollback()
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

func StateStep(ctx context.Context, chainReader consensus.ChainReader, engine consensus.Engine, txc wrap.TxContainer, stateSync *stagedsync.Sync, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	// Construct side fork if we have one
	if unwindPoint > 0 {
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
		if err = stateSync.RunNoInterrupt(nil, txc, false /* firstCycle */); err != nil {
			return err
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
	// Run state sync
	if err = stateSync.RunNoInterrupt(nil, txc, false /* firstCycle */); err != nil {
		return err
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
	db kv.RwDB,
	snapDb kv.RwDB,
	p2pCfg p2p.Config,
	cfg *ethconfig.Config,
	controlServer *sentry_multi_client.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	blockRetire services.BlockRetire,
	agg *state.Aggregator,
	silkworm *silkworm.Silkworm,
	forkValidator *engine_helpers.ForkValidator,
	heimdallClient heimdall.HeimdallClient,
	recents *lru.ARCCache[libcommon.Hash, *bor.Snapshot],
	signatures *lru.ARCCache[libcommon.Hash, libcommon.Address],
	logger log.Logger,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockWriter := blockio.NewBlockWriter()

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	loopBreakCheck := NewLoopBreakCheck(cfg, heimdallClient)

	if heimdallClient != nil && flags.Milestone {
		loopBreakCheck = func(int) bool {
			return finality.IsMilestoneRewindPending()
		}
	}

	if cfg.Sync.LoopBlockLimit > 0 {
		previousBreakCheck := loopBreakCheck
		loopBreakCheck = func(loopCount int) bool {
			if loopCount > int(cfg.Sync.LoopBlockLimit) {
				return true
			}

			if previousBreakCheck != nil {
				return previousBreakCheck(loopCount)
			}

			return false
		}
	}

	var depositContract *libcommon.Address
	if cfg.Genesis != nil {
		depositContract = cfg.Genesis.Config.DepositContract
	}

	historyV3 := true
	return stagedsync.DefaultStages(ctx,
		stagedsync.StageSnapshotsCfg(db, *controlServer.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, blockReader, notifications, agg, cfg.InternalCL && cfg.CaplinConfig.Backfilling, cfg.CaplinConfig.BlobBackfilling, silkworm, cfg.Prune),
		stagedsync.StageHeadersCfg(db, controlServer.Hd, controlServer.Bd, *controlServer.ChainConfig, cfg.Sync, controlServer.SendHeaderRequest, controlServer.PropagateNewBlockHashes, controlServer.Penalize, cfg.BatchSize, p2pCfg.NoDiscovery, blockReader, blockWriter, dirs.Tmp, notifications, loopBreakCheck),
		stagedsync.StageBorHeimdallCfg(db, snapDb, stagedsync.MiningState{}, *controlServer.ChainConfig, heimdallClient, blockReader, controlServer.Hd, controlServer.Penalize, loopBreakCheck, recents, signatures, cfg.WithHeimdallWaypointRecording, nil),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
		stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, *controlServer.ChainConfig, blockReader, blockWriter, loopBreakCheck),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, cfg.Sync, false, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd, loopBreakCheck),
		stagedsync.StageExecuteBlocksCfg(
			db,
			cfg.Prune,
			cfg.BatchSize,
			nil,
			controlServer.ChainConfig,
			controlServer.Engine,
			&vm.Config{},
			notifications.Accumulator,
			cfg.StateStream,
			/*stateStream=*/ false,
			dirs,
			blockReader,
			controlServer.Hd,
			cfg.Genesis,
			cfg.Sync,
			agg,
			SilkwormForExecutionStage(silkworm, cfg),
		),
		stagedsync.StageHashStateCfg(db, dirs),
		stagedsync.StageTrieCfg(db, true, true, false, dirs.Tmp, blockReader, controlServer.Hd, historyV3, agg),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp, depositContract),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}

func NewPipelineStages(ctx context.Context,
	db kv.RwDB,
	cfg *ethconfig.Config,
	p2pCfg p2p.Config,
	controlServer *sentry_multi_client.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	blockRetire services.BlockRetire,
	agg *state.Aggregator,
	silkworm *silkworm.Silkworm,
	forkValidator *engine_helpers.ForkValidator,
	logger log.Logger,
	checkStateRoot bool,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockWriter := blockio.NewBlockWriter()

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode
	loopBreakCheck := NewLoopBreakCheck(cfg, nil)

	var depositContract *libcommon.Address
	if cfg.Genesis != nil {
		depositContract = cfg.Genesis.Config.DepositContract
	}

	if len(cfg.Sync.UploadLocation) == 0 {
		historyV3 := true
		return stagedsync.PipelineStages(ctx,
			stagedsync.StageSnapshotsCfg(db, *controlServer.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, blockReader, notifications, agg, cfg.InternalCL && cfg.CaplinConfig.Backfilling, cfg.CaplinConfig.BlobBackfilling, silkworm, cfg.Prune),
			stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, cfg.Sync, false, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd, loopBreakCheck),
			stagedsync.StageExecuteBlocksCfg(
				db,
				cfg.Prune,
				cfg.BatchSize,
				nil,
				controlServer.ChainConfig,
				controlServer.Engine,
				&vm.Config{},
				notifications.Accumulator,
				cfg.StateStream,
				/*stateStream=*/ false,
				dirs,
				blockReader,
				controlServer.Hd,
				cfg.Genesis,
				cfg.Sync,
				agg,
				SilkwormForExecutionStage(silkworm, cfg),
			),
			stagedsync.StageHashStateCfg(db, dirs),
			stagedsync.StageTrieCfg(db, checkStateRoot, true, false, dirs.Tmp, blockReader, controlServer.Hd, historyV3, agg),
			stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
			stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp, depositContract),
			stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
			stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
			stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
			runInTestMode)
	}

	historyV3 := true
	return stagedsync.UploaderPipelineStages(ctx,
		stagedsync.StageSnapshotsCfg(db, *controlServer.ChainConfig, cfg.Sync, dirs, blockRetire, snapDownloader, blockReader, notifications, agg, cfg.InternalCL && cfg.CaplinConfig.Backfilling, cfg.CaplinConfig.BlobBackfilling, silkworm, cfg.Prune),
		stagedsync.StageHeadersCfg(db, controlServer.Hd, controlServer.Bd, *controlServer.ChainConfig, cfg.Sync, controlServer.SendHeaderRequest, controlServer.PropagateNewBlockHashes, controlServer.Penalize, cfg.BatchSize, p2pCfg.NoDiscovery, blockReader, blockWriter, dirs.Tmp, notifications, loopBreakCheck),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, cfg.Sync, false, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd, loopBreakCheck),
		stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, *controlServer.ChainConfig, blockReader, blockWriter, loopBreakCheck),
		stagedsync.StageExecuteBlocksCfg(
			db,
			cfg.Prune,
			cfg.BatchSize,
			nil,
			controlServer.ChainConfig,
			controlServer.Engine,
			&vm.Config{},
			notifications.Accumulator,
			cfg.StateStream,
			/*stateStream=*/ false,
			dirs,
			blockReader,
			controlServer.Hd,
			cfg.Genesis,
			cfg.Sync,
			agg,
			SilkwormForExecutionStage(silkworm, cfg),
		),
		stagedsync.StageHashStateCfg(db, dirs),
		stagedsync.StageTrieCfg(db, checkStateRoot, true, false, dirs.Tmp, blockReader, controlServer.Hd, historyV3, agg),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp, depositContract),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)

}

func NewInMemoryExecution(ctx context.Context, db kv.RwDB, cfg *ethconfig.Config, controlServer *sentry_multi_client.MultiClient,
	dirs datadir.Dirs, notifications *shards.Notifications, blockReader services.FullBlockReader, blockWriter *blockio.BlockWriter, agg *state.Aggregator,
	silkworm *silkworm.Silkworm, logger log.Logger) *stagedsync.Sync {
	historyV3 := true
	return stagedsync.New(
		cfg.Sync,
		stagedsync.StateStages(ctx,
			stagedsync.StageHeadersCfg(db, controlServer.Hd, controlServer.Bd, *controlServer.ChainConfig, cfg.Sync, controlServer.SendHeaderRequest, controlServer.PropagateNewBlockHashes, controlServer.Penalize, cfg.BatchSize, false, blockReader, blockWriter, dirs.Tmp, nil, nil),
			stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, *controlServer.ChainConfig, blockReader, blockWriter, nil),
			stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, cfg.Sync, true, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd, nil),
			stagedsync.StageExecuteBlocksCfg(
				db,
				cfg.Prune,
				cfg.BatchSize,
				nil,
				controlServer.ChainConfig,
				controlServer.Engine,
				&vm.Config{},
				notifications.Accumulator,
				cfg.StateStream,
				true,
				cfg.Dirs,
				blockReader,
				controlServer.Hd,
				cfg.Genesis,
				cfg.Sync,
				agg,
				SilkwormForExecutionStage(silkworm, cfg),
			),
			stagedsync.StageHashStateCfg(db, dirs),
			stagedsync.StageTrieCfg(db, true, true, true, dirs.Tmp, blockReader, controlServer.Hd, historyV3, agg)),
		stagedsync.StateUnwindOrder,
		nil, /* pruneOrder */
		logger,
	)
}

func NewPolygonSyncStages(
	ctx context.Context,
	db kv.RwDB,
	config *ethconfig.Config,
	chainConfig *chain.Config,
	consensusEngine consensus.Engine,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	blockRetire services.BlockRetire,
	agg *state.Aggregator,
	silkworm *silkworm.Silkworm,
	forkValidator *engine_helpers.ForkValidator,
	heimdallClient heimdall.HeimdallClient,
) []*stagedsync.Stage {
	loopBreakCheck := NewLoopBreakCheck(config, heimdallClient)
	return stagedsync.PolygonSyncStages(
		ctx,
		stagedsync.StageSnapshotsCfg(
			db,
			*chainConfig,
			config.Sync,
			config.Dirs,
			blockRetire,
			snapDownloader,
			blockReader,
			notifications,
			agg,
			config.InternalCL && config.CaplinConfig.Backfilling,
			config.CaplinConfig.BlobBackfilling,
			silkworm,
			config.Prune,
		),
		stagedsync.StageBlockHashesCfg(
			db,
			config.Dirs.Tmp,
			chainConfig,
			blockio.NewBlockWriter(),
		),
		stagedsync.StageSendersCfg(
			db,
			chainConfig,
			config.Sync,
			false, /* badBlockHalt */
			config.Dirs.Tmp,
			config.Prune,
			blockReader,
			nil, /* hd */
			loopBreakCheck,
		),
		stagedsync.StageExecuteBlocksCfg(
			db,
			config.Prune,
			config.BatchSize,
			nil, /* changeSetHook */
			chainConfig,
			consensusEngine,
			&vm.Config{},
			notifications.Accumulator,
			config.StateStream,
			false, /* badBlockHalt */
			config.Dirs,
			blockReader,
			nil, /* hd */
			config.Genesis,
			config.Sync,
			agg,
			SilkwormForExecutionStage(silkworm, config),
		),
		stagedsync.StageTxLookupCfg(
			db,
			config.Prune,
			config.Dirs.Tmp,
			chainConfig.Bor,
			blockReader,
		),
		stagedsync.StageFinishCfg(
			db,
			config.Dirs.Tmp,
			forkValidator,
		),
	)
}

func NewLoopBreakCheck(cfg *ethconfig.Config, heimdallClient heimdall.HeimdallClient) func(int) bool {
	var loopBreakCheck func(int) bool

	if heimdallClient != nil && flags.Milestone {
		loopBreakCheck = func(int) bool {
			return finality.IsMilestoneRewindPending()
		}
	}

	if cfg.Sync.LoopBlockLimit == 0 {
		return loopBreakCheck
	}

	previousBreakCheck := loopBreakCheck
	return func(loopCount int) bool {
		if loopCount > int(cfg.Sync.LoopBlockLimit) {
			return true
		}

		if previousBreakCheck != nil {
			return previousBreakCheck(loopCount)
		}

		return false
	}
}
