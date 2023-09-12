package stages

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/turbo/engineapi/engine_helpers"
	"github.com/ledgerwatch/erigon/turbo/services"

	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

// StageLoop runs the continuous loop of staged sync
func StageLoop(ctx context.Context,
	db kv.RwDB,
	sync *stagedsync.Sync,
	hd *headerdownload.HeaderDownload,
	waitForDone chan struct{},
	loopMinTime time.Duration,
	logger log.Logger,
	blockReader services.FullBlockReader,
	hook *Hook,
	forcePartialCommit bool,
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
		err := StageLoopIteration(ctx, db, nil, sync, initialCycle, logger, blockReader, hook, forcePartialCommit)

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

func StageLoopIteration(ctx context.Context, db kv.RwDB, tx kv.RwTx, sync *stagedsync.Sync, initialCycle bool, logger log.Logger, blockReader services.FullBlockReader, hook *Hook, forcePartialCommit bool) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	externalTx := tx != nil
	finishProgressBefore, borProgressBefore, headersProgressBefore, err := stagesHeadersAndFinish(db, tx)
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
	if forcePartialCommit {
		canRunCycleInOneTransaction = false
	}

	// Main steps:
	// - process new blocks
	// - commit(no_sync). NoSync - making data available for readers as-soon-as-possible. Can
	//       send notifications Now and do write to disks Later.
	// - Send Notifications: about new blocks, new receipts, state changes, etc...
	// - Prune(limited time)+Commit(sync). Write to disk happening here.

	if canRunCycleInOneTransaction && !externalTx {
		tx, err = db.BeginRwNosync(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if hook != nil {
		if err = hook.BeforeRun(tx, isSynced); err != nil {
			return err
		}
	}
	err = sync.Run(db, tx, initialCycle)
	if err != nil {
		return err
	}
	logCtx := sync.PrintTimings()
	var tableSizes []interface{}
	var commitTime time.Duration
	if canRunCycleInOneTransaction && !externalTx {
		tableSizes = stagedsync.PrintTables(db, tx) // Need to do this before commit to access tx
		commitStart := time.Now()
		errTx := tx.Commit()
		tx = nil
		if errTx != nil {
			return errTx
		}
		commitTime = time.Since(commitStart)
	}

	// -- send notifications START
	if hook != nil {
		if externalTx {
			if err = hook.AfterRun(tx, finishProgressBefore); err != nil {
				return err
			}
		} else {
			if err := db.View(ctx, func(tx kv.Tx) error {
				return hook.AfterRun(tx, finishProgressBefore)
			}); err != nil {
				return err
			}
		}
	}
	if canRunCycleInOneTransaction && !externalTx && commitTime > 500*time.Millisecond {
		logger.Info("Commit cycle", "in", commitTime)
	}
	if len(logCtx) > 0 { // No printing of timings or table sizes if there were no progress
		logger.Info("Timings (slower than 50ms)", logCtx...)
		if len(tableSizes) > 0 {
			logger.Info("Tables", tableSizes...)
		}
	}
	// -- send notifications END

	// -- Prune+commit(sync)
	if err := stageLoopStepPrune(ctx, db, tx, sync, initialCycle); err != nil {
		return err
	}

	return nil
}
func stageLoopStepPrune(ctx context.Context, db kv.RwDB, tx kv.RwTx, sync *stagedsync.Sync, initialCycle bool) (err error) {
	if tx != nil {
		return sync.RunPrune(db, tx, initialCycle)
	}
	return db.Update(ctx, func(tx kv.RwTx) error { return sync.RunPrune(db, tx, initialCycle) })
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
	updateHead    func(ctx context.Context, headHeight uint64, headTime uint64, hash libcommon.Hash, td *uint256.Int)
}

func NewHook(ctx context.Context, notifications *shards.Notifications, sync *stagedsync.Sync, blockReader services.FullBlockReader, chainConfig *chain.Config, logger log.Logger, updateHead func(ctx context.Context, headHeight uint64, headTime uint64, hash libcommon.Hash, td *uint256.Int)) *Hook {
	return &Hook{ctx: ctx, notifications: notifications, sync: sync, blockReader: blockReader, chainConfig: chainConfig, logger: logger, updateHead: updateHead}
}
func (h *Hook) BeforeRun(tx kv.Tx, inSync bool) error {
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
func (h *Hook) AfterRun(tx kv.Tx, finishProgressBefore uint64) error {
	notifications := h.notifications
	blockReader := h.blockReader
	// -- send notifications START
	//TODO: can this 2 headers be 1
	var headHeader, currentHeder *types.Header

	// Update sentry status for peers to see our sync status
	var headTd *big.Int
	var plainStateVersion, finalizedBlock uint64
	head, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return err
	}
	headHash, err := rawdb.ReadCanonicalHash(tx, head)
	if err != nil {
		return err
	}
	if headTd, err = rawdb.ReadTd(tx, headHash, head); err != nil {
		return err
	}
	headHeader = rawdb.ReadHeader(tx, headHash, head)
	currentHeder = rawdb.ReadCurrentHeader(tx)
	finalizedHeaderHash := rawdb.ReadForkchoiceFinalized(tx)
	if fb := rawdb.ReadHeaderNumber(tx, finalizedHeaderHash); fb != nil {
		finalizedBlock = *fb
	}
	// update the accumulator with a new plain state version so the cache can be notified that
	// state has moved on
	if plainStateVersion, err = rawdb.GetStateVersion(tx); err != nil {
		return err
	}
	if notifications != nil && notifications.Accumulator != nil {
		notifications.Accumulator.SetStateID(plainStateVersion)
	}

	if headTd != nil && headHeader != nil {
		headTd256, overflow := uint256.FromBig(headTd)
		if overflow {
			return fmt.Errorf("headTds higher than 2^256-1")
		}
		h.updateHead(h.ctx, head, headHeader.Time, headHash, headTd256)
	}

	if notifications != nil && notifications.Events != nil {
		if err = stagedsync.NotifyNewHeaders(h.ctx, finishProgressBefore, head, h.sync.PrevUnwindPoint(), notifications.Events, tx, h.logger, blockReader); err != nil {
			return nil
		}
	}
	if notifications != nil && notifications.Accumulator != nil && currentHeder != nil {

		pendingBaseFee := misc.CalcBaseFee(h.chainConfig, currentHeder)
		if currentHeder.Number.Uint64() == 0 {
			notifications.Accumulator.StartChange(0, currentHeder.Hash(), nil, false)
		}

		notifications.Accumulator.SendAndReset(h.ctx, notifications.StateChangesConsumer, pendingBaseFee.Uint64(), currentHeder.GasLimit, finalizedBlock)
	}
	// -- send notifications END
	return nil
}

func MiningStep(ctx context.Context, kv kv.RwDB, mining *stagedsync.Sync, tmpDir string) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	tx, err := kv.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	miningBatch := memdb.NewMemoryBatch(tx, tmpDir)
	defer miningBatch.Rollback()

	if err = mining.Run(nil, miningBatch, false /* firstCycle */); err != nil {
		return err
	}
	tx.Rollback()
	return nil
}

func StateStep(ctx context.Context, chainReader consensus.ChainHeaderReader, engine consensus.Engine, batch kv.RwTx, blockWriter *blockio.BlockWriter, stateSync *stagedsync.Sync, Bd *bodydownload.BodyDownload, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	// Construct side fork if we have one
	if unwindPoint > 0 {
		// Run it through the unwind
		stateSync.UnwindTo(unwindPoint, libcommon.Hash{})
		if err = stateSync.RunUnwind(nil, batch); err != nil {
			return err
		}
	}
	if err := rawdb.TruncateCanonicalChain(ctx, batch, header.Number.Uint64()+1); err != nil {
		return err
	}
	// Once we unwound we can start constructing the chain (assumption: len(headersChain) == len(bodiesChain))
	for i := range headersChain {
		currentHeader := headersChain[i]
		currentBody := bodiesChain[i]
		currentHeight := headersChain[i].Number.Uint64()
		currentHash := headersChain[i].Hash()
		if chainReader != nil {
			if err := engine.VerifyHeader(chainReader, currentHeader, true); err != nil {
				log.Warn("Header Verification Failed", "number", currentHeight, "hash", currentHash, "reason", err)
				return err
			}
		}

		// Prepare memory state for block execution
		if err := rawdb.WriteHeader(batch, currentHeader); err != nil {
			return err
		}
		if currentBody != nil {
			Bd.AddToPrefetch(currentHeader, currentBody)
		}

		if err := rawdb.WriteCanonicalHash(batch, currentHash, currentHeight); err != nil {
			return err
		}
		if err := rawdb.WriteHeadHeaderHash(batch, currentHash); err != nil {
			return err
		}
		if err = stages.SaveStageProgress(batch, stages.Headers, currentHeight); err != nil {
			return err
		}
		// Run state sync
		if err = stateSync.RunNoInterrupt(nil, batch, false /* firstCycle */); err != nil {
			return err
		}
	}

	// If we did not specify header we stop here
	if header == nil {
		return nil
	}
	if err := engine.VerifyHeader(chainReader, header, true); err != nil {
		log.Warn("Header Verification Failed", "number", header.Number.Uint64(), "hash", header.Hash(), "reason", err)
		return err
	}

	// Setup
	height := header.Number.Uint64()
	hash := header.Hash()
	// Prepare memory state for block execution
	if err = rawdb.WriteHeader(batch, header); err != nil {
		return err
	}
	if err = rawdb.WriteCanonicalHash(batch, hash, height); err != nil {
		return err
	}
	if err := rawdb.WriteHeadHeaderHash(batch, hash); err != nil {
		return err
	}
	if err = stages.SaveStageProgress(batch, stages.Headers, height); err != nil {
		return err
	}
	if body != nil {
		Bd.AddToPrefetch(header, body)
	}
	// Run state sync
	if err = stateSync.RunNoInterrupt(nil, batch, false /* firstCycle */); err != nil {
		return err
	}
	return nil
}

func NewDefaultStages(ctx context.Context,
	db kv.RwDB,
	p2pCfg p2p.Config,
	cfg *ethconfig.Config,
	controlServer *sentry.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	blockRetire services.BlockRetire,
	agg *state.AggregatorV3,
	forkValidator *engine_helpers.ForkValidator,
	heimdallClient bor.IHeimdallClient,
	logger log.Logger,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockWriter := blockio.NewBlockWriter(cfg.HistoryV3)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return stagedsync.DefaultStages(ctx,
		stagedsync.StageSnapshotsCfg(db, *controlServer.ChainConfig, dirs, blockRetire, snapDownloader, blockReader, notifications.Events, cfg.HistoryV3, agg),
		stagedsync.StageHeadersCfg(db, controlServer.Hd, controlServer.Bd, *controlServer.ChainConfig, controlServer.SendHeaderRequest, controlServer.PropagateNewBlockHashes, controlServer.Penalize, cfg.BatchSize, p2pCfg.NoDiscovery, blockReader, blockWriter, dirs.Tmp, notifications, forkValidator),
		stagedsync.StageBorHeimdallCfg(db, stagedsync.MiningState{}, *controlServer.ChainConfig, heimdallClient, blockReader),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
		stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, *controlServer.ChainConfig, blockReader, cfg.HistoryV3, blockWriter),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, false, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd),
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
			cfg.HistoryV3,
			dirs,
			blockReader,
			controlServer.Hd,
			cfg.Genesis,
			cfg.Sync,
			agg,
		),
		stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3),
		stagedsync.StageTrieCfg(db, true, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}

func NewPipelineStages(ctx context.Context,
	db kv.RwDB,
	cfg *ethconfig.Config,
	controlServer *sentry.MultiClient,
	notifications *shards.Notifications,
	snapDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader,
	blockRetire services.BlockRetire,
	agg *state.AggregatorV3,
	forkValidator *engine_helpers.ForkValidator,
	logger log.Logger,
	checkStateRoot bool,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockWriter := blockio.NewBlockWriter(cfg.HistoryV3)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return stagedsync.PipelineStages(ctx,
		stagedsync.StageSnapshotsCfg(db, *controlServer.ChainConfig, dirs, blockRetire, snapDownloader, blockReader, notifications.Events, cfg.HistoryV3, agg),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, false, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd),
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
			cfg.HistoryV3,
			dirs,
			blockReader,
			controlServer.Hd,
			cfg.Genesis,
			cfg.Sync,
			agg,
		),
		stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3),
		stagedsync.StageTrieCfg(db, checkStateRoot, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}

func NewInMemoryExecution(ctx context.Context, db kv.RwDB, cfg *ethconfig.Config, controlServer *sentry.MultiClient,
	dirs datadir.Dirs, notifications *shards.Notifications, blockReader services.FullBlockReader, blockWriter *blockio.BlockWriter, agg *state.AggregatorV3,
	logger log.Logger) (*stagedsync.Sync, error) {
	return stagedsync.New(
		stagedsync.StateStages(ctx,
			stagedsync.StageHeadersCfg(db, controlServer.Hd, controlServer.Bd, *controlServer.ChainConfig, controlServer.SendHeaderRequest, controlServer.PropagateNewBlockHashes, controlServer.Penalize, cfg.BatchSize, false, blockReader, blockWriter, dirs.Tmp, nil, nil),
			stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, *controlServer.ChainConfig, blockReader, cfg.HistoryV3, blockWriter),
			stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, true, dirs.Tmp, cfg.Prune, blockReader, controlServer.Hd),
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
				cfg.HistoryV3,
				cfg.Dirs,
				blockReader,
				controlServer.Hd,
				cfg.Genesis,
				cfg.Sync,
				agg,
			),
			stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3),
			stagedsync.StageTrieCfg(db, true, true, true, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg)),
		stagedsync.StateUnwindOrder,
		nil, /* pruneOrder */
		logger,
	), nil
}
