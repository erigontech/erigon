package stages

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/consensus"

	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/turbo/engineapi"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/bodydownload"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

func SendPayloadStatus(hd *headerdownload.HeaderDownload, headBlockHash libcommon.Hash, err error) {
	if pendingPayloadStatus := hd.GetPendingPayloadStatus(); pendingPayloadStatus != nil {
		if err != nil {
			hd.PayloadStatusCh <- engineapi.PayloadStatus{CriticalError: err}
		} else {
			hd.PayloadStatusCh <- *pendingPayloadStatus
		}
	} else if pendingPayloadHash := hd.GetPendingPayloadHash(); pendingPayloadHash != (libcommon.Hash{}) {
		if err != nil {
			hd.PayloadStatusCh <- engineapi.PayloadStatus{CriticalError: err}
		} else {
			var status remote.EngineStatus
			if headBlockHash == pendingPayloadHash {
				status = remote.EngineStatus_VALID
			} else {
				log.Warn("Failed to execute pending payload", "pendingPayload", pendingPayloadHash, "headBlock", headBlockHash)
				status = remote.EngineStatus_INVALID
			}
			hd.PayloadStatusCh <- engineapi.PayloadStatus{
				Status:          status,
				LatestValidHash: headBlockHash,
			}
		}
	}
	hd.ClearPendingPayloadHash()
	hd.SetPendingPayloadStatus(nil)
}

// StageLoop runs the continuous loop of staged sync
func StageLoop(ctx context.Context,
	db kv.RwDB,
	sync *stagedsync.Sync,
	hd *headerdownload.HeaderDownload,
	waitForDone chan struct{},
	loopMinTime time.Duration,
	logger log.Logger,
	blockSnapshots *snapshotsync.RoSnapshots,
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
		headBlockHash, err := StageLoopStep(ctx, db, sync, initialCycle, logger, blockSnapshots, hook)

		SendPayloadStatus(hd, headBlockHash, err)

		if err != nil {
			if errors.Is(err, libcommon.ErrStopped) || errors.Is(err, context.Canceled) {
				return
			}

			log.Error("Staged Sync", "err", err)
			if recoveryErr := hd.RecoverFromDb(db); recoveryErr != nil {
				log.Error("Failed to recover header sentriesClient", "err", recoveryErr)
			}
			time.Sleep(500 * time.Millisecond) // just to avoid too much similar errors in logs
			continue
		}

		initialCycle = false
		hd.AfterInitialCycle()

		if loopMinTime != 0 {
			waitTime := loopMinTime - time.Since(start)
			log.Info("Wait time until next loop", "for", waitTime)
			c := time.After(waitTime)
			select {
			case <-ctx.Done():
				return
			case <-c:
			}
		}
	}
}

func StageLoopStep(ctx context.Context, db kv.RwDB, sync *stagedsync.Sync, initialCycle bool, logger log.Logger, blockSnapshots *snapshotsync.RoSnapshots, hook *Hook) (headBlockHash libcommon.Hash, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	var finishProgressBefore, headersProgressBefore uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		if finishProgressBefore, err = stages.GetStageProgress(tx, stages.Finish); err != nil {
			return err
		}
		if headersProgressBefore, err = stages.GetStageProgress(tx, stages.Headers); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return headBlockHash, err
	}

	// Sync from scratch must be able Commit partial progress
	// In all other cases - process blocks batch in 1 RwTx
	blocksInSnapshots := uint64(0)
	if blockSnapshots != nil {
		blocksInSnapshots = blockSnapshots.BlocksAvailable()
	}
	// 2 corner-cases: when sync with --snapshots=false and when executed only blocks from snapshots (in this case all stages progress is equal and > 0, but node is not synced)
	isSynced := finishProgressBefore > 0 && finishProgressBefore > blocksInSnapshots && finishProgressBefore == headersProgressBefore
	canRunCycleInOneTransaction := true
	if initialCycle && !isSynced {
		canRunCycleInOneTransaction = false
	}

	// Main steps:
	// - process new blocks
	// - commit(no_sync). NoSync - making data available for readers as-soon-as-possible. Can
	//       send notifications Now and do write to disks Later.
	// - Send Notifications: about new blocks, new receipts, state changes, etc...
	// - Prune(limited time)+Commit(sync). Write to disk happening here.

	var tx kv.RwTx // on this variable will run sync cycle.
	if canRunCycleInOneTransaction {
		tx, err = db.BeginRwNosync(ctx)
		if err != nil {
			return headBlockHash, err
		}
		defer tx.Rollback()
	}

	if hook != nil {
		if err = hook.BeforeRun(tx, canRunCycleInOneTransaction); err != nil {
			return headBlockHash, err
		}
	}
	err = sync.Run(db, tx, initialCycle)
	if err != nil {
		return headBlockHash, err
	}
	logCtx := sync.PrintTimings()
	var tableSizes []interface{}
	var commitTime time.Duration
	if canRunCycleInOneTransaction {
		tableSizes = stagedsync.PrintTables(db, tx) // Need to do this before commit to access tx
		commitStart := time.Now()
		errTx := tx.Commit()
		if errTx != nil {
			return headBlockHash, errTx
		}
		commitTime = time.Since(commitStart)
	}

	// -- send notifications START
	var head uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		headBlockHash = rawdb.ReadHeadBlockHash(tx)
		if head, err = stages.GetStageProgress(tx, stages.Headers); err != nil {
			return err
		}
		if hook != nil {
			if err = hook.AfterRun(tx, finishProgressBefore); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return headBlockHash, err
	}
	if canRunCycleInOneTransaction && (head != finishProgressBefore || commitTime > 500*time.Millisecond) {
		logger.Info("Commit cycle", "in", commitTime)
	}
	if head != finishProgressBefore && len(logCtx) > 0 { // No printing of timings or table sizes if there were no progress
		logger.Info("Timings (slower than 50ms)", logCtx...)
		if len(tableSizes) > 0 {
			logger.Info("Tables", tableSizes...)
		}
	}
	// -- send notifications END

	// -- Prune+commit(sync)
	if err := db.Update(ctx, func(tx kv.RwTx) error { return sync.RunPrune(db, tx, initialCycle) }); err != nil {
		return headBlockHash, err
	}

	return headBlockHash, nil
}

type Hook struct {
	ctx           context.Context
	notifications *shards.Notifications
	sync          *stagedsync.Sync
	chainConfig   *chain.Config
	logger        log.Logger
	updateHead    func(ctx context.Context, headHeight uint64, headTime uint64, hash libcommon.Hash, td *uint256.Int)
}

func NewHook(ctx context.Context, notifications *shards.Notifications, sync *stagedsync.Sync, chainConfig *chain.Config, logger log.Logger, updateHead func(ctx context.Context, headHeight uint64, headTime uint64, hash libcommon.Hash, td *uint256.Int)) *Hook {
	return &Hook{ctx: ctx, notifications: notifications, sync: sync, chainConfig: chainConfig, logger: logger, updateHead: updateHead}
}
func (h *Hook) BeforeRun(tx kv.Tx, canRunCycleInOneTransaction bool) error {
	notifications := h.notifications
	if notifications != nil && notifications.Accumulator != nil && canRunCycleInOneTransaction {
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
	// -- send notifications START
	//TODO: can this 2 headers be 1
	var headHeader, currentHeder *types.Header

	// Update sentry status for peers to see our sync status
	var headTd *big.Int
	var plainStateVersion uint64
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

	// update the accumulator with a new plain state version so the cache can be notified that
	// state has moved on
	if plainStateVersion, err = rawdb.GetStateVersion(tx); err != nil {
		return err
	}
	notifications.Accumulator.SetStateID(plainStateVersion)

	if headTd != nil && headHeader != nil {
		headTd256, overflow := uint256.FromBig(headTd)
		if overflow {
			return fmt.Errorf("headTds higher than 2^256-1")
		}
		h.updateHead(h.ctx, head, headHeader.Time, headHash, headTd256)
	}

	if notifications != nil && notifications.Events != nil {
		if err = stagedsync.NotifyNewHeaders(h.ctx, finishProgressBefore, head, h.sync.PrevUnwindPoint(), notifications.Events, tx, h.logger); err != nil {
			return nil
		}
	}

	if notifications != nil && notifications.Accumulator != nil && currentHeder != nil {
		pendingBaseFee := misc.CalcBaseFee(h.chainConfig, currentHeder)
		if currentHeder.Number.Uint64() == 0 {
			notifications.Accumulator.StartChange(0, currentHeder.Hash(), nil, false)
		}

		notifications.Accumulator.SendAndReset(h.ctx, notifications.StateChangesConsumer, pendingBaseFee.Uint64(), currentHeder.GasLimit)
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

func StateStep(ctx context.Context, batch kv.RwTx, blockWriter *blockio.BlockWriter, stateSync *stagedsync.Sync, Bd *bodydownload.BodyDownload, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody) (err error) {
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
	// Once we unwound we can start constructing the chain (assumption: len(headersChain) == len(bodiesChain))
	for i := range headersChain {
		currentHeader := headersChain[i]
		currentBody := bodiesChain[i]
		currentHeight := headersChain[i].Number.Uint64()
		currentHash := headersChain[i].Hash()
		// Prepare memory state for block execution
		Bd.AddToPrefetch(currentHeader, currentBody)
		if err := blockWriter.WriteHeader(batch, currentHeader); err != nil {
			return err
		}
		if err := blockWriter.WriteCanonicalHash(batch, currentHash, currentHeight); err != nil {
			return err
		}
	}

	// If we did not specify header or body we stop here
	if header == nil {
		return nil
	}
	// Setup
	height := header.Number.Uint64()
	hash := header.Hash()
	// Prepare memory state for block execution
	if err = blockWriter.WriteHeader(batch, header); err != nil {
		return err
	}
	if err = blockWriter.WriteCanonicalHash(batch, hash, height); err != nil {
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
	if err = stateSync.Run(nil, batch, false /* firstCycle */); err != nil {
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
	snapshots *snapshotsync.RoSnapshots,
	agg *state.AggregatorV3,
	forkValidator *engineapi.ForkValidator,
	engine consensus.Engine,
	logger log.Logger,
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockReader, blockWriter := snapshotsync.NewBlockReader(snapshots, cfg.TransactionsV3), blockio.NewBlockWriter(cfg.TransactionsV3)
	blockRetire := snapshotsync.NewBlockRetire(1, dirs.Tmp, snapshots, db, snapDownloader, notifications.Events, logger)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode

	return stagedsync.DefaultStages(ctx,
		stagedsync.StageSnapshotsCfg(db,
			*controlServer.ChainConfig,
			dirs,
			snapshots,
			blockRetire,
			snapDownloader,
			blockReader,
			notifications.Events,
			engine,
			cfg.HistoryV3,
			agg,
		),
		stagedsync.StageHeadersCfg(db,
			controlServer.Hd,
			controlServer.Bd,
			*controlServer.ChainConfig,
			controlServer.SendHeaderRequest,
			controlServer.PropagateNewBlockHashes,
			controlServer.Penalize,
			cfg.BatchSize,
			p2pCfg.NoDiscovery,
			snapshots,
			blockReader,
			blockWriter,
			dirs.Tmp,
			notifications,
			forkValidator,
		),
		stagedsync.StageCumulativeIndexCfg(db),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
		stagedsync.StageBodiesCfg(
			db,
			controlServer.Bd,
			controlServer.SendBodyRequest,
			controlServer.Penalize,
			controlServer.BroadcastNewBlock,
			cfg.Sync.BodyDownloadTimeoutSeconds,
			*controlServer.ChainConfig,
			snapshots,
			blockReader,
			cfg.HistoryV3,
			cfg.TransactionsV3,
		),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, false, dirs.Tmp, cfg.Prune, blockRetire, blockWriter, controlServer.Hd),
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
		stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3, agg),
		stagedsync.StageTrieCfg(db, true, true, false, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg),
		stagedsync.StageHistoryCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageLogIndexCfg(db, cfg.Prune, dirs.Tmp),
		stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, dirs.Tmp),
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, snapshots, controlServer.ChainConfig.Bor, blockReader),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}

func NewInMemoryExecution(ctx context.Context, db kv.RwDB, cfg *ethconfig.Config, controlServer *sentry.MultiClient,
	dirs datadir.Dirs, notifications *shards.Notifications, snapshots *snapshotsync.RoSnapshots, agg *state.AggregatorV3,
	logger log.Logger) (*stagedsync.Sync, error) {
	blockReader, blockWriter := snapshotsync.NewBlockReader(snapshots, cfg.TransactionsV3), blockio.NewBlockWriter(cfg.TransactionsV3)

	return stagedsync.New(
		stagedsync.StateStages(ctx,
			stagedsync.StageHeadersCfg(
				db,
				controlServer.Hd,
				controlServer.Bd,
				*controlServer.ChainConfig,
				controlServer.SendHeaderRequest,
				controlServer.PropagateNewBlockHashes,
				controlServer.Penalize,
				cfg.BatchSize,
				false,
				snapshots,
				blockReader,
				blockWriter,
				dirs.Tmp,
				nil, nil,
			),
			stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, *controlServer.ChainConfig, snapshots, blockReader, cfg.HistoryV3, cfg.TransactionsV3),
			stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig, blockWriter),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, true, dirs.Tmp, cfg.Prune, nil, blockWriter, controlServer.Hd),
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
			stagedsync.StageHashStateCfg(db, dirs, cfg.HistoryV3, agg),
			stagedsync.StageTrieCfg(db, true, true, true, dirs.Tmp, blockReader, controlServer.Hd, cfg.HistoryV3, agg)),
		stagedsync.StateUnwindOrder,
		nil, /* pruneOrder */
		logger,
	), nil
}
