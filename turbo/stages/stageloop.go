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
func StageLoop(
	ctx context.Context,
	chainConfig *chain.Config,
	db kv.RwDB,
	sync *stagedsync.Sync,
	hd *headerdownload.HeaderDownload,
	notifications *shards.Notifications,
	updateHead func(ctx context.Context, headHeight, headTime uint64, hash libcommon.Hash, td *uint256.Int),
	waitForDone chan struct{},
	loopMinTime time.Duration,
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
		headBlockHash, err := StageLoopStep(ctx, chainConfig, db, sync, notifications, initialCycle, updateHead)

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

func StageLoopStep(ctx context.Context, chainConfig *chain.Config, db kv.RwDB, sync *stagedsync.Sync, notifications *shards.Notifications, initialCycle bool,
	updateHead func(ctx context.Context, headHeight uint64, headTime uint64, hash libcommon.Hash, td *uint256.Int),
) (headBlockHash libcommon.Hash, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	var finishProgressBefore uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		finishProgressBefore, err = stages.GetStageProgress(tx, stages.Finish)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return headBlockHash, err
	}
	canRunCycleInOneTransaction := !initialCycle

	var tx kv.RwTx // on this variable will run sync cycle.
	if canRunCycleInOneTransaction {
		// -- Process new blocks + commit(no_sync)
		tx, err = db.BeginRwNosync(ctx)
		if err != nil {
			return headBlockHash, err
		}
		defer tx.Rollback()
	}

	if notifications != nil && notifications.Accumulator != nil && canRunCycleInOneTransaction {
		stateVersion, err := rawdb.GetStateVersion(tx)
		if err != nil {
			log.Error("problem reading plain state version", "err", err)
		}
		notifications.Accumulator.Reset(stateVersion)
	}

	err = sync.Run(db, tx, initialCycle, false /* quiet */)
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
	//TODO: can this 2 headers be 1
	var headHeader, currentHeder *types.Header
	if err := db.View(ctx, func(tx kv.Tx) error {
		// Update sentry status for peers to see our sync status
		var headTd *big.Int
		var head uint64
		var headHash libcommon.Hash
		var plainStateVersion uint64
		if head, err = stages.GetStageProgress(tx, stages.Headers); err != nil {
			return err
		}
		if headHash, err = rawdb.ReadCanonicalHash(tx, head); err != nil {
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

		if canRunCycleInOneTransaction && (head != finishProgressBefore || commitTime > 500*time.Millisecond) {
			log.Info("Commit cycle", "in", commitTime)
		}
		if head != finishProgressBefore && len(logCtx) > 0 { // No printing of timings or table sizes if there were no progress
			log.Info("Timings (slower than 50ms)", logCtx...)
			if len(tableSizes) > 0 {
				log.Info("Tables", tableSizes...)
			}
		}

		if headTd != nil && headHeader != nil {
			headTd256, overflow := uint256.FromBig(headTd)
			if overflow {
				return fmt.Errorf("headTds higher than 2^256-1")
			}
			updateHead(ctx, head, headHeader.Time, headHash, headTd256)
		}

		if notifications != nil && notifications.Events != nil {
			if err = stagedsync.NotifyNewHeaders(ctx, finishProgressBefore, head, sync.PrevUnwindPoint(), notifications.Events, tx); err != nil {
				return nil
			}
		}

		headBlockHash = rawdb.ReadHeadBlockHash(tx)
		return nil
	}); err != nil {
		return headBlockHash, err
	}
	if notifications != nil && notifications.Accumulator != nil && currentHeder != nil {
		pendingBaseFee := misc.CalcBaseFee(chainConfig, currentHeder)
		if currentHeder.Number.Uint64() == 0 {
			notifications.Accumulator.StartChange(0, currentHeder.Hash(), nil, false)
		}

		notifications.Accumulator.SendAndReset(ctx, notifications.StateChangesConsumer, pendingBaseFee.Uint64(), currentHeder.GasLimit)
	}
	// -- send notifications END

	// -- Prune+commit(sync)
	if err := db.Update(ctx, func(tx kv.RwTx) error { return sync.RunPrune(db, tx, initialCycle) }); err != nil {
		return headBlockHash, err
	}

	return headBlockHash, nil
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

	if err = mining.Run(nil, miningBatch, false /* firstCycle */, false /* quiet */); err != nil {
		return err
	}
	tx.Rollback()
	return nil
}

func StateStep(ctx context.Context, batch kv.RwTx, stateSync *stagedsync.Sync, Bd *bodydownload.BodyDownload, header *types.Header, body *types.RawBody, unwindPoint uint64, headersChain []*types.Header, bodiesChain []*types.RawBody, quiet bool) (err error) {
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
		rawdb.WriteHeader(batch, currentHeader)
		if err = rawdb.WriteHeaderNumber(batch, currentHash, currentHeight); err != nil {
			return err
		}
		if err = rawdb.WriteCanonicalHash(batch, currentHash, currentHeight); err != nil {
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
	rawdb.WriteHeader(batch, header)
	if err = rawdb.WriteHeaderNumber(batch, hash, height); err != nil {
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
	if err = stateSync.Run(nil, batch, false /* firstCycle */, quiet); err != nil {
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
) []*stagedsync.Stage {
	dirs := cfg.Dirs
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(snapshots, cfg.TransactionsV3)
	blockRetire := snapshotsync.NewBlockRetire(1, dirs.Tmp, snapshots, db, snapDownloader, notifications.Events)

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
			dirs.Tmp,
			notifications,
			forkValidator,
		),
		stagedsync.StageCumulativeIndexCfg(db),
		stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig),
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
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, false, dirs.Tmp, cfg.Prune, blockRetire, controlServer.Hd),
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
		stagedsync.StageTxLookupCfg(db, cfg.Prune, dirs.Tmp, snapshots, controlServer.ChainConfig.Bor),
		stagedsync.StageFinishCfg(db, dirs.Tmp, forkValidator),
		runInTestMode)
}

func NewInMemoryExecution(ctx context.Context, db kv.RwDB, cfg *ethconfig.Config, controlServer *sentry.MultiClient,
	dirs datadir.Dirs, notifications *shards.Notifications, snapshots *snapshotsync.RoSnapshots, agg *state.AggregatorV3,
	logger log.Logger) (*stagedsync.Sync, error) {
	blockReader := snapshotsync.NewBlockReaderWithSnapshots(snapshots, cfg.TransactionsV3)

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
				dirs.Tmp,
				nil, nil,
			),
			stagedsync.StageBodiesCfg(db, controlServer.Bd, controlServer.SendBodyRequest, controlServer.Penalize, controlServer.BroadcastNewBlock, cfg.Sync.BodyDownloadTimeoutSeconds, *controlServer.ChainConfig, snapshots, blockReader, cfg.HistoryV3, cfg.TransactionsV3),
			stagedsync.StageBlockHashesCfg(db, dirs.Tmp, controlServer.ChainConfig),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, true, dirs.Tmp, cfg.Prune, nil, controlServer.Hd),
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
