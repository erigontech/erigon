package stages

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/dbg"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/memdb"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/log/v3"
)

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db kv.RwDB,
	sync *stagedsync.Sync,
	hd *headerdownload.HeaderDownload,
	notifications *stagedsync.Notifications,
	updateHead func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int),
	waitForDone chan struct{},
	loopMinTime time.Duration,
) {
	defer close(waitForDone)
	initialCycle := true

	for {
		start := time.Now()

		// Estimate the current top height seen from the peer
		height := hd.TopSeenHeight()
		headBlockHash, err := StageLoopStep(ctx, db, sync, height, notifications, initialCycle, updateHead, nil)

		pendingPayloadStatus := hd.GetPendingPayloadStatus()
		if pendingPayloadStatus != (common.Hash{}) {
			if err != nil {
				hd.PayloadStatusCh <- privateapi.PayloadStatus{CriticalError: err}
			} else {
				var status remote.EngineStatus
				if headBlockHash == pendingPayloadStatus {
					status = remote.EngineStatus_VALID
				} else {
					status = remote.EngineStatus_INVALID
				}
				hd.PayloadStatusCh <- privateapi.PayloadStatus{
					Status:          status,
					LatestValidHash: headBlockHash,
				}
			}

			hd.ClearPendingPayloadStatus()
		}

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
		hd.EnableRequestChaining()

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

func StageLoopStep(
	ctx context.Context,
	db kv.RwDB,
	sync *stagedsync.Sync,
	highestSeenHeader uint64,
	notifications *stagedsync.Notifications,
	initialCycle bool,
	updateHead func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int),
	snapshotMigratorFinal func(tx kv.Tx) error,
) (headBlockHash common.Hash, err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	var origin, finishProgressBefore uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		origin, err = stages.GetStageProgress(tx, stages.Headers)
		if err != nil {
			return err
		}
		finishProgressBefore, err = stages.GetStageProgress(tx, stages.Finish)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return headBlockHash, err
	}

	canRunCycleInOneTransaction := !initialCycle && highestSeenHeader < origin+8096 && highestSeenHeader < finishProgressBefore+8096

	var tx kv.RwTx // on this variable will run sync cycle.
	if canRunCycleInOneTransaction {
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return headBlockHash, err
		}
		defer tx.Rollback()
	}

	if notifications != nil && notifications.Accumulator != nil && canRunCycleInOneTransaction {
		notifications.Accumulator.Reset(tx.ViewID())
	}

	err = sync.Run(db, tx, initialCycle)
	if err != nil {
		return headBlockHash, err
	}
	if canRunCycleInOneTransaction {
		commitStart := time.Now()
		errTx := tx.Commit()
		if errTx != nil {
			return headBlockHash, errTx
		}
		log.Info("Commit cycle", "in", time.Since(commitStart))
	}
	var rotx kv.Tx
	if rotx, err = db.BeginRo(ctx); err != nil {
		return headBlockHash, err
	}
	defer rotx.Rollback()

	// Update sentry status for peers to see our sync status
	var headTd *big.Int
	var head uint64
	var headHash common.Hash
	if head, err = stages.GetStageProgress(rotx, stages.Headers); err != nil {
		return headBlockHash, err
	}
	if headHash, err = rawdb.ReadCanonicalHash(rotx, head); err != nil {
		return headBlockHash, err
	}
	if headTd, err = rawdb.ReadTd(rotx, headHash, head); err != nil {
		return headBlockHash, err
	}
	headBlockHash = rawdb.ReadHeadBlockHash(rotx)

	if canRunCycleInOneTransaction && snapshotMigratorFinal != nil {
		err = snapshotMigratorFinal(rotx)
		if err != nil {
			log.Error("snapshot migration failed", "err", err)
		}
	}

	headTd256, overflow := uint256.FromBig(headTd)
	if overflow {
		return headBlockHash, fmt.Errorf("headTds higher than 2^256-1")
	}
	updateHead(ctx, head, headHash, headTd256)

	if notifications != nil && notifications.Accumulator != nil {
		header := rawdb.ReadCurrentHeader(rotx)
		if header != nil && header.Number.Uint64() != finishProgressBefore {

			pendingBaseFee := misc.CalcBaseFee(notifications.Accumulator.ChainConfig(), header)
			if header.Number.Uint64() == 0 {
				notifications.Accumulator.StartChange(0, header.Hash(), nil, false)
			}
			notifications.Accumulator.SendAndReset(ctx, notifications.StateChangesConsumer, pendingBaseFee.Uint64(), header.GasLimit)

			if err = stagedsync.NotifyNewHeaders(ctx, finishProgressBefore, head, sync.PrevUnwindPoint(), notifications.Events, rotx); err != nil {
				return headBlockHash, nil
			}
		}
	}

	return headBlockHash, nil
}

func MiningStep(ctx context.Context, kv kv.RwDB, mining *stagedsync.Sync) (err error) {
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

	miningBatch := memdb.NewMemoryBatch(tx)
	defer miningBatch.Rollback()

	if err = mining.Run(nil, miningBatch, false); err != nil {
		return err
	}
	tx.Rollback()
	return nil
}

func StateStep(ctx context.Context, batch kv.RwTx, stateSync *stagedsync.Sync, headerReader services.FullBlockReader, header *types.Header, body *types.RawBody) (err error) {
	// Setup
	height := header.Number.Uint64()
	hash := header.Hash()

	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("%+v, trace: %s", rec, dbg.Stack())
		}
	}() // avoid crash because Erigon's core does many things

	// Prepare memory state for block execution
	if err = rawdb.WriteRawBodyIfNotExists(batch, hash, height, body); err != nil {
		return err
	}

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

	if err = stages.SaveStageProgress(batch, stages.Bodies, height); err != nil {
		return err
	}

	if height == 0 {
		return nil
	}
	ancestorHash := hash
	ancestorHeight := height

	var ch common.Hash
	for ch, err = headerReader.CanonicalHash(context.Background(), batch, ancestorHeight); err == nil && ch != ancestorHash; ch, err = headerReader.CanonicalHash(context.Background(), batch, ancestorHeight) {
		if err = rawdb.WriteCanonicalHash(batch, ancestorHash, ancestorHeight); err != nil {
			return fmt.Errorf("marking canonical header %d %x: %w", ancestorHeight, ancestorHash, err)
		}

		ancestor, err := headerReader.Header(context.Background(), batch, ancestorHash, ancestorHeight)
		if err != nil {
			return err
		}
		if ancestor == nil {
			return fmt.Errorf("ancestor is nil. height %d, hash %x", ancestorHeight, ancestorHash)
		}

		select {
		default:
		}
		ancestorHash = ancestor.ParentHash
		ancestorHeight--
	}
	if err != nil {
		return fmt.Errorf("reading canonical hash for %d: %w", ancestorHeight, err)
	}

	// Run state sync
	if err = stateSync.Run(nil, batch, false); err != nil {
		return err
	}
	return nil
}

func NewStagedSync(
	ctx context.Context,
	logger log.Logger,
	db kv.RwDB,
	p2pCfg p2p.Config,
	cfg ethconfig.Config,
	controlServer *sentry.MultiClient,
	tmpdir string,
	notifications *stagedsync.Notifications,
	snapshotDownloader proto_downloader.DownloaderClient,
	snapshots *snapshotsync.RoSnapshots,
	headCh chan *types.Block,
	execPayload stagedsync.ExecutePayloadFunc,
) (*stagedsync.Sync, error) {
	var blockReader services.FullBlockReader
	if cfg.Snapshot.Enabled {
		blockReader = snapshotsync.NewBlockReaderWithSnapshots(snapshots)
	} else {
		blockReader = snapshotsync.NewBlockReader()
	}
	blockRetire := snapshotsync.NewBlockRetire(1, tmpdir, snapshots, db, snapshotDownloader, notifications.Events)

	// During Import we don't want other services like header requests, body requests etc. to be running.
	// Hence we run it in the test mode.
	runInTestMode := cfg.ImportMode
	isBor := controlServer.ChainConfig.Bor != nil
	return stagedsync.New(
		stagedsync.DefaultStages(ctx, cfg.Prune,
			stagedsync.StageHeadersCfg(
				db,
				controlServer.Hd,
				controlServer.Bd,
				*controlServer.ChainConfig,
				controlServer.SendHeaderRequest,
				controlServer.PropagateNewBlockHashes,
				controlServer.Penalize,
				cfg.BatchSize,
				p2pCfg.NoDiscovery,
				cfg.MemoryOverlay,
				snapshots,
				snapshotDownloader,
				blockReader,
				tmpdir,
				notifications.Events,
				execPayload),
			stagedsync.StageCumulativeIndexCfg(db),
			stagedsync.StageBlockHashesCfg(db, tmpdir, controlServer.ChainConfig),
			stagedsync.StageBodiesCfg(
				db,
				controlServer.Bd,
				controlServer.SendBodyRequest,
				controlServer.Penalize,
				controlServer.BroadcastNewBlock,
				cfg.Sync.BodyDownloadTimeoutSeconds,
				*controlServer.ChainConfig,
				cfg.BatchSize,
				snapshots,
				blockReader,
			),
			stagedsync.StageIssuanceCfg(db, controlServer.ChainConfig, blockReader, cfg.EnabledIssuance),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, tmpdir, cfg.Prune, blockRetire),
			stagedsync.StageExecuteBlocksCfg(
				db,
				cfg.Prune,
				cfg.BatchSize,
				nil,
				controlServer.ChainConfig,
				controlServer.Engine,
				&vm.Config{EnableTEMV: cfg.Prune.Experiments.TEVM},
				notifications.Accumulator,
				cfg.StateStream,
				/*stateStream=*/ false,
				tmpdir,
				blockReader,
			),
			stagedsync.StageTranspileCfg(db, cfg.BatchSize, controlServer.ChainConfig),
			stagedsync.StageHashStateCfg(db, tmpdir),
			stagedsync.StageTrieCfg(db, true, true, tmpdir, blockReader),
			stagedsync.StageHistoryCfg(db, cfg.Prune, tmpdir),
			stagedsync.StageLogIndexCfg(db, cfg.Prune, tmpdir),
			stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, tmpdir),
			stagedsync.StageTxLookupCfg(db, cfg.Prune, tmpdir, snapshots, isBor),
			stagedsync.StageFinishCfg(db, tmpdir, logger, headCh), runInTestMode),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
	), nil
}

func NewInMemoryExecution(
	ctx context.Context,
	logger log.Logger,
	db kv.RwDB,
	p2pCfg p2p.Config,
	cfg ethconfig.Config,
	controlServer *sentry.MultiClient,
	tmpdir string,
	notifications *stagedsync.Notifications,
	snapshotDownloader proto_downloader.DownloaderClient,
	snapshots *snapshotsync.RoSnapshots,
	headCh chan *types.Block,
) (*stagedsync.Sync, error) {
	var blockReader services.FullBlockReader
	if cfg.Snapshot.Enabled {
		blockReader = snapshotsync.NewBlockReaderWithSnapshots(snapshots)
	} else {
		blockReader = snapshotsync.NewBlockReader()
	}

	return stagedsync.New(
		stagedsync.StateStages(ctx,
			stagedsync.StageBlockHashesCfg(db, tmpdir, controlServer.ChainConfig),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, tmpdir, cfg.Prune, nil),
			stagedsync.StageExecuteBlocksCfg(
				db,
				cfg.Prune,
				cfg.BatchSize,
				nil,
				controlServer.ChainConfig,
				controlServer.Engine,
				&vm.Config{EnableTEMV: cfg.Prune.Experiments.TEVM},
				notifications.Accumulator,
				cfg.StateStream,
				true,
				tmpdir,
				blockReader,
			),
			stagedsync.StageHashStateCfg(db, tmpdir),
			stagedsync.StageTrieCfg(db, true, true, tmpdir, blockReader)),
		nil,
		nil,
	), nil
}
