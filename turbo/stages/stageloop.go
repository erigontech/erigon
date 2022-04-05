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
	"github.com/ledgerwatch/erigon-lib/common/dir"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus/misc"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/p2p"
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
		select {
		case <-ctx.Done():
			return
		default:
		}

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
				log.Error("Failed to recover header downloader", "err", recoveryErr)
			}
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

	canRunCycleInOneTransaction := !initialCycle && highestSeenHeader-origin < 8096 && highestSeenHeader-finishProgressBefore < 8096

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
	rotx.Rollback()

	headTd256, overflow := uint256.FromBig(headTd)
	if overflow {
		return headBlockHash, fmt.Errorf("headTds higher than 2^256-1")
	}
	updateHead(ctx, head, headHash, headTd256)

	if notifications != nil && notifications.Accumulator != nil {
		if err := db.View(ctx, func(tx kv.Tx) error {
			header := rawdb.ReadCurrentHeader(tx)
			if header == nil {
				return nil
			}

			pendingBaseFee := misc.CalcBaseFee(notifications.Accumulator.ChainConfig(), header)
			if header.Number.Uint64() == 0 {
				notifications.Accumulator.StartChange(0, header.Hash(), nil, false)
			}
			notifications.Accumulator.SendAndReset(ctx, notifications.StateChangesConsumer, pendingBaseFee.Uint64(), header.GasLimit)

			return stagedsync.NotifyNewHeaders(ctx, finishProgressBefore, head, sync.PrevUnwindPoint(), notifications.Events, tx)
		}); err != nil {
			return headBlockHash, err
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

	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = mining.Run(nil, tx, false); err != nil {
		return err
	}
	tx.Rollback()
	return nil
}

func NewStagedSync(
	ctx context.Context,
	logger log.Logger,
	db kv.RwDB,
	p2pCfg p2p.Config,
	cfg ethconfig.Config,
	terminalTotalDifficulty *big.Int,
	controlServer *sentry.ControlServerImpl,
	tmpdir string,
	notifications *stagedsync.Notifications,
	snapshotDownloader proto_downloader.DownloaderClient,
	snapshots *snapshotsync.RoSnapshots,
	snapshotDir *dir.Rw,
) (*stagedsync.Sync, error) {
	var blockReader interfaces.FullBlockReader
	if cfg.Snapshot.Enabled {
		blockReader = snapshotsync.NewBlockReaderWithSnapshots(snapshots)
	} else {
		blockReader = snapshotsync.NewBlockReader()
	}
	blockRetire := snapshotsync.NewBlockRetire(1, tmpdir, snapshots, snapshotDir, db, snapshotDownloader, notifications.Events)

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
				snapshots,
				snapshotDownloader,
				blockReader,
				tmpdir,
				cfg.SnapshotDir,
			),
			stagedsync.StageCumulativeIndexCfg(db),
			stagedsync.StageBlockHashesCfg(db, tmpdir, controlServer.ChainConfig),
			stagedsync.StageBodiesCfg(
				db,
				controlServer.Bd,
				controlServer.SendBodyRequest,
				controlServer.Penalize,
				controlServer.BroadcastNewBlock,
				cfg.BodyDownloadTimeoutSeconds,
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
			stagedsync.StageFinishCfg(db, tmpdir, logger), runInTestMode),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
	), nil
}
