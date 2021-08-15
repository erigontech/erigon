package stages

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/txpool"
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
		if err := StageLoopStep(ctx, db, sync, height, notifications, initialCycle, updateHead, nil); err != nil {
			if errors.Is(err, common.ErrStopped) {
				return
			}

			log.Error("Staged Sync", "error", err)
			if recoveryErr := hd.RecoverFromDb(db); recoveryErr != nil {
				log.Error("Failed to recover header downloader", "error", recoveryErr)
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
) (err error) {
	defer func() { err = debug.ReportPanicAndRecover(err) }() // avoid crash because Erigon's core does many things -
	var origin, hashStateStageProgress, finishProgressBefore uint64
	if err := db.View(ctx, func(tx kv.Tx) error {
		origin, err = stages.GetStageProgress(tx, stages.Headers)
		if err != nil {
			return err
		}
		hashStateStageProgress, err = stages.GetStageProgress(tx, stages.Bodies) // TODO: shift this when more stages are added
		if err != nil {
			return err
		}
		finishProgressBefore, err = stages.GetStageProgress(tx, stages.Finish) // TODO: shift this when more stages are added
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	if notifications != nil && notifications.Accumulator != nil {
		notifications.Accumulator.Reset()
	}

	canRunCycleInOneTransaction := !initialCycle && highestSeenHeader-origin < 1024 && highestSeenHeader-hashStateStageProgress < 1024

	var tx kv.RwTx // on this variable will run sync cycle.
	if canRunCycleInOneTransaction {
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	err = sync.Run(db, tx, initialCycle)
	if err != nil {
		return err
	}
	if canRunCycleInOneTransaction {
		commitStart := time.Now()
		errTx := tx.Commit()
		if errTx != nil {
			return errTx
		}
		log.Info("Commit cycle", "in", time.Since(commitStart))
	}
	var rotx kv.Tx
	if rotx, err = db.BeginRo(ctx); err != nil {
		return err
	}
	defer rotx.Rollback()

	// Update sentry status for peers to see our sync status
	var headTd *big.Int
	var head uint64
	var headHash common.Hash
	if head, err = stages.GetStageProgress(rotx, stages.Finish); err != nil {
		return err
	}
	if headHash, err = rawdb.ReadCanonicalHash(rotx, head); err != nil {
		return err
	}
	if headTd, err = rawdb.ReadTd(rotx, headHash, head); err != nil {
		return err
	}

	if canRunCycleInOneTransaction && snapshotMigratorFinal != nil {
		err = snapshotMigratorFinal(rotx)
		if err != nil {
			log.Error("snapshot migration failed", "err", err)
		}
	}
	rotx.Rollback()
	headTd256 := new(uint256.Int)
	overflow := headTd256.SetFromBig(headTd)
	if overflow {
		return fmt.Errorf("headTds higher than 2^256-1")
	}
	updateHead(ctx, head, headHash, headTd256)

	err = stagedsync.NotifyNewHeaders(ctx, finishProgressBefore, sync.PrevUnwindPoint(), notifications.Events, db)
	if err != nil {
		return err
	}

	notifications.Accumulator.SendAndReset(ctx, notifications.StateChangesConsumer)
	return nil
}

func MiningStep(ctx context.Context, kv kv.RwDB, mining *stagedsync.Sync) (err error) {
	defer func() { err = debug.ReportPanicAndRecover(err) }() // avoid crash because Erigon's core does many things -

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
	controlServer *download.ControlServerImpl,
	tmpdir string,
	txPool *core.TxPool,
	txPoolServer *txpool.P2PServer,

	client *snapshotsync.Client,
	snapshotMigrator *snapshotsync.SnapshotMigrator,
	accumulator *shards.Accumulator,
) (*stagedsync.Sync, error) {
	return stagedsync.New(
		stagedsync.DefaultStages(
			ctx,
			cfg.Prune,
			stagedsync.StageHeadersCfg(
				db,
				controlServer.Hd,
				*controlServer.ChainConfig,
				controlServer.SendHeaderRequest,
				controlServer.PropagateNewBlockHashes,
				controlServer.Penalize,
				cfg.BatchSize,
				p2pCfg.NoDiscovery,
			),
			stagedsync.StageBlockHashesCfg(db, tmpdir),
			stagedsync.StageSnapshotHeadersCfg(db, cfg.Snapshot, client, snapshotMigrator, logger),
			stagedsync.StageBodiesCfg(
				db,
				controlServer.Bd,
				controlServer.SendBodyRequest,
				controlServer.Penalize,
				controlServer.BroadcastNewBlock,
				cfg.BodyDownloadTimeoutSeconds,
				*controlServer.ChainConfig,
				cfg.BatchSize,
			),
			stagedsync.StageSnapshotBodiesCfg(db, cfg.Snapshot, client, snapshotMigrator, tmpdir),
			stagedsync.StageSendersCfg(db, controlServer.ChainConfig, tmpdir),
			stagedsync.StageExecuteBlocksCfg(
				db,
				cfg.Prune,
				cfg.BatchSize,
				nil,
				controlServer.ChainConfig,
				controlServer.Engine,
				&vm.Config{EnableTEMV: cfg.Prune.Experiments.TEVM},
				accumulator,
				cfg.StateStream,
				tmpdir,
			),
			stagedsync.StageTranspileCfg(
				db,
				cfg.BatchSize,
				controlServer.ChainConfig,
			),
			stagedsync.StageSnapshotStateCfg(db, cfg.Snapshot, tmpdir, client, snapshotMigrator),
			stagedsync.StageHashStateCfg(db, tmpdir),
			stagedsync.StageTrieCfg(db, true, true, tmpdir),
			stagedsync.StageHistoryCfg(db, cfg.Prune, tmpdir),
			stagedsync.StageLogIndexCfg(db, cfg.Prune, tmpdir),
			stagedsync.StageCallTracesCfg(db, cfg.Prune, 0, tmpdir),
			stagedsync.StageTxLookupCfg(db, cfg.Prune, tmpdir),
			stagedsync.StageTxPoolCfg(db, txPool, func() {
				for i := range txPoolServer.Sentries {
					go func(i int) {
						txpool.RecvTxMessageLoop(ctx, txPoolServer.Sentries[i], txPoolServer.HandleInboundMessage, nil)
					}(i)
					go func(i int) {
						txpool.RecvPeersLoop(ctx, txPoolServer.Sentries[i], txPoolServer.RecentPeers, nil)
					}(i)
				}
				txPoolServer.TxFetcher.Start()
			}),
			stagedsync.StageFinishCfg(db, tmpdir, client, snapshotMigrator, logger),
			false, /* test */
		),
		stagedsync.DefaultUnwindOrder,
		stagedsync.DefaultPruneOrder,
	), nil
}
