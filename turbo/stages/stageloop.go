package stages

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/holiman/uint256"
	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/shards"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
	"github.com/ledgerwatch/erigon/turbo/txpool"
)

func NewStagedSync(
	ctx context.Context,
	sm ethdb.StorageMode,
	headers stagedsync.HeadersCfg,
	blockHashes stagedsync.BlockHashesCfg,
	snapshotHeaderGen stagedsync.HeadersSnapshotGenCfg,
	bodies stagedsync.BodiesCfg,
	senders stagedsync.SendersCfg,
	exec stagedsync.ExecuteBlockCfg,
	trans stagedsync.TranspileCfg,
	hashState stagedsync.HashStateCfg,
	trieCfg stagedsync.TrieCfg,
	history stagedsync.HistoryCfg,
	logIndex stagedsync.LogIndexCfg,
	callTraces stagedsync.CallTracesCfg,
	txLookup stagedsync.TxLookupCfg,
	txPool stagedsync.TxPoolCfg,
	finish stagedsync.FinishCfg,
	test bool,
) *stagedsync.StagedSync {
	return stagedsync.New(
		stagedsync.ReplacementStages(ctx, sm, headers, blockHashes, snapshotHeaderGen, bodies, senders, exec, trans, hashState, trieCfg, history, logIndex, callTraces, txLookup, txPool, finish, test),
		stagedsync.ReplacementUnwindOrder(),
		stagedsync.OptionalParameters{},
	)
}

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db ethdb.RwKV,
	sync *stagedsync.StagedSync,
	hd *headerdownload.HeaderDownload,
	chainConfig *params.ChainConfig,
	notifier stagedsync.ChainEventNotifier,
	stateStream bool,
	updateHead func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int),
	waitForDone chan struct{},
) {
	defer close(waitForDone)
	initialCycle := true

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Estimate the current top height seen from the peer
		height := hd.TopSeenHeight()
		var accumulator *shards.Accumulator
		if !initialCycle && stateStream {
			accumulator = &shards.Accumulator{}
		}
		if err := StageLoopStep(ctx, db, sync, height, chainConfig, notifier, initialCycle, accumulator, updateHead, sync.GetSnapshotMigratorFinal()); err != nil {
			if errors.Is(err, common.ErrStopped) {
				return
			}

			log.Error("Stage loop failure", "error", err)
			if recoveryErr := hd.RecoverFromDb(db); recoveryErr != nil {
				log.Error("Failed to recover header downloader", "error", recoveryErr)
			}
			continue
		}

		initialCycle = false
		hd.EnableRequestChaining()
	}
}

func StageLoopStep(
	ctx context.Context,
	db ethdb.RwKV,
	sync *stagedsync.StagedSync,
	highestSeenHeader uint64,
	chainConfig *params.ChainConfig,
	notifier stagedsync.ChainEventNotifier,
	initialCycle bool,
	accumulator *shards.Accumulator,
	updateHead func(ctx context.Context, head uint64, hash common.Hash, td *uint256.Int),
	snapshotMigratorFinal func(tx ethdb.Tx) error,
) (err error) {
	// avoid crash because Erigon's core does many things -
	defer func() { err = debug.LogPanic(err, false, recover()) }()
	var sm ethdb.StorageMode
	var origin, hashStateStageProgress, finishProgressBefore, unwindTo uint64
	if err := db.View(ctx, func(tx ethdb.Tx) error {
		sm, err = ethdb.GetStorageModeFromDB(tx)
		if err != nil {
			return err
		}
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
		var v []byte
		v, err = tx.GetOne(dbutils.SyncStageUnwind, []byte(stages.Finish))
		if err != nil {
			return err
		}
		if len(v) > 0 {
			unwindTo = binary.BigEndian.Uint64(v)
		}

		return nil
	}); err != nil {
		return err
	}

	st, err1 := sync.Prepare(nil, chainConfig, nil, &vm.Config{}, ethdb.NewObjectDatabase(db), nil, "downloader", sm, ".", 512*datasize.MB, ctx.Done(), nil, nil, initialCycle, nil, accumulator)
	if err1 != nil {
		return fmt.Errorf("prepare staged sync: %w", err1)
	}

	canRunCycleInOneTransaction := !initialCycle && highestSeenHeader-origin < 1024 && highestSeenHeader-hashStateStageProgress < 1024

	var tx ethdb.RwTx // on this variable will run sync cycle.
	if canRunCycleInOneTransaction {
		tx, err = db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	err = st.Run(db, tx)
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
	var rotx ethdb.Tx
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

	err = stagedsync.NotifyNewHeaders(ctx, finishProgressBefore, unwindTo, notifier, db)
	if err != nil {
		return err
	}

	return nil
}

func MiningStep(ctx context.Context, kv ethdb.RwKV, mining *stagedsync.StagedSync) (err error) {
	// avoid crash because TG's core does many things -
	defer func() { err = debug.LogPanic(err, false, recover()) }()

	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	miningState, err := mining.Prepare(
		nil,
		nil,
		nil,
		nil,
		nil,
		tx,
		"",
		ethdb.DefaultStorageMode,
		".",
		0,
		ctx.Done(),
		nil,
		nil,
		false,
		stagedsync.StageMiningCfg(true),
		nil,
	)
	if err != nil {
		return err
	}
	if err = miningState.Run(nil, tx); err != nil {
		return err
	}
	tx.Rollback()
	return nil
}

func NewStagedSync2(
	ctx context.Context,
	db ethdb.RwKV,
	sm ethdb.StorageMode,
	batchSize datasize.ByteSize,
	bodyDownloadTimeout int,
	controlServer *download.ControlServerImpl,
	tmpdir string,
	snapshotsDir string,
	txPool *core.TxPool,
	txPoolServer *txpool.P2PServer,
) (*stagedsync.StagedSync, error) {
	var pruningDistance uint64
	if !sm.History {
		pruningDistance = params.FullImmutabilityThreshold
	}

	return NewStagedSync(ctx, sm,
		stagedsync.StageHeadersCfg(
			db,
			controlServer.Hd,
			*controlServer.ChainConfig,
			controlServer.SendHeaderRequest,
			controlServer.PropagateNewBlockHashes,
			controlServer.Penalize,
			batchSize,
		),
		stagedsync.StageBlockHashesCfg(db, tmpdir),
		stagedsync.StageHeadersSnapshotGenCfg(db, snapshotsDir),
		stagedsync.StageBodiesCfg(
			db,
			controlServer.Bd,
			controlServer.SendBodyRequest,
			controlServer.Penalize,
			controlServer.BroadcastNewBlock,
			bodyDownloadTimeout,
			*controlServer.ChainConfig,
			batchSize,
		),
		stagedsync.StageSendersCfg(db, controlServer.ChainConfig, tmpdir),
		stagedsync.StageExecuteBlocksCfg(
			db,
			sm.Receipts,
			sm.CallTraces,
			sm.TEVM,
			pruningDistance,
			batchSize,
			nil,
			controlServer.ChainConfig,
			controlServer.Engine,
			&vm.Config{NoReceipts: !sm.Receipts, EnableTEMV: sm.TEVM},
			tmpdir,
		),
		stagedsync.StageTranspileCfg(
			db,
			batchSize,
			nil,
			nil,
			controlServer.ChainConfig,
		),
		stagedsync.StageHashStateCfg(db, tmpdir),
		stagedsync.StageTrieCfg(db, true, true, tmpdir),
		stagedsync.StageHistoryCfg(db, tmpdir),
		stagedsync.StageLogIndexCfg(db, tmpdir),
		stagedsync.StageCallTracesCfg(db, 0, batchSize, tmpdir, controlServer.ChainConfig, controlServer.Engine),
		stagedsync.StageTxLookupCfg(db, tmpdir),
		stagedsync.StageTxPoolCfg(db, txPool, func() {
			for _, s := range txPoolServer.Sentries {
				go txpool.RecvTxMessageLoop(ctx, s, controlServer, txPoolServer.HandleInboundMessage, nil)
			}
			txPoolServer.TxFetcher.Start()
		}),
		stagedsync.StageFinishCfg(db, tmpdir),
		false, /* test */
	), nil
}
