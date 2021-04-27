package stages

import (
	"context"
	"fmt"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
)

const (
	logInterval = 30 * time.Second
)

func NewStagedSync(
	ctx context.Context,
	sm ethdb.StorageMode,
	headers stagedsync.HeadersCfg,
	bodies stagedsync.BodiesCfg,
	senders stagedsync.SendersCfg,
	exec stagedsync.ExecuteBlockCfg,
	hashState stagedsync.HashStateCfg,
	trieCfg stagedsync.TrieCfg,
	history stagedsync.HistoryCfg,
	logIndex stagedsync.LogIndexCfg,
	callTraces stagedsync.CallTracesCfg,
	txLookup stagedsync.TxLookupCfg,
	txPool stagedsync.TxPoolCfg,
) *stagedsync.StagedSync {
	return stagedsync.New(
		stagedsync.ReplacementStages(ctx, sm, headers, bodies, senders, exec, hashState, trieCfg, history, logIndex, callTraces, txLookup, txPool),
		stagedsync.ReplacementUnwindOrder(),
		stagedsync.OptionalParameters{Notifier: remotedbserver.NewEvents()},
	)
}

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db ethdb.Database,
	sync *stagedsync.StagedSync,
	hd *headerdownload.HeaderDownload,
	chainConfig *params.ChainConfig,
) error {
	initialCycle := true
	stopped := false
	logEvery := time.NewTicker(logInterval)
	defer logEvery.Stop()

	for !stopped {
		// Estimate the current top height seen from the peer
		height := hd.TopSeenHeight()

		origin, err := stages.GetStageProgress(db, stages.Headers)
		if err != nil {
			return err
		}
		hashStateStageProgress, err1 := stages.GetStageProgress(db, stages.Bodies) // TODO: shift this when more stages are added
		if err1 != nil {
			return err1
		}

		canRunCycleInOneTransaction := !initialCycle && height-origin < 1024 && height-hashStateStageProgress < 1024

		var writeDB ethdb.Database // on this variable will run sync cycle.

		// create empty TxDb object, it's not usable before .Begin() call which will use this object
		// It allows inject tx object to stages now, define rollback now,
		// but call .Begin() after hearer/body download stages
		var tx ethdb.DbWithPendingMutations
		if canRunCycleInOneTransaction {
			tx, err = db.Begin(context.Background(), ethdb.RW)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			writeDB = tx
		} else {
			writeDB = db
		}

		st, err1 := sync.Prepare(nil, chainConfig, nil, &vm.Config{}, db, writeDB, "downloader", ethdb.DefaultStorageMode, ".", 512*datasize.MB, make(chan struct{}), nil, nil, initialCycle, nil, stagedsync.StageSendersCfg(chainConfig))
		if err1 != nil {
			return fmt.Errorf("prepare staged sync: %w", err1)
		}

		err = st.Run(db, writeDB)
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
		initialCycle = false
		select {
		case <-ctx.Done():
			stopped = true
		default:
		}
	}
	return nil
}
