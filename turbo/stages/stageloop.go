package stages

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/turbo/stages/headerdownload"
)

func NewStagedSync(
	ctx context.Context,
	sm ethdb.StorageMode,
	headers stagedsync.HeadersCfg,
	blockHashes stagedsync.BlockHashesCfg,
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
	finish stagedsync.FinishCfg,
) *stagedsync.StagedSync {
	return stagedsync.New(
		stagedsync.ReplacementStages(ctx, sm, headers, blockHashes, bodies, senders, exec, hashState, trieCfg, history, logIndex, callTraces, txLookup, txPool, finish),
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
		if err := StageLoopStep(ctx, db, sync, height, chainConfig, notifier, initialCycle); err != nil {
			if errors.Is(err, common.ErrStopped) {
				return
			}

			log.Error("Stage loop failure", "error", err)
			if recoveryErr := hd.RecoverFromDb(db); recoveryErr != nil {
				log.Error("Failed to recover header downoader", "error", recoveryErr)
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
) (err error) {
	// avoid crash because TG's core does many things -
	defer func() {
		if r := recover(); r != nil { // just log is enough
			panicReplacer := strings.NewReplacer("\n", " ", "\t", "", "\r", "")
			stack := panicReplacer.Replace(string(debug.Stack()))
			switch typed := r.(type) {
			case error:
				err = fmt.Errorf("%w, trace: %s", typed, stack)
			default:
				err = fmt.Errorf("%+v, trace: %s", typed, stack)
			}
		}
	}()
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

	st, err1 := sync.Prepare(nil, chainConfig, nil, &vm.Config{}, ethdb.NewObjectDatabase(db), nil, "downloader", sm, ".", 512*datasize.MB, ctx.Done(), nil, nil, initialCycle, nil)
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

	err = st.Run(ethdb.NewObjectDatabase(db), tx)
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

	err = stagedsync.NotifyNewHeaders(ctx, finishProgressBefore, unwindTo, notifier, db)
	if err != nil {
		return err
	}
	return nil
}
