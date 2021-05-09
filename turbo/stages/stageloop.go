package stages

import (
	"context"
	"fmt"
	"runtime/debug"
	"strings"
	"time"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/stages/headerdownload"
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
	notifier stagedsync.ChainEventNotifier,
) *stagedsync.StagedSync {
	return stagedsync.New(
		stagedsync.ReplacementStages(ctx, sm, headers, bodies, senders, exec, hashState, trieCfg, history, logIndex, callTraces, txLookup, txPool),
		stagedsync.ReplacementUnwindOrder(),
		stagedsync.OptionalParameters{Notifier: notifier},
	)
}

// StageLoop runs the continuous loop of staged sync
func StageLoop(
	ctx context.Context,
	db ethdb.Database,
	sync *stagedsync.StagedSync,
	hd *headerdownload.HeaderDownload,
	chainConfig *params.ChainConfig,
) {
	initialCycle := true

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Estimate the current top height seen from the peer
		height := hd.TopSeenHeight()
		if err := StageLoopStep(ctx, db, sync, height, chainConfig, initialCycle); err != nil {
			log.Error("Stage loop failure", "error", err)
			continue
		}

		initialCycle = false
		hd.EnableRequestChaining()
	}
}

func StageLoopStep(
	ctx context.Context,
	db ethdb.Database,
	sync *stagedsync.StagedSync,
	highestSeenHeader uint64,
	chainConfig *params.ChainConfig,
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

	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		return err
	}

	st, err1 := sync.Prepare(nil, chainConfig, nil, &vm.Config{}, nil, nil, "downloader", sm, ".", 512*datasize.MB, ctx.Done(), nil, nil, initialCycle, nil)
	if err1 != nil {
		return fmt.Errorf("prepare staged sync: %w", err1)
	}

	origin, err := stages.GetStageProgress(db, stages.Headers)
	if err != nil {
		return err
	}
	hashStateStageProgress, err1 := stages.GetStageProgress(db, stages.Bodies) // TODO: shift this when more stages are added
	if err1 != nil {
		return err1
	}
		finishProgressBefore, err1 := stages.GetStageProgress(db, stages.Finish) // TODO: shift this when more stages are added
		if err1 != nil {
			return err1
		}

	canRunCycleInOneTransaction := !initialCycle && highestSeenHeader-origin < 1024 && highestSeenHeader-hashStateStageProgress < 1024

	var tx ethdb.RwTx // on this variable will run sync cycle.
	if canRunCycleInOneTransaction {
		tx, err = db.RwKV().BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	unwindTo, err := st.GetUnwindTo(db)
	if err != nil {
		return err
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

	err = stagedsync.NotifyNewHeaders2(finishProgressBefore, unwindTo, sync.Notifier, db)
	if err != nil {
		return err
	}
	return nil
}
