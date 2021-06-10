package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"

	"github.com/ledgerwatch/erigon/ethdb"
)

type FinishCfg struct {
	db     ethdb.RwKV
	tmpDir string
}

func StageFinishCfg(db ethdb.RwKV, tmpDir string) FinishCfg {
	return FinishCfg{
		db:     db,
		tmpDir: tmpDir,
	}
}

func FinishForward(s *StageState, tx ethdb.RwTx, cfg FinishCfg, btClient *snapshotsync.Client, snBuilder *snapshotsync.SnapshotMigrator) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	var executionAt uint64
	var err error
	if executionAt, err = s.ExecutionAt(tx); err != nil {
		return err
	}
	if executionAt <= s.BlockNumber {
		s.Done()
		return nil
	}

	if snBuilder != nil && useExternalTx {
		snBlock := snapshotsync.CalculateEpoch(executionAt, snapshotsync.EpochSize)
		err = snBuilder.AsyncStages(snBlock, cfg.db, tx, btClient, true)
		if err != nil {
			return err
		}
		if snBuilder.Replaced() {
			err = snBuilder.SyncStages(snBlock, cfg.db, tx)
			if err != nil {
				return err
			}
		}
	}
	rawdb.WriteHeadBlockHash(tx, rawdb.ReadHeadHeaderHash(tx))
	err = s.DoneAndUpdate(tx, executionAt)
	if err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindFinish(u *UnwindState, s *StageState, tx ethdb.RwTx, cfg FinishCfg) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	err := u.Done(tx)
	if err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func NotifyNewHeaders(ctx context.Context, finishStageBeforeSync, unwindTo uint64, notifier ChainEventNotifier, db ethdb.RwKV) error {
	tx, err := db.BeginRo(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	notifyTo, err := stages.GetStageProgress(tx, stages.Finish) // because later stages can be disabled
	if err != nil {
		return err
	}
	notifyFrom := finishStageBeforeSync + 1
	if unwindTo != 0 && unwindTo < finishStageBeforeSync {
		notifyFrom = unwindTo + 1
	}
	if notifier == nil {
		log.Warn("rpc notifier is not set, rpc daemon won't be updated about headers")
		return nil
	}
	log.Info("Update current block for the RPC API", "from", notifyFrom, "to", notifyTo)
	for i := notifyFrom; i <= notifyTo; i++ {
		header := rawdb.ReadHeaderByNumber(tx, i)
		if header == nil {
			return fmt.Errorf("could not find canonical header for number: %d", i)
		}
		notifier.OnNewHeader(header)
	}
	return nil
}
