package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/params"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
)

type FinishCfg struct {
	db        kv.RwDB
	tmpDir    string
	btClient  *snapshotsync.Client
	snBuilder *snapshotsync.SnapshotMigrator
	log       log.Logger
}

func StageFinishCfg(db kv.RwDB, tmpDir string, btClient *snapshotsync.Client, snBuilder *snapshotsync.SnapshotMigrator, logger log.Logger) FinishCfg {
	return FinishCfg{
		db:        db,
		log:       logger,
		tmpDir:    tmpDir,
		btClient:  btClient,
		snBuilder: snBuilder,
	}
}

func FinishForward(s *StageState, tx kv.RwTx, cfg FinishCfg, initialCycle bool) error {
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
		return nil
	}

	if cfg.snBuilder != nil && useExternalTx {
		snBlock := snapshotsync.CalculateEpoch(executionAt, snapshotsync.EpochSize)
		err = cfg.snBuilder.AsyncStages(snBlock, cfg.log, cfg.db, tx, cfg.btClient, true)
		if err != nil {
			return err
		}
		if cfg.snBuilder.Replaced() {
			err = cfg.snBuilder.SyncStages(snBlock, cfg.db, tx)
			if err != nil {
				return err
			}
		}
	}
	rawdb.WriteHeadBlockHash(tx, rawdb.ReadHeadHeaderHash(tx))
	err = s.Update(tx, executionAt)
	if err != nil {
		return err
	}

	if initialCycle {
		if err := params.SetErigonVersion(tx, params.VersionKeyFinished); err != nil {
			return err
		}
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindFinish(u *UnwindState, tx kv.RwTx, cfg FinishCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err = u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneFinish(u *PruneState, tx kv.RwTx, cfg FinishCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func NotifyNewHeaders(ctx context.Context, finishStageBeforeSync uint64, unwindTo *uint64, notifier ChainEventNotifier, tx kv.Tx) error {
	notifyTo, err := stages.GetStageProgress(tx, stages.Finish) // because later stages can be disabled
	if err != nil {
		return err
	}
	notifyFrom := finishStageBeforeSync + 1
	if unwindTo != nil && *unwindTo != 0 && (*unwindTo) < finishStageBeforeSync {
		notifyFrom = *unwindTo + 1
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
