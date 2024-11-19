package stages

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/zk/l1infotree"
	"github.com/ledgerwatch/log/v3"
)

type L1InfoTreeCfg struct {
	db      kv.RwDB
	zkCfg   *ethconfig.Zk
	updater *l1infotree.Updater
}

func StageL1InfoTreeCfg(db kv.RwDB, zkCfg *ethconfig.Zk, updater *l1infotree.Updater) L1InfoTreeCfg {
	return L1InfoTreeCfg{
		db:      db,
		zkCfg:   zkCfg,
		updater: updater,
	}
}

func SpawnL1InfoTreeStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	cfg L1InfoTreeCfg,
	ctx context.Context,
	logger log.Logger,
) (funcErr error) {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting L1 Info Tree stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished L1 Info Tree stage", logPrefix))

	freshTx := tx == nil
	if freshTx {
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("cfg.db.BeginRw: %w", err)
		}
		defer tx.Rollback()
	}

	if err := cfg.updater.WarmUp(tx); err != nil {
		return fmt.Errorf("cfg.updater.WarmUp: %w", err)
	}

	allLogs, err := cfg.updater.CheckForInfoTreeUpdates(logPrefix, tx)
	if err != nil {
		return fmt.Errorf("CheckForInfoTreeUpdates: %w", err)
	}

	var latestIndex uint64
	latestUpdate := cfg.updater.GetLatestUpdate()
	if latestUpdate != nil {
		latestIndex = latestUpdate.Index
	}
	log.Info(fmt.Sprintf("[%s] Info tree updates", logPrefix), "count", len(allLogs), "latestIndex", latestIndex)

	if freshTx {
		if funcErr = tx.Commit(); funcErr != nil {
			return fmt.Errorf("tx.Commit: %w", funcErr)
		}
	}

	return nil
}

func UnwindL1InfoTreeStage(u *stagedsync.UnwindState, tx kv.RwTx, cfg L1InfoTreeCfg, ctx context.Context) error {
	return nil
}

func PruneL1InfoTreeStage(s *stagedsync.PruneState, tx kv.RwTx, cfg L1InfoTreeCfg, ctx context.Context) error {
	return nil
}
