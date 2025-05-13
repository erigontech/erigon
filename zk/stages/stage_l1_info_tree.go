package stages

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/zk/l1infotree"
	"github.com/erigontech/erigon/zk/sequencer"
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

	progress, err := stages.GetStageProgress(tx, stages.L1InfoTree)
	if err != nil {
		return err
	}
	// L2InfoTreeUpdatesEnabled must be enabled, this method uses an updated rpc method that uses to and from.
	if progress == 0 && !sequencer.IsSequencer() && cfg.zkCfg.L2InfoTreeUpdatesEnabled {
		select {
		default:
			// If we are a rpc node, and we are starting from the beginning, we need to check for updates from the L2
			infoTrees, err := cfg.updater.CheckL2RpcForInfoTreeUpdates(logPrefix, tx)
			if err != nil {
				log.Warn(fmt.Sprintf("[%s] L2 Info Tree sync failed, getting Info Tree from L1", logPrefix), "err", err)
				break
			}

			var latestIndex uint64
			latestUpdate := cfg.updater.GetLatestUpdate()
			if latestUpdate != nil {
				latestIndex = latestUpdate.Index
			}

			log.Info(fmt.Sprintf("[%s] Synced Info Tree updates from L2 Sequencer RPC", logPrefix), "count", len(infoTrees), "latestIndex", latestIndex)

			if freshTx {
				if funcErr = tx.Commit(); funcErr != nil {
					return fmt.Errorf("tx.Commit: %w", funcErr)
				}
			}

			return nil
		}
	}

	if err = cfg.updater.WarmUp(tx); err != nil {
		return fmt.Errorf("cfg.updater.WarmUp: %w", err)
	}

	processedLogs, err := cfg.updater.CheckForInfoTreeUpdates(logPrefix, tx)
	if err != nil {
		return fmt.Errorf("CheckForInfoTreeUpdates: %w", err)
	}

	var latestIndex uint64
	latestUpdate := cfg.updater.GetLatestUpdate()
	if latestUpdate != nil {
		latestIndex = latestUpdate.Index
	}

	log.Info(fmt.Sprintf("[%s] Info tree updates", logPrefix), "count", processedLogs, "latestIndex", latestIndex)

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
