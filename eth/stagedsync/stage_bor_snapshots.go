package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"
)

func BorSnapshotsForward(
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg SnapshotsCfg,
	initialCycle bool,
	logger log.Logger,
) (err error) {
	if cfg.chainConfig.Bor == nil {
		return
	}
	if err := DownloadAndIndexBorSnapshotsIfNeed(s, ctx, tx, cfg, initialCycle, logger); err != nil {
		return err
	}
	return
}

func DownloadAndIndexBorSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg SnapshotsCfg, initialCycle bool, logger log.Logger) error {
	if !initialCycle {
		return nil
	}
	if !cfg.blockReader.FreezingCfg().Enabled {
		return nil
	}
	return nil
}

/* ====== PRUNING ====== */
// snapshots pruning sections works more as a retiring of blocks
// retiring blocks means moving block data from db into snapshots
func BorSnapshotsPrune(s *PruneState, initialCycle bool, cfg SnapshotsCfg, ctx context.Context, tx kv.RwTx) (err error) {
	if cfg.chainConfig.Bor == nil {
		return
	}
	return
}
