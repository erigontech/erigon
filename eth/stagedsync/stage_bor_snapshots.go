package stagedsync

import (
	"context"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	proto_downloader "github.com/ledgerwatch/erigon-lib/gointerfaces/downloader"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/log/v3"
)

type BorSnapshotsCfg struct {
	db          kv.RwDB
	chainConfig chain.Config
	dirs        datadir.Dirs

	borRetire          services.BorRetire
	snapshotDownloader proto_downloader.DownloaderClient
	blockReader        services.FullBlockReader
	dbEventNotifier    services.DBEventNotifier

	historyV3 bool
	agg       *state.AggregatorV3
}

func StageBorSnapshotsCfg(db kv.RwDB,
	chainConfig chain.Config, dirs datadir.Dirs,
	borRetire services.BorRetire,
	snapshotDownloader proto_downloader.DownloaderClient,
	blockReader services.FullBlockReader, dbEventNotifier services.DBEventNotifier,
	historyV3 bool, agg *state.AggregatorV3,
) BorSnapshotsCfg {
	return BorSnapshotsCfg{
		db:                 db,
		chainConfig:        chainConfig,
		dirs:               dirs,
		borRetire:          borRetire,
		snapshotDownloader: snapshotDownloader,
		blockReader:        blockReader,
		dbEventNotifier:    dbEventNotifier,
		historyV3:          historyV3,
		agg:                agg,
	}
}

func BorSnapshotsForward(
	s *StageState,
	ctx context.Context,
	tx kv.RwTx,
	cfg BorSnapshotsCfg,
	initialCycle bool,
	logger log.Logger,
) (err error) {
	if cfg.chainConfig.Bor == nil {
		return
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	if err := DownloadAndIndexBorSnapshotsIfNeed(s, ctx, tx, cfg, initialCycle, logger); err != nil {
		return err
	}
	var minProgress uint64
	for _, stage := range []stages.SyncStage{stages.Headers, stages.Bodies, stages.Senders, stages.TxLookup} {
		progress, err := stages.GetStageProgress(tx, stage)
		if err != nil {
			return err
		}
		if minProgress == 0 || progress < minProgress {
			minProgress = progress
		}
	}
	if minProgress > s.BlockNumber {
		if err = s.Update(tx, minProgress); err != nil {
			return err
		}
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return
}

func DownloadAndIndexBorSnapshotsIfNeed(s *StageState, ctx context.Context, tx kv.RwTx, cfg BorSnapshotsCfg, initialCycle bool, logger log.Logger) error {
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
func BorSnapshotsPrune(s *PruneState, initialCycle bool, cfg BorSnapshotsCfg, ctx context.Context, tx kv.RwTx) (err error) {
	if cfg.chainConfig.Bor == nil {
		return
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	freezingCfg := cfg.blockReader.FreezingCfg()
	if freezingCfg.Enabled {
		if err := cfg.borRetire.PruneAncientBlocks(tx, 100); err != nil {
			return err
		}
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return
}
