package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/chain"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb/blockio"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/log/v3"
)

type BlockHashesCfg struct {
	db     kv.RwDB
	tmpDir string
	cc     *chain.Config

	headerWriter *blockio.BlockWriter
}

func StageBlockHashesCfg(db kv.RwDB, tmpDir string, cc *chain.Config, headerWriter *blockio.BlockWriter) BlockHashesCfg {
	return BlockHashesCfg{
		db:           db,
		tmpDir:       tmpDir,
		cc:           cc,
		headerWriter: headerWriter,
	}
}

func SpawnBlockHashStage(s *StageState, tx kv.RwTx, cfg BlockHashesCfg, ctx context.Context, logger log.Logger) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	headNumber, err := stages.GetStageProgress(tx, stages.Headers)
	if err != nil {
		return fmt.Errorf("getting headers progress: %w", err)
	}
	if s.BlockNumber == headNumber {
		return nil
	}

	// etl.Tranform uses ExractEndKey as exclusive bound, therefore +1
	if err := cfg.headerWriter.FillHeaderNumberIndex(s.LogPrefix(), tx, cfg.tmpDir, s.BlockNumber, headNumber+1, ctx, logger); err != nil {
		return err
	}

	if err = s.Update(tx, headNumber); err != nil {
		return err
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindBlockHashStage(u *UnwindState, tx kv.RwTx, cfg BlockHashesCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	if err = u.Done(tx); err != nil {
		return fmt.Errorf(" reset: %w", err)
	}
	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("failed to write db commit: %w", err)
		}
	}
	return nil
}

func PruneBlockHashStage(p *PruneState, tx kv.RwTx, cfg BlockHashesCfg, ctx context.Context) (err error) {
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
