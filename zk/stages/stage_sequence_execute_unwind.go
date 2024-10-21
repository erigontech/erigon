package stages

import (
	"context"
	"errors"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
)

func UnwindSequenceExecutionStage(u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context, cfg SequenceBlockCfg, initialCycle bool, logger log.Logger) (err error) {
	if u.UnwindPoint >= s.BlockNumber {
		return nil
	}
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	logPrefix := u.LogPrefix()
	logger.Info(fmt.Sprintf("[%s] Unwind Execution", logPrefix), "from", s.BlockNumber, "to", u.UnwindPoint)

	if err = unwindSequenceExecutionStage(u, s, tx, ctx, cfg, initialCycle, logger); err != nil {
		return err
	}

	//Do not invoke u.Done, because its effect is handled by updateSequencerProgress

	if !useExternalTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func unwindSequenceExecutionStage(u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx, ctx context.Context, cfg SequenceBlockCfg, initialCycle bool, logger log.Logger) error {
	hermezDb := hermez_db.NewHermezDb(tx)
	fromBatch, err := hermezDb.GetBatchNoByL2Block(u.UnwindPoint)
	if err != nil && !errors.Is(err, hermez_db.ErrorNotStored) {
		return err
	}

	if err := stagedsync.UnwindExecutionStageErigon(u, s, tx, ctx, cfg.toErigonExecuteBlockCfg(), initialCycle, logger); err != nil {
		return err
	}

	if err = stagedsync.UnwindExecutionStageDbWrites(ctx, u, s, tx); err != nil {
		return err
	}

	if err := UnwindSequenceExecutionStageDbWrites(ctx, u, s, tx); err != nil {
		return err
	}

	if err = updateSequencerProgress(tx, u.UnwindPoint, fromBatch, true); err != nil {
		return err
	}

	return nil
}

func UnwindSequenceExecutionStageDbWrites(ctx context.Context, u *stagedsync.UnwindState, s *stagedsync.StageState, tx kv.RwTx) error {
	// Truncate CallTraceSet
	keyStart := hexutility.EncodeTs(u.UnwindPoint + 1)
	c, err := tx.RwCursorDupSort(kv.CallTraceSet)
	if err != nil {
		return err
	}
	defer c.Close()
	for k, _, err := c.Seek(keyStart); k != nil; k, _, err = c.NextNoDup() {
		if err != nil {
			return err
		}
		err = c.DeleteCurrentDuplicates()
		if err != nil {
			return err
		}
	}

	// fromBlock := u.UnwindPoint
	hermezDb := hermez_db.NewHermezDb(tx)
	fromBatch, err := hermezDb.GetBatchNoByL2Block(u.UnwindPoint + 1)
	if err != nil {
		return fmt.Errorf("get fromBatch no by l2 block error: %v", err)
	}
	toBatch, err := hermezDb.GetBatchNoByL2Block(s.BlockNumber)
	if err != nil {
		return fmt.Errorf("get toBatch no by l2 block error: %v", err)
	}

	lastBatchToKeepBeforeFrom, err := hermezDb.GetBatchNoByL2Block(u.UnwindPoint)
	if err != nil {
		return fmt.Errorf("get fromBatch no by l2 block error: %v", err)
	}
	fromBatchForForkIdDeletion := fromBatch
	if lastBatchToKeepBeforeFrom == fromBatch {
		fromBatchForForkIdDeletion++
	}

	// only seq
	if err = hermezDb.DeleteLatestUsedGers(u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("truncate latest used gers error: %v", err)
	}
	// only seq
	if err = hermezDb.DeleteBlockGlobalExitRoots(u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("truncate block ger error: %v", err)
	}
	// only seq
	if err = hermezDb.DeleteBlockL1BlockHashes(u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("truncate block l1 block hash error: %v", err)
	}
	// only seq
	if err = hermezDb.DeleteBlockL1InfoTreeIndexes(u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("truncate block l1 info tree index error: %v", err)
	}
	// only seq
	if err = hermezDb.DeleteBlockL1InfoTreeIndexesProgress(u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("truncate block l1 info tree index error: %v", err)
	}
	// only seq
	if err = hermezDb.DeleteBlockBatches(u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("truncate block batches error: %v", err)
	}
	// only seq
	if err = hermezDb.DeleteForkIds(fromBatchForForkIdDeletion, toBatch); err != nil {
		return fmt.Errorf("truncate fork id error: %v", err)
	}
	// only seq
	if err = hermezDb.DeleteBatchCounters(u.UnwindPoint+1, s.BlockNumber); err != nil {
		return fmt.Errorf("truncate block batches error: %v", err)
	}

	return nil
}
