package stages

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/log/v3"

	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/sync_stages"
	"github.com/ledgerwatch/erigon/zk/hermez_db"
	"github.com/ledgerwatch/erigon/zk/types"
)

type ISequencesSyncer interface {
	GetSequences(logPrefix string, startBlock uint64) ([]types.L1BatchInfo, uint64, error)
}

type L1SequencesCfg struct {
	db             kv.RwDB
	sequenceSyncer ISequencesSyncer

	zkCfg *ethconfig.Zk
}

func StageL1SequencesCfg(db kv.RwDB, sequenceSyncer ISequencesSyncer, zkCfg *ethconfig.Zk) L1SequencesCfg {
	return L1SequencesCfg{
		db:             db,
		sequenceSyncer: sequenceSyncer,
		zkCfg:          zkCfg,
	}
}

func SpawnStageL1Sequences(
	s *sync_stages.StageState,
	u sync_stages.Unwinder,
	ctx context.Context,
	tx kv.RwTx,
	cfg L1SequencesCfg,
	firstCycle bool,
	quiet bool,
) error {
	logPrefix := s.LogPrefix()
	log.Info(fmt.Sprintf("[%s] Starting L1 Sequences download stage", logPrefix))
	defer log.Info(fmt.Sprintf("[%s] Finished L1 Sequences download stage ", logPrefix))

	if tx == nil {
		log.Debug("l1 sequences: no tx provided, creating a new one")
		var err error
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("failed to open tx, %w", err)
		}
		defer tx.Rollback()
	}

	hermezDb, err := hermez_db.NewHermezDb(tx)
	if err != nil {
		return err
	}

	l1BlockProgress, err := sync_stages.GetStageProgress(tx, sync_stages.L1Sequences)
	if err != nil {
		return fmt.Errorf("failed to get l1 block progress, %w", err)
	}
	if l1BlockProgress == 0 {
		l1BlockProgress = cfg.zkCfg.L1FirstBlock - 1
	}

	log.Info(fmt.Sprintf("[%s] Downloading sequences from L1...", logPrefix))

	batchInfo, l1Block, err := cfg.sequenceSyncer.GetSequences(logPrefix, l1BlockProgress)
	if err != nil {
		return fmt.Errorf("failed to get highest sequence, %w", err)
	}

	err = sync_stages.SaveStageProgress(tx, sync_stages.L1Sequences, l1Block)
	if err != nil {
		return fmt.Errorf("failed to save l1 sequences progress, %w", err)
	}

	for _, b := range batchInfo {
		err = hermezDb.WriteSequence(b.L1BlockNo, b.BatchNo, b.L1TxHash, b.StateRoot)
		if err != nil {
			return fmt.Errorf("failed to write batch info, %w", err)
		}
	}

	if firstCycle {
		log.Debug("l1 sequences: first cycle, committing tx")
		if err := tx.Commit(); err != nil {
			return fmt.Errorf("failed to commit tx, %w", err)
		}
	}

	return nil
}

func UnwindL1SequencesStage(u *sync_stages.UnwindState, tx kv.RwTx, cfg L1SequencesCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err := cfg.db.BeginRw(ctx)
		if err != nil {
			return fmt.Errorf("failed to open tx, %w", err)
		}
		defer tx.Rollback()
	}

	// TODO: how can we handle the unwind of this stage?

	if err := u.Done(tx); err != nil {
		return err
	}
	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func PruneL1SequencesStage(p *sync_stages.PruneState, tx kv.RwTx) error {

	// TODO: how to handle pruning of the sequences stage?

	return nil
}
