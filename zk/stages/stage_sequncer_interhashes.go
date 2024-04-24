package stages

import (
	"context"

	"github.com/gateway-fm/cdk-erigon-lib/kv"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/turbo/shards"
)

type SequencerInterhashesCfg struct {
	db          kv.RwDB
	accumulator *shards.Accumulator
}

func StageSequencerInterhashesCfg(
	db kv.RwDB,
	accumulator *shards.Accumulator,
) SequencerInterhashesCfg {
	return SequencerInterhashesCfg{
		db:          db,
		accumulator: accumulator,
	}
}

func SpawnSequencerInterhashesStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerInterhashesCfg,
	initialCycle bool,
	quiet bool,
) error {
	var err error

	freshTx := tx == nil
	if freshTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	if err := s.Update(tx, to); err != nil {
		return err
	}

	if freshTx {
		if err = tx.Commit(); err != nil {
			return err
		}
	}

	return nil
}

func UnwindSequencerInterhashsStage(
	u *stagedsync.UnwindState,
	s *stagedsync.StageState,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerInterhashesCfg,
	initialCycle bool,
) error {
	return nil
}

func PruneSequencerInterhashesStage(
	s *stagedsync.PruneState,
	tx kv.RwTx,
	cfg SequencerInterhashesCfg,
	ctx context.Context,
	initialCycle bool,
) error {
	return nil
}
