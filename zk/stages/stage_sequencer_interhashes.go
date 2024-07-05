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

// This stages does NOTHING while going forward, because its done during execution
func SpawnSequencerInterhashesStage(
	s *stagedsync.StageState,
	u stagedsync.Unwinder,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerInterhashesCfg,
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

// The unwind of interhashes must happen separate from execution’s unwind although execution includes interhashes while going forward.
// This is because interhashes MUST be unwound before history/calltraces while execution MUST be after history/calltraces
func UnwindSequencerInterhashsStage(
	u *stagedsync.UnwindState,
	s *stagedsync.StageState,
	tx kv.RwTx,
	ctx context.Context,
	cfg SequencerInterhashesCfg,
) error {
	return UnwindZkIntermediateHashesStage(u, s, tx, ZkInterHashesCfg{}, ctx)
}

func PruneSequencerInterhashesStage(
	s *stagedsync.PruneState,
	tx kv.RwTx,
	cfg SequencerInterhashesCfg,
	ctx context.Context,
) error {
	return nil
}
