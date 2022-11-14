package stagedsync

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon-lib/kv"
)

// PostExec stage is run after execution stage to peform extra verifications that are only possible when state is available.
// It is used for consensus engines which keep validators inside smart contracts (Bor, AuRa)

type PostExecCfg struct {
	db    kv.RwDB
	borDb kv.RwDB
}

func StagePostExecCfg(db kv.RwDB, borDb kv.RwDB) PostExecCfg {
	return PostExecCfg{
		db:    db,
		borDb: borDb,
	}
}

func SpawnPostExecStage(s *StageState, tx kv.RwTx, cfg PostExecCfg, ctx context.Context) error {
	useExternalTx := tx != nil
	if !useExternalTx {
		var err error
		tx, err = cfg.db.BeginRw(context.Background())
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	//logPrefix := s.LogPrefix()
	to, err := s.ExecutionAt(tx)
	if err != nil {
		return err
	}

	if s.BlockNumber == to {
		// we already did hash check for this block
		// we don't do the obvious `if s.BlockNumber > to` to support reorgs more naturally
		return nil
	}
	if s.BlockNumber > to {
		return fmt.Errorf("hashstate: promotion backwards from %d to %d", s.BlockNumber, to)
	}

	if err = s.Update(tx, to); err != nil {
		return err
	}

	if !useExternalTx {
		if err := tx.Commit(); err != nil {
			return err
		}
	}
	return nil
}

func UnwindPostExecStage(u *UnwindState, s *StageState, tx kv.RwTx, cfg PostExecCfg, ctx context.Context) (err error) {
	useExternalTx := tx != nil
	if !useExternalTx {
		tx, err = cfg.db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}

	//logPrefix := u.LogPrefix()

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
