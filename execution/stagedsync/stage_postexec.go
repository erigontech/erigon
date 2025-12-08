// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package stagedsync

import (
	"context"
	"fmt"

	"github.com/erigontech/erigon/db/kv"
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
