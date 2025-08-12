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

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/execution/chain"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
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
