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

	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/rawdb/blockio"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
)

type BlockHashesCfg struct {
	tmpDir       string
	headerWriter *blockio.BlockWriter
}

func StageBlockHashesCfg(tmpDir string, headerWriter *blockio.BlockWriter) BlockHashesCfg {
	return BlockHashesCfg{
		tmpDir:       tmpDir,
		headerWriter: headerWriter,
	}
}

func SpawnBlockHashStage(s *StageState, tx kv.RwTx, cfg BlockHashesCfg, ctx context.Context, logger log.Logger) (err error) {
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
	return nil
}

func UnwindBlockHashStage(u *UnwindState, tx kv.RwTx) error {
	return u.Done(tx)
}
