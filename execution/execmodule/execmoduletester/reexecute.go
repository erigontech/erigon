// Copyright 2026 The Erigon Authors
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

package execmoduletester

import (
	"context"

	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/exec"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/execution/vm"
)

func (emt *ExecModuleTester) ReExecuteTo(ctx context.Context, toBlock uint64) error {
	cfg := stagedsync.StageExecuteBlocksCfg(
		emt.DB,
		emt.cfg.Prune,
		emt.cfg.BatchSize,
		emt.ChainConfig,
		emt.Engine,
		&vm.Config{},
		emt.Notifications,
		emt.cfg.StateStream,
		true,
		emt.Dirs,
		emt.BlockReader,
		emt.cfg.Genesis,
		emt.cfg.Sync,
		false,
		exec.NewBlockReadAheader(),
	)
	if agg, ok := emt.DB.(dbstate.HasAgg); ok {
		if aggT, okT := agg.Agg().(*dbstate.Aggregator); okT {
			aggT.PresetOfflineExecution()
		}
	}
	for {
		progress, err := emt.reExecuteBatch(ctx, toBlock, cfg)
		if err != nil {
			return err
		}
		if progress >= toBlock {
			return nil
		}
	}
}

func (emt *ExecModuleTester) reExecuteBatch(ctx context.Context, toBlock uint64, cfg stagedsync.ExecuteBlockCfg) (uint64, error) {
	tx, err := emt.DB.BeginTemporalRw(ctx)
	if err != nil {
		return 0, err
	}
	defer tx.Rollback()
	doms, err := execctx.NewSharedDomains(ctx, tx, emt.Log)
	if err != nil {
		return 0, err
	}
	doms.SetInMemHistoryReads(false)
	s, err := emt.Sync.StageState(stages.Execution, tx, true, false)
	if err != nil {
		doms.Close()
		return 0, err
	}
	err = stagedsync.SpawnExecuteBlocksStage(s, emt.Sync, doms, tx, toBlock, ctx, cfg, emt.Log)
	if err != nil && !stagedsync.IsOnlyLoopExhausted(err) {
		doms.Close()
		return 0, err
	}
	progress, progressErr := stages.GetStageProgress(tx, stages.Execution)
	if progressErr == nil {
		progressErr = doms.Commit(ctx, tx)
	}
	doms.Close()
	if progressErr != nil {
		return 0, progressErr
	}
	return progress, nil
}
