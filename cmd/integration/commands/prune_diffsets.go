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

package commands

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/debug"
)

var pruneDiffSetsBatch int

var cmdPruneDiffSets = &cobra.Command{
	Use:   "prune_diffsets",
	Short: "Prune block diffsets (ChangeSets3 table) below --prune.to, committing in batches",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()
		// minimal table cfg: works on DBs created by older erigon versions and
		// guarantees no other table can be touched or created
		db, err := dbCfg(dbcfg.ChainDB, chaindata).WithTableCfg(func(_ kv.TableCfg) kv.TableCfg {
			return kv.TableCfg{kv.ChangeSets3: {}, kv.SyncStageProgress: {}}
		}).Open(ctx)
		if err != nil {
			return err
		}
		defer db.Close()
		var execProgress uint64
		err = db.View(ctx, func(tx kv.Tx) error {
			progress, err := stages.GetStageProgress(tx, stages.Execution)
			if err != nil {
				return err
			}
			execProgress = progress
			return nil
		})
		if err != nil {
			return err
		}
		target, err := resolvePruneDiffSetsTarget(execProgress, pruneTo, dbg.MaxReorgDepth)
		if err != nil {
			return err
		}
		if target == 0 {
			logger.Info("[prune_diffsets] nothing to prune", "execProgress", execProgress, "maxReorgDepth", dbg.MaxReorgDepth)
			return nil
		}
		logger.Info("[prune_diffsets] pruning", "to", target, "execProgress", execProgress, "batch", pruneDiffSetsBatch)
		deleted, err := pruneDiffSets(ctx, db, target, pruneDiffSetsBatch, logger)
		if err != nil {
			return err
		}
		logger.Info("[prune_diffsets] done - freed pages go to MDBX GC; to shrink the file use mdbx_copy -c", "deleted", deleted, "to", target)
		return nil
	},
}

func resolvePruneDiffSetsTarget(execProgress uint64, requested uint64, maxReorgDepth uint64) (uint64, error) {
	var safeTarget uint64
	if execProgress > maxReorgDepth {
		safeTarget = execProgress - maxReorgDepth
	}
	if requested == 0 {
		return safeTarget, nil
	}
	if requested > safeTarget {
		return 0, fmt.Errorf("--prune.to=%d is inside the reorg window (Execution progress %d - max reorg depth %d = %d): node would not be able to unwind", requested, execProgress, maxReorgDepth, safeTarget)
	}
	return requested, nil
}

func pruneDiffSets(ctx context.Context, db kv.RwDB, pruneTo uint64, batch int, logger log.Logger) (uint64, error) {
	if batch <= 0 {
		batch = math.MaxInt
	}
	var total uint64
	start := time.Now()
	for {
		var deleted int
		err := db.Update(ctx, func(tx kv.RwTx) error {
			var err error
			deleted, err = rawdb.PruneTable(tx, kv.ChangeSets3, pruneTo, ctx, batch, time.Hour, logger, "prune_diffsets")
			return err
		})
		if err != nil {
			return total, err
		}
		total += uint64(deleted)
		if deleted == 0 {
			return total, nil
		}
		logger.Info("[prune_diffsets] progress", "deleted", total, "elapsed", time.Since(start))
	}
}

func init() {
	withDataDir(cmdPruneDiffSets)
	cmdPruneDiffSets.Flags().Uint64Var(&pruneTo, "prune.to", 0, "delete diffsets of blocks below this block number (0 = Execution progress - MAX_REORG_DEPTH)")
	cmdPruneDiffSets.Flags().IntVar(&pruneDiffSetsBatch, "prune.batch", 1_000_000, "max rows deleted per database commit (0 = everything in one commit)")
	rootCmd.AddCommand(cmdPruneDiffSets)
}
