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
	"errors"
	"fmt"
	"time"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"

	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/node/debug"
)

var cmdBuildTxNum = &cobra.Command{
	Use:   "build_txnum",
	Short: "Rebuild the MaxTxNum table (blockNum -> maxTxNum) from snapshots and DB",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		db, err := openDB(dbCfg(dbcfg.ChainDB, chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := buildTxNum(ctx, db, logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func init() {
	withConfig(cmdBuildTxNum)
	withDataDir(cmdBuildTxNum)
	withChain(cmdBuildTxNum)
	rootCmd.AddCommand(cmdBuildTxNum)
}

func buildTxNum(ctx context.Context, db kv.TemporalRwDB, logger log.Logger) error {
	blockReader, _ := blocksIO(db, logger)
	frozenBlocks := blockReader.FrozenBlocks()
	pruneMarkerBlockThreshold := rawdbreset.GetPruneMarkerSafeThreshold(blockReader)

	logger.Info("Rebuilding MaxTxNum index",
		"frozenBlocks", frozenBlocks,
		"pruneThreshold", pruneMarkerBlockThreshold,
	)

	if err := db.Update(ctx, func(tx kv.RwTx) error {
		if err := tx.ClearTable(kv.MaxTxNum); err != nil {
			return fmt.Errorf("clear MaxTxNum: %w", err)
		}

		logEvery := time.NewTicker(20 * time.Second)
		defer logEvery.Stop()

		if err := blockReader.IterateFrozenBodies(func(blockNum, baseTxNum, txAmount uint64) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-logEvery.C:
				logger.Info(fmt.Sprintf("[build_txnum] MaxTxNums from snapshots: %s/%s",
					common.PrettyCounter(blockNum), common.PrettyCounter(frozenBlocks)))
			default:
			}
			if baseTxNum+txAmount == 0 {
				panic(fmt.Sprintf("uint underflow: baseTxNum=%d txAmount=%d", baseTxNum, txAmount))
			}
			maxTxNum := baseTxNum + txAmount - 1
			if blockNum > frozenBlocks {
				return nil
			}
			if blockNum >= pruneMarkerBlockThreshold || blockNum == 0 {
				if err := rawdbv3.TxNums.Append(tx, blockNum, maxTxNum); err != nil {
					return fmt.Errorf("%w. blockNum=%d, maxTxNum=%d", err, blockNum, maxTxNum)
				}
			}
			return nil
		}); err != nil {
			return fmt.Errorf("build txNum => blockNum mapping from snapshots: %w", err)
		}

		return nil
	}); err != nil {
		return err
	}

	// Print result
	if err := db.View(ctx, func(tx kv.Tx) error {
		firstBlock, firstTxNum, err := rawdbv3.TxNums.First(tx)
		if err != nil {
			return err
		}
		lastBlock, lastTxNum, err := rawdbv3.TxNums.Last(tx)
		if err != nil {
			return err
		}
		logger.Info("MaxTxNum index rebuilt",
			"firstBlock", firstBlock, "firstTxNum", firstTxNum,
			"lastBlock", lastBlock, "lastTxNum", lastTxNum,
		)
		return nil
	}); err != nil {
		return err
	}

	return nil
}
