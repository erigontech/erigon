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

package commands

import (
	"encoding/binary"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/backup"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/db/kv/rawdbv3"
	"github.com/erigontech/erigon/db/rawdb"
	"github.com/erigontech/erigon/node/debug"
)

func init() {
	withDataDir(cmdMaxTxNumPopulate)
	withChain(cmdMaxTxNumPopulate)
	withReset(cmdMaxTxNumPopulate)
	rootCmd.AddCommand(cmdMaxTxNumPopulate)
}

var cmdMaxTxNumPopulate = &cobra.Command{
	Use:   "maxtxnum_populate",
	Short: "Populate the MaxTxNum table from canonical block bodies",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		db, err := openDB(dbCfg(dbcfg.ChainDB, chaindata), true, chain, logger)
		if err != nil {
			return fmt.Errorf("opening DB: %w", err)
		}
		defer db.Close()

		blockReader, _ := blocksIO(db, logger)

		if err := db.Update(ctx, func(tx kv.RwTx) error {
			lastBlock, lastTxNum, err := rawdbv3.TxNums.Last(tx)
			if err != nil {
				return fmt.Errorf("reading last TxNum: %w", err)
			}

			if reset || lastTxNum == 0 {
				if lastTxNum != 0 {
					logger.Info("Clearing MaxTxNum table")
					if err := backup.ClearTables(ctx, tx, kv.MaxTxNum); err != nil {
						return fmt.Errorf("clearing MaxTxNum: %w", err)
					}
				}

				// First populate from frozen snapshot bodies
				frozenBlocks := blockReader.FrozenBlocks()
				logger.Info("Populating MaxTxNum from frozen bodies", "frozenBlocks", frozenBlocks)
				if err := blockReader.IterateFrozenBodies(func(blockNum, baseTxNum, txCount uint64) error {
					if baseTxNum+txCount == 0 {
						panic(baseTxNum + txCount)
					}
					maxTxNum := baseTxNum + txCount - 1
					if blockNum > frozenBlocks {
						return nil
					}
					return rawdbv3.TxNums.Append(tx, blockNum, maxTxNum)
				}); err != nil {
					return fmt.Errorf("iterating frozen bodies: %w", err)
				}

				// Then continue from DB bodies
				if frozenBlocks > 0 {
					logger.Info("Populating MaxTxNum from DB bodies", "from", frozenBlocks+1)
					return rawdb.AppendCanonicalTxNums(tx, frozenBlocks+1)
				}
				logger.Info("Populating MaxTxNum from DB bodies", "from", 0)
				return rawdb.AppendCanonicalTxNums(tx, 0)
			}

			from := lastBlock + 1
			logger.Info("Continuing MaxTxNum population", "from", from, "currentLastBlock", lastBlock, "currentLastTxNum", lastTxNum)
			return rawdb.AppendCanonicalTxNums(tx, from)
		}); err != nil {
			return fmt.Errorf("populating MaxTxNum: %w", err)
		}

		// Verify in a read-only transaction
		frozenBlocks := blockReader.FrozenBlocks()
		return db.View(ctx, func(tx kv.Tx) error {
			return verifyMaxTxNum(tx, frozenBlocks)
		})
	},
}

func verifyMaxTxNum(tx kv.Tx, frozenBlocks uint64) error {
	firstBlock, firstTxNum, err := rawdbv3.TxNums.First(tx)
	if err != nil {
		return err
	}
	lastBlock, lastTxNum, err := rawdbv3.TxNums.Last(tx)
	if err != nil {
		return err
	}

	if lastTxNum == 0 && firstTxNum == 0 && lastBlock == 0 {
		fmt.Println("MaxTxNum table is empty")
		return nil
	}

	totalBlocks := lastBlock - firstBlock + 1
	fmt.Printf("First: block=%d txNum=%d\n", firstBlock, firstTxNum)
	fmt.Printf("Last:  block=%d txNum=%d\n", lastBlock, lastTxNum)
	fmt.Printf("Total blocks: %d\n", totalBlocks)

	// Last() block must cover frozen snapshot blocks
	fmt.Printf("\nFrozen blocks: %d\n", frozenBlocks)
	if lastBlock < frozenBlocks {
		return fmt.Errorf("MaxTxNum last block %d is behind frozen blocks %d", lastBlock, frozenBlocks)
	}
	fmt.Printf("  last block %d >= frozen blocks %d OK\n", lastBlock, frozenBlocks)

	// Last() block must cover the last BlockBody entry in the DB
	lastBodyKey, err := rawdbv3.LastKey(tx, kv.BlockBody)
	if err != nil {
		return err
	}
	if len(lastBodyKey) >= 8 {
		lastBodyBlock := binary.BigEndian.Uint64(lastBodyKey[:8])
		fmt.Printf("\nLast BlockBody in DB: block=%d\n", lastBodyBlock)
		if lastBlock < lastBodyBlock {
			return fmt.Errorf("MaxTxNum last block %d is behind last BlockBody %d", lastBlock, lastBodyBlock)
		}
		fmt.Printf("  last block %d >= last body block %d OK\n", lastBlock, lastBodyBlock)
	}

	fmt.Println("\nVerification passed")
	return nil
}
