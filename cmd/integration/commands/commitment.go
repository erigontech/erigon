// Copyright 2025 The Erigon Authors
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
	"encoding/hex"
	"fmt"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/node/debug"
)

var branchPrefixFlag string

func init() {
	withDataDir(commitmentBranchCmd)
	commitmentBranchCmd.Flags().StringVar(&branchPrefixFlag, "prefix", "", "hex prefix to read (e.g., 'aa', '0a1b')")

	commitmentCmd.AddCommand(commitmentBranchCmd)
	rootCmd.AddCommand(commitmentCmd)
}

var commitmentCmd = &cobra.Command{
	Use:   "commitment",
	Short: "Commitment domain commands",
}

var commitmentBranchCmd = &cobra.Command{
	Use:   "branch",
	Short: "Read branch data from the commitment domain",
	Long: `Opens the commitment domain from a given datadir and reads the branch data
for the specified prefix. The prefix should be provided as hex nibbles.

Examples:
  integration commitment branch --datadir ~/data/eth-mainnet --prefix aa
  integration commitment branch --datadir /path/to/datadir --prefix 0a1b
  integration commitment branch --datadir /path/to/datadir  # reads root (empty prefix)`,
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		ctx, _ := common.RootContext()

		prefix, err := commitment.PrefixStringToNibbles(branchPrefixFlag)
		if err != nil {
			logger.Error("Failed to parse prefix", "error", err)
			return
		}

		dirs := datadir.New(datadirCli)
		chainDb, err := openDB(dbCfg(dbcfg.ChainDB, dirs.Chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer chainDb.Close()

		// Start a read-only temporal transaction
		tx, err := chainDb.BeginTemporalRo(ctx)
		if err != nil {
			logger.Error("Failed to begin temporal tx", "error", err)
			return
		}
		defer tx.Rollback()

		// Use LatestStateReader to read from the commitment domain.
		// This is the same approach used by commitmentdb.TrieContext.Branch internally:
		// TrieContext.Branch -> TrieContext.readDomain -> StateReader.Read
		commitmentReader := commitmentdb.NewLatestStateReader(tx)

		if err := readBranch(commitmentReader, prefix, logger); err != nil {
			logger.Error("Failed to read branch", "error", err)
			return
		}
	},
}

func readBranch(stateReader *commitmentdb.LatestStateReader, prefix []byte, logger interface {
	Info(msg string, ctx ...interface{})
}) error {
	compactKey := commitment.HexNibblesToCompactBytes(prefix)
	val, step, err := stateReader.Read(kv.CommitmentDomain, compactKey, config3.DefaultStepSize)
	if err != nil {
		return fmt.Errorf("failed to get branch for prefix %x: %w", prefix, err)
	}

	fmt.Printf("Prefix: 0x%s\n", hex.EncodeToString(prefix))
	fmt.Printf("Step: %d\n", step)

	if len(val) == 0 {
		fmt.Println("Branch data: <empty>")
		return nil
	}

	fmt.Printf("Branch data (hex): %s\n", hex.EncodeToString(val))
	fmt.Printf("Branch data length: %d bytes\n", len(val))

	// Parse and display the branch data in human-readable format
	branchData := commitment.BranchData(val)
	fmt.Printf("\nParsed branch data:\n%s\n", branchData.String())

	return nil
}
