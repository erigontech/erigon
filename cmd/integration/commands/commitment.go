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
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"path/filepath"
	"runtime"

	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/erigon/cmd/utils/app"
	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/estimate"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/db/config3"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/db/kv"
	"github.com/erigontech/erigon/db/kv/dbcfg"
	dbstate "github.com/erigontech/erigon/db/state"
	"github.com/erigontech/erigon/db/state/statecfg"
	"github.com/erigontech/erigon/execution/commitment"
	"github.com/erigontech/erigon/execution/commitment/commitmentdb"
	"github.com/erigontech/erigon/execution/stagedsync"
	"github.com/erigontech/erigon/execution/stagedsync/rawdbreset"
	"github.com/erigontech/erigon/execution/stagedsync/stages"
	"github.com/erigontech/erigon/node/debug"
)

var branchPrefixFlag string

func init() {

	// commitment branch
	withChain(commitmentBranchCmd)
	withDataDir(commitmentBranchCmd)
	withConfig(commitmentBranchCmd)
	commitmentBranchCmd.Flags().StringVar(&branchPrefixFlag, "prefix", "", "hex prefix to read (e.g., 'aa', '0a1b')")
	commitmentCmd.AddCommand(commitmentBranchCmd)

	// commitment rebuild
	withChain(cmdCommitmentRebuild)
	withDataDir(cmdCommitmentRebuild)
	withConfig(cmdCommitmentRebuild)
	withReset(cmdCommitmentRebuild)
	withSqueeze(cmdCommitmentRebuild)
	withBlock(cmdCommitmentRebuild)
	withConcurrentCommitment(cmdCommitmentRebuild)
	withUnwind(cmdCommitmentRebuild)
	withPruneTo(cmdCommitmentRebuild)
	withIntegrityChecks(cmdCommitmentRebuild)
	withHeimdall(cmdCommitmentRebuild)
	withChaosMonkey(cmdCommitmentRebuild)
	commitmentCmd.AddCommand(cmdCommitmentRebuild)

	// commitment print
	withChain(cmdCommitmentPrint)
	withDataDir(cmdCommitmentPrint)
	withConfig(cmdCommitmentPrint)
	commitmentCmd.AddCommand(cmdCommitmentPrint)

	rootCmd.AddCommand(commitmentCmd)

}

var commitmentCmd = &cobra.Command{
	Use:   "commitment",
	Short: "Commitment domain commands",
}

// integration commitment branch
var commitmentBranchCmd = &cobra.Command{
	Use:   "branch",
	Short: "Read branch data from the commitment domain",
	Long: `Opens the commitment domain from a given datadir and reads the branch data
for the specified prefix. The prefix should be provided as hex nibbles.

Examples:
  integration commitment branch --chain=mainnet --datadir ~/data/eth-mainnet --prefix aa
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

// integration commitment rebuild
var cmdCommitmentRebuild = &cobra.Command{
	Use:   "rebuild",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(dbcfg.ChainDB, chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := commitmentRebuild(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func commitmentRebuild(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)
	if reset {
		return rawdbreset.Reset(ctx, db, stages.Execution)
	}

	br, _ := blocksIO(db, logger)
	cfg := stagedsync.StageTrieCfg(db, true, true, dirs.Tmp, br)

	rwTx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer rwTx.Rollback()

	// remove all existing state commitment snapshots
	if err := app.DeleteStateSnapshots(dirs, false, true, false, "0-999999", kv.CommitmentDomain.String()); err != nil {
		return err
	}

	log.Info("Clearing commitment-related DB tables to rebuild on clean data...")
	sconf := statecfg.Schema.CommitmentDomain
	for _, tn := range sconf.Tables() {
		log.Info("Clearing", "table", tn)
		if err := rwTx.ClearTable(tn); err != nil {
			return fmt.Errorf("failed to clear table %s: %w", tn, err)
		}
	}
	if err := rwTx.Commit(); err != nil {
		return err
	}

	agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	if err = agg.OpenFolder(); err != nil { // reopen after snapshot file deletions
		return fmt.Errorf("failed to re-open aggregator: %w", err)
	}

	blockSnapBuildSema := semaphore.NewWeighted(int64(runtime.NumCPU()))
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	agg.SetCollateAndBuildWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetMergeWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	agg.PeriodicalyPrintProcessSet(ctx)

	if _, err := stagedsync.RebuildPatriciaTrieBasedOnFiles(ctx, cfg, squeeze); err != nil {
		return err
	}
	return nil
}

// integration commitment print
var cmdCommitmentPrint = &cobra.Command{
	Use:   "print",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(dbcfg.ChainDB, chaindata), true, chain, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := printCommitment(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func printCommitment(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	agg := db.(dbstate.HasAgg).Agg().(*dbstate.Aggregator)
	blockSnapBuildSema := semaphore.NewWeighted(int64(runtime.NumCPU()))
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	agg.SetCollateAndBuildWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetMergeWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	agg.PeriodicalyPrintProcessSet(ctx)

	// disable hard alignment; allowing commitment and storage/account to have
	// different visibleFiles
	agg.DisableAllDependencies()

	acRo := agg.BeginFilesRo() // this tx is used to read existing domain files and closed in the end
	defer acRo.Close()
	defer acRo.MadvNormal().DisableReadAhead()

	commitmentFiles := acRo.Files(kv.CommitmentDomain)
	fmt.Printf("Commitment files: %d\n", len(commitmentFiles))
	for _, f := range commitmentFiles {
		name := filepath.Base(f.Fullpath())
		count := acRo.KeyCountInFiles(kv.CommitmentDomain, f.StartRootNum(), f.EndRootNum())
		rootNodePrefix := []byte("state")
		rootNode, _, _, _, err := acRo.DebugGetLatestFromFiles(kv.CommitmentDomain, rootNodePrefix, f.EndRootNum()-1)
		if err != nil {
			return fmt.Errorf("failed to get root node from files: %w", err)
		}
		rootString, err := commitment.HexTrieStateToShortString(rootNode)
		if err != nil {
			return fmt.Errorf("failed to extract state root from root node: %w", err)
		}
		fmt.Printf("%28s: prefixes %8s %s\n", name, common.PrettyCounter(count), rootString)
	}

	str, err := dbstate.CheckCommitmentForPrint(ctx, db)
	if err != nil {
		return fmt.Errorf("failed to check commitment: %w", err)
	}
	fmt.Printf("\n%s", str)

	return nil
}
