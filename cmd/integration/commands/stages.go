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
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/c2h5oh/datasize"
	lru "github.com/hashicorp/golang-lru/arc/v2"
	"github.com/spf13/cobra"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sync/semaphore"

	"github.com/erigontech/mdbx-go/mdbx"
	"github.com/erigontech/secp256k1"

	"github.com/erigontech/erigon-db/rawdb"
	"github.com/erigontech/erigon-db/rawdb/blockio"
	chain2 "github.com/erigontech/erigon-lib/chain"
	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/dbg"
	"github.com/erigontech/erigon-lib/config3"
	"github.com/erigontech/erigon-lib/downloader"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/backup"
	"github.com/erigontech/erigon-lib/kv/prune"
	"github.com/erigontech/erigon-lib/kv/rawdbv3"
	"github.com/erigontech/erigon-lib/log/v3"
	libstate "github.com/erigontech/erigon-lib/state"
	"github.com/erigontech/erigon-lib/state/stats"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon-lib/wrap"
	p2p "github.com/erigontech/erigon-p2p"
	"github.com/erigontech/erigon-p2p/sentry"
	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/hack/tool/fromdb"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/core/tracing"
	"github.com/erigontech/erigon/core/vm"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/ethconfig/estimate"
	"github.com/erigontech/erigon/eth/ethconfig/features"
	"github.com/erigontech/erigon/eth/ethconsensusconfig"
	"github.com/erigontech/erigon/eth/integrity"
	reset2 "github.com/erigontech/erigon/eth/rawdbreset"
	"github.com/erigontech/erigon/eth/stagedsync"
	"github.com/erigontech/erigon/eth/stagedsync/stages"
	"github.com/erigontech/erigon/execution/builder"
	"github.com/erigontech/erigon/execution/consensus"
	"github.com/erigontech/erigon/execution/sentry_multi_client"
	"github.com/erigontech/erigon/node/migrations"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/params"
	"github.com/erigontech/erigon/polygon/bor"
	"github.com/erigontech/erigon/polygon/bridge"
	"github.com/erigontech/erigon/polygon/heimdall"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/logging"
	"github.com/erigontech/erigon/turbo/services"
	"github.com/erigontech/erigon/turbo/shards"
	"github.com/erigontech/erigon/turbo/snapshotsync/freezeblocks"
	stages2 "github.com/erigontech/erigon/turbo/stages"
)

var cmdStageSnapshots = &cobra.Command{
	Use:   "stage_snapshots",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := stageSnapshots(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageHeaders = &cobra.Command{
	Use:   "stage_headers",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := stageHeaders(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageBodies = &cobra.Command{
	Use:   "stage_bodies",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := stageBodies(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageSenders = &cobra.Command{
	Use:   "stage_senders",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := stageSenders(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageExec = &cobra.Command{
	Use:   "stage_exec",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		defer func(t time.Time) { logger.Info("total", "took", time.Since(t)) }(time.Now())

		if err := stageExec(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageCustomTrace = &cobra.Command{
	Use:   "stage_custom_trace",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		defer func(t time.Time) { logger.Info("total", "took", time.Since(t)) }(time.Now())

		if err := stageCustomTrace(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdStagePatriciaTrie = &cobra.Command{
	Use:   "commitment_rebuild",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := stagePatriciaTrie(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdStageTxLookup = &cobra.Command{
	Use:   "stage_tx_lookup",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := stageTxLookup(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdPrintStages = &cobra.Command{
	Use:   "print_stages",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Flags().Set(logging.LogConsoleVerbosityFlag.Name, "debug")
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), false, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		if err := printAllStages(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdAlloc = &cobra.Command{
	Use:     "alloc",
	Example: "integration allocates and holds 1Gb (or given size)",
	Run: func(cmd *cobra.Command, args []string) {
		cmd.Flags().Set(logging.LogConsoleVerbosityFlag.Name, "debug")
		v, err := datasize.ParseString(args[0])
		if err != nil {
			panic(err)
		}
		n := make([]byte, v.Bytes())
		common.Sleep(cmd.Context(), 265*24*time.Hour)
		_ = n
	},
}

var cmdPrintTableSizes = &cobra.Command{
	Use:   "print_table_sizes",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), false, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()

		allTablesCfg := db.AllTables()
		allTables := make([]string, 0, len(allTablesCfg))
		for table, cfg := range allTablesCfg {
			if cfg.IsDeprecated {
				continue
			}

			allTables = append(allTables, table)
		}

		var tableSizes []interface{}
		err = db.View(cmd.Context(), func(tx kv.Tx) error {
			tableSizes = stagedsync.CollectTableSizes(db, tx, allTables)
			return nil
		})
		if err != nil {
			logger.Error("error while collecting table sizes", "err", err)
			return
		}

		if len(tableSizes)%2 != 0 {
			logger.Error("table sizes len not even", "len", len(tableSizes))
			return
		}

		var sb strings.Builder
		sb.WriteString("Table")
		sb.WriteRune(',')
		sb.WriteString("Size")
		sb.WriteRune('\n')
		for i := 0; i < len(tableSizes)/2; i++ {
			sb.WriteString(tableSizes[i*2].(string))
			sb.WriteRune(',')
			sb.WriteString(tableSizes[i*2+1].(string))
			sb.WriteRune('\n')
		}

		if outputCsvFile == "" {
			logger.Info("table sizes", "csv", sb.String())
			return
		}

		f, err := os.Create(outputCsvFile)
		if err != nil {
			logger.Error("issue creating file", "file", outputCsvFile, "err", err)
			return
		}

		_, err = f.WriteString(sb.String())
		if err != nil {
			logger.Error("issue writing output to file", "file", outputCsvFile, "err", err)
			return
		}

		logger.Info("wrote table sizes to csv output file", "file", outputCsvFile)
	},
}

var cmdPrintMigrations = &cobra.Command{
	Use:   "print_migrations",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), false, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()
		if err := printAppliedMigrations(db, cmd.Context(), logger); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdRemoveMigration = &cobra.Command{
	Use:   "remove_migration",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), false, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()
		if err := removeMigration(db, cmd.Context()); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

var cmdRunMigrations = &cobra.Command{
	Use:   "run_migrations",
	Short: "",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		//non-accede and exclusive mode - to apply create new tables if need.
		cfg := dbCfg(kv.ChainDB, chaindata).RemoveFlags(mdbx.Accede).Exclusive(true)
		db, err := openDB(cfg, true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		defer db.Close()
		// Nothing to do, migrations will be applied automatically
	},
}

func init() {
	withConfig(cmdPrintStages)
	withDataDir(cmdPrintStages)
	withChain(cmdPrintStages)
	withHeimdall(cmdPrintStages)
	rootCmd.AddCommand(cmdPrintStages)

	rootCmd.AddCommand(cmdAlloc)

	withDataDir(cmdPrintTableSizes)
	withOutputCsvFile(cmdPrintTableSizes)
	rootCmd.AddCommand(cmdPrintTableSizes)

	withConfig(cmdStageSenders)
	withIntegrityChecks(cmdStageSenders)
	withReset(cmdStageSenders)
	withBlock(cmdStageSenders)
	withUnwind(cmdStageSenders)
	withDataDir(cmdStageSenders)
	withChain(cmdStageSenders)
	withHeimdall(cmdStageSenders)
	withChaosMonkey(cmdStageSenders)
	rootCmd.AddCommand(cmdStageSenders)

	withConfig(cmdStageSnapshots)
	withDataDir(cmdStageSnapshots)
	withChain(cmdStageSnapshots)
	withReset(cmdStageSnapshots)
	withChaosMonkey(cmdStageSnapshots)
	rootCmd.AddCommand(cmdStageSnapshots)

	withConfig(cmdStageHeaders)
	withIntegrityChecks(cmdStageHeaders)
	withDataDir(cmdStageHeaders)
	withUnwind(cmdStageHeaders)
	withReset(cmdStageHeaders)
	withChain(cmdStageHeaders)
	withHeimdall(cmdStageHeaders)
	withChaosMonkey(cmdStageHeaders)
	rootCmd.AddCommand(cmdStageHeaders)

	withConfig(cmdStageBodies)
	withDataDir(cmdStageBodies)
	withUnwind(cmdStageBodies)
	withChain(cmdStageBodies)
	withHeimdall(cmdStageBodies)
	withChaosMonkey(cmdStageBodies)
	rootCmd.AddCommand(cmdStageBodies)

	withConfig(cmdStageExec)
	withDataDir(cmdStageExec)
	withReset(cmdStageExec)
	withBlock(cmdStageExec)
	withUnwind(cmdStageExec)
	withNoCommit(cmdStageExec)
	withPruneTo(cmdStageExec)
	withBatchSize(cmdStageExec)
	withTxTrace(cmdStageExec)
	withChain(cmdStageExec)
	withHeimdall(cmdStageExec)
	withWorkers(cmdStageExec)
	withChaosMonkey(cmdStageExec)
	withChainTipMode(cmdStageExec)
	rootCmd.AddCommand(cmdStageExec)

	withConfig(cmdStageCustomTrace)
	withDataDir(cmdStageCustomTrace)
	withReset(cmdStageCustomTrace)
	withBlock(cmdStageCustomTrace)
	withUnwind(cmdStageCustomTrace)
	withNoCommit(cmdStageCustomTrace)
	withPruneTo(cmdStageCustomTrace)
	withBatchSize(cmdStageCustomTrace)
	withTxTrace(cmdStageCustomTrace)
	withChain(cmdStageCustomTrace)
	withHeimdall(cmdStageCustomTrace)
	withWorkers(cmdStageCustomTrace)
	withChaosMonkey(cmdStageCustomTrace)
	withDomain(cmdStageCustomTrace)
	rootCmd.AddCommand(cmdStageCustomTrace)

	withConfig(cmdStagePatriciaTrie)
	withDataDir(cmdStagePatriciaTrie)
	withReset(cmdStagePatriciaTrie)
	withBlock(cmdStagePatriciaTrie)
	withUnwind(cmdStagePatriciaTrie)
	withPruneTo(cmdStagePatriciaTrie)
	withIntegrityChecks(cmdStagePatriciaTrie)
	withChain(cmdStagePatriciaTrie)
	withHeimdall(cmdStagePatriciaTrie)
	withChaosMonkey(cmdStagePatriciaTrie)
	rootCmd.AddCommand(cmdStagePatriciaTrie)

	withConfig(cmdStageTxLookup)
	withReset(cmdStageTxLookup)
	withBlock(cmdStageTxLookup)
	withUnwind(cmdStageTxLookup)
	withDataDir(cmdStageTxLookup)
	withPruneTo(cmdStageTxLookup)
	withChain(cmdStageTxLookup)
	withHeimdall(cmdStageTxLookup)
	withChaosMonkey(cmdStageTxLookup)
	rootCmd.AddCommand(cmdStageTxLookup)

	withConfig(cmdPrintMigrations)
	withDataDir(cmdPrintMigrations)
	rootCmd.AddCommand(cmdPrintMigrations)

	withConfig(cmdRemoveMigration)
	withDataDir(cmdRemoveMigration)
	withMigration(cmdRemoveMigration)
	withChain(cmdRemoveMigration)
	withHeimdall(cmdRemoveMigration)
	rootCmd.AddCommand(cmdRemoveMigration)

	withConfig(cmdRunMigrations)
	withDataDir(cmdRunMigrations)
	withChain(cmdRunMigrations)
	withHeimdall(cmdRunMigrations)
	rootCmd.AddCommand(cmdRunMigrations)
}

func stageSnapshots(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	br, bw := blocksIO(db, logger)

	tx, err := db.BeginTemporalRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		if err := stages.SaveStageProgress(tx, stages.Snapshots, 0); err != nil {
			return fmt.Errorf("saving Snapshots progress failed: %w", err)
		}
	}
	dirs := datadir.New(datadirCli)
	if err := reset2.ResetBlocks(tx, db, br, bw, dirs, logger); err != nil {
		return fmt.Errorf("resetting blocks: %w", err)
	}
	domains, err := libstate.NewSharedDomains(tx, logger)
	if err != nil {
		return err
	}
	defer domains.Close()
	//txnUm := domains.TxNum()
	blockNum := domains.BlockNum()

	// stagedsync.SpawnStageSnapshots(s, ctx, rwTx, logger)
	progress, err := stages.GetStageProgress(tx, stages.Snapshots)
	if err != nil {
		return fmt.Errorf("re-read Snapshots progress: %w", err)
	}

	if blockNum > progress {
		if err := stages.SaveStageProgress(tx, stages.Execution, blockNum); err != nil {
			return fmt.Errorf("saving Snapshots progress failed: %w", err)
		}
		progress, err = stages.GetStageProgress(tx, stages.Snapshots)
		if err != nil {
			return fmt.Errorf("re-read Snapshots progress: %w", err)
		}
	}
	logger.Info("Progress", "snapshots", progress)
	return nil
}

func stageHeaders(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)
	if err := datadir.ApplyMigrations(dirs); err != nil {
		return err
	}

	br, bw := blocksIO(db, logger)

	if integritySlow {
		if err := db.View(ctx, func(tx kv.Tx) error {
			log.Info("[integrity] no gaps in canonical headers")
			integrity.NoGapsInCanonicalHeaders(tx, ctx, br)
			return nil
		}); err != nil {
			return err
		}
		return nil
	}

	if !(unwind > 0 || reset) {
		logger.Error("This command only works with --unwind or --reset options")
		return nil
	}

	return db.Update(ctx, func(tx kv.RwTx) error {
		if reset {
			if err := reset2.ResetBlocks(tx, db, br, bw, dirs, logger); err != nil {
				return err
			}
			return nil
		}

		progress, err := stages.GetStageProgress(tx, stages.Headers)
		if err != nil {
			return fmt.Errorf("read Bodies progress: %w", err)
		}
		var unwindTo uint64
		if unwind > progress {
			unwindTo = 1 // keep genesis
		} else {
			unwindTo = uint64(max(1, int(progress)-int(unwind)))
		}

		if err = stages.SaveStageProgress(tx, stages.Headers, unwindTo); err != nil {
			return fmt.Errorf("saving Headers progress failed: %w", err)
		}
		progress, err = stages.GetStageProgress(tx, stages.Headers)
		if err != nil {
			return fmt.Errorf("re-read Headers progress: %w", err)
		}
		{ // hard-unwind stage_body also
			if err := rawdb.TruncateBlocks(ctx, tx, progress+1); err != nil {
				return err
			}
			progressBodies, err := stages.GetStageProgress(tx, stages.Bodies)
			if err != nil {
				return fmt.Errorf("read Bodies progress: %w", err)
			}
			if progress < progressBodies {
				if err = stages.SaveStageProgress(tx, stages.Bodies, progress); err != nil {
					return fmt.Errorf("saving Bodies progress failed: %w", err)
				}
			}
		}
		// remove all canonical markers from this point
		if err = rawdb.TruncateCanonicalHash(tx, progress+1, false /* markChainAsBad */); err != nil {
			return err
		}
		if err = rawdb.TruncateTd(tx, progress+1); err != nil {
			return err
		}
		hash, ok, err := br.CanonicalHash(ctx, tx, progress-1)
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("canonical hash not found: %d", progress-1)
		}
		if err = rawdb.WriteHeadHeaderHash(tx, hash); err != nil {
			return err
		}

		logger.Info("Progress", "headers", progress)
		return nil
	})
}

func stageBodies(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	chainConfig := fromdb.ChainConfig(db)
	_, _, _, sync, _, _ := newSync(ctx, db, nil /* miningConfig */, logger)
	br, bw := blocksIO(db, logger)

	if err := db.Update(ctx, func(tx kv.RwTx) error {
		s := stage(sync, tx, nil, stages.Bodies)

		if unwind > 0 {
			if unwind > s.BlockNumber {
				return errors.New("cannot unwind past 0")
			}

			u := sync.NewUnwindState(stages.Bodies, s.BlockNumber-unwind, s.BlockNumber, true, false)
			cfg := stagedsync.StageBodiesCfg(db, nil, nil, nil, nil, 0, chainConfig, br, bw)
			if err := stagedsync.UnwindBodiesStage(u, tx, cfg, ctx); err != nil {
				return err
			}

			progress, err := stages.GetStageProgress(tx, stages.Bodies)
			if err != nil {
				return fmt.Errorf("re-read Bodies progress: %w", err)
			}
			logger.Info("Progress", "bodies", progress)
			return nil
		}
		logger.Info("This command only works with --unwind option")
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func stageSenders(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	tmpdir := datadir.New(datadirCli).Tmp
	chainConfig := fromdb.ChainConfig(db)
	_, _, _, sync, _, _ := newSync(ctx, db, nil /* miningConfig */, logger)

	must(sync.SetCurrentStage(stages.Senders))

	br, _ := blocksIO(db, logger)
	if reset {
		return db.Update(ctx, func(tx kv.RwTx) error { return reset2.ResetSenders(ctx, tx) })
	}

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if integritySlow {
		secp256k1.ContextForThread(1)
		for i := block; ; i++ {
			if err := common.Stopped(ctx.Done()); err != nil {
				return err
			}
			h, _ := br.HeaderByNumber(ctx, tx, i)
			if h == nil {
				break
			}
			withoutSenders, senders, err := br.BlockWithSenders(ctx, tx, h.Hash(), h.Number.Uint64())
			if err != nil {
				return err
			}
			withoutSenders.Body().SendersFromTxs() //remove senders info from txs
			txs := withoutSenders.Transactions()
			if txs.Len() != len(senders) {
				logger.Error("not equal amount of senders", "block", i, "db", len(senders), "expect", txs.Len())
				return nil
			}
			if txs.Len() == 0 {
				continue
			}
			signer := types.MakeSigner(chainConfig, i, h.Time)
			for j := 0; j < txs.Len(); j++ {
				from, err := signer.Sender(txs[j])
				if err != nil {
					return err
				}
				if !bytes.Equal(from[:], senders[j][:]) {
					logger.Error("wrong sender", "block", i, "tx", j, "db", fmt.Sprintf("%x", senders[j]), "expect", fmt.Sprintf("%x", from))
				}
			}
			if i%10 == 0 {
				logger.Info("checked", "block", i)
			}
		}
		return nil
	}

	s := stage(sync, tx, nil, stages.Senders)
	logger.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	pm, err := prune.Get(tx)
	if err != nil {
		return err
	}

	cfg := stagedsync.StageSendersCfg(db, chainConfig, sync.Cfg(), false, tmpdir, pm, br, nil)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.Senders, s.BlockNumber-unwind, s.BlockNumber, true, false)
		if err = stagedsync.UnwindSendersStage(u, tx, cfg, ctx); err != nil {
			return err
		}
	} else if pruneTo > 0 {
		//noop
		return nil
	} else {
		if err = stagedsync.SpawnRecoverSendersStage(cfg, s, sync, tx, block, ctx, logger); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageExec(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)
	if err := datadir.ApplyMigrations(dirs); err != nil {
		return err
	}

	_, engine, vmConfig, sync, _, _ := newSync(ctx, db, nil /* miningConfig */, logger)
	must(sync.SetCurrentStage(stages.Execution))
	if reset {
		if err := reset2.ResetExec(ctx, db); err != nil {
			return err
		}
		return nil
	}

	if txtrace {
		// Activate tracing and writing into json files for each transaction
		vmConfig.Tracer = &tracing.Hooks{}
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	s := stage(sync, nil, db, stages.Execution)
	if chainTipMode {
		s.CurrentSyncCycle.IsFirstCycle = false
		s.CurrentSyncCycle.IsInitialCycle = false
	}

	logger.Info("Stage", "name", s.ID, "progress", s.BlockNumber)
	chainConfig, pm := fromdb.ChainConfig(db), fromdb.PruneMode(db)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
	}

	genesis := core.GenesisBlockByChainName(chain)
	br, _ := blocksIO(db, logger)

	notifications := shards.NewNotifications(nil)
	cfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, chainConfig, engine, vmConfig, notifications,
		/*stateStream=*/ false,
		/*badBlockHalt=*/ true,
		dirs, br, nil, genesis, syncCfg, nil)

	if unwind > 0 {
		if err := db.View(ctx, func(tx kv.Tx) error {
			minUnwindableBlockNum, _, err := libstate.AggTx(tx).CanUnwindBeforeBlockNum(s.BlockNumber-unwind, tx)
			if err != nil {
				return err
			}
			unwind = s.BlockNumber - minUnwindableBlockNum
			return nil
		}); err != nil {
			return err
		}
	}

	var tx kv.RwTx //nil - means lower-level code (each stage) will manage transactions
	if noCommit {
		var err error
		tx, err = db.BeginRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
	}
	txc := wrap.NewTxContainer(tx, nil)

	if unwind > 0 {
		u := sync.NewUnwindState(stages.Execution, s.BlockNumber-unwind, s.BlockNumber, true, false)
		err := stagedsync.UnwindExecutionStage(u, s, txc, ctx, cfg, logger)
		if err != nil {
			return err
		}
		return nil
	}

	if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.Execution, s.BlockNumber, tx, db, true)
		if err != nil {
			return err
		}
		err = stagedsync.PruneExecutionStage(p, tx, cfg, ctx, logger)
		if err != nil {
			return err
		}
		return nil
	}

	if chainTipMode {
		var sendersProgress, execProgress uint64
		if err := db.ViewTemporal(ctx, func(tx kv.TemporalTx) error {
			var err error
			if execProgress, err = stages.GetStageProgress(tx, stages.Execution); err != nil {
				return err
			}
			if execProgress == 0 {
				doms, err := libstate.NewSharedDomains(tx, log.New())
				if err != nil {
					panic(err)
				}
				execProgress = doms.BlockNum()
				doms.Close()
			}

			if sendersProgress, err = stages.GetStageProgress(tx, stages.Senders); err != nil {
				return err
			}
			return nil
		}); err != nil {
			return err
		}
		if block == 0 {
			block = sendersProgress
		}

		if noCommit {
			tx, err := db.BeginTemporalRw(ctx)
			if err != nil {
				return err
			}
			defer tx.Rollback()
			for bn := execProgress; bn < block; bn++ {
				txc = wrap.NewTxContainer(tx, txc.Doms)
				if err := stagedsync.SpawnExecuteBlocksStage(s, sync, txc, bn, ctx, cfg, logger); err != nil {
					return err
				}
			}
		} else {
			if err := db.Update(ctx, func(tx kv.RwTx) error {
				for bn := execProgress; bn < block; bn++ {
					txc = wrap.NewTxContainer(tx, txc.Doms)
					if err := stagedsync.SpawnExecuteBlocksStage(s, sync, txc, bn, ctx, cfg, logger); err != nil {
						return err
					}
				}
				return nil
			}); err != nil {
				return err
			}
		}
		return nil
	}

	if err := stagedsync.SpawnExecuteBlocksStage(s, sync, txc, block, ctx, cfg, logger); err != nil {
		return err
	}

	return nil
}

func stageCustomTrace(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)
	if err := datadir.ApplyMigrations(dirs); err != nil {
		return err
	}

	br, engine, vmConfig, sync, _, _ := newSync(ctx, db, nil /* miningConfig */, logger)
	must(sync.SetCurrentStage(stages.Execution))

	chainConfig := fromdb.ChainConfig(db)
	genesis := core.GenesisBlockByChainName(chain)
	blockReader, _ := blocksIO(db, logger)

	cfg := stagedsync.StageCustomTraceCfg(strings.Split(domain, ","), db, dirs, blockReader, chainConfig, engine, genesis, &syncCfg)
	if reset {
		tx, err := db.BeginTemporalRw(ctx)
		if err != nil {
			return err
		}
		defer tx.Rollback()
		var tables []string
		if cfg.Produce.ReceiptDomain {
			tables = append(tables, db.Debug().DomainTables(kv.ReceiptDomain)...)
		}
		if cfg.Produce.RCacheDomain {
			tables = append(tables, db.Debug().DomainTables(kv.RCacheDomain)...)
		}
		if cfg.Produce.LogAddr {
			tables = append(tables, db.Debug().InvertedIdxTables(kv.LogAddrIdx)...)
		}
		if cfg.Produce.LogTopic {
			tables = append(tables, db.Debug().InvertedIdxTables(kv.LogTopicIdx)...)
		}
		if cfg.Produce.TraceFrom {
			tables = append(tables, db.Debug().InvertedIdxTables(kv.TracesFromIdx)...)
		}
		if cfg.Produce.TraceTo {
			tables = append(tables, db.Debug().InvertedIdxTables(kv.TracesToIdx)...)
		}
		if err := backup.ClearTables(ctx, tx, tables...); err != nil {
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		if err := reset2.Reset(ctx, db, stages.CustomTrace); err != nil {
			return err
		}
		return nil
	}

	if txtrace {
		// Activate tracing and writing into json files for each transaction
		vmConfig.Tracer = &tracing.Hooks{}
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	agg := db.(libstate.HasAgg).Agg().(*libstate.Aggregator)
	defer br.(*freezeblocks.BlockRetire).MadvNormal().DisableReadAhead()
	//defer agg.MadvNormal().DisableReadAhead()
	blockSnapBuildSema := semaphore.NewWeighted(int64(runtime.NumCPU()))
	agg.SetSnapshotBuildSema(blockSnapBuildSema)
	agg.SetCollateAndBuildWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetMergeWorkers(min(4, estimate.StateV3Collate.Workers()))
	agg.SetCompressWorkers(estimate.CompressSnapshot.Workers())
	agg.PeriodicalyPrintProcessSet(ctx)

	err := stagedsync.SpawnCustomTrace(cfg, ctx, logger)
	if err != nil {
		return err
	}

	return nil
}

func stagePatriciaTrie(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	dirs := datadir.New(datadirCli)
	if reset {
		return reset2.Reset(ctx, db, stages.Execution)
	}

	br, _ := blocksIO(db, logger)
	historyV3 := true
	cfg := stagedsync.StageTrieCfg(db, true, true, false, dirs.Tmp, br, nil, historyV3)

	if _, err := stagedsync.RebuildPatriciaTrieBasedOnFiles(ctx, cfg); err != nil {
		return err
	}
	return nil
}

func stageTxLookup(db kv.TemporalRwDB, ctx context.Context, logger log.Logger) error {
	dirs, pm := datadir.New(datadirCli), fromdb.PruneMode(db)
	_, _, _, sync, _, _ := newSync(ctx, db, nil /* miningConfig */, logger)
	chainConfig := fromdb.ChainConfig(db)
	must(sync.SetCurrentStage(stages.TxLookup))
	if reset {
		return db.Update(ctx, reset2.ResetTxLookup)
	}
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	s := stage(sync, tx, nil, stages.TxLookup)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
	}
	logger.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	br, _ := blocksIO(db, logger)
	cfg := stagedsync.StageTxLookupCfg(db, pm, dirs.Tmp, chainConfig.Bor, br)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.TxLookup, s.BlockNumber-unwind, s.BlockNumber, true, false)
		err = stagedsync.UnwindTxLookup(u, s, tx, cfg, ctx, logger)
		if err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.TxLookup, s.BlockNumber, tx, nil, true)
		if err != nil {
			return err
		}
		err = stagedsync.PruneTxLookup(p, tx, cfg, ctx, logger)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnTxLookup(s, tx, block, cfg, ctx, logger)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func printAllStages(db kv.RoDB, ctx context.Context, logger log.Logger) error {
	sn, borSn, _, _, _, _, _ := allSnapshots(ctx, db, logger) // ignore error here to get some stat.
	defer sn.Close()
	defer borSn.Close()
	return db.View(ctx, func(tx kv.Tx) error { return printStages(tx, sn, borSn) })
}

func printAppliedMigrations(db kv.RwDB, ctx context.Context, logger log.Logger) error {
	return db.View(ctx, func(tx kv.Tx) error {
		applied, err := migrations.AppliedMigrations(tx, false /* withPayload */)
		if err != nil {
			return err
		}
		var appliedStrs = make([]string, len(applied))
		i := 0
		for k := range applied {
			appliedStrs[i] = k
			i++
		}
		slices.Sort(appliedStrs)
		logger.Info("Applied", "migrations", strings.Join(appliedStrs, " "))
		return nil
	})
}

func removeMigration(db kv.RwDB, ctx context.Context) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Delete(kv.Migrations, []byte(migration))
	})
}

var openSnapshotOnce sync.Once
var _allSnapshotsSingleton *freezeblocks.RoSnapshots
var _allBorSnapshotsSingleton *heimdall.RoSnapshots
var _allCaplinSnapshotsSingleton *freezeblocks.CaplinSnapshots
var _aggSingleton *libstate.Aggregator
var _bridgeStoreSingleton bridge.Store
var _heimdallStoreSingleton heimdall.Store

func allSnapshots(ctx context.Context, db kv.RoDB, logger log.Logger) (*freezeblocks.RoSnapshots, *heimdall.RoSnapshots, *libstate.Aggregator, *freezeblocks.CaplinSnapshots, bridge.Store, heimdall.Store, error) {
	var err error

	openSnapshotOnce.Do(func() {
		if syncCfg, err = features.EnableSyncCfg(db, syncCfg); err != nil {
			return
		}

		dirs := datadir.New(datadirCli)

		chainConfig := fromdb.ChainConfig(db)
		snapCfg := ethconfig.NewSnapCfg(true, true, true, chainConfig.ChainName)

		_allSnapshotsSingleton = freezeblocks.NewRoSnapshots(snapCfg, dirs.Snap, 0, logger)
		_allBorSnapshotsSingleton = heimdall.NewRoSnapshots(snapCfg, dirs.Snap, 0, logger)
		_bridgeStoreSingleton = bridge.NewSnapshotStore(bridge.NewDbStore(db), _allBorSnapshotsSingleton, chainConfig.Bor)
		_heimdallStoreSingleton = heimdall.NewSnapshotStore(heimdall.NewDbStore(db), _allBorSnapshotsSingleton)
		blockReader := freezeblocks.NewBlockReader(_allSnapshotsSingleton, _allBorSnapshotsSingleton, _heimdallStoreSingleton, _bridgeStoreSingleton)
		txNums := rawdbv3.TxNums.WithCustomReadTxNumFunc(freezeblocks.ReadTxNumFuncFromBlockReader(ctx, blockReader))

		_aggSingleton, err = libstate.NewAggregator(ctx, dirs, config3.DefaultStepSize, db, logger)
		if err != nil {
			err = fmt.Errorf("aggregator init: %w", err)
			return
		}
		if err = _aggSingleton.ReloadSalt(); err != nil {
			err = fmt.Errorf("aggregator ReloadSalt: %w", err)
			return
		}

		_aggSingleton.SetProduceMod(snapCfg.ProduceE3)

		g := &errgroup.Group{}
		g.Go(func() error {
			_allSnapshotsSingleton.OptimisticalyOpenFolder()
			return nil
		})
		g.Go(func() error {
			_allBorSnapshotsSingleton.OptimisticalyOpenFolder()
			return nil
		})
		g.Go(func() error {
			err := _aggSingleton.OpenFolder()
			if err != nil {
				return fmt.Errorf("aggregator opening: %w", err)
			}
			return nil
		})
		g.Go(func() error {
			chainConfig := fromdb.ChainConfig(db)
			var beaconConfig *clparams.BeaconChainConfig
			_, beaconConfig, _, err = clparams.GetConfigsByNetworkName(chainConfig.ChainName)
			if err == nil {
				_allCaplinSnapshotsSingleton = freezeblocks.NewCaplinSnapshots(snapCfg, beaconConfig, dirs, logger)
				if err = _allCaplinSnapshotsSingleton.OpenFolder(); err != nil {
					return fmt.Errorf("caplin snapshots: %w", err)
				}
				_allCaplinSnapshotsSingleton.LogStat("caplin")
			}
			return nil
		})

		g.Go(func() error {
			ls, er := os.Stat(filepath.Join(dirs.Snap, downloader.ProhibitNewDownloadsFileName))
			mtime := time.Time{}
			if er == nil {
				mtime = ls.ModTime()
			}
			logger.Info("[downloads]", "locked", er == nil, "at", mtime.Format("02 Jan 06 15:04 2006"))
			return nil
		})
		if err = g.Wait(); err != nil {
			return
		}

		_allSnapshotsSingleton.LogStat("blocks")
		_allBorSnapshotsSingleton.LogStat("bor")
		_ = db.View(context.Background(), func(tx kv.Tx) error {
			ac := _aggSingleton.BeginFilesRo()
			defer ac.Close()
			stats.LogStats(ac, tx, logger, func(endTxNumMinimax uint64) (uint64, error) {
				_, histBlockNumProgress, err := txNums.FindBlockNum(tx, endTxNumMinimax)
				if err != nil {
					return histBlockNumProgress, fmt.Errorf("findBlockNum(%d) fails: %w", endTxNumMinimax, err)
				}
				return histBlockNumProgress, nil
			})
			return nil
		})
	})

	if err != nil {
		log.Error("[snapshots] failed to open", "err", err)
		return nil, nil, nil, nil, nil, nil, err
	}
	return _allSnapshotsSingleton, _allBorSnapshotsSingleton, _aggSingleton, _allCaplinSnapshotsSingleton, _bridgeStoreSingleton, _heimdallStoreSingleton, nil
}

var openBlockReaderOnce sync.Once
var _blockReaderSingleton services.FullBlockReader
var _blockWriterSingleton *blockio.BlockWriter

func blocksIO(db kv.RoDB, logger log.Logger) (services.FullBlockReader, *blockio.BlockWriter) {
	openBlockReaderOnce.Do(func() {
		sn, borSn, _, _, bridgeStore, heimdallStore, err := allSnapshots(context.Background(), db, logger)
		if err != nil {
			panic(err)
		}
		_blockReaderSingleton = freezeblocks.NewBlockReader(sn, borSn, heimdallStore, bridgeStore)
		_blockWriterSingleton = blockio.NewBlockWriter()
	})
	return _blockReaderSingleton, _blockWriterSingleton
}

const blockBufferSize = 128

func newSync(ctx context.Context, db kv.TemporalRwDB, miningConfig *params.MiningConfig, logger log.Logger) (
	services.BlockRetire, consensus.Engine, *vm.Config, *stagedsync.Sync, *stagedsync.Sync, stagedsync.MiningState,
) {
	dirs, pm := datadir.New(datadirCli), fromdb.PruneMode(db)

	vmConfig := &vm.Config{}

	events := shards.NewEvents()

	genesis := core.GenesisBlockByChainName(chain)
	chainConfig, genesisBlock, genesisErr := core.CommitGenesisBlock(db, genesis, dirs, logger)
	if _, ok := genesisErr.(*chain2.ConfigCompatError); genesisErr != nil && !ok {
		panic(genesisErr)
	}
	//logger.Info("Initialised chain configuration", "config", chainConfig)

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	cfg := ethconfig.Defaults
	if chainTipMode {
		syncCfg.LoopBlockLimit = 1
		syncCfg.AlwaysGenerateChangesets = true
		noCommit = false
	}

	cfg.Sync = syncCfg

	cfg.Prune = pm
	cfg.BatchSize = batchSize
	cfg.TxPool.Disable = true
	cfg.Genesis = genesis
	if miningConfig != nil {
		cfg.Miner = *miningConfig
	}
	cfg.Dirs = dirs
	allSn, borSn, agg, _, _, _, err := allSnapshots(ctx, db, logger)
	if err != nil {
		panic(err) // we do already panic above on genesis error
	}
	cfg.Snapshot = allSn.Cfg()

	blockReader, blockWriter := blocksIO(db, logger)
	engine, heimdallClient := initConsensusEngine(ctx, chainConfig, cfg.Dirs.DataDir, db, blockReader, logger)

	statusDataProvider := sentry.NewStatusDataProvider(
		db,
		chainConfig,
		genesisBlock,
		chainConfig.ChainID.Uint64(),
		logger,
	)

	maxBlockBroadcastPeers := func(header *types.Header) uint { return 0 }

	sentryControlServer, err := sentry_multi_client.NewMultiClient(
		db,
		chainConfig,
		engine,
		nil,
		ethconfig.Defaults.Sync,
		blockReader,
		blockBufferSize,
		statusDataProvider,
		false,
		maxBlockBroadcastPeers,
		false, /* disableBlockDownload */
		logger,
	)
	if err != nil {
		panic(err)
	}

	blockSnapBuildSema := semaphore.NewWeighted(int64(dbg.BuildSnapshotAllowance))
	agg.SetSnapshotBuildSema(blockSnapBuildSema)

	notifications := shards.NewNotifications(nil)

	var (
		snapDb        kv.RwDB
		recents       *lru.ARCCache[common.Hash, *bor.Snapshot]
		signatures    *lru.ARCCache[common.Hash, common.Address]
		bridgeStore   bridge.Store
		heimdallStore heimdall.Store
	)
	if bor, ok := engine.(*bor.Bor); ok {
		snapDb = bor.DB
		recents = bor.Recents
		signatures = bor.Signatures
		bridgeStore = bridge.NewSnapshotStore(bridge.NewDbStore(db), borSn, chainConfig.Bor)
		heimdallStore = heimdall.NewSnapshotStore(heimdall.NewDbStore(db), borSn)
	}
	borSn.DownloadComplete() // mark as ready
	blockRetire := freezeblocks.NewBlockRetire(estimate.CompressSnapshot.Workers(), dirs, blockReader, blockWriter, db, heimdallStore, bridgeStore, chainConfig, &cfg, notifications.Events, blockSnapBuildSema, logger)

	stageList := stages2.NewDefaultStages(context.Background(), db, snapDb, p2p.Config{}, &cfg, sentryControlServer, notifications, nil, blockReader, blockRetire, nil, nil,
		heimdallClient, heimdallStore, bridgeStore, recents, signatures, logger, nil)
	sync := stagedsync.New(cfg.Sync, stageList, stagedsync.DefaultUnwindOrder, stagedsync.DefaultPruneOrder, logger, stages.ModeApplyingBlocks)

	miner := stagedsync.NewMiningState(&cfg.Miner)
	miningCancel := make(chan struct{})
	go func() {
		<-ctx.Done()
		close(miningCancel)
	}()

	miningSync := stagedsync.New(
		cfg.Sync,
		stagedsync.MiningStages(ctx,
			stagedsync.StageMiningCreateBlockCfg(db, miner, chainConfig, engine, nil, dirs.Tmp, blockReader),
			stagedsync.StageExecuteBlocksCfg(
				db,
				cfg.Prune,
				cfg.BatchSize,
				sentryControlServer.ChainConfig,
				sentryControlServer.Engine,
				&vm.Config{},
				notifications,
				cfg.StateStream,
				/*stateStream=*/ false,
				dirs,
				blockReader,
				sentryControlServer.Hd,
				cfg.Genesis,
				cfg.Sync,
				nil,
			),
			stagedsync.StageSendersCfg(db, sentryControlServer.ChainConfig, cfg.Sync, false, dirs.Tmp, cfg.Prune, blockReader, sentryControlServer.Hd),
			stagedsync.StageMiningExecCfg(db, miner, events, chainConfig, engine, &vm.Config{}, dirs.Tmp, nil, 0, nil, blockReader),
			stagedsync.StageMiningFinishCfg(db, chainConfig, engine, miner, miningCancel, blockReader, builder.NewLatestBlockBuiltStore()),
			false,
		),
		stagedsync.MiningUnwindOrder,
		stagedsync.MiningPruneOrder,
		logger,
		stages.ModeBlockProduction,
	)

	return blockRetire, engine, vmConfig, sync, miningSync, miner
}

func progress(tx kv.Getter, stage stages.SyncStage) uint64 {
	res, err := stages.GetStageProgress(tx, stage)
	if err != nil {
		panic(err)
	}
	return res
}

func stage(st *stagedsync.Sync, tx kv.Tx, db kv.RoDB, stage stages.SyncStage) *stagedsync.StageState {
	res, err := st.StageState(stage, tx, db, true, false)
	if err != nil {
		panic(err)
	}
	return res
}

func initConsensusEngine(ctx context.Context, cc *chain2.Config, dir string, db kv.RwDB, blockReader services.FullBlockReader, logger log.Logger) (engine consensus.Engine, heimdallClient heimdall.Client) {
	config := ethconfig.Defaults

	var consensusConfig interface{}
	if cc.Clique != nil {
		consensusConfig = params.CliqueSnapshot
	} else if cc.Aura != nil {
		consensusConfig = &config.Aura
	} else if cc.Bor != nil {
		consensusConfig = cc.Bor
		config.HeimdallURL = HeimdallURL
		if !config.WithoutHeimdall {
			heimdallClient = heimdall.NewHttpClient(config.HeimdallURL, logger)
		}
	} else {
		consensusConfig = &config.Ethash
	}
	return ethconsensusconfig.CreateConsensusEngine(ctx, &nodecfg.Config{Dirs: datadir.New(dir)}, cc, consensusConfig, config.Miner.Notify, config.Miner.Noverify,
		heimdallClient, config.WithoutHeimdall, blockReader, db.ReadOnly(), logger, nil, nil), heimdallClient
}
