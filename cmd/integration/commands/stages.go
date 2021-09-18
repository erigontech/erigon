package commands

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/fetcher"
	"github.com/ledgerwatch/erigon/eth/integrity"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/privateapi"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/params"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/txpool"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

var cmdStageBodies = &cobra.Command{
	Use:   "stage_bodies",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		if err := stageBodies(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageSenders = &cobra.Command{
	Use:   "stage_senders",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		ctx, _ := utils.RootContext()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		if err := stageSenders(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageExec = &cobra.Command{
	Use:   "stage_exec",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		if err := stageExec(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageTrie = &cobra.Command{
	Use:   "stage_trie",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		if err := stageTrie(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageHashState = &cobra.Command{
	Use:   "stage_hash_state",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		ctx, _ := utils.RootContext()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		if err := stageHashState(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageHistory = &cobra.Command{
	Use:   "stage_history",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		if err := stageHistory(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdLogIndex = &cobra.Command{
	Use:   "stage_log_index",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		if err := stageLogIndex(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdCallTraces = &cobra.Command{
	Use:   "stage_call_traces",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		if err := stageCallTraces(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageTxLookup = &cobra.Command{
	Use:   "stage_tx_lookup",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		if err := stageTxLookup(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}
var cmdPrintStages = &cobra.Command{
	Use:   "print_stages",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, false)
		defer db.Close()

		if err := printAllStages(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdPrintMigrations = &cobra.Command{
	Use:   "print_migrations",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, false)
		defer db.Close()
		if err := printAppliedMigrations(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdRemoveMigration = &cobra.Command{
	Use:   "remove_migration",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, false)
		defer db.Close()
		if err := removeMigration(db, ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdRunMigrations = &cobra.Command{
	Use:   "run_migrations",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()
		// Nothing to do, migrations will be applied automatically
		return nil
	},
}

var cmdSetPrune = &cobra.Command{
	Use:   "set_prune",
	Short: "Override existing --prune flag value (if you know what you are doing)",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()
		return overrideStorageMode(db)
	},
}

func init() {
	withDatadir(cmdPrintStages)
	withChain(cmdPrintStages)
	rootCmd.AddCommand(cmdPrintStages)

	withReset(cmdStageSenders)
	withBlock(cmdStageSenders)
	withUnwind(cmdStageSenders)
	withDatadir(cmdStageSenders)
	withChain(cmdStageSenders)

	rootCmd.AddCommand(cmdStageSenders)

	withDatadir(cmdStageBodies)
	withUnwind(cmdStageBodies)
	withChain(cmdStageBodies)

	rootCmd.AddCommand(cmdStageBodies)

	withDatadir(cmdStageExec)
	withReset(cmdStageExec)
	withBlock(cmdStageExec)
	withUnwind(cmdStageExec)
	withPruneTo(cmdStageExec)
	withBatchSize(cmdStageExec)
	withTxTrace(cmdStageExec)
	withChain(cmdStageExec)

	rootCmd.AddCommand(cmdStageExec)

	withDatadir(cmdStageHashState)
	withReset(cmdStageHashState)
	withBlock(cmdStageHashState)
	withUnwind(cmdStageHashState)
	withPruneTo(cmdStageHashState)
	withBatchSize(cmdStageHashState)
	withChain(cmdStageHashState)

	rootCmd.AddCommand(cmdStageHashState)

	withDatadir(cmdStageTrie)
	withReset(cmdStageTrie)
	withBlock(cmdStageTrie)
	withUnwind(cmdStageTrie)
	withPruneTo(cmdStageTrie)
	withIntegrityChecks(cmdStageTrie)
	withChain(cmdStageTrie)

	rootCmd.AddCommand(cmdStageTrie)

	withDatadir(cmdStageHistory)
	withReset(cmdStageHistory)
	withBlock(cmdStageHistory)
	withUnwind(cmdStageHistory)
	withPruneTo(cmdStageHistory)
	withChain(cmdStageHistory)

	rootCmd.AddCommand(cmdStageHistory)

	withDatadir(cmdLogIndex)
	withReset(cmdLogIndex)
	withBlock(cmdLogIndex)
	withUnwind(cmdLogIndex)
	withPruneTo(cmdLogIndex)
	withChain(cmdLogIndex)

	rootCmd.AddCommand(cmdLogIndex)

	withDatadir(cmdCallTraces)
	withReset(cmdCallTraces)
	withBlock(cmdCallTraces)
	withUnwind(cmdCallTraces)
	withPruneTo(cmdCallTraces)
	withChain(cmdCallTraces)

	rootCmd.AddCommand(cmdCallTraces)

	withReset(cmdStageTxLookup)
	withBlock(cmdStageTxLookup)
	withUnwind(cmdStageTxLookup)
	withDatadir(cmdStageTxLookup)
	withPruneTo(cmdStageTxLookup)
	withChain(cmdStageTxLookup)

	rootCmd.AddCommand(cmdStageTxLookup)

	withDatadir(cmdPrintMigrations)
	rootCmd.AddCommand(cmdPrintMigrations)

	withDatadir(cmdRemoveMigration)
	withMigration(cmdRemoveMigration)
	withChain(cmdRemoveMigration)
	rootCmd.AddCommand(cmdRemoveMigration)

	withDatadir(cmdRunMigrations)
	withChain(cmdRunMigrations)
	rootCmd.AddCommand(cmdRunMigrations)

	withDatadir(cmdSetPrune)
	withChain(cmdSetPrune)
	cmdSetPrune.Flags().StringVar(&pruneFlag, "prune", "hrtc", "")
	cmdSetPrune.Flags().Uint64Var(&pruneH, "--prune.h.older", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneR, "--prune.r.older", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneT, "--prune.t.older", 0, "")
	cmdSetPrune.Flags().Uint64Var(&pruneC, "--prune.c.older", 0, "")
	cmdSetPrune.Flags().StringSliceVar(&experiments, "experiments", nil, "Storage mode to override database")
	rootCmd.AddCommand(cmdSetPrune)
}

func stageBodies(db kv.RwDB, ctx context.Context) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		if unwind > 0 {
			progress, err := stages.GetStageProgress(tx, stages.Bodies)
			if err != nil {
				return fmt.Errorf("read Bodies progress: %w", err)
			}
			if unwind > progress {
				return fmt.Errorf("cannot unwind past 0")
			}
			if err = stages.SaveStageProgress(tx, stages.Bodies, progress-unwind); err != nil {
				return fmt.Errorf("saving Bodies progress failed: %w", err)
			}
			progress, err = stages.GetStageProgress(tx, stages.Bodies)
			if err != nil {
				return fmt.Errorf("re-read Bodies progress: %w", err)
			}
			log.Info("Progress", "bodies", progress)
			return nil
		}
		log.Info("This command only works with --unwind option")
		return nil
	})
}

func stageSenders(db kv.RwDB, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)
	_, _, chainConfig, _, _, sync, _, _ := newSync(ctx, db, nil)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = resetSenders(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	s := stage(sync, tx, nil, stages.Senders)
	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	pm, err := prune.Get(tx)
	if err != nil {
		return err
	}
	cfg := stagedsync.StageSendersCfg(db, chainConfig, tmpdir, pm)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.Senders, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindSendersStage(u, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnRecoverSendersStage(cfg, s, sync, tx, block, ctx)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func stageExec(db kv.RwDB, ctx context.Context) error {
	pm, engine, chainConfig, vmConfig, _, sync, _, _ := newSync(ctx, db, nil)

	if reset {
		genesis, _ := byChain()
		if err := db.Update(ctx, func(tx kv.RwTx) error { return resetExec(tx, genesis) }); err != nil {
			return err
		}
		return nil
	}
	if txtrace {
		// Activate tracing and writing into json files for each transaction
		vmConfig.Tracer = nil
		vmConfig.Debug = true
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	s := stage(sync, nil, db, stages.Execution)

	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}

	cfg := stagedsync.StageExecuteBlocksCfg(db, pm, batchSize, nil, chainConfig, engine, vmConfig, nil, false, tmpDBPath)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.Execution, s.BlockNumber-unwind, s.BlockNumber)
		err := stagedsync.UnwindExecutionStage(u, s, nil, ctx, cfg, false)
		if err != nil {
			return err
		}
		return nil
	}

	if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.Execution, s.BlockNumber, nil, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneExecutionStage(p, nil, cfg, ctx, false)
		if err != nil {
			return err
		}
		return nil
	}

	err := stagedsync.SpawnExecuteBlocksStage(s, sync, nil, block, ctx, cfg, false)
	if err != nil {
		return err
	}
	return nil
}

func stageTrie(db kv.RwDB, ctx context.Context) error {
	pm, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)
	tmpdir := path.Join(datadir, etl.TmpDirName)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		if err := stagedsync.ResetIH(tx); err != nil {
			return err
		}
		return tx.Commit()
	}
	execStage := stage(sync, tx, nil, stages.Execution)
	s := stage(sync, tx, nil, stages.IntermediateHashes)

	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}
	fmt.Printf("distance: %d\n", pm.History)

	log.Info("Stage4", "progress", execStage.BlockNumber)
	log.Info("Stage5", "progress", s.BlockNumber)
	cfg := stagedsync.StageTrieCfg(db, true, true, tmpdir)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.IntermediateHashes, s.BlockNumber-unwind, s.BlockNumber)
		if err := stagedsync.UnwindIntermediateHashesStage(u, s, tx, cfg, ctx); err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.IntermediateHashes, s.BlockNumber, tx, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneIntermediateHashesStage(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		if _, err := stagedsync.SpawnIntermediateHashesStage(s, sync /* Unwinder */, tx, cfg, ctx); err != nil {
			return err
		}
	}
	integrity.Trie(tx, integritySlow, ctx)
	return tx.Commit()
}

func stageHashState(db kv.RwDB, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	pm, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = stagedsync.ResetHashState(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	s := stage(sync, tx, nil, stages.HashState)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}

	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)
	cfg := stagedsync.StageHashStateCfg(db, tmpdir)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.HashState, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindHashStateStage(u, s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.HashState, s.BlockNumber, tx, nil)
		if err != nil {
			return err
		}
		err = stagedsync.PruneHashStateStage(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnHashStateStage(s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageLogIndex(db kv.RwDB, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	pm, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = resetLogIndex(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	execAt := progress(tx, stages.Execution)
	s := stage(sync, tx, nil, stages.LogIndex)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}

	log.Info("Stage exec", "progress", execAt)
	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	cfg := stagedsync.StageLogIndexCfg(db, pm, tmpdir)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.LogIndex, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindLogIndex(u, s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.LogIndex, s.BlockNumber, nil, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneLogIndex(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		if err := stagedsync.SpawnLogIndex(s, tx, cfg, ctx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageCallTraces(kv kv.RwDB, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	pm, _, _, _, _, sync, _, _ := newSync(ctx, kv, nil)
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = resetCallTraces(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	execStage := progress(tx, stages.Execution)
	s := stage(sync, tx, nil, stages.CallTraces)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}
	log.Info("ID exec", "progress", execStage)
	if block != 0 {
		s.BlockNumber = block
		log.Info("Overriding initial state", "block", block)
	}
	log.Info("ID call traces", "progress", s.BlockNumber)

	cfg := stagedsync.StageCallTracesCfg(kv, pm, block, tmpdir)

	if unwind > 0 {
		u := sync.NewUnwindState(stages.CallTraces, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindCallTraces(u, s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.CallTraces, s.BlockNumber, tx, nil)
		if err != nil {
			return err
		}
		err = stagedsync.PruneCallTraces(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		if err := stagedsync.SpawnCallTraces(s, tx, cfg, ctx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageHistory(db kv.RwDB, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)
	pm, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = resetHistory(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	execStage := progress(tx, stages.Execution)
	stageStorage := stage(sync, tx, nil, stages.StorageHistoryIndex)
	stageAcc := stage(sync, tx, nil, stages.AccountHistoryIndex)
	if pruneTo > 0 {
		pm.History = prune.Distance(stageAcc.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(stageAcc.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(stageAcc.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(stageAcc.BlockNumber - pruneTo)
	}
	log.Info("ID exec", "progress", execStage)
	log.Info("ID acc history", "progress", stageAcc.BlockNumber)
	log.Info("ID storage history", "progress", stageStorage.BlockNumber)

	cfg := stagedsync.StageHistoryCfg(db, pm, tmpdir)
	if unwind > 0 { //nolint:staticcheck
		u := sync.NewUnwindState(stages.StorageHistoryIndex, stageStorage.BlockNumber-unwind, stageStorage.BlockNumber)
		if err := stagedsync.UnwindStorageHistoryIndex(u, stageStorage, tx, cfg, ctx); err != nil {
			return err
		}
		u = sync.NewUnwindState(stages.AccountHistoryIndex, stageAcc.BlockNumber-unwind, stageAcc.BlockNumber)
		if err := stagedsync.UnwindAccountHistoryIndex(u, stageAcc, tx, cfg, ctx); err != nil {
			return err
		}
	} else if pruneTo > 0 {
		pa, err := sync.PruneStageState(stages.AccountHistoryIndex, stageAcc.BlockNumber, tx, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneAccountHistoryIndex(pa, tx, cfg, ctx)
		if err != nil {
			return err
		}
		ps, err := sync.PruneStageState(stages.StorageHistoryIndex, stageStorage.BlockNumber, tx, db)
		if err != nil {
			return err
		}
		err = stagedsync.PruneAccountHistoryIndex(ps, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		if err := stagedsync.SpawnAccountHistoryIndex(stageAcc, tx, cfg, ctx); err != nil {
			return err
		}
		if err := stagedsync.SpawnStorageHistoryIndex(stageStorage, tx, cfg, ctx); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageTxLookup(db kv.RwDB, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	pm, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	if reset {
		err = resetTxLookup(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	s := stage(sync, tx, nil, stages.TxLookup)
	if pruneTo > 0 {
		pm.History = prune.Distance(s.BlockNumber - pruneTo)
		pm.Receipts = prune.Distance(s.BlockNumber - pruneTo)
		pm.CallTraces = prune.Distance(s.BlockNumber - pruneTo)
		pm.TxIndex = prune.Distance(s.BlockNumber - pruneTo)
	}
	log.Info("Stage", "name", s.ID, "progress", s.BlockNumber)

	cfg := stagedsync.StageTxLookupCfg(db, pm, tmpdir)
	if unwind > 0 {
		u := sync.NewUnwindState(stages.TxLookup, s.BlockNumber-unwind, s.BlockNumber)
		err = stagedsync.UnwindTxLookup(u, s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else if pruneTo > 0 {
		p, err := sync.PruneStageState(stages.TxPool, s.BlockNumber, tx, nil)
		if err != nil {
			return err
		}
		err = stagedsync.PruneTxLookup(p, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnTxLookup(s, tx, cfg, ctx)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func printAllStages(db kv.RoDB, ctx context.Context) error {
	return db.View(ctx, func(tx kv.Tx) error { return printStages(tx) })
}

func printAppliedMigrations(db kv.RwDB, ctx context.Context) error {
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
		sort.Strings(appliedStrs)
		log.Info("Applied", "migrations", strings.Join(appliedStrs, " "))
		return nil
	})
}

func removeMigration(db kv.RwDB, ctx context.Context) error {
	return db.Update(ctx, func(tx kv.RwTx) error {
		return tx.Delete(kv.Migrations, []byte(migration), nil)
	})
}

func byChain() (*core.Genesis, *params.ChainConfig) {
	var chainConfig *params.ChainConfig

	var genesis *core.Genesis
	switch chain {
	case "", params.MainnetChainName:
		chainConfig = params.MainnetChainConfig
		genesis = core.DefaultGenesisBlock()
	case params.RopstenChainName:
		chainConfig = params.RopstenChainConfig
		genesis = core.DefaultRopstenGenesisBlock()
	case params.GoerliChainName:
		chainConfig = params.GoerliChainConfig
		genesis = core.DefaultGoerliGenesisBlock()
	case params.RinkebyChainName:
		chainConfig = params.RinkebyChainConfig
		genesis = core.DefaultRinkebyGenesisBlock()
	case params.SokolChainName:
		chainConfig = params.SokolChainConfig
		genesis = core.DefaultSokolGenesisBlock()
	}
	return genesis, chainConfig
}

func newSync(ctx context.Context, db kv.RwDB, miningConfig *params.MiningConfig) (prune.Mode, consensus.Engine, *params.ChainConfig, *vm.Config, *core.TxPool, *stagedsync.Sync, *stagedsync.Sync, stagedsync.MiningState) {
	tmpdir := path.Join(datadir, etl.TmpDirName)
	snapshotDir = path.Join(datadir, "snapshot")
	logger := log.New()

	var pm prune.Mode
	var err error
	if err = db.View(context.Background(), func(tx kv.Tx) error {
		pm, err = prune.Get(tx)
		if err != nil {
			return err
		}
		if err = stagedsync.UpdateMetrics(tx); err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	vmConfig := &vm.Config{}

	genesis, chainConfig := byChain()
	var engine consensus.Engine
	engine = ethash.NewFaker()
	switch chain {
	case params.SokolChainName:
		engine = ethconfig.CreateConsensusEngine(chainConfig, logger, &params.AuRaConfig{DBPath: path.Join(datadir, "aura")}, nil, false)
	}

	events := privateapi.NewEvents()

	txPool := core.NewTxPool(ethconfig.Defaults.TxPool, chainConfig, db)

	chainConfig, genesisBlock, genesisErr := core.CommitGenesisBlock(db, genesis)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		panic(genesisErr)
	}
	log.Info("Initialised chain configuration", "config", chainConfig)

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	blockDownloaderWindow := 65536
	downloadServer, err := download.NewControlServer(db, "", chainConfig, genesisBlock.Hash(), engine, 1, nil, blockDownloaderWindow)
	if err != nil {
		panic(err)
	}

	txPoolP2PServer, err := txpool.NewP2PServer(context.Background(), nil, txPool)
	if err != nil {
		panic(err)
	}
	fetchTx := func(peerID string, hashes []common.Hash) error {
		txPoolP2PServer.SendTxsRequest(context.TODO(), peerID, hashes)
		return nil
	}

	txPoolP2PServer.TxFetcher = fetcher.NewTxFetcher(txPool.Has, txPool.AddRemotes, fetchTx)

	cfg := ethconfig.Defaults
	cfg.Prune = pm
	cfg.BatchSize = batchSize
	cfg.TxPool.Disable = true
	if miningConfig != nil {
		cfg.Miner = *miningConfig
	}

	sync, err := stages2.NewStagedSync(context.Background(), logger, db, p2p.Config{}, cfg,
		downloadServer,
		tmpdir,
		txPool,
		txPoolP2PServer,
		nil, nil, nil,
	)
	if err != nil {
		panic(err)
	}
	miner := stagedsync.NewMiningState(&cfg.Miner)

	miningSync := stagedsync.New(
		stagedsync.MiningStages(ctx,
			stagedsync.StageMiningCreateBlockCfg(db, miner, *chainConfig, engine, txPool, nil, nil, tmpdir),
			stagedsync.StageMiningExecCfg(db, miner, events, *chainConfig, engine, &vm.Config{}, tmpdir),
			stagedsync.StageHashStateCfg(db, tmpdir),
			stagedsync.StageTrieCfg(db, false, true, tmpdir),
			stagedsync.StageMiningFinishCfg(db, *chainConfig, engine, miner, ctx.Done()),
		),
		stagedsync.MiningUnwindOrder,
		stagedsync.MiningPruneOrder,
	)

	return pm, engine, chainConfig, vmConfig, txPool, sync, miningSync, miner
}

func progress(tx kv.Getter, stage stages.SyncStage) uint64 {
	res, err := stages.GetStageProgress(tx, stage)
	if err != nil {
		panic(err)
	}
	return res
}

func stage(st *stagedsync.Sync, tx kv.Tx, db kv.RoDB, stage stages.SyncStage) *stagedsync.StageState {
	res, err := st.StageState(stage, tx, db)
	if err != nil {
		panic(err)
	}
	return res
}

func overrideStorageMode(db kv.RwDB) error {
	pm, err := prune.FromCli(pruneFlag, pruneH, pruneR, pruneT, pruneC, experiments)
	if err != nil {
		return err
	}
	return db.Update(context.Background(), func(tx kv.RwTx) error {
		if err = prune.Override(tx, pm); err != nil {
			return err
		}
		pm, err = prune.Get(tx)
		if err != nil {
			return err
		}
		log.Info("Storage mode in DB", "mode", pm.String())
		return nil
	})
}
