package commands

import (
	"context"
	"fmt"
	"path"
	"sort"
	"strings"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/common/etl"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/core/vm"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/fetcher"
	"github.com/ledgerwatch/erigon/eth/integrity"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/migrations"
	"github.com/ledgerwatch/erigon/params"
	stages2 "github.com/ledgerwatch/erigon/turbo/stages"
	"github.com/ledgerwatch/erigon/turbo/txpool"
	"github.com/spf13/cobra"
)

var cmdStageBodies = &cobra.Command{
	Use:   "stage_bodies",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		db := openDB(chaindata, true)
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
		ctx, _ := utils.RootContext()
		db := openDB(chaindata, true)
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
		db := openDB(chaindata, true)
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
		db := openDB(chaindata, true)
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
		ctx, _ := utils.RootContext()
		db := openDB(chaindata, true)
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
		db := openDB(chaindata, true)
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
		db := openDB(chaindata, true)
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
		db := openDB(chaindata, true)
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
		db := openDB(chaindata, true)
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
		db := openDB(chaindata, false)
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
		db := openDB(chaindata, false)
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
		db := openDB(chaindata, false)
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
		db := openDB(chaindata, true)
		defer db.Close()
		// Nothing to do, migrations will be applied automatically
		return nil
	},
}

var cmdSetStorageMode = &cobra.Command{
	Use:   "set_storage_mode",
	Short: "Override storage mode (if you know what you are doing)",
	RunE: func(cmd *cobra.Command, args []string) error {
		db := openDB(chaindata, true)
		defer db.Close()
		return overrideStorageMode(db)
	},
}

func init() {
	withDatadir(cmdPrintStages)
	rootCmd.AddCommand(cmdPrintStages)

	//withChaindata(cmdStageSenders)
	withReset(cmdStageSenders)
	withBlock(cmdStageSenders)
	withUnwind(cmdStageSenders)
	withDatadir(cmdStageSenders)

	rootCmd.AddCommand(cmdStageSenders)

	withDatadir(cmdStageBodies)
	withUnwind(cmdStageBodies)

	rootCmd.AddCommand(cmdStageBodies)

	withDatadir(cmdStageExec)
	withReset(cmdStageExec)
	withBlock(cmdStageExec)
	withUnwind(cmdStageExec)
	withBatchSize(cmdStageExec)
	withSilkworm(cmdStageExec)
	withTxTrace(cmdStageExec)
	withChain(cmdStageExec)

	rootCmd.AddCommand(cmdStageExec)

	withDatadir(cmdStageHashState)
	withReset(cmdStageHashState)
	withBlock(cmdStageHashState)
	withUnwind(cmdStageHashState)
	withBatchSize(cmdStageHashState)
	withChain(cmdStageHashState)

	rootCmd.AddCommand(cmdStageHashState)

	withDatadir(cmdStageTrie)
	withReset(cmdStageTrie)
	withBlock(cmdStageTrie)
	withUnwind(cmdStageTrie)
	withIntegrityChecks(cmdStageTrie)
	withChain(cmdStageTrie)

	rootCmd.AddCommand(cmdStageTrie)

	withDatadir(cmdStageHistory)
	withReset(cmdStageHistory)
	withBlock(cmdStageHistory)
	withUnwind(cmdStageHistory)

	rootCmd.AddCommand(cmdStageHistory)

	withDatadir(cmdLogIndex)
	withReset(cmdLogIndex)
	withBlock(cmdLogIndex)
	withUnwind(cmdLogIndex)

	rootCmd.AddCommand(cmdLogIndex)

	withDatadir(cmdCallTraces)
	withReset(cmdCallTraces)
	withBlock(cmdCallTraces)
	withUnwind(cmdCallTraces)

	rootCmd.AddCommand(cmdCallTraces)

	withReset(cmdStageTxLookup)
	withBlock(cmdStageTxLookup)
	withUnwind(cmdStageTxLookup)
	withDatadir(cmdStageTxLookup)

	rootCmd.AddCommand(cmdStageTxLookup)

	withDatadir(cmdPrintMigrations)
	rootCmd.AddCommand(cmdPrintMigrations)

	withDatadir(cmdRemoveMigration)
	withMigration(cmdRemoveMigration)
	rootCmd.AddCommand(cmdRemoveMigration)

	withDatadir(cmdRunMigrations)
	rootCmd.AddCommand(cmdRunMigrations)

	withDatadir(cmdSetStorageMode)
	cmdSetStorageMode.Flags().StringVar(&storageMode, "storage-mode", "htre", "Storage mode to override database")
	rootCmd.AddCommand(cmdSetStorageMode)
}

func stageBodies(db ethdb.RwKV, ctx context.Context) error {
	return db.Update(ctx, func(tx ethdb.RwTx) error {
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

func stageSenders(db ethdb.RwKV, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)
	sm, engine, chainConfig, vmConfig, _, st, _, _, _ := newSync(db)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, ethdb.NewObjectDatabase(db), tx, "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil, nil)
	if err != nil {
		return nil
	}

	if reset {
		err = resetSenders(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	stage2 := stage(sync, tx, stages.Bodies)
	stage3 := stage(sync, tx, stages.Senders)
	log.Info("Stage2", "progress", stage2.BlockNumber)
	log.Info("Stage3", "progress", stage3.BlockNumber)
	ch := make(chan struct{})
	defer close(ch)

	cfg := stagedsync.StageSendersCfg(db, params.MainnetChainConfig, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.Senders, UnwindPoint: stage3.BlockNumber - unwind}
		err = stagedsync.UnwindSendersStage(u, stage3, tx, cfg)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnRecoverSendersStage(cfg, stage3, sync, tx, block, ch)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func stageExec(db ethdb.RwKV, ctx context.Context) error {
	sm, engine, chainConfig, vmConfig, _, st, _, _, _ := newSync(db)
	tmpdir := path.Join(datadir, etl.TmpDirName)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, ethdb.NewObjectDatabase(db), nil, "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil, nil)
	if err != nil {
		return nil
	}

	if reset {
		if err = db.Update(ctx, func(tx ethdb.RwTx) error { return resetExec(tx) }); err != nil {
			return err
		}
	}
	if txtrace {
		// Activate tracing and writing into json files for each transaction
		vmConfig.Tracer = nil
		vmConfig.Debug = true
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	var execStage *stagedsync.StageState
	err = db.View(ctx, func(tx ethdb.Tx) error {
		execStage = stage(sync, tx, stages.Execution)
		return nil
	})
	if err != nil {
		return err
	}

	log.Info("Stage4", "progress", execStage.BlockNumber)
	ch := ctx.Done()
	cfg := stagedsync.StageExecuteBlocksCfg(db, sm.Receipts, sm.CallTraces, sm.TEVM, 0, batchSize, nil, nil, nil, chainConfig, engine, vmConfig, tmpDBPath)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: execStage.BlockNumber - unwind}
		err = stagedsync.UnwindExecutionStage(u, execStage, nil, ch, cfg, nil)
		if err != nil {
			return err
		}
		return nil
	}

	err = stagedsync.SpawnExecuteBlocksStage(execStage, sync, nil, block, ch, cfg, nil)
	if err != nil {
		return err
	}
	return nil
}

func stageTrie(db ethdb.RwKV, ctx context.Context) error {
	sm, engine, chainConfig, vmConfig, _, st, _, _, _ := newSync(db)
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, ethdb.NewObjectDatabase(db), nil, "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil, nil)
	if err != nil {
		return nil
	}

	if reset {
		if err = db.Update(ctx, func(tx ethdb.RwTx) error { return stagedsync.ResetIH(tx) }); err != nil {
			return err
		}
	}
	var execStage, trieStage *stagedsync.StageState
	if err = db.View(ctx, func(tx ethdb.Tx) error {
		execStage = stage(sync, tx, stages.Execution)
		trieStage = stage(sync, tx, stages.IntermediateHashes)
		return nil
	}); err != nil {
		return err
	}

	log.Info("Stage4", "progress", execStage.BlockNumber)
	log.Info("Stage5", "progress", trieStage.BlockNumber)
	ch := ctx.Done()
	cfg := stagedsync.StageTrieCfg(db, true, true, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: trieStage.BlockNumber - unwind}
		if err := stagedsync.UnwindIntermediateHashesStage(u, trieStage, nil, cfg, ch); err != nil {
			return err
		}
	} else {
		if _, err := stagedsync.SpawnIntermediateHashesStage(trieStage, nil /* Unwinder */, nil, cfg, ch); err != nil {
			return err
		}
	}
	if err = db.View(ctx, func(tx ethdb.Tx) error {
		integrity.Trie(tx, integritySlow, ch)
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func stageHashState(db ethdb.RwKV, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _, _, _ := newSync(db)
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, ethdb.NewObjectDatabase(db), tx, "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil, nil)
	if err != nil {
		return nil
	}

	if reset {
		err = stagedsync.ResetHashState(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	stage5 := stage(sync, tx, stages.IntermediateHashes)
	stage6 := stage(sync, tx, stages.HashState)
	log.Info("Stage5", "progress", stage5.BlockNumber)
	log.Info("Stage6", "progress", stage6.BlockNumber)
	ch := ctx.Done()
	cfg := stagedsync.StageHashStateCfg(db, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.HashState, UnwindPoint: stage6.BlockNumber - unwind}
		err = stagedsync.UnwindHashStateStage(u, stage6, tx, cfg, ch)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnHashStateStage(stage6, tx, cfg, ch)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageLogIndex(db ethdb.RwKV, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _, _, _ := newSync(db)
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, ethdb.NewObjectDatabase(db), tx, "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil, nil)
	if err != nil {
		return nil
	}

	if reset {
		err = resetLogIndex(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	execAt := progress(tx, stages.Execution)
	s := stage(sync, tx, stages.LogIndex)
	log.Info("Stage exec", "progress", execAt)
	log.Info("Stage log index", "progress", s.BlockNumber)
	ch := ctx.Done()

	cfg := stagedsync.StageLogIndexCfg(db, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.LogIndex, UnwindPoint: s.BlockNumber - unwind}
		err = stagedsync.UnwindLogIndex(u, s, tx, cfg, ch)
		if err != nil {
			return err
		}
	} else {
		if err := stagedsync.SpawnLogIndex(s, tx, cfg, ch); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageCallTraces(kv ethdb.RwKV, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _, _, _ := newSync(kv)
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, ethdb.NewObjectDatabase(kv), tx, "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil, nil)
	if err != nil {
		return nil
	}

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
	s := stage(sync, tx, stages.CallTraces)
	log.Info("Stage exec", "progress", execStage)
	if block != 0 {
		s.BlockNumber = block
		log.Info("Overriding initial state", "block", block)
	}
	log.Info("Stage call traces", "progress", s.BlockNumber)
	ch := ctx.Done()
	cfg := stagedsync.StageCallTracesCfg(kv, block, batchSize, tmpdir, chainConfig, engine)

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.CallTraces, UnwindPoint: s.BlockNumber - unwind}
		err = stagedsync.UnwindCallTraces(u, s, tx, ch, cfg)
		if err != nil {
			return err
		}
	} else {
		if err := stagedsync.SpawnCallTraces(s, tx, ch, cfg); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageHistory(db ethdb.RwKV, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)
	sm, engine, chainConfig, vmConfig, _, st, _, _, _ := newSync(db)
	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, ethdb.NewObjectDatabase(db), tx, "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil, nil)
	if err != nil {
		return nil
	}

	if reset {
		err = resetHistory(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	execStage := progress(tx, stages.Execution)
	stageStorage := stage(sync, tx, stages.StorageHistoryIndex)
	stageAcc := stage(sync, tx, stages.AccountHistoryIndex)
	log.Info("Stage exec", "progress", execStage)
	log.Info("Stage acc history", "progress", stageAcc.BlockNumber)
	log.Info("Stage storage history", "progress", stageStorage.BlockNumber)
	ch := ctx.Done()

	cfg := stagedsync.StageHistoryCfg(db, tmpdir)
	if unwind > 0 { //nolint:staticcheck
		u := &stagedsync.UnwindState{Stage: stages.StorageHistoryIndex, UnwindPoint: stageStorage.BlockNumber - unwind}
		if err := stagedsync.UnwindStorageHistoryIndex(u, stageStorage, tx, cfg, ch); err != nil {
			return err
		}
		u = &stagedsync.UnwindState{Stage: stages.AccountHistoryIndex, UnwindPoint: stageAcc.BlockNumber - unwind}
		if err := stagedsync.UnwindAccountHistoryIndex(u, stageAcc, tx, cfg, ch); err != nil {
			return err
		}
	} else {

		if err := stagedsync.SpawnAccountHistoryIndex(stageAcc, tx, cfg, ch); err != nil {
			return err
		}
		if err := stagedsync.SpawnStorageHistoryIndex(stageStorage, tx, cfg, ch); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageTxLookup(db ethdb.RwKV, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _, _, _ := newSync(db)

	tx, err := db.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, ethdb.NewObjectDatabase(db), tx, "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil, nil)
	if err != nil {
		return nil
	}

	if reset {
		err = resetTxLookup(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	stage9 := stage(sync, tx, stages.TxLookup)
	log.Info("Stage9", "progress", stage9.BlockNumber)
	ch := ctx.Done()

	cfg := stagedsync.StageTxLookupCfg(db, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.TxLookup, UnwindPoint: stage9.BlockNumber - unwind}
		s := stage(sync, tx, stages.TxLookup)
		err = stagedsync.UnwindTxLookup(u, s, tx, cfg, ch)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnTxLookup(stage9, tx, cfg, ch)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func printAllStages(db ethdb.RoKV, ctx context.Context) error {
	return db.View(ctx, func(tx ethdb.Tx) error { return printStages(tx) })
}

func printAppliedMigrations(db ethdb.RwKV, ctx context.Context) error {
	return db.View(ctx, func(tx ethdb.Tx) error {
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

func removeMigration(db ethdb.RwKV, ctx context.Context) error {
	return db.Update(ctx, func(tx ethdb.RwTx) error {
		return tx.Delete(dbutils.Migrations, []byte(migration), nil)
	})
}

func newSync(db ethdb.RwKV) (ethdb.StorageMode, consensus.Engine, *params.ChainConfig, *vm.Config, *core.TxPool, *stagedsync.StagedSync, *stagedsync.StagedSync, chan *types.Block, chan *types.Block) {
	var sm ethdb.StorageMode
	var err error
	if err = db.View(context.Background(), func(tx ethdb.Tx) error {
		sm, err = ethdb.GetStorageModeFromDB(tx)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		panic(err)
	}
	vmConfig := &vm.Config{NoReceipts: !sm.Receipts}
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
	case params.CalaverasChainName:
		chainConfig = params.CalaverasChainConfig
		genesis = core.DefaultCalaverasGenesisBlock()
	}
	events := remotedbserver.NewEvents()

	txPool := core.NewTxPool(ethconfig.Defaults.TxPool, chainConfig, db)

	chainConfig, genesisBlock, genesisErr := core.CommitGenesisBlock(db, genesis, sm.History)
	if _, ok := genesisErr.(*params.ConfigCompatError); genesisErr != nil && !ok {
		panic(genesisErr)
	}
	log.Info("Initialised chain configuration", "config", chainConfig)

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	bodyDownloadTimeoutSeconds := 30 // TODO: convert to duration, make configurable

	engine := ethash.NewFaker()
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
	st, err := stages2.NewStagedSync2(context.Background(), db, sm, batchSize,
		bodyDownloadTimeoutSeconds,
		downloadServer,
		path.Join(datadir, etl.TmpDirName),
		txPool,
		txPoolP2PServer,
	)
	if err != nil {
		panic(err)
	}
	pendingResultCh := make(chan *types.Block, 1)
	miningResultCh := make(chan *types.Block, 1)

	stMining := stagedsync.New(
		stagedsync.MiningStages(
			stagedsync.StageMiningCreateBlockCfg(db, ethconfig.Defaults.Miner, *chainConfig, engine, txPool, ""),
			stagedsync.StageMiningExecCfg(db, ethconfig.Defaults.Miner, events, *chainConfig, engine, &vm.Config{}, ""),
			stagedsync.StageHashStateCfg(db, ""),
			stagedsync.StageTrieCfg(db, false, true, ""),
			stagedsync.StageMiningFinishCfg(db, *chainConfig, engine, pendingResultCh, miningResultCh, nil),
		),
		stagedsync.MiningUnwindOrder(),
		stagedsync.OptionalParameters{},
	)
	return sm, engine, chainConfig, vmConfig, txPool, st, stMining, pendingResultCh, miningResultCh
}

func progress(tx ethdb.KVGetter, stage stages.SyncStage) uint64 {
	res, err := stages.GetStageProgress(tx, stage)
	if err != nil {
		panic(err)
	}
	return res
}

func stage(st *stagedsync.State, db ethdb.KVGetter, stage stages.SyncStage) *stagedsync.StageState {
	res, err := st.StageState(stage, db)
	if err != nil {
		panic(err)
	}
	return res
}

func overrideStorageMode(db ethdb.RwKV) error {
	sm, err := ethdb.StorageModeFromString(storageMode)
	if err != nil {
		return err
	}
	return db.Update(context.Background(), func(tx ethdb.RwTx) error {
		if err = ethdb.OverrideStorageMode(tx, sm); err != nil {
			return err
		}
		sm, err = ethdb.GetStorageModeFromDB(tx)
		if err != nil {
			return err
		}
		log.Info("Storage mode in DB", "mode", sm.ToString())
		return nil
	})
}
