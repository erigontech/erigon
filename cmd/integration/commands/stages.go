package commands

import (
	"context"
	"fmt"
	"path"
	"runtime"
	"sort"
	"strings"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/consensus"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/ethconfig"
	"github.com/ledgerwatch/turbo-geth/eth/integrity"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote/remotedbserver"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/migrations"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/silkworm"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/spf13/cobra"
)

var cmdStageBodies = &cobra.Command{
	Use:   "stage_bodies",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		db := openDatabase(chaindata, true)
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
		db := openDatabase(chaindata, true)
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
		db := openDatabase(chaindata, true)
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
		db := openDatabase(chaindata, true)
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
		db := openDatabase(chaindata, true)
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
		db := openDatabase(chaindata, true)
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
		db := openDatabase(chaindata, true)
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
		db := openDatabase(chaindata, true)
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
		db := openDatabase(chaindata, true)
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
		db := openDatabase(chaindata, false)
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
		db := openDatabase(chaindata, false)
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
		db := openDatabase(chaindata, false)
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
		db := openDatabase(chaindata, true)
		defer db.Close()
		// Nothing to do, migrations will be applied automatically
		return nil
	},
}

var cmdSetStorageMode = &cobra.Command{
	Use:   "set_storage_mode",
	Short: "Override storage mode (if you know what you are doing)",
	RunE: func(cmd *cobra.Command, args []string) error {
		db := openDatabase(chaindata, true)
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

	rootCmd.AddCommand(cmdStageExec)

	withDatadir(cmdStageHashState)
	withReset(cmdStageHashState)
	withBlock(cmdStageHashState)
	withUnwind(cmdStageHashState)
	withBatchSize(cmdStageHashState)

	rootCmd.AddCommand(cmdStageHashState)

	withDatadir(cmdStageTrie)
	withReset(cmdStageTrie)
	withBlock(cmdStageTrie)
	withUnwind(cmdStageTrie)
	withIntegrityChecks(cmdStageTrie)

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
	cmdSetStorageMode.Flags().StringVar(&storageMode, "storage-mode", "htr", "Storage mode to override database")
	rootCmd.AddCommand(cmdSetStorageMode)
}

func stageBodies(db ethdb.Database, ctx context.Context) error {
	if unwind > 0 {
		progress, err := stages.GetStageProgress(db, stages.Bodies)
		if err != nil {
			return fmt.Errorf("read Bodies progress: %w", err)
		}
		if unwind > progress {
			return fmt.Errorf("cannot unwind past 0")
		}
		if err = stages.SaveStageProgress(db, stages.Bodies, progress-unwind); err != nil {
			return fmt.Errorf("saving Bodies progress failed: %w", err)
		}
		progress, err = stages.GetStageProgress(db, stages.Bodies)
		if err != nil {
			return fmt.Errorf("re-read Bodies progress: %w", err)
		}
		log.Info("Progress", "bodies", progress)
		return nil
	}
	log.Info("This command only works with --unwind option")
	return nil
}

func stageSenders(db ethdb.Database, ctx context.Context) error {
	kv := db.RwKV()
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _ := newSync2(db, tx)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, db, ethdb.WrapIntoTxDB(tx), "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil)
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

	cfg := stagedsync.StageSendersCfg(kv, params.MainnetChainConfig)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.Senders, UnwindPoint: stage3.BlockNumber - unwind}
		err = stagedsync.UnwindSendersStage(u, stage3, tx, cfg)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnRecoverSendersStage(cfg, stage3, tx, block, tmpdir, ch)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func silkwormExecutionFunc() unsafe.Pointer {
	if silkwormPath == "" {
		return nil
	}

	funcPtr, err := silkworm.LoadExecutionFunctionPointer(silkwormPath)
	if err != nil {
		panic(fmt.Errorf("failed to load Silkworm dynamic library: %v", err))
	}
	return funcPtr
}

func stageExec(db ethdb.Database, ctx context.Context) error {
	kv := db.RwKV()
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _ := newSync2(db, tx)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, db, ethdb.WrapIntoTxDB(tx), "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil)
	if err != nil {
		return nil
	}

	if reset {
		err = resetExec(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	if txtrace {
		// Activate tracing and writing into json files for each transaction
		vmConfig.Tracer = nil
		vmConfig.Debug = true
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	stage4 := stage(sync, tx, stages.Execution)
	log.Info("Stage4", "progress", stage4.BlockNumber)
	ch := ctx.Done()
	cfg := stagedsync.StageExecuteBlocksCfg(kv, sm.Receipts, sm.CallTraces, batchSize, nil, nil, silkwormExecutionFunc(), nil, chainConfig, engine, vmConfig, tmpDBPath)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: stage4.BlockNumber - unwind}
		err = stagedsync.UnwindExecutionStage(u, stage4, tx, ch, cfg)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnExecuteBlocksStage(stage4, tx, block, ch, cfg)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageTrie(db ethdb.Database, ctx context.Context) error {
	kv := db.RwKV()
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _ := newSync2(db, tx)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, db, ethdb.WrapIntoTxDB(tx), "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil)
	if err != nil {
		return nil
	}

	if reset {
		err = stagedsync.ResetIH(tx)
		if err != nil {
			return err
		}
		return tx.Commit()
	}

	stage4 := stage(sync, tx, stages.Execution)
	stage5 := stage(sync, tx, stages.IntermediateHashes)
	log.Info("Stage4", "progress", stage4.BlockNumber)
	log.Info("Stage5", "progress", stage5.BlockNumber)
	ch := ctx.Done()
	cfg := stagedsync.StageTrieCfg(kv, true, true, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: stage5.BlockNumber - unwind}
		if err := stagedsync.UnwindIntermediateHashesStage(u, stage5, tx, cfg, ch); err != nil {
			return err
		}
	} else {
		if _, err := stagedsync.SpawnIntermediateHashesStage(stage5, nil /* Unwinder */, tx, cfg, ch); err != nil {
			return err
		}
	}
	integrity.Trie(tx, integritySlow, ch)
	return tx.Commit()
}

func stageHashState(db ethdb.Database, ctx context.Context) error {
	kv := db.RwKV()
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _ := newSync2(db, tx)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, db, ethdb.WrapIntoTxDB(tx), "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil)
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
	cfg := stagedsync.StageHashStateCfg(kv, tmpdir)
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

func stageLogIndex(db ethdb.Database, ctx context.Context) error {
	kv := db.RwKV()
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _ := newSync2(db, tx)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, db, ethdb.WrapIntoTxDB(tx), "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil)
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
	s := stage(sync, db, stages.LogIndex)
	log.Info("Stage exec", "progress", execAt)
	log.Info("Stage log index", "progress", s.BlockNumber)
	ch := ctx.Done()

	cfg := stagedsync.StageLogIndexCfg(db.RwKV(), tmpdir)
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

func stageCallTraces(db ethdb.Database, ctx context.Context) error {
	kv := db.RwKV()
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _ := newSync2(db, tx)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, db, ethdb.WrapIntoTxDB(tx), "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil)
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

func stageHistory(db ethdb.Database, ctx context.Context) error {
	kv := db.RwKV()
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _ := newSync2(db, tx)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, db, ethdb.WrapIntoTxDB(tx), "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil)
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
	execStage := progress(db, stages.Execution)
	stageStorage := stage(sync, db, stages.StorageHistoryIndex)
	stageAcc := stage(sync, db, stages.AccountHistoryIndex)
	log.Info("Stage exec", "progress", execStage)
	log.Info("Stage acc history", "progress", stageAcc.BlockNumber)
	log.Info("Stage storage history", "progress", stageStorage.BlockNumber)
	ch := ctx.Done()

	cfg := stagedsync.StageHistoryCfg(db.RwKV(), tmpdir)
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

func stageTxLookup(db ethdb.Database, ctx context.Context) error {
	kv := db.RwKV()
	tx, err := kv.BeginRw(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	tmpdir := path.Join(datadir, etl.TmpDirName)

	sm, engine, chainConfig, vmConfig, _, st, _ := newSync2(db, tx)
	sync, err := st.Prepare(nil, chainConfig, engine, vmConfig, db, ethdb.WrapIntoTxDB(tx), "integration_test", sm, tmpdir, 0, ctx.Done(), nil, nil, false, nil)
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

	cfg := stagedsync.StageTxLookupCfg(db.RwKV(), tmpdir)
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

func printAllStages(db ethdb.Getter, _ context.Context) error {
	return printStages(db)
}

func printAppliedMigrations(db ethdb.Database, _ context.Context) error {
	applied, err := migrations.AppliedMigrations(db, false /* withPayload */)
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
}

func removeMigration(db ethdb.Deleter, _ context.Context) error {
	if err := db.Delete(dbutils.Migrations, []byte(migration), nil); err != nil {
		return err
	}
	return nil
}

type progressFunc func(stage stages.SyncStage) *stagedsync.StageState

func newSync2(db ethdb.Database, tx ethdb.RwTx) (ethdb.StorageMode, consensus.Engine, *params.ChainConfig, *vm.Config, *core.TxPool, *stagedsync.StagedSync, *stagedsync.StagedSync) {
	sm, err := ethdb.GetStorageModeFromDB(tx)
	if err != nil {
		panic(err)
	}

	vmConfig := &vm.Config{NoReceipts: !sm.Receipts}
	chainConfig := params.MainnetChainConfig
	events := remotedbserver.NewEvents()

	txCacher := core.NewTxSenderCacher(runtime.NumCPU())
	txPool := core.NewTxPool(ethconfig.Defaults.TxPool, chainConfig, db, txCacher)

	st := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
		stagedsync.OptionalParameters{SilkwormExecutionFunc: silkwormExecutionFunc(), Notifier: events},
	)
	stMining := stagedsync.New(
		stagedsync.MiningStages(),
		stagedsync.MiningUnwindOrder(),
		stagedsync.OptionalParameters{SilkwormExecutionFunc: silkwormExecutionFunc(), Notifier: events},
	)
	return sm, ethash.NewFaker(), chainConfig, vmConfig, txPool, st, stMining
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

//nolint:unparam
func newSync(quitCh <-chan struct{}, db ethdb.Database, tx ethdb.Database, miningParams *stagedsync.MiningCfg) (consensus.Engine, *params.ChainConfig, *vm.Config, *stagedsync.State, *stagedsync.State, progressFunc) {
	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		panic(err)
	}

	chainConfig := params.MainnetChainConfig
	vmConfig := &vm.Config{NoReceipts: !sm.Receipts}
	engine := ethash.NewFaker()
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	st, err := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
		stagedsync.OptionalParameters{SilkwormExecutionFunc: silkwormExecutionFunc()},
	).Prepare(nil, chainConfig, engine, vmConfig, db, tx, "integration_test", sm, path.Join(datadir, etl.TmpDirName), batchSize, quitCh, nil, nil, false, nil)
	if err != nil {
		panic(err)
	}

	stMining, err := stagedsync.New(
		stagedsync.MiningStages(),
		stagedsync.MiningUnwindOrder(),
		stagedsync.OptionalParameters{SilkwormExecutionFunc: silkwormExecutionFunc()},
	).Prepare(nil, chainConfig, engine, vmConfig, db, tx, "integration_test", sm, path.Join(datadir, etl.TmpDirName), batchSize, quitCh, nil, nil, false, miningParams)
	if err != nil {
		panic(err)
	}

	progress := func(stage stages.SyncStage) *stagedsync.StageState {
		if hasTx, ok := tx.(ethdb.HasTx); ok && hasTx.Tx() != nil {
			s, err := st.StageState(stage, tx)
			if err != nil {
				panic(err)
			}
			return s
		}
		s, err := st.StageState(stage, tx)
		if err != nil {
			panic(err)
		}
		return s
	}
	return engine, chainConfig, vmConfig, st, stMining, progress
}

func SetSnapshotKV(db ethdb.Database, snapshotDir string, mode snapshotsync.SnapshotMode) error {
	if len(snapshotDir) > 0 {
		//todo change to new format
		snapshotKV := db.(ethdb.HasRwKV).RwKV()
		var err error
		snapshotKV, err = snapshotsync.WrapBySnapshotsFromDir(snapshotKV, snapshotDir, mode)
		if err != nil {
			return err
		}
		db.(ethdb.HasRwKV).SetRwKV(snapshotKV)
	}
	return nil
}

func overrideStorageMode(db ethdb.Database) error {
	sm, err := ethdb.StorageModeFromString(storageMode)
	if err != nil {
		return err
	}
	if err = ethdb.OverrideStorageMode(db, sm); err != nil {
		return err
	}
	sm, err = ethdb.GetStorageModeFromDB(db)
	if err != nil {
		return err
	}
	log.Info("Storage mode in DB", "mode", sm.ToString())
	return nil
}
