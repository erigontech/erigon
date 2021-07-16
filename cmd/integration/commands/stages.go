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
	withBatchSize(cmdStageExec)
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
	withChain(cmdStageHistory)

	rootCmd.AddCommand(cmdStageHistory)

	withDatadir(cmdLogIndex)
	withReset(cmdLogIndex)
	withBlock(cmdLogIndex)
	withUnwind(cmdLogIndex)
	withChain(cmdLogIndex)

	rootCmd.AddCommand(cmdLogIndex)

	withDatadir(cmdCallTraces)
	withReset(cmdCallTraces)
	withBlock(cmdCallTraces)
	withUnwind(cmdCallTraces)
	withChain(cmdCallTraces)

	rootCmd.AddCommand(cmdCallTraces)

	withReset(cmdStageTxLookup)
	withBlock(cmdStageTxLookup)
	withUnwind(cmdStageTxLookup)
	withDatadir(cmdStageTxLookup)
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

	withDatadir(cmdSetStorageMode)
	withChain(cmdSetStorageMode)
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

	stage2 := stage(sync, tx, stages.Bodies)
	stage3 := stage(sync, tx, stages.Senders)
	log.Info("Stage2", "progress", stage2.BlockNumber)
	log.Info("Stage3", "progress", stage3.BlockNumber)

	cfg := stagedsync.StageSendersCfg(db, chainConfig, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{ID: stages.Senders, UnwindPoint: stage3.BlockNumber - unwind}
		err = stagedsync.UnwindSendersStage(u, stage3, tx, cfg)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnRecoverSendersStage(cfg, stage3, sync, tx, block, ctx)
		if err != nil {
			return err
		}
	}

	return tx.Commit()
}

func stageExec(db ethdb.RwKV, ctx context.Context) error {
	sm, engine, chainConfig, vmConfig, _, sync, _, _ := newSync(ctx, db, nil)

	if reset {
		genesis, _ := byChain()
		if err := db.Update(ctx, func(tx ethdb.RwTx) error { return resetExec(tx, genesis) }); err != nil {
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

	var execStage *stagedsync.StageState
	if err := db.View(ctx, func(tx ethdb.Tx) error {
		execStage = stage(sync, tx, stages.Execution)
		return nil
	}); err != nil {
		return err
	}

	log.Info("Stage4", "progress", execStage.BlockNumber)
	cfg := stagedsync.StageExecuteBlocksCfg(db, sm.Receipts, sm.CallTraces, sm.TEVM, 0, batchSize, nil, chainConfig, engine, vmConfig, nil, false, tmpDBPath)
	if unwind > 0 {
		u := &stagedsync.UnwindState{ID: stages.Execution, UnwindPoint: execStage.BlockNumber - unwind}
		err := stagedsync.UnwindExecutionStage(u, execStage, nil, ctx, cfg, false)
		if err != nil {
			return err
		}
		return nil
	}

	err := stagedsync.SpawnExecuteBlocksStage(execStage, sync, nil, block, ctx, cfg, false)
	if err != nil {
		return err
	}
	return nil
}

func stageTrie(db ethdb.RwKV, ctx context.Context) error {
	_, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)
	tmpdir := path.Join(datadir, etl.TmpDirName)

	if reset {
		if err := db.Update(ctx, func(tx ethdb.RwTx) error { return stagedsync.ResetIH(tx) }); err != nil {
			return err
		}
	}
	var execStage, trieStage *stagedsync.StageState
	if err := db.View(ctx, func(tx ethdb.Tx) error {
		execStage = stage(sync, tx, stages.Execution)
		trieStage = stage(sync, tx, stages.IntermediateHashes)
		return nil
	}); err != nil {
		return err
	}

	log.Info("Stage4", "progress", execStage.BlockNumber)
	log.Info("Stage5", "progress", trieStage.BlockNumber)
	cfg := stagedsync.StageTrieCfg(db, true, true, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{ID: stages.IntermediateHashes, UnwindPoint: trieStage.BlockNumber - unwind}
		if err := stagedsync.UnwindIntermediateHashesStage(u, trieStage, nil, cfg, ctx); err != nil {
			return err
		}
	} else {
		if _, err := stagedsync.SpawnIntermediateHashesStage(trieStage, nil /* Unwinder */, nil, cfg, ctx); err != nil {
			return err
		}
	}
	if err := db.View(ctx, func(tx ethdb.Tx) error {
		integrity.Trie(tx, integritySlow, ctx)
		return nil
	}); err != nil {
		return err
	}
	return nil
}

func stageHashState(db ethdb.RwKV, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	_, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)

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

	stage5 := stage(sync, tx, stages.IntermediateHashes)
	stage6 := stage(sync, tx, stages.HashState)
	log.Info("Stage5", "progress", stage5.BlockNumber)
	log.Info("Stage6", "progress", stage6.BlockNumber)
	cfg := stagedsync.StageHashStateCfg(db, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{ID: stages.HashState, UnwindPoint: stage6.BlockNumber - unwind}
		err = stagedsync.UnwindHashStateStage(u, stage6, tx, cfg, ctx)
		if err != nil {
			return err
		}
	} else {
		err = stagedsync.SpawnHashStateStage(stage6, tx, cfg, ctx)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

func stageLogIndex(db ethdb.RwKV, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	_, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)
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
	s := stage(sync, tx, stages.LogIndex)
	log.Info("ID exec", "progress", execAt)
	log.Info("ID log index", "progress", s.BlockNumber)
	ch := ctx.Done()

	cfg := stagedsync.StageLogIndexCfg(db, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{ID: stages.LogIndex, UnwindPoint: s.BlockNumber - unwind}
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

	_, engine, chainConfig, _, _, sync, _, _ := newSync(ctx, kv, nil)
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
	s := stage(sync, tx, stages.CallTraces)
	log.Info("ID exec", "progress", execStage)
	if block != 0 {
		s.BlockNumber = block
		log.Info("Overriding initial state", "block", block)
	}
	log.Info("ID call traces", "progress", s.BlockNumber)
	ch := ctx.Done()
	cfg := stagedsync.StageCallTracesCfg(kv, block, batchSize, tmpdir, chainConfig, engine)

	if unwind > 0 {
		u := &stagedsync.UnwindState{ID: stages.CallTraces, UnwindPoint: s.BlockNumber - unwind}
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
	_, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)
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
	stageStorage := stage(sync, tx, stages.StorageHistoryIndex)
	stageAcc := stage(sync, tx, stages.AccountHistoryIndex)
	log.Info("ID exec", "progress", execStage)
	log.Info("ID acc history", "progress", stageAcc.BlockNumber)
	log.Info("ID storage history", "progress", stageStorage.BlockNumber)
	ch := ctx.Done()

	cfg := stagedsync.StageHistoryCfg(db, tmpdir)
	if unwind > 0 { //nolint:staticcheck
		u := &stagedsync.UnwindState{ID: stages.StorageHistoryIndex, UnwindPoint: stageStorage.BlockNumber - unwind}
		if err := stagedsync.UnwindStorageHistoryIndex(u, stageStorage, tx, cfg, ch); err != nil {
			return err
		}
		u = &stagedsync.UnwindState{ID: stages.AccountHistoryIndex, UnwindPoint: stageAcc.BlockNumber - unwind}
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

	_, _, _, _, _, sync, _, _ := newSync(ctx, db, nil)

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
	stage9 := stage(sync, tx, stages.TxLookup)
	log.Info("Stage9", "progress", stage9.BlockNumber)
	ch := ctx.Done()

	cfg := stagedsync.StageTxLookupCfg(db, tmpdir)
	if unwind > 0 {
		u := &stagedsync.UnwindState{ID: stages.TxLookup, UnwindPoint: stage9.BlockNumber - unwind}
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
	case params.CalaverasChainName:
		chainConfig = params.CalaverasChainConfig
		genesis = core.DefaultCalaverasGenesisBlock()
	case params.SokolChainName:
		chainConfig = params.SokolChainConfig
		genesis = core.DefaultSokolGenesisBlock()
	}
	return genesis, chainConfig
}

func newSync(ctx context.Context, db ethdb.RwKV, miningConfig *params.MiningConfig) (ethdb.StorageMode, consensus.Engine, *params.ChainConfig, *vm.Config, *core.TxPool, *stagedsync.State, *stagedsync.State, stagedsync.MiningState) {
	tmpdir := path.Join(datadir, etl.TmpDirName)
	snapshotDir = path.Join(datadir, "erigon", "snapshot")

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

	genesis, chainConfig := byChain()
	var engine consensus.Engine
	engine = ethash.NewFaker()
	switch chain {
	case params.SokolChainName:
		engine = ethconfig.CreateConsensusEngine(chainConfig, &params.AuRaConfig{DBPath: path.Join(datadir, "aura")}, nil, false)
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
	cfg.StorageMode = sm
	cfg.BatchSize = batchSize
	if miningConfig != nil {
		cfg.Miner = *miningConfig
	}

	st, err := stages2.NewStagedSync2(context.Background(), db, cfg,
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

	stMining := stagedsync.New(
		stagedsync.MiningStages(ctx,
			stagedsync.StageMiningCreateBlockCfg(db, miner, *chainConfig, engine, txPool, tmpdir),
			stagedsync.StageMiningExecCfg(db, miner, events, *chainConfig, engine, &vm.Config{}, tmpdir),
			stagedsync.StageHashStateCfg(db, tmpdir),
			stagedsync.StageTrieCfg(db, false, true, tmpdir),
			stagedsync.StageMiningFinishCfg(db, *chainConfig, engine, miner, ctx.Done()),
		),
		stagedsync.MiningUnwindOrder(),
	)

	var sync *stagedsync.State
	var miningSync *stagedsync.State
	if err := db.View(context.Background(), func(tx ethdb.Tx) (err error) {
		sync, err = st.Prepare(nil, tx)
		if err != nil {
			return nil
		}
		miningSync, err = stMining.Prepare(nil, tx)
		if err != nil {
			return nil
		}
		return nil
	}); err != nil {
		panic(err)
	}

	return sm, engine, chainConfig, vmConfig, txPool, sync, miningSync, miner
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
