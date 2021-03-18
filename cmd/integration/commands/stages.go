package commands

import (
	"context"
	"fmt"
	"path"
	"runtime"
	"sort"
	"strings"
	"time"
	"unsafe"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/common/etl"
	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/integrity"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/migrations"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/ledgerwatch/turbo-geth/turbo/shards"
	"github.com/ledgerwatch/turbo-geth/turbo/silkworm"
	"github.com/ledgerwatch/turbo-geth/turbo/snapshotsync"
	"github.com/spf13/cobra"
)

var cmdStageSenders = &cobra.Command{
	Use:   "stage_senders",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		ctx := utils.RootContext()
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
		db := openDatabase(chaindata, false)
		defer db.Close()
		// Nothing to do, migrations will be applied automatically
		return nil
	},
}

func init() {
	withChaindata(cmdPrintStages)
	withLmdbFlags(cmdPrintStages)
	rootCmd.AddCommand(cmdPrintStages)

	withChaindata(cmdStageSenders)
	withLmdbFlags(cmdStageSenders)
	withReset(cmdStageSenders)
	withBlock(cmdStageSenders)
	withUnwind(cmdStageSenders)
	withDatadir(cmdStageSenders)

	rootCmd.AddCommand(cmdStageSenders)

	withChaindata(cmdStageExec)
	withLmdbFlags(cmdStageExec)
	withReset(cmdStageExec)
	withBlock(cmdStageExec)
	withUnwind(cmdStageExec)
	withBatchSize(cmdStageExec)
	withSilkworm(cmdStageExec)

	rootCmd.AddCommand(cmdStageExec)

	withChaindata(cmdStageHashState)
	withLmdbFlags(cmdStageHashState)
	withReset(cmdStageHashState)
	withBlock(cmdStageHashState)
	withUnwind(cmdStageHashState)
	withBatchSize(cmdStageHashState)
	withDatadir(cmdStageHashState)

	rootCmd.AddCommand(cmdStageHashState)

	withChaindata(cmdStageTrie)
	withLmdbFlags(cmdStageTrie)
	withReset(cmdStageTrie)
	withBlock(cmdStageTrie)
	withUnwind(cmdStageTrie)
	withDatadir(cmdStageTrie)
	withIntegrityChecks(cmdStageTrie)

	rootCmd.AddCommand(cmdStageTrie)

	withChaindata(cmdStageHistory)
	withLmdbFlags(cmdStageHistory)
	withReset(cmdStageHistory)
	withBlock(cmdStageHistory)
	withUnwind(cmdStageHistory)
	withDatadir(cmdStageHistory)

	rootCmd.AddCommand(cmdStageHistory)

	withChaindata(cmdLogIndex)
	withLmdbFlags(cmdLogIndex)
	withReset(cmdLogIndex)
	withBlock(cmdLogIndex)
	withUnwind(cmdLogIndex)
	withDatadir(cmdLogIndex)

	rootCmd.AddCommand(cmdLogIndex)

	withChaindata(cmdCallTraces)
	withLmdbFlags(cmdCallTraces)
	withReset(cmdCallTraces)
	withBlock(cmdCallTraces)
	withUnwind(cmdCallTraces)
	withDatadir(cmdCallTraces)

	rootCmd.AddCommand(cmdCallTraces)

	withChaindata(cmdStageTxLookup)
	withLmdbFlags(cmdStageTxLookup)
	withReset(cmdStageTxLookup)
	withBlock(cmdStageTxLookup)
	withUnwind(cmdStageTxLookup)
	withDatadir(cmdStageTxLookup)

	rootCmd.AddCommand(cmdStageTxLookup)

	withChaindata(cmdPrintMigrations)
	rootCmd.AddCommand(cmdPrintMigrations)

	withChaindata(cmdRemoveMigration)
	withLmdbFlags(cmdRemoveMigration)
	withMigration(cmdRemoveMigration)
	rootCmd.AddCommand(cmdRemoveMigration)

	withChaindata(cmdRunMigrations)
	withLmdbFlags(cmdRunMigrations)
	withDatadir(cmdRunMigrations)
	rootCmd.AddCommand(cmdRunMigrations)
}

func stageSenders(db ethdb.Database, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)
	_, bc, _, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetSenders(db); err != nil {
			return err
		}
	}

	stage2 := progress(stages.Bodies)
	stage3 := progress(stages.Senders)
	log.Info("Stage2", "progress", stage2.BlockNumber)
	log.Info("Stage3", "progress", stage3.BlockNumber)
	ch := make(chan struct{})
	defer close(ch)

	const batchSize = 10000
	const blockSize = 4096
	n := runtime.NumCPU()

	cfg := stagedsync.Stage3Config{
		BatchSize:       batchSize,
		BlockSize:       blockSize,
		BufferSize:      (blockSize * 10 / 20) * 10000, // 20*4096
		NumOfGoroutines: n,
		ReadChLen:       4,
		Now:             time.Now(),
	}
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.Senders, UnwindPoint: stage3.BlockNumber - unwind}
		return stagedsync.UnwindSendersStage(u, stage3, db)
	}

	return stagedsync.SpawnRecoverSendersStage(cfg, stage3, db, params.MainnetChainConfig, block, tmpdir, ch)
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
	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		panic(err)
	}

	cc, bc, _, cache, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		return resetExec(db)
	}

	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	stage4 := progress(stages.Execution)
	log.Info("Stage4", "progress", stage4.BlockNumber)
	ch := ctx.Done()
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: stage4.BlockNumber - unwind}
		return stagedsync.UnwindExecutionStage(u, stage4, db, ch,
			stagedsync.ExecuteBlockStageParams{
				ToBlock:               block, // limit execution to the specified block
				WriteReceipts:         sm.Receipts,
				Cache:                 cache,
				BatchSize:             batchSize,
				SilkwormExecutionFunc: silkwormExecutionFunc(),
			})
	}
	return stagedsync.SpawnExecuteBlocksStage(stage4, db,
		bc.Config(), cc, bc.GetVMConfig(),
		ch,
		stagedsync.ExecuteBlockStageParams{
			ToBlock:               block, // limit execution to the specified block
			WriteReceipts:         sm.Receipts,
			Cache:                 cache,
			BatchSize:             batchSize,
			SilkwormExecutionFunc: silkwormExecutionFunc(),
		})
}

func stageTrie(db ethdb.Database, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	var tx ethdb.DbWithPendingMutations = ethdb.NewTxDbWithoutTransaction(db, ethdb.RW)
	defer tx.Rollback()

	cc, bc, _, cache, progress := newSync(ctx.Done(), db, tx, nil)
	defer bc.Stop()
	cc.SetDB(tx)

	var err1 error
	tx, err1 = tx.Begin(ctx, ethdb.RW)
	if err1 != nil {
		return err1
	}

	if reset {
		if err := stagedsync.ResetIH(tx); err != nil {
			return err
		}
		if _, err := tx.Commit(); err != nil {
			panic(err)
		}
		return nil
	}

	stage4 := progress(stages.Execution)
	stage5 := progress(stages.IntermediateHashes)
	log.Info("Stage4", "progress", stage4.BlockNumber)
	log.Info("Stage5", "progress", stage5.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: stage5.BlockNumber - unwind}
		if err := stagedsync.UnwindIntermediateHashesStage(u, stage5, tx, cache, tmpdir, ch); err != nil {
			return err
		}
	} else {
		if err := stagedsync.SpawnIntermediateHashesStage(stage5, tx, true /* checkRoot */, cache, tmpdir, ch); err != nil {
			return err
		}
	}
	integrity.Trie(tx.(ethdb.HasTx).Tx(), integritySlow, ch)
	if _, err := tx.Commit(); err != nil {
		panic(err)
	}
	return nil
}

func stageHashState(db ethdb.Database, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	_, bc, _, cache, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := stagedsync.ResetHashState(db); err != nil {
			return err
		}
		return nil
	}

	stage5 := progress(stages.IntermediateHashes)
	stage6 := progress(stages.HashState)
	log.Info("Stage5", "progress", stage5.BlockNumber)
	log.Info("Stage6", "progress", stage6.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.HashState, UnwindPoint: stage6.BlockNumber - unwind}
		return stagedsync.UnwindHashStateStage(u, stage6, db, cache, tmpdir, ch)
	}
	return stagedsync.SpawnHashStateStage(stage6, db, cache, tmpdir, ch)
}

func stageLogIndex(db ethdb.Database, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	_, bc, _, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetLogIndex(db); err != nil {
			return err
		}
		return nil
	}
	execStage := progress(stages.Execution)
	s := progress(stages.LogIndex)
	log.Info("Stage exec", "progress", execStage.BlockNumber)
	log.Info("Stage log index", "progress", s.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.LogIndex, UnwindPoint: s.BlockNumber - unwind}
		return stagedsync.UnwindLogIndex(u, s, db, ch)
	}

	if err := stagedsync.SpawnLogIndex(s, db, tmpdir, ch); err != nil {
		return err
	}
	return nil
}

func stageCallTraces(db ethdb.Database, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	_, bc, _, cache, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetCallTraces(db); err != nil {
			return err
		}
		return nil
	}
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))

	execStage := progress(stages.Execution)
	s := progress(stages.CallTraces)
	log.Info("Stage exec", "progress", execStage.BlockNumber)
	if block != 0 {
		s.BlockNumber = block
		log.Info("Overriding initial state", "block", block)
	}
	log.Info("Stage call traces", "progress", s.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.CallTraces, UnwindPoint: s.BlockNumber - unwind}
		return stagedsync.UnwindCallTraces(u, s, db, bc.Config(), bc, ch,
			stagedsync.CallTracesStageParams{
				ToBlock:   block,
				Cache:     cache,
				BatchSize: batchSize,
			})
	}

	if err := stagedsync.SpawnCallTraces(s, db, bc.Config(), bc, tmpdir, ch,
		stagedsync.CallTracesStageParams{
			ToBlock:   block,
			Cache:     cache,
			BatchSize: batchSize,
		}); err != nil {
		return err
	}
	return nil
}

func stageHistory(db ethdb.Database, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)

	_, bc, _, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetHistory(db); err != nil {
			return err
		}
		return nil
	}
	execStage := progress(stages.Execution)
	stageAcc := progress(stages.AccountHistoryIndex)
	stageStorage := progress(stages.StorageHistoryIndex)
	log.Info("Stage exec", "progress", execStage.BlockNumber)
	log.Info("Stage acc history", "progress", stageAcc.BlockNumber)
	log.Info("Stage storage history", "progress", stageStorage.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 { //nolint:staticcheck
		u := &stagedsync.UnwindState{Stage: stages.StorageHistoryIndex, UnwindPoint: stageStorage.BlockNumber - unwind}
		s := progress(stages.StorageHistoryIndex)
		if err := stagedsync.UnwindStorageHistoryIndex(u, s, db, ch); err != nil {
			return err
		}
		u = &stagedsync.UnwindState{Stage: stages.AccountHistoryIndex, UnwindPoint: stageAcc.BlockNumber - unwind}
		s = progress(stages.AccountHistoryIndex)
		if err := stagedsync.UnwindAccountHistoryIndex(u, s, db, ch); err != nil {
			return err
		}
		return nil
	}
	if err := stagedsync.SpawnAccountHistoryIndex(stageAcc, db, tmpdir, ch); err != nil {
		return err
	}
	if err := stagedsync.SpawnStorageHistoryIndex(stageStorage, db, tmpdir, ch); err != nil {
		return err
	}
	return nil
}

func stageTxLookup(db ethdb.Database, ctx context.Context) error {
	tmpdir := path.Join(datadir, etl.TmpDirName)
	_, bc, _, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetTxLookup(db); err != nil {
			return err
		}
		return nil
	}
	stage9 := progress(stages.TxLookup)
	log.Info("Stage9", "progress", stage9.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.TxLookup, UnwindPoint: stage9.BlockNumber - unwind}
		s := progress(stages.TxLookup)
		return stagedsync.UnwindTxLookup(u, s, db, tmpdir, ch)
	}

	return stagedsync.SpawnTxLookup(stage9, db, tmpdir, ch)
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

func removeMigration(db rawdb.DatabaseDeleter, _ context.Context) error {
	if err := db.Delete(dbutils.Migrations, []byte(migration), nil); err != nil {
		return err
	}
	return nil
}

type progressFunc func(stage stages.SyncStage) *stagedsync.StageState

func newSync(quitCh <-chan struct{}, db ethdb.Database, tx ethdb.Database, hook stagedsync.ChangeSetHook) (*core.TinyChainContext, *core.BlockChain, *stagedsync.State, *shards.StateCache, progressFunc) {
	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		panic(err)
	}

	chainConfig, bc, err := newBlockChain(db, sm)
	if err != nil {
		panic(err)
	}

	cc := &core.TinyChainContext{}
	cc.SetDB(tx)
	cc.SetEngine(ethash.NewFaker())
	var cacheSize datasize.ByteSize
	must(cacheSize.UnmarshalText([]byte(cacheSizeStr)))
	var batchSize datasize.ByteSize
	must(batchSize.UnmarshalText([]byte(batchSizeStr)))
	var cache *shards.StateCache
	if cacheSize > 0 {
		cache = shards.NewStateCache(32, cacheSize)
	}
	st, err := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
		stagedsync.OptionalParameters{SilkwormExecutionFunc: silkwormExecutionFunc()},
	).Prepare(nil, chainConfig, cc, bc.GetVMConfig(), db, tx, "integration_test", sm, path.Join(datadir, etl.TmpDirName), cache, batchSize, quitCh, nil, nil, func() error { return nil }, hook, false)
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
	return cc, bc, st, cache, progress
}

func newBlockChain(db ethdb.Database, sm ethdb.StorageMode) (*params.ChainConfig, *core.BlockChain, error) {
	blockchain, err1 := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{
		NoReceipts: !sm.Receipts,
	}, nil, nil)
	if err1 != nil {
		return nil, nil, err1
	}
	return params.MainnetChainConfig, blockchain, nil
}

func SetSnapshotKV(db ethdb.Database, snapshotDir, snapshotMode string) error {
	if len(snapshotMode) > 0 && len(snapshotDir) > 0 {
		mode, err := snapshotsync.SnapshotModeFromString(snapshotMode)
		if err != nil {
			panic(err)
		}

		snapshotKV := db.(ethdb.HasKV).KV()
		snapshotKV, err = snapshotsync.WrapBySnapshotsFromDir(snapshotKV, snapshotDir, mode)
		if err != nil {
			return err
		}
		db.(ethdb.HasKV).SetKV(snapshotKV)
	}
	return nil
}
