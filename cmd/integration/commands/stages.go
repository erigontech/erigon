package commands

import (
	"context"
	"runtime"
	"time"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"

	"github.com/ledgerwatch/turbo-geth/consensus/ethash"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/vm"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/params"
	"github.com/spf13/cobra"
)

var cmdStageSenders = &cobra.Command{
	Use:   "stage_senders",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		if err := stageSenders(ctx); err != nil {
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
		if err := stageExec(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStageIHash = &cobra.Command{
	Use:   "stage_ih",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		if err := stageIHash(ctx); err != nil {
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
		if err := stageHashState(ctx); err != nil {
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
		if err := stageHistory(ctx); err != nil {
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
		if err := stageTxLookup(ctx); err != nil {
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
		if err := printAllStages(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

func init() {
	withChaindata(cmdPrintStages)
	rootCmd.AddCommand(cmdPrintStages)

	withChaindata(cmdStageSenders)
	withReset(cmdStageSenders)
	withBlock(cmdStageSenders)
	withUnwind(cmdStageSenders)
	withDatadir(cmdStageSenders)

	rootCmd.AddCommand(cmdStageSenders)

	withChaindata(cmdStageExec)
	withReset(cmdStageExec)
	withBlock(cmdStageExec)
	withUnwind(cmdStageExec)
	withHDD(cmdStageExec)

	rootCmd.AddCommand(cmdStageExec)

	withChaindata(cmdStageIHash)
	withReset(cmdStageIHash)
	withBlock(cmdStageIHash)
	withUnwind(cmdStageIHash)
	withDatadir(cmdStageIHash)

	rootCmd.AddCommand(cmdStageIHash)

	withChaindata(cmdStageHashState)
	withReset(cmdStageHashState)
	withBlock(cmdStageHashState)
	withUnwind(cmdStageHashState)
	withDatadir(cmdStageHashState)

	rootCmd.AddCommand(cmdStageHashState)

	withChaindata(cmdStageHistory)
	withReset(cmdStageHistory)
	withBlock(cmdStageHistory)
	withUnwind(cmdStageHistory)
	withDatadir(cmdStageHistory)

	rootCmd.AddCommand(cmdStageHistory)

	withChaindata(cmdStageTxLookup)
	withReset(cmdStageTxLookup)
	withBlock(cmdStageTxLookup)
	withUnwind(cmdStageTxLookup)
	withDatadir(cmdStageTxLookup)

	rootCmd.AddCommand(cmdStageTxLookup)
}

func stageSenders(ctx context.Context) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	bc, _, progress := newSync(ctx.Done(), db, db, nil)
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
		StartTrace:      false,
		Prof:            false,
		NumOfGoroutines: n,
		ReadChLen:       4,
		Now:             time.Now(),
	}

	return stagedsync.SpawnRecoverSendersStage(cfg, stage3, db, params.MainnetChainConfig, block, datadir, ch)
}

func stageExec(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		panic(err)
	}

	bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset { //nolint:staticcheck
		// TODO
	}

	stage4 := progress(stages.Execution)
	log.Info("Stage4", "progress", stage4.BlockNumber)
	ch := ctx.Done()
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: stage4.BlockNumber - unwind}
		return stagedsync.UnwindExecutionStage(u, stage4, db, false)
	}
	return stagedsync.SpawnExecuteBlocksStage(stage4, db, bc.Config(), bc, bc.GetVMConfig(), block, ch, sm.Receipts, hdd, nil)
}

func stageIHash(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := stagedsync.ResetHashState(db); err != nil {
			return err
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
		return stagedsync.UnwindIntermediateHashesStage(u, stage5, db, datadir, ch)
	}
	return stagedsync.SpawnIntermediateHashesStage(stage5, db, datadir, ch)
}

func stageHashState(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	bc, _, progress := newSync(ctx.Done(), db, db, nil)
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
		return stagedsync.UnwindHashStateStage(u, stage6, db, datadir, ch)
	}
	return stagedsync.SpawnHashStateStage(stage6, db, datadir, ch)
}

func stageHistory(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetHistory(db); err != nil {
			return err
		}
	}
	execStage := progress(stages.Execution)
	stage7 := progress(stages.AccountHistoryIndex)
	stage8 := progress(stages.StorageHistoryIndex)
	log.Info("Stage4", "progress", execStage.BlockNumber)
	log.Info("Stage7", "progress", stage7.BlockNumber)
	log.Info("Stage8", "progress", stage8.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 { //nolint:staticcheck
		// TODO
	}

	if err := stagedsync.SpawnAccountHistoryIndex(stage7, db, datadir, ch); err != nil {
		return err
	}
	if err := stagedsync.SpawnStorageHistoryIndex(stage8, db, datadir, ch); err != nil {
		return err
	}
	return nil
}

func stageTxLookup(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	bc, _, progress := newSync(ctx.Done(), db, db, nil)
	defer bc.Stop()

	if reset {
		if err := resetTxLookup(db); err != nil {
			return err
		}
	}
	stage9 := progress(stages.TxLookup)
	log.Info("Stage9", "progress", stage9.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.TxLookup, UnwindPoint: stage9.BlockNumber - unwind}
		s := progress(stages.TxLookup)
		return stagedsync.UnwindTxLookup(u, s, db, datadir, ch)
	}

	return stagedsync.SpawnTxLookup(stage9, db, datadir, ch)
}

func printAllStages(_ context.Context) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	return printStages(db)
}

type progressFunc func(stage stages.SyncStage) *stagedsync.StageState

func newSync(quitCh <-chan struct{}, db ethdb.Database, tx ethdb.Database, hook stagedsync.ChangeSetHook) (*core.BlockChain, *stagedsync.State, progressFunc) {
	chainConfig, bc, err := newBlockChain(tx)
	if err != nil {
		panic(err)
	}

	sm, err := ethdb.GetStorageModeFromDB(db)
	if err != nil {
		panic(err)
	}
	st, err := stagedsync.New(
		stagedsync.DefaultStages(),
		stagedsync.DefaultUnwindOrder(),
	).Prepare(nil, chainConfig, bc, bc.GetVMConfig(), db, tx, "integration_test", sm, "", false, quitCh, nil, nil, func() error { return nil }, hook)
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
		s, err := st.StageState(stage, db)
		if err != nil {
			panic(err)
		}
		return s
	}

	return bc, st, progress
}

func newBlockChain(db ethdb.Database) (*params.ChainConfig, *core.BlockChain, error) {
	blockchain, err1 := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil)
	if err1 != nil {
		return nil, nil, err1
	}
	return params.MainnetChainConfig, blockchain, nil
}
