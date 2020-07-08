package main

import (
	"context"
	"runtime"
	"time"

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

var cmdStage3 = &cobra.Command{
	Use:   "stage3",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if err := stage3(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStage4 = &cobra.Command{
	Use:   "stage4",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if err := stage4(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStage5 = &cobra.Command{
	Use:   "stage5",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if err := stage5(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStage6 = &cobra.Command{
	Use:   "stage6",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if err := stage6(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStage78 = &cobra.Command{
	Use:   "stage78",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if err := stage78(ctx); err != nil {
			log.Error("Error", "err", err)
			return err
		}
		return nil
	},
}

var cmdStage9 = &cobra.Command{
	Use:   "stage9",
	Short: "",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if err := stage9(ctx); err != nil {
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
		ctx := rootContext()
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

	withChaindata(cmdStage3)
	withReset(cmdStage3)
	withBlock(cmdStage3)
	withUnwind(cmdStage3)

	rootCmd.AddCommand(cmdStage3)

	withChaindata(cmdStage4)
	withReset(cmdStage4)
	withBlock(cmdStage4)
	withUnwind(cmdStage4)

	rootCmd.AddCommand(cmdStage4)

	withChaindata(cmdStage5)
	withReset(cmdStage5)
	withBlock(cmdStage5)
	withUnwind(cmdStage5)

	rootCmd.AddCommand(cmdStage5)

	withChaindata(cmdStage6)
	withReset(cmdStage6)
	withBlock(cmdStage6)
	withUnwind(cmdStage6)

	rootCmd.AddCommand(cmdStage6)

	withChaindata(cmdStage78)
	withReset(cmdStage78)
	withBlock(cmdStage78)
	withUnwind(cmdStage78)

	rootCmd.AddCommand(cmdStage78)

	withChaindata(cmdStage9)
	withReset(cmdStage9)
	withBlock(cmdStage9)
	withUnwind(cmdStage9)

	rootCmd.AddCommand(cmdStage9)
}

func stage3(_ context.Context) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	if reset {
		if err := resetSenders(db); err != nil {
			return err
		}
	}

	stage2 := progress(db, stages.Bodies)
	stage3 := progress(db, stages.Senders)
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

	return stagedsync.SpawnRecoverSendersStage(cfg, stage3, db, params.MainnetChainConfig, block, "", ch)
}

func stage4(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	if reset { //nolint:staticcheck
		// TODO
	}

	stage4 := progress(db, stages.Execution)
	log.Info("Stage4", "progress", stage4.BlockNumber)
	ch := ctx.Done()
	blockchain, _ := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil, nil)
	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: stage4.BlockNumber - unwind}
		return stagedsync.UnwindExecutionStage(u, stage4, db)
	}
	return stagedsync.SpawnExecuteBlocksStage(stage4, db, blockchain, block, ch, nil, false)
}

func stage5(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	if reset {
		if err := resetHashState(db); err != nil {
			return err
		}
	}

	stage4 := progress(db, stages.Execution)
	stage5 := progress(db, stages.IntermediateHashes)
	log.Info("Stage4", "progress", stage4.BlockNumber)
	log.Info("Stage5", "progress", stage5.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: stage5.BlockNumber - unwind}
		return stagedsync.UnwindIntermediateHashesStage(u, stage5, db, "", ch)
	}
	return stagedsync.SpawnIntermediateHashesStage(stage5, db, "", ch)
}

func stage6(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	if reset {
		if err := resetHashState(db); err != nil {
			return err
		}
	}
	stage5 := progress(db, stages.IntermediateHashes)
	stage6 := progress(db, stages.HashState)
	log.Info("Stage5", "progress", stage5.BlockNumber)
	log.Info("Stage6", "progress", stage6.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.HashState, UnwindPoint: stage6.BlockNumber - unwind}
		return stagedsync.UnwindIntermediateHashesStage(u, stage6, db, "", ch)
	}
	return stagedsync.SpawnHashStateStage(stage6, db, "", ch)
}

func stage78(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	if reset {
		if err := resetHistory(db); err != nil {
			return err
		}
	}
	execStage := progress(db, stages.Execution)
	stage7 := progress(db, stages.AccountHistoryIndex)
	stage8 := progress(db, stages.StorageHistoryIndex)
	log.Info("Stage4", "progress", execStage.BlockNumber)
	log.Info("Stage7", "progress", stage7.BlockNumber)
	log.Info("Stage8", "progress", stage8.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 { //nolint:staticcheck
		// TODO
	}

	if err := stagedsync.SpawnAccountHistoryIndex(stage7, db, "", ch); err != nil {
		return err
	}
	if err := stagedsync.SpawnStorageHistoryIndex(stage8, db, "", ch); err != nil {
		return err
	}
	return nil
}

func stage9(ctx context.Context) error {
	core.UsePlainStateExecution = true

	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	if reset {
		if err := resetTxLookup(db); err != nil {
			return err
		}
	}
	stage9 := progress(db, stages.TxLookup)
	log.Info("Stage9", "progress", stage9.BlockNumber)
	ch := ctx.Done()

	if unwind > 0 {
		u := &stagedsync.UnwindState{Stage: stages.TxLookup, UnwindPoint: stage9.BlockNumber - unwind}
		s := progress(db, stages.TxLookup)
		return stagedsync.UnwindTxLookup(u, s, db, "", ch)
	}

	return stagedsync.SpawnTxLookup(stage9, db, "", ch)
}

func printAllStages(_ context.Context) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	return printStages(db)
}
