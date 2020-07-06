package main

import (
	"context"
	"fmt"

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

var cmdSyncBySmallSteps = &cobra.Command{
	Use:   "sync_by_small_steps",
	Short: "Staged sync in mode '1 step back 2 steps forward' with integrity checks after each step",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if err := syncBySmallSteps(ctx, chaindata); err != nil {
			log.Error("Error", "err", err)
		}
		return nil
	},
}

func init() {
	withChaindata(cmdSyncBySmallSteps)
	withBlocksPerStep(cmdSyncBySmallSteps)

	rootCmd.AddCommand(cmdSyncBySmallSteps)
}

func syncBySmallSteps(ctx context.Context, chaindata string) error {
	core.UsePlainStateExecution = true
	db := ethdb.MustOpen(chaindata)
	defer db.Close()
	blockchain, err := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil, nil)
	if err != nil {
		return err
	}
	defer blockchain.Stop()
	ch := ctx.Done()

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		{
			stage := progress(db, stages.Execution)
			execToBlock := stage.BlockNumber + 2*blocksPerStep
			if err = stagedsync.SpawnExecuteBlocksStage(stage, db, blockchain, execToBlock, ch, nil, false); err != nil {
				return fmt.Errorf("SpawnExecuteBlocksStage: %w", err)
			}
		}

		{
			stage := progress(db, stages.IntermediateHashes)
			if err = stagedsync.SpawnIntermediateHashesStage(stage, db, "", ch); err != nil {
				return fmt.Errorf("SpawnIntermediateHashesStage: %w", err)
			}
		}

		{
			stage := progress(db, stages.HashState)
			if err = stagedsync.SpawnHashStateStage(stage, db, "", ch); err != nil {
				return fmt.Errorf("SpawnHashStateStage: %w", err)
			}
		}

		{
			stage7 := progress(db, stages.AccountHistoryIndex)
			stage8 := progress(db, stages.StorageHistoryIndex)
			if err = stagedsync.SpawnAccountHistoryIndex(stage7, db, "", ch); err != nil {
				return fmt.Errorf("SpawnAccountHistoryIndex: %w", err)
			}
			if err = stagedsync.SpawnStorageHistoryIndex(stage8, db, "", ch); err != nil {
				return fmt.Errorf("SpawnStorageHistoryIndex: %w", err)
			}
		}

		// Unwind all stages to `execStage - blocksPerStep` block
		execStage := progress(db, stages.Execution)
		to := execStage.BlockNumber - blocksPerStep
		{
			u := &stagedsync.UnwindState{Stage: stages.StorageHistoryIndex, UnwindPoint: to}
			if err = stagedsync.UnwindStorageHistoryIndex(u, db, ch); err != nil {
				return fmt.Errorf("UnwindStorageHistoryIndex: %w", err)
			}

			u = &stagedsync.UnwindState{Stage: stages.AccountHistoryIndex, UnwindPoint: to}
			if err = stagedsync.UnwindAccountHistoryIndex(u, db, ch); err != nil {
				return fmt.Errorf("UnwindAccountHistoryIndex: %w", err)
			}
		}

		{
			u := &stagedsync.UnwindState{Stage: stages.HashState, UnwindPoint: to}
			stage := progress(db, stages.HashState)
			if err = stagedsync.UnwindHashStateStage(u, stage, db, "", ch); err != nil {
				return fmt.Errorf("UnwindHashStateStage: %w", err)
			}
		}

		{
			u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: to}
			stage := progress(db, stages.IntermediateHashes)
			if err = stagedsync.UnwindIntermediateHashesStage(u, stage, db, "", ch); err != nil {
				return fmt.Errorf("UnwindIntermediateHashesStage: %w", err)
			}
		}

		{
			stage := progress(db, stages.Execution)
			u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: to}
			if err = stagedsync.UnwindExecutionStage(u, stage, db); err != nil {
				return fmt.Errorf("UnwindExecutionStage: %w", err)
			}
		}
	}
}

func progress(db ethdb.Getter, stage stages.SyncStage) *stagedsync.StageState {
	stageProgress, _, err := stages.GetStageProgress(db, stage)
	if err != nil {
		panic(err)
	}
	return &stagedsync.StageState{Stage: stage, BlockNumber: stageProgress}
}
