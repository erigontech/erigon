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

var stateStags = &cobra.Command{
	Use: "state_stages",
	Short: `
		Move all StateStages (4,5,6,7,8) forward. 
		Stops at Stage 3 progress or at "--stop".
		Each iteration test will move forward "--unwind_every" blocks, then unwind "--unwind" blocks.
		Use reset_state command to re-run this test.
		Example: go run ./cmd/integration state_stages --chaindata=... --verbosity=3 --unwind=100 --unwind_every=100000 --block=2000000
		`,
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if err := syncBySmallSteps(ctx, chaindata); err != nil {
			log.Error("Error", "err", err)
		}
		return nil
	},
}

func init() {
	withChaindata(stateStags)
	withUnwind(stateStags)
	withUnwindEvery(stateStags)
	withStop(stateStags)

	rootCmd.AddCommand(stateStags)
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
	ch := make(chan struct{})
	defer close(ch)

	senderStageProgress := progress(db, stages.Senders).BlockNumber

	var stopAt = senderStageProgress
	if stop > 0 && stop < senderStageProgress {
		stopAt = stop
	}

	for progress(db, stages.Execution).BlockNumber+unwindEvery < stopAt {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		// All stages forward to `execStage + unwindEvery` block
		{
			stage := progress(db, stages.Execution)
			execToBlock := stage.BlockNumber + unwindEvery
			if err = stagedsync.SpawnExecuteBlocksStage(stage, db, blockchain, execToBlock, ch, nil, false); err != nil {
				return fmt.Errorf("spawnExecuteBlocksStage: %w", err)
			}
		}

		{
			stage := progress(db, stages.IntermediateHashes)
			if err = stagedsync.SpawnIntermediateHashesStage(stage, db, "", ch); err != nil {
				return fmt.Errorf("spawnIntermediateHashesStage: %w", err)
			}
		}

		{
			stage := progress(db, stages.HashState)
			if err = stagedsync.SpawnHashStateStage(stage, db, "", ch); err != nil {
				return fmt.Errorf("spawnHashStateStage: %w", err)
			}
		}

		{
			stage7 := progress(db, stages.AccountHistoryIndex)
			stage8 := progress(db, stages.StorageHistoryIndex)
			if err = stagedsync.SpawnAccountHistoryIndex(stage7, db, "", ch); err != nil {
				return fmt.Errorf("spawnAccountHistoryIndex: %w", err)
			}
			if err = stagedsync.SpawnStorageHistoryIndex(stage8, db, "", ch); err != nil {
				return fmt.Errorf("spawnStorageHistoryIndex: %w", err)
			}
		}

		// Unwind all stages to `execStage - unwind` block
		execStage := progress(db, stages.Execution)
		to := execStage.BlockNumber - unwind
		{
			u := &stagedsync.UnwindState{Stage: stages.StorageHistoryIndex, UnwindPoint: to}
			if err = stagedsync.UnwindStorageHistoryIndex(u, db, ch); err != nil {
				return fmt.Errorf("unwindStorageHistoryIndex: %w", err)
			}

			u = &stagedsync.UnwindState{Stage: stages.AccountHistoryIndex, UnwindPoint: to}
			if err = stagedsync.UnwindAccountHistoryIndex(u, db, ch); err != nil {
				return fmt.Errorf("unwindAccountHistoryIndex: %w", err)
			}
		}

		{
			u := &stagedsync.UnwindState{Stage: stages.HashState, UnwindPoint: to}
			stage := progress(db, stages.HashState)
			if err = stagedsync.UnwindHashStateStage(u, stage, db, "", ch); err != nil {
				return fmt.Errorf("unwindHashStateStage: %w", err)
			}
		}

		{
			u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: to}
			stage := progress(db, stages.IntermediateHashes)
			if err = stagedsync.UnwindIntermediateHashesStage(u, stage, db, "", ch); err != nil {
				return fmt.Errorf("unwindIntermediateHashesStage: %w", err)
			}
		}

		{
			stage := progress(db, stages.Execution)
			u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: to}
			if err = stagedsync.UnwindExecutionStage(u, stage, db); err != nil {
				return fmt.Errorf("unwindExecutionStage: %w", err)
			}
		}
	}

	return nil
}

func progress(db ethdb.Getter, stage stages.SyncStage) *stagedsync.StageState {
	stageProgress, _, err := stages.GetStageProgress(db, stage)
	if err != nil {
		panic(err)
	}
	return &stagedsync.StageState{Stage: stage, BlockNumber: stageProgress}
}
