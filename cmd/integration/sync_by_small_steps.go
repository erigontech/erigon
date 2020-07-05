package main

import (
	"context"

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

var syncBySmallSteps = &cobra.Command{
	Use:   "sync_by_small_steps",
	Short: "Staged sync in mode '1 step back 2 steps forward' with integrity checks after each step",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		return testStagesIntegration(ctx, chaindata)
	},
}

func init() {
	rootCmd.AddCommand(syncBySmallSteps)
}

func testStagesIntegration(ctx context.Context, chaindata string) error {
	var stage4progress, stage5progress uint64
	blocksPerIteration := uint64(1) // how much blocks unwind/exec on each iteration
	db := ethdb.MustOpen(chaindata)
	defer db.Close()
	var err error

	blockchain, err := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil, nil)
	if err != nil {
		return err
	}
	ch := make(chan struct{})
	defer close(ch)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		rewind := blocksPerIteration

		if stage4progress, _, err = stages.GetStageProgress(db, stages.Execution); err != nil {
			return err
		}
		if stage5progress, _, err = stages.GetStageProgress(db, stages.IntermediateHashes); err != nil {
			return err
		}
		log.Info("Stages", "Exec", stage4progress, "IH", stage5progress)
		core.UsePlainStateExecution = true
		if stage4progress == 0 {
			rewind = 0
		}

		// Stage 4: 1 step back, 2 forward
		{
			u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: stage4progress - rewind}
			s := &stagedsync.StageState{Stage: stages.Execution, BlockNumber: stage4progress}
			if err = stagedsync.UnwindExecutionStage(u, s, db); err != nil {
				return err
			}
		}
		{
			s := &stagedsync.StageState{Stage: stages.Execution, BlockNumber: stage4progress}
			if err = stagedsync.SpawnExecuteBlocksStage(s, db, blockchain, stage4progress+blocksPerIteration, ch, nil, false); err != nil {
				return err
			}
		}

		// Stage 5: 1 step back, 2 forward
		{
			u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: stage5progress - rewind}
			s := &stagedsync.StageState{Stage: stages.IntermediateHashes, BlockNumber: stage5progress}
			if err = stagedsync.UnwindHashStateStage(u, s, db, "", ch); err != nil {
				return err
			}
		}
		{
			stageState := &stagedsync.StageState{Stage: stages.IntermediateHashes, BlockNumber: stage5progress}
			if err = stagedsync.SpawnIntermediateHashesStage(stageState, db, "", ch); err != nil {
				return err
			}
		}
	}
	return nil
}
