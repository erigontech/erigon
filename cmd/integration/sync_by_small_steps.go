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

	var execProgress, ihProgress uint64
	db := ethdb.MustOpen(chaindata)
	defer db.Close()
	var err error

	blockchain, err := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil, nil)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}

		rewind := blocksPerStep

		execProgress, ihProgress = progress(db)

		if execProgress <= blocksPerStep+1 {
			rewind = 0
		}

		// Stage 4: 1 step back, 2 forward
		{
			u := &stagedsync.UnwindState{Stage: stages.Execution, UnwindPoint: execProgress - rewind}
			s := &stagedsync.StageState{Stage: stages.Execution, BlockNumber: execProgress}
			if err = stagedsync.UnwindExecutionStage(u, s, db); err != nil {
				return err
			}
		}

		execProgress, ihProgress = progress(db)
		{
			s := &stagedsync.StageState{Stage: stages.Execution, BlockNumber: execProgress}
			if err = stagedsync.SpawnExecuteBlocksStage(s, db, blockchain, execProgress+2*blocksPerStep, ctx.Done(), nil, false); err != nil {
				return err
			}
		}

		execProgress, ihProgress = progress(db)
		// Stage 5: 1 step back, 2 forward
		if ihProgress, _, err = stages.GetStageProgress(db, stages.IntermediateHashes); err != nil {
			return err
		}

		if ihProgress > blocksPerStep+1 {
			u := &stagedsync.UnwindState{Stage: stages.IntermediateHashes, UnwindPoint: execProgress - rewind}
			s := &stagedsync.StageState{Stage: stages.IntermediateHashes, BlockNumber: ihProgress}
			if err = stagedsync.UnwindIntermediateHashesStage(u, s, db, "", ctx.Done()); err != nil {
				return err
			}
		}

		execProgress, ihProgress = progress(db)
		{
			stageState := &stagedsync.StageState{Stage: stages.IntermediateHashes, BlockNumber: ihProgress}
			if err = stagedsync.SpawnIntermediateHashesStage(stageState, db, "", ctx.Done()); err != nil {
				return err
			}
		}
	}
}

func progress(db ethdb.Getter) (uint64, uint64) {
	var stage4progress, stage5progress uint64
	var err error
	stage4progress, _, err = stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		panic(err)
	}
	stage5progress, _, err = stages.GetStageProgress(db, stages.Execution)
	if err != nil {
		panic(err)
	}

	return stage4progress, stage5progress
}
