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
	for {
		// Stage 4: forward
		{
			var err error
			db := ethdb.MustOpen(chaindata)
			var stage4progress uint64
			if stage4progress, _, err = stages.GetStageProgress(db, stages.Execution); err != nil {
				return err
			}
			core.UsePlainStateExecution = true
			ch := make(chan struct{})
			s := &stagedsync.StageState{Stage: stages.Execution, BlockNumber: stage4progress}
			blockchain, _ := core.NewBlockChain(db, nil, params.MainnetChainConfig, ethash.NewFaker(), vm.Config{}, nil, nil, nil)
			if err = stagedsync.SpawnExecuteBlocksStage(s, db, blockchain, stage4progress+2, ch, nil, false); err != nil {
				return err
			}
			close(ch)
			db.Close()
		}

		// Stage 5: forward
		{
			var err error
			db := ethdb.MustOpen(chaindata)
			var stage5progress uint64
			if stage5progress, _, err = stages.GetStageProgress(db, stages.IntermediateHashes); err != nil {
				return err
			}
			log.Info("Stage5", "progress", stage5progress)
			core.UsePlainStateExecution = true
			ch := make(chan struct{})
			stageState := &stagedsync.StageState{Stage: stages.IntermediateHashes, BlockNumber: stage5progress}
			if err = stagedsync.SpawnIntermediateHashesStage(stageState, db, "", ch); err != nil {
				return err
			}
			close(ch)
			db.Close()
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
	stage5progress, _, err = stages.GetStageProgress(db, stages.IntermediateHashes)
	if err != nil {
		panic(err)
	}

	return stage4progress, stage5progress
}
