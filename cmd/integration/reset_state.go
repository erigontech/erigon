package main

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var cmdResetState = &cobra.Command{
	Use:   "reset",
	Short: "Reset sync stages and buckets",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := rootContext()
		if err := resetState(ctx, chaindata); err != nil {
			log.Error("Error", "err", err)
		}
		return nil
	},
}

func init() {
	withChaindata(cmdResetState)

	rootCmd.AddCommand(cmdResetState)
}

func resetState(_ context.Context, chaindata string) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()
	fmt.Printf("Before reset: \n")
	if err := printStages(db); err != nil {
		return err
	}

	core.UsePlainStateExecution = true
	if err := resetExec(db); err != nil {
		return err
	}
	if err := resetHashState(db); err != nil {
		return err
	}
	if err := resetHistory(db); err != nil {
		return err
	}

	// set genesis after reset all buckets
	if _, _, err := core.DefaultGenesisBlock().CommitGenesisState(db, false); err != nil {
		return err
	}

	fmt.Printf("After reset: \n")
	if err := printStages(db); err != nil {
		return err
	}
	return nil
}

func resetExec(db *ethdb.ObjectDatabase) error {
	var err error
	err = db.ClearBuckets(
		dbutils.CurrentStateBucket,
		dbutils.AccountChangeSetBucket,
		dbutils.StorageChangeSetBucket,
		dbutils.ContractCodeBucket,
		dbutils.PlainStateBucket,
		dbutils.PlainAccountChangeSetBucket,
		dbutils.PlainStorageChangeSetBucket,
		dbutils.PlainContractCodeBucket,
		dbutils.IncarnationMapBucket,
		dbutils.CodeBucket,
	)
	if err != nil {
		return err
	}
	err = stages.SaveStageProgress(db, stages.Execution, 0, nil)
	if err != nil {
		return err
	}
	return nil
}

func resetHashState(db *ethdb.ObjectDatabase) error {
	if err := db.ClearBuckets(
		dbutils.CurrentStateBucket,
		dbutils.ContractCodeBucket,
		dbutils.IntermediateTrieHashBucket,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.IntermediateHashes, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.HashState, 0, nil); err != nil {
		return err
	}
	return nil
}

func resetHistory(db *ethdb.ObjectDatabase) error {
	if err := db.ClearBuckets(
		dbutils.AccountsHistoryBucket,
		dbutils.StorageHistoryBucket,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.AccountHistoryIndex, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.StorageHistoryIndex, 0, nil); err != nil {
		return err
	}

	return nil
}

func printStages(db *ethdb.ObjectDatabase) error {
	var err error
	var progress uint64
	for stage := stages.SyncStage(0); stage < stages.Finish; stage++ {
		if progress, _, err = stages.GetStageProgress(db, stage); err != nil {
			return err
		}
		fmt.Printf("Stage: %d, progress: %d\n", stage, progress)
	}
	return nil
}
