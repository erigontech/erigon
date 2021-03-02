package commands

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
	"github.com/ledgerwatch/turbo-geth/core/rawdb"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync"
	"github.com/ledgerwatch/turbo-geth/eth/stagedsync/stages"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var cmdResetState = &cobra.Command{
	Use:   "reset_state",
	Short: "Reset StateStages (5,6,7,8,9,10) and buckets",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		err := resetState(db, ctx)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		return nil
	},
}

var cmdClearUnwindStack = &cobra.Command{
	Use:   "clear_unwind_stack",
	Short: "Clear unwind stack",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		db := openDatabase(chaindata, true)
		defer db.Close()

		err := clearUnwindStack(db, ctx)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		return nil
	},
}

func init() {
	withChaindata(cmdResetState)

	rootCmd.AddCommand(cmdResetState)

	withChaindata(cmdClearUnwindStack)

	rootCmd.AddCommand(cmdClearUnwindStack)
}

func clearUnwindStack(db rawdb.DatabaseWriter, _ context.Context) error {
	for _, stage := range stages.AllStages {
		if err := stages.SaveStageUnwind(db, stage, 0); err != nil {
			return err
		}
	}
	return nil
}

func resetState(db ethdb.Database, _ context.Context) error {
	fmt.Printf("Before reset: \n")
	if err := printStages(db); err != nil {
		return err
	}

	// don't reset senders here
	if err := resetExec(db); err != nil {
		return err
	}
	if err := stagedsync.ResetHashState(db); err != nil {
		return err
	}
	if err := stagedsync.ResetIH(db); err != nil {
		return err
	}
	if err := resetHistory(db); err != nil {
		return err
	}
	if err := resetLogIndex(db); err != nil {
		return err
	}
	if err := resetCallTraces(db); err != nil {
		return err
	}
	if err := resetTxLookup(db); err != nil {
		return err
	}
	if err := resetTxPool(db); err != nil {
		return err
	}
	if err := resetFinish(db); err != nil {
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

func resetSenders(db rawdb.DatabaseWriter) error {
	if err := db.(ethdb.BucketsMigrator).ClearBuckets(
		dbutils.Senders,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.Senders, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.Senders, 0); err != nil {
		return err
	}
	return nil
}

func resetExec(db rawdb.DatabaseWriter) error {
	if err := db.(ethdb.BucketsMigrator).ClearBuckets(
		dbutils.HashedAccountsBucket,
		dbutils.HashedStorageBucket,
		dbutils.ContractCodeBucket,
		dbutils.PlainStateBucket,
		dbutils.PlainAccountChangeSetBucket,
		dbutils.PlainStorageChangeSetBucket,
		dbutils.PlainContractCodeBucket,
		dbutils.BlockReceiptsPrefix,
		dbutils.Log,
		dbutils.IncarnationMapBucket,
		dbutils.CodeBucket,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.Execution, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.Execution, 0); err != nil {
		return err
	}
	return nil
}

func resetHistory(db rawdb.DatabaseWriter) error {
	if err := db.(ethdb.BucketsMigrator).ClearBuckets(
		dbutils.AccountsHistoryBucket,
		dbutils.StorageHistoryBucket,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.AccountHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.StorageHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.AccountHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.StorageHistoryIndex, 0); err != nil {
		return err
	}

	return nil
}

func resetLogIndex(db rawdb.DatabaseWriter) error {
	if err := db.(ethdb.BucketsMigrator).ClearBuckets(
		dbutils.LogAddressIndex,
		dbutils.LogTopicIndex,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.LogIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.LogIndex, 0); err != nil {
		return err
	}

	return nil
}

func resetCallTraces(db rawdb.DatabaseWriter) error {
	if err := db.(ethdb.BucketsMigrator).ClearBuckets(
		dbutils.CallFromIndex,
		dbutils.CallToIndex,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.CallTraces, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.CallTraces, 0); err != nil {
		return err
	}

	return nil
}

func resetTxLookup(db rawdb.DatabaseWriter) error {
	if err := db.(ethdb.BucketsMigrator).ClearBuckets(
		dbutils.TxLookupPrefix,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.TxLookup, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.TxLookup, 0); err != nil {
		return err
	}

	return nil
}

func resetTxPool(db ethdb.Putter) error {
	if err := stages.SaveStageProgress(db, stages.TxPool, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.TxPool, 0); err != nil {
		return err
	}

	return nil
}

func resetFinish(db ethdb.Putter) error {
	if err := stages.SaveStageProgress(db, stages.Finish, 0); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.Finish, 0); err != nil {
		return err
	}

	return nil
}

func printStages(db ethdb.Getter) error {
	var err error
	var progress uint64
	w := new(tabwriter.Writer)
	defer w.Flush()
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	for _, stage := range stages.AllStages {
		if progress, err = stages.GetStageProgress(db, stage); err != nil {
			return err
		}
		fmt.Fprintf(w, "%s \t %d\n", string(stage), progress)
	}
	return nil
}
