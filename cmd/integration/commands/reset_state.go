package commands

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/dbutils"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/log"
	"github.com/spf13/cobra"
)

var cmdResetState = &cobra.Command{
	Use:   "reset_state",
	Short: "Reset StateStages (5,6,7,8,9,10) and buckets",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := utils.RootContext()
		db := openDB(chaindata, true)
		defer db.Close()

		err := resetState(db, ctx)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		return nil
	},
}

func init() {
	withDatadir(cmdResetState)
	withChain(cmdResetState)

	rootCmd.AddCommand(cmdResetState)
}

func resetState(kv ethdb.RwKV, ctx context.Context) error {
	if err := kv.View(ctx, func(tx ethdb.Tx) error { return printStages(tx) }); err != nil {
		return err
	}
	// don't reset senders here
	if err := kv.Update(ctx, stagedsync.ResetHashState); err != nil {
		return err
	}
	if err := kv.Update(ctx, stagedsync.ResetIH); err != nil {
		return err
	}
	if err := kv.Update(ctx, resetHistory); err != nil {
		return err
	}
	if err := kv.Update(ctx, resetLogIndex); err != nil {
		return err
	}
	if err := kv.Update(ctx, resetCallTraces); err != nil {
		return err
	}
	if err := kv.Update(ctx, resetTxLookup); err != nil {
		return err
	}
	if err := kv.Update(ctx, resetTxPool); err != nil {
		return err
	}
	if err := kv.Update(ctx, resetFinish); err != nil {
		return err
	}

	genesis, _ := byChain()
	if err := kv.Update(ctx, func(tx ethdb.RwTx) error { return resetExec(tx, genesis) }); err != nil {
		return err
	}

	// set genesis after reset all buckets
	fmt.Printf("After reset: \n")
	if err := kv.View(ctx, func(tx ethdb.Tx) error { return printStages(tx) }); err != nil {
		return err
	}
	return nil
}

func resetSenders(tx ethdb.RwTx) error {
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.Senders); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Senders, 0); err != nil {
		return err
	}
	return nil
}

func resetExec(tx ethdb.RwTx, g *core.Genesis) error {
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.HashedAccountsBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.HashedStorageBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.ContractCodeBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.PlainStateBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.AccountChangeSetBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.StorageChangeSetBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.PlainContractCodeBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.Receipts); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.Log); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.IncarnationMapBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.CodeBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.CallTraceSet); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.Epoch); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Execution, 0); err != nil {
		return err
	}

	_, _, err := core.OverrideGenesisBlock(tx, g)
	if err != nil {
		return err
	}
	return nil
}

func resetHistory(tx ethdb.RwTx) error {
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.AccountsHistoryBucket); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.StorageHistoryBucket); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.AccountHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.StorageHistoryIndex, 0); err != nil {
		return err
	}

	return nil
}

func resetLogIndex(tx ethdb.RwTx) error {
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.LogAddressIndex); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.LogTopicIndex); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.LogIndex, 0); err != nil {
		return err
	}
	return nil
}

func resetCallTraces(tx ethdb.RwTx) error {
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.CallFromIndex); err != nil {
		return err
	}
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.CallToIndex); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.CallTraces, 0); err != nil {
		return err
	}
	return nil
}

func resetTxLookup(tx ethdb.RwTx) error {
	if err := tx.(ethdb.BucketMigrator).ClearBucket(dbutils.TxLookupPrefix); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	return nil
}

func resetTxPool(tx ethdb.RwTx) error {
	if err := stages.SaveStageProgress(tx, stages.TxPool, 0); err != nil {
		return err
	}
	return nil
}

func resetFinish(tx ethdb.RwTx) error {
	if err := stages.SaveStageProgress(tx, stages.Finish, 0); err != nil {
		return err
	}
	return nil
}

func printStages(db ethdb.KVGetter) error {
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
