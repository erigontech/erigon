package commands

import (
	"context"
	"fmt"
	"os"
	"path"
	"sync"
	"text/tabwriter"
	"time"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"

	"github.com/ledgerwatch/lmdb-go/lmdb"
	"github.com/ledgerwatch/turbo-geth/common"
	"github.com/ledgerwatch/turbo-geth/common/dbutils"
	"github.com/ledgerwatch/turbo-geth/core"
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
		err := resetState(ctx)
		if err != nil {
			log.Error(err.Error())
			return err
		}
		if compact {
			if err := copyCompact(); err != nil {
				return err
			}
		}

		return nil
	},
}

var cmdClearUnwindStack = &cobra.Command{
	Use:   "clear_unwind_stack",
	Short: "Clear unwind stack",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := utils.RootContext()
		err := clearUnwindStack(ctx)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		return nil
	},
}

func init() {
	withChaindata(cmdResetState)
	withCompact(cmdResetState)

	rootCmd.AddCommand(cmdResetState)

	withChaindata(cmdClearUnwindStack)

	rootCmd.AddCommand(cmdClearUnwindStack)
}

func clearUnwindStack(_ context.Context) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()

	for _, stage := range stages.AllStages {
		if err := stages.SaveStageUnwind(db, stage, 0, nil); err != nil {
			return err
		}
	}
	return nil
}

func resetState(_ context.Context) error {
	db := ethdb.MustOpen(chaindata)
	defer db.Close()
	fmt.Printf("Before reset: \n")
	if err := printStages(db); err != nil {
		return err
	}

	core.UsePlainStateExecution = true
	// don't reset senders here
	if err := resetExec(db); err != nil {
		return err
	}
	if err := stagedsync.ResetHashState(db); err != nil {
		return err
	}
	if err := resetHistory(db); err != nil {
		return err
	}
	if err := resetTxLookup(db); err != nil {
		return err
	}
	if err := resetTxPool(db); err != nil {
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

func resetSenders(db *ethdb.ObjectDatabase) error {
	if err := db.ClearBuckets(
		dbutils.Senders,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.Senders, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.Senders, 0, nil); err != nil {
		return err
	}
	return nil
}

func resetExec(db *ethdb.ObjectDatabase) error {
	if err := db.ClearBuckets(
		dbutils.CurrentStateBucket,
		dbutils.AccountChangeSetBucket,
		dbutils.StorageChangeSetBucket,
		dbutils.ContractCodeBucket,
		dbutils.PlainStateBucket,
		dbutils.PlainAccountChangeSetBucket,
		dbutils.PlainStorageChangeSetBucket,
		dbutils.PlainContractCodeBucket,
		dbutils.BlockReceiptsPrefix,
		dbutils.IncarnationMapBucket,
		dbutils.CodeBucket,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.Execution, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.Execution, 0, nil); err != nil {
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
	if err := stages.SaveStageUnwind(db, stages.AccountHistoryIndex, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.StorageHistoryIndex, 0, nil); err != nil {
		return err
	}

	return nil
}

func resetTxLookup(db *ethdb.ObjectDatabase) error {
	if err := db.ClearBuckets(
		dbutils.TxLookupPrefix,
	); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(db, stages.TxLookup, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.TxLookup, 0, nil); err != nil {
		return err
	}

	return nil
}

func resetTxPool(db ethdb.Putter) error {
	if err := stages.SaveStageProgress(db, stages.TxPool, 0, nil); err != nil {
		return err
	}
	if err := stages.SaveStageUnwind(db, stages.TxPool, 0, nil); err != nil {
		return err
	}

	return nil
}

func printStages(db *ethdb.ObjectDatabase) error {
	var err error
	var progress uint64
	w := new(tabwriter.Writer)
	defer w.Flush()
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	for _, stage := range stages.AllStages {
		if progress, _, err = stages.GetStageProgress(db, stage); err != nil {
			return err
		}
		fmt.Fprintf(w, "%s \t %d\n", string(stage), progress)
	}
	return nil
}

func copyCompact() error {
	from := chaindata
	backup := from + "_backup"
	to := chaindata + "_copy"

	log.Info("Start db copy-compact")

	env, errOpen := lmdb.NewEnv()
	if errOpen != nil {
		return errOpen
	}

	if err := env.Open(from, lmdb.Readonly, 0644); err != nil {
		return err
	}
	_ = os.RemoveAll(to)
	if err := os.MkdirAll(to, 0744); err != nil {
		return fmt.Errorf("could not create dir: %s, %w", to, err)
	}
	if err := env.SetMapSize(int64(ethdb.LMDBMapSize.Bytes())); err != nil {
		return err
	}

	f, err := os.Stat(path.Join(from, "data.mdb"))
	if err != nil {
		return err
	}

	ctx, stopLogging := context.WithCancel(context.Background())
	defer stopLogging()
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		for {
			time.Sleep(20 * time.Second)

			select {
			case <-ctx.Done():
				return
			default:
			}

			f2, err := os.Stat(path.Join(to, "data.mdb"))
			if err != nil {
				log.Error("Progress check failed", "err", err)
				return
			}
			log.Info("Progress", "done", common.StorageSize(f2.Size()), "from", common.StorageSize(f.Size()))
		}
	}()

	if err := env.CopyFlag(to, lmdb.CopyCompact); err != nil {
		return fmt.Errorf("%w, from: %s, to: %s", err, from, to)
	}

	stopLogging()
	wg.Wait()
	if err := os.Rename(from, backup); err != nil {
		return err
	}
	if err := os.Rename(to, from); err != nil {
		return err
	}
	if err := os.RemoveAll(backup); err != nil {
		return err
	}

	return nil
}
