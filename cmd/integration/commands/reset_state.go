package commands

import (
	"context"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/eth/stagedsync"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

var cmdResetState = &cobra.Command{
	Use:   "reset_state",
	Short: "Reset StateStages (5,6,7,8,9,10) and buckets",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common.RootContext()
		logger := log.New()
		db := openDB(chaindata, logger, true)
		defer db.Close()

		err := resetState(db, logger, ctx)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		return nil
	},
}

func init() {
	withDataDir(cmdResetState)
	withChain(cmdResetState)

	rootCmd.AddCommand(cmdResetState)
}

func resetState(db kv.RwDB, logger log.Logger, ctx context.Context) error {
	if err := db.View(ctx, func(tx kv.Tx) error { return printStages(tx) }); err != nil {
		return err
	}
	// don't reset senders here
	if err := db.Update(ctx, stagedsync.ResetHashState); err != nil {
		return err
	}
	if err := db.Update(ctx, stagedsync.ResetIH); err != nil {
		return err
	}
	if err := db.Update(ctx, resetHistory); err != nil {
		return err
	}
	if err := db.Update(ctx, resetLogIndex); err != nil {
		return err
	}
	if err := db.Update(ctx, resetCallTraces); err != nil {
		return err
	}
	if err := db.Update(ctx, resetTxLookup); err != nil {
		return err
	}
	if err := db.Update(ctx, resetFinish); err != nil {
		return err
	}

	genesis, _ := byChain(chain)
	if err := db.Update(ctx, func(tx kv.RwTx) error { return resetExec(tx, genesis) }); err != nil {
		return err
	}

	// set genesis after reset all buckets
	fmt.Printf("After reset: \n")
	if err := db.View(ctx, func(tx kv.Tx) error { return printStages(tx) }); err != nil {
		return err
	}
	return nil
}

func resetSenders(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.Senders); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Senders, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.Senders, 0); err != nil {
		return err
	}
	return nil
}

func resetExec(tx kv.RwTx, g *core.Genesis) error {
	if err := tx.ClearBucket(kv.HashedAccounts); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.HashedStorage); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.ContractCode); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.PlainState); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.AccountChangeSet); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.StorageChangeSet); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.PlainContractCode); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.Receipts); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.Log); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.IncarnationMap); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.Code); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.CallTraceSet); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.Epoch); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.PendingEpoch); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.Execution, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.Execution, 0); err != nil {
		return err
	}

	if _, _, err := g.WriteGenesisState(tx); err != nil {
		return err
	}
	return nil
}

func resetHistory(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.AccountsHistory); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.StorageHistory); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.AccountHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.StorageHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.AccountHistoryIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.StorageHistoryIndex, 0); err != nil {
		return err
	}

	return nil
}

func resetLogIndex(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.LogAddressIndex); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.LogTopicIndex); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.LogIndex, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.LogIndex, 0); err != nil {
		return err
	}
	return nil
}

func resetCallTraces(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.CallFromIndex); err != nil {
		return err
	}
	if err := tx.ClearBucket(kv.CallToIndex); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.CallTraces, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.CallTraces, 0); err != nil {
		return err
	}
	return nil
}

func resetTxLookup(tx kv.RwTx) error {
	if err := tx.ClearBucket(kv.TxLookup); err != nil {
		return err
	}
	if err := stages.SaveStageProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.TxLookup, 0); err != nil {
		return err
	}
	return nil
}

func resetFinish(tx kv.RwTx) error {
	if err := stages.SaveStageProgress(tx, stages.Finish, 0); err != nil {
		return err
	}
	if err := stages.SaveStagePruneProgress(tx, stages.Finish, 0); err != nil {
		return err
	}
	return nil
}

func printStages(db kv.Tx) error {
	var err error
	var progress uint64
	w := new(tabwriter.Writer)
	defer w.Flush()
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	fmt.Fprintf(w, "Note: prune_at doesn't mean 'all data before were deleted' - it just mean stage.Prune function were run to this block. Because 1 stage may prune multiple data types to different prune distance.\n")
	fmt.Fprint(w, "\n \t stage_at \t prune_at\n")
	for _, stage := range stages.AllStages {
		if progress, err = stages.GetStageProgress(db, stage); err != nil {
			return err
		}
		prunedTo, err := stages.GetStagePruneProgress(db, stage)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s \t %d \t %d\n", string(stage), progress, prunedTo)
	}
	pm, err := prune.Get(db)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "--\n")
	fmt.Fprintf(w, "prune distance: %s\n\n", pm.String())

	s1, err := db.ReadSequence(kv.EthTx)
	if err != nil {
		return err
	}
	s2, err := db.ReadSequence(kv.NonCanonicalTxs)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "sequence: EthTx=%d, NonCanonicalTx=%d\n\n", s1, s2)

	return nil
}
