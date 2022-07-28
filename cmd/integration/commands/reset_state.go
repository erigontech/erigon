package commands

import (
	"encoding/binary"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon/core/rawdb"
	reset2 "github.com/ledgerwatch/erigon/core/rawdb/rawdbreset"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

var cmdResetState = &cobra.Command{
	Use:   "reset_state",
	Short: "Reset StateStages (5,6,7,8,9,10) and buckets",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx, _ := common.RootContext()
		logger := log.New()
		db := openDB(dbCfg(kv.ChainDB, logger, chaindata), true)
		defer db.Close()
		if err := db.View(ctx, func(tx kv.Tx) error { return printStages(tx, allSnapshots(db)) }); err != nil {
			return err
		}

		genesis, _ := genesisByChain(chain)
		err := reset2.ResetState(db, ctx, genesis)
		if err != nil {
			log.Error(err.Error())
			return err
		}

		// set genesis after reset all buckets
		fmt.Printf("After reset: \n")
		if err := db.View(ctx, func(tx kv.Tx) error { return printStages(tx, allSnapshots(db)) }); err != nil {
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

func printStages(db kv.Tx, snapshots *snapshotsync.RoSnapshots) error {
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

	{
		firstNonGenesis, err := rawdb.SecondKey(db, kv.Headers)
		if err != nil {
			return err
		}
		if firstNonGenesis != nil {
			fmt.Fprintf(w, "first header in db: %d\n", binary.BigEndian.Uint64(firstNonGenesis))
		} else {
			fmt.Fprintf(w, "no headers in db\n")
		}
		firstNonGenesis, err = rawdb.SecondKey(db, kv.BlockBody)
		if err != nil {
			return err
		}
		if firstNonGenesis != nil {
			fmt.Fprintf(w, "first body in db: %d\n\n", binary.BigEndian.Uint64(firstNonGenesis))
		} else {
			fmt.Fprintf(w, "no bodies in db\n\n")
		}
	}

	fmt.Fprintf(w, "--\n")
	fmt.Fprintf(w, "snapsthos: blocks=%d, segments=%d, indices=%d\n\n", snapshots.BlocksAvailable(), snapshots.SegmentsMax(), snapshots.IndicesMax())

	return nil
}
