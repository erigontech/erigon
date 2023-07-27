package commands

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"text/tabwriter"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/spf13/cobra"

	"github.com/ledgerwatch/erigon/core/rawdb/rawdbhelpers"
	reset2 "github.com/ledgerwatch/erigon/core/rawdb/rawdbreset"
	"github.com/ledgerwatch/erigon/eth/stagedsync/stages"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/turbo/debug"
)

var cmdResetState = &cobra.Command{
	Use:   "reset_state",
	Short: "Reset StateStages (5,6,7,8,9,10) and buckets",
	Run: func(cmd *cobra.Command, args []string) {
		logger := debug.SetupCobra(cmd, "integration")
		db, err := openDB(dbCfg(kv.ChainDB, chaindata), true, logger)
		if err != nil {
			logger.Error("Opening DB", "error", err)
			return
		}
		ctx, _ := common.RootContext()
		defer db.Close()
		sn, agg := allSnapshots(ctx, db, logger)
		defer sn.Close()
		defer agg.Close()

		if err := db.View(ctx, func(tx kv.Tx) error { return printStages(tx, sn, agg) }); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}

		if err = reset2.ResetState(db, ctx, chain, ""); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}

		// set genesis after reset all buckets
		fmt.Printf("After reset: \n")
		if err := db.View(ctx, func(tx kv.Tx) error { return printStages(tx, sn, agg) }); err != nil {
			if !errors.Is(err, context.Canceled) {
				logger.Error(err.Error())
			}
			return
		}
	},
}

func init() {
	withConfig(cmdResetState)
	withDataDir(cmdResetState)
	withChain(cmdResetState)

	rootCmd.AddCommand(cmdResetState)
}

func printStages(tx kv.Tx, snapshots *freezeblocks.RoSnapshots, agg *state.AggregatorV3) error {
	var err error
	var progress uint64
	w := new(tabwriter.Writer)
	defer w.Flush()
	w.Init(os.Stdout, 8, 8, 0, '\t', 0)
	fmt.Fprintf(w, "Note: prune_at doesn't mean 'all data before were deleted' - it just mean stage.Prune function were run to this block. Because 1 stage may prune multiple data types to different prune distance.\n")
	fmt.Fprint(w, "\n \t\t stage_at \t prune_at\n")
	for _, stage := range stages.AllStages {
		if progress, err = stages.GetStageProgress(tx, stage); err != nil {
			return err
		}
		prunedTo, err := stages.GetStagePruneProgress(tx, stage)
		if err != nil {
			return err
		}
		fmt.Fprintf(w, "%s \t\t %d \t %d\n", string(stage), progress, prunedTo)
	}
	pm, err := prune.Get(tx)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "--\n")
	fmt.Fprintf(w, "prune distance: %s\n\n", pm.String())
	fmt.Fprintf(w, "blocks.v2: blocks=%d, segments=%d, indices=%d\n\n", snapshots.BlocksAvailable(), snapshots.SegmentsMax(), snapshots.IndicesMax())
	h3, err := kvcfg.HistoryV3.Enabled(tx)
	if err != nil {
		return err
	}
	lastK, lastV, err := rawdbv3.Last(tx, kv.MaxTxNum)
	if err != nil {
		return err
	}

	_, lastBlockInHistSnap, _ := rawdbv3.TxNums.FindBlockNum(tx, agg.EndTxNumMinimax())
	fmt.Fprintf(w, "history.v3: %t,  idx steps: %.02f, lastMaxTxNum=%d->%d, lastBlockInSnap=%d\n\n", h3, rawdbhelpers.IdxStepsCountV3(tx), u64or0(lastK), u64or0(lastV), lastBlockInHistSnap)
	s1, err := tx.ReadSequence(kv.EthTx)
	if err != nil {
		return err
	}
	s2, err := tx.ReadSequence(kv.NonCanonicalTxs)
	if err != nil {
		return err
	}
	fmt.Fprintf(w, "sequence: EthTx=%d, NonCanonicalTx=%d\n\n", s1, s2)

	{
		firstNonGenesisHeader, err := rawdbv3.SecondKey(tx, kv.Headers)
		if err != nil {
			return err
		}
		lastHeaders, err := rawdbv3.LastKey(tx, kv.Headers)
		if err != nil {
			return err
		}
		firstNonGenesisBody, err := rawdbv3.SecondKey(tx, kv.BlockBody)
		if err != nil {
			return err
		}
		lastBody, err := rawdbv3.LastKey(tx, kv.BlockBody)
		if err != nil {
			return err
		}
		fstHeader := u64or0(firstNonGenesisHeader)
		lstHeader := u64or0(lastHeaders)
		fstBody := u64or0(firstNonGenesisBody)
		lstBody := u64or0(lastBody)
		fmt.Fprintf(w, "in db: first header %d, last header %d, first body %d, last body %d\n", fstHeader, lstHeader, fstBody, lstBody)
	}

	fmt.Fprintf(w, "--\n")

	//fmt.Printf("==== state =====\n")
	//db.ForEach(kv.PlainState, nil, func(k, v []byte) error {
	//	fmt.Printf("st: %x, %x\n", k, v)
	//	return nil
	//})
	//fmt.Printf("====  code =====\n")
	//db.ForEach(kv.Code, nil, func(k, v []byte) error {
	//	fmt.Printf("code: %x, %x\n", k, v)
	//	return nil
	//})
	//fmt.Printf("==== PlainContractCode =====\n")
	//db.ForEach(kv.PlainContractCode, nil, func(k, v []byte) error {
	//	fmt.Printf("code2: %x, %x\n", k, v)
	//	return nil
	//})
	//fmt.Printf("====  IncarnationMap =====\n")
	//db.ForEach(kv.IncarnationMap, nil, func(k, v []byte) error {
	//	fmt.Printf("IncarnationMap: %x, %x\n", k, v)
	//	return nil
	//})
	return nil
}
func u64or0(in []byte) (v uint64) {
	if len(in) > 0 {
		v = binary.BigEndian.Uint64(in)
	}
	return v
}
