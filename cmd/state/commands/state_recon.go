package commands

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"sort"
	"syscall"

	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(reconCmd)
	withDataDir(reconCmd)
	rootCmd.AddCommand(reconCmd)
}

var reconCmd = &cobra.Command{
	Use:   "recon",
	Short: "Exerimental command to reconstitute the state from state history at given block",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Recon(genesis, logger)
	},
}

func Recon(genesis *core.Genesis, logger log.Logger) error {
	sigs := make(chan os.Signal, 1)
	interruptCh := make(chan bool, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		interruptCh <- true
	}()
	historyDb, err := kv2.NewMDBX(logger).Path(path.Join(datadir, "chaindata")).Open()
	if err != nil {
		return fmt.Errorf("opening chaindata as read only: %v", err)
	}
	defer historyDb.Close()
	ctx := context.Background()
	historyTx, err1 := historyDb.BeginRo(ctx)
	if err1 != nil {
		return err1
	}
	defer historyTx.Rollback()
	aggPath := filepath.Join(datadir, "erigon22")
	agg, err := libstate.NewAggregator(aggPath, AggregationStep)
	if err != nil {
		return fmt.Errorf("create history: %w", err)
	}
	defer agg.Close()
	reconDbPath := path.Join(datadir, "recondb")
	if block == 0 {
		if _, err = os.Stat(reconDbPath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if err = os.RemoveAll(reconDbPath); err != nil {
			return err
		}
	}
	db, err := kv2.NewMDBX(logger).Path(reconDbPath).WriteMap().Open()
	if err != nil {
		return err
	}
	var rwTx kv.RwTx
	defer func() {
		if rwTx != nil {
			rwTx.Rollback()
		}
	}()
	if rwTx, err = db.BeginRw(ctx); err != nil {
		return err
	}
	/*
		chainConfig := genesis.Config
		vmConfig := vm.Config{}

		interrupt := false
		blockNum := uint64(0)
		var txNum uint64 = 2
		trace := false
		logEvery := time.NewTicker(logInterval)
		defer logEvery.Stop()
		prevBlock := blockNum
		prevTime := time.Now()

		var blockReader services.FullBlockReader
	*/
	var allSnapshots *snapshotsync.RoSnapshots
	allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadir, "snapshots"))
	defer allSnapshots.Close()
	if err := allSnapshots.Reopen(); err != nil {
		return fmt.Errorf("reopen snapshot segments: %w", err)
	}
	//blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
	// Compute mapping blockNum -> last TxNum in that block
	txNums := make([]uint64, allSnapshots.BlocksAvailable()+1)
	if err = allSnapshots.Bodies.View(func(bs []*snapshotsync.BodySegment) error {
		for _, b := range bs {
			if err = b.Iterate(func(blockNum, baseTxNum, txAmount uint64) {
				txNums[blockNum] = baseTxNum + txAmount
			}); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return fmt.Errorf("build txNum => blockNum mapping: %w", err)
	}
	endTxNumMinimax := agg.EndTxNumMinimax()
	fmt.Printf("Max txNum in files: %d\n", endTxNumMinimax)
	blockNum := uint64(sort.Search(len(txNums), func(i int) bool {
		return txNums[i] > endTxNumMinimax
	}))
	txNum := txNums[blockNum-1]
	fmt.Printf("Corresponding block num = %d, txNum = %d\n", blockNum, txNum)
	bitmap := agg.ReconBitmap(txNum)
	fmt.Printf("Bitmap length = %d\n", bitmap.GetCardinality())

	return nil
}
