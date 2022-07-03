package commands

import (
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

func init() {
	withBlock(recon1Cmd)
	withDataDir(recon1Cmd)
	rootCmd.AddCommand(recon1Cmd)
}

var recon1Cmd = &cobra.Command{
	Use:   "recon1",
	Short: "Exerimental command to reconstitute the state at given block",
	RunE: func(cmd *cobra.Command, args []string) error {
		logger := log.New()
		return Recon1(genesis, logger)
	},
}

func Recon1(genesis *core.Genesis, logger log.Logger) error {
	/*
		sigs := make(chan os.Signal, 1)
		interruptCh := make(chan bool, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

		go func() {
			<-sigs
			interruptCh <- true
		}()
		ctx := context.Background()
		reconDbPath := path.Join(datadir, "recon1db")
		var err error
		if _, err = os.Stat(reconDbPath); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				return err
			}
		} else if err = os.RemoveAll(reconDbPath); err != nil {
			return err
		}
		db, err := kv2.NewMDBX(logger).Path(reconDbPath).WriteMap().Open()
		if err != nil {
			return err
		}
		var blockReader services.FullBlockReader
		var allSnapshots *snapshotsync.RoSnapshots
		allSnapshots = snapshotsync.NewRoSnapshots(ethconfig.NewSnapCfg(true, false, true), path.Join(datadir, "snapshots"))
		defer allSnapshots.Close()
		if err := allSnapshots.Reopen(); err != nil {
			return fmt.Errorf("reopen snapshot segments: %w", err)
		}
		blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
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
		blockNum := block + 1
		txNum := txNums[blockNum-1]
		fmt.Printf("Corresponding block num = %d, txNum = %d\n", blockNum, txNum)
		workerCount := runtime.NumCPU()
	*/
	return nil
}
