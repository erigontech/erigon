package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/common/paths"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))

}
func Execute() {
	if err := rootCmd.ExecuteContext(rootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func rootContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		ch := make(chan os.Signal, 1)
		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case <-ch:
			log.Info("Got interrupt, shutting down...")
		case <-ctx.Done():
		}

		cancel()
	}()
	return ctx
}

var (
	datadir      string
	database     string
	chaindata    string
	snapshotFile string
	block        uint64
	snapshotDir  string
	snapshotMode string
)

var rootCmd = &cobra.Command{
	Use:   "generate_snapshot",
	Short: "generate snapshot",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}
		if chaindata == "" {
			chaindata = path.Join(datadir, "tg", "chaindata")
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withBlock(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 1, "specifies a block number for operation")
}
func withSnapshotData(cmd *cobra.Command) {
	cmd.Flags().StringVar(&snapshotMode, "snapshot.mode", "", "set of snapshots to use")
	cmd.Flags().StringVar(&snapshotDir, "snapshot.dir", "", "snapshot dir")
}

func withDatadir(cmd *cobra.Command) {
	cmd.Flags().StringVar(&datadir, "datadir", paths.DefaultDataDir(), "data directory for temporary ELT files")
	must(cmd.MarkFlagDirname("datadir"))

	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))

	cmd.Flags().StringVar(&snapshotMode, "snapshot.mode", "", "set of snapshots to use")
	cmd.Flags().StringVar(&snapshotDir, "snapshot.dir", "", "snapshot dir")
	must(cmd.MarkFlagDirname("snapshot.dir"))

	cmd.Flags().StringVar(&database, "database", "", "lmdb|mdbx")
}

func withSnapshotFile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&snapshotFile, "snapshot", "", "path where to write the snapshot file")
}
