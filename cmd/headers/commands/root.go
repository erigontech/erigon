package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var (
	sentryAddr string // Address of the sentry <host>:<port>
	chaindata  string // Path to chaindata
	datadir    string // Path to td working dir
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))
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

var rootCmd = &cobra.Command{
	Use:   "headers",
	Short: "headers is Proof Of Concept for new header/block downloading algorithms",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}
		if chaindata == "" {
			chaindata = path.Join(datadir, "tg", "chaindata")
		}
		//if snapshotDir == "" {
		//	snapshotDir = path.Join(datadir, "tg", "snapshot")
		//}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
}

func Execute() {
	if err := rootCmd.ExecuteContext(rootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
