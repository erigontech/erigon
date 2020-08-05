package commands

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/migrations"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"syscall"
)

var rootCmd = &cobra.Command{
	Use:   "integration",
	Short: "long and heavy integration tests for turbo-geth",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}

		if len(chaindata) > 0 {
			db := ethdb.MustOpen(chaindata)
			defer db.Close()
			if err := migrations.NewMigrator().Apply(db, ""); err != nil {
				panic(err)
			}
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
}

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricsEnabledFlag, utils.MetricsEnabledExpensiveFlag))
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

func Execute() {
	if err := rootCmd.ExecuteContext(rootContext()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
