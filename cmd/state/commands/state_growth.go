package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

func init() {
	stateGrowthCmd := &cobra.Command{
		Use:   "stateGrowth",
		Short: "stateGrowth",
		RunE: func(cmd *cobra.Command, args []string) error {
			reporter, err := stateless.NewReporter(remoteDbAdddress)
			if err != nil {
				return err
			}

			ctx, _ := getContext()

			fmt.Println("Processing started...")
			reporter.StateGrowth1(ctx, chaindata)
			reporter.StateGrowth2(ctx, chaindata)
			return nil
		},
	}

	withChaindata(stateGrowthCmd)

	withRemoteDb(stateGrowthCmd)

	rootCmd.AddCommand(stateGrowthCmd)
}

func getContext() (context.Context, func()) {
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
	return ctx, cancel
}
