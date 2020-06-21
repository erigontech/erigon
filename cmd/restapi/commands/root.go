package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/ledgerwatch/turbo-geth/cmd/restapi/rest"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/spf13/cobra"
)

var (
	remoteDbAddress string
	chaindata       string
	listenAddress   string
)

func init() {
	rootCmd.Flags().StringVar(&remoteDbAddress, "remote-db-addr", "", "address of remote DB listener of a turbo-geth node")
	rootCmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the boltdb database")
	rootCmd.Flags().StringVar(&listenAddress, "rpcaddr", "localhost:8080", "REST server listening interface")
}

var rootCmd = &cobra.Command{
	Use:   "restapi",
	Short: "restapi exposes read-only blockchain APIs through REST (requires running turbo-geth node)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return rest.ServeREST(cmd.Context(), listenAddress, remoteDbAddress, chaindata)
	},
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
