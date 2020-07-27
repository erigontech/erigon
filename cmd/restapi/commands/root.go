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
	rpcAddr   string
	chaindata string
	addr      string
)

func init() {
	rootCmd.Flags().StringVar(&rpcAddr, "private.rpc.addr", "127.0.0.1:9090", "binary RPC network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface")
	rootCmd.Flags().StringVar(&addr, "http.addr", "127.0.0.1:8080", "REST server listening host")
	rootCmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the database")
}

var rootCmd = &cobra.Command{
	Use:   "restapi",
	Short: "restapi exposes read-only blockchain APIs through REST (requires running turbo-geth node)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return rest.ServeREST(cmd.Context(), addr, rpcAddr, chaindata)
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
