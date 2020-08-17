package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"

	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
)

type Config struct {
	PrivateApiAddr    string
	Chaindata         string
	HttpListenAddress string
	HttpPort          int
	HttpCORSDomain    string
	HttpVirtualHost   string
	API               string
	Gascap            uint64
}

var (
	cfg Config
)

func init() {
	rootCmd.Flags().StringVar(&cfg.PrivateApiAddr, "private.api.addr", "", "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface")
	rootCmd.Flags().StringVar(&cfg.Chaindata, "chaindata", "", "path to the database")
	rootCmd.Flags().StringVar(&cfg.HttpListenAddress, "http.addr", node.DefaultHTTPHost, "HTTP-RPC server listening interface")
	rootCmd.Flags().IntVar(&cfg.HttpPort, "http.port", node.DefaultHTTPPort, "HTTP-RPC server listening port")
	rootCmd.Flags().StringVar(&cfg.HttpCORSDomain, "http.corsdomain", "", "Comma separated list of domains from which to accept cross origin requests (browser enforced)")
	rootCmd.Flags().StringVar(&cfg.HttpVirtualHost, "http.vhosts", strings.Join(node.DefaultConfig.HTTPVirtualHosts, ","), "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.")
	rootCmd.Flags().StringVar(&cfg.API, "http.api", "", "API's offered over the HTTP-RPC interface")
	rootCmd.Flags().Uint64Var(&cfg.Gascap, "rpc.gascap", 0, "Sets a cap on gas that can be used in eth_call/estimateGas")
}

var rootCmd = &cobra.Command{
	Use:   "rpcdaemon",
	Short: "rpcdaemon is JSON RPC server that connects to turbo-geth node for remote DB access",
	RunE: func(cmd *cobra.Command, args []string) error {
		daemon(cmd, cfg)
		return nil
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
