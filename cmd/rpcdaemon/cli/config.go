package cli

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/utils"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/internal/debug"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/spf13/cobra"
)

type Flags struct {
	PrivateApiAddr    string
	Chaindata         string
	HttpListenAddress string
	HttpPort          int
	HttpCORSDomain    []string
	HttpVirtualHost   []string
	API               []string
	Gascap            uint64
}

var rootCmd = &cobra.Command{
	Use:   "rpcdaemon",
	Short: "rpcdaemon is JSON RPC server that connects to turbo-geth node for remote DB access",

	PersistentPreRunE: func(cmd *cobra.Command, args []string) error {
		if err := utils.SetupCobra(cmd); err != nil {
			return err
		}
		return nil
	},
	PersistentPostRunE: func(cmd *cobra.Command, args []string) error {
		utils.StopDebug()
		return nil
	},
}

func RootCommand() (*cobra.Command, *Flags) {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))

	cfg := &Flags{}
	rootCmd.PersistentFlags().StringVar(&cfg.PrivateApiAddr, "private.api.addr", "127.0.0.1:9090", "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface")
	rootCmd.PersistentFlags().StringVar(&cfg.Chaindata, "chaindata", "", "path to the database")
	rootCmd.PersistentFlags().StringVar(&cfg.HttpListenAddress, "http.addr", node.DefaultHTTPHost, "HTTP-RPC server listening interface")
	rootCmd.PersistentFlags().IntVar(&cfg.HttpPort, "http.port", node.DefaultHTTPPort, "HTTP-RPC server listening port")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpCORSDomain, "http.corsdomain", []string{}, "Comma separated list of domains from which to accept cross origin requests (browser enforced)")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpVirtualHost, "http.vhosts", node.DefaultConfig.HTTPVirtualHosts, "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.API, "http.api", []string{"eth"}, "API's offered over the HTTP-RPC interface")
	rootCmd.PersistentFlags().Uint64Var(&cfg.Gascap, "rpc.gascap", 0, "Sets a cap on gas that can be used in eth_call/estimateGas")

	return rootCmd, cfg
}

func OpenDB(cfg Flags) (ethdb.KV, ethdb.Backend, error) {
	var db ethdb.KV
	var txPool ethdb.Backend
	var err error
	if cfg.PrivateApiAddr != "" {
		db, txPool, err = ethdb.NewRemote().Path(cfg.PrivateApiAddr).Open()
		if err != nil {
			return nil, nil, fmt.Errorf("could not connect to remoteDb: %w", err)
		}
	} else if cfg.Chaindata != "" {
		if database, errOpen := ethdb.Open(cfg.Chaindata); errOpen == nil {
			db = database.KV()
		} else {
			err = errOpen
		}
	} else {
		return nil, nil, fmt.Errorf("either remote db or bolt db must be specified")
	}

	if err != nil {
		return nil, nil, fmt.Errorf("could not connect to remoteDb: %w", err)
	}

	return db, txPool, err
}

func StartRpcServer(ctx context.Context, cfg Flags, rpcAPI []rpc.API) {
	// register apis and create handler stack
	httpEndpoint := fmt.Sprintf("%s:%d", cfg.HttpListenAddress, cfg.HttpPort)
	srv := rpc.NewServer()
	if err := node.RegisterApisFromWhitelist(rpcAPI, cfg.API, srv, false); err != nil {
		log.Error("Could not start register RPC apis", "error", err)
		return
	}
	handler := node.NewHTTPHandlerStack(srv, cfg.HttpCORSDomain, cfg.HttpVirtualHost)

	listener, _, err := node.StartHTTPEndpoint(httpEndpoint, rpc.DefaultHTTPTimeouts, handler)
	if err != nil {
		log.Error("Could not start RPC api", "error", err)
		return
	}
	extapiURL := fmt.Sprintf("http://%s", httpEndpoint)
	log.Info("HTTP endpoint opened", "url", extapiURL)

	defer func() {
		listener.Close()
		log.Info("HTTP endpoint closed", "url", httpEndpoint)
	}()
	sig := <-ctx.Done()
	log.Info("Exiting...", "signal", sig)
}
