package rpc

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/ledgerwatch/turbo-geth/log"
	"github.com/ledgerwatch/turbo-geth/node"
	"github.com/ledgerwatch/turbo-geth/rpc"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"strings"
	"syscall"
)

type Flags struct {
	PrivateApiAddr    string
	Chaindata         string
	HttpListenAddress string
	HttpPort          int
	httpCORSDomain    string
	HttpCORSDomain    []string
	httpVirtualHost   string
	HttpVirtualHost   []string
	api               string
	API               []string
	Gascap            uint64
}

var rootCmd = &cobra.Command{
	Use:   "rpcdaemon",
	Short: "rpcdaemon is JSON RPC server that connects to turbo-geth node for remote DB access",
}

func RootCommand() (*cobra.Command, Flags) {
	var cfg Flags

	rootCmd.Flags().StringVar(&cfg.PrivateApiAddr, "private.api.addr", "", "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface")
	rootCmd.Flags().StringVar(&cfg.Chaindata, "chaindata", "", "path to the database")
	rootCmd.Flags().StringVar(&cfg.HttpListenAddress, "http.addr", node.DefaultHTTPHost, "HTTP-RPC server listening interface")
	rootCmd.Flags().IntVar(&cfg.HttpPort, "http.port", node.DefaultHTTPPort, "HTTP-RPC server listening port")
	rootCmd.Flags().StringVar(&cfg.httpCORSDomain, "http.corsdomain", "", "Comma separated list of domains from which to accept cross origin requests (browser enforced)")
	rootCmd.Flags().StringVar(&cfg.httpVirtualHost, "http.vhosts", strings.Join(node.DefaultConfig.HTTPVirtualHosts, ","), "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.")
	rootCmd.Flags().StringVar(&cfg.api, "http.api", "", "API's offered over the HTTP-RPC interface")
	rootCmd.Flags().Uint64Var(&cfg.Gascap, "rpc.gascap", 0, "Sets a cap on gas that can be used in eth_call/estimateGas")
	rootCmd.Flags().StringVar(&cfg.api, "http.api", "", "API's offered over the HTTP-RPC interface")

	cfg.HttpVirtualHost = splitAndTrim(cfg.httpVirtualHost)
	cfg.HttpCORSDomain = splitAndTrim(cfg.httpCORSDomain)
	cfg.API = splitAndTrim(cfg.api)

	return rootCmd, cfg
}

// splitAndTrim splits input separated by a comma
// and trims excessive white space from the substrings.
func splitAndTrim(input string) []string {
	result := strings.Split(input, ",")
	for i, r := range result {
		result[i] = strings.TrimSpace(r)
	}
	return result
}

func RootContext() context.Context {
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		defer cancel()

		ch := make(chan os.Signal, 1)
		defer close(ch)

		signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(ch)

		select {
		case <-ch:
			log.Info("Got interrupt, shutting down...")
		case <-ctx.Done():
		}
	}()
	return ctx
}

func DefaultConnection(cfg Flags) (ethdb.KV, ethdb.Backend, error) {
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

func StartRpcServer(cfg Flags, rpcAPI []rpc.API) {
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
}
