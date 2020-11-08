package cli

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/ledgerwatch/turbo-geth/turbo/torrent"

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
	SnapshotDir       string
	SnapshotMode      string
	HttpListenAddress string
	TLSCertfile       string
	TLSCACert         string
	TLSKeyFile        string
	HttpPort          int
	HttpCORSDomain    []string
	HttpVirtualHost   []string
	API               []string
	Gascap            uint64
	MaxTraces         uint64
	TraceType         string
	WebsocketEnabled  bool
	RpcAccessFile     string
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
	rootCmd.PersistentFlags().StringVar(&cfg.SnapshotDir, "snapshotDir", "", "path to snapshot dir(only for chaindata mode)")
	rootCmd.PersistentFlags().StringVar(&cfg.SnapshotMode, "snapshot-mode", "", `Configures the storage mode of the app(only for chaindata mode):
* h - use headers snapshot
* b - use bodies snapshot
* s - use state snapshot
* r - use receipts snapshot
`)
	rootCmd.PersistentFlags().StringVar(&cfg.HttpListenAddress, "http.addr", node.DefaultHTTPHost, "HTTP-RPC server listening interface")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSCertfile, "tls.cert", "", "certificate for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSKeyFile, "tls.key", "", "key file for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSCACert, "tls.cacert", "", "CA certificate for client side TLS handshake")
	rootCmd.PersistentFlags().IntVar(&cfg.HttpPort, "http.port", node.DefaultHTTPPort, "HTTP-RPC server listening port")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpCORSDomain, "http.corsdomain", []string{}, "Comma separated list of domains from which to accept cross origin requests (browser enforced)")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpVirtualHost, "http.vhosts", node.DefaultConfig.HTTPVirtualHosts, "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.API, "http.api", []string{"eth", "tg"}, "API's offered over the HTTP-RPC interface")
	rootCmd.PersistentFlags().Uint64Var(&cfg.Gascap, "rpc.gascap", 0, "Sets a cap on gas that can be used in eth_call/estimateGas")
	rootCmd.PersistentFlags().Uint64Var(&cfg.MaxTraces, "trace.maxtraces", 200, "Sets a limit on traces that can be returned in trace_filter")
	rootCmd.PersistentFlags().StringVar(&cfg.TraceType, "trace.type", "parity", "Specify the type of tracing [geth|parity*] (experimental)")
	rootCmd.PersistentFlags().BoolVar(&cfg.WebsocketEnabled, "ws", false, "Enable Websockets")
	rootCmd.PersistentFlags().StringVar(&cfg.RpcAccessFile, "rpc.access", "", "Specify granular (method-by-method) API allowlist")

	if err := rootCmd.MarkFlagFilename("rpc.access", "json"); err != nil {
		panic(err)
	}

	return rootCmd, cfg
}

func OpenDB(cfg Flags) (ethdb.KV, ethdb.Backend, error) {
	var db ethdb.KV
	var txPool ethdb.Backend
	var err error
	// Do not change the order of these checks. Chaindata needs to be checked first, because PrivateApiAddr has default value which is not ""
	// If PrivateApiAddr is checked first, the Chaindata option will never work
	if cfg.Chaindata != "" {
		if database, errOpen := ethdb.Open(cfg.Chaindata, true); errOpen == nil {
			db = database.KV()
		} else {
			err = errOpen
		}
		if cfg.SnapshotMode != "" {
			mode, innerErr := torrent.SnapshotModeFromString(cfg.SnapshotMode)
			if innerErr != nil {
				return nil, nil, fmt.Errorf("can't process snapshot-mode err:%w", innerErr)
			}
			kv, innerErr := torrent.WrapBySnapshots(db, cfg.SnapshotDir, mode)
			if innerErr != nil {
				return nil, nil, fmt.Errorf("can't wrap by snapshots err:%w", innerErr)
			}
			db = kv
		}
	} else if cfg.PrivateApiAddr != "" {
		db, txPool, err = ethdb.NewRemote().Path(cfg.PrivateApiAddr).Open(cfg.TLSCertfile, cfg.TLSKeyFile, cfg.TLSCACert)
		if err != nil {
			return nil, nil, fmt.Errorf("could not connect to remoteDb: %w", err)
		}
	} else {
		return nil, nil, fmt.Errorf("either remote db or lmdb must be specified")
	}

	if err != nil {
		return nil, nil, fmt.Errorf("could not connect to remoteDb: %w", err)
	}

	return db, txPool, err
}

func StartRpcServer(ctx context.Context, cfg Flags, rpcAPI []rpc.API) error {
	// register apis and create handler stack
	httpEndpoint := fmt.Sprintf("%s:%d", cfg.HttpListenAddress, cfg.HttpPort)

	srv := rpc.NewServer()
	srv.SetAllowList(nil)
	if err := node.RegisterApisFromWhitelist(rpcAPI, cfg.API, srv, false); err != nil {
		return fmt.Errorf("could not start register RPC apis: %w", err)
	}

	var err error

	httpHandler := node.NewHTTPHandlerStack(srv, cfg.HttpCORSDomain, cfg.HttpVirtualHost)
	var wsHandler http.Handler
	if cfg.WebsocketEnabled {
		wsHandler = srv.WebsocketHandler([]string{"*"})
	}

	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.WebsocketEnabled && r.Method == "GET" {
			wsHandler.ServeHTTP(w, r)
		}
		httpHandler.ServeHTTP(w, r)
	})

	listener, _, err := node.StartHTTPEndpoint(httpEndpoint, rpc.DefaultHTTPTimeouts, handler)
	if err != nil {
		return fmt.Errorf("could not start RPC api: %w", err)
	}

	if cfg.TraceType != "parity" {
		log.Info("Tracing output type: ", cfg.TraceType)
	}
	log.Info("HTTP endpoint opened", "url", httpEndpoint, "ws", cfg.WebsocketEnabled)

	defer func() {
		srv.Stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = listener.Shutdown(shutdownCtx)
		log.Info("HTTP endpoint closed", "url", httpEndpoint)
	}()
	<-ctx.Done()
	log.Info("Exiting...")
	return nil
}
