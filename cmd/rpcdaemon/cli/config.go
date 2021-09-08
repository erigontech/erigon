package cli

import (
	"context"
	"encoding/binary"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/kv"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/remotedb"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/ethdb/remotedbserver"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
)

type Flags struct {
	PrivateApiAddr       string
	SingleNodeMode       bool // Erigon's database can be read by separated processes on same machine - in read-only mode - with full support of transactions. It will share same "OS PageCache" with Erigon process.
	Datadir              string
	Chaindata            string
	SnapshotDir          string
	SnapshotMode         string
	HttpListenAddress    string
	TLSCertfile          string
	TLSCACert            string
	TLSKeyFile           string
	HttpPort             int
	HttpCORSDomain       []string
	HttpVirtualHost      []string
	HttpCompression      bool
	API                  []string
	Gascap               uint64
	MaxTraces            uint64
	WebsocketEnabled     bool
	WebsocketCompression bool
	RpcAllowListFilePath string
	RpcBatchConcurrency  uint
	TraceCompatibility   bool // Bug for bug compatibility for trace_ routines with OpenEthereum
	TxPoolV2             bool
	TxPoolApiAddr        string
}

var rootCmd = &cobra.Command{
	Use:   "rpcdaemon",
	Short: "rpcdaemon is JSON RPC server that connects to Erigon node for remote DB access",
}

func RootCommand() (*cobra.Command, *Flags) {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))

	cfg := &Flags{}
	rootCmd.PersistentFlags().StringVar(&cfg.PrivateApiAddr, "private.api.addr", "127.0.0.1:9090", "private api network address, for example: 127.0.0.1:9090")
	rootCmd.PersistentFlags().StringVar(&cfg.Datadir, "datadir", "", "path to Erigon working directory")
	rootCmd.PersistentFlags().StringVar(&cfg.Chaindata, "chaindata", "", "path to the database")
	rootCmd.PersistentFlags().StringVar(&cfg.SnapshotDir, "snapshot.dir", "", "path to snapshot dir(only for chaindata mode)")
	rootCmd.PersistentFlags().StringVar(&cfg.SnapshotMode, "snapshot.mode", "", `Configures the storage mode of the app(only for chaindata mode):
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
	rootCmd.PersistentFlags().BoolVar(&cfg.HttpCompression, "http.compression", true, "Disable http compression")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.API, "http.api", []string{"eth", "erigon"}, "API's offered over the HTTP-RPC interface: eth,erigon,web3,net,debug,trace,txpool,shh,db. Supported methods: https://github.com/ledgerwatch/erigon/tree/devel/cmd/rpcdaemon")
	rootCmd.PersistentFlags().Uint64Var(&cfg.Gascap, "rpc.gascap", 50000000, "Sets a cap on gas that can be used in eth_call/estimateGas")
	rootCmd.PersistentFlags().Uint64Var(&cfg.MaxTraces, "trace.maxtraces", 200, "Sets a limit on traces that can be returned in trace_filter")
	rootCmd.PersistentFlags().BoolVar(&cfg.WebsocketEnabled, "ws", false, "Enable Websockets")
	rootCmd.PersistentFlags().BoolVar(&cfg.WebsocketCompression, "ws.compression", false, "Enable Websocket compression (RFC 7692)")
	rootCmd.PersistentFlags().StringVar(&cfg.RpcAllowListFilePath, "rpc.accessList", "", "Specify granular (method-by-method) API allowlist")
	rootCmd.PersistentFlags().UintVar(&cfg.RpcBatchConcurrency, "rpc.batch.concurrency", 50, "Does limit amount of goroutines to process 1 batch request. Means 1 bach request can't overload server. 1 batch still can have unlimited amount of request")
	rootCmd.PersistentFlags().BoolVar(&cfg.TraceCompatibility, "trace.compat", false, "Bug for bug compatibility with OE for trace_ routines")
	rootCmd.PersistentFlags().BoolVar(&cfg.TxPoolV2, "txpool.v2", false, "experimental external txpool")
	rootCmd.PersistentFlags().StringVar(&cfg.TxPoolApiAddr, "txpool.api.addr", "127.0.0.1:9094", "txpool api network address, for example: 127.0.0.1:9094")

	if err := rootCmd.MarkPersistentFlagFilename("rpc.accessList", "json"); err != nil {
		panic(err)
	}
	if err := rootCmd.MarkPersistentFlagDirname("datadir"); err != nil {
		panic(err)
	}
	if err := rootCmd.MarkPersistentFlagDirname("chaindata"); err != nil {
		panic(err)
	}
	if err := rootCmd.MarkPersistentFlagDirname("snapshot.dir"); err != nil {
		panic(err)
	}

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if err := utils.SetupCobra(cmd); err != nil {
			return err
		}
		cfg.SingleNodeMode = cfg.Datadir != "" || cfg.Chaindata != ""
		if cfg.SingleNodeMode {
			if cfg.Datadir == "" {
				cfg.Datadir = paths.DefaultDataDir()
			}
			if cfg.Chaindata == "" {
				cfg.Chaindata = path.Join(cfg.Datadir, "chaindata")
			}
		}
		return nil
	}
	rootCmd.PersistentPostRunE = func(cmd *cobra.Command, args []string) error {
		utils.StopDebug()
		return nil
	}

	return rootCmd, cfg
}

func checkDbCompatibility(db kv.RoDB) error {
	// DB schema version compatibility check
	var version []byte
	var compatErr error
	var compatTx kv.Tx
	if compatTx, compatErr = db.BeginRo(context.Background()); compatErr != nil {
		return fmt.Errorf("open Ro Tx for DB schema compability check: %w", compatErr)
	}
	defer compatTx.Rollback()
	if version, compatErr = compatTx.GetOne(kv.DatabaseInfo, kv.DBSchemaVersionKey); compatErr != nil {
		return fmt.Errorf("read version for DB schema compability check: %w", compatErr)
	}
	if len(version) != 12 {
		return fmt.Errorf("database does not have major schema version. upgrade and restart Erigon core")
	}
	major := binary.BigEndian.Uint32(version)
	minor := binary.BigEndian.Uint32(version[4:])
	patch := binary.BigEndian.Uint32(version[8:])
	var compatible bool
	dbSchemaVersion := &kv.DBSchemaVersion
	if major != dbSchemaVersion.Major {
		compatible = false
	} else if minor != dbSchemaVersion.Minor {
		compatible = false
	} else {
		compatible = true
	}
	if !compatible {
		return fmt.Errorf("incompatible DB Schema versions: reader %d.%d.%d, database %d.%d.%d",
			dbSchemaVersion.Major, dbSchemaVersion.Minor, dbSchemaVersion.Patch,
			major, minor, patch)
	}
	log.Info("DB schemas compatible", "reader", fmt.Sprintf("%d.%d.%d", dbSchemaVersion.Major, dbSchemaVersion.Minor, dbSchemaVersion.Patch),
		"database", fmt.Sprintf("%d.%d.%d", major, minor, patch))
	return nil
}

func RemoteServices(ctx context.Context, cfg Flags, logger log.Logger, rootCancel context.CancelFunc) (db kv.RoDB, eth services.ApiBackend, txPool *services.TxPoolService, mining *services.MiningService, err error) {
	if !cfg.SingleNodeMode && cfg.PrivateApiAddr == "" {
		return nil, nil, nil, nil, fmt.Errorf("either remote db or local db must be specified")
	}
	// Do not change the order of these checks. Chaindata needs to be checked first, because PrivateApiAddr has default value which is not ""
	// If PrivateApiAddr is checked first, the Chaindata option will never work
	if cfg.SingleNodeMode {
		var rwKv kv.RwDB
		rwKv, err = kv2.NewMDBX(logger).Path(cfg.Chaindata).Readonly().Open()
		if err != nil {
			return nil, nil, nil, nil, err
		}
		if compatErr := checkDbCompatibility(rwKv); compatErr != nil {
			return nil, nil, nil, nil, compatErr
		}
		db = rwKv
	} else {
		log.Info("if you run RPCDaemon on same machine with Erigon add --datadir option")
	}
	if cfg.PrivateApiAddr != "" {
		creds, err := grpcutil.TLS(cfg.TLSCACert, cfg.TLSCertfile, cfg.TLSKeyFile)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("open tls cert: %w", err)
		}
		conn, err := grpcutil.Connect(creds, cfg.PrivateApiAddr)
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("could not connect to execution service privateApi: %w", err)
		}

		kvClient := remote.NewKVClient(conn)
		remoteKv, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), logger, kvClient).Open()
		if err != nil {
			return nil, nil, nil, nil, fmt.Errorf("could not connect to remoteKv: %w", err)
		}
		remoteEth := services.NewRemoteBackend(conn)
		txpoolConn := conn
		if cfg.TxPoolV2 {
			txpoolConn, err = grpcutil.Connect(creds, cfg.TxPoolApiAddr)
			if err != nil {
				return nil, nil, nil, nil, fmt.Errorf("could not connect to txpool api: %w", err)
			}
		}
		mining = services.NewMiningService(txpoolConn)
		txPool = services.NewTxPoolService(txpoolConn)
		if db == nil {
			db = remoteKv
		}
		eth = remoteEth
		go func() {
			if !remoteKv.EnsureVersionCompatibility() {
				rootCancel()
			}
			if !remoteEth.EnsureVersionCompatibility() {
				rootCancel()
			}
			if mining != nil && !mining.EnsureVersionCompatibility() {
				rootCancel()
			}
			if !txPool.EnsureVersionCompatibility() {
				rootCancel()
			}
		}()
	}
	return db, eth, txPool, mining, err
}

func StartRpcServer(ctx context.Context, cfg Flags, rpcAPI []rpc.API) error {
	// register apis and create handler stack
	httpEndpoint := fmt.Sprintf("%s:%d", cfg.HttpListenAddress, cfg.HttpPort)

	srv := rpc.NewServer(cfg.RpcBatchConcurrency)

	allowListForRPC, err := parseAllowListForRPC(cfg.RpcAllowListFilePath)
	if err != nil {
		return err
	}
	srv.SetAllowList(allowListForRPC)

	if err := node.RegisterApisFromWhitelist(rpcAPI, cfg.API, srv, false); err != nil {
		return fmt.Errorf("could not start register RPC apis: %w", err)
	}

	httpHandler := node.NewHTTPHandlerStack(srv, cfg.HttpCORSDomain, cfg.HttpVirtualHost, cfg.HttpCompression)
	var wsHandler http.Handler
	if cfg.WebsocketEnabled {
		wsHandler = srv.WebsocketHandler([]string{"*"}, cfg.WebsocketCompression)
	}

	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.WebsocketEnabled && r.Method == "GET" {
			wsHandler.ServeHTTP(w, r)
			return
		}
		httpHandler.ServeHTTP(w, r)
	})

	listener, _, err := node.StartHTTPEndpoint(httpEndpoint, rpc.DefaultHTTPTimeouts, handler)
	if err != nil {
		return fmt.Errorf("could not start RPC api: %w", err)
	}

	log.Info("HTTP endpoint opened", "url", httpEndpoint, "ws", cfg.WebsocketEnabled, "ws.compression", cfg.WebsocketCompression)

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
