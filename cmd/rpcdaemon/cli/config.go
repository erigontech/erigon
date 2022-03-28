package cli

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/remotedb"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/filters"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/health"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/interfaces"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/services"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/params"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
)

var rootCmd = &cobra.Command{
	Use:   "rpcdaemon",
	Short: "rpcdaemon is JSON RPC server that connects to Erigon node for remote DB access",
}

const JwtDefaultFile = "jwt.hex"

func RootCommand() (*cobra.Command, *httpcfg.HttpCfg) {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))

	cfg := &httpcfg.HttpCfg{StateCache: kvcache.DefaultCoherentConfig}
	rootCmd.PersistentFlags().StringVar(&cfg.PrivateApiAddr, "private.api.addr", "127.0.0.1:9090", "private api network address, for example: 127.0.0.1:9090")
	rootCmd.PersistentFlags().StringVar(&cfg.DataDir, "datadir", "", "path to Erigon working directory")
	rootCmd.PersistentFlags().StringVar(&cfg.Chaindata, "chaindata", "", "path to the database")
	rootCmd.PersistentFlags().StringVar(&cfg.HttpListenAddress, "http.addr", node.DefaultHTTPHost, "HTTP-RPC server listening interface")
	rootCmd.PersistentFlags().StringVar(&cfg.EngineHTTPListenAddress, "engine.addr", node.DefaultHTTPHost, "HTTP-RPC server listening interface for engineAPI")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSCertfile, "tls.cert", "", "certificate for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSKeyFile, "tls.key", "", "key file for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSCACert, "tls.cacert", "", "CA certificate for client side TLS handshake")
	rootCmd.PersistentFlags().IntVar(&cfg.HttpPort, "http.port", node.DefaultHTTPPort, "HTTP-RPC server listening port")
	rootCmd.PersistentFlags().IntVar(&cfg.EnginePort, "engine.port", node.DefaultEngineHTTPPort, "HTTP-RPC server listening port for the engineAPI")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpCORSDomain, "http.corsdomain", []string{}, "Comma separated list of domains from which to accept cross origin requests (browser enforced)")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpVirtualHost, "http.vhosts", node.DefaultConfig.HTTPVirtualHosts, "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.")
	rootCmd.PersistentFlags().BoolVar(&cfg.HttpCompression, "http.compression", true, "Disable http compression")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.API, "http.api", []string{"eth", "erigon"}, "API's offered over the HTTP-RPC interface: eth,engine,erigon,web3,net,debug,trace,txpool,db,starknet. Supported methods: https://github.com/ledgerwatch/erigon/tree/devel/cmd/rpcdaemon")
	rootCmd.PersistentFlags().Uint64Var(&cfg.Gascap, "rpc.gascap", 50000000, "Sets a cap on gas that can be used in eth_call/estimateGas")
	rootCmd.PersistentFlags().Uint64Var(&cfg.MaxTraces, "trace.maxtraces", 200, "Sets a limit on traces that can be returned in trace_filter")
	rootCmd.PersistentFlags().BoolVar(&cfg.WebsocketEnabled, "ws", false, "Enable Websockets")
	rootCmd.PersistentFlags().BoolVar(&cfg.WebsocketCompression, "ws.compression", false, "Enable Websocket compression (RFC 7692)")
	rootCmd.PersistentFlags().StringVar(&cfg.RpcAllowListFilePath, "rpc.accessList", "", "Specify granular (method-by-method) API allowlist")
	rootCmd.PersistentFlags().UintVar(&cfg.RpcBatchConcurrency, "rpc.batch.concurrency", 2, "Does limit amount of goroutines to process 1 batch request. Means 1 bach request can't overload server. 1 batch still can have unlimited amount of request")
	rootCmd.PersistentFlags().IntVar(&cfg.DBReadConcurrency, "db.read.concurrency", runtime.NumCPU(), "Does limit amount of parallel db reads")
	rootCmd.PersistentFlags().BoolVar(&cfg.TraceCompatibility, "trace.compat", false, "Bug for bug compatibility with OE for trace_ routines")
	rootCmd.PersistentFlags().StringVar(&cfg.TxPoolApiAddr, "txpool.api.addr", "", "txpool api network address, for example: 127.0.0.1:9090 (default: use value of --private.api.addr)")
	rootCmd.PersistentFlags().BoolVar(&cfg.TevmEnabled, utils.TevmFlag.Name, false, utils.TevmFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&cfg.Snapshot.Enabled, ethconfig.FlagSnapshot, false, "Enables Snapshot Sync")
	rootCmd.PersistentFlags().IntVar(&cfg.StateCache.KeysLimit, "state.cache", kvcache.DefaultCoherentConfig.KeysLimit, "Amount of keys to store in StateCache (enabled if no --datadir set). Set 0 to disable StateCache. 1_000_000 keys ~ equal to 2Gb RAM (maybe we will add RAM accounting in future versions).")
	rootCmd.PersistentFlags().BoolVar(&cfg.GRPCServerEnabled, "grpc", false, "Enable GRPC server")
	rootCmd.PersistentFlags().StringVar(&cfg.GRPCListenAddress, "grpc.addr", node.DefaultGRPCHost, "GRPC server listening interface")
	rootCmd.PersistentFlags().IntVar(&cfg.GRPCPort, "grpc.port", node.DefaultGRPCPort, "GRPC server listening port")
	rootCmd.PersistentFlags().BoolVar(&cfg.GRPCHealthCheckEnabled, "grpc.healthcheck", false, "Enable GRPC health check")
	rootCmd.PersistentFlags().StringVar(&cfg.StarknetGRPCAddress, "starknet.grpc.address", "127.0.0.1:6066", "Starknet GRPC address")
	rootCmd.PersistentFlags().StringVar(&cfg.JWTSecretPath, utils.JWTSecretPath.Name, utils.JWTSecretPath.Value, "Token to ensure safe connection between CL and EL")

	if err := rootCmd.MarkPersistentFlagFilename("rpc.accessList", "json"); err != nil {
		panic(err)
	}
	if err := rootCmd.MarkPersistentFlagDirname("datadir"); err != nil {
		panic(err)
	}
	if err := rootCmd.MarkPersistentFlagDirname("chaindata"); err != nil {
		panic(err)
	}

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		if err := utils.SetupCobra(cmd); err != nil {
			return err
		}
		cfg.SingleNodeMode = cfg.DataDir != "" || cfg.Chaindata != ""
		if cfg.SingleNodeMode {
			if cfg.DataDir == "" {
				cfg.DataDir = paths.DefaultDataDir()
			}
			if cfg.Chaindata == "" {
				cfg.Chaindata = filepath.Join(cfg.DataDir, "chaindata")
			}
			cfg.Snapshot = ethconfig.NewSnapshotCfg(cfg.Snapshot.Enabled, cfg.Snapshot.KeepBlocks)
		}
		if cfg.TxPoolApiAddr == "" {
			cfg.TxPoolApiAddr = cfg.PrivateApiAddr
		}
		return nil
	}
	rootCmd.PersistentPostRunE = func(cmd *cobra.Command, args []string) error {
		utils.StopDebug()
		return nil
	}

	cfg.StateCache.MetricsLabel = "rpc"

	return rootCmd, cfg
}

type StateChangesClient interface {
	StateChanges(ctx context.Context, in *remote.StateChangeRequest, opts ...grpc.CallOption) (remote.KV_StateChangesClient, error)
}

func subscribeToStateChangesLoop(ctx context.Context, client StateChangesClient, cache kvcache.Cache) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
			}
			if err := subscribeToStateChanges(ctx, client, cache); err != nil {
				if grpcutil.IsRetryLater(err) || grpcutil.IsEndOfStream(err) {
					time.Sleep(3 * time.Second)
					continue
				}
				log.Warn("[txpool.handleStateChanges]", "err", err)
			}
		}
	}()
}

func subscribeToStateChanges(ctx context.Context, client StateChangesClient, cache kvcache.Cache) error {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	stream, err := client.StateChanges(streamCtx, &remote.StateChangeRequest{WithStorage: true, WithTransactions: false}, grpc.WaitForReady(true))
	if err != nil {
		return err
	}
	for req, err := stream.Recv(); ; req, err = stream.Recv() {
		if err != nil {
			return err
		}
		if req == nil {
			return nil
		}

		cache.OnNewBlock(req)
	}
}

func checkDbCompatibility(ctx context.Context, db kv.RoDB) error {
	// DB schema version compatibility check
	var version []byte
	var compatErr error
	var compatTx kv.Tx
	if compatTx, compatErr = db.BeginRo(ctx); compatErr != nil {
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

func EmbeddedServices(ctx context.Context, erigonDB kv.RoDB, stateCacheCfg kvcache.CoherentConfig, blockReader interfaces.BlockAndTxnReader, ethBackendServer remote.ETHBACKENDServer,
	txPoolServer txpool.TxpoolServer, miningServer txpool.MiningServer,
) (
	eth services.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, starknet *services.StarknetService, stateCache kvcache.Cache, ff *filters.Filters, err error,
) {
	if stateCacheCfg.KeysLimit > 0 {
		stateCache = kvcache.New(stateCacheCfg)
	} else {
		stateCache = kvcache.NewDummy()
	}
	kvRPC := remotedbserver.NewKvServer(ctx, erigonDB)
	stateDiffClient := direct.NewStateDiffClientDirect(kvRPC)
	_ = stateDiffClient
	subscribeToStateChangesLoop(ctx, stateDiffClient, stateCache)

	directClient := direct.NewEthBackendClientDirect(ethBackendServer)

	eth = services.NewRemoteBackend(directClient, erigonDB, blockReader)
	txPool = direct.NewTxPoolClient(txPoolServer)
	mining = direct.NewMiningClient(miningServer)
	ff = filters.New(ctx, eth, txPool, mining, func() {})
	return
}

// RemoteServices - use when RPCDaemon run as independent process. Still it can use --datadir flag to enable
// `cfg.SingleNodeMode` (mode when it on 1 machine with Erigon)
func RemoteServices(ctx context.Context, cfg httpcfg.HttpCfg, logger log.Logger, rootCancel context.CancelFunc) (
	db kv.RoDB, borDb kv.RoDB,
	eth services.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient,
	starknet *services.StarknetService,
	stateCache kvcache.Cache, blockReader interfaces.BlockAndTxnReader,
	ff *filters.Filters, err error) {
	if !cfg.SingleNodeMode && cfg.PrivateApiAddr == "" {
		return nil, nil, nil, nil, nil, nil, nil, nil, ff, fmt.Errorf("either remote db or local db must be specified")
	}

	// Do not change the order of these checks. Chaindata needs to be checked first, because PrivateApiAddr has default value which is not ""
	// If PrivateApiAddr is checked first, the Chaindata option will never work
	if cfg.SingleNodeMode {
		var rwKv kv.RwDB
		log.Trace("Creating chain db", "path", cfg.Chaindata)
		limiter := make(chan struct{}, cfg.DBReadConcurrency)
		rwKv, err = kv2.NewMDBX(logger).RoTxsLimiter(limiter).Path(cfg.Chaindata).Readonly().Open()
		if err != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, ff, err
		}
		if compatErr := checkDbCompatibility(ctx, rwKv); compatErr != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, ff, compatErr
		}
		db = rwKv
		stateCache = kvcache.NewDummy()
		blockReader = snapshotsync.NewBlockReader()

		// bor (consensus) specific db
		var borKv kv.RoDB
		borDbPath := filepath.Join(cfg.DataDir, "bor")
		{
			// ensure db exist
			tmpDb, err := kv2.NewMDBX(logger).Path(borDbPath).Label(kv.ConsensusDB).Open()
			if err != nil {
				return nil, nil, nil, nil, nil, nil, nil, nil, ff, err
			}
			tmpDb.Close()
		}
		log.Trace("Creating consensus db", "path", borDbPath)
		borKv, err = kv2.NewMDBX(logger).Path(borDbPath).Label(kv.ConsensusDB).Readonly().Open()
		if err != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, ff, err
		}
		// Skip the compatibility check, until we have a schema in erigon-lib
		borDb = borKv
	} else {
		if cfg.StateCache.KeysLimit > 0 {
			stateCache = kvcache.New(cfg.StateCache)
		} else {
			stateCache = kvcache.NewDummy()
		}
		log.Info("if you run RPCDaemon on same machine with Erigon add --datadir option")
	}
	if db != nil {
		var cc *params.ChainConfig
		if err := db.View(context.Background(), func(tx kv.Tx) error {
			genesisBlock, err := rawdb.ReadBlockByNumber(tx, 0)
			if err != nil {
				return err
			}
			cc, err = rawdb.ReadChainConfig(tx, genesisBlock.Hash())
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, ff, err
		}
		if cc == nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, ff, fmt.Errorf("chain config not found in db. Need start erigon at least once on this db")
		}

		// if chain config has terminal total difficulty then rpc has to have these API's to function
		if cc.TerminalTotalDifficulty != nil {
			hasEthApiEnabled := false
			hasEngineApiEnabled := false
			hasNetApiEnabled := false

			for _, api := range cfg.API {
				switch api {
				case "eth":
					hasEthApiEnabled = true
				case "engine":
					hasEngineApiEnabled = true
				case "net":
					hasNetApiEnabled = true
				}
			}

			if !hasEthApiEnabled {
				cfg.API = append(cfg.API, "eth")
			}

			if !hasEngineApiEnabled {
				cfg.API = append(cfg.API, "engine")
			}

			if !hasNetApiEnabled {
				cfg.API = append(cfg.API, "net")
			}

		}
	}

	var onNewSnapshot func()
	if cfg.SingleNodeMode {
		if cfg.Snapshot.Enabled {
			allSnapshots := snapshotsync.NewRoSnapshots(cfg.Snapshot, filepath.Join(cfg.DataDir, "snapshots"))
			allSnapshots.AsyncOpenAll(ctx)
			onNewSnapshot = func() {
				allSnapshots.Reopen()
			}
			blockReader = snapshotsync.NewBlockReaderWithSnapshots(allSnapshots)
		} else {
			blockReader = snapshotsync.NewBlockReader()
			onNewSnapshot = func() {}
		}
	}

	creds, err := grpcutil.TLS(cfg.TLSCACert, cfg.TLSCertfile, cfg.TLSKeyFile)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, nil, ff, fmt.Errorf("open tls cert: %w", err)
	}
	conn, err := grpcutil.Connect(creds, cfg.PrivateApiAddr)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, nil, ff, fmt.Errorf("could not connect to execution service privateApi: %w", err)
	}

	kvClient := remote.NewKVClient(conn)
	remoteKv, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), logger, kvClient).Open()
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, nil, ff, fmt.Errorf("could not connect to remoteKv: %w", err)
	}

	subscribeToStateChangesLoop(ctx, kvClient, stateCache)

	if !cfg.SingleNodeMode {
		blockReader = snapshotsync.NewRemoteBlockReader(remote.NewETHBACKENDClient(conn))
	}
	remoteEth := services.NewRemoteBackend(remote.NewETHBACKENDClient(conn), db, blockReader)
	blockReader = remoteEth

	txpoolConn := conn
	if cfg.TxPoolApiAddr != cfg.PrivateApiAddr {
		txpoolConn, err = grpcutil.Connect(creds, cfg.TxPoolApiAddr)
		if err != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, ff, fmt.Errorf("could not connect to txpool api: %w", err)
		}
	}

	mining = txpool.NewMiningClient(txpoolConn)
	miningService := services.NewMiningService(mining)
	txPool = txpool.NewTxpoolClient(txpoolConn)
	txPoolService := services.NewTxPoolService(txPool)
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
		if mining != nil && !miningService.EnsureVersionCompatibility() {
			rootCancel()
		}
		if !txPoolService.EnsureVersionCompatibility() {
			rootCancel()
		}
	}()

	if cfg.StarknetGRPCAddress != "" {
		starknetConn, err := grpcutil.Connect(creds, cfg.StarknetGRPCAddress)
		if err != nil {
			return nil, nil, nil, nil, nil, nil, nil, nil, ff, fmt.Errorf("could not connect to starknet api: %w", err)
		}
		starknet = services.NewStarknetService(starknetConn)
	}

	ff = filters.New(ctx, eth, txPool, mining, onNewSnapshot)

	return db, borDb, eth, txPool, mining, starknet, stateCache, blockReader, ff, err
}

func StartRpcServer(ctx context.Context, cfg httpcfg.HttpCfg, rpcAPI []rpc.API) error {
	var engineListener *http.Server
	var engineListenerAuth *http.Server
	var engineSrv *rpc.Server
	var engineHttpEndpoint string

	// register apis and create handler stack
	httpEndpoint := fmt.Sprintf("%s:%d", cfg.HttpListenAddress, cfg.HttpPort)

	srv := rpc.NewServer(cfg.RpcBatchConcurrency)

	allowListForRPC, err := parseAllowListForRPC(cfg.RpcAllowListFilePath)
	if err != nil {
		return err
	}
	srv.SetAllowList(allowListForRPC)

	var defaultAPIList []rpc.API
	var engineAPI []rpc.API

	for _, api := range rpcAPI {
		if api.Namespace != "engine" {
			defaultAPIList = append(defaultAPIList, api)
		} else {
			engineAPI = append(engineAPI, api)
		}
	}

	if len(engineAPI) != 0 {
		// eth API should also be exposed on the same port as engine API
		for _, api := range rpcAPI {
			if api.Namespace == "eth" {
				engineAPI = append(engineAPI, api)
			}
		}
	}

	var apiFlags []string
	for _, flag := range cfg.API {
		if flag != "engine" {
			apiFlags = append(apiFlags, flag)
		}
	}

	if err := node.RegisterApisFromWhitelist(defaultAPIList, apiFlags, srv, false); err != nil {
		return fmt.Errorf("could not start register RPC apis: %w", err)
	}

	httpHandler := node.NewHTTPHandlerStack(srv, cfg.HttpCORSDomain, cfg.HttpVirtualHost, cfg.HttpCompression)
	var wsHandler http.Handler
	if cfg.WebsocketEnabled {
		wsHandler = srv.WebsocketHandler([]string{"*"}, nil, cfg.WebsocketCompression)
	}

	apiHandler, err := createHandler(cfg, defaultAPIList, httpHandler, wsHandler, nil)
	if err != nil {
		return err
	}

	listener, _, err := node.StartHTTPEndpoint(httpEndpoint, rpc.DefaultHTTPTimeouts, apiHandler)
	if err != nil {
		return fmt.Errorf("could not start RPC api: %w", err)
	}
	info := []interface{}{"url", httpEndpoint, "ws", cfg.WebsocketEnabled,
		"ws.compression", cfg.WebsocketCompression, "grpc", cfg.GRPCServerEnabled}

	if len(engineAPI) > 0 {
		engineListener, engineListenerAuth, engineSrv, engineHttpEndpoint, err = createEngineListener(cfg, engineAPI)
		if err != nil {
			return fmt.Errorf("could not start RPC api for engine: %w", err)
		}
	}

	var (
		healthServer *grpcHealth.Server
		grpcServer   *grpc.Server
		grpcListener net.Listener
		grpcEndpoint string
	)
	if cfg.GRPCServerEnabled {
		grpcEndpoint = fmt.Sprintf("%s:%d", cfg.GRPCListenAddress, cfg.GRPCPort)
		if grpcListener, err = net.Listen("tcp", grpcEndpoint); err != nil {
			return fmt.Errorf("could not start GRPC listener: %w", err)
		}
		grpcServer = grpc.NewServer()
		if cfg.GRPCHealthCheckEnabled {
			healthServer = grpcHealth.NewServer()
			grpc_health_v1.RegisterHealthServer(grpcServer, healthServer)
		}
		go grpcServer.Serve(grpcListener)
		info = append(info, "grpc.port", cfg.GRPCPort)
	}

	log.Info("HTTP endpoint opened", info...)

	defer func() {
		srv.Stop()
		if engineSrv != nil {
			engineSrv.Stop()
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = listener.Shutdown(shutdownCtx)
		log.Info("HTTP endpoint closed", "url", httpEndpoint)

		if engineListener != nil {
			_ = engineListener.Shutdown(shutdownCtx)
			log.Info("Engine HTTP endpoint close", "url", engineHttpEndpoint)
		}

		if engineListenerAuth != nil {
			_ = engineListenerAuth.Shutdown(shutdownCtx)
			log.Info("Engine HTTP endpoint close", "url", engineHttpEndpoint)
		}

		if cfg.GRPCServerEnabled {
			if cfg.GRPCHealthCheckEnabled {
				healthServer.Shutdown()
			}
			grpcServer.GracefulStop()
			_ = grpcListener.Close()
			log.Info("GRPC endpoint closed", "url", grpcEndpoint)
		}
	}()
	<-ctx.Done()
	log.Info("Exiting...")
	return nil
}

// isWebsocket checks the header of a http request for a websocket upgrade request.
func isWebsocket(r *http.Request) bool {
	return strings.ToLower(r.Header.Get("Upgrade")) == "websocket" &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// obtainJWTSecret loads the jwt-secret, either from the provided config,
// or from the default location. If neither of those are present, it generates
// a new secret and stores to the default location.
func obtainJWTSecret(cfg httpcfg.HttpCfg) ([]byte, error) {
	// try reading from file
	log.Info("Reading JWT secret", "path", cfg.JWTSecretPath)
	if data, err := os.ReadFile(cfg.JWTSecretPath); err == nil {
		jwtSecret := common.FromHex(strings.TrimSpace(string(data)))
		if len(jwtSecret) == 32 {
			return jwtSecret, nil
		}
		log.Error("Invalid JWT secret", "path", cfg.JWTSecretPath, "length", len(jwtSecret))
		return nil, errors.New("invalid JWT secret")
	}
	// Need to generate one
	jwtSecret := make([]byte, 32)
	rand.Read(jwtSecret)

	if err := os.WriteFile(cfg.JWTSecretPath, []byte(hexutil.Encode(jwtSecret)), 0600); err != nil {
		return nil, err
	}
	log.Info("Generated JWT secret", "path", cfg.JWTSecretPath)
	return jwtSecret, nil
}

func createHandler(cfg httpcfg.HttpCfg, apiList []rpc.API, httpHandler http.Handler, wsHandler http.Handler, jwtSecret []byte) (http.Handler, error) {
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// adding a healthcheck here
		if health.ProcessHealthcheckIfNeeded(w, r, apiList) {
			return
		}
		if cfg.WebsocketEnabled && wsHandler != nil && isWebsocket(r) {
			wsHandler.ServeHTTP(w, r)
			return
		}

		if jwtSecret != nil && !rpc.CheckJwtSecret(w, r, jwtSecret) {
			return
		}

		httpHandler.ServeHTTP(w, r)
	})

	return handler, nil
}

func createEngineListener(cfg httpcfg.HttpCfg, engineApi []rpc.API) (*http.Server, *http.Server, *rpc.Server, string, error) {
	engineHttpEndpoint := fmt.Sprintf("%s:%d", cfg.EngineHTTPListenAddress, cfg.EnginePort)
	engineHttpEndpointAuth := fmt.Sprintf("%s:%d", cfg.EngineHTTPListenAddress, cfg.EnginePort+1)

	engineSrv := rpc.NewServer(cfg.RpcBatchConcurrency)

	allowListForRPC, err := parseAllowListForRPC(cfg.RpcAllowListFilePath)
	if err != nil {
		return nil, nil, nil, "", err
	}
	engineSrv.SetAllowList(allowListForRPC)

	if err := node.RegisterApisFromWhitelist(engineApi, nil, engineSrv, true); err != nil {
		return nil, nil, nil, "", fmt.Errorf("could not start register RPC engine api: %w", err)
	}

	jwtSecret, err := obtainJWTSecret(cfg)
	if err != nil {
		return nil, nil, nil, "", err
	}

	var wsHandlerNonAuth http.Handler
	var wsHandlerAuth http.Handler

	if cfg.WebsocketEnabled {
		wsHandlerNonAuth = engineSrv.WebsocketHandler([]string{"*"}, nil, cfg.WebsocketCompression)
		wsHandlerAuth = engineSrv.WebsocketHandler([]string{"*"}, jwtSecret, cfg.WebsocketCompression)
	}

	engineHttpHandler := node.NewHTTPHandlerStack(engineSrv, cfg.HttpCORSDomain, cfg.HttpVirtualHost, cfg.HttpCompression)
	engineApiHandler, err := createHandler(cfg, engineApi, engineHttpHandler, wsHandlerNonAuth, nil)
	if err != nil {
		return nil, nil, nil, "", err
	}

	engineApiHandlerAuth, err := createHandler(cfg, engineApi, engineHttpHandler, wsHandlerAuth, jwtSecret)
	if err != nil {
		return nil, nil, nil, "", err
	}

	engineListener, _, err := node.StartHTTPEndpoint(engineHttpEndpoint, rpc.DefaultHTTPTimeouts, engineApiHandler)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("could not start RPC api: %w", err)
	}

	engineListenerAuth, _, err := node.StartHTTPEndpoint(engineHttpEndpointAuth, rpc.DefaultHTTPTimeouts, engineApiHandlerAuth)
	if err != nil {
		return nil, nil, nil, "", fmt.Errorf("could not start RPC api: %w", err)
	}

	engineInfo := []interface{}{"url", engineHttpEndpoint, "ws", cfg.WebsocketEnabled}
	log.Info("HTTP endpoint opened for engine", engineInfo...)
	engineInfoAuth := []interface{}{"url", engineHttpEndpointAuth, "ws", cfg.WebsocketEnabled}
	log.Info("HTTP endpoint opened for auth engine", engineInfoAuth...)

	return engineListener, engineListenerAuth, engineSrv, engineHttpEndpoint, nil
}
