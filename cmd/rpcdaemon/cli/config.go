package cli

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/config3"
	"github.com/ledgerwatch/erigon-lib/direct"
	"github.com/ledgerwatch/erigon-lib/gointerfaces"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/grpcutil"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon-lib/gointerfaces/txpool"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	kv2 "github.com/ledgerwatch/erigon-lib/kv/mdbx"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	"github.com/ledgerwatch/erigon-lib/kv/remotedb"
	"github.com/ledgerwatch/erigon-lib/kv/remotedbserver"
	"github.com/ledgerwatch/erigon-lib/kv/temporal"
	libstate "github.com/ledgerwatch/erigon-lib/state"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/graphql"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/health"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/cmd/utils/flags"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/core/state"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/polygon/bor"
	"github.com/ledgerwatch/erigon/polygon/bor/borcfg"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"

	// Force-load native and js packages, to trigger registration
	_ "github.com/ledgerwatch/erigon/eth/tracers/js"
	_ "github.com/ledgerwatch/erigon/eth/tracers/native"
)

var rootCmd = &cobra.Command{
	Use:   "rpcdaemon",
	Short: "rpcdaemon is JSON RPC server that connects to Erigon node for remote DB access",
}

var (
	stateCacheStr string
)

func RootCommand() (*cobra.Command, *httpcfg.HttpCfg) {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)

	cfg := &httpcfg.HttpCfg{Sync: ethconfig.Defaults.Sync, Enabled: true, StateCache: kvcache.DefaultCoherentConfig}
	rootCmd.PersistentFlags().StringVar(&cfg.PrivateApiAddr, "private.api.addr", "127.0.0.1:9090", "Erigon's components (txpool, rpcdaemon, sentry, downloader, ...) can be deployed as independent Processes on same/another server. Then components will connect to erigon by this internal grpc API. Example: 127.0.0.1:9090")
	rootCmd.PersistentFlags().StringVar(&cfg.DataDir, "datadir", "", "path to Erigon working directory")
	rootCmd.PersistentFlags().BoolVar(&cfg.GraphQLEnabled, "graphql", false, "enables graphql endpoint (disabled by default)")
	rootCmd.PersistentFlags().Uint64Var(&cfg.Gascap, "rpc.gascap", 50_000_000, "Sets a cap on gas that can be used in eth_call/estimateGas")
	rootCmd.PersistentFlags().Uint64Var(&cfg.MaxTraces, "trace.maxtraces", 200, "Sets a limit on traces that can be returned in trace_filter")

	rootCmd.PersistentFlags().StringVar(&cfg.RpcAllowListFilePath, utils.RpcAccessListFlag.Name, "", "Specify granular (method-by-method) API allowlist")
	rootCmd.PersistentFlags().UintVar(&cfg.RpcBatchConcurrency, utils.RpcBatchConcurrencyFlag.Name, 2, utils.RpcBatchConcurrencyFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&cfg.RpcStreamingDisable, utils.RpcStreamingDisableFlag.Name, false, utils.RpcStreamingDisableFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&cfg.DebugSingleRequest, utils.HTTPDebugSingleFlag.Name, false, utils.HTTPDebugSingleFlag.Usage)
	rootCmd.PersistentFlags().IntVar(&cfg.DBReadConcurrency, utils.DBReadConcurrencyFlag.Name, utils.DBReadConcurrencyFlag.Value, utils.DBReadConcurrencyFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&cfg.TraceCompatibility, "trace.compat", false, "Bug for bug compatibility with OE for trace_ routines")
	rootCmd.PersistentFlags().StringVar(&cfg.TxPoolApiAddr, "txpool.api.addr", "", "txpool api network address, for example: 127.0.0.1:9090 (default: use value of --private.api.addr)")

	rootCmd.PersistentFlags().StringVar(&stateCacheStr, "state.cache", "0MB", "Amount of data to store in StateCache (enabled if no --datadir set). Set 0 to disable StateCache. Defaults to 0MB RAM")
	rootCmd.PersistentFlags().BoolVar(&cfg.GRPCServerEnabled, "grpc", false, "Enable GRPC server")
	rootCmd.PersistentFlags().StringVar(&cfg.GRPCListenAddress, "grpc.addr", nodecfg.DefaultGRPCHost, "GRPC server listening interface")
	rootCmd.PersistentFlags().IntVar(&cfg.GRPCPort, "grpc.port", nodecfg.DefaultGRPCPort, "GRPC server listening port")
	rootCmd.PersistentFlags().BoolVar(&cfg.GRPCHealthCheckEnabled, "grpc.healthcheck", false, "Enable GRPC health check")
	rootCmd.PersistentFlags().Float64Var(&ethconfig.Defaults.RPCTxFeeCap, utils.RPCGlobalTxFeeCapFlag.Name, utils.RPCGlobalTxFeeCapFlag.Value, utils.RPCGlobalTxFeeCapFlag.Usage)
	rootCmd.PersistentFlags().StringVar(&cfg.TLSCertfile, "tls.cert", "", "certificate for client side TLS handshake for GRPC")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSKeyFile, "tls.key", "", "key file for client side TLS handshake for GRPC")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSCACert, "tls.cacert", "", "CA certificate for client side TLS handshake for GRPC")

	rootCmd.PersistentFlags().StringSliceVar(&cfg.API, "http.api", []string{"eth", "erigon"}, "API's offered over the RPC interface: eth,erigon,web3,net,debug,trace,txpool,db. Supported methods: https://github.com/ledgerwatch/erigon/tree/main/cmd/rpcdaemon")

	rootCmd.PersistentFlags().BoolVar(&cfg.HttpServerEnabled, "http.enabled", true, "enable http server")
	rootCmd.PersistentFlags().StringVar(&cfg.HttpListenAddress, "http.addr", nodecfg.DefaultHTTPHost, "HTTP server listening interface")
	rootCmd.PersistentFlags().IntVar(&cfg.HttpPort, "http.port", nodecfg.DefaultHTTPPort, "HTTP server listening port")
	rootCmd.PersistentFlags().StringVar(&cfg.HttpURL, "http.url", "", "HTTP server listening url. will OVERRIDE http.addr and http.port. will NOT respect http paths. prefix supported are tcp, unix")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpCORSDomain, "http.corsdomain", []string{}, "Comma separated list of domains from which to accept cross origin requests (browser enforced)")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpVirtualHost, "http.vhosts", nodecfg.DefaultConfig.HTTPVirtualHosts, "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.")
	rootCmd.PersistentFlags().BoolVar(&cfg.HttpCompression, "http.compression", true, "Disable http compression")
	rootCmd.PersistentFlags().BoolVar(&cfg.WebsocketEnabled, "ws", false, "Enable Websockets - Same port as HTTP[S]")
	rootCmd.PersistentFlags().BoolVar(&cfg.WebsocketCompression, "ws.compression", false, "Enable Websocket compression (RFC 7692)")

	rootCmd.PersistentFlags().BoolVar(&cfg.HttpsServerEnabled, "https.enabled", false, "enable http server")
	rootCmd.PersistentFlags().StringVar(&cfg.HttpsListenAddress, "https.addr", nodecfg.DefaultHTTPHost, "rpc HTTPS server listening interface")
	rootCmd.PersistentFlags().IntVar(&cfg.HttpsPort, "https.port", 0, "rpc HTTPS server listening port. default to http+363 if not set")
	rootCmd.PersistentFlags().StringVar(&cfg.HttpsURL, "https.url", "", "rpc HTTPS server listening url. will OVERRIDE https.addr and https.port. will NOT respect paths. prefix supported are tcp, unix")
	rootCmd.PersistentFlags().StringVar(&cfg.HttpsCertfile, "https.cert", "", "certificate for rpc HTTPS server")
	rootCmd.PersistentFlags().StringVar(&cfg.HttpsKeyFile, "https.key", "", "key file for rpc HTTPS server")

	rootCmd.PersistentFlags().BoolVar(&cfg.SocketServerEnabled, "socket.enabled", false, "Enable IPC server")
	rootCmd.PersistentFlags().StringVar(&cfg.SocketListenUrl, "socket.url", "unix:///var/run/erigon.sock", "IPC server listening url. prefix supported are tcp, unix")

	rootCmd.PersistentFlags().BoolVar(&cfg.TraceRequests, utils.HTTPTraceFlag.Name, false, "Trace HTTP requests with INFO level")
	rootCmd.PersistentFlags().DurationVar(&cfg.HTTPTimeouts.ReadTimeout, "http.timeouts.read", rpccfg.DefaultHTTPTimeouts.ReadTimeout, "Maximum duration for reading the entire request, including the body.")
	rootCmd.PersistentFlags().DurationVar(&cfg.HTTPTimeouts.WriteTimeout, "http.timeouts.write", rpccfg.DefaultHTTPTimeouts.WriteTimeout, "Maximum duration before timing out writes of the response. It is reset whenever a new request's header is read")
	rootCmd.PersistentFlags().DurationVar(&cfg.HTTPTimeouts.IdleTimeout, "http.timeouts.idle", rpccfg.DefaultHTTPTimeouts.IdleTimeout, "Maximum amount of time to wait for the next request when keep-alives are enabled. If http.timeouts.idle is zero, the value of http.timeouts.read is used")
	rootCmd.PersistentFlags().DurationVar(&cfg.EvmCallTimeout, "rpc.evmtimeout", rpccfg.DefaultEvmCallTimeout, "Maximum amount of time to wait for the answer from EVM call.")
	rootCmd.PersistentFlags().DurationVar(&cfg.OverlayGetLogsTimeout, "rpc.overlay.getlogstimeout", rpccfg.DefaultOverlayGetLogsTimeout, "Maximum amount of time to wait for the answer from the overlay_getLogs call.")
	rootCmd.PersistentFlags().DurationVar(&cfg.OverlayReplayBlockTimeout, "rpc.overlay.replayblocktimeout", rpccfg.DefaultOverlayReplayBlockTimeout, "Maximum amount of time to wait for the answer to replay a single block when called from an overlay_getLogs call.")
	rootCmd.PersistentFlags().IntVar(&cfg.BatchLimit, utils.RpcBatchLimit.Name, utils.RpcBatchLimit.Value, utils.RpcBatchLimit.Usage)
	rootCmd.PersistentFlags().IntVar(&cfg.ReturnDataLimit, utils.RpcReturnDataLimit.Name, utils.RpcReturnDataLimit.Value, utils.RpcReturnDataLimit.Usage)
	rootCmd.PersistentFlags().BoolVar(&cfg.AllowUnprotectedTxs, utils.AllowUnprotectedTxs.Name, utils.AllowUnprotectedTxs.Value, utils.AllowUnprotectedTxs.Usage)
	rootCmd.PersistentFlags().IntVar(&cfg.MaxGetProofRewindBlockCount, utils.RpcMaxGetProofRewindBlockCount.Name, utils.RpcMaxGetProofRewindBlockCount.Value, utils.RpcMaxGetProofRewindBlockCount.Usage)
	rootCmd.PersistentFlags().Uint64Var(&cfg.OtsMaxPageSize, utils.OtsSearchMaxCapFlag.Name, utils.OtsSearchMaxCapFlag.Value, utils.OtsSearchMaxCapFlag.Usage)
	rootCmd.PersistentFlags().DurationVar(&cfg.RPCSlowLogThreshold, utils.RPCSlowFlag.Name, utils.RPCSlowFlag.Value, utils.RPCSlowFlag.Usage)
	rootCmd.PersistentFlags().IntVar(&cfg.WebsocketSubscribeLogsChannelSize, utils.WSSubscribeLogsChannelSize.Name, utils.WSSubscribeLogsChannelSize.Value, utils.WSSubscribeLogsChannelSize.Usage)

	if err := rootCmd.MarkPersistentFlagFilename("rpc.accessList", "json"); err != nil {
		panic(err)
	}
	if err := rootCmd.MarkPersistentFlagDirname("datadir"); err != nil {
		panic(err)
	}

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {

		err := cfg.StateCache.CacheSize.UnmarshalText([]byte(stateCacheStr))
		if err != nil {
			return fmt.Errorf("state.cache value of %v is not valid", stateCacheStr)
		}

		err = cfg.StateCache.CodeCacheSize.UnmarshalText([]byte(stateCacheStr))
		if err != nil {
			return fmt.Errorf("state.cache value of %v is not valid", stateCacheStr)
		}

		cfg.WithDatadir = cfg.DataDir != ""
		if cfg.WithDatadir {
			if cfg.DataDir == "" {
				cfg.DataDir = paths.DefaultDataDir()
			}
			var dataDir flags.DirectoryString
			dataDir.Set(cfg.DataDir)
			cfg.Dirs = datadir.New(string(dataDir))
		}
		if cfg.TxPoolApiAddr == "" {
			cfg.TxPoolApiAddr = cfg.PrivateApiAddr
		}
		return nil
	}
	rootCmd.PersistentPostRunE = func(cmd *cobra.Command, args []string) error {
		debug.Exit()
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
				log.Warn("[rpcdaemon subscribeToStateChanges]", "err", err)
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
	var compatErr error
	var compatTx kv.Tx
	if compatTx, compatErr = db.BeginRo(ctx); compatErr != nil {
		return fmt.Errorf("open Ro Tx for DB schema compability check: %w", compatErr)
	}
	defer compatTx.Rollback()
	major, minor, patch, ok, err := rawdb.ReadDBSchemaVersion(compatTx)
	if err != nil {
		return fmt.Errorf("read version for DB schema compability check: %w", compatErr)
	}
	if ok {
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
	}

	return nil
}

func EmbeddedServices(ctx context.Context,
	erigonDB kv.RoDB, stateCacheCfg kvcache.CoherentConfig,
	blockReader services.FullBlockReader, ethBackendServer remote.ETHBACKENDServer, txPoolServer txpool.TxpoolServer,
	miningServer txpool.MiningServer, stateDiffClient StateChangesClient,
	logger log.Logger,
) (eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient, stateCache kvcache.Cache, ff *rpchelper.Filters, err error) {
	if stateCacheCfg.CacheSize > 0 {
		// notification about new blocks (state stream) doesn't work now inside erigon - because
		// erigon does send this stream to privateAPI (erigon with enabled rpc, still have enabled privateAPI).
		// without this state stream kvcache can't work and only slow-down things
		// ... adding back in place to see about the above statement
		stateCache = kvcache.New(stateCacheCfg)
	} else {
		stateCache = kvcache.NewDummy(stateCacheCfg.StateV3)
	}

	subscribeToStateChangesLoop(ctx, stateDiffClient, stateCache)

	directClient := direct.NewEthBackendClientDirect(ethBackendServer)

	eth = rpcservices.NewRemoteBackend(directClient, erigonDB, blockReader)

	txPool = direct.NewTxPoolClient(txPoolServer)
	mining = direct.NewMiningClient(miningServer)
	ff = rpchelper.New(ctx, eth, txPool, mining, func() {}, logger)

	return
}

// RemoteServices - use when RPCDaemon run as independent process. Still it can use --datadir flag to enable
// `cfg.WithDatadir` (mode when it on 1 machine with Erigon)
func RemoteServices(ctx context.Context, cfg *httpcfg.HttpCfg, logger log.Logger, rootCancel context.CancelFunc) (
	db kv.RoDB, eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient,
	stateCache kvcache.Cache, blockReader services.FullBlockReader, engine consensus.EngineReader,
	ff *rpchelper.Filters, agg *libstate.Aggregator, err error) {
	if !cfg.WithDatadir && cfg.PrivateApiAddr == "" {
		return nil, nil, nil, nil, nil, nil, nil, ff, nil, fmt.Errorf("either remote db or local db must be specified")
	}
	creds, err := grpcutil.TLS(cfg.TLSCACert, cfg.TLSCertfile, cfg.TLSKeyFile)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, ff, nil, fmt.Errorf("open tls cert: %w", err)
	}
	conn, err := grpcutil.Connect(creds, cfg.PrivateApiAddr)
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, ff, nil, fmt.Errorf("could not connect to execution service privateApi: %w", err)
	}

	remoteBackendClient := remote.NewETHBACKENDClient(conn)
	remoteKvClient := remote.NewKVClient(conn)
	remoteKv, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), logger, remoteKvClient).Open()
	if err != nil {
		return nil, nil, nil, nil, nil, nil, nil, ff, nil, fmt.Errorf("could not connect to remoteKv: %w", err)
	}

	// Configure DB first
	var allSnapshots *freezeblocks.RoSnapshots
	var allBorSnapshots *freezeblocks.BorRoSnapshots
	onNewSnapshot := func() {}

	var cc *chain.Config

	if cfg.WithDatadir {
		// Opening all databases in Accede and non-Readonly modes. Here is the motivation:
		// Rpcdaemon must provide 2 features:
		//     1. ability to start even if Erigon is down (to prevent cascade outage).
		//     2. don't create databases by itself - because it doesn't know right parameters (Erigon may have cli flags: pagesize, etc...)
		// Some databases (consensus, txpool, downloader) are woring in SafeNoSync mode - in this mode
		//    power-off may leave db in recoverable-non-consistent state. Such db can be recovered only if open in non-Readonly mode.
		// Accede mode preventing db-creation:
		//    at first start RpcDaemon may start earlier than Erigon
		//    Accede mode will check db existence (may wait with retries). It's ok to fail in this case - some supervisor will restart us.
		var rwKv kv.RwDB
		logger.Warn("Opening chain db", "path", cfg.Dirs.Chaindata)
		limiter := semaphore.NewWeighted(int64(cfg.DBReadConcurrency))
		rwKv, err = kv2.NewMDBX(logger).RoTxsLimiter(limiter).Path(cfg.Dirs.Chaindata).Accede().Open(ctx)
		if err != nil {
			return nil, nil, nil, nil, nil, nil, nil, ff, nil, err
		}
		if compatErr := checkDbCompatibility(ctx, rwKv); compatErr != nil {
			return nil, nil, nil, nil, nil, nil, nil, ff, nil, compatErr
		}
		db = rwKv

		if err := db.View(context.Background(), func(tx kv.Tx) error {
			genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
			if err != nil {
				return err
			}
			cc, err = rawdb.ReadChainConfig(tx, genesisHash)
			if err != nil {
				return err
			}
			cfg.Snap.Enabled, err = snap.Enabled(tx)
			if err != nil {
				return err
			}
			return nil
		}); err != nil {
			return nil, nil, nil, nil, nil, nil, nil, ff, nil, err
		}
		if cc == nil {
			return nil, nil, nil, nil, nil, nil, nil, ff, nil, fmt.Errorf("chain config not found in db. Need start erigon at least once on this db")
		}
		cfg.Snap.Enabled = cfg.Snap.Enabled || cfg.Sync.UseSnapshots
		if !cfg.Snap.Enabled {
			logger.Info("Use --snapshots=false")
		}

		// Configure sapshots
		allSnapshots = freezeblocks.NewRoSnapshots(cfg.Snap, cfg.Dirs.Snap, 0, logger)
		allBorSnapshots = freezeblocks.NewBorRoSnapshots(cfg.Snap, cfg.Dirs.Snap, 0, logger)
		// To povide good UX - immediatly can read snapshots after RPCDaemon start, even if Erigon is down
		// Erigon does store list of snapshots in db: means RPCDaemon can read this list now, but read by `remoteKvClient.Snapshots` after establish grpc connection
		allSnapshots.OptimisticReopenWithDB(db)
		allBorSnapshots.OptimisticalyReopenWithDB(db)
		allSnapshots.LogStat("remote")
		allBorSnapshots.LogStat("remote")

		if agg, err = libstate.NewAggregator(ctx, cfg.Dirs, config3.HistoryV3AggregationStep, db, logger); err != nil {
			return nil, nil, nil, nil, nil, nil, nil, ff, nil, fmt.Errorf("create aggregator: %w", err)
		}
		_ = agg.OpenFolder(true) //TODO: must use analog of `OptimisticReopenWithDB`

		db.View(context.Background(), func(tx kv.Tx) error {
			aggTx := agg.BeginFilesRo()
			defer aggTx.Close()
			aggTx.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
				_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
				return histBlockNumProgress
			})
			return nil
		})
		onNewSnapshot = func() {
			go func() { // don't block events processing by network communication
				reply, err := remoteKvClient.Snapshots(ctx, &remote.SnapshotsRequest{}, grpc.WaitForReady(true))
				if err != nil {
					logger.Warn("[snapshots] reopen", "err", err)
					return
				}
				if err := allSnapshots.ReopenList(reply.BlocksFiles, true); err != nil {
					logger.Error("[snapshots] reopen", "err", err)
				} else {
					allSnapshots.LogStat("reopen")
				}
				if err := allBorSnapshots.ReopenList(reply.BlocksFiles, true); err != nil {
					logger.Error("[bor snapshots] reopen", "err", err)
				} else {
					allBorSnapshots.LogStat("reopen")
				}

				//if err = agg.OpenList(reply.HistoryFiles, true); err != nil {
				if err = agg.OpenFolder(true); err != nil {
					logger.Error("[snapshots] reopen", "err", err)
				} else {
					db.View(context.Background(), func(tx kv.Tx) error {
						ac := agg.BeginFilesRo()
						defer ac.Close()
						ac.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
							_, histBlockNumProgress, _ := rawdbv3.TxNums.FindBlockNum(tx, endTxNumMinimax)
							return histBlockNumProgress
						})
						return nil
					})
				}
			}()
		}
		onNewSnapshot()
		blockReader = freezeblocks.NewBlockReader(allSnapshots, allBorSnapshots)

		var histV3Enabled bool
		_ = db.View(ctx, func(tx kv.Tx) error {
			histV3Enabled, _ = kvcfg.HistoryV3.Enabled(tx)
			return nil
		})
		cfg.StateCache.StateV3 = histV3Enabled
		if histV3Enabled {
			logger.Info("HistoryV3", "enable", histV3Enabled)
			db, err = temporal.New(rwKv, agg)
			if err != nil {
				return nil, nil, nil, nil, nil, nil, nil, nil, nil, err
			}
		}
		stateCache = kvcache.NewDummy(cfg.StateCache.StateV3)
	}
	// If DB can't be configured - used PrivateApiAddr as remote DB
	if db == nil {
		db = remoteKv
	}

	if !cfg.WithDatadir {
		if cfg.StateCache.CacheSize > 0 {
			stateCache = kvcache.New(cfg.StateCache)
		} else {
			stateCache = kvcache.NewDummy(cfg.StateCache.StateV3)
		}
		logger.Info("if you run RPCDaemon on same machine with Erigon add --datadir option")
	}

	subscribeToStateChangesLoop(ctx, remoteKvClient, stateCache)

	txpoolConn := conn
	if cfg.TxPoolApiAddr != cfg.PrivateApiAddr {
		txpoolConn, err = grpcutil.Connect(creds, cfg.TxPoolApiAddr)
		if err != nil {
			return nil, nil, nil, nil, nil, nil, nil, ff, nil, fmt.Errorf("could not connect to txpool api: %w", err)
		}
	}

	mining = txpool.NewMiningClient(txpoolConn)
	miningService := rpcservices.NewMiningService(mining)
	txPool = txpool.NewTxpoolClient(txpoolConn)
	txPoolService := rpcservices.NewTxPoolService(txPool)

	if !cfg.WithDatadir {
		blockReader = freezeblocks.NewRemoteBlockReader(remoteBackendClient)
	}

	remoteEth := rpcservices.NewRemoteBackend(remoteBackendClient, db, blockReader)
	blockReader = remoteEth
	eth = remoteEth

	var remoteCE *remoteConsensusEngine

	if cfg.WithDatadir {
		switch {
		case cc != nil:
			switch {
			case cc.Bor != nil:
				var borKv kv.RoDB

				// bor (consensus) specific db
				borDbPath := filepath.Join(cfg.DataDir, "bor")
				logger.Warn("[rpc] Opening Bor db", "path", borDbPath)
				borKv, err = kv2.NewMDBX(logger).Path(borDbPath).Label(kv.ConsensusDB).Accede().Open(ctx)
				if err != nil {
					return nil, nil, nil, nil, nil, nil, nil, ff, nil, err
				}
				// Skip the compatibility check, until we have a schema in erigon-lib

				borConfig := cc.Bor.(*borcfg.BorConfig)

				engine = bor.NewRo(cc, borKv, blockReader,
					bor.NewChainSpanner(bor.GenesisContractValidatorSetABI(), cc, true, logger),
					bor.NewGenesisContractsClient(cc, borConfig.ValidatorContract, borConfig.StateReceiverContract, logger), logger)

			default:
				engine = ethash.NewFaker()
			}

		default:
			engine = ethash.NewFaker()
		}
	} else {
		remoteCE = &remoteConsensusEngine{}
		engine = remoteCE
	}

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
		if remoteCE != nil {
			if !remoteCE.init(db, blockReader, remoteKvClient, logger) {
				rootCancel()
			}
		}
	}()

	ff = rpchelper.New(ctx, eth, txPool, mining, onNewSnapshot, logger)
	return db, eth, txPool, mining, stateCache, blockReader, engine, ff, agg, err
}

func StartRpcServer(ctx context.Context, cfg *httpcfg.HttpCfg, rpcAPI []rpc.API, logger log.Logger) error {
	if cfg.Enabled {
		return startRegularRpcServer(ctx, cfg, rpcAPI, logger)
	}

	return nil
}

func StartRpcServerWithJwtAuthentication(ctx context.Context, cfg *httpcfg.HttpCfg, rpcAPI []rpc.API, logger log.Logger) error {
	if len(rpcAPI) == 0 {
		return nil
	}
	engineInfo, err := startAuthenticatedRpcServer(cfg, rpcAPI, logger)
	if err != nil {
		return err
	}
	go stopAuthenticatedRpcServer(ctx, engineInfo, logger)
	return nil
}

func startRegularRpcServer(ctx context.Context, cfg *httpcfg.HttpCfg, rpcAPI []rpc.API, logger log.Logger) error {
	// register apis and create handler stack
	srv := rpc.NewServer(cfg.RpcBatchConcurrency, cfg.TraceRequests, cfg.DebugSingleRequest, cfg.RpcStreamingDisable, logger, cfg.RPCSlowLogThreshold)

	allowListForRPC, err := parseAllowListForRPC(cfg.RpcAllowListFilePath)
	if err != nil {
		return err
	}
	srv.SetAllowList(allowListForRPC)

	srv.SetBatchLimit(cfg.BatchLimit)

	defer srv.Stop()

	var defaultAPIList []rpc.API

	for _, api := range rpcAPI {
		if api.Namespace != "engine" {
			defaultAPIList = append(defaultAPIList, api)
		}
	}

	var apiFlags []string
	for _, flag := range cfg.API {
		if flag != "engine" {
			apiFlags = append(apiFlags, flag)
		}
	}

	if err := node.RegisterApisFromWhitelist(defaultAPIList, apiFlags, srv, false, logger); err != nil {
		return fmt.Errorf("could not start register RPC apis: %w", err)
	}

	info := []interface{}{
		"ws", cfg.WebsocketEnabled,
		"ws.compression", cfg.WebsocketCompression, "grpc", cfg.GRPCServerEnabled,
	}

	if cfg.SocketServerEnabled {
		socketUrl, err := url.Parse(cfg.SocketListenUrl)
		if err != nil {
			return fmt.Errorf("malformatted socket url %s: %w", cfg.SocketListenUrl, err)
		}
		tcpListener, err := net.Listen(socketUrl.Scheme, socketUrl.Host+socketUrl.EscapedPath())
		if err != nil {
			return fmt.Errorf("could not start Socket Listener: %w", err)
		}
		defer tcpListener.Close()
		go func() {
			err := srv.ServeListener(tcpListener)
			if err != nil {
				if !errors.Is(err, net.ErrClosed) {
					logger.Error("Socket Listener Fatal Error", "err", err)
				}
			}
		}()
		info = append(info, "socket.url", socketUrl)
		logger.Info("Socket Endpoint opened", "url", socketUrl)
	}

	httpHandler := node.NewHTTPHandlerStack(srv, cfg.HttpCORSDomain, cfg.HttpVirtualHost, cfg.HttpCompression)
	var wsHandler http.Handler
	if cfg.WebsocketEnabled {
		wsHandler = srv.WebsocketHandler([]string{"*"}, nil, cfg.WebsocketCompression, logger)
	}
	graphQLHandler := graphql.CreateHandler(defaultAPIList)
	apiHandler, err := createHandler(cfg, defaultAPIList, httpHandler, wsHandler, graphQLHandler, nil)
	if err != nil {
		return err
	}

	// Separate Websocket handler if websocket port flag specified
	if cfg.WebsocketEnabled && cfg.WebsocketPort != cfg.HttpPort {
		wsEndpoint := fmt.Sprintf("tcp://%s:%d", cfg.HttpListenAddress, cfg.WebsocketPort)
		wsApiHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if isWebsocket(r) {
				wsHandler.ServeHTTP(w, r)
			}
		})
		wsListener, wsAddr, err := node.StartHTTPEndpoint(wsEndpoint, &node.HttpEndpointConfig{Timeouts: cfg.HTTPTimeouts}, wsApiHandler)
		if err != nil {
			return fmt.Errorf("could not start separate Websocket RPC api at port %d: %w", cfg.WebsocketPort, err)
		}
		info = append(info, "websocket.url", wsAddr)
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = wsListener.Shutdown(shutdownCtx)
			logger.Info("HTTP endpoint closed", "url", wsAddr)
		}()
	}

	if cfg.HttpServerEnabled {
		httpEndpoint := fmt.Sprintf("tcp://%s:%d", cfg.HttpListenAddress, cfg.HttpPort)
		if cfg.HttpURL != "" {
			httpEndpoint = cfg.HttpURL
		}
		listener, httpAddr, err := node.StartHTTPEndpoint(httpEndpoint, &node.HttpEndpointConfig{
			Timeouts: cfg.HTTPTimeouts,
		}, apiHandler)
		if err != nil {
			return fmt.Errorf("could not start RPC api: %w", err)
		}
		info = append(info, "http.url", httpAddr)
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = listener.Shutdown(shutdownCtx)
			logger.Info("HTTP endpoint closed", "url", httpAddr)
		}()
	}
	if cfg.HttpsURL != "" {
		cfg.HttpsServerEnabled = true
	}
	if cfg.HttpsServerEnabled {
		if cfg.HttpsPort == 0 {
			cfg.HttpsPort = cfg.HttpPort + 363
		}
		httpsEndpoint := fmt.Sprintf("tcp://%s:%d", cfg.HttpsListenAddress, cfg.HttpsPort)
		if cfg.HttpsURL != "" {
			httpsEndpoint = cfg.HttpsURL
		}
		listener, httpAddr, err := node.StartHTTPEndpoint(httpsEndpoint, &node.HttpEndpointConfig{
			Timeouts: cfg.HTTPTimeouts,
			HTTPS:    true,
			CertFile: cfg.HttpsCertfile,
			KeyFile:  cfg.HttpsKeyFile,
		}, apiHandler)
		if err != nil {
			return fmt.Errorf("could not start RPC api: %w", err)
		}
		info = append(info, "https.url", httpAddr)
		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = listener.Shutdown(shutdownCtx)
			logger.Info("HTTPS endpoint closed", "url", httpAddr)
		}()
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

		defer func() {
			if cfg.GRPCServerEnabled {
				if cfg.GRPCHealthCheckEnabled {
					healthServer.Shutdown()
				}
				grpcServer.GracefulStop()
				_ = grpcListener.Close()
				logger.Info("GRPC endpoint closed", "url", grpcEndpoint)
			}
		}()
	}

	logger.Info("JsonRpc endpoint opened", info...)
	<-ctx.Done()
	logger.Info("Exiting...")
	return nil
}

type engineInfo struct {
	Srv                *rpc.Server
	EngineSrv          *rpc.Server
	EngineListener     *http.Server
	EngineHttpEndpoint string
}

func startAuthenticatedRpcServer(cfg *httpcfg.HttpCfg, rpcAPI []rpc.API, logger log.Logger) (*engineInfo, error) {
	srv := rpc.NewServer(cfg.RpcBatchConcurrency, cfg.TraceRequests, cfg.DebugSingleRequest, cfg.RpcStreamingDisable, logger, cfg.RPCSlowLogThreshold)

	engineListener, engineSrv, engineHttpEndpoint, err := createEngineListener(cfg, rpcAPI, logger)
	if err != nil {
		return nil, fmt.Errorf("could not start RPC api for engine: %w", err)
	}
	return &engineInfo{Srv: srv, EngineSrv: engineSrv, EngineListener: engineListener, EngineHttpEndpoint: engineHttpEndpoint}, nil
}

func stopAuthenticatedRpcServer(ctx context.Context, engineInfo *engineInfo, logger log.Logger) {
	defer func() {
		engineInfo.Srv.Stop()
		if engineInfo.EngineSrv != nil {
			engineInfo.EngineSrv.Stop()
		}
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if engineInfo.EngineListener != nil {
			_ = engineInfo.EngineListener.Shutdown(shutdownCtx)
			logger.Info("Engine HTTP endpoint close", "url", engineInfo.EngineHttpEndpoint)
		}
	}()
	<-ctx.Done()
	logger.Info("Exiting Engine...")
}

// isWebsocket checks the header of a http request for a websocket upgrade request.
func isWebsocket(r *http.Request) bool {
	return strings.EqualFold(r.Header.Get("Upgrade"), "websocket") &&
		strings.Contains(strings.ToLower(r.Header.Get("Connection")), "upgrade")
}

// ObtainJWTSecret loads the jwt-secret, either from the provided config,
// or from the default location. If neither of those are present, it generates
// a new secret and stores to the default location.
func ObtainJWTSecret(cfg *httpcfg.HttpCfg, logger log.Logger) ([]byte, error) {
	// try reading from file
	logger.Info("Reading JWT secret", "path", cfg.JWTSecretPath)
	// If we run the rpcdaemon and datadir is not specified we just use jwt.hex in current directory.
	if len(cfg.JWTSecretPath) == 0 {
		cfg.JWTSecretPath = "jwt.hex"
	}
	if data, err := os.ReadFile(cfg.JWTSecretPath); err == nil {
		jwtSecret := common.FromHex(strings.TrimSpace(string(data)))
		if len(jwtSecret) == 32 {
			return jwtSecret, nil
		}
		logger.Error("Invalid JWT secret", "path", cfg.JWTSecretPath, "length", len(jwtSecret))
		return nil, errors.New("invalid JWT secret")
	}
	// Need to generate one
	jwtSecret := make([]byte, 32)
	rand.Read(jwtSecret)

	if err := os.WriteFile(cfg.JWTSecretPath, []byte(hexutility.Encode(jwtSecret)), 0600); err != nil {
		return nil, err
	}
	logger.Info("Generated JWT secret", "path", cfg.JWTSecretPath)
	return jwtSecret, nil
}

func createHandler(cfg *httpcfg.HttpCfg, apiList []rpc.API, httpHandler http.Handler, wsHandler http.Handler, graphQLHandler http.Handler, jwtSecret []byte) (http.Handler, error) {
	var handler http.Handler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if cfg.GraphQLEnabled && graphql.ProcessGraphQLcheckIfNeeded(graphQLHandler, w, r) {
			return
		}

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

func createEngineListener(cfg *httpcfg.HttpCfg, engineApi []rpc.API, logger log.Logger) (*http.Server, *rpc.Server, string, error) {
	engineHttpEndpoint := fmt.Sprintf("tcp://%s:%d", cfg.AuthRpcHTTPListenAddress, cfg.AuthRpcPort)

	engineSrv := rpc.NewServer(cfg.RpcBatchConcurrency, cfg.TraceRequests, cfg.DebugSingleRequest, true, logger, cfg.RPCSlowLogThreshold)

	if err := node.RegisterApisFromWhitelist(engineApi, nil, engineSrv, true, logger); err != nil {
		return nil, nil, "", fmt.Errorf("could not start register RPC engine api: %w", err)
	}

	jwtSecret, err := ObtainJWTSecret(cfg, logger)
	if err != nil {
		return nil, nil, "", err
	}

	wsHandler := engineSrv.WebsocketHandler([]string{"*"}, jwtSecret, cfg.WebsocketCompression, logger)

	engineHttpHandler := node.NewHTTPHandlerStack(engineSrv, nil /* authCors */, cfg.AuthRpcVirtualHost, cfg.HttpCompression)

	graphQLHandler := graphql.CreateHandler(engineApi)

	engineApiHandler, err := createHandler(cfg, engineApi, engineHttpHandler, wsHandler, graphQLHandler, jwtSecret)
	if err != nil {
		return nil, nil, "", err
	}

	engineListener, engineAddr, err := node.StartHTTPEndpoint(engineHttpEndpoint, &node.HttpEndpointConfig{
		Timeouts: cfg.AuthRpcTimeouts,
	}, engineApiHandler)
	if err != nil {
		return nil, nil, "", fmt.Errorf("could not start RPC api: %w", err)
	}

	engineInfo := []interface{}{"url", engineAddr, "ws", true, "ws.compression", cfg.WebsocketCompression}
	logger.Info("HTTP endpoint opened for Engine API", engineInfo...)

	return engineListener, engineSrv, engineAddr.String(), nil
}

var remoteConsensusEngineNotReadyErr = errors.New("remote consensus engine not ready")

type remoteConsensusEngine struct {
	engine consensus.Engine
}

func (e *remoteConsensusEngine) HasEngine() bool {
	return e.engine != nil
}

func (e *remoteConsensusEngine) Engine() consensus.EngineReader {
	return e.engine
}

func (e *remoteConsensusEngine) validateEngineReady() error {
	if !e.HasEngine() {
		return remoteConsensusEngineNotReadyErr
	}

	return nil
}

// init - reasoning behind init is that we would like to initialise the remote consensus engine either post rpcdaemon
// service startup or in a background goroutine, so that we do not depend on the liveness of other services when
// starting up rpcdaemon and do not block startup (avoiding "cascade outage" scenario). In this case the DB dependency
// can be a remote DB service running on another machine.
func (e *remoteConsensusEngine) init(db kv.RoDB, blockReader services.FullBlockReader, remoteKV remote.KVClient, logger log.Logger) bool {
	var cc *chain.Config

	if err := db.View(context.Background(), func(tx kv.Tx) error {
		genesisHash, err := rawdb.ReadCanonicalHash(tx, 0)
		if err != nil {
			return err
		}
		cc, err = rawdb.ReadChainConfig(tx, genesisHash)
		if err != nil {
			return err
		}
		return nil
	}); err != nil {
		return false
	}

	if cc.Bor != nil {
		borKv, err := remotedb.NewRemote(gointerfaces.VersionFromProto(remotedbserver.KvServiceAPIVersion), logger, remoteKV).
			WithBucketsConfig(kv.BorTablesCfg).
			Open()

		if err != nil {
			return false
		}

		borConfig := cc.Bor.(*borcfg.BorConfig)

		e.engine = bor.NewRo(cc, borKv, blockReader,
			bor.NewChainSpanner(bor.GenesisContractValidatorSetABI(), cc, true, logger),
			bor.NewGenesisContractsClient(cc, borConfig.ValidatorContract, borConfig.StateReceiverContract, logger), logger)
	} else {
		e.engine = ethash.NewFaker()
	}

	return true
}

func (e *remoteConsensusEngine) Author(header *types.Header) (libcommon.Address, error) {
	if err := e.validateEngineReady(); err != nil {
		return libcommon.Address{}, err
	}

	return e.engine.Author(header)
}

func (e *remoteConsensusEngine) IsServiceTransaction(sender libcommon.Address, syscall consensus.SystemCall) bool {
	if err := e.validateEngineReady(); err != nil {
		panic(err)
	}

	return e.engine.IsServiceTransaction(sender, syscall)
}

func (e *remoteConsensusEngine) Type() chain.ConsensusName {
	if err := e.validateEngineReady(); err != nil {
		panic(err)
	}

	return e.engine.Type()
}

func (e *remoteConsensusEngine) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall consensus.SystemCall) ([]consensus.Reward, error) {
	if err := e.validateEngineReady(); err != nil {
		return nil, err
	}

	return e.engine.CalculateRewards(config, header, uncles, syscall)
}

func (e *remoteConsensusEngine) Close() error {
	if err := e.validateEngineReady(); err != nil {
		return err
	}

	return e.engine.Close()
}

func (e *remoteConsensusEngine) Initialize(config *chain.Config, chain consensus.ChainHeaderReader, header *types.Header, state *state.IntraBlockState, syscall consensus.SysCallCustom, logger log.Logger) {
	if err := e.validateEngineReady(); err != nil {
		panic(err)
	}

	e.engine.Initialize(config, chain, header, state, syscall, logger)
}

func (e *remoteConsensusEngine) VerifyHeader(_ consensus.ChainHeaderReader, _ *types.Header, _ bool) error {
	panic("remoteConsensusEngine.VerifyHeader not supported")
}

func (e *remoteConsensusEngine) VerifyUncles(_ consensus.ChainReader, _ *types.Header, _ []*types.Header) error {
	panic("remoteConsensusEngine.VerifyUncles not supported")
}

func (e *remoteConsensusEngine) Prepare(_ consensus.ChainHeaderReader, _ *types.Header, _ *state.IntraBlockState) error {
	panic("remoteConsensusEngine.Prepare not supported")
}

func (e *remoteConsensusEngine) Finalize(_ *chain.Config, _ *types.Header, _ *state.IntraBlockState, _ types.Transactions, _ []*types.Header, _ types.Receipts, _ []*types.Withdrawal, _ consensus.ChainReader, _ consensus.SystemCall, _ log.Logger) (types.Transactions, types.Receipts, error) {
	panic("remoteConsensusEngine.Finalize not supported")
}

func (e *remoteConsensusEngine) FinalizeAndAssemble(_ *chain.Config, _ *types.Header, _ *state.IntraBlockState, _ types.Transactions, _ []*types.Header, _ types.Receipts, _ []*types.Withdrawal, _ consensus.ChainReader, _ consensus.SystemCall, _ consensus.Call, _ log.Logger) (*types.Block, types.Transactions, types.Receipts, error) {
	panic("remoteConsensusEngine.FinalizeAndAssemble not supported")
}

func (e *remoteConsensusEngine) Seal(_ consensus.ChainHeaderReader, _ *types.Block, _ chan<- *types.Block, _ <-chan struct{}) error {
	panic("remoteConsensusEngine.Seal not supported")
}

func (e *remoteConsensusEngine) SealHash(_ *types.Header) libcommon.Hash {
	panic("remoteConsensusEngine.SealHash not supported")
}

func (e *remoteConsensusEngine) CalcDifficulty(_ consensus.ChainHeaderReader, _ uint64, _ uint64, _ *big.Int, _ uint64, _ libcommon.Hash, _ libcommon.Hash, _ uint64) *big.Int {
	panic("remoteConsensusEngine.CalcDifficulty not supported")
}

func (e *remoteConsensusEngine) GenerateSeal(_ consensus.ChainHeaderReader, _ *types.Header, _ *types.Header, _ consensus.Call) []byte {
	panic("remoteConsensusEngine.GenerateSeal not supported")
}

func (e *remoteConsensusEngine) APIs(_ consensus.ChainHeaderReader) []rpc.API {
	panic("remoteConsensusEngine.APIs not supported")
}
