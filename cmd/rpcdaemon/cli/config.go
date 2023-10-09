package cli

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon-lib/chain"
	libcommon "github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon-lib/common/datadir"
	"github.com/ledgerwatch/erigon-lib/common/dir"
	"github.com/ledgerwatch/erigon-lib/common/hexutility"
	"github.com/ledgerwatch/erigon-lib/kv/kvcfg"
	"github.com/ledgerwatch/erigon-lib/kv/rawdbv3"
	libstate "github.com/ledgerwatch/erigon-lib/state"
	"github.com/ledgerwatch/erigon/consensus"
	"github.com/ledgerwatch/erigon/consensus/bor"
	"github.com/ledgerwatch/erigon/consensus/bor/contract"
	"github.com/ledgerwatch/erigon/consensus/bor/heimdall/span"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core/state/temporal"
	"github.com/ledgerwatch/erigon/core/systemcontracts"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/rpc/rpccfg"
	"github.com/ledgerwatch/erigon/turbo/debug"
	"github.com/ledgerwatch/erigon/turbo/logging"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/freezeblocks"
	"github.com/ledgerwatch/erigon/turbo/snapshotsync/snap"

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
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"golang.org/x/sync/semaphore"
	"google.golang.org/grpc"
	grpcHealth "google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/graphql"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/health"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/rpcservices"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/cmd/utils/flags"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/core/rawdb"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/erigon/turbo/services"

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

	cfg := &httpcfg.HttpCfg{Enabled: true, StateCache: kvcache.DefaultCoherentConfig}
	rootCmd.PersistentFlags().StringVar(&cfg.PrivateApiAddr, "private.api.addr", "127.0.0.1:9090", "Erigon's components (txpool, rpcdaemon, sentry, downloader, ...) can be deployed as independent Processes on same/another server. Then components will connect to erigon by this internal grpc API. Example: 127.0.0.1:9090")
	rootCmd.PersistentFlags().StringVar(&cfg.DataDir, "datadir", "", "path to Erigon working directory")
	rootCmd.PersistentFlags().BoolVar(&cfg.GraphQLEnabled, "graphql", false, "enables graphql endpoint (disabled by default)")
	rootCmd.PersistentFlags().StringVar(&cfg.HttpListenAddress, "http.addr", nodecfg.DefaultHTTPHost, "HTTP-RPC server listening interface")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSCertfile, "tls.cert", "", "certificate for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSKeyFile, "tls.key", "", "key file for client side TLS handshake")
	rootCmd.PersistentFlags().StringVar(&cfg.TLSCACert, "tls.cacert", "", "CA certificate for client side TLS handshake")
	rootCmd.PersistentFlags().IntVar(&cfg.HttpPort, "http.port", nodecfg.DefaultHTTPPort, "HTTP-RPC server listening port")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpCORSDomain, "http.corsdomain", []string{}, "Comma separated list of domains from which to accept cross origin requests (browser enforced)")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.HttpVirtualHost, "http.vhosts", nodecfg.DefaultConfig.HTTPVirtualHosts, "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.")
	rootCmd.PersistentFlags().BoolVar(&cfg.HttpCompression, "http.compression", true, "Disable http compression")
	rootCmd.PersistentFlags().StringSliceVar(&cfg.API, "http.api", []string{"eth", "erigon"}, "API's offered over the HTTP-RPC interface: eth,erigon,web3,net,debug,trace,txpool,db. Supported methods: https://github.com/ledgerwatch/erigon/tree/devel/cmd/rpcdaemon")
	rootCmd.PersistentFlags().Uint64Var(&cfg.Gascap, "rpc.gascap", 50_000_000, "Sets a cap on gas that can be used in eth_call/estimateGas")
	rootCmd.PersistentFlags().Uint64Var(&cfg.MaxTraces, "trace.maxtraces", 200, "Sets a limit on traces that can be returned in trace_filter")
	rootCmd.PersistentFlags().BoolVar(&cfg.WebsocketEnabled, "ws", false, "Enable Websockets - Same port as HTTP")
	rootCmd.PersistentFlags().BoolVar(&cfg.WebsocketCompression, "ws.compression", false, "Enable Websocket compression (RFC 7692)")
	rootCmd.PersistentFlags().StringVar(&cfg.RpcAllowListFilePath, utils.RpcAccessListFlag.Name, "", "Specify granular (method-by-method) API allowlist")
	rootCmd.PersistentFlags().UintVar(&cfg.RpcBatchConcurrency, utils.RpcBatchConcurrencyFlag.Name, 2, utils.RpcBatchConcurrencyFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&cfg.RpcStreamingDisable, utils.RpcStreamingDisableFlag.Name, false, utils.RpcStreamingDisableFlag.Usage)
	rootCmd.PersistentFlags().IntVar(&cfg.DBReadConcurrency, utils.DBReadConcurrencyFlag.Name, utils.DBReadConcurrencyFlag.Value, utils.DBReadConcurrencyFlag.Usage)
	rootCmd.PersistentFlags().BoolVar(&cfg.TraceCompatibility, "trace.compat", false, "Bug for bug compatibility with OE for trace_ routines")
	rootCmd.PersistentFlags().StringVar(&cfg.TxPoolApiAddr, "txpool.api.addr", "", "txpool api network address, for example: 127.0.0.1:9090 (default: use value of --private.api.addr)")
	rootCmd.PersistentFlags().BoolVar(&cfg.Sync.UseSnapshots, "snapshot", true, utils.SnapshotFlag.Usage)
	rootCmd.PersistentFlags().StringVar(&stateCacheStr, "state.cache", "0MB", "Amount of data to store in StateCache (enabled if no --datadir set). Set 0 to disable StateCache. Defaults to 0MB RAM")
	rootCmd.PersistentFlags().BoolVar(&cfg.GRPCServerEnabled, "grpc", false, "Enable GRPC server")
	rootCmd.PersistentFlags().StringVar(&cfg.GRPCListenAddress, "grpc.addr", nodecfg.DefaultGRPCHost, "GRPC server listening interface")
	rootCmd.PersistentFlags().IntVar(&cfg.GRPCPort, "grpc.port", nodecfg.DefaultGRPCPort, "GRPC server listening port")
	rootCmd.PersistentFlags().BoolVar(&cfg.GRPCHealthCheckEnabled, "grpc.healthcheck", false, "Enable GRPC health check")
	rootCmd.PersistentFlags().Float64Var(&ethconfig.Defaults.RPCTxFeeCap, utils.RPCGlobalTxFeeCapFlag.Name, utils.RPCGlobalTxFeeCapFlag.Value, utils.RPCGlobalTxFeeCapFlag.Usage)

	rootCmd.PersistentFlags().BoolVar(&cfg.TCPServerEnabled, "tcp", false, "Enable TCP server")
	rootCmd.PersistentFlags().StringVar(&cfg.TCPListenAddress, "tcp.addr", nodecfg.DefaultTCPHost, "TCP server listening interface")
	rootCmd.PersistentFlags().IntVar(&cfg.TCPPort, "tcp.port", nodecfg.DefaultTCPPort, "TCP server listening port")

	rootCmd.PersistentFlags().BoolVar(&cfg.TraceRequests, utils.HTTPTraceFlag.Name, false, "Trace HTTP requests with INFO level")
	rootCmd.PersistentFlags().DurationVar(&cfg.HTTPTimeouts.ReadTimeout, "http.timeouts.read", rpccfg.DefaultHTTPTimeouts.ReadTimeout, "Maximum duration for reading the entire request, including the body.")
	rootCmd.PersistentFlags().DurationVar(&cfg.HTTPTimeouts.WriteTimeout, "http.timeouts.write", rpccfg.DefaultHTTPTimeouts.WriteTimeout, "Maximum duration before timing out writes of the response. It is reset whenever a new request's header is read")
	rootCmd.PersistentFlags().DurationVar(&cfg.HTTPTimeouts.IdleTimeout, "http.timeouts.idle", rpccfg.DefaultHTTPTimeouts.IdleTimeout, "Maximum amount of time to wait for the next request when keep-alives are enabled. If http.timeouts.idle is zero, the value of http.timeouts.read is used")
	rootCmd.PersistentFlags().DurationVar(&cfg.EvmCallTimeout, "rpc.evmtimeout", rpccfg.DefaultEvmCallTimeout, "Maximum amount of time to wait for the answer from EVM call.")
	rootCmd.PersistentFlags().IntVar(&cfg.BatchLimit, utils.RpcBatchLimit.Name, utils.RpcBatchLimit.Value, utils.RpcBatchLimit.Usage)
	rootCmd.PersistentFlags().IntVar(&cfg.ReturnDataLimit, utils.RpcReturnDataLimit.Name, utils.RpcReturnDataLimit.Value, utils.RpcReturnDataLimit.Usage)
	rootCmd.PersistentFlags().BoolVar(&cfg.AllowUnprotectedTxs, utils.AllowUnprotectedTxs.Name, utils.AllowUnprotectedTxs.Value, utils.AllowUnprotectedTxs.Usage)
	rootCmd.PersistentFlags().Uint64Var(&cfg.OtsMaxPageSize, utils.OtsSearchMaxCapFlag.Name, utils.OtsSearchMaxCapFlag.Value, utils.OtsSearchMaxCapFlag.Usage)

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
		stateCache = kvcache.NewDummy()
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
func RemoteServices(ctx context.Context, cfg httpcfg.HttpCfg, logger log.Logger, rootCancel context.CancelFunc) (
	db kv.RoDB, eth rpchelper.ApiBackend, txPool txpool.TxpoolClient, mining txpool.MiningClient,
	stateCache kvcache.Cache, blockReader services.FullBlockReader, engine consensus.EngineReader,
	ff *rpchelper.Filters, agg *libstate.AggregatorV3, err error) {
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
		var rwKv kv.RwDB
		dir.MustExist(cfg.Dirs.SnapHistory)
		logger.Trace("Creating chain db", "path", cfg.Dirs.Chaindata)
		limiter := semaphore.NewWeighted(int64(cfg.DBReadConcurrency))
		rwKv, err = kv2.NewMDBX(logger).RoTxsLimiter(limiter).Path(cfg.Dirs.Chaindata).Open()
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
		allSnapshots = freezeblocks.NewRoSnapshots(cfg.Snap, cfg.Dirs.Snap, logger)
		allBorSnapshots = freezeblocks.NewBorRoSnapshots(cfg.Snap, cfg.Dirs.Snap, logger)
		// To povide good UX - immediatly can read snapshots after RPCDaemon start, even if Erigon is down
		// Erigon does store list of snapshots in db: means RPCDaemon can read this list now, but read by `remoteKvClient.Snapshots` after establish grpc connection
		allSnapshots.OptimisticReopenWithDB(db)
		allBorSnapshots.OptimisticalyReopenWithDB(db)
		allSnapshots.LogStat()
		allBorSnapshots.LogStat()

		if agg, err = libstate.NewAggregatorV3(ctx, cfg.Dirs.SnapHistory, cfg.Dirs.Tmp, ethconfig.HistoryV3AggregationStep, db, logger); err != nil {
			return nil, nil, nil, nil, nil, nil, nil, ff, nil, fmt.Errorf("create aggregator: %w", err)
		}
		_ = agg.OpenFolder()

		db.View(context.Background(), func(tx kv.Tx) error {
			agg.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
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
					allSnapshots.LogStat()
				}
				if err := allBorSnapshots.ReopenList(reply.BlocksFiles, true); err != nil {
					logger.Error("[bor snapshots] reopen", "err", err)
				} else {
					allSnapshots.LogStat()
				}

				_ = reply.HistoryFiles

				if err = agg.OpenFolder(); err != nil {
					logger.Error("[snapshots] reopen", "err", err)
				} else {
					db.View(context.Background(), func(tx kv.Tx) error {
						agg.LogStats(tx, func(endTxNumMinimax uint64) uint64 {
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
		if histV3Enabled {
			logger.Info("HistoryV3", "enable", histV3Enabled)
			db, err = temporal.New(rwKv, agg, systemcontracts.SystemContractCodeLookup[cc.ChainName])
			if err != nil {
				return nil, nil, nil, nil, nil, nil, nil, nil, nil, err
			}
		}
		stateCache = kvcache.NewDummy()
	}
	// If DB can't be configured - used PrivateApiAddr as remote DB
	if db == nil {
		db = remoteKv
	}

	if !cfg.WithDatadir {
		if cfg.StateCache.CacheSize > 0 {
			stateCache = kvcache.New(cfg.StateCache)
		} else {
			stateCache = kvcache.NewDummy()
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
				{
					// ensure db exist
					tmpDb, err := kv2.NewMDBX(logger).Path(borDbPath).Label(kv.ConsensusDB).Open()
					if err != nil {
						return nil, nil, nil, nil, nil, nil, nil, ff, nil, err
					}
					tmpDb.Close()
				}
				logger.Trace("Creating consensus db", "path", borDbPath)
				borKv, err = kv2.NewMDBX(logger).Path(borDbPath).Label(kv.ConsensusDB).Readonly().Open()
				if err != nil {
					return nil, nil, nil, nil, nil, nil, nil, ff, nil, err
				}
				// Skip the compatibility check, until we have a schema in erigon-lib

				engine = bor.NewRo(cc, borKv, blockReader,
					span.NewChainSpanner(contract.ValidatorSet(), cc, true, logger),
					contract.NewGenesisContractsClient(cc, cc.Bor.ValidatorContract, cc.Bor.StateReceiverContract, logger), logger)

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

func StartRpcServer(ctx context.Context, cfg httpcfg.HttpCfg, rpcAPI []rpc.API, logger log.Logger) error {
	if cfg.Enabled {
		return startRegularRpcServer(ctx, cfg, rpcAPI, logger)
	}

	return nil
}

func StartRpcServerWithJwtAuthentication(ctx context.Context, cfg httpcfg.HttpCfg, rpcAPI []rpc.API, logger log.Logger) error {
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

func startRegularRpcServer(ctx context.Context, cfg httpcfg.HttpCfg, rpcAPI []rpc.API, logger log.Logger) error {
	// register apis and create handler stack
	httpEndpoint := fmt.Sprintf("%s:%d", cfg.HttpListenAddress, cfg.HttpPort)

	srv := rpc.NewServer(cfg.RpcBatchConcurrency, cfg.TraceRequests, cfg.RpcStreamingDisable, logger)

	allowListForRPC, err := parseAllowListForRPC(cfg.RpcAllowListFilePath)
	if err != nil {
		return err
	}
	srv.SetAllowList(allowListForRPC)

	srv.SetBatchLimit(cfg.BatchLimit)

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

	listener, httpAddr, err := node.StartHTTPEndpoint(httpEndpoint, cfg.HTTPTimeouts, apiHandler)
	if err != nil {
		return fmt.Errorf("could not start RPC api: %w", err)
	}

	if cfg.TCPServerEnabled {
		tcpEndpoint := fmt.Sprintf("%s:%d", cfg.TCPListenAddress, cfg.TCPPort)
		tcpListener, err := net.Listen("tcp", tcpEndpoint)
		if err != nil {
			return fmt.Errorf("could not start TCP Listener: %w", err)
		}
		go func() {
			defer tcpListener.Close()
			err := srv.ServeListener(tcpListener)
			if err != nil {
				logger.Error("TCP Listener Fatal Error", "err", err)
			}
		}()
		logger.Info("TCP Endpoint opened", "url", tcpEndpoint)
	}

	info := []interface{}{
		"url", httpAddr, "ws", cfg.WebsocketEnabled,
		"ws.compression", cfg.WebsocketCompression, "grpc", cfg.GRPCServerEnabled,
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

	logger.Info("HTTP endpoint opened", info...)

	defer func() {
		srv.Stop()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = listener.Shutdown(shutdownCtx)
		logger.Info("HTTP endpoint closed", "url", httpAddr)

		if cfg.GRPCServerEnabled {
			if cfg.GRPCHealthCheckEnabled {
				healthServer.Shutdown()
			}
			grpcServer.GracefulStop()
			_ = grpcListener.Close()
			logger.Info("GRPC endpoint closed", "url", grpcEndpoint)
		}
	}()
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

func startAuthenticatedRpcServer(cfg httpcfg.HttpCfg, rpcAPI []rpc.API, logger log.Logger) (*engineInfo, error) {
	srv := rpc.NewServer(cfg.RpcBatchConcurrency, cfg.TraceRequests, cfg.RpcStreamingDisable, logger)

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

// obtainJWTSecret loads the jwt-secret, either from the provided config,
// or from the default location. If neither of those are present, it generates
// a new secret and stores to the default location.
func obtainJWTSecret(cfg httpcfg.HttpCfg, logger log.Logger) ([]byte, error) {
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

func createHandler(cfg httpcfg.HttpCfg, apiList []rpc.API, httpHandler http.Handler, wsHandler http.Handler, graphQLHandler http.Handler, jwtSecret []byte) (http.Handler, error) {
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

func createEngineListener(cfg httpcfg.HttpCfg, engineApi []rpc.API, logger log.Logger) (*http.Server, *rpc.Server, string, error) {
	engineHttpEndpoint := fmt.Sprintf("%s:%d", cfg.AuthRpcHTTPListenAddress, cfg.AuthRpcPort)

	engineSrv := rpc.NewServer(cfg.RpcBatchConcurrency, cfg.TraceRequests, true, logger)

	if err := node.RegisterApisFromWhitelist(engineApi, nil, engineSrv, true, logger); err != nil {
		return nil, nil, "", fmt.Errorf("could not start register RPC engine api: %w", err)
	}

	jwtSecret, err := obtainJWTSecret(cfg, logger)
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

	engineListener, engineAddr, err := node.StartHTTPEndpoint(engineHttpEndpoint, cfg.AuthRpcTimeouts, engineApiHandler)
	if err != nil {
		return nil, nil, "", fmt.Errorf("could not start RPC api: %w", err)
	}

	engineInfo := []interface{}{"url", engineAddr, "ws", true, "ws.compression", cfg.WebsocketCompression}
	logger.Info("HTTP endpoint opened for Engine API", engineInfo...)

	return engineListener, engineSrv, engineAddr.String(), nil
}

type remoteConsensusEngine struct {
	engine consensus.EngineReader
}

func (e *remoteConsensusEngine) HasEngine() bool {
	return e.engine != nil
}

func (e *remoteConsensusEngine) Engine() consensus.EngineReader {
	return e.engine
}

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

		e.engine = bor.NewRo(cc, borKv, blockReader,
			span.NewChainSpanner(contract.ValidatorSet(), cc, true, logger),
			contract.NewGenesisContractsClient(cc, cc.Bor.ValidatorContract, cc.Bor.StateReceiverContract, logger), logger)
	} else {
		e.engine = ethash.NewFaker()
	}

	return true
}

func (e *remoteConsensusEngine) Author(header *types.Header) (libcommon.Address, error) {
	if e.engine != nil {
		return e.engine.Author(header)
	}

	return libcommon.Address{}, fmt.Errorf("remote consensus engine not iinitialized")
}

func (e *remoteConsensusEngine) IsServiceTransaction(sender libcommon.Address, syscall consensus.SystemCall) bool {
	if e.engine != nil {
		return e.engine.IsServiceTransaction(sender, syscall)
	}

	return false
}

func (e *remoteConsensusEngine) Type() chain.ConsensusName {
	if e.engine != nil {
		return e.engine.Type()
	}

	return ""
}

func (e *remoteConsensusEngine) CalculateRewards(config *chain.Config, header *types.Header, uncles []*types.Header, syscall consensus.SystemCall) ([]consensus.Reward, error) {
	if e.engine != nil {
		return e.engine.CalculateRewards(config, header, uncles, syscall)
	}

	return nil, fmt.Errorf("remote consensus engine not iinitialized")
}

func (e *remoteConsensusEngine) Close() error {
	if e.engine != nil {
		return e.engine.Close()
	}

	return nil
}
