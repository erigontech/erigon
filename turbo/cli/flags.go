package cli

import (
	"fmt"
	"time"

	"github.com/ledgerwatch/erigon-lib/txpool/txpoolcfg"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/rpc/rpccfg"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/pflag"
	"github.com/urfave/cli/v2"

	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/node/nodecfg"
)

var (
	DatabaseVerbosityFlag = cli.IntFlag{
		Name:  "database.verbosity",
		Usage: "Enabling internal db logs. Very high verbosity levels may require recompile db. Default: 2, means warning.",
		Value: 2,
	}
	BatchSizeFlag = cli.StringFlag{
		Name:  "batchSize",
		Usage: "Batch size for the execution stage",
		Value: "256M",
	}
	EtlBufferSizeFlag = cli.StringFlag{
		Name:  "etl.bufferSize",
		Usage: "Buffer size for ETL operations.",
		Value: etl.BufferOptimalSize.String(),
	}
	BodyCacheLimitFlag = cli.StringFlag{
		Name:  "bodies.cache",
		Usage: "Limit on the cache for block bodies",
		Value: fmt.Sprintf("%d", ethconfig.Defaults.Sync.BodyCacheLimit),
	}

	PrivateApiAddr = cli.StringFlag{
		Name:  "private.api.addr",
		Usage: "Erigon's components (txpool, rpcdaemon, sentry, downloader, ...) can be deployed as independent Processes on same/another server. Then components will connect to erigon by this internal grpc API. example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface",
		Value: "127.0.0.1:9090",
	}

	PrivateApiRateLimit = cli.IntFlag{
		Name:  "private.api.ratelimit",
		Usage: "Amount of requests server handle simultaneously - requests over this limit will wait. Increase it - if clients see 'request timeout' while server load is low - it means your 'hot data' is small or have much RAM. ",
		Value: kv.ReadersLimit - 128,
	}

	PruneFlag = cli.StringFlag{
		Name: "prune",
		Usage: `Choose which ancient data delete from DB:
	h - prune history (ChangeSets, HistoryIndices - used by historical state access, like eth_getStorageAt, eth_getBalanceAt, debug_traceTransaction, trace_block, trace_transaction, etc.)
	r - prune receipts (Receipts, Logs, LogTopicIndex, LogAddressIndex - used by eth_getLogs and similar RPC methods)
	t - prune transaction by it's hash index
	c - prune call traces (used by trace_filter method)
	Does delete data older than 90K blocks, --prune=h is shortcut for: --prune.h.older=90000.
	Similarly, --prune=t is shortcut for: --prune.t.older=90000 and --prune=c is shortcut for: --prune.c.older=90000.
	However, --prune=r means to prune receipts before the Beacon Chain genesis (Consensus Layer might need receipts after that).
	If an item is NOT on the list - means NO pruning for this data.
	Example: --prune=htc`,
		Value: "disabled",
	}
	PruneHistoryFlag = cli.Uint64Flag{
		Name:  "prune.h.older",
		Usage: `Prune data older than this number of blocks from the tip of the chain (if --prune flag has 'h', then default is 90K)`,
	}
	PruneReceiptFlag = cli.Uint64Flag{
		Name:  "prune.r.older",
		Usage: `Prune data older than this number of blocks from the tip of the chain`,
	}
	PruneTxIndexFlag = cli.Uint64Flag{
		Name:  "prune.t.older",
		Usage: `Prune data older than this number of blocks from the tip of the chain (if --prune flag has 't', then default is 90K)`,
	}
	PruneCallTracesFlag = cli.Uint64Flag{
		Name:  "prune.c.older",
		Usage: `Prune data older than this number of blocks from the tip of the chain (if --prune flag has 'c', then default is 90K)`,
	}

	PruneHistoryBeforeFlag = cli.Uint64Flag{
		Name:  "prune.h.before",
		Usage: `Prune data before this block`,
	}
	PruneReceiptBeforeFlag = cli.Uint64Flag{
		Name:  "prune.r.before",
		Usage: `Prune data before this block`,
	}
	PruneTxIndexBeforeFlag = cli.Uint64Flag{
		Name:  "prune.t.before",
		Usage: `Prune data before this block`,
	}
	PruneCallTracesBeforeFlag = cli.Uint64Flag{
		Name:  "prune.c.before",
		Usage: `Prune data before this block`,
	}

	ExperimentsFlag = cli.StringFlag{
		Name: "experiments",
		Usage: `Enable some experimental stages:
* tevm - write TEVM translated code to the DB`,
		Value: "default",
	}

	// mTLS flags
	TLSFlag = cli.BoolFlag{
		Name:  "tls",
		Usage: "Enable TLS handshake",
	}
	TLSCertFlag = cli.StringFlag{
		Name:  "tls.cert",
		Usage: "Specify certificate",
		Value: "",
	}
	TLSKeyFlag = cli.StringFlag{
		Name:  "tls.key",
		Usage: "Specify key file",
		Value: "",
	}
	TLSCACertFlag = cli.StringFlag{
		Name:  "tls.cacert",
		Usage: "Specify certificate authority",
		Value: "",
	}
	StateStreamDisableFlag = cli.BoolFlag{
		Name:  "state.stream.disable",
		Usage: "Disable streaming of state changes from core to RPC daemon",
	}

	// Throttling Flags
	SyncLoopThrottleFlag = cli.StringFlag{
		Name:  "sync.loop.throttle",
		Usage: "Sets the minimum time between sync loop starts (e.g. 1h30m, default is none)",
		Value: "",
	}

	BadBlockFlag = cli.StringFlag{
		Name:  "bad.block",
		Usage: "Marks block with given hex string as bad and forces initial reorg before normal staged sync",
		Value: "",
	}

	HealthCheckFlag = cli.BoolFlag{
		Name:  "healthcheck",
		Usage: "Enable grpc health check",
	}

	HTTPReadTimeoutFlag = cli.DurationFlag{
		Name:  "http.timeouts.read",
		Usage: "Maximum duration for reading the entire request, including the body.",
		Value: rpccfg.DefaultHTTPTimeouts.ReadTimeout,
	}
	HTTPWriteTimeoutFlag = cli.DurationFlag{
		Name:  "http.timeouts.write",
		Usage: "Maximum duration before timing out writes of the response. It is reset whenever a new request's header is read.",
		Value: rpccfg.DefaultHTTPTimeouts.WriteTimeout,
	}
	HTTPIdleTimeoutFlag = cli.DurationFlag{
		Name:  "http.timeouts.idle",
		Usage: "Maximum amount of time to wait for the next request when keep-alives are enabled. If http.timeouts.idle is zero, the value of http.timeouts.read is used.",
		Value: rpccfg.DefaultHTTPTimeouts.IdleTimeout,
	}

	AuthRpcReadTimeoutFlag = cli.DurationFlag{
		Name:  "authrpc.timeouts.read",
		Usage: "Maximum duration for reading the entire request, including the body.",
		Value: rpccfg.DefaultHTTPTimeouts.ReadTimeout,
	}
	AuthRpcWriteTimeoutFlag = cli.DurationFlag{
		Name:  "authrpc.timeouts.write",
		Usage: "Maximum duration before timing out writes of the response. It is reset whenever a new request's header is read.",
		Value: rpccfg.DefaultHTTPTimeouts.WriteTimeout,
	}
	AuthRpcIdleTimeoutFlag = cli.DurationFlag{
		Name:  "authrpc.timeouts.idle",
		Usage: "Maximum amount of time to wait for the next request when keep-alives are enabled. If authrpc.timeouts.idle is zero, the value of authrpc.timeouts.read is used.",
		Value: rpccfg.DefaultHTTPTimeouts.IdleTimeout,
	}

	EvmCallTimeoutFlag = cli.DurationFlag{
		Name:  "rpc.evmtimeout",
		Usage: "Maximum amount of time to wait for the answer from EVM call.",
		Value: rpccfg.DefaultEvmCallTimeout,
	}

	TxPoolCommitEvery = cli.DurationFlag{
		Name:  "txpool.commit.every",
		Usage: "How often transactions should be committed to the storage",
		Value: txpoolcfg.DefaultConfig.CommitEvery,
	}
)

func ApplyFlagsForEthConfig(ctx *cli.Context, cfg *ethconfig.Config) {
	mode, err := prune.FromCli(
		cfg.Genesis.Config.ChainID.Uint64(),
		ctx.String(PruneFlag.Name),
		ctx.Uint64(PruneHistoryFlag.Name),
		ctx.Uint64(PruneReceiptFlag.Name),
		ctx.Uint64(PruneTxIndexFlag.Name),
		ctx.Uint64(PruneCallTracesFlag.Name),
		ctx.Uint64(PruneHistoryBeforeFlag.Name),
		ctx.Uint64(PruneReceiptBeforeFlag.Name),
		ctx.Uint64(PruneTxIndexBeforeFlag.Name),
		ctx.Uint64(PruneCallTracesBeforeFlag.Name),
		utils.SplitAndTrim(ctx.String(ExperimentsFlag.Name)),
	)
	if err != nil {
		utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
	}
	cfg.Prune = mode
	if ctx.String(BatchSizeFlag.Name) != "" {
		err := cfg.BatchSize.UnmarshalText([]byte(ctx.String(BatchSizeFlag.Name)))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
	}

	if ctx.String(EtlBufferSizeFlag.Name) != "" {
		sizeVal := datasize.ByteSize(0)
		size := &sizeVal
		err := size.UnmarshalText([]byte(ctx.String(EtlBufferSizeFlag.Name)))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
		etl.BufferOptimalSize = *size
	}

	cfg.StateStream = !ctx.Bool(StateStreamDisableFlag.Name)
	if ctx.String(BodyCacheLimitFlag.Name) != "" {
		err := cfg.Sync.BodyCacheLimit.UnmarshalText([]byte(ctx.String(BodyCacheLimitFlag.Name)))
		if err != nil {
			utils.Fatalf("Invalid bodyCacheLimit provided: %v", err)
		}
	}

	if ctx.String(SyncLoopThrottleFlag.Name) != "" {
		syncLoopThrottle, err := time.ParseDuration(ctx.String(SyncLoopThrottleFlag.Name))
		if err != nil {
			utils.Fatalf("Invalid time duration provided in %s: %v", SyncLoopThrottleFlag.Name, err)
		}
		cfg.Sync.LoopThrottle = syncLoopThrottle
	}

	if ctx.String(BadBlockFlag.Name) != "" {
		bytes, err := hexutil.Decode(ctx.String(BadBlockFlag.Name))
		if err != nil {
			log.Warn("Error decoding block hash", "hash", ctx.String(BadBlockFlag.Name), "err", err)
		} else {
			cfg.BadBlockHash = libcommon.BytesToHash(bytes)
		}
	}

	disableIPV6 := ctx.Bool(utils.DisableIPV6.Name)
	disableIPV4 := ctx.Bool(utils.DisableIPV4.Name)
	downloadRate := ctx.String(utils.TorrentDownloadRateFlag.Name)
	uploadRate := ctx.String(utils.TorrentUploadRateFlag.Name)

	log.Info("[Downloader] Runnning with", "ipv6-enabled", !disableIPV6, "ipv4-enabled", !disableIPV4, "download.rate", downloadRate, "upload.rate", uploadRate)
	if ctx.Bool(utils.DisableIPV6.Name) {
		cfg.Downloader.ClientConfig.DisableIPv6 = true
	}

	if ctx.Bool(utils.DisableIPV4.Name) {
		cfg.Downloader.ClientConfig.DisableIPv4 = true
	}
}

func ApplyFlagsForEthConfigCobra(f *pflag.FlagSet, cfg *ethconfig.Config) {
	if v := f.String(PruneFlag.Name, PruneFlag.Value, PruneFlag.Usage); v != nil {
		var experiments []string
		if exp := f.StringSlice(ExperimentsFlag.Name, nil, ExperimentsFlag.Usage); exp != nil {
			experiments = *exp
		}
		var exactH, exactR, exactT, exactC uint64
		if v := f.Uint64(PruneHistoryFlag.Name, PruneHistoryFlag.Value, PruneHistoryFlag.Usage); v != nil {
			exactH = *v
		}
		if v := f.Uint64(PruneReceiptFlag.Name, PruneReceiptFlag.Value, PruneReceiptFlag.Usage); v != nil {
			exactR = *v
		}
		if v := f.Uint64(PruneTxIndexFlag.Name, PruneTxIndexFlag.Value, PruneTxIndexFlag.Usage); v != nil {
			exactT = *v
		}
		if v := f.Uint64(PruneCallTracesFlag.Name, PruneCallTracesFlag.Value, PruneCallTracesFlag.Usage); v != nil {
			exactC = *v
		}

		var beforeH, beforeR, beforeT, beforeC uint64
		if v := f.Uint64(PruneHistoryBeforeFlag.Name, PruneHistoryBeforeFlag.Value, PruneHistoryBeforeFlag.Usage); v != nil {
			beforeH = *v
		}
		if v := f.Uint64(PruneReceiptBeforeFlag.Name, PruneReceiptBeforeFlag.Value, PruneReceiptBeforeFlag.Usage); v != nil {
			beforeR = *v
		}
		if v := f.Uint64(PruneTxIndexBeforeFlag.Name, PruneTxIndexBeforeFlag.Value, PruneTxIndexBeforeFlag.Usage); v != nil {
			beforeT = *v
		}
		if v := f.Uint64(PruneCallTracesBeforeFlag.Name, PruneCallTracesBeforeFlag.Value, PruneCallTracesBeforeFlag.Usage); v != nil {
			beforeC = *v
		}

		mode, err := prune.FromCli(cfg.Genesis.Config.ChainID.Uint64(), *v, exactH, exactR, exactT, exactC, beforeH, beforeR, beforeT, beforeC, experiments)
		if err != nil {
			utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
		}
		cfg.Prune = mode
	}
	if v := f.String(BatchSizeFlag.Name, BatchSizeFlag.Value, BatchSizeFlag.Usage); v != nil {
		err := cfg.BatchSize.UnmarshalText([]byte(*v))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
	}
	if v := f.String(EtlBufferSizeFlag.Name, EtlBufferSizeFlag.Value, EtlBufferSizeFlag.Usage); v != nil {
		sizeVal := datasize.ByteSize(0)
		size := &sizeVal
		err := size.UnmarshalText([]byte(*v))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
		etl.BufferOptimalSize = *size
	}

	cfg.StateStream = true
	if v := f.Bool(StateStreamDisableFlag.Name, false, StateStreamDisableFlag.Usage); v != nil {
		cfg.StateStream = false
	}
}

func ApplyFlagsForNodeConfig(ctx *cli.Context, cfg *nodecfg.Config) {
	setPrivateApi(ctx, cfg)
	setEmbeddedRpcDaemon(ctx, cfg)
	cfg.DatabaseVerbosity = kv.DBVerbosityLvl(ctx.Int(DatabaseVerbosityFlag.Name))
}

func setEmbeddedRpcDaemon(ctx *cli.Context, cfg *nodecfg.Config) {
	jwtSecretPath := ctx.String(utils.JWTSecretPath.Name)
	if jwtSecretPath == "" {
		jwtSecretPath = cfg.Dirs.DataDir + "/jwt.hex"
	}

	apis := ctx.String(utils.HTTPApiFlag.Name)
	log.Info("starting HTTP APIs", "APIs", apis)

	c := &httpcfg.HttpCfg{
		Enabled: ctx.Bool(utils.HTTPEnabledFlag.Name),
		Dirs:    cfg.Dirs,

		TLSKeyFile:  cfg.TLSKeyFile,
		TLSCACert:   cfg.TLSCACert,
		TLSCertfile: cfg.TLSCertFile,

		GraphQLEnabled:           ctx.Bool(utils.GraphQLEnabledFlag.Name),
		HttpListenAddress:        ctx.String(utils.HTTPListenAddrFlag.Name),
		HttpPort:                 ctx.Int(utils.HTTPPortFlag.Name),
		AuthRpcHTTPListenAddress: ctx.String(utils.AuthRpcAddr.Name),
		AuthRpcPort:              ctx.Int(utils.AuthRpcPort.Name),
		JWTSecretPath:            jwtSecretPath,
		TraceRequests:            ctx.Bool(utils.HTTPTraceFlag.Name),
		HttpCORSDomain:           utils.SplitAndTrim(ctx.String(utils.HTTPCORSDomainFlag.Name)),
		HttpVirtualHost:          utils.SplitAndTrim(ctx.String(utils.HTTPVirtualHostsFlag.Name)),
		AuthRpcVirtualHost:       utils.SplitAndTrim(ctx.String(utils.AuthRpcVirtualHostsFlag.Name)),
		API:                      utils.SplitAndTrim(apis),
		HTTPTimeouts: rpccfg.HTTPTimeouts{
			ReadTimeout:  ctx.Duration(HTTPReadTimeoutFlag.Name),
			WriteTimeout: ctx.Duration(HTTPWriteTimeoutFlag.Name),
			IdleTimeout:  ctx.Duration(HTTPIdleTimeoutFlag.Name),
		},
		AuthRpcTimeouts: rpccfg.HTTPTimeouts{
			ReadTimeout:  ctx.Duration(AuthRpcReadTimeoutFlag.Name),
			WriteTimeout: ctx.Duration(AuthRpcWriteTimeoutFlag.Name),
			IdleTimeout:  ctx.Duration(HTTPIdleTimeoutFlag.Name),
		},
		EvmCallTimeout: ctx.Duration(EvmCallTimeoutFlag.Name),

		WebsocketEnabled:     ctx.IsSet(utils.WSEnabledFlag.Name),
		RpcBatchConcurrency:  ctx.Uint(utils.RpcBatchConcurrencyFlag.Name),
		RpcStreamingDisable:  ctx.Bool(utils.RpcStreamingDisableFlag.Name),
		DBReadConcurrency:    ctx.Int(utils.DBReadConcurrencyFlag.Name),
		RpcAllowListFilePath: ctx.String(utils.RpcAccessListFlag.Name),
		Gascap:               ctx.Uint64(utils.RpcGasCapFlag.Name),
		MaxTraces:            ctx.Uint64(utils.TraceMaxtracesFlag.Name),
		TraceCompatibility:   ctx.Bool(utils.RpcTraceCompatFlag.Name),
		BatchLimit:           ctx.Int(utils.RpcBatchLimit.Name),
		ReturnDataLimit:      ctx.Int(utils.RpcReturnDataLimit.Name),

		TxPoolApiAddr: ctx.String(utils.TxpoolApiAddrFlag.Name),

		StateCache: kvcache.DefaultCoherentConfig,
	}
	if ctx.IsSet(utils.HttpCompressionFlag.Name) {
		c.HttpCompression = ctx.Bool(utils.HttpCompressionFlag.Name)
	} else {
		c.HttpCompression = true
	}
	if ctx.IsSet(utils.WsCompressionFlag.Name) {
		c.WebsocketCompression = ctx.Bool(utils.WsCompressionFlag.Name)
	} else {
		c.WebsocketCompression = true
	}

	err := c.StateCache.CacheSize.UnmarshalText([]byte(ctx.String(utils.StateCacheFlag.Name)))
	if err != nil {
		utils.Fatalf("Invalid state.cache value provided")
	}

	err = c.StateCache.CodeCacheSize.UnmarshalText([]byte(ctx.String(utils.StateCacheFlag.Name)))
	if err != nil {
		utils.Fatalf("Invalid state.cache value provided")
	}

	/*
		rootCmd.PersistentFlags().BoolVar(&cfg.GRPCServerEnabled, "grpc", false, "Enable GRPC server")
		rootCmd.PersistentFlags().StringVar(&cfg.GRPCListenAddress, "grpc.addr", node.DefaultGRPCHost, "GRPC server listening interface")
		rootCmd.PersistentFlags().IntVar(&cfg.GRPCPort, "grpc.port", node.DefaultGRPCPort, "GRPC server listening port")
		rootCmd.PersistentFlags().BoolVar(&cfg.GRPCHealthCheckEnabled, "grpc.healthcheck", false, "Enable GRPC health check")
	*/
	cfg.Http = *c
}

// setPrivateApi populates configuration fields related to the remote
// read-only interface to the database
func setPrivateApi(ctx *cli.Context, cfg *nodecfg.Config) {
	cfg.PrivateApiAddr = ctx.String(PrivateApiAddr.Name)
	cfg.PrivateApiRateLimit = uint32(ctx.Uint64(PrivateApiRateLimit.Name))
	maxRateLimit := uint32(kv.ReadersLimit - 128) // leave some readers for P2P
	if cfg.PrivateApiRateLimit > maxRateLimit {
		log.Warn("private.api.ratelimit is too big", "force", maxRateLimit)
		cfg.PrivateApiRateLimit = maxRateLimit
	}
	if ctx.Bool(TLSFlag.Name) {
		certFile := ctx.String(TLSCertFlag.Name)
		keyFile := ctx.String(TLSKeyFlag.Name)
		if certFile == "" {
			log.Warn("Could not establish TLS grpc: missing certificate")
			return
		} else if keyFile == "" {
			log.Warn("Could not establish TLS grpc: missing key file")
			return
		}
		cfg.TLSConnection = true
		cfg.TLSCertFile = certFile
		cfg.TLSKeyFile = keyFile
		cfg.TLSCACert = ctx.String(TLSCACertFlag.Name)
	}
	cfg.HealthCheck = ctx.Bool(HealthCheckFlag.Name)
}
