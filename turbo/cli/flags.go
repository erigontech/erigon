package cli

import (
	"fmt"
	"strings"
	"time"

	"github.com/ledgerwatch/erigon/rpc/rpccfg"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/etl"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/ethdb/prune"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/pflag"
	"github.com/urfave/cli"
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
	BlockDownloaderWindowFlag = cli.IntFlag{
		Name:  "blockDownloaderWindow",
		Usage: "Outstanding limit of block bodies being downloaded",
		Value: ethconfig.Defaults.Sync.BlockDownloaderWindow,
	}

	PrivateApiAddr = cli.StringFlag{
		Name:  "private.api.addr",
		Usage: "private api network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface",
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
	Does delete data older than 90K blocks, --prune=h is shortcut for: --prune.h.older=90_000 
	If item is NOT in the list - means NO pruning for this data.
	Example: --prune=hrtc`,
		Value: "disabled",
	}
	PruneHistoryFlag = cli.Uint64Flag{
		Name:  "prune.h.older",
		Usage: `Prune data after this amount of blocks (if --prune flag has 'h', then default is 90K)`,
	}
	PruneReceiptFlag = cli.Uint64Flag{
		Name:  "prune.r.older",
		Usage: `Prune data after this amount of blocks (if --prune flag has 'r', then default is 90K)`,
	}
	PruneTxIndexFlag = cli.Uint64Flag{
		Name:  "prune.t.older",
		Usage: `Prune data after this amount of blocks (if --prune flag has 't', then default is 90K)`,
	}
	PruneCallTracesFlag = cli.Uint64Flag{
		Name:  "prune.c.older",
		Usage: `Prune data after this amount of blocks (if --prune flag has 'c', then default is 90K)`,
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
)

func ApplyFlagsForEthConfig(ctx *cli.Context, cfg *ethconfig.Config) {
	mode, err := prune.FromCli(
		ctx.GlobalString(PruneFlag.Name),
		ctx.GlobalUint64(PruneHistoryFlag.Name),
		ctx.GlobalUint64(PruneReceiptFlag.Name),
		ctx.GlobalUint64(PruneTxIndexFlag.Name),
		ctx.GlobalUint64(PruneCallTracesFlag.Name),
		ctx.GlobalUint64(PruneHistoryBeforeFlag.Name),
		ctx.GlobalUint64(PruneReceiptBeforeFlag.Name),
		ctx.GlobalUint64(PruneTxIndexBeforeFlag.Name),
		ctx.GlobalUint64(PruneCallTracesBeforeFlag.Name),
		strings.Split(ctx.GlobalString(ExperimentsFlag.Name), ","),
	)
	if err != nil {
		utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
	}
	cfg.Prune = mode
	if ctx.GlobalString(BatchSizeFlag.Name) != "" {
		err := cfg.BatchSize.UnmarshalText([]byte(ctx.GlobalString(BatchSizeFlag.Name)))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
	}

	if ctx.GlobalString(EtlBufferSizeFlag.Name) != "" {
		sizeVal := datasize.ByteSize(0)
		size := &sizeVal
		err := size.UnmarshalText([]byte(ctx.GlobalString(EtlBufferSizeFlag.Name)))
		if err != nil {
			utils.Fatalf("Invalid batchSize provided: %v", err)
		}
		etl.BufferOptimalSize = *size
	}

	cfg.StateStream = !ctx.GlobalBool(StateStreamDisableFlag.Name)
	cfg.Sync.BlockDownloaderWindow = ctx.GlobalInt(BlockDownloaderWindowFlag.Name)

	if ctx.GlobalString(SyncLoopThrottleFlag.Name) != "" {
		syncLoopThrottle, err := time.ParseDuration(ctx.GlobalString(SyncLoopThrottleFlag.Name))
		if err != nil {
			utils.Fatalf("Invalid time duration provided in %s: %v", SyncLoopThrottleFlag.Name, err)
		}
		cfg.Sync.LoopThrottle = syncLoopThrottle
	}

	if ctx.GlobalString(BadBlockFlag.Name) != "" {
		bytes, err := hexutil.Decode(ctx.GlobalString(BadBlockFlag.Name))
		if err != nil {
			log.Warn("Error decoding block hash", "hash", ctx.GlobalString(BadBlockFlag.Name), "err", err)
		} else {
			cfg.BadBlockHash = common.BytesToHash(bytes)
		}
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

		mode, err := prune.FromCli(*v, exactH, exactR, exactT, exactC, beforeH, beforeR, beforeT, beforeC, experiments)
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
	cfg.DatabaseVerbosity = kv.DBVerbosityLvl(ctx.GlobalInt(DatabaseVerbosityFlag.Name))
}

func setEmbeddedRpcDaemon(ctx *cli.Context, cfg *nodecfg.Config) {
	jwtSecretPath := ctx.GlobalString(utils.JWTSecretPath.Name)
	if jwtSecretPath == "" {
		jwtSecretPath = cfg.Dirs.DataDir + "/jwt.hex"
	}

	apis := ctx.GlobalString(utils.HTTPApiFlag.Name)
	log.Info("starting HTTP APIs", "APIs", apis)

	c := &httpcfg.HttpCfg{
		Enabled: ctx.GlobalBool(utils.HTTPEnabledFlag.Name),
		Dirs:    cfg.Dirs,

		TLSKeyFile:  cfg.TLSKeyFile,
		TLSCACert:   cfg.TLSCACert,
		TLSCertfile: cfg.TLSCertFile,

		HttpListenAddress:        ctx.GlobalString(utils.HTTPListenAddrFlag.Name),
		HttpPort:                 ctx.GlobalInt(utils.HTTPPortFlag.Name),
		AuthRpcHTTPListenAddress: ctx.GlobalString(utils.AuthRpcAddr.Name),
		AuthRpcPort:              ctx.GlobalInt(utils.AuthRpcPort.Name),
		JWTSecretPath:            jwtSecretPath,
		TraceRequests:            ctx.GlobalBool(utils.HTTPTraceFlag.Name),
		HttpCORSDomain:           strings.Split(ctx.GlobalString(utils.HTTPCORSDomainFlag.Name), ","),
		HttpVirtualHost:          strings.Split(ctx.GlobalString(utils.HTTPVirtualHostsFlag.Name), ","),
		AuthRpcVirtualHost:       strings.Split(ctx.GlobalString(utils.AuthRpcVirtualHostsFlag.Name), ","),
		API:                      strings.Split(apis, ","),
		HTTPTimeouts: rpccfg.HTTPTimeouts{
			ReadTimeout:  ctx.GlobalDuration(HTTPReadTimeoutFlag.Name),
			WriteTimeout: ctx.GlobalDuration(HTTPWriteTimeoutFlag.Name),
			IdleTimeout:  ctx.GlobalDuration(HTTPIdleTimeoutFlag.Name),
		},
		AuthRpcTimeouts: rpccfg.HTTPTimeouts{
			ReadTimeout:  ctx.GlobalDuration(AuthRpcReadTimeoutFlag.Name),
			WriteTimeout: ctx.GlobalDuration(AuthRpcWriteTimeoutFlag.Name),
			IdleTimeout:  ctx.GlobalDuration(HTTPIdleTimeoutFlag.Name),
		},

		WebsocketEnabled:     ctx.GlobalIsSet(utils.WSEnabledFlag.Name),
		RpcBatchConcurrency:  ctx.GlobalUint(utils.RpcBatchConcurrencyFlag.Name),
		RpcStreamingDisable:  ctx.GlobalBool(utils.RpcStreamingDisableFlag.Name),
		DBReadConcurrency:    ctx.GlobalInt(utils.DBReadConcurrencyFlag.Name),
		RpcAllowListFilePath: ctx.GlobalString(utils.RpcAccessListFlag.Name),
		Gascap:               ctx.GlobalUint64(utils.RpcGasCapFlag.Name),
		MaxTraces:            ctx.GlobalUint64(utils.TraceMaxtracesFlag.Name),
		TraceCompatibility:   ctx.GlobalBool(utils.RpcTraceCompatFlag.Name),
		StarknetGRPCAddress:  ctx.GlobalString(utils.StarknetGrpcAddressFlag.Name),
		TevmEnabled:          ctx.GlobalBool(utils.TevmFlag.Name),

		TxPoolApiAddr: ctx.GlobalString(utils.TxpoolApiAddrFlag.Name),

		StateCache: kvcache.DefaultCoherentConfig,
	}
	if ctx.GlobalIsSet(utils.HttpCompressionFlag.Name) {
		c.HttpCompression = ctx.GlobalBool(utils.HttpCompressionFlag.Name)
	} else {
		c.HttpCompression = true
	}
	if ctx.GlobalIsSet(utils.WsCompressionFlag.Name) {
		c.WebsocketCompression = ctx.GlobalBool(utils.WsCompressionFlag.Name)
	} else {
		c.WebsocketCompression = true
	}

	c.StateCache.CodeKeysLimit = ctx.GlobalInt(utils.StateCacheFlag.Name)

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
	cfg.PrivateApiAddr = ctx.GlobalString(PrivateApiAddr.Name)
	cfg.PrivateApiRateLimit = uint32(ctx.GlobalUint64(PrivateApiRateLimit.Name))
	maxRateLimit := uint32(kv.ReadersLimit - 128) // leave some readers for P2P
	if cfg.PrivateApiRateLimit > maxRateLimit {
		log.Warn("private.api.ratelimit is too big", "force", maxRateLimit)
		cfg.PrivateApiRateLimit = maxRateLimit
	}
	if ctx.GlobalBool(TLSFlag.Name) {
		certFile := ctx.GlobalString(TLSCertFlag.Name)
		keyFile := ctx.GlobalString(TLSKeyFlag.Name)
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
		cfg.TLSCACert = ctx.GlobalString(TLSCACertFlag.Name)
	}
	cfg.HealthCheck = ctx.GlobalBool(HealthCheckFlag.Name)
}
