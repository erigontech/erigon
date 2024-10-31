// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package cli

import (
	"fmt"
	"math"
	"time"

	"github.com/erigontech/erigon-lib/common/hexutil"
	"github.com/erigontech/erigon-lib/config3"

	"github.com/erigontech/erigon-lib/txpool/txpoolcfg"

	libcommon "github.com/erigontech/erigon-lib/common"

	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpccfg"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/pflag"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/etl"
	"github.com/erigontech/erigon-lib/kv"
	"github.com/erigontech/erigon-lib/kv/kvcache"

	"github.com/erigontech/erigon/cmd/rpcdaemon/cli/httpcfg"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/ethdb/prune"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/turbo/rpchelper"
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
		Value: "512M",
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

	PruneModeFlag = cli.StringFlag{
		Name: "prune.mode",
		Usage: `Choose a pruning preset to run onto. Available values: "archive","full","minimal".
				Archive: Keep the entire indexed database, aka. no pruning. (Pruning is flexible),
				Full: Keep only blocks and latest state (Pruning is not flexible)
				Minimal: Keep only latest state (Pruning is not flexible)`,
		Value: "archive",
	}
	PruneDistanceFlag = cli.Uint64Flag{
		Name:  "prune.distance",
		Usage: `Keep state history for the latest N blocks (default: everything)`,
	}
	PruneBlocksDistanceFlag = cli.Uint64Flag{
		Name:  "prune.distance.blocks",
		Usage: `Keep block history for the latest N blocks (default: everything)`,
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

	SyncLoopBreakAfterFlag = cli.StringFlag{
		Name:  "sync.loop.break.after",
		Usage: "Sets the last stage of the sync loop to run",
		Value: "",
	}

	SyncLoopBlockLimitFlag = cli.UintFlag{
		Name:  "sync.loop.block.limit",
		Usage: "Sets the maximum number of blocks to process per loop iteration",
		Value: 5_000,
	}

	SyncParallelStateFlushing = cli.BoolFlag{
		Name:  "sync.parallel-state-flushing",
		Usage: "Enables parallel state flushing",
		Value: true,
	}

	UploadLocationFlag = cli.StringFlag{
		Name:  "upload.location",
		Usage: "Location to upload snapshot segments to",
		Value: "",
	}

	UploadFromFlag = cli.StringFlag{
		Name:  "upload.from",
		Usage: "Blocks to upload from: number, or 'earliest' (start of the chain), 'latest' (last segment previously uploaded)",
		Value: "latest",
	}

	FrozenBlockLimitFlag = cli.UintFlag{
		Name:  "upload.snapshot.limit",
		Usage: "Sets the maximum number of snapshot blocks to hold on the local disk when uploading",
		Value: 1500000,
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

	OverlayGetLogsFlag = cli.DurationFlag{
		Name:  "rpc.overlay.getlogstimeout",
		Usage: "Maximum amount of time to wait for the answer from the overlay_getLogs call.",
		Value: rpccfg.DefaultOverlayGetLogsTimeout,
	}

	OverlayReplayBlockFlag = cli.DurationFlag{
		Name:  "rpc.overlay.replayblocktimeout",
		Usage: "Maximum amount of time to wait for the answer to replay a single block when called from an overlay_getLogs call.",
		Value: rpccfg.DefaultOverlayReplayBlockTimeout,
	}

	RpcSubscriptionFiltersMaxLogsFlag = cli.IntFlag{
		Name:  "rpc.subscription.filters.maxlogs",
		Usage: "Maximum number of logs to store per subscription.",
		Value: rpchelper.DefaultFiltersConfig.RpcSubscriptionFiltersMaxLogs,
	}
	RpcSubscriptionFiltersMaxHeadersFlag = cli.IntFlag{
		Name:  "rpc.subscription.filters.maxheaders",
		Usage: "Maximum number of block headers to store per subscription.",
		Value: rpchelper.DefaultFiltersConfig.RpcSubscriptionFiltersMaxHeaders,
	}
	RpcSubscriptionFiltersMaxTxsFlag = cli.IntFlag{
		Name:  "rpc.subscription.filters.maxtxs",
		Usage: "Maximum number of transactions to store per subscription.",
		Value: rpchelper.DefaultFiltersConfig.RpcSubscriptionFiltersMaxTxs,
	}
	RpcSubscriptionFiltersMaxAddressesFlag = cli.IntFlag{
		Name:  "rpc.subscription.filters.maxaddresses",
		Usage: "Maximum number of addresses per subscription to filter logs by.",
		Value: rpchelper.DefaultFiltersConfig.RpcSubscriptionFiltersMaxAddresses,
	}
	RpcSubscriptionFiltersMaxTopicsFlag = cli.IntFlag{
		Name:  "rpc.subscription.filters.maxtopics",
		Usage: "Maximum number of topics per subscription to filter logs by.",
		Value: rpchelper.DefaultFiltersConfig.RpcSubscriptionFiltersMaxTopics,
	}

	TxPoolCommitEvery = cli.DurationFlag{
		Name:  "txpool.commit.every",
		Usage: "How often transactions should be committed to the storage",
		Value: txpoolcfg.DefaultConfig.CommitEvery,
	}
)

func ApplyFlagsForEthConfig(ctx *cli.Context, cfg *ethconfig.Config, logger log.Logger) {
	chainId := cfg.NetworkID
	if cfg.Genesis != nil {
		chainId = cfg.Genesis.Config.ChainID.Uint64()
	}
	// Sanitize prune flag
	if ctx.String(PruneModeFlag.Name) != "archive" && (ctx.IsSet(PruneBlocksDistanceFlag.Name) || ctx.IsSet(PruneDistanceFlag.Name)) {
		utils.Fatalf("error: --prune.distance and --prune.distance.blocks are only allowed with --prune.mode=archive")
	}
	distance := ctx.Uint64(PruneDistanceFlag.Name)
	blockDistance := ctx.Uint64(PruneBlocksDistanceFlag.Name)

	if !ctx.IsSet(PruneBlocksDistanceFlag.Name) {
		blockDistance = math.MaxUint64
	}
	if !ctx.IsSet(PruneDistanceFlag.Name) {
		distance = math.MaxUint64
	}
	mode, err := prune.FromCli(
		chainId,
		distance,
		blockDistance,
		libcommon.CliString2Array(ctx.String(ExperimentsFlag.Name)),
	)
	if err != nil {
		utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
	}
	// Full mode prunes all but the latest state
	if ctx.String(PruneModeFlag.Name) == "full" {
		mode.Blocks = prune.Distance(math.MaxUint64)
		mode.History = prune.Distance(config3.DefaultPruneDistance)
	}
	// Minimal mode prunes all but the latest state including blocks
	if ctx.String(PruneModeFlag.Name) == "minimal" {
		mode.Blocks = prune.Distance(config3.DefaultPruneDistance)
		mode.History = prune.Distance(config3.DefaultPruneDistance)
	}

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

	if stage := ctx.String(SyncLoopBreakAfterFlag.Name); len(stage) > 0 {
		cfg.Sync.BreakAfterStage = stage
	}

	if limit := ctx.Uint(SyncLoopBlockLimitFlag.Name); limit > 0 {
		cfg.Sync.LoopBlockLimit = limit
	}
	cfg.Sync.ParallelStateFlushing = ctx.Bool(SyncParallelStateFlushing.Name)

	if location := ctx.String(UploadLocationFlag.Name); len(location) > 0 {
		cfg.Sync.UploadLocation = location
	}

	if blockno := ctx.String(UploadFromFlag.Name); len(blockno) > 0 {
		cfg.Sync.UploadFrom = rpc.AsBlockNumber(blockno)
	} else {
		cfg.Sync.UploadFrom = rpc.LatestBlockNumber
	}

	if limit := ctx.Uint(FrozenBlockLimitFlag.Name); limit > 0 {
		cfg.Sync.FrozenBlockLimit = uint64(limit)
	}

	if ctx.String(BadBlockFlag.Name) != "" {
		bytes, err := hexutil.Decode(ctx.String(BadBlockFlag.Name))
		if err != nil {
			logger.Warn("Error decoding block hash", "hash", ctx.String(BadBlockFlag.Name), "err", err)
		} else {
			cfg.BadBlockHash = libcommon.BytesToHash(bytes)
		}
	}

	disableIPV6 := ctx.Bool(utils.DisableIPV6.Name)
	disableIPV4 := ctx.Bool(utils.DisableIPV4.Name)
	downloadRate := ctx.String(utils.TorrentDownloadRateFlag.Name)
	uploadRate := ctx.String(utils.TorrentUploadRateFlag.Name)

	logger.Info("[Downloader] Running with", "ipv6-enabled", !disableIPV6, "ipv4-enabled", !disableIPV4, "download.rate", downloadRate, "upload.rate", uploadRate)
	if ctx.Bool(utils.DisableIPV6.Name) {
		cfg.Downloader.ClientConfig.DisableIPv6 = true
	}

	if ctx.Bool(utils.DisableIPV4.Name) {
		cfg.Downloader.ClientConfig.DisableIPv4 = true
	}

	if ctx.Bool(utils.ChaosMonkeyFlag.Name) {
		cfg.ChaosMonkey = true
	}
}

func ApplyFlagsForEthConfigCobra(f *pflag.FlagSet, cfg *ethconfig.Config) {
	pruneMode := f.String(PruneModeFlag.Name, PruneModeFlag.DefaultText, PruneModeFlag.Usage)
	pruneBlockDistance := f.Uint64(PruneBlocksDistanceFlag.Name, PruneBlocksDistanceFlag.Value, PruneBlocksDistanceFlag.Usage)
	pruneDistance := f.Uint64(PruneDistanceFlag.Name, PruneDistanceFlag.Value, PruneDistanceFlag.Usage)

	chainId := cfg.NetworkID
	if *pruneMode != "archive" && (pruneBlockDistance != nil || pruneDistance != nil) {
		utils.Fatalf("error: --prune.distance and --prune.distance.blocks are only allowed with --prune.mode=archive")
	}
	var distance, blockDistance uint64 = math.MaxUint64, math.MaxUint64
	if pruneBlockDistance != nil {
		blockDistance = *pruneBlockDistance
	}
	if pruneDistance != nil {
		distance = *pruneDistance
	}

	experiments := f.String(ExperimentsFlag.Name, ExperimentsFlag.Value, ExperimentsFlag.Usage)
	experimentsVal := ""
	if experiments != nil {
		experimentsVal = *experiments
	}
	mode, err := prune.FromCli(
		chainId,
		distance,
		blockDistance,
		libcommon.CliString2Array(experimentsVal),
	)
	if err != nil {
		utils.Fatalf(fmt.Sprintf("error while parsing mode: %v", err))
	}
	switch *pruneMode {
	case "archive":
	case "full":
		mode.Blocks = prune.Distance(math.MaxUint64)
		mode.History = prune.Distance(config3.DefaultPruneDistance)
	case "minimal":
		mode.Blocks = prune.Distance(config3.DefaultPruneDistance) // 2048 is just some blocks to allow reorgs and data for rpc
		mode.History = prune.Distance(config3.DefaultPruneDistance)
	default:
		utils.Fatalf("error: --prune.mode must be one of archive, full, minimal")
	}
	cfg.Prune = mode

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

func ApplyFlagsForNodeConfig(ctx *cli.Context, cfg *nodecfg.Config, logger log.Logger) {
	setPrivateApi(ctx, cfg)
	setEmbeddedRpcDaemon(ctx, cfg, logger)
	cfg.DatabaseVerbosity = kv.DBVerbosityLvl(ctx.Int(DatabaseVerbosityFlag.Name))
}

func setEmbeddedRpcDaemon(ctx *cli.Context, cfg *nodecfg.Config, logger log.Logger) {
	jwtSecretPath := ctx.String(utils.JWTSecretPath.Name)
	if jwtSecretPath == "" {
		jwtSecretPath = cfg.Dirs.DataDir + "/jwt.hex"
	}

	apis := ctx.String(utils.HTTPApiFlag.Name)

	c := &httpcfg.HttpCfg{
		Enabled: func() bool {
			if ctx.IsSet(utils.HTTPEnabledFlag.Name) {
				return ctx.Bool(utils.HTTPEnabledFlag.Name)
			}

			return true
		}(),
		HttpServerEnabled: ctx.Bool(utils.HTTPServerEnabledFlag.Name),
		Dirs:              cfg.Dirs,

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
		DebugSingleRequest:       ctx.Bool(utils.HTTPDebugSingleFlag.Name),
		HttpCORSDomain:           libcommon.CliString2Array(ctx.String(utils.HTTPCORSDomainFlag.Name)),
		HttpVirtualHost:          libcommon.CliString2Array(ctx.String(utils.HTTPVirtualHostsFlag.Name)),
		AuthRpcVirtualHost:       libcommon.CliString2Array(ctx.String(utils.AuthRpcVirtualHostsFlag.Name)),
		API:                      libcommon.CliString2Array(apis),
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
		EvmCallTimeout:                    ctx.Duration(EvmCallTimeoutFlag.Name),
		OverlayGetLogsTimeout:             ctx.Duration(OverlayGetLogsFlag.Name),
		OverlayReplayBlockTimeout:         ctx.Duration(OverlayReplayBlockFlag.Name),
		WebsocketPort:                     ctx.Int(utils.WSPortFlag.Name),
		WebsocketEnabled:                  ctx.IsSet(utils.WSEnabledFlag.Name),
		WebsocketSubscribeLogsChannelSize: ctx.Int(utils.WSSubscribeLogsChannelSize.Name),
		RpcBatchConcurrency:               ctx.Uint(utils.RpcBatchConcurrencyFlag.Name),
		RpcStreamingDisable:               ctx.Bool(utils.RpcStreamingDisableFlag.Name),
		DBReadConcurrency:                 ctx.Int(utils.DBReadConcurrencyFlag.Name),
		RpcAllowListFilePath:              ctx.String(utils.RpcAccessListFlag.Name),
		RpcFiltersConfig: rpchelper.FiltersConfig{
			RpcSubscriptionFiltersMaxLogs:      ctx.Int(RpcSubscriptionFiltersMaxLogsFlag.Name),
			RpcSubscriptionFiltersMaxHeaders:   ctx.Int(RpcSubscriptionFiltersMaxHeadersFlag.Name),
			RpcSubscriptionFiltersMaxTxs:       ctx.Int(RpcSubscriptionFiltersMaxTxsFlag.Name),
			RpcSubscriptionFiltersMaxAddresses: ctx.Int(RpcSubscriptionFiltersMaxAddressesFlag.Name),
			RpcSubscriptionFiltersMaxTopics:    ctx.Int(RpcSubscriptionFiltersMaxTopicsFlag.Name),
		},
		Gascap:                      ctx.Uint64(utils.RpcGasCapFlag.Name),
		Feecap:                      ctx.Float64(utils.RPCGlobalTxFeeCapFlag.Name),
		MaxTraces:                   ctx.Uint64(utils.TraceMaxtracesFlag.Name),
		TraceCompatibility:          ctx.Bool(utils.RpcTraceCompatFlag.Name),
		BatchLimit:                  ctx.Int(utils.RpcBatchLimit.Name),
		ReturnDataLimit:             ctx.Int(utils.RpcReturnDataLimit.Name),
		AllowUnprotectedTxs:         ctx.Bool(utils.AllowUnprotectedTxs.Name),
		MaxGetProofRewindBlockCount: ctx.Int(utils.RpcMaxGetProofRewindBlockCount.Name),

		OtsMaxPageSize: ctx.Uint64(utils.OtsSearchMaxCapFlag.Name),

		TxPoolApiAddr: ctx.String(utils.TxpoolApiAddrFlag.Name),

		StateCache:          kvcache.DefaultCoherentConfig,
		RPCSlowLogThreshold: ctx.Duration(utils.RPCSlowFlag.Name),
	}

	if c.Enabled {
		logger.Info("starting HTTP APIs", "port", c.HttpPort, "APIs", apis)
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
