package utils

import (
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/params/networkname"
	turboCLI "github.com/ledgerwatch/erigon/turbo/cli"
	"github.com/urfave/cli"
)

var (
	DataDirFlag = utils.DirectoryFlag{
		Name:  "datadir",
		Usage: "Data directory for the databases",
		Value: utils.DirectoryString("~/dev"),
	}
	
	ChainFlag = cli.StringFlag{
		Name:  "chain",
		Usage: "Name of the testnet to join",
		Value: networkname.DevChainName,
	}
	
	DefaultFlags = []cli.Flag{
		DataDirFlag,
		utils.EthashDatasetDirFlag,
		utils.SyncModeFlag,
		utils.TxPoolDisableFlag,
		utils.TxPoolLocalsFlag,
		utils.TxPoolNoLocalsFlag,
		utils.TxPoolJournalFlag,
		utils.TxPoolRejournalFlag,
		utils.TxPoolPriceLimitFlag,
		utils.TxPoolPriceBumpFlag,
		utils.TxPoolAccountSlotsFlag,
		utils.TxPoolGlobalSlotsFlag,
		utils.TxPoolGlobalBaseFeeSlotsFlag,
		utils.TxPoolAccountQueueFlag,
		utils.TxPoolGlobalQueueFlag,
		utils.TxPoolLifetimeFlag,
		utils.TxPoolTraceSendersFlag,
		turboCLI.PruneFlag,
		turboCLI.PruneHistoryFlag,
		turboCLI.PruneReceiptFlag,
		turboCLI.PruneTxIndexFlag,
		turboCLI.PruneCallTracesFlag,
		turboCLI.PruneHistoryBeforeFlag,
		turboCLI.PruneReceiptBeforeFlag,
		turboCLI.PruneTxIndexBeforeFlag,
		turboCLI.PruneCallTracesBeforeFlag,
		turboCLI.BatchSizeFlag,
		turboCLI.BlockDownloaderWindowFlag,
		turboCLI.DatabaseVerbosityFlag,
		turboCLI.PrivateApiAddr,
		turboCLI.PrivateApiRateLimit,
		turboCLI.EtlBufferSizeFlag,
		turboCLI.TLSFlag,
		turboCLI.TLSCertFlag,
		turboCLI.TLSKeyFlag,
		turboCLI.TLSCACertFlag,
		turboCLI.StateStreamDisableFlag,
		turboCLI.SyncLoopThrottleFlag,
		turboCLI.BadBlockFlag,

		utils.HTTPEnabledFlag,
		utils.HTTPListenAddrFlag,
		utils.HTTPPortFlag,
		utils.EngineAddr,
		utils.EnginePort,
		utils.JWTSecretPath,
		utils.HttpCompressionFlag,
		utils.HTTPCORSDomainFlag,
		utils.HTTPVirtualHostsFlag,
		utils.HTTPApiFlag,
		utils.WSEnabledFlag,
		utils.WsCompressionFlag,
		utils.StateCacheFlag,
		utils.RpcBatchConcurrencyFlag,
		utils.DBReadConcurrencyFlag,
		utils.RpcAccessListFlag,
		utils.RpcTraceCompatFlag,
		utils.RpcGasCapFlag,
		utils.StarknetGrpcAddressFlag,
		utils.TevmFlag,
		utils.TxpoolApiAddrFlag,
		utils.TraceMaxtracesFlag,

		utils.SnapshotKeepBlocksFlag,
		utils.DbPageSizeFlag,
		utils.TorrentPortFlag,
		utils.TorrentUploadRateFlag,
		utils.TorrentDownloadRateFlag,
		utils.TorrentVerbosityFlag,
		utils.ListenPortFlag,
		utils.NATFlag,
		utils.NoDiscoverFlag,
		utils.DiscoveryV5Flag,
		utils.NetrestrictFlag,
		utils.NodeKeyFileFlag,
		utils.NodeKeyHexFlag,
		utils.DNSDiscoveryFlag,
		utils.BootnodesFlag,
		utils.StaticPeersFlag,
		utils.TrustedPeersFlag,
		utils.MaxPeersFlag,
		ChainFlag,
		utils.DeveloperPeriodFlag,
		utils.VMEnableDebugFlag,
		utils.NetworkIdFlag,
		utils.FakePoWFlag,
		utils.GpoBlocksFlag,
		utils.GpoPercentileFlag,
		utils.InsecureUnlockAllowedFlag,
		utils.MetricsEnabledFlag,
		utils.MetricsEnabledExpensiveFlag,
		utils.MetricsHTTPFlag,
		utils.MetricsPortFlag,
		utils.IdentityFlag,
		utils.CliqueSnapshotCheckpointIntervalFlag,
		utils.CliqueSnapshotInmemorySnapshotsFlag,
		utils.CliqueSnapshotInmemorySignaturesFlag,
		utils.CliqueDataDirFlag,
		utils.EnabledIssuance,
		utils.MiningEnabledFlag,
		utils.ProposingDisableFlag,
		utils.MinerNotifyFlag,
		utils.MinerGasLimitFlag,
		utils.MinerEtherbaseFlag,
		utils.MinerExtraDataFlag,
		utils.MinerNoVerfiyFlag,
		utils.MinerSigningKeyFileFlag,
		utils.SentryAddrFlag,
		utils.DownloaderAddrFlag,
		turboCLI.HealthCheckFlag,
		utils.HeimdallURLFlag,
		utils.WithoutHeimdallFlag,
	}
)

type RPCFlags struct {
	WebsocketEnabled bool
}

type ErigonFlags struct {

}

var (
	verbosityFlag = cli.IntFlag{
		Name:  "verbosity",
		Usage: "Logging verbosity: 0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail",
		Value: 0,
	}
	logjsonFlag = cli.BoolFlag{
		Name:  "log.json",
		Usage: "Format logs with JSON",
	}
	//nolint
	vmoduleFlag = cli.StringFlag{
		Name:  "vmodule",
		Usage: "Per-module verbosity: comma-separated list of <pattern>=<level> (e.g. eth/*=5,p2p=4)",
		Value: "",
	}
	metricsAddrFlag = cli.StringFlag{
		Name: "metrics.addr",
	}
	metricsPortFlag = cli.UintFlag{
		Name:  "metrics.port",
		Value: 6060,
	}
	pprofFlag = cli.BoolFlag{
		Name:  "pprof",
		Usage: "Enable the pprof HTTP server",
	}
	pprofPortFlag = cli.IntFlag{
		Name:  "pprof.port",
		Usage: "pprof HTTP server listening port",
		Value: 6060,
	}
	pprofAddrFlag = cli.StringFlag{
		Name:  "pprof.addr",
		Usage: "pprof HTTP server listening interface",
		Value: "127.0.0.1",
	}
	cpuprofileFlag = cli.StringFlag{
		Name:  "pprof.cpuprofile",
		Usage: "Write CPU profile to the given file",
	}
	traceFlag = cli.StringFlag{
		Name:  "trace",
		Usage: "Write execution trace to the given file",
	}
)

// DebugFlags holds all command-line flags required for debugging.
var DebugFlags = []cli.Flag{
	verbosityFlag, logjsonFlag, //backtraceAtFlag, vmoduleFlag, debugFlag,
	pprofFlag, pprofAddrFlag, pprofPortFlag,
	cpuprofileFlag, traceFlag,
}