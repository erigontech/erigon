// Copyright 2015 The go-ethereum Authors
// This file is part of go-ethereum.
//
// go-ethereum is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// go-ethereum is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU General Public License for more details.
//
// You should have received a copy of the GNU General Public License
// along with go-ethereum. If not, see <http://www.gnu.org/licenses/>.

// Package utils contains internal helper functions for go-ethereum commands.
package utils

import (
	"crypto/ecdsa"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"text/tabwriter"
	"text/template"

	"github.com/c2h5oh/datasize"
	"github.com/ledgerwatch/erigon-lib/kv"
	"github.com/ledgerwatch/erigon-lib/kv/kvcache"
	"github.com/ledgerwatch/erigon-lib/txpool"
	"github.com/ledgerwatch/log/v3"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/urfave/cli"

	"github.com/ledgerwatch/erigon/cmd/downloader/downloader/downloadercfg"
	"github.com/ledgerwatch/erigon/node/nodecfg"
	"github.com/ledgerwatch/erigon/node/nodecfg/datadir"

	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/params/networkname"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/gasprice"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/p2p"
	"github.com/ledgerwatch/erigon/p2p/enode"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/erigon/p2p/netutil"
	"github.com/ledgerwatch/erigon/params"
)

func init() {
	cli.AppHelpTemplate = `{{.Name}} {{if .Flags}}[global options] {{end}}command{{if .Flags}} [command options]{{end}} [arguments...]

VERSION:
   {{.Version}}

COMMANDS:
   {{range .Commands}}{{.Name}}{{with .ShortName}}, {{.}}{{end}}{{ "\t" }}{{.Usage}}
   {{end}}{{if .Flags}}
GLOBAL OPTIONS:
   {{range .Flags}}{{.}}
   {{end}}{{end}}
`
	cli.HelpPrinter = printHelp
}

func printHelp(out io.Writer, templ string, data interface{}) {
	funcMap := template.FuncMap{"join": strings.Join}
	t := template.Must(template.New("help").Funcs(funcMap).Parse(templ))
	w := tabwriter.NewWriter(out, 38, 8, 2, ' ', 0)
	if err := t.Execute(w, data); err != nil {
		panic(err)
	}
	w.Flush()
}

// These are all the command line flags we support.
// If you add to this list, please remember to include the
// flag in the appropriate command definition.
//
// The flags are defined here so their names and help texts
// are the same for all commands.

var (
	// General settings
	DataDirFlag = DirectoryFlag{
		Name:  "datadir",
		Usage: "Data directory for the databases",
		Value: DirectoryString(paths.DefaultDataDir()),
	}

	AncientFlag = DirectoryFlag{
		Name:  "datadir.ancient",
		Usage: "Data directory for ancient chain segments (default = inside chaindata)",
	}
	MinFreeDiskSpaceFlag = DirectoryFlag{
		Name:  "datadir.minfreedisk",
		Usage: "Minimum free disk space in MB, once reached triggers auto shut down (default = --cache.gc converted to MB, 0 = disabled)",
	}
	NetworkIdFlag = cli.Uint64Flag{
		Name:  "networkid",
		Usage: "Explicitly set network id (integer)(For testnets: use --chain <testnet_name> instead)",
		Value: ethconfig.Defaults.NetworkID,
	}
	DeveloperPeriodFlag = cli.IntFlag{
		Name:  "dev.period",
		Usage: "Block period to use in developer mode (0 = mine only if transaction pending)",
	}
	ChainFlag = cli.StringFlag{
		Name:  "chain",
		Usage: "Name of the testnet to join",
		Value: networkname.MainnetChainName,
	}
	IdentityFlag = cli.StringFlag{
		Name:  "identity",
		Usage: "Custom node name",
	}
	WhitelistFlag = cli.StringFlag{
		Name:  "whitelist",
		Usage: "Comma separated block number-to-hash mappings to enforce (<number>=<hash>)",
	}
	OverrideTerminalTotalDifficulty = BigFlag{
		Name:  "override.terminaltotaldifficulty",
		Usage: "Manually specify TerminalTotalDifficulty, overriding the bundled setting",
	}
	OverrideMergeNetsplitBlock = BigFlag{
		Name:  "override.mergeNetsplitBlock",
		Usage: "Manually specify FORK_NEXT_VALUE (see EIP-3675), overriding the bundled setting",
	}
	// Ethash settings
	EthashCachesInMemoryFlag = cli.IntFlag{
		Name:  "ethash.cachesinmem",
		Usage: "Number of recent ethash caches to keep in memory (16MB each)",
		Value: ethconfig.Defaults.Ethash.CachesInMem,
	}
	EthashCachesLockMmapFlag = cli.BoolFlag{
		Name:  "ethash.cacheslockmmap",
		Usage: "Lock memory maps of recent ethash caches",
	}
	EthashDatasetDirFlag = DirectoryFlag{
		Name:  "ethash.dagdir",
		Usage: "Directory to store the ethash mining DAGs",
		Value: DirectoryString(ethconfig.Defaults.Ethash.DatasetDir),
	}
	EthashDatasetsLockMmapFlag = cli.BoolFlag{
		Name:  "ethash.dagslockmmap",
		Usage: "Lock memory maps for recent ethash mining DAGs",
	}
	SnapshotFlag = cli.BoolTFlag{
		Name:  "snapshots",
		Usage: `Default: use snapshots "true" for BSC, Mainnet and Goerli. use snapshots "false" in all other cases`,
	}
	// Transaction pool settings
	TxPoolDisableFlag = cli.BoolFlag{
		Name:  "txpool.disable",
		Usage: "experimental external pool and block producer, see ./cmd/txpool/readme.md for more info. Disabling internal txpool and block producer.",
	}
	TxPoolLocalsFlag = cli.StringFlag{
		Name:  "txpool.locals",
		Usage: "Comma separated accounts to treat as locals (no flush, priority inclusion)",
	}
	TxPoolNoLocalsFlag = cli.BoolFlag{
		Name:  "txpool.nolocals",
		Usage: "Disables price exemptions for locally submitted transactions",
	}
	TxPoolPriceLimitFlag = cli.Uint64Flag{
		Name:  "txpool.pricelimit",
		Usage: "Minimum gas price (fee cap) limit to enforce for acceptance into the pool",
		Value: ethconfig.Defaults.DeprecatedTxPool.PriceLimit,
	}
	TxPoolPriceBumpFlag = cli.Uint64Flag{
		Name:  "txpool.pricebump",
		Usage: "Price bump percentage to replace an already existing transaction",
		Value: txpool.DefaultConfig.PriceBump,
	}
	TxPoolAccountSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.accountslots",
		Usage: "Minimum number of executable transaction slots guaranteed per account",
		Value: ethconfig.Defaults.DeprecatedTxPool.AccountSlots,
	}
	TxPoolGlobalSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.globalslots",
		Usage: "Maximum number of executable transaction slots for all accounts",
		Value: ethconfig.Defaults.DeprecatedTxPool.GlobalSlots,
	}
	TxPoolGlobalBaseFeeSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.globalbasefeeslots",
		Usage: "Maximum number of non-executable transactions where only not enough baseFee",
		Value: ethconfig.Defaults.DeprecatedTxPool.GlobalQueue,
	}
	TxPoolAccountQueueFlag = cli.Uint64Flag{
		Name:  "txpool.accountqueue",
		Usage: "Maximum number of non-executable transaction slots permitted per account",
		Value: ethconfig.Defaults.DeprecatedTxPool.AccountQueue,
	}
	TxPoolGlobalQueueFlag = cli.Uint64Flag{
		Name:  "txpool.globalqueue",
		Usage: "Maximum number of non-executable transaction slots for all accounts",
		Value: ethconfig.Defaults.DeprecatedTxPool.GlobalQueue,
	}
	TxPoolLifetimeFlag = cli.DurationFlag{
		Name:  "txpool.lifetime",
		Usage: "Maximum amount of time non-executable transaction are queued",
		Value: ethconfig.Defaults.DeprecatedTxPool.Lifetime,
	}
	TxPoolTraceSendersFlag = cli.StringFlag{
		Name:  "txpool.trace.senders",
		Usage: "Comma separared list of addresses, whoes transactions will traced in transaction pool with debug printing",
		Value: "",
	}
	EnabledIssuance = cli.BoolFlag{
		Name:  "watch-the-burn",
		Usage: "Enable WatchTheBurn stage to keep track of ETH issuance",
	}
	// Miner settings
	MiningEnabledFlag = cli.BoolFlag{
		Name:  "mine",
		Usage: "Enable mining",
	}
	ProposingDisableFlag = cli.BoolFlag{
		Name:  "proposer.disable",
		Usage: "Disables PoS proposer",
	}
	MinerNotifyFlag = cli.StringFlag{
		Name:  "miner.notify",
		Usage: "Comma separated HTTP URL list to notify of new work packages",
	}
	MinerGasLimitFlag = cli.Uint64Flag{
		Name:  "miner.gaslimit",
		Usage: "Target gas limit for mined blocks",
		Value: ethconfig.Defaults.Miner.GasLimit,
	}
	MinerGasPriceFlag = BigFlag{
		Name:  "miner.gasprice",
		Usage: "Minimum gas price for mining a transaction",
		Value: ethconfig.Defaults.Miner.GasPrice,
	}
	MinerEtherbaseFlag = cli.StringFlag{
		Name:  "miner.etherbase",
		Usage: "Public address for block mining rewards",
		Value: "0",
	}
	MinerSigningKeyFileFlag = cli.StringFlag{
		Name:  "miner.sigfile",
		Usage: "Private key to sign blocks with",
		Value: "",
	}
	MinerExtraDataFlag = cli.StringFlag{
		Name:  "miner.extradata",
		Usage: "Block extra data set by the miner (default = client version)",
	}
	MinerRecommitIntervalFlag = cli.DurationFlag{
		Name:  "miner.recommit",
		Usage: "Time interval to recreate the block being mined",
		Value: ethconfig.Defaults.Miner.Recommit,
	}
	MinerNoVerfiyFlag = cli.BoolFlag{
		Name:  "miner.noverify",
		Usage: "Disable remote sealing verification",
	}
	VMEnableDebugFlag = cli.BoolFlag{
		Name:  "vmdebug",
		Usage: "Record information useful for VM and contract debugging",
	}
	InsecureUnlockAllowedFlag = cli.BoolFlag{
		Name:  "allow-insecure-unlock",
		Usage: "Allow insecure account unlocking when account-related RPCs are exposed by http",
	}
	RPCGlobalGasCapFlag = cli.Uint64Flag{
		Name:  "rpc.gascap",
		Usage: "Sets a cap on gas that can be used in eth_call/estimateGas (0=infinite)",
		Value: ethconfig.Defaults.RPCGasCap,
	}
	RPCGlobalTxFeeCapFlag = cli.Float64Flag{
		Name:  "rpc.txfeecap",
		Usage: "Sets a cap on transaction fee (in ether) that can be sent via the RPC APIs (0 = no cap)",
		Value: ethconfig.Defaults.RPCTxFeeCap,
	}
	// Logging and debug settings
	EthStatsURLFlag = cli.StringFlag{
		Name:  "ethstats",
		Usage: "Reporting URL of a ethstats service (nodename:secret@host:port)",
		Value: "",
	}
	FakePoWFlag = cli.BoolFlag{
		Name:  "fakepow",
		Usage: "Disables proof-of-work verification",
	}
	// RPC settings
	IPCDisabledFlag = cli.BoolFlag{
		Name:  "ipcdisable",
		Usage: "Disable the IPC-RPC server",
	}
	IPCPathFlag = DirectoryFlag{
		Name:  "ipcpath",
		Usage: "Filename for IPC socket/pipe within the datadir (explicit paths escape it)",
	}
	HTTPEnabledFlag = cli.BoolTFlag{
		Name:  "http",
		Usage: "HTTP-RPC server (enabled by default). Use --http=false to disable it",
	}
	HTTPListenAddrFlag = cli.StringFlag{
		Name:  "http.addr",
		Usage: "HTTP-RPC server listening interface",
		Value: nodecfg.DefaultHTTPHost,
	}
	HTTPPortFlag = cli.IntFlag{
		Name:  "http.port",
		Usage: "HTTP-RPC server listening port",
		Value: nodecfg.DefaultHTTPPort,
	}
	AuthRpcAddr = cli.StringFlag{
		Name:  "authrpc.addr",
		Usage: "HTTP-RPC server listening interface for the Engine API",
		Value: nodecfg.DefaultHTTPHost,
	}
	AuthRpcPort = cli.UintFlag{
		Name:  "authrpc.port",
		Usage: "HTTP-RPC server listening port for the Engine API",
		Value: nodecfg.DefaultAuthRpcPort,
	}

	JWTSecretPath = cli.StringFlag{
		Name:  "authrpc.jwtsecret",
		Usage: "Path to the token that ensures safe connection between CL and EL",
		Value: "",
	}

	HttpCompressionFlag = cli.BoolFlag{
		Name:  "http.compression",
		Usage: "Enable compression over HTTP-RPC",
	}
	WsCompressionFlag = cli.BoolFlag{
		Name:  "ws.compression",
		Usage: "Enable compression over WebSocket",
	}
	HTTPCORSDomainFlag = cli.StringFlag{
		Name:  "http.corsdomain",
		Usage: "Comma separated list of domains from which to accept cross origin requests (browser enforced)",
		Value: "",
	}
	HTTPVirtualHostsFlag = cli.StringFlag{
		Name:  "http.vhosts",
		Usage: "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.",
		Value: strings.Join(nodecfg.DefaultConfig.HTTPVirtualHosts, ","),
	}
	AuthRpcVirtualHostsFlag = cli.StringFlag{
		Name:  "authrpc.vhosts",
		Usage: "Comma separated list of virtual hostnames from which to accept Engine API requests (server enforced). Accepts '*' wildcard.",
		Value: strings.Join(nodecfg.DefaultConfig.HTTPVirtualHosts, ","),
	}
	HTTPApiFlag = cli.StringFlag{
		Name:  "http.api",
		Usage: "API's offered over the HTTP-RPC interface",
		Value: "eth,erigon,engine",
	}
	RpcBatchConcurrencyFlag = cli.UintFlag{
		Name:  "rpc.batch.concurrency",
		Usage: "Does limit amount of goroutines to process 1 batch request. Means 1 bach request can't overload server. 1 batch still can have unlimited amount of request",
		Value: 2,
	}
	RpcStreamingDisableFlag = cli.BoolFlag{
		Name:  "rpc.streaming.disable",
		Usage: "Erigon has enalbed json streaming for some heavy endpoints (like trace_*). It's treadoff: greatly reduce amount of RAM (in some cases from 30GB to 30mb), but it produce invalid json format if error happened in the middle of streaming (because json is not streaming-friendly format)",
	}
	HTTPTraceFlag = cli.BoolFlag{
		Name:  "http.trace",
		Usage: "Trace HTTP requests with INFO level",
	}
	DBReadConcurrencyFlag = cli.IntFlag{
		Name:  "db.read.concurrency",
		Usage: "Does limit amount of parallel db reads. Default: equal to GOMAXPROCS (or number of CPU)",
		Value: runtime.GOMAXPROCS(-1),
	}
	RpcAccessListFlag = cli.StringFlag{
		Name:  "rpc.accessList",
		Usage: "Specify granular (method-by-method) API allowlist",
	}

	RpcGasCapFlag = cli.UintFlag{
		Name:  "rpc.gascap",
		Usage: "Sets a cap on gas that can be used in eth_call/estimateGas",
		Value: 50000000,
	}
	RpcTraceCompatFlag = cli.BoolFlag{
		Name:  "trace.compat",
		Usage: "Bug for bug compatibility with OE for trace_ routines",
	}

	StarknetGrpcAddressFlag = cli.StringFlag{
		Name:  "starknet.grpc.address",
		Usage: "Starknet GRPC address",
		Value: "127.0.0.1:6066",
	}

	TevmFlag = cli.BoolFlag{
		Name:  "experimental.tevm",
		Usage: "Enables Transpiled EVM experiment",
	}
	MemoryOverlayFlag = cli.BoolTFlag{
		Name:  "experimental.overlay",
		Usage: "Enables In-Memory Overlay for PoS",
	}
	TxpoolApiAddrFlag = cli.StringFlag{
		Name:  "txpool.api.addr",
		Usage: "txpool api network address, for example: 127.0.0.1:9090 (default: use value of --private.api.addr)",
	}

	TraceMaxtracesFlag = cli.UintFlag{
		Name:  "trace.maxtraces",
		Usage: "Sets a limit on traces that can be returned in trace_filter",
		Value: 200,
	}

	HTTPPathPrefixFlag = cli.StringFlag{
		Name:  "http.rpcprefix",
		Usage: "HTTP path path prefix on which JSON-RPC is served. Use '/' to serve on all paths.",
		Value: "",
	}
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
	WSEnabledFlag = cli.BoolFlag{
		Name:  "ws",
		Usage: "Enable the WS-RPC server",
	}
	WSListenAddrFlag = cli.StringFlag{
		Name:  "ws.addr",
		Usage: "WS-RPC server listening interface",
		Value: nodecfg.DefaultWSHost,
	}
	WSPortFlag = cli.IntFlag{
		Name:  "ws.port",
		Usage: "WS-RPC server listening port",
		Value: nodecfg.DefaultWSPort,
	}
	WSApiFlag = cli.StringFlag{
		Name:  "ws.api",
		Usage: "API's offered over the WS-RPC interface",
		Value: "",
	}
	WSAllowedOriginsFlag = cli.StringFlag{
		Name:  "ws.origins",
		Usage: "Origins from which to accept websockets requests",
		Value: "",
	}
	WSPathPrefixFlag = cli.StringFlag{
		Name:  "ws.rpcprefix",
		Usage: "HTTP path prefix on which JSON-RPC is served. Use '/' to serve on all paths.",
		Value: "",
	}
	ExecFlag = cli.StringFlag{
		Name:  "exec",
		Usage: "Execute JavaScript statement",
	}
	PreloadJSFlag = cli.StringFlag{
		Name:  "preload",
		Usage: "Comma separated list of JavaScript files to preload into the console",
	}
	AllowUnprotectedTxs = cli.BoolFlag{
		Name:  "rpc.allow-unprotected-txs",
		Usage: "Allow for unprotected (non EIP155 signed) transactions to be submitted via RPC",
	}
	StateCacheFlag = cli.IntFlag{
		Name:  "state.cache",
		Value: kvcache.DefaultCoherentConfig.KeysLimit,
		Usage: "Amount of keys to store in StateCache (enabled if no --datadir set). Set 0 to disable StateCache. 1_000_000 keys ~ equal to 2Gb RAM (maybe we will add RAM accounting in future versions).",
	}

	// Network Settings
	MaxPeersFlag = cli.IntFlag{
		Name:  "maxpeers",
		Usage: "Maximum number of network peers (network disabled if set to 0)",
		Value: nodecfg.DefaultConfig.P2P.MaxPeers,
	}
	MaxPendingPeersFlag = cli.IntFlag{
		Name:  "maxpendpeers",
		Usage: "Maximum number of TCP connections pending to become connected peers",
		Value: nodecfg.DefaultConfig.P2P.MaxPendingPeers,
	}
	ListenPortFlag = cli.IntFlag{
		Name:  "port",
		Usage: "Network listening port",
		Value: 30303,
	}
	P2pProtocolVersionFlag = cli.IntFlag{
		Name:  "p2p.protocol",
		Usage: "Version of eth p2p protocol",
		Value: int(nodecfg.DefaultConfig.P2P.ProtocolVersion),
	}
	SentryAddrFlag = cli.StringFlag{
		Name:  "sentry.api.addr",
		Usage: "comma separated sentry addresses '<host>:<port>,<host>:<port>'",
	}
	SentryLogPeerInfoFlag = cli.BoolFlag{
		Name:  "sentry.log-peer-info",
		Usage: "Log detailed peer info when a peer connects or disconnects. Enable to integrate with observer.",
	}
	DownloaderAddrFlag = cli.StringFlag{
		Name:  "downloader.api.addr",
		Usage: "downloader address '<host>:<port>'",
	}
	BootnodesFlag = cli.StringFlag{
		Name:  "bootnodes",
		Usage: "Comma separated enode URLs for P2P discovery bootstrap",
		Value: "",
	}
	StaticPeersFlag = cli.StringFlag{
		Name:  "staticpeers",
		Usage: "Comma separated enode URLs to connect to",
		Value: "",
	}
	TrustedPeersFlag = cli.StringFlag{
		Name:  "trustedpeers",
		Usage: "Comma separated enode URLs which are always allowed to connect, even above the peer limit",
		Value: "",
	}
	NodeKeyFileFlag = cli.StringFlag{
		Name:  "nodekey",
		Usage: "P2P node key file",
	}
	NodeKeyHexFlag = cli.StringFlag{
		Name:  "nodekeyhex",
		Usage: "P2P node key as hex (for testing)",
	}
	NATFlag = cli.StringFlag{
		Name: "nat",
		Usage: `NAT port mapping mechanism (any|none|upnp|pmp|stun|extip:<IP>)
	     "" or "none"         default - do not nat
	     "extip:77.12.33.4"   will assume the local machine is reachable on the given IP
	     "any"                uses the first auto-detected mechanism
	     "upnp"               uses the Universal Plug and Play protocol
	     "pmp"                uses NAT-PMP with an auto-detected gateway address
	     "pmp:192.168.0.1"    uses NAT-PMP with the given gateway address
	     "stun"               uses STUN to detect an external IP using a default server
	     "stun:<server>"      uses STUN to detect an external IP using the given server (host:port)
`,
		Value: "",
	}
	NoDiscoverFlag = cli.BoolFlag{
		Name:  "nodiscover",
		Usage: "Disables the peer discovery mechanism (manual peer addition)",
	}
	DiscoveryV5Flag = cli.BoolFlag{
		Name:  "v5disc",
		Usage: "Enables the experimental RLPx V5 (Topic Discovery) mechanism",
	}
	NetrestrictFlag = cli.StringFlag{
		Name:  "netrestrict",
		Usage: "Restricts network communication to the given IP networks (CIDR masks)",
	}
	DNSDiscoveryFlag = cli.StringFlag{
		Name:  "discovery.dns",
		Usage: "Sets DNS discovery entry points (use \"\" to disable DNS)",
	}

	// ATM the url is left to the user and deployment to
	JSpathFlag = cli.StringFlag{
		Name:  "jspath",
		Usage: "JavaScript root path for `loadScript`",
		Value: ".",
	}

	// Gas price oracle settings
	GpoBlocksFlag = cli.IntFlag{
		Name:  "gpo.blocks",
		Usage: "Number of recent blocks to check for gas prices",
		Value: ethconfig.Defaults.GPO.Blocks,
	}
	GpoPercentileFlag = cli.IntFlag{
		Name:  "gpo.percentile",
		Usage: "Suggested gas price is the given percentile of a set of recent transaction gas prices",
		Value: ethconfig.Defaults.GPO.Percentile,
	}
	GpoMaxGasPriceFlag = cli.Int64Flag{
		Name:  "gpo.maxprice",
		Usage: "Maximum gas price will be recommended by gpo",
		Value: ethconfig.Defaults.GPO.MaxPrice.Int64(),
	}

	// Metrics flags
	MetricsEnabledFlag = cli.BoolFlag{
		Name:  "metrics",
		Usage: "Enable metrics collection and reporting",
	}
	MetricsEnabledExpensiveFlag = cli.BoolFlag{
		Name:  "metrics.expensive",
		Usage: "Enable expensive metrics collection and reporting",
	}

	// MetricsHTTPFlag defines the endpoint for a stand-alone metrics HTTP endpoint.
	// Since the pprof service enables sensitive/vulnerable behavior, this allows a user
	// to enable a public-OK metrics endpoint without having to worry about ALSO exposing
	// other profiling behavior or information.
	MetricsHTTPFlag = cli.StringFlag{
		Name:  "metrics.addr",
		Usage: "Enable stand-alone metrics HTTP server listening interface",
		Value: metrics.DefaultConfig.HTTP,
	}
	MetricsPortFlag = cli.IntFlag{
		Name:  "metrics.port",
		Usage: "Metrics HTTP server listening port",
		Value: metrics.DefaultConfig.Port,
	}

	CliqueSnapshotCheckpointIntervalFlag = cli.UintFlag{
		Name:  "clique.checkpoint",
		Usage: "number of blocks after which to save the vote snapshot to the database",
		Value: 10,
	}
	CliqueSnapshotInmemorySnapshotsFlag = cli.IntFlag{
		Name:  "clique.snapshots",
		Usage: "number of recent vote snapshots to keep in memory",
		Value: 1024,
	}
	CliqueSnapshotInmemorySignaturesFlag = cli.IntFlag{
		Name:  "clique.signatures",
		Usage: "number of recent block signatures to keep in memory",
		Value: 16384,
	}
	CliqueDataDirFlag = DirectoryFlag{
		Name:  "clique.datadir",
		Usage: "a path to clique db folder",
		Value: "",
	}

	SnapKeepBlocksFlag = cli.BoolFlag{
		Name:  ethconfig.FlagSnapKeepBlocks,
		Usage: "Keep ancient blocks in db (useful for debug)",
	}
	SnapStopFlag = cli.BoolFlag{
		Name:  ethconfig.FlagSnapStop,
		Usage: "Workaround to stop producing new snapshots, if you meet some snapshots-related critical bug",
	}
	TorrentVerbosityFlag = cli.IntFlag{
		Name:  "torrent.verbosity",
		Value: 2,
		Usage: "0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail (must set --verbosity to equal or higher level and has defeault: 3)",
	}
	TorrentDownloadRateFlag = cli.StringFlag{
		Name:  "torrent.download.rate",
		Value: "16mb",
		Usage: "bytes per second, example: 32mb",
	}
	TorrentUploadRateFlag = cli.StringFlag{
		Name:  "torrent.upload.rate",
		Value: "4mb",
		Usage: "bytes per second, example: 32mb",
	}
	TorrentDownloadSlotsFlag = cli.IntFlag{
		Name:  "torrent.download.slots",
		Value: 3,
		Usage: "amount of files to download in parallel. If network has enough seeders 1-3 slot enough, if network has lack of seeders increase to 5-7 (too big value will slow down everything).",
	}
	NoDownloaderFlag = cli.BoolFlag{
		Name:  "no-downloader",
		Usage: "to disable downloader component",
	}
	DownloaderVerifyFlag = cli.BoolFlag{
		Name:  "downloader.verify",
		Usage: "verify snapshots on startup. it will not report founded problems but just re-download broken pieces",
	}
	TorrentPortFlag = cli.IntFlag{
		Name:  "torrent.port",
		Value: 42069,
		Usage: "port to listen and serve BitTorrent protocol",
	}
	TorrentMaxPeersFlag = cli.IntFlag{
		Name:  "torrent.maxpeers",
		Value: 100,
		Usage: "unused parameter (reserved for future use)",
	}
	TorrentConnsPerFileFlag = cli.IntFlag{
		Name:  "torrent.conns.perfile",
		Value: 10,
		Usage: "connections per file",
	}
	DbPageSizeFlag = cli.StringFlag{
		Name:  "db.pagesize",
		Usage: "set mdbx pagesize on db creation: must be power of 2 and '256b <= pagesize <= 64kb'. default: equal to OperationSystem's pageSize",
		Value: datasize.ByteSize(kv.DefaultPageSize()).String(),
	}

	HealthCheckFlag = cli.BoolFlag{
		Name:  "healthcheck",
		Usage: "Enabling grpc health check",
	}

	HeimdallURLFlag = cli.StringFlag{
		Name:  "bor.heimdall",
		Usage: "URL of Heimdall service",
		Value: "http://localhost:1317",
	}

	// WithoutHeimdallFlag no heimdall (for testing purpose)
	WithoutHeimdallFlag = cli.BoolFlag{
		Name:  "bor.withoutheimdall",
		Usage: "Run without Heimdall service (for testing purpose)",
	}

	ConfigFlag = cli.StringFlag{
		Name:  "config",
		Usage: "Sets erigon flags from YAML/TOML file",
		Value: "",
	}
)

var MetricFlags = []cli.Flag{MetricsEnabledFlag, MetricsEnabledExpensiveFlag, MetricsHTTPFlag, MetricsPortFlag}

// setNodeKey loads a node key from command line flags if provided,
// otherwise it tries to load it from datadir,
// otherwise it generates a new key in datadir.
func setNodeKey(ctx *cli.Context, cfg *p2p.Config, datadir string) {
	file := ctx.GlobalString(NodeKeyFileFlag.Name)
	hex := ctx.GlobalString(NodeKeyHexFlag.Name)

	config := p2p.NodeKeyConfig{}
	key, err := config.LoadOrParseOrGenerateAndSave(file, hex, datadir)
	if err != nil {
		Fatalf("%v", err)
	}
	cfg.PrivateKey = key
}

// setNodeUserIdent creates the user identifier from CLI flags.
func setNodeUserIdent(ctx *cli.Context, cfg *nodecfg.Config) {
	if identity := ctx.GlobalString(IdentityFlag.Name); len(identity) > 0 {
		cfg.UserIdent = identity
	}
}
func setNodeUserIdentCobra(f *pflag.FlagSet, cfg *nodecfg.Config) {
	if identity := f.String(IdentityFlag.Name, IdentityFlag.Value, IdentityFlag.Usage); identity != nil && len(*identity) > 0 {
		cfg.UserIdent = *identity
	}
}

func setBootstrapNodes(ctx *cli.Context, cfg *p2p.Config) {
	// If already set, don't apply defaults.
	if cfg.BootstrapNodes != nil {
		return
	}

	nodes, err := GetBootnodesFromFlags(ctx.GlobalString(BootnodesFlag.Name), ctx.GlobalString(ChainFlag.Name))
	if err != nil {
		Fatalf("Option %s: %v", BootnodesFlag.Name, err)
	}

	cfg.BootstrapNodes = nodes
}

func setBootstrapNodesV5(ctx *cli.Context, cfg *p2p.Config) {
	// If already set, don't apply defaults.
	if cfg.BootstrapNodesV5 != nil {
		return
	}

	nodes, err := GetBootnodesFromFlags(ctx.GlobalString(BootnodesFlag.Name), ctx.GlobalString(ChainFlag.Name))
	if err != nil {
		Fatalf("Option %s: %v", BootnodesFlag.Name, err)
	}

	cfg.BootstrapNodesV5 = nodes
}

// GetBootnodesFromFlags makes a list of bootnodes from command line flags.
// If urlsStr is given, it is used and parsed as a comma-separated list of enode:// urls,
// otherwise a list of preconfigured bootnodes of the specified chain is returned.
func GetBootnodesFromFlags(urlsStr, chain string) ([]*enode.Node, error) {
	var urls []string
	if urlsStr != "" {
		urls = SplitAndTrim(urlsStr)
	} else {
		urls = params.BootnodeURLsOfChain(chain)
	}
	return ParseNodesFromURLs(urls)
}

func setStaticPeers(ctx *cli.Context, cfg *p2p.Config) {
	var urls []string
	if ctx.GlobalIsSet(StaticPeersFlag.Name) {
		urls = SplitAndTrim(ctx.GlobalString(StaticPeersFlag.Name))
	} else {
		chain := ctx.GlobalString(ChainFlag.Name)
		urls = params.StaticPeerURLsOfChain(chain)
	}

	nodes, err := ParseNodesFromURLs(urls)
	if err != nil {
		Fatalf("Option %s: %v", StaticPeersFlag.Name, err)
	}

	cfg.StaticNodes = nodes
}

func setTrustedPeers(ctx *cli.Context, cfg *p2p.Config) {
	if !ctx.GlobalIsSet(TrustedPeersFlag.Name) {
		return
	}

	urls := SplitAndTrim(ctx.GlobalString(TrustedPeersFlag.Name))
	trustedNodes, err := ParseNodesFromURLs(urls)
	if err != nil {
		Fatalf("Option %s: %v", TrustedPeersFlag.Name, err)
	}

	cfg.TrustedNodes = append(cfg.TrustedNodes, trustedNodes...)
}

func ParseNodesFromURLs(urls []string) ([]*enode.Node, error) {
	nodes := make([]*enode.Node, 0, len(urls))
	for _, url := range urls {
		if url == "" {
			continue
		}
		n, err := enode.Parse(enode.ValidSchemes, url)
		if err != nil {
			return nil, fmt.Errorf("invalid node URL %s: %w", url, err)
		}
		nodes = append(nodes, n)
	}
	return nodes, nil
}

// NewP2PConfig
//  - doesn't setup bootnodes - they will set when genesisHash will know
func NewP2PConfig(
	nodiscover bool,
	dirs datadir.Dirs,
	netRestrict string,
	natSetting string,
	maxPeers int,
	maxPendPeers int,
	nodeName string,
	staticPeers []string,
	trustedPeers []string,
	port,
	protocol uint,
) (*p2p.Config, error) {
	var enodeDBPath string
	switch protocol {
	case eth.ETH66:
		enodeDBPath = filepath.Join(dirs.Nodes, "eth66")
	case eth.ETH67:
		enodeDBPath = filepath.Join(dirs.Nodes, "eth67")
	default:
		return nil, fmt.Errorf("unknown protocol: %v", protocol)
	}

	serverKey, err := nodeKey(dirs.DataDir)
	if err != nil {
		return nil, err
	}

	cfg := &p2p.Config{
		ListenAddr:      fmt.Sprintf(":%d", port),
		MaxPeers:        maxPeers,
		MaxPendingPeers: maxPendPeers,
		NAT:             nat.Any(),
		NoDiscovery:     nodiscover,
		PrivateKey:      serverKey,
		Name:            nodeName,
		Log:             log.New(),
		NodeDatabase:    enodeDBPath,
	}
	if netRestrict != "" {
		cfg.NetRestrict = new(netutil.Netlist)
		cfg.NetRestrict.Add(netRestrict)
	}
	if staticPeers != nil {
		staticNodes, err := ParseNodesFromURLs(staticPeers)
		if err != nil {
			return nil, fmt.Errorf("bad option %s: %w", StaticPeersFlag.Name, err)
		}
		cfg.StaticNodes = staticNodes
	}
	if trustedPeers != nil {
		trustedNodes, err := ParseNodesFromURLs(trustedPeers)
		if err != nil {
			return nil, fmt.Errorf("bad option %s: %w", TrustedPeersFlag.Name, err)
		}
		cfg.TrustedNodes = trustedNodes
	}
	natif, err := nat.Parse(natSetting)
	if err != nil {
		return nil, fmt.Errorf("invalid nat option %s: %w", natSetting, err)
	}
	cfg.NAT = natif
	return cfg, nil
}

// nodeKey loads a node key from datadir if it exists,
// otherwise it generates a new key in datadir.
func nodeKey(datadir string) (*ecdsa.PrivateKey, error) {
	config := p2p.NodeKeyConfig{}
	keyfile := config.DefaultPath(datadir)
	return config.LoadOrGenerateAndSave(keyfile)
}

// setListenAddress creates a TCP listening address string from set command
// line flags.
func setListenAddress(ctx *cli.Context, cfg *p2p.Config) {
	if ctx.GlobalIsSet(ListenPortFlag.Name) {
		cfg.ListenAddr = fmt.Sprintf(":%d", ctx.GlobalInt(ListenPortFlag.Name))
	}
	if ctx.GlobalIsSet(P2pProtocolVersionFlag.Name) {
		cfg.ProtocolVersion = uint(ctx.GlobalInt(P2pProtocolVersionFlag.Name))
	}
	if ctx.GlobalIsSet(SentryAddrFlag.Name) {
		cfg.SentryAddr = SplitAndTrim(ctx.GlobalString(SentryAddrFlag.Name))
	}
}

// setNAT creates a port mapper from command line flags.
func setNAT(ctx *cli.Context, cfg *p2p.Config) {
	if ctx.GlobalIsSet(NATFlag.Name) {
		natif, err := nat.Parse(ctx.GlobalString(NATFlag.Name))
		if err != nil {
			Fatalf("Option %s: %v", NATFlag.Name, err)
		}
		cfg.NAT = natif
	}
}

// SplitAndTrim splits input separated by a comma
// and trims excessive white space from the substrings.
func SplitAndTrim(input string) (ret []string) {
	l := strings.Split(input, ",")
	for _, r := range l {
		if r = strings.TrimSpace(r); r != "" {
			ret = append(ret, r)
		}
	}
	return ret
}

// setEtherbase retrieves the etherbase from the directly specified
// command line flags.
func setEtherbase(ctx *cli.Context, cfg *ethconfig.Config) {
	var etherbase string
	if ctx.GlobalIsSet(MinerEtherbaseFlag.Name) {
		etherbase = ctx.GlobalString(MinerEtherbaseFlag.Name)
		if etherbase != "" {
			cfg.Miner.Etherbase = common.HexToAddress(etherbase)
		}
	}

	setSigKey := func(ctx *cli.Context, cfg *ethconfig.Config) {
		if ctx.GlobalIsSet(MinerSigningKeyFileFlag.Name) {
			signingKeyFileName := ctx.GlobalString(MinerSigningKeyFileFlag.Name)
			key, err := crypto.LoadECDSA(signingKeyFileName)
			if err != nil {
				panic(err)
			}
			cfg.Miner.SigKey = key
		}
	}

	if ctx.GlobalString(ChainFlag.Name) == networkname.DevChainName || ctx.GlobalString(ChainFlag.Name) == networkname.BorDevnetChainName {
		if etherbase == "" {
			cfg.Miner.SigKey = core.DevnetSignPrivateKey
			cfg.Miner.Etherbase = core.DevnetEtherbase
		}
		setSigKey(ctx, cfg)
	}

	chainsWithValidatorMode := map[string]bool{
		networkname.FermionChainName: true,
		networkname.BSCChainName:     true,
		networkname.RialtoChainName:  true,
		networkname.ChapelChainName:  true,
	}
	if _, ok := chainsWithValidatorMode[ctx.GlobalString(ChainFlag.Name)]; ok || ctx.GlobalIsSet(MinerSigningKeyFileFlag.Name) {
		if ctx.GlobalIsSet(MiningEnabledFlag.Name) && !ctx.GlobalIsSet(MinerSigningKeyFileFlag.Name) {
			panic(fmt.Sprintf("Flag --%s is required in %s chain with --%s flag", MinerSigningKeyFileFlag.Name, ChainFlag.Name, MiningEnabledFlag.Name))
		}
		setSigKey(ctx, cfg)
		if cfg.Miner.SigKey != nil {
			cfg.Miner.Etherbase = crypto.PubkeyToAddress(cfg.Miner.SigKey.PublicKey)
		}
	}
}

func SetP2PConfig(ctx *cli.Context, cfg *p2p.Config, nodeName, datadir string) {
	cfg.Name = nodeName
	setNodeKey(ctx, cfg, datadir)
	setNAT(ctx, cfg)
	setListenAddress(ctx, cfg)
	setBootstrapNodes(ctx, cfg)
	setBootstrapNodesV5(ctx, cfg)
	setStaticPeers(ctx, cfg)
	setTrustedPeers(ctx, cfg)

	if ctx.GlobalIsSet(MaxPeersFlag.Name) {
		cfg.MaxPeers = ctx.GlobalInt(MaxPeersFlag.Name)
	}

	if ctx.GlobalIsSet(MaxPendingPeersFlag.Name) {
		cfg.MaxPendingPeers = ctx.GlobalInt(MaxPendingPeersFlag.Name)
	}
	if ctx.GlobalIsSet(NoDiscoverFlag.Name) {
		cfg.NoDiscovery = true
	}

	if ctx.GlobalIsSet(DiscoveryV5Flag.Name) {
		cfg.DiscoveryV5 = ctx.GlobalBool(DiscoveryV5Flag.Name)
	}

	ethPeers := cfg.MaxPeers
	cfg.Name = nodeName
	log.Info("Maximum peer count", "ETH", ethPeers, "total", cfg.MaxPeers)

	if netrestrict := ctx.GlobalString(NetrestrictFlag.Name); netrestrict != "" {
		list, err := netutil.ParseNetlist(netrestrict)
		if err != nil {
			Fatalf("Option %q: %v", NetrestrictFlag.Name, err)
		}
		cfg.NetRestrict = list
	}

	if ctx.GlobalString(ChainFlag.Name) == networkname.DevChainName {
		// --dev mode can't use p2p networking.
		//cfg.MaxPeers = 0 // It can have peers otherwise local sync is not possible
		if !ctx.GlobalIsSet(ListenPortFlag.Name) {
			cfg.ListenAddr = ":0"
		}
		cfg.NoDiscovery = true
		cfg.DiscoveryV5 = false
		log.Info("Development chain flags set", "--nodiscover", cfg.NoDiscovery, "--v5disc", cfg.DiscoveryV5, "--port", cfg.ListenAddr)
	}
}

// SetNodeConfig applies node-related command line flags to the config.
func SetNodeConfig(ctx *cli.Context, cfg *nodecfg.Config) {
	setDataDir(ctx, cfg)
	setNodeUserIdent(ctx, cfg)
	SetP2PConfig(ctx, &cfg.P2P, cfg.NodeName(), cfg.Dirs.DataDir)

	cfg.SentryLogPeerInfo = ctx.GlobalIsSet(SentryLogPeerInfoFlag.Name)
}

func SetNodeConfigCobra(cmd *cobra.Command, cfg *nodecfg.Config) {
	flags := cmd.Flags()
	//SetP2PConfig(ctx, &cfg.P2P)
	setNodeUserIdentCobra(flags, cfg)
	setDataDirCobra(flags, cfg)
}

func DataDirForNetwork(datadir string, network string) string {
	if datadir != paths.DefaultDataDir() {
		return datadir
	}

	switch network {
	case networkname.DevChainName:
		return "" // unless explicitly requested, use memory databases
	case networkname.RinkebyChainName:
		return networkDataDirCheckingLegacy(datadir, "rinkeby")
	case networkname.GoerliChainName:
		return networkDataDirCheckingLegacy(datadir, "goerli")
	case networkname.KilnDevnetChainName:
		return networkDataDirCheckingLegacy(datadir, "kiln-devnet")
	case networkname.SokolChainName:
		return networkDataDirCheckingLegacy(datadir, "sokol")
	case networkname.FermionChainName:
		return networkDataDirCheckingLegacy(datadir, "fermion")
	case networkname.MumbaiChainName:
		return networkDataDirCheckingLegacy(datadir, "mumbai")
	case networkname.BorMainnetChainName:
		return networkDataDirCheckingLegacy(datadir, "bor-mainnet")
	case networkname.BorDevnetChainName:
		return networkDataDirCheckingLegacy(datadir, "bor-devnet")
	case networkname.SepoliaChainName:
		return networkDataDirCheckingLegacy(datadir, "sepolia")
	case networkname.GnosisChainName:
		return networkDataDirCheckingLegacy(datadir, "gnosis")

	default:
		return datadir
	}
}

// networkDataDirCheckingLegacy checks if the datadir for the network already exists and uses that if found.
// if not checks for a LOCK file at the root of the datadir and uses this if found
// or by default assume a fresh node and to use the nested directory for the network
func networkDataDirCheckingLegacy(datadir, network string) string {
	anticipated := filepath.Join(datadir, network)

	if _, err := os.Stat(anticipated); !os.IsNotExist(err) {
		return anticipated
	}

	legacyLockFile := filepath.Join(datadir, "LOCK")
	if _, err := os.Stat(legacyLockFile); !os.IsNotExist(err) {
		log.Info("Using legacy datadir")
		return datadir
	}

	return anticipated
}

func setDataDir(ctx *cli.Context, cfg *nodecfg.Config) {
	if ctx.GlobalIsSet(DataDirFlag.Name) {
		cfg.Dirs.DataDir = ctx.GlobalString(DataDirFlag.Name)
	} else {
		cfg.Dirs.DataDir = DataDirForNetwork(cfg.Dirs.DataDir, ctx.GlobalString(ChainFlag.Name))
	}
	cfg.Dirs = datadir.New(cfg.Dirs.DataDir)

	if err := cfg.MdbxPageSize.UnmarshalText([]byte(ctx.GlobalString(DbPageSizeFlag.Name))); err != nil {
		panic(err)
	}
	sz := cfg.MdbxPageSize.Bytes()
	if !isPowerOfTwo(sz) || sz < 256 || sz > 64*1024 {
		panic("invalid --db.pagesize: " + DbPageSizeFlag.Usage)
	}
}

func isPowerOfTwo(n uint64) bool {
	if n == 0 { //corner case: if n is zero it will also consider as power 2
		return true
	}
	return n&(n-1) == 0
}

func setDataDirCobra(f *pflag.FlagSet, cfg *nodecfg.Config) {
	dirname, err := f.GetString(DataDirFlag.Name)
	if err != nil {
		panic(err)
	}
	chain, err := f.GetString(ChainFlag.Name)
	if err != nil {
		panic(err)
	}
	if dirname != "" {
		cfg.Dirs.DataDir = dirname
	} else {
		cfg.Dirs.DataDir = DataDirForNetwork(cfg.Dirs.DataDir, chain)
	}

	cfg.Dirs.DataDir = DataDirForNetwork(cfg.Dirs.DataDir, chain)
	cfg.Dirs = datadir.New(cfg.Dirs.DataDir)
}

func setGPO(ctx *cli.Context, cfg *gasprice.Config) {
	if ctx.GlobalIsSet(GpoBlocksFlag.Name) {
		cfg.Blocks = ctx.GlobalInt(GpoBlocksFlag.Name)
	}
	if ctx.GlobalIsSet(GpoPercentileFlag.Name) {
		cfg.Percentile = ctx.GlobalInt(GpoPercentileFlag.Name)
	}
	if ctx.GlobalIsSet(GpoMaxGasPriceFlag.Name) {
		cfg.MaxPrice = big.NewInt(ctx.GlobalInt64(GpoMaxGasPriceFlag.Name))
	}
}

//nolint
func setGPOCobra(f *pflag.FlagSet, cfg *gasprice.Config) {
	if v := f.Int(GpoBlocksFlag.Name, GpoBlocksFlag.Value, GpoBlocksFlag.Usage); v != nil {
		cfg.Blocks = *v
	}
	if v := f.Int(GpoPercentileFlag.Name, GpoPercentileFlag.Value, GpoPercentileFlag.Usage); v != nil {
		cfg.Percentile = *v
	}
	if v := f.Int64(GpoMaxGasPriceFlag.Name, GpoMaxGasPriceFlag.Value, GpoMaxGasPriceFlag.Usage); v != nil {
		cfg.MaxPrice = big.NewInt(*v)
	}
}

func setTxPool(ctx *cli.Context, cfg *core.TxPoolConfig) {
	if ctx.GlobalIsSet(TxPoolDisableFlag.Name) {
		cfg.Disable = true
	}
	if ctx.GlobalIsSet(TxPoolLocalsFlag.Name) {
		locals := strings.Split(ctx.GlobalString(TxPoolLocalsFlag.Name), ",")
		for _, account := range locals {
			if trimmed := strings.TrimSpace(account); !common.IsHexAddress(trimmed) {
				Fatalf("Invalid account in --txpool.locals: %s", trimmed)
			} else {
				cfg.Locals = append(cfg.Locals, common.HexToAddress(account))
			}
		}
	}
	if ctx.GlobalIsSet(TxPoolNoLocalsFlag.Name) {
		cfg.NoLocals = ctx.GlobalBool(TxPoolNoLocalsFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolPriceLimitFlag.Name) {
		cfg.PriceLimit = ctx.GlobalUint64(TxPoolPriceLimitFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolPriceBumpFlag.Name) {
		cfg.PriceBump = ctx.GlobalUint64(TxPoolPriceBumpFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolAccountSlotsFlag.Name) {
		cfg.AccountSlots = ctx.GlobalUint64(TxPoolAccountSlotsFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolGlobalSlotsFlag.Name) {
		cfg.GlobalSlots = ctx.GlobalUint64(TxPoolGlobalSlotsFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolAccountQueueFlag.Name) {
		cfg.AccountQueue = ctx.GlobalUint64(TxPoolAccountQueueFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolGlobalQueueFlag.Name) {
		cfg.GlobalQueue = ctx.GlobalUint64(TxPoolGlobalQueueFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolGlobalBaseFeeSlotsFlag.Name) {
		cfg.GlobalBaseFeeQueue = ctx.GlobalUint64(TxPoolGlobalBaseFeeSlotsFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolLifetimeFlag.Name) {
		cfg.Lifetime = ctx.GlobalDuration(TxPoolLifetimeFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolTraceSendersFlag.Name) {
		// Parse the command separated flag
		senderHexes := SplitAndTrim(ctx.GlobalString(TxPoolTraceSendersFlag.Name))
		cfg.TracedSenders = make([]string, len(senderHexes))
		for i, senderHex := range senderHexes {
			sender := common.HexToAddress(senderHex)
			cfg.TracedSenders[i] = string(sender[:])
		}
	}
}

func setEthash(ctx *cli.Context, datadir string, cfg *ethconfig.Config) {
	if ctx.GlobalIsSet(EthashDatasetDirFlag.Name) {
		cfg.Ethash.DatasetDir = ctx.GlobalString(EthashDatasetDirFlag.Name)
	} else {
		cfg.Ethash.DatasetDir = filepath.Join(datadir, "ethash-dags")
	}
	if ctx.GlobalIsSet(EthashCachesInMemoryFlag.Name) {
		cfg.Ethash.CachesInMem = ctx.GlobalInt(EthashCachesInMemoryFlag.Name)
	}
	if ctx.GlobalIsSet(EthashCachesLockMmapFlag.Name) {
		cfg.Ethash.CachesLockMmap = ctx.GlobalBool(EthashCachesLockMmapFlag.Name)
	}
	if ctx.GlobalIsSet(FakePoWFlag.Name) {
		cfg.Ethash.PowMode = ethash.ModeFake
	}
	if ctx.GlobalIsSet(EthashDatasetsLockMmapFlag.Name) {
		cfg.Ethash.DatasetsLockMmap = ctx.GlobalBool(EthashDatasetsLockMmapFlag.Name)
	}
}

func SetupMinerCobra(cmd *cobra.Command, cfg *params.MiningConfig) {
	flags := cmd.Flags()
	var err error
	cfg.Enabled, err = flags.GetBool(MiningEnabledFlag.Name)
	if err != nil {
		panic(err)
	}
	if cfg.Enabled && len(cfg.Etherbase.Bytes()) == 0 {
		panic(fmt.Sprintf("Erigon supports only remote miners. Flag --%s or --%s is required", MinerNotifyFlag.Name, MinerSigningKeyFileFlag.Name))
	}
	cfg.Notify, err = flags.GetStringArray(MinerNotifyFlag.Name)
	if err != nil {
		panic(err)
	}
	extraDataStr, err := flags.GetString(MinerExtraDataFlag.Name)
	if err != nil {
		panic(err)
	}
	cfg.ExtraData = []byte(extraDataStr)
	cfg.GasLimit, err = flags.GetUint64(MinerGasLimitFlag.Name)
	if err != nil {
		panic(err)
	}
	price, err := flags.GetInt64(MinerGasPriceFlag.Name)
	if err != nil {
		panic(err)
	}
	cfg.GasPrice = big.NewInt(price)
	cfg.Recommit, err = flags.GetDuration(MinerRecommitIntervalFlag.Name)
	if err != nil {
		panic(err)
	}
	cfg.Noverify, err = flags.GetBool(MinerNoVerfiyFlag.Name)
	if err != nil {
		panic(err)
	}

	// Extract the current etherbase, new flag overriding legacy one
	var etherbase string
	etherbase, err = flags.GetString(MinerEtherbaseFlag.Name)
	if err != nil {
		panic(err)
	}

	// Convert the etherbase into an address and configure it
	if etherbase == "" {
		Fatalf("No etherbase configured")
	}
	cfg.Etherbase = common.HexToAddress(etherbase)
}

func setClique(ctx *cli.Context, cfg *params.ConsensusSnapshotConfig, datadir string) {
	cfg.CheckpointInterval = ctx.GlobalUint64(CliqueSnapshotCheckpointIntervalFlag.Name)
	cfg.InmemorySnapshots = ctx.GlobalInt(CliqueSnapshotInmemorySnapshotsFlag.Name)
	cfg.InmemorySignatures = ctx.GlobalInt(CliqueSnapshotInmemorySignaturesFlag.Name)
	if ctx.GlobalIsSet(CliqueDataDirFlag.Name) {
		cfg.DBPath = filepath.Join(ctx.GlobalString(CliqueDataDirFlag.Name), "clique", "db")
	} else {
		cfg.DBPath = filepath.Join(datadir, "clique", "db")
	}
}

func setAuRa(ctx *cli.Context, cfg *params.AuRaConfig, datadir string) {
	cfg.DBPath = filepath.Join(datadir, "aura")
}

func setParlia(ctx *cli.Context, cfg *params.ParliaConfig, datadir string) {
	cfg.DBPath = filepath.Join(datadir, "parlia")
}

func setBorConfig(ctx *cli.Context, cfg *ethconfig.Config) {
	cfg.HeimdallURL = ctx.GlobalString(HeimdallURLFlag.Name)
	cfg.WithoutHeimdall = ctx.GlobalBool(WithoutHeimdallFlag.Name)
}

func setMiner(ctx *cli.Context, cfg *params.MiningConfig) {
	cfg.Enabled = ctx.GlobalIsSet(MiningEnabledFlag.Name)
	cfg.EnabledPOS = !ctx.GlobalIsSet(ProposingDisableFlag.Name)

	if cfg.Enabled && len(cfg.Etherbase.Bytes()) == 0 {
		panic(fmt.Sprintf("Erigon supports only remote miners. Flag --%s or --%s is required", MinerNotifyFlag.Name, MinerSigningKeyFileFlag.Name))
	}
	if ctx.GlobalIsSet(MinerNotifyFlag.Name) {
		cfg.Notify = strings.Split(ctx.GlobalString(MinerNotifyFlag.Name), ",")
	}
	if ctx.GlobalIsSet(MinerExtraDataFlag.Name) {
		cfg.ExtraData = []byte(ctx.GlobalString(MinerExtraDataFlag.Name))
	}
	if ctx.GlobalIsSet(MinerGasLimitFlag.Name) {
		cfg.GasLimit = ctx.GlobalUint64(MinerGasLimitFlag.Name)
	}
	if ctx.GlobalIsSet(MinerGasPriceFlag.Name) {
		cfg.GasPrice = GlobalBig(ctx, MinerGasPriceFlag.Name)
	}
	if ctx.GlobalIsSet(MinerRecommitIntervalFlag.Name) {
		cfg.Recommit = ctx.GlobalDuration(MinerRecommitIntervalFlag.Name)
	}
	if ctx.GlobalIsSet(MinerNoVerfiyFlag.Name) {
		cfg.Noverify = ctx.GlobalBool(MinerNoVerfiyFlag.Name)
	}
}

func setWhitelist(ctx *cli.Context, cfg *ethconfig.Config) {
	whitelist := ctx.GlobalString(WhitelistFlag.Name)
	if whitelist == "" {
		return
	}
	cfg.Whitelist = make(map[uint64]common.Hash)
	for _, entry := range strings.Split(whitelist, ",") {
		parts := strings.Split(entry, "=")
		if len(parts) != 2 {
			Fatalf("Invalid whitelist entry: %s", entry)
		}
		number, err := strconv.ParseUint(parts[0], 0, 64)
		if err != nil {
			Fatalf("Invalid whitelist block number %s: %v", parts[0], err)
		}
		var hash common.Hash
		if err = hash.UnmarshalText([]byte(parts[1])); err != nil {
			Fatalf("Invalid whitelist hash %s: %v", parts[1], err)
		}
		cfg.Whitelist[number] = hash
	}
}

// CheckExclusive verifies that only a single instance of the provided flags was
// set by the user. Each flag might optionally be followed by a string type to
// specialize it further.
func CheckExclusive(ctx *cli.Context, args ...interface{}) {
	set := make([]string, 0, 1)
	for i := 0; i < len(args); i++ {
		// Make sure the next argument is a flag and skip if not set
		flag, ok := args[i].(cli.Flag)
		if !ok {
			panic(fmt.Sprintf("invalid argument, not cli.Flag type: %T", args[i]))
		}
		// Check if next arg extends current and expand its name if so
		name := flag.GetName()

		if i+1 < len(args) {
			switch option := args[i+1].(type) {
			case string:
				// Extended flag check, make sure value set doesn't conflict with passed in option
				if ctx.GlobalString(flag.GetName()) == option {
					name += "=" + option
					set = append(set, "--"+name)
				}
				// shift arguments and continue
				i++
				continue

			case cli.Flag:
			default:
				panic(fmt.Sprintf("invalid argument, not cli.Flag or string extension: %T", args[i+1]))
			}
		}
		// Mark the flag if it's set
		if ctx.GlobalIsSet(flag.GetName()) {
			set = append(set, "--"+name)
		}
	}
	if len(set) > 1 {
		Fatalf("Flags %v can't be used at the same time", strings.Join(set, ", "))
	}
}

// SetEthConfig applies eth-related command line flags to the config.
func SetEthConfig(ctx *cli.Context, nodeConfig *nodecfg.Config, cfg *ethconfig.Config) {
	cfg.Sync.UseSnapshots = ctx.GlobalBoolT(SnapshotFlag.Name)
	cfg.Dirs = nodeConfig.Dirs
	cfg.MemoryOverlay = ctx.GlobalBool(MemoryOverlayFlag.Name)
	cfg.Snapshot.KeepBlocks = ctx.GlobalBool(SnapKeepBlocksFlag.Name)
	cfg.Snapshot.Produce = !ctx.GlobalBool(SnapStopFlag.Name)
	cfg.Snapshot.NoDownloader = ctx.GlobalBool(NoDownloaderFlag.Name)
	cfg.Snapshot.Verify = ctx.GlobalBool(DownloaderVerifyFlag.Name)
	cfg.Snapshot.DownloaderAddr = strings.TrimSpace(ctx.GlobalString(DownloaderAddrFlag.Name))
	if cfg.Snapshot.DownloaderAddr == "" {
		downloadRateStr := ctx.GlobalString(TorrentDownloadRateFlag.Name)
		uploadRateStr := ctx.GlobalString(TorrentUploadRateFlag.Name)
		var downloadRate, uploadRate datasize.ByteSize
		if err := downloadRate.UnmarshalText([]byte(downloadRateStr)); err != nil {
			panic(err)
		}
		if err := uploadRate.UnmarshalText([]byte(uploadRateStr)); err != nil {
			panic(err)
		}
		lvl, dbg, err := downloadercfg.Int2LogLevel(ctx.GlobalInt(TorrentVerbosityFlag.Name))
		if err != nil {
			panic(err)
		}
		log.Info("torrent verbosity", "level", lvl.LogString())
		cfg.Downloader, err = downloadercfg.New(cfg.Dirs.Snap, lvl, dbg, nodeConfig.P2P.NAT, downloadRate, uploadRate, ctx.GlobalInt(TorrentPortFlag.Name), ctx.GlobalInt(TorrentConnsPerFileFlag.Name), ctx.GlobalInt(TorrentDownloadSlotsFlag.Name))
		if err != nil {
			panic(err)
		}
	}

	nodeConfig.Http.Snap = cfg.Snapshot

	if ctx.Command.Name == "import" {
		cfg.ImportMode = true
	}

	setEtherbase(ctx, cfg)
	setGPO(ctx, &cfg.GPO)

	setTxPool(ctx, &cfg.DeprecatedTxPool)
	cfg.TxPool = core.DefaultTxPool2Config(cfg.DeprecatedTxPool)
	cfg.TxPool.DBDir = nodeConfig.Dirs.TxPool

	setEthash(ctx, nodeConfig.Dirs.DataDir, cfg)
	setClique(ctx, &cfg.Clique, nodeConfig.Dirs.DataDir)
	setAuRa(ctx, &cfg.Aura, nodeConfig.Dirs.DataDir)
	setParlia(ctx, &cfg.Parlia, nodeConfig.Dirs.DataDir)
	setMiner(ctx, &cfg.Miner)
	setWhitelist(ctx, cfg)
	setBorConfig(ctx, cfg)

	cfg.Ethstats = ctx.GlobalString(EthStatsURLFlag.Name)
	cfg.P2PEnabled = len(nodeConfig.P2P.SentryAddr) == 0
	cfg.EnabledIssuance = ctx.GlobalIsSet(EnabledIssuance.Name)
	if ctx.GlobalIsSet(NetworkIdFlag.Name) {
		cfg.NetworkID = ctx.GlobalUint64(NetworkIdFlag.Name)
	}

	if ctx.GlobalIsSet(RPCGlobalGasCapFlag.Name) {
		cfg.RPCGasCap = ctx.GlobalUint64(RPCGlobalGasCapFlag.Name)
	}
	if cfg.RPCGasCap != 0 {
		log.Info("Set global gas cap", "cap", cfg.RPCGasCap)
	} else {
		log.Info("Global gas cap disabled")
	}
	if ctx.GlobalIsSet(RPCGlobalTxFeeCapFlag.Name) {
		cfg.RPCTxFeeCap = ctx.GlobalFloat64(RPCGlobalTxFeeCapFlag.Name)
	}
	if ctx.GlobalIsSet(NoDiscoverFlag.Name) {
		cfg.EthDiscoveryURLs = []string{}
	} else if ctx.GlobalIsSet(DNSDiscoveryFlag.Name) {
		urls := ctx.GlobalString(DNSDiscoveryFlag.Name)
		if urls == "" {
			cfg.EthDiscoveryURLs = []string{}
		} else {
			cfg.EthDiscoveryURLs = SplitAndTrim(urls)
		}
	}
	// Override any default configs for hard coded networks.
	chain := ctx.GlobalString(ChainFlag.Name)

	switch chain {
	default:
		genesis := core.DefaultGenesisBlockByChainName(chain)
		genesisHash := params.GenesisHashByChainName(chain)
		if (genesis == nil) || (genesisHash == nil) {
			Fatalf("ChainDB name is not recognized: %s", chain)
			return
		}
		cfg.Genesis = genesis
		if !ctx.GlobalIsSet(NetworkIdFlag.Name) {
			cfg.NetworkID = params.NetworkIDByChainName(chain)
		}
		SetDNSDiscoveryDefaults(cfg, *genesisHash)
	case "":
		if cfg.NetworkID == 1 {
			SetDNSDiscoveryDefaults(cfg, params.MainnetGenesisHash)
		}
	case networkname.DevChainName:
		if !ctx.GlobalIsSet(NetworkIdFlag.Name) {
			cfg.NetworkID = 1337
		}

		// Create new developer account or reuse existing one
		developer := cfg.Miner.Etherbase
		if developer == (common.Address{}) {
			Fatalf("Please specify developer account address using --miner.etherbase")
		}
		log.Info("Using developer account", "address", developer)

		// Create a new developer genesis block or reuse existing one
		cfg.Genesis = core.DeveloperGenesisBlock(uint64(ctx.GlobalInt(DeveloperPeriodFlag.Name)), developer)
		log.Info("Using custom developer period", "seconds", cfg.Genesis.Config.Clique.Period)
		if !ctx.GlobalIsSet(MinerGasPriceFlag.Name) {
			cfg.Miner.GasPrice = big.NewInt(1)
		}
	}

	if ctx.GlobalIsSet(OverrideTerminalTotalDifficulty.Name) {
		cfg.OverrideTerminalTotalDifficulty = GlobalBig(ctx, OverrideTerminalTotalDifficulty.Name)
	}
	if ctx.GlobalIsSet(OverrideMergeNetsplitBlock.Name) {
		cfg.OverrideMergeNetsplitBlock = GlobalBig(ctx, OverrideMergeNetsplitBlock.Name)
	}
}

// SetDNSDiscoveryDefaults configures DNS discovery with the given URL if
// no URLs are set.
func SetDNSDiscoveryDefaults(cfg *ethconfig.Config, genesis common.Hash) {
	if cfg.EthDiscoveryURLs != nil {
		return // already set through flags/config
	}
	protocol := "all"
	if url := params.KnownDNSNetwork(genesis, protocol); url != "" {
		cfg.EthDiscoveryURLs = []string{url}
	}
}

func SplitTagsFlag(tagsFlag string) map[string]string {
	tags := strings.Split(tagsFlag, ",")
	tagsMap := map[string]string{}

	for _, t := range tags {
		if t != "" {
			kv := strings.Split(t, "=")

			if len(kv) == 2 {
				tagsMap[kv[0]] = kv[1]
			}
		}
	}

	return tagsMap
}

// MakeConsolePreloads retrieves the absolute paths for the console JavaScript
// scripts to preload before starting.
func MakeConsolePreloads(ctx *cli.Context) []string {
	// Skip preloading if there's nothing to preload
	if ctx.GlobalString(PreloadJSFlag.Name) == "" {
		return nil
	}
	// Otherwise resolve absolute paths and return them
	var preloads []string

	for _, file := range strings.Split(ctx.GlobalString(PreloadJSFlag.Name), ",") {
		preloads = append(preloads, strings.TrimSpace(file))
	}
	return preloads
}

func CobraFlags(cmd *cobra.Command, urfaveCliFlags []cli.Flag) {
	flags := cmd.PersistentFlags()
	for _, flag := range urfaveCliFlags {
		switch f := flag.(type) {
		case cli.IntFlag:
			flags.Int(f.Name, f.Value, f.Usage)
		case cli.StringFlag:
			flags.String(f.Name, f.Value, f.Usage)
		case cli.BoolFlag:
			flags.Bool(f.Name, false, f.Usage)
		default:
			panic(fmt.Errorf("unexpected type: %T", flag))
		}
	}
}
