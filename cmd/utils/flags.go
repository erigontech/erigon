// Copyright 2015 The go-ethereum Authors
// (original work)
// Copyright 2024 The Erigon Authors
// (modifications)
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

// Package utils contains internal helper functions for go-ethereum commands.
package utils

import (
	"crypto/ecdsa"
	"fmt"
	"math/big"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/erigontech/erigon-lib/chain/networkid"

	"github.com/c2h5oh/datasize"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/urfave/cli/v2"

	"github.com/erigontech/erigon-lib/log/v3"

	"github.com/erigontech/erigon-lib/chain/networkname"
	"github.com/erigontech/erigon-lib/chain/snapcfg"
	libcommon "github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/common/datadir"
	"github.com/erigontech/erigon-lib/common/metrics"
	libkzg "github.com/erigontech/erigon-lib/crypto/kzg"
	"github.com/erigontech/erigon-lib/direct"
	downloadercfg2 "github.com/erigontech/erigon-lib/downloader/downloadercfg"
	"github.com/erigontech/erigon-lib/txpool/txpoolcfg"

	"github.com/erigontech/erigon/cl/clparams"
	"github.com/erigontech/erigon/cmd/downloader/downloadernat"
	"github.com/erigontech/erigon/cmd/utils/flags"
	common2 "github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/paths"
	"github.com/erigontech/erigon/consensus/ethash/ethashcfg"
	"github.com/erigontech/erigon/core"
	"github.com/erigontech/erigon/crypto"
	"github.com/erigontech/erigon/eth/ethconfig"
	"github.com/erigontech/erigon/eth/gasprice/gaspricecfg"
	"github.com/erigontech/erigon/node/nodecfg"
	"github.com/erigontech/erigon/p2p"
	"github.com/erigontech/erigon/p2p/enode"
	"github.com/erigontech/erigon/p2p/nat"
	"github.com/erigontech/erigon/p2p/netutil"
	"github.com/erigontech/erigon/params"
	borsnaptype "github.com/erigontech/erigon/polygon/bor/snaptype"
	"github.com/erigontech/erigon/rpc/rpccfg"
	"github.com/erigontech/erigon/turbo/logging"
)

// These are all the command line flags we support.
// If you add to this list, please remember to include the
// flag in the appropriate command definition.
//
// The flags are defined here so their names and help texts
// are the same for all commands.

var (
	// General settings
	DataDirFlag = flags.DirectoryFlag{
		Name:  "datadir",
		Usage: "Data directory for the databases",
		Value: flags.DirectoryString(paths.DefaultDataDir()),
	}

	AncientFlag = flags.DirectoryFlag{
		Name:  "datadir.ancient",
		Usage: "Data directory for ancient chain segments (default = inside chaindata)",
	}
	MinFreeDiskSpaceFlag = flags.DirectoryFlag{
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
		Usage: "name of the network to join",
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
	OverridePragueFlag = flags.BigFlag{
		Name:  "override.prague",
		Usage: "Manually specify the Prague fork time, overriding the bundled setting",
	}
	PragueEOFFlag = flags.BigFlag{
		Name:  "prague.eof",
		Usage: "Manually specify the PragueEOF fork time",
	}
	TrustedSetupFile = cli.StringFlag{
		Name:  "trusted-setup-file",
		Usage: "Absolute path to trusted_setup.json file",
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
	EthashDatasetDirFlag = flags.DirectoryFlag{
		Name:  "ethash.dagdir",
		Usage: "Directory to store the ethash mining DAGs",
		Value: flags.DirectoryString(ethconfig.Defaults.Ethash.DatasetDir),
	}
	EthashDatasetsLockMmapFlag = cli.BoolFlag{
		Name:  "ethash.dagslockmmap",
		Usage: "Lock memory maps for recent ethash mining DAGs",
	}
	ExternalConsensusFlag = cli.BoolFlag{
		Name:  "externalcl",
		Usage: "Enables the external consensus layer",
	}
	// Transaction pool settings
	TxPoolDisableFlag = cli.BoolFlag{
		Name:  "txpool.disable",
		Usage: "Experimental external pool and block producer, see ./cmd/txpool/readme.md for more info. Disabling internal txpool and block producer.",
		Value: false,
	}
	TxPoolGossipDisableFlag = cli.BoolFlag{
		Name:  "txpool.gossip.disable",
		Usage: "Disabling p2p gossip of txs. Any txs received by p2p - will be dropped. Some networks like 'Optimism execution engine'/'Optimistic Rollup' - using it to protect against MEV attacks",
		Value: txpoolcfg.DefaultConfig.NoGossip,
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
		Value: txpoolcfg.DefaultConfig.PriceBump,
	}
	TxPoolBlobPriceBumpFlag = cli.Uint64Flag{
		Name:  "txpool.blobpricebump",
		Usage: "Price bump percentage to replace existing (type-3) blob transaction",
		Value: txpoolcfg.DefaultConfig.BlobPriceBump,
	}
	TxPoolAccountSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.accountslots",
		Usage: "Minimum number of executable transaction slots guaranteed per account",
		Value: ethconfig.Defaults.DeprecatedTxPool.AccountSlots,
	}
	TxPoolBlobSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.blobslots",
		Usage: "Max allowed total number of blobs (within type-3 txs) per account",
		Value: txpoolcfg.DefaultConfig.BlobSlots,
	}
	TxPoolTotalBlobPoolLimit = cli.Uint64Flag{
		Name:  "txpool.totalblobpoollimit",
		Usage: "Total limit of number of all blobs in txs within the txpool",
		Value: txpoolcfg.DefaultConfig.TotalBlobPoolLimit,
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
		Usage: "Comma separated list of addresses, whose transactions will traced in transaction pool with debug printing",
		Value: "",
	}
	TxPoolCommitEveryFlag = cli.DurationFlag{
		Name:  "txpool.commit.every",
		Usage: "How often transactions should be committed to the storage",
		Value: txpoolcfg.DefaultConfig.CommitEvery,
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
	MinerGasPriceFlag = flags.BigFlag{
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
	IPCPathFlag = flags.DirectoryFlag{
		Name:  "ipcpath",
		Usage: "Filename for IPC socket/pipe within the datadir (explicit paths escape it)",
	}
	GraphQLEnabledFlag = cli.BoolFlag{
		Name:  "graphql",
		Usage: "Enable the graphql endpoint",
		Value: nodecfg.DefaultConfig.GraphQLEnabled,
	}
	HTTPEnabledFlag = cli.BoolFlag{
		Name:  "http",
		Usage: "JSON-RPC server (enabled by default). Use --http=false to disable it",
		Value: true,
	}
	HTTPServerEnabledFlag = cli.BoolFlag{
		Name:  "http.enabled",
		Usage: "JSON-RPC HTTP server (enabled by default). Use --http.enabled=false to disable it",
		Value: true,
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
		Usage: "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts 'any' or '*' as wildcard.",
		Value: strings.Join(nodecfg.DefaultConfig.HTTPVirtualHosts, ","),
	}
	AuthRpcVirtualHostsFlag = cli.StringFlag{
		Name:  "authrpc.vhosts",
		Usage: "Comma separated list of virtual hostnames from which to accept Engine API requests (server enforced). Accepts 'any' or '*' as wildcard.",
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
		Usage: "Erigon has enabled json streaming for some heavy endpoints (like trace_*). It's a trade-off: greatly reduce amount of RAM (in some cases from 30GB to 30mb), but it produce invalid json format if error happened in the middle of streaming (because json is not streaming-friendly format)",
	}
	RpcBatchLimit = cli.IntFlag{
		Name:  "rpc.batch.limit",
		Usage: "Maximum number of requests in a batch",
		Value: 100,
	}
	RpcReturnDataLimit = cli.IntFlag{
		Name:  "rpc.returndata.limit",
		Usage: "Maximum number of bytes returned from eth_call or similar invocations",
		Value: 100_000,
	}
	HTTPTraceFlag = cli.BoolFlag{
		Name:  "http.trace",
		Usage: "Print all HTTP requests to logs with INFO level",
	}
	HTTPDebugSingleFlag = cli.BoolFlag{
		Name:  "http.dbg.single",
		Usage: "Allow pass HTTP header 'dbg: true' to printt more detailed logs - how this request was executed",
	}
	DBReadConcurrencyFlag = cli.IntFlag{
		Name:  "db.read.concurrency",
		Usage: "Does limit amount of parallel db reads. Default: equal to GOMAXPROCS (or number of CPU)",
		Value: min(max(10, runtime.GOMAXPROCS(-1)*64), 9_000),
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

	TxpoolApiAddrFlag = cli.StringFlag{
		Name:  "txpool.api.addr",
		Usage: "TxPool api network address, for example: 127.0.0.1:9090 (default: use value of --private.api.addr)",
	}

	TraceMaxtracesFlag = cli.UintFlag{
		Name:  "trace.maxtraces",
		Usage: "Sets a limit on traces that can be returned in trace_filter",
		Value: 200,
	}

	HTTPPathPrefixFlag = cli.StringFlag{
		Name:  "http.rpcprefix",
		Usage: "HTTP path prefix on which JSON-RPC is served. Use '/' to serve on all paths.",
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
	WSSubscribeLogsChannelSize = cli.IntFlag{
		Name:  "ws.api.subscribelogs.channelsize",
		Usage: "Size of the channel used for websocket logs subscriptions",
		Value: 8192,
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
		Usage: "Allow for unprotected (non-EIP155 signed) transactions to be submitted via RPC",
	}
	// Careful! Because we must rewind the hash state
	// and re-compute the state trie, the further back in time the request, the more
	// computationally intensive the operation becomes.
	// The current default has been chosen arbitrarily as 'useful' without likely being overly computationally intense.
	RpcMaxGetProofRewindBlockCount = cli.IntFlag{
		Name:  "rpc.maxgetproofrewindblockcount.limit",
		Usage: "Max GetProof rewind block count",
		Value: 100_000,
	}
	StateCacheFlag = cli.StringFlag{
		Name:  "state.cache",
		Value: "0MB",
		Usage: "Amount of data to store in StateCache (enabled if no --datadir set). Set 0 to disable StateCache. Defaults to 0MB",
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
	P2pProtocolVersionFlag = cli.UintSliceFlag{
		Name:  "p2p.protocol",
		Usage: "Version of eth p2p protocol",
		Value: cli.NewUintSlice(nodecfg.DefaultConfig.P2P.ProtocolVersion...),
	}
	P2pProtocolAllowedPorts = cli.UintSliceFlag{
		Name:  "p2p.allowed-ports",
		Usage: "Allowed ports to pick for different eth p2p protocol versions as follows <porta>,<portb>,..,<porti>",
		Value: cli.NewUintSlice(uint(ListenPortFlag.Value), 30304, 30305, 30306, 30307),
	}
	SentryAddrFlag = cli.StringFlag{
		Name:  "sentry.api.addr",
		Usage: "Comma separated sentry addresses '<host>:<port>,<host>:<port>'",
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
			 "" or "none"         Default - do not nat
			 "extip:77.12.33.4"   Will assume the local machine is reachable on the given IP
			 "any"                Uses the first auto-detected mechanism
			 "upnp"               Uses the Universal Plug and Play protocol
			 "pmp"                Uses NAT-PMP with an auto-detected gateway address
			 "pmp:192.168.0.1"    Uses NAT-PMP with the given gateway address
			 "stun"               Uses STUN to detect an external IP using a default server
			 "stun:<server>"      Uses STUN to detect an external IP using the given server (host:port)
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
		Usage: "Number of blocks after which to save the vote snapshot to the database",
		Value: 10,
	}
	CliqueSnapshotInmemorySnapshotsFlag = cli.IntFlag{
		Name:  "clique.snapshots",
		Usage: "Number of recent vote snapshots to keep in memory",
		Value: 1024,
	}
	CliqueSnapshotInmemorySignaturesFlag = cli.IntFlag{
		Name:  "clique.signatures",
		Usage: "Number of recent block signatures to keep in memory",
		Value: 16384,
	}
	CliqueDataDirFlag = flags.DirectoryFlag{
		Name:  "clique.datadir",
		Usage: "Path to clique db folder",
		Value: "",
	}

	SnapKeepBlocksFlag = cli.BoolFlag{
		Name:  ethconfig.FlagSnapKeepBlocks,
		Usage: "Keep ancient blocks in db (useful for debug)",
	}
	SnapStopFlag = cli.BoolFlag{
		Name:  ethconfig.FlagSnapStop,
		Usage: "Workaround to stop producing new snapshots, if you meet some snapshots-related critical bug. It will stop move historical data from DB to new immutable snapshots. DB will grow and may slightly slow-down - and removing this flag in future will not fix this effect (db size will not greatly reduce).",
	}
	SnapStateStopFlag = cli.BoolFlag{
		Name:  ethconfig.FlagSnapStateStop,
		Usage: "Workaround to stop producing new state files, if you meet some state-related critical bug. It will stop aggregate DB history in a state files. DB will grow and may slightly slow-down - and removing this flag in future will not fix this effect (db size will not greatly reduce).",
	}
	TorrentVerbosityFlag = cli.IntFlag{
		Name:  "torrent.verbosity",
		Value: 2,
		Usage: "0=silent, 1=error, 2=warn, 3=info, 4=debug, 5=detail (must set --verbosity to equal or higher level and has default: 2)",
	}
	TorrentDownloadRateFlag = cli.StringFlag{
		Name:  "torrent.download.rate",
		Value: "128mb",
		Usage: "Bytes per second, example: 32mb",
	}
	TorrentUploadRateFlag = cli.StringFlag{
		Name:  "torrent.upload.rate",
		Value: "4mb",
		Usage: "Bytes per second, example: 32mb",
	}
	TorrentDownloadSlotsFlag = cli.IntFlag{
		Name:  "torrent.download.slots",
		Value: 128,
		Usage: "Amount of files to download in parallel.",
	}
	TorrentStaticPeersFlag = cli.StringFlag{
		Name:  "torrent.staticpeers",
		Usage: "Comma separated host:port to connect to",
		Value: "",
	}
	NoDownloaderFlag = cli.BoolFlag{
		Name:  "no-downloader",
		Usage: "Disables downloader component",
	}
	DownloaderVerifyFlag = cli.BoolFlag{
		Name:  "downloader.verify",
		Usage: "Verify snapshots on startup. It will not report problems found, but re-download broken pieces.",
	}
	DisableIPV6 = cli.BoolFlag{
		Name:  "downloader.disable.ipv6",
		Usage: "Turns off ipv6 for the downloader",
		Value: false,
	}

	DisableIPV4 = cli.BoolFlag{
		Name:  "downloader.disable.ipv4",
		Usage: "Turns off ipv4 for the downloader",
		Value: false,
	}
	TorrentPortFlag = cli.IntFlag{
		Name:  "torrent.port",
		Value: 42069,
		Usage: "Port to listen and serve BitTorrent protocol",
	}
	TorrentMaxPeersFlag = cli.IntFlag{
		Name:  "torrent.maxpeers",
		Value: 100,
		Usage: "Unused parameter (reserved for future use)",
	}
	TorrentConnsPerFileFlag = cli.IntFlag{
		Name:  "torrent.conns.perfile",
		Value: 10,
		Usage: "Number of connections per file",
	}
	DbPageSizeFlag = cli.StringFlag{
		Name:  "db.pagesize",
		Usage: "DB is splitted to 'pages' of fixed size. Can't change DB creation. Must be power of 2 and '256b <= pagesize <= 64kb'. Default: equal to OperationSystem's pageSize. Bigger pageSize causing: 1. More writes to disk during commit 2. Smaller b-tree high 3. Less fragmentation 4. Less overhead on 'free-pages list' maintainance (a bit faster Put/Commit) 5. If expecting DB-size > 8Tb then set pageSize >= 8Kb",
		Value: "4KB",
	}
	DbSizeLimitFlag = cli.StringFlag{
		Name:  "db.size.limit",
		Usage: "Runtime limit of chaindata db size (can change at any time)",
		Value: (200 * datasize.GB).String(),
	}
	DbWriteMapFlag = cli.BoolFlag{
		Name:  "db.writemap",
		Usage: "Enable WRITE_MAP feature for fast database writes and fast commit times",
		Value: true,
	}
	HealthCheckFlag = cli.BoolFlag{
		Name:  "healthcheck",
		Usage: "Enabling grpc health check",
	}

	WebSeedsFlag = cli.StringFlag{
		Name:  "webseed",
		Usage: "Comma-separated URL's, holding metadata about network-support infrastructure (like S3 buckets with snapshots, bootnodes, etc...)",
		Value: "",
	}

	HeimdallURLFlag = cli.StringFlag{
		Name:  "bor.heimdall",
		Usage: "URL of Heimdall service",
		Value: "http://localhost:1317",
	}

	// WithoutHeimdallFlag no heimdall (for testing purpose)
	WithoutHeimdallFlag = cli.BoolFlag{
		Name:  "bor.withoutheimdall",
		Usage: "Run without Heimdall service (for testing purposes)",
	}

	BorBlockPeriodFlag = cli.BoolFlag{
		Name:  "bor.period",
		Usage: "Override the bor block period (for testing purposes)",
	}

	BorBlockSizeFlag = cli.BoolFlag{
		Name:  "bor.minblocksize",
		Usage: "Ignore the bor block period and wait for 'blocksize' transactions (for testing purposes)",
	}

	WithHeimdallMilestones = cli.BoolFlag{
		Name:  "bor.milestone",
		Usage: "Enabling bor milestone processing",
		Value: true,
	}

	WithHeimdallWaypoints = cli.BoolFlag{
		Name:  "bor.waypoints",
		Usage: "Enabling bor waypont recording",
		Value: false,
	}

	PolygonSyncFlag = cli.BoolFlag{
		Name:  "polygon.sync",
		Usage: "Enabling syncing using the new polygon sync component",
	}

	PolygonSyncStageFlag = cli.BoolFlag{
		Name:  "polygon.sync.stage",
		Usage: "Enabling syncing with a stage that uses the polygon sync component",
	}

	ConfigFlag = cli.StringFlag{
		Name:  "config",
		Usage: "Sets erigon flags from YAML/TOML file",
		Value: "",
	}

	CaplinDiscoveryAddrFlag = cli.StringFlag{
		Name:  "caplin.discovery.addr",
		Usage: "Address for Caplin DISCV5 protocol",
		Value: "127.0.0.1",
	}
	CaplinDiscoveryPortFlag = cli.Uint64Flag{
		Name:  "caplin.discovery.port",
		Usage: "Port for Caplin DISCV5 protocol",
		Value: 4000,
	}
	CaplinDiscoveryTCPPortFlag = cli.Uint64Flag{
		Name:  "caplin.discovery.tcpport",
		Usage: "TCP Port for Caplin DISCV5 protocol",
		Value: 4001,
	}
	CaplinCheckpointSyncUrlFlag = cli.StringSliceFlag{
		Name:  "caplin.checkpoint-sync-url",
		Usage: "checkpoint sync endpoint",
		Value: cli.NewStringSlice(),
	}
	CaplinSubscribeAllTopicsFlag = cli.BoolFlag{
		Name:  "caplin.subscibe-all-topics",
		Usage: "Subscribe to all gossip topics",
		Value: false,
	}
	CaplinMevRelayUrl = cli.StringFlag{
		Name:  "caplin.mev-relay-url",
		Usage: "MEV relay endpoint. Caplin runs in builder mode if this is set",
		Value: "",
	}
	CaplinValidatorMonitorFlag = cli.BoolFlag{
		Name:  "caplin.validator-monitor",
		Usage: "Enable caplin validator monitoring metrics",
		Value: false,
	}
	CaplinMaxPeerCount = cli.Uint64Flag{
		Name:  "caplin.max-peer-count",
		Usage: "Max number of peers to connect",
		Value: 128,
	}

	SentinelAddrFlag = cli.StringFlag{
		Name:  "sentinel.addr",
		Usage: "Address for sentinel",
		Value: "localhost",
	}
	SentinelPortFlag = cli.Uint64Flag{
		Name:  "sentinel.port",
		Usage: "Port for sentinel",
		Value: 7777,
	}
	SentinelBootnodes = cli.StringSliceFlag{
		Name:  "sentinel.bootnodes",
		Usage: "Comma separated enode URLs for P2P discovery bootstrap",
		Value: cli.NewStringSlice(),
	}

	OtsSearchMaxCapFlag = cli.Uint64Flag{
		Name:  "ots.search.max.pagesize",
		Usage: "Max allowed page size for search methods",
		Value: 25,
	}

	DiagnosticsURLFlag = cli.StringFlag{
		Name:  "diagnostics.addr",
		Usage: "Address of the diagnostics system provided by the support team",
	}

	DiagnosticsInsecureFlag = cli.BoolFlag{
		Name:  "diagnostics.insecure",
		Usage: "Allows communication with diagnostics system using self-signed TLS certificates",
	}

	DiagnosticsSessionsFlag = cli.StringSliceFlag{
		Name:  "diagnostics.ids",
		Usage: "Comma separated list of support session ids to connect to",
	}

	SilkwormExecutionFlag = cli.BoolFlag{
		Name:  "silkworm.exec",
		Usage: "Enable Silkworm block execution",
	}
	SilkwormRpcDaemonFlag = cli.BoolFlag{
		Name:  "silkworm.rpc",
		Usage: "Enable embedded Silkworm RPC service",
	}
	SilkwormSentryFlag = cli.BoolFlag{
		Name:  "silkworm.sentry",
		Usage: "Enable embedded Silkworm Sentry service",
	}
	SilkwormVerbosityFlag = cli.StringFlag{
		Name:  "silkworm.verbosity",
		Usage: "Set the log level for Silkworm console logs",
		Value: log.LvlInfo.String(),
	}
	SilkwormNumContextsFlag = cli.UintFlag{
		Name:  "silkworm.contexts",
		Usage: "Number of I/O contexts used in embedded Silkworm RPC and Sentry services (zero means use default in Silkworm)",
		Value: 0,
	}
	SilkwormRpcLogEnabledFlag = cli.BoolFlag{
		Name:  "silkworm.rpc.log",
		Usage: "Enable interface log for embedded Silkworm RPC service",
		Value: false,
	}
	SilkwormRpcLogMaxFileSizeFlag = cli.UintFlag{
		Name:  "silkworm.rpc.log.maxsize",
		Usage: "Max interface log file size in MB for embedded Silkworm RPC service",
		Value: 1,
	}
	SilkwormRpcLogMaxFilesFlag = cli.UintFlag{
		Name:  "silkworm.rpc.log.maxfiles",
		Usage: "Max interface log files for embedded Silkworm RPC service",
		Value: 100,
	}
	SilkwormRpcLogDumpResponseFlag = cli.BoolFlag{
		Name:  "silkworm.rpc.log.response",
		Usage: "Dump responses in interface logs for embedded Silkworm RPC service",
		Value: false,
	}
	SilkwormRpcNumWorkersFlag = cli.UintFlag{
		Name:  "silkworm.rpc.workers",
		Usage: "Number of worker threads used in embedded Silkworm RPC service (zero means use default in Silkworm)",
		Value: 0,
	}
	SilkwormRpcJsonCompatibilityFlag = cli.BoolFlag{
		Name:  "silkworm.rpc.compatibility",
		Usage: "Preserve JSON-RPC compatibility using embedded Silkworm RPC service",
		Value: true,
	}

	BeaconAPIFlag = cli.StringSliceFlag{
		Name:  "beacon.api",
		Usage: "Enable beacon API (available endpoints: beacon, builder, config, debug, events, node, validator, lighthouse)",
	}
	BeaconApiProtocolFlag = cli.StringFlag{
		Name:  "beacon.api.protocol",
		Usage: "Protocol for beacon API",
		Value: "tcp",
	}
	BeaconApiReadTimeoutFlag = cli.Uint64Flag{
		Name:  "beacon.api.read.timeout",
		Usage: "Sets the seconds for a read time out in the beacon api",
		Value: 5,
	}
	BeaconApiWriteTimeoutFlag = cli.Uint64Flag{
		Name:  "beacon.api.write.timeout",
		Usage: "Sets the seconds for a write time out in the beacon api",
		Value: 5,
	}
	BeaconApiIdleTimeoutFlag = cli.Uint64Flag{
		Name:  "beacon.api.ide.timeout",
		Usage: "Sets the seconds for a write time out in the beacon api",
		Value: 25,
	}
	BeaconApiAddrFlag = cli.StringFlag{
		Name:  "beacon.api.addr",
		Usage: "sets the host to listen for beacon api requests",
		Value: "localhost",
	}
	BeaconApiPortFlag = cli.UintFlag{
		Name:  "beacon.api.port",
		Usage: "sets the port to listen for beacon api requests",
		Value: 5555,
	}
	RPCSlowFlag = cli.DurationFlag{
		Name:  "rpc.slow",
		Usage: "Print in logs RPC requests slower than given threshold: 100ms, 1s, 1m. Exluded methods: " + strings.Join(rpccfg.SlowLogBlackList, ","),
		Value: 0,
	}
	CaplinBackfillingFlag = cli.BoolFlag{
		Name:  "caplin.backfilling",
		Usage: "sets whether backfilling is enabled for caplin",
		Value: false,
	}
	CaplinBlobBackfillingFlag = cli.BoolFlag{
		Name:  "caplin.backfilling.blob",
		Usage: "sets whether backfilling is enabled for caplin",
		Value: false,
	}
	CaplinDisableBlobPruningFlag = cli.BoolFlag{
		Name:  "caplin.backfilling.blob.no-pruning",
		Usage: "disable blob pruning in caplin",
		Value: false,
	}
	CaplinDisableCheckpointSyncFlag = cli.BoolFlag{
		Name:  "caplin.checkpoint-sync.disable",
		Usage: "disable checkpoint sync in caplin",
		Value: false,
	}
	CaplinArchiveFlag = cli.BoolFlag{
		Name:  "caplin.archive",
		Usage: "enables archival node in caplin",
		Value: false,
	}
	CaplinEnableSnapshotGeneration = cli.BoolFlag{
		Name:  "caplin.snapgen",
		Usage: "enables snapshot generation in caplin",
		Value: false,
	}
	BeaconApiAllowCredentialsFlag = cli.BoolFlag{
		Name:  "beacon.api.cors.allow-credentials",
		Usage: "set the cors' allow credentials",
		Value: false,
	}
	BeaconApiAllowMethodsFlag = cli.StringSliceFlag{
		Name:  "beacon.api.cors.allow-methods",
		Usage: "set the cors' allow methods",
		Value: cli.NewStringSlice("GET", "POST", "PUT", "DELETE", "OPTIONS"),
	}
	BeaconApiAllowOriginsFlag = cli.StringSliceFlag{
		Name:  "beacon.api.cors.allow-origins",
		Usage: "set the cors' allow origins",
		Value: cli.NewStringSlice(),
	}
	CaplinCustomConfigFlag = cli.StringFlag{
		Name:  "caplin.custom-config",
		Usage: "set the custom config for caplin",
		Value: "",
	}
	CaplinCustomGenesisFlag = cli.StringFlag{
		Name:  "caplin.custom-genesis",
		Usage: "set the custom genesis for caplin",
		Value: "",
	}
	DiagDisabledFlag = cli.BoolFlag{
		Name:  "diagnostics.disabled",
		Usage: "Disable diagnostics",
		Value: false,
	}
	DiagEndpointAddrFlag = cli.StringFlag{
		Name:  "diagnostics.endpoint.addr",
		Usage: "Diagnostics HTTP server listening interface",
		Value: "127.0.0.1",
	}
	DiagEndpointPortFlag = cli.UintFlag{
		Name:  "diagnostics.endpoint.port",
		Usage: "Diagnostics HTTP server listening port",
		Value: 6062,
	}
	DiagSpeedTestFlag = cli.BoolFlag{
		Name:  "diagnostics.speedtest",
		Usage: "Enable speed test",
		Value: false,
	}
)

var MetricFlags = []cli.Flag{&MetricsEnabledFlag, &MetricsHTTPFlag, &MetricsPortFlag, &DiagDisabledFlag, &DiagEndpointAddrFlag, &DiagEndpointPortFlag, &DiagSpeedTestFlag}

var DiagnosticsFlags = []cli.Flag{&DiagnosticsURLFlag, &DiagnosticsURLFlag, &DiagnosticsSessionsFlag}

// setNodeKey loads a node key from command line flags if provided,
// otherwise it tries to load it from datadir,
// otherwise it generates a new key in datadir.
func setNodeKey(ctx *cli.Context, cfg *p2p.Config, datadir string) {
	file := ctx.String(NodeKeyFileFlag.Name)
	hex := ctx.String(NodeKeyHexFlag.Name)

	config := p2p.NodeKeyConfig{}
	key, err := config.LoadOrParseOrGenerateAndSave(file, hex, datadir)
	if err != nil {
		Fatalf("%v", err)
	}
	cfg.PrivateKey = key
}

// setNodeUserIdent creates the user identifier from CLI flags.
func setNodeUserIdent(ctx *cli.Context, cfg *nodecfg.Config) {
	if identity := ctx.String(IdentityFlag.Name); len(identity) > 0 {
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

	nodes, err := GetBootnodesFromFlags(ctx.String(BootnodesFlag.Name), ctx.String(ChainFlag.Name))
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

	nodes, err := GetBootnodesFromFlags(ctx.String(BootnodesFlag.Name), ctx.String(ChainFlag.Name))
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
		urls = libcommon.CliString2Array(urlsStr)
	} else {
		urls = params.BootnodeURLsOfChain(chain)
	}
	return ParseNodesFromURLs(urls)
}

func setStaticPeers(ctx *cli.Context, cfg *p2p.Config) {
	var urls []string
	if ctx.IsSet(StaticPeersFlag.Name) {
		urls = libcommon.CliString2Array(ctx.String(StaticPeersFlag.Name))
	} else {
		chain := ctx.String(ChainFlag.Name)
		urls = params.StaticPeerURLsOfChain(chain)
	}

	nodes, err := ParseNodesFromURLs(urls)
	if err != nil {
		Fatalf("Option %s: %v", StaticPeersFlag.Name, err)
	}

	cfg.StaticNodes = nodes
}

func setTrustedPeers(ctx *cli.Context, cfg *p2p.Config) {
	if !ctx.IsSet(TrustedPeersFlag.Name) {
		return
	}

	urls := libcommon.CliString2Array(ctx.String(TrustedPeersFlag.Name))
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
//   - doesn't setup bootnodes - they will set when genesisHash will know
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
	port uint,
	protocol uint,
	allowedPorts []uint,
	metricsEnabled bool,
) (*p2p.Config, error) {
	var enodeDBPath string
	switch protocol {
	case direct.ETH66:
		enodeDBPath = filepath.Join(dirs.Nodes, "eth66")
	case direct.ETH67:
		enodeDBPath = filepath.Join(dirs.Nodes, "eth67")
	case direct.ETH68:
		enodeDBPath = filepath.Join(dirs.Nodes, "eth68")
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
		NodeDatabase:    enodeDBPath,
		AllowedPorts:    allowedPorts,
		TmpDir:          dirs.Tmp,
		MetricsEnabled:  metricsEnabled,
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
	cfg.NATSpec = natSetting
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
	if ctx.IsSet(ListenPortFlag.Name) {
		cfg.ListenAddr = fmt.Sprintf(":%d", ctx.Int(ListenPortFlag.Name))
	}
	if ctx.IsSet(P2pProtocolVersionFlag.Name) {
		cfg.ProtocolVersion = ctx.UintSlice(P2pProtocolVersionFlag.Name)
	}
	if ctx.IsSet(SentryAddrFlag.Name) {
		cfg.SentryAddr = libcommon.CliString2Array(ctx.String(SentryAddrFlag.Name))
	}
	// TODO cli lib doesn't store defaults for UintSlice properly so we have to get value directly
	cfg.AllowedPorts = P2pProtocolAllowedPorts.Value.Value()
	if ctx.IsSet(P2pProtocolAllowedPorts.Name) {
		cfg.AllowedPorts = ctx.UintSlice(P2pProtocolAllowedPorts.Name)
	}

	if ctx.IsSet(ListenPortFlag.Name) {
		// add non-default port to allowed port list
		lp := ctx.Int(ListenPortFlag.Name)
		found := false
		for _, p := range cfg.AllowedPorts {
			if int(p) == lp {
				found = true
				break
			}
		}
		if !found {
			cfg.AllowedPorts = append([]uint{uint(lp)}, cfg.AllowedPorts...)
		}
	}
}

// setNAT creates a port mapper from command line flags.
func setNAT(ctx *cli.Context, cfg *p2p.Config) {
	if ctx.IsSet(NATFlag.Name) {
		natSetting := ctx.String(NATFlag.Name)
		natif, err := nat.Parse(natSetting)
		if err != nil {
			Fatalf("Option %s: %v", NATFlag.Name, err)
		}
		cfg.NAT = natif
		cfg.NATSpec = natSetting
	}
}

// setEtherbase retrieves the etherbase from the directly specified
// command line flags.
func setEtherbase(ctx *cli.Context, cfg *ethconfig.Config) {
	var etherbase string
	if ctx.IsSet(MinerEtherbaseFlag.Name) {
		etherbase = ctx.String(MinerEtherbaseFlag.Name)
		if etherbase != "" {
			cfg.Miner.Etherbase = libcommon.HexToAddress(etherbase)
		}
	}

	setSigKey := func(ctx *cli.Context, cfg *ethconfig.Config) {
		if ctx.IsSet(MinerSigningKeyFileFlag.Name) {
			signingKeyFileName := ctx.String(MinerSigningKeyFileFlag.Name)
			key, err := crypto.LoadECDSA(signingKeyFileName)
			if err != nil {
				panic(err)
			}
			cfg.Miner.SigKey = key
		}
	}

	if chainName := ctx.String(ChainFlag.Name); chainName == networkname.DevChainName || chainName == networkname.BorDevnetChainName {
		if etherbase == "" {
			cfg.Miner.Etherbase = core.DevnetEtherbase
		}

		cfg.Miner.SigKey = core.DevnetSignKey(cfg.Miner.Etherbase)

		setSigKey(ctx, cfg)
	}

	chainsWithValidatorMode := map[string]bool{}
	if _, ok := chainsWithValidatorMode[ctx.String(ChainFlag.Name)]; ok || ctx.IsSet(MinerSigningKeyFileFlag.Name) {
		if ctx.IsSet(MiningEnabledFlag.Name) && !ctx.IsSet(MinerSigningKeyFileFlag.Name) {
			panic(fmt.Sprintf("Flag --%s is required in %s chain with --%s flag", MinerSigningKeyFileFlag.Name, ChainFlag.Name, MiningEnabledFlag.Name))
		}
		setSigKey(ctx, cfg)
		if cfg.Miner.SigKey != nil {
			cfg.Miner.Etherbase = crypto.PubkeyToAddress(cfg.Miner.SigKey.PublicKey)
		}
	}
}

func SetP2PConfig(ctx *cli.Context, cfg *p2p.Config, nodeName, datadir string, logger log.Logger) {
	cfg.Name = nodeName
	setNodeKey(ctx, cfg, datadir)
	setNAT(ctx, cfg)
	setListenAddress(ctx, cfg)
	setBootstrapNodes(ctx, cfg)
	setBootstrapNodesV5(ctx, cfg)
	setStaticPeers(ctx, cfg)
	setTrustedPeers(ctx, cfg)

	if ctx.IsSet(MaxPeersFlag.Name) {
		cfg.MaxPeers = ctx.Int(MaxPeersFlag.Name)
	}

	if ctx.IsSet(MaxPendingPeersFlag.Name) {
		cfg.MaxPendingPeers = ctx.Int(MaxPendingPeersFlag.Name)
	}
	if ctx.IsSet(NoDiscoverFlag.Name) {
		cfg.NoDiscovery = true
	}

	if ctx.IsSet(DiscoveryV5Flag.Name) {
		cfg.DiscoveryV5 = ctx.Bool(DiscoveryV5Flag.Name)
	}

	if ctx.IsSet(MetricsEnabledFlag.Name) {
		cfg.MetricsEnabled = ctx.Bool(MetricsEnabledFlag.Name)
	}

	ethPeers := cfg.MaxPeers
	logger.Info("Maximum peer count", "ETH", ethPeers, "total", cfg.MaxPeers)

	if netrestrict := ctx.String(NetrestrictFlag.Name); netrestrict != "" {
		list, err := netutil.ParseNetlist(netrestrict)
		if err != nil {
			Fatalf("Option %q: %v", NetrestrictFlag.Name, err)
		}
		cfg.NetRestrict = list
	}

	if ctx.String(ChainFlag.Name) == networkname.DevChainName {
		// --dev mode can't use p2p networking.
		//cfg.MaxPeers = 0 // It can have peers otherwise local sync is not possible
		if !ctx.IsSet(ListenPortFlag.Name) {
			cfg.ListenAddr = ":0"
		}
		cfg.NoDiscovery = true
		cfg.DiscoveryV5 = false
		logger.Info("Development chain flags set", "--nodiscover", cfg.NoDiscovery, "--v5disc", cfg.DiscoveryV5, "--port", cfg.ListenAddr)
	}
}

// SetNodeConfig applies node-related command line flags to the config.
func SetNodeConfig(ctx *cli.Context, cfg *nodecfg.Config, logger log.Logger) {
	setDataDir(ctx, cfg)
	setNodeUserIdent(ctx, cfg)
	SetP2PConfig(ctx, &cfg.P2P, cfg.NodeName(), cfg.Dirs.DataDir, logger)

	cfg.SentryLogPeerInfo = ctx.IsSet(SentryLogPeerInfoFlag.Name)
}

func SetNodeConfigCobra(cmd *cobra.Command, cfg *nodecfg.Config) {
	flags := cmd.Flags()
	//SetP2PConfig(ctx, &cfg.P2P)
	setNodeUserIdentCobra(flags, cfg)
	setDataDirCobra(flags, cfg)
}

func setDataDir(ctx *cli.Context, cfg *nodecfg.Config) {
	if ctx.IsSet(DataDirFlag.Name) {
		cfg.Dirs = datadir.New(ctx.String(DataDirFlag.Name))
	} else {
		cfg.Dirs = datadir.New(paths.DataDirForNetwork(paths.DefaultDataDir(), ctx.String(ChainFlag.Name)))
	}
	snapcfg.LoadRemotePreverified()

	cfg.MdbxPageSize = flags.DBPageSizeFlagUnmarshal(ctx, DbPageSizeFlag.Name, DbPageSizeFlag.Usage)
	if err := cfg.MdbxDBSizeLimit.UnmarshalText([]byte(ctx.String(DbSizeLimitFlag.Name))); err != nil {
		panic(err)
	}
	cfg.MdbxWriteMap = ctx.Bool(DbWriteMapFlag.Name)
	szLimit := cfg.MdbxDBSizeLimit.Bytes()
	if szLimit%256 != 0 || szLimit < 256 {
		panic(fmt.Errorf("invalid --db.size.limit: %s=%d, see: %s", ctx.String(DbSizeLimitFlag.Name), szLimit, DbSizeLimitFlag.Usage))
	}
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
		cfg.Dirs = datadir.New(dirname)
	} else {
		cfg.Dirs = datadir.New(paths.DataDirForNetwork(paths.DefaultDataDir(), chain))
	}
}

func setGPO(ctx *cli.Context, cfg *gaspricecfg.Config) {
	if ctx.IsSet(GpoBlocksFlag.Name) {
		cfg.Blocks = ctx.Int(GpoBlocksFlag.Name)
	}
	if ctx.IsSet(GpoPercentileFlag.Name) {
		cfg.Percentile = ctx.Int(GpoPercentileFlag.Name)
	}
	if ctx.IsSet(GpoMaxGasPriceFlag.Name) {
		cfg.MaxPrice = big.NewInt(ctx.Int64(GpoMaxGasPriceFlag.Name))
	}
}

// nolint
func setGPOCobra(f *pflag.FlagSet, cfg *gaspricecfg.Config) {
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

func setTxPool(ctx *cli.Context, fullCfg *ethconfig.Config) {
	cfg := &fullCfg.DeprecatedTxPool
	if ctx.IsSet(TxPoolDisableFlag.Name) || TxPoolDisableFlag.Value {
		cfg.Disable = true
	}
	if ctx.IsSet(TxPoolLocalsFlag.Name) {
		locals := libcommon.CliString2Array(ctx.String(TxPoolLocalsFlag.Name))
		for _, account := range locals {
			if !libcommon.IsHexAddress(account) {
				Fatalf("Invalid account in --txpool.locals: %s", account)
			} else {
				cfg.Locals = append(cfg.Locals, libcommon.HexToAddress(account))
			}
		}
	}
	if ctx.IsSet(TxPoolNoLocalsFlag.Name) {
		cfg.NoLocals = ctx.Bool(TxPoolNoLocalsFlag.Name)
	}
	if ctx.IsSet(TxPoolPriceLimitFlag.Name) {
		cfg.PriceLimit = ctx.Uint64(TxPoolPriceLimitFlag.Name)
	}
	if ctx.IsSet(TxPoolPriceBumpFlag.Name) {
		cfg.PriceBump = ctx.Uint64(TxPoolPriceBumpFlag.Name)
	}
	if ctx.IsSet(TxPoolBlobPriceBumpFlag.Name) {
		fullCfg.TxPool.BlobPriceBump = ctx.Uint64(TxPoolBlobPriceBumpFlag.Name)
	}
	if ctx.IsSet(TxPoolAccountSlotsFlag.Name) {
		cfg.AccountSlots = ctx.Uint64(TxPoolAccountSlotsFlag.Name)
	}
	if ctx.IsSet(TxPoolBlobSlotsFlag.Name) {
		fullCfg.TxPool.BlobSlots = ctx.Uint64(TxPoolBlobSlotsFlag.Name)
	}
	if ctx.IsSet(TxPoolTotalBlobPoolLimit.Name) {
		fullCfg.TxPool.TotalBlobPoolLimit = ctx.Uint64(TxPoolTotalBlobPoolLimit.Name)
	}
	if ctx.IsSet(TxPoolGlobalSlotsFlag.Name) {
		cfg.GlobalSlots = ctx.Uint64(TxPoolGlobalSlotsFlag.Name)
	}
	if ctx.IsSet(TxPoolAccountQueueFlag.Name) {
		cfg.AccountQueue = ctx.Uint64(TxPoolAccountQueueFlag.Name)
	}
	if ctx.IsSet(TxPoolGlobalQueueFlag.Name) {
		cfg.GlobalQueue = ctx.Uint64(TxPoolGlobalQueueFlag.Name)
	}
	if ctx.IsSet(TxPoolGlobalBaseFeeSlotsFlag.Name) {
		cfg.GlobalBaseFeeQueue = ctx.Uint64(TxPoolGlobalBaseFeeSlotsFlag.Name)
	}
	if ctx.IsSet(TxPoolLifetimeFlag.Name) {
		cfg.Lifetime = ctx.Duration(TxPoolLifetimeFlag.Name)
	}
	if ctx.IsSet(TxPoolTraceSendersFlag.Name) {
		// Parse the command separated flag
		senderHexes := libcommon.CliString2Array(ctx.String(TxPoolTraceSendersFlag.Name))
		cfg.TracedSenders = make([]string, len(senderHexes))
		for i, senderHex := range senderHexes {
			sender := libcommon.HexToAddress(senderHex)
			cfg.TracedSenders[i] = string(sender[:])
		}
	}
	if ctx.IsSet(TxPoolBlobPriceBumpFlag.Name) {
		fullCfg.TxPool.BlobPriceBump = ctx.Uint64(TxPoolBlobPriceBumpFlag.Name)
	}
	if ctx.IsSet(DbWriteMapFlag.Name) {
		fullCfg.TxPool.MdbxWriteMap = ctx.Bool(DbWriteMapFlag.Name)
	}
	cfg.CommitEvery = common2.RandomizeDuration(ctx.Duration(TxPoolCommitEveryFlag.Name))
}

func setEthash(ctx *cli.Context, datadir string, cfg *ethconfig.Config) {
	if ctx.IsSet(EthashDatasetDirFlag.Name) {
		cfg.Ethash.DatasetDir = ctx.String(EthashDatasetDirFlag.Name)
	} else {
		cfg.Ethash.DatasetDir = filepath.Join(datadir, "ethash-dags")
	}
	if ctx.IsSet(EthashCachesInMemoryFlag.Name) {
		cfg.Ethash.CachesInMem = ctx.Int(EthashCachesInMemoryFlag.Name)
	}
	if ctx.IsSet(EthashCachesLockMmapFlag.Name) {
		cfg.Ethash.CachesLockMmap = ctx.Bool(EthashCachesLockMmapFlag.Name)
	}
	if ctx.IsSet(FakePoWFlag.Name) {
		cfg.Ethash.PowMode = ethashcfg.ModeFake
	}
	if ctx.IsSet(EthashDatasetsLockMmapFlag.Name) {
		cfg.Ethash.DatasetsLockMmap = ctx.Bool(EthashDatasetsLockMmapFlag.Name)
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
	cfg.Etherbase = libcommon.HexToAddress(etherbase)
}

func setClique(ctx *cli.Context, cfg *params.ConsensusSnapshotConfig, datadir string) {
	cfg.CheckpointInterval = ctx.Uint64(CliqueSnapshotCheckpointIntervalFlag.Name)
	cfg.InmemorySnapshots = ctx.Int(CliqueSnapshotInmemorySnapshotsFlag.Name)
	cfg.InmemorySignatures = ctx.Int(CliqueSnapshotInmemorySignaturesFlag.Name)
	if ctx.IsSet(CliqueDataDirFlag.Name) {
		cfg.DBPath = filepath.Join(ctx.String(CliqueDataDirFlag.Name), "clique", "db")
	} else {
		cfg.DBPath = filepath.Join(datadir, "clique", "db")
	}
}

func setBorConfig(ctx *cli.Context, cfg *ethconfig.Config) {
	cfg.HeimdallURL = ctx.String(HeimdallURLFlag.Name)
	cfg.WithoutHeimdall = ctx.Bool(WithoutHeimdallFlag.Name)
	cfg.WithHeimdallMilestones = ctx.Bool(WithHeimdallMilestones.Name)
	cfg.WithHeimdallWaypointRecording = ctx.Bool(WithHeimdallWaypoints.Name)
	borsnaptype.RecordWayPoints(cfg.WithHeimdallWaypointRecording)
	cfg.PolygonSync = ctx.Bool(PolygonSyncFlag.Name)
	cfg.PolygonSyncStage = ctx.Bool(PolygonSyncStageFlag.Name)
}

func setMiner(ctx *cli.Context, cfg *params.MiningConfig) {
	cfg.Enabled = ctx.IsSet(MiningEnabledFlag.Name)
	cfg.EnabledPOS = !ctx.IsSet(ProposingDisableFlag.Name)

	if cfg.Enabled && len(cfg.Etherbase.Bytes()) == 0 {
		panic(fmt.Sprintf("Erigon supports only remote miners. Flag --%s or --%s is required", MinerNotifyFlag.Name, MinerSigningKeyFileFlag.Name))
	}
	if ctx.IsSet(MinerNotifyFlag.Name) {
		cfg.Notify = libcommon.CliString2Array(ctx.String(MinerNotifyFlag.Name))
	}
	if ctx.IsSet(MinerExtraDataFlag.Name) {
		cfg.ExtraData = []byte(ctx.String(MinerExtraDataFlag.Name))
	}
	if ctx.IsSet(MinerGasLimitFlag.Name) {
		cfg.GasLimit = ctx.Uint64(MinerGasLimitFlag.Name)
	}
	if ctx.IsSet(MinerGasPriceFlag.Name) {
		cfg.GasPrice = flags.GlobalBig(ctx, MinerGasPriceFlag.Name)
	}
	if ctx.IsSet(MinerRecommitIntervalFlag.Name) {
		cfg.Recommit = ctx.Duration(MinerRecommitIntervalFlag.Name)
	}
	if ctx.IsSet(MinerNoVerfiyFlag.Name) {
		cfg.Noverify = ctx.Bool(MinerNoVerfiyFlag.Name)
	}
}

func setWhitelist(ctx *cli.Context, cfg *ethconfig.Config) {
	whitelist := ctx.String(WhitelistFlag.Name)
	if whitelist == "" {
		return
	}
	cfg.Whitelist = make(map[uint64]libcommon.Hash)
	for _, entry := range libcommon.CliString2Array(whitelist) {
		parts := strings.Split(entry, "=")
		if len(parts) != 2 {
			Fatalf("Invalid whitelist entry: %s", entry)
		}
		number, err := strconv.ParseUint(parts[0], 0, 64)
		if err != nil {
			Fatalf("Invalid whitelist block number %s: %v", parts[0], err)
		}
		var hash libcommon.Hash
		if err = hash.UnmarshalText([]byte(parts[1])); err != nil {
			Fatalf("Invalid whitelist hash %s: %v", parts[1], err)
		}
		cfg.Whitelist[number] = hash
	}
}

func setBeaconAPI(ctx *cli.Context, cfg *ethconfig.Config) error {
	allowed := ctx.StringSlice(BeaconAPIFlag.Name)
	if err := cfg.CaplinConfig.BeaconAPIRouter.UnwrapEndpointsList(allowed); err != nil {
		return err
	}

	cfg.CaplinConfig.BeaconAPIRouter.Protocol = ctx.String(BeaconApiProtocolFlag.Name)
	cfg.CaplinConfig.BeaconAPIRouter.Address = fmt.Sprintf("%s:%d", ctx.String(BeaconApiAddrFlag.Name), ctx.Int(BeaconApiPortFlag.Name))
	cfg.CaplinConfig.BeaconAPIRouter.ReadTimeTimeout = time.Duration(ctx.Uint64(BeaconApiReadTimeoutFlag.Name)) * time.Second
	cfg.CaplinConfig.BeaconAPIRouter.WriteTimeout = time.Duration(ctx.Uint64(BeaconApiWriteTimeoutFlag.Name)) * time.Second
	cfg.CaplinConfig.BeaconAPIRouter.IdleTimeout = time.Duration(ctx.Uint64(BeaconApiIdleTimeoutFlag.Name)) * time.Second
	cfg.CaplinConfig.BeaconAPIRouter.AllowedMethods = ctx.StringSlice(BeaconApiAllowMethodsFlag.Name)
	cfg.CaplinConfig.BeaconAPIRouter.AllowedOrigins = ctx.StringSlice(BeaconApiAllowOriginsFlag.Name)
	cfg.CaplinConfig.BeaconAPIRouter.AllowCredentials = ctx.Bool(BeaconApiAllowCredentialsFlag.Name)
	return nil
}

func setCaplin(ctx *cli.Context, cfg *ethconfig.Config) {
	// Caplin's block's backfilling is enabled if any of the following flags are set
	cfg.CaplinConfig.Backfilling = ctx.Bool(CaplinBackfillingFlag.Name) || ctx.Bool(CaplinArchiveFlag.Name) || ctx.Bool(CaplinBlobBackfillingFlag.Name)
	cfg.CaplinConfig.SnapshotGenerationEnabled = ctx.Bool(CaplinEnableSnapshotGeneration.Name)
	// More granularity here.
	cfg.CaplinConfig.BlobBackfilling = ctx.Bool(CaplinBlobBackfillingFlag.Name)
	cfg.CaplinConfig.BlobPruningDisabled = ctx.Bool(CaplinDisableBlobPruningFlag.Name)
	cfg.CaplinConfig.DisabledCheckpointSync = ctx.Bool(CaplinDisableCheckpointSyncFlag.Name)
	cfg.CaplinConfig.Archive = ctx.Bool(CaplinArchiveFlag.Name)
	cfg.CaplinConfig.MevRelayUrl = ctx.String(CaplinMevRelayUrl.Name)
	cfg.CaplinConfig.EnableValidatorMonitor = ctx.Bool(CaplinValidatorMonitorFlag.Name)
	if checkpointUrls := ctx.StringSlice(CaplinCheckpointSyncUrlFlag.Name); len(checkpointUrls) > 0 {
		clparams.ConfigurableCheckpointsURLs = checkpointUrls
	}
	cfg.CaplinConfig.CustomConfigPath = ctx.String(CaplinCustomConfigFlag.Name)
	cfg.CaplinConfig.CustomGenesisStatePath = ctx.String(CaplinCustomGenesisFlag.Name)
}

func setSilkworm(ctx *cli.Context, cfg *ethconfig.Config) {
	cfg.SilkwormExecution = ctx.Bool(SilkwormExecutionFlag.Name)
	cfg.SilkwormRpcDaemon = ctx.Bool(SilkwormRpcDaemonFlag.Name)
	cfg.SilkwormSentry = ctx.Bool(SilkwormSentryFlag.Name)
	cfg.SilkwormVerbosity = ctx.String(SilkwormVerbosityFlag.Name)
	cfg.SilkwormNumContexts = uint32(ctx.Uint64(SilkwormNumContextsFlag.Name))
	cfg.SilkwormRpcLogEnabled = ctx.Bool(SilkwormRpcLogEnabledFlag.Name)
	cfg.SilkwormRpcLogDirPath = logging.LogDirPath(ctx)
	cfg.SilkwormRpcLogMaxFileSize = uint16(ctx.Uint64(SilkwormRpcLogMaxFileSizeFlag.Name))
	cfg.SilkwormRpcLogMaxFiles = uint16(ctx.Uint(SilkwormRpcLogMaxFilesFlag.Name))
	cfg.SilkwormRpcLogDumpResponse = ctx.Bool(SilkwormRpcLogDumpResponseFlag.Name)
	cfg.SilkwormRpcNumWorkers = uint32(ctx.Uint64(SilkwormRpcNumWorkersFlag.Name))
	cfg.SilkwormRpcJsonCompatibility = ctx.Bool(SilkwormRpcJsonCompatibilityFlag.Name)
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
		name := flag.Names()[0]

		if i+1 < len(args) {
			switch option := args[i+1].(type) {
			case string:
				// Extended flag check, make sure value set doesn't conflict with passed in option
				if ctx.String(flag.Names()[0]) == option {
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
		if ctx.IsSet(flag.Names()[0]) {
			set = append(set, "--"+name)
		}
	}
	if len(set) > 1 {
		Fatalf("Flags %v can't be used at the same time", strings.Join(set, ", "))
	}
}

// SetEthConfig applies eth-related command line flags to the config.
func SetEthConfig(ctx *cli.Context, nodeConfig *nodecfg.Config, cfg *ethconfig.Config, logger log.Logger) {
	cfg.CaplinConfig.CaplinDiscoveryAddr = ctx.String(CaplinDiscoveryAddrFlag.Name)
	cfg.CaplinConfig.CaplinDiscoveryPort = ctx.Uint64(CaplinDiscoveryPortFlag.Name)
	cfg.CaplinConfig.CaplinDiscoveryTCPPort = ctx.Uint64(CaplinDiscoveryTCPPortFlag.Name)
	cfg.CaplinConfig.SubscribeAllTopics = ctx.Bool(CaplinSubscribeAllTopicsFlag.Name)
	cfg.CaplinConfig.MaxPeerCount = ctx.Uint64(CaplinMaxPeerCount.Name)

	cfg.CaplinConfig.SentinelAddr = ctx.String(SentinelAddrFlag.Name)
	cfg.CaplinConfig.SentinelPort = ctx.Uint64(SentinelPortFlag.Name)
	cfg.CaplinConfig.BootstrapNodes = ctx.StringSlice(SentinelBootnodes.Name)

	chain := ctx.String(ChainFlag.Name) // mainnet by default
	if ctx.IsSet(NetworkIdFlag.Name) {
		cfg.NetworkID = ctx.Uint64(NetworkIdFlag.Name)
		if cfg.NetworkID != 1 && !ctx.IsSet(ChainFlag.Name) {
			chainName, ok := networkid.NetworkNameByID[cfg.NetworkID]
			if !ok {
				chain = "" // don't default to mainnet if NetworkID != 1 and it's devchain or smth
			} else {
				chain = chainName // fetch network name from id if name wasn't provided
			}

		}
	} else {
		cfg.NetworkID = params.NetworkIDByChainName(chain)
	}

	cfg.Dirs = nodeConfig.Dirs
	cfg.Snapshot.KeepBlocks = ctx.Bool(SnapKeepBlocksFlag.Name)
	cfg.Snapshot.ProduceE2 = !ctx.Bool(SnapStopFlag.Name)
	cfg.Snapshot.ProduceE3 = !ctx.Bool(SnapStateStopFlag.Name)
	cfg.Snapshot.NoDownloader = ctx.Bool(NoDownloaderFlag.Name)
	cfg.Snapshot.Verify = ctx.Bool(DownloaderVerifyFlag.Name)
	cfg.Snapshot.DownloaderAddr = strings.TrimSpace(ctx.String(DownloaderAddrFlag.Name))
	if cfg.Snapshot.DownloaderAddr == "" {
		downloadRateStr := ctx.String(TorrentDownloadRateFlag.Name)
		uploadRateStr := ctx.String(TorrentUploadRateFlag.Name)
		var downloadRate, uploadRate datasize.ByteSize
		if err := downloadRate.UnmarshalText([]byte(downloadRateStr)); err != nil {
			panic(err)
		}
		if err := uploadRate.UnmarshalText([]byte(uploadRateStr)); err != nil {
			panic(err)
		}
		lvl, _, err := downloadercfg2.Int2LogLevel(ctx.Int(TorrentVerbosityFlag.Name))
		if err != nil {
			panic(err)
		}
		logger.Info("torrent verbosity", "level", lvl.LogString())
		version := "erigon: " + params.VersionWithCommit(params.GitCommit)
		webseedsList := libcommon.CliString2Array(ctx.String(WebSeedsFlag.Name))
		if known, ok := snapcfg.KnownWebseeds[chain]; ok {
			webseedsList = append(webseedsList, known...)
		}
		cfg.Downloader, err = downloadercfg2.New(cfg.Dirs, version, lvl, downloadRate, uploadRate,
			ctx.Int(TorrentPortFlag.Name), ctx.Int(TorrentConnsPerFileFlag.Name), ctx.Int(TorrentDownloadSlotsFlag.Name),
			libcommon.CliString2Array(ctx.String(TorrentStaticPeersFlag.Name)),
			webseedsList, chain, true, ctx.Bool(DbWriteMapFlag.Name),
		)
		if err != nil {
			panic(err)
		}
		downloadernat.DoNat(nodeConfig.P2P.NAT, cfg.Downloader.ClientConfig, logger)
	}

	nodeConfig.Http.Snap = cfg.Snapshot

	if ctx.Command.Name == "import" {
		cfg.ImportMode = true
	}

	setEtherbase(ctx, cfg)
	setGPO(ctx, &cfg.GPO)

	setTxPool(ctx, cfg)
	cfg.TxPool = ethconfig.DefaultTxPool2Config(cfg)
	cfg.TxPool.DBDir = nodeConfig.Dirs.TxPool

	setEthash(ctx, nodeConfig.Dirs.DataDir, cfg)
	setClique(ctx, &cfg.Clique, nodeConfig.Dirs.DataDir)
	setMiner(ctx, &cfg.Miner)
	setWhitelist(ctx, cfg)
	setBorConfig(ctx, cfg)
	setSilkworm(ctx, cfg)
	if err := setBeaconAPI(ctx, cfg); err != nil {
		log.Error("Failed to set beacon API", "err", err)
	}
	setCaplin(ctx, cfg)

	cfg.Ethstats = ctx.String(EthStatsURLFlag.Name)

	if ctx.IsSet(RPCGlobalGasCapFlag.Name) {
		cfg.RPCGasCap = ctx.Uint64(RPCGlobalGasCapFlag.Name)
	}
	if cfg.RPCGasCap != 0 {
		logger.Info("Set global gas cap", "cap", cfg.RPCGasCap)
	} else {
		logger.Info("Global gas cap disabled")
	}
	if ctx.IsSet(RPCGlobalTxFeeCapFlag.Name) {
		cfg.RPCTxFeeCap = ctx.Float64(RPCGlobalTxFeeCapFlag.Name)
	}
	if ctx.IsSet(NoDiscoverFlag.Name) {
		cfg.EthDiscoveryURLs = []string{}
	} else if ctx.IsSet(DNSDiscoveryFlag.Name) {
		urls := ctx.String(DNSDiscoveryFlag.Name)
		if urls == "" {
			cfg.EthDiscoveryURLs = []string{}
		} else {
			cfg.EthDiscoveryURLs = libcommon.CliString2Array(urls)
		}
	}

	// Override any default configs for hard coded networks.
	switch chain {
	default:
		genesis := core.GenesisBlockByChainName(chain)
		genesisHash := params.GenesisHashByChainName(chain)
		if (genesis == nil) || (genesisHash == nil) {
			Fatalf("ChainDB name is not recognized: %s", chain)
			return
		}
		cfg.Genesis = genesis
		SetDNSDiscoveryDefaults(cfg, *genesisHash)
	case "":
		if cfg.NetworkID == 1 {
			SetDNSDiscoveryDefaults(cfg, params.MainnetGenesisHash)
		}
	case networkname.DevChainName:
		// Create new developer account or reuse existing one
		developer := cfg.Miner.Etherbase
		if developer == (libcommon.Address{}) {
			Fatalf("Please specify developer account address using --miner.etherbase")
		}
		logger.Info("Using developer account", "address", developer)

		// Create a new developer genesis block or reuse existing one
		cfg.Genesis = core.DeveloperGenesisBlock(uint64(ctx.Int(DeveloperPeriodFlag.Name)), developer)
		logger.Info("Using custom developer period", "seconds", cfg.Genesis.Config.Clique.Period)
		if !ctx.IsSet(MinerGasPriceFlag.Name) {
			cfg.Miner.GasPrice = big.NewInt(1)
		}
	}

	if ctx.IsSet(OverridePragueFlag.Name) {
		cfg.OverridePragueTime = flags.GlobalBig(ctx, OverridePragueFlag.Name)
		cfg.TxPool.OverridePragueTime = cfg.OverridePragueTime
	}

	if ctx.IsSet(PragueEOFFlag.Name) {
		cfg.PragueEOFTime = flags.GlobalBig(ctx, PragueEOFFlag.Name)
	}

	if clparams.EmbeddedSupported(cfg.NetworkID) || cfg.CaplinConfig.IsDevnet() {
		cfg.InternalCL = !ctx.Bool(ExternalConsensusFlag.Name)
	}

	if ctx.IsSet(TrustedSetupFile.Name) {
		libkzg.SetTrustedSetupFilePath(ctx.String(TrustedSetupFile.Name))
	}

	if ctx.IsSet(TxPoolGossipDisableFlag.Name) {
		cfg.DisableTxPoolGossip = ctx.Bool(TxPoolGossipDisableFlag.Name)
	}
}

// SetDNSDiscoveryDefaults configures DNS discovery with the given URL if
// no URLs are set.
func SetDNSDiscoveryDefaults(cfg *ethconfig.Config, genesis libcommon.Hash) {
	if cfg.EthDiscoveryURLs != nil {
		return // already set through flags/config
	}
	protocol := "all"
	if url := params.KnownDNSNetwork(genesis, protocol); url != "" {
		cfg.EthDiscoveryURLs = []string{url}
	}
}

func SplitTagsFlag(tagsFlag string) map[string]string {
	tags := libcommon.CliString2Array(tagsFlag)
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

func CobraFlags(cmd *cobra.Command, urfaveCliFlagsLists ...[]cli.Flag) {
	flags := cmd.PersistentFlags()
	for _, urfaveCliFlags := range urfaveCliFlagsLists {
		for _, flag := range urfaveCliFlags {
			switch f := flag.(type) {
			case *cli.IntFlag:
				flags.Int(f.Name, f.Value, f.Usage)
			case *cli.UintFlag:
				flags.Uint(f.Name, f.Value, f.Usage)
			case *cli.StringFlag:
				flags.String(f.Name, f.Value, f.Usage)
			case *cli.BoolFlag:
				flags.Bool(f.Name, false, f.Usage)
			default:
				panic(fmt.Errorf("unexpected type: %T", flag))
			}
		}
	}
}
