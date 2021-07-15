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
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"text/tabwriter"
	"text/template"

	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/spf13/cobra"
	"github.com/spf13/pflag"
	"github.com/urfave/cli"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/consensus/ethash"
	"github.com/ledgerwatch/erigon/core"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/ethconfig"
	"github.com/ledgerwatch/erigon/eth/gasprice"
	"github.com/ledgerwatch/erigon/ethdb"
	"github.com/ledgerwatch/erigon/internal/flags"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/metrics"
	"github.com/ledgerwatch/erigon/node"
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
	cli.CommandHelpTemplate = flags.CommandHelpTemplate
	cli.HelpPrinter = printHelp
}

func printHelp(out io.Writer, templ string, data interface{}) {
	funcMap := template.FuncMap{"join": strings.Join}
	t := template.Must(template.New("help").Funcs(funcMap).Parse(templ))
	w := tabwriter.NewWriter(out, 38, 8, 2, ' ', 0)
	err := t.Execute(w, data)
	if err != nil {
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
		Value: params.MainnetChainName,
	}
	IdentityFlag = cli.StringFlag{
		Name:  "identity",
		Usage: "Custom node name",
	}
	DocRootFlag = DirectoryFlag{
		Name:  "docroot",
		Usage: "Document Root for HTTPClient file scheme",
		Value: DirectoryString(HomeDir()),
	}
	ExitWhenSyncedFlag = cli.BoolFlag{
		Name:  "exitwhensynced",
		Usage: "Exits after block synchronisation completes",
	}
	IterativeOutputFlag = cli.BoolFlag{
		Name:  "iterative",
		Usage: "Print streaming JSON iteratively, delimited by newlines",
	}
	ExcludeStorageFlag = cli.BoolFlag{
		Name:  "nostorage",
		Usage: "Exclude storage entries (save db lookups)",
	}
	ExcludeCodeFlag = cli.BoolFlag{
		Name:  "nocode",
		Usage: "Exclude contract code (save db lookups)",
	}
	WhitelistFlag = cli.StringFlag{
		Name:  "whitelist",
		Usage: "Comma separated block number-to-hash mappings to enforce (<number>=<hash>)",
	}
	OverrideLondonFlag = cli.Uint64Flag{
		Name:  "override.london",
		Usage: "Manually specify London fork-block, overriding the bundled setting",
	}
	DebugProtocolFlag = cli.BoolFlag{
		Name:  "debug-protocol",
		Usage: "Enable the DBG (debug) protocol",
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
	// Transaction pool settings
	TxPoolLocalsFlag = cli.StringFlag{
		Name:  "txpool.locals",
		Usage: "Comma separated accounts to treat as locals (no flush, priority inclusion)",
	}
	TxPoolNoLocalsFlag = cli.BoolFlag{
		Name:  "txpool.nolocals",
		Usage: "Disables price exemptions for locally submitted transactions",
	}
	TxPoolJournalFlag = cli.StringFlag{
		Name:  "txpool.journal",
		Usage: "Disk journal for local transaction to survive node restarts",
		Value: core.DefaultTxPoolConfig.Journal,
	}
	TxPoolRejournalFlag = cli.DurationFlag{
		Name:  "txpool.rejournal",
		Usage: "Time interval to regenerate the local transaction journal",
		Value: core.DefaultTxPoolConfig.Rejournal,
	}
	TxPoolPriceLimitFlag = cli.Uint64Flag{
		Name:  "txpool.pricelimit",
		Usage: "Minimum gas price limit to enforce for acceptance into the pool",
		Value: ethconfig.Defaults.TxPool.PriceLimit,
	}
	TxPoolPriceBumpFlag = cli.Uint64Flag{
		Name:  "txpool.pricebump",
		Usage: "Price bump percentage to replace an already existing transaction",
		Value: ethconfig.Defaults.TxPool.PriceBump,
	}
	TxPoolAccountSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.accountslots",
		Usage: "Minimum number of executable transaction slots guaranteed per account",
		Value: ethconfig.Defaults.TxPool.AccountSlots,
	}
	TxPoolGlobalSlotsFlag = cli.Uint64Flag{
		Name:  "txpool.globalslots",
		Usage: "Maximum number of executable transaction slots for all accounts",
		Value: ethconfig.Defaults.TxPool.GlobalSlots,
	}
	TxPoolAccountQueueFlag = cli.Uint64Flag{
		Name:  "txpool.accountqueue",
		Usage: "Maximum number of non-executable transaction slots permitted per account",
		Value: ethconfig.Defaults.TxPool.AccountQueue,
	}
	TxPoolGlobalQueueFlag = cli.Uint64Flag{
		Name:  "txpool.globalqueue",
		Usage: "Maximum number of non-executable transaction slots for all accounts",
		Value: ethconfig.Defaults.TxPool.GlobalQueue,
	}
	TxPoolLifetimeFlag = cli.DurationFlag{
		Name:  "txpool.lifetime",
		Usage: "Maximum amount of time non-executable transaction are queued",
		Value: ethconfig.Defaults.TxPool.Lifetime,
	}
	// Miner settings
	MiningEnabledFlag = cli.BoolFlag{
		Name:  "mine",
		Usage: "Enable mining",
	}
	MinerNotifyFlag = cli.StringFlag{
		Name:  "miner.notify",
		Usage: "Comma separated HTTP URL list to notify of new work packages",
	}
	MinerGasTargetFlag = cli.Uint64Flag{
		Name:  "miner.gastarget",
		Usage: "Target gas floor for mined blocks",
		Value: ethconfig.Defaults.Miner.GasFloor,
	}
	MinerGasLimitFlag = cli.Uint64Flag{
		Name:  "miner.gaslimit",
		Usage: "Target gas ceiling for mined blocks",
		Value: ethconfig.Defaults.Miner.GasCeil,
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
	MinerSigningKeyFlag = cli.StringFlag{
		Name:  "miner.sigkey",
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
	HTTPEnabledFlag = cli.BoolFlag{
		Name:  "http",
		Usage: "Enable the HTTP-RPC server",
	}
	HTTPListenAddrFlag = cli.StringFlag{
		Name:  "http.addr",
		Usage: "HTTP-RPC server listening interface",
		Value: node.DefaultHTTPHost,
	}
	HTTPPortFlag = cli.IntFlag{
		Name:  "http.port",
		Usage: "HTTP-RPC server listening port",
		Value: node.DefaultHTTPPort,
	}
	HTTPCORSDomainFlag = cli.StringFlag{
		Name:  "http.corsdomain",
		Usage: "Comma separated list of domains from which to accept cross origin requests (browser enforced)",
		Value: "",
	}
	HTTPVirtualHostsFlag = cli.StringFlag{
		Name:  "http.vhosts",
		Usage: "Comma separated list of virtual hostnames from which to accept requests (server enforced). Accepts '*' wildcard.",
		Value: strings.Join(node.DefaultConfig.HTTPVirtualHosts, ","),
	}
	HTTPApiFlag = cli.StringFlag{
		Name:  "http.api",
		Usage: "API's offered over the HTTP-RPC interface",
		Value: "",
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
		Value: node.DefaultWSHost,
	}
	WSPortFlag = cli.IntFlag{
		Name:  "ws.port",
		Usage: "WS-RPC server listening port",
		Value: node.DefaultWSPort,
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

	// Network Settings
	MaxPeersFlag = cli.IntFlag{
		Name:  "maxpeers",
		Usage: "Maximum number of network peers (network disabled if set to 0)",
		Value: node.DefaultConfig.P2P.MaxPeers,
	}
	MaxPendingPeersFlag = cli.IntFlag{
		Name:  "maxpendpeers",
		Usage: "Maximum number of pending connection attempts (defaults used if set to 0)",
		Value: node.DefaultConfig.P2P.MaxPendingPeers,
	}
	ListenPortFlag = cli.IntFlag{
		Name:  "port",
		Usage: "Network listening port",
		Value: 30303,
	}
	ListenPort65Flag = cli.IntFlag{
		Name:  "p2p.eth65.port",
		Usage: "ETH65 Network listening port",
		Value: 30304,
	}
	SentryAddrFlag = cli.StringFlag{
		Name:  "sentry.api.addr",
		Usage: "comma separated sentry addresses '<host>:<port>,<host>:<port>'",
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
		Usage: `NAT port mapping mechanism (any|none|upnp|pmp|extip:<IP>)
	     "" or "none"         default - do not nat
	     "extip:77.12.33.4"   will assume the local machine is reachable on the given IP
	     "any"                uses the first auto-detected mechanism
	     "upnp"               uses the Universal Plug and Play protocol
	     "pmp"                uses NAT-PMP with an auto-detected gateway address
	     "pmp:192.168.0.1"    uses NAT-PMP with the given gateway address
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
)

var MetricFlags = []cli.Flag{MetricsEnabledFlag, MetricsEnabledExpensiveFlag, MetricsHTTPFlag, MetricsPortFlag}

// setNodeKey creates a node key from set command line flags, either loading it
// from a file or as a specified hex value. If neither flags were provided, this
// method returns nil and an emphemeral key is to be generated.
func setNodeKey(ctx *cli.Context, cfg *p2p.Config, nodeName, dataDir string) {
	cfg.Name = nodeName
	var (
		hex  = ctx.GlobalString(NodeKeyHexFlag.Name)
		file = ctx.GlobalString(NodeKeyFileFlag.Name)
		key  *ecdsa.PrivateKey
		err  error
	)
	switch {
	case file != "" && hex != "":
		Fatalf("Options %q and %q are mutually exclusive", NodeKeyFileFlag.Name, NodeKeyHexFlag.Name)
	case file != "":
		if err := os.MkdirAll(path.Dir(file), 0755); err != nil {
			panic(err)
		}

		if key, err = crypto.LoadECDSA(file); err != nil {
			Fatalf("Option %q: %v", NodeKeyFileFlag.Name, err)
		}
		cfg.PrivateKey = key
	case hex != "":
		if key, err = crypto.HexToECDSA(hex); err != nil {
			Fatalf("Option %q: %v", NodeKeyHexFlag.Name, err)
		}
		cfg.PrivateKey = key
	default:
		cfg.PrivateKey = nodeKey(path.Join(dataDir, "erigon", "nodekey"))
	}
}

// setNodeUserIdent creates the user identifier from CLI flags.
func setNodeUserIdent(ctx *cli.Context, cfg *node.Config) {
	if identity := ctx.GlobalString(IdentityFlag.Name); len(identity) > 0 {
		cfg.UserIdent = identity
	}
}
func setNodeUserIdentCobra(f *pflag.FlagSet, cfg *node.Config) {
	if identity := f.String(IdentityFlag.Name, IdentityFlag.Value, IdentityFlag.Usage); identity != nil && len(*identity) > 0 {
		cfg.UserIdent = *identity
	}
}

// setBootstrapNodes creates a list of bootstrap nodes from the command line
// flags, reverting to pre-configured ones if none have been specified.
func setBootstrapNodes(ctx *cli.Context, cfg *p2p.Config) {
	urls := params.MainnetBootnodes
	if ctx.GlobalIsSet(BootnodesFlag.Name) {
		urls = SplitAndTrim(ctx.GlobalString(BootnodesFlag.Name))
	} else {
		chain := ctx.GlobalString(ChainFlag.Name)
		switch chain {
		case params.RopstenChainName:
			urls = params.RopstenBootnodes
		case params.RinkebyChainName:
			urls = params.RinkebyBootnodes
		case params.GoerliChainName:
			urls = params.GoerliBootnodes
		case params.ErigonMineName:
			urls = params.ErigonBootnodes
		case params.CalaverasChainName:
			urls = params.CalaverasBootnodes
		case params.SokolChainName:
			urls = params.SokolBootnodes
		default:
			if cfg.BootstrapNodes != nil {
				return // already set, don't apply defaults.
			}
		}
	}

	cfg.BootstrapNodes = make([]*enode.Node, 0, len(urls))
	for _, url := range urls {
		if url != "" {
			node, err := enode.Parse(enode.ValidSchemes, url)
			if err != nil {
				log.Crit("Bootstrap URL invalid", "enode", url, "err", err)
				continue
			}
			cfg.BootstrapNodes = append(cfg.BootstrapNodes, node)
		}
	}
}

// setBootstrapNodesV5 creates a list of bootstrap nodes from the command line
// flags, reverting to pre-configured ones if none have been specified.
func setBootstrapNodesV5(ctx *cli.Context, cfg *p2p.Config) {
	urls := params.MainnetBootnodes
	if ctx.GlobalIsSet(BootnodesFlag.Name) {
		urls = SplitAndTrim(ctx.GlobalString(BootnodesFlag.Name))
	} else {

		chain := ctx.GlobalString(ChainFlag.Name)
		switch chain {
		case params.RopstenChainName:
			urls = params.RopstenBootnodes
		case params.RinkebyChainName:
			urls = params.RinkebyBootnodes
		case params.GoerliChainName:
			urls = params.GoerliBootnodes
		case params.ErigonMineName:
			urls = params.ErigonBootnodes
		case params.CalaverasChainName:
			urls = params.CalaverasBootnodes
		case params.SokolChainName:
			urls = params.SokolBootnodes
		default:
			if cfg.BootstrapNodesV5 != nil {
				return // already set, don't apply defaults.
			}
		}
	}

	cfg.BootstrapNodesV5 = make([]*enode.Node, 0, len(urls))
	for _, url := range urls {
		if url != "" {
			node, err := enode.Parse(enode.ValidSchemes, url)
			if err != nil {
				log.Error("Bootstrap URL invalid", "enode", url, "err", err)
				continue
			}
			cfg.BootstrapNodesV5 = append(cfg.BootstrapNodesV5, node)
		}
	}
}

func setStaticPeers(ctx *cli.Context, cfg *p2p.Config) {
	if !ctx.GlobalIsSet(StaticPeersFlag.Name) {
		return
	}
	urls := SplitAndTrim(ctx.GlobalString(StaticPeersFlag.Name))
	err := SetStaticPeers(cfg, urls)
	if err != nil {
		log.Error("setStaticPeers", "err", err)
	}
}

func SetStaticPeers(cfg *p2p.Config, urls []string) error {
	for _, url := range urls {
		if url == "" {
			continue
		}
		node, err := enode.Parse(enode.ValidSchemes, url)
		if err != nil {
			return fmt.Errorf("static peer URL invalid: %s, %w", url, err)
		}
		cfg.StaticNodes = append(cfg.StaticNodes, node)
	}
	return nil
}

// NewP2PConfig
//  - doesn't setup bootnodes - they will set when genesisHash will know
func NewP2PConfig(nodiscover bool, datadir, netRestrict, natSetting, nodeName string, staticPeers []string, port, protocol uint) (*p2p.Config, error) {
	var enodeDBPath string
	switch protocol {
	case eth.ETH65:
		enodeDBPath = path.Join(datadir, "nodes", "eth65")
	case eth.ETH66:
		enodeDBPath = path.Join(datadir, "nodes", "eth66")
	default:
		return nil, fmt.Errorf("unknown protocol: %v", protocol)
	}
	serverKey := nodeKey(path.Join(datadir, "erigon", "nodekey"))

	cfg := &p2p.Config{
		ListenAddr:   fmt.Sprintf(":%d", port),
		MaxPeers:     100,
		NAT:          nat.Any(),
		NoDiscovery:  nodiscover,
		PrivateKey:   serverKey,
		Name:         nodeName,
		Logger:       log.New(),
		NodeDatabase: enodeDBPath,
	}
	if netRestrict != "" {
		cfg.NetRestrict = new(netutil.Netlist)
		cfg.NetRestrict.Add(netRestrict)
	}
	if staticPeers != nil {
		if err := SetStaticPeers(cfg, staticPeers); err != nil {
			return nil, err
		}
	}
	natif, err := nat.Parse(natSetting)
	if err != nil {
		return nil, fmt.Errorf("invalid nat option %s: %v", natSetting, err)
	}
	cfg.NAT = natif
	return cfg, nil
}

func nodeKey(keyfile string) *ecdsa.PrivateKey {
	if err := os.MkdirAll(path.Dir(keyfile), 0755); err != nil {
		panic(err)
	}
	if key, err := crypto.LoadECDSA(keyfile); err == nil {
		return key
	}
	// No persistent key found, generate and store a new one.
	key, err := crypto.GenerateKey()
	if err != nil {
		log.Crit(fmt.Sprintf("Failed to generate node key: %v", err))
	}
	if err := crypto.SaveECDSA(keyfile, key); err != nil {
		log.Error(fmt.Sprintf("Failed to persist node key: %v", err))
	}
	return key
}

// setListenAddress creates a TCP listening address string from set command
// line flags.
func setListenAddress(ctx *cli.Context, cfg *p2p.Config) {
	if ctx.GlobalIsSet(ListenPortFlag.Name) {
		cfg.ListenAddr = fmt.Sprintf(":%d", ctx.GlobalInt(ListenPortFlag.Name))
	}
	if ctx.GlobalIsSet(ListenPort65Flag.Name) {
		cfg.ListenAddr65 = fmt.Sprintf(":%d", ctx.GlobalInt(ListenPort65Flag.Name))
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
	if ctx.GlobalIsSet(MinerSigningKeyFlag.Name) {
		sigkey := ctx.GlobalString(MinerSigningKeyFlag.Name)
		if sigkey != "" {
			var err error
			cfg.Miner.SigKey, err = crypto.HexToECDSA(sigkey)
			if err != nil {
				Fatalf("Failed to parse ECDSA private key: %v", err)
			}
			cfg.Miner.Etherbase = crypto.PubkeyToAddress(cfg.Miner.SigKey.PublicKey)
		}
	} else if ctx.GlobalIsSet(MinerEtherbaseFlag.Name) {
		etherbase := ctx.GlobalString(MinerEtherbaseFlag.Name)
		if etherbase != "" {
			cfg.Miner.Etherbase = common.HexToAddress(etherbase)
		}
	}
}

func SetP2PConfig(ctx *cli.Context, cfg *p2p.Config, nodeName, dataDir string) {
	setNodeKey(ctx, cfg, nodeName, dataDir)
	setNAT(ctx, cfg)
	setListenAddress(ctx, cfg)
	setBootstrapNodes(ctx, cfg)
	setBootstrapNodesV5(ctx, cfg)
	setStaticPeers(ctx, cfg)

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

	if ctx.GlobalString(ChainFlag.Name) == params.DevChainName {
		// --dev mode can't use p2p networking.
		cfg.MaxPeers = 0
		cfg.ListenAddr = ":0"
		cfg.NoDiscovery = true
		cfg.DiscoveryV5 = false
	}
}

// SetNodeConfig applies node-related command line flags to the config.
func SetNodeConfig(ctx *cli.Context, cfg *node.Config) {
	setDataDir(ctx, cfg)
	setNodeUserIdent(ctx, cfg)
	SetP2PConfig(ctx, &cfg.P2P, cfg.NodeName(), cfg.DataDir)
}

func SetNodeConfigCobra(cmd *cobra.Command, cfg *node.Config) {
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
	case params.DevChainName:
		return "" // unless explicitly requested, use memory databases
	case params.RinkebyChainName:
		return filepath.Join(datadir, "rinkeby")
	case params.GoerliChainName:
		filepath.Join(datadir, "goerli")
	case params.CalaverasChainName:
		return filepath.Join(datadir, "calaveras")
	case params.SokolChainName:
		return filepath.Join(datadir, "sokol")
	default:
		return datadir
	}

	return datadir
}

func setDataDir(ctx *cli.Context, cfg *node.Config) {
	if ctx.GlobalIsSet(DataDirFlag.Name) {
		cfg.DataDir = ctx.GlobalString(DataDirFlag.Name)
	} else {
		cfg.DataDir = DataDirForNetwork(cfg.DataDir, ctx.GlobalString(ChainFlag.Name))
	}
}

func setDataDirCobra(f *pflag.FlagSet, cfg *node.Config) {
	dirname, err := f.GetString(DataDirFlag.Name)
	if err != nil {
		panic(err)
	}
	chain, err := f.GetString(ChainFlag.Name)
	if err != nil {
		panic(err)
	}
	if dirname != "" {
		cfg.DataDir = dirname
	} else {
		cfg.DataDir = DataDirForNetwork(cfg.DataDir, chain)
	}
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
	if ctx.GlobalIsSet(TxPoolJournalFlag.Name) {
		cfg.Journal = ctx.GlobalString(TxPoolJournalFlag.Name)
	}
	if ctx.GlobalIsSet(TxPoolRejournalFlag.Name) {
		cfg.Rejournal = ctx.GlobalDuration(TxPoolRejournalFlag.Name)
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
	if ctx.GlobalIsSet(TxPoolLifetimeFlag.Name) {
		cfg.Lifetime = ctx.GlobalDuration(TxPoolLifetimeFlag.Name)
	}
}

func setEthash(ctx *cli.Context, datadir string, cfg *ethconfig.Config) {
	if ctx.GlobalIsSet(EthashDatasetDirFlag.Name) {
		cfg.Ethash.DatasetDir = ctx.GlobalString(EthashDatasetDirFlag.Name)
	} else {
		cfg.Ethash.DatasetDir = path.Join(datadir, "erigon", "ethash-dags")
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
		panic(fmt.Sprintf("Erigon supports only remote miners. Flag --%s or --%s is required", MinerNotifyFlag.Name, MinerSigningKeyFlag.Name))
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
	cfg.GasFloor, err = flags.GetUint64(MinerGasTargetFlag.Name)
	if err != nil {
		panic(err)
	}
	cfg.GasCeil, err = flags.GetUint64(MinerGasLimitFlag.Name)
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

func setClique(ctx *cli.Context, cfg *params.SnapshotConfig, datadir string) {
	cfg.CheckpointInterval = ctx.GlobalUint64(CliqueSnapshotCheckpointIntervalFlag.Name)
	cfg.InmemorySnapshots = ctx.GlobalInt(CliqueSnapshotInmemorySnapshotsFlag.Name)
	cfg.InmemorySignatures = ctx.GlobalInt(CliqueSnapshotInmemorySignaturesFlag.Name)
	if ctx.GlobalIsSet(CliqueDataDirFlag.Name) {
		cfg.DBPath = path.Join(ctx.GlobalString(CliqueDataDirFlag.Name), "clique/db")
	} else {
		cfg.DBPath = path.Join(datadir, "clique/db")
	}
}

func setAuRa(ctx *cli.Context, cfg *params.AuRaConfig, datadir string) {
	cfg.DBPath = path.Join(datadir, "aura")
}

func setMiner(ctx *cli.Context, cfg *params.MiningConfig) {
	if ctx.GlobalIsSet(MiningEnabledFlag.Name) {
		cfg.Enabled = true
	}
	if cfg.Enabled && len(cfg.Etherbase.Bytes()) == 0 {
		panic(fmt.Sprintf("Erigon supports only remote miners. Flag --%s or --%s is required", MinerNotifyFlag.Name, MinerSigningKeyFlag.Name))
	}
	if ctx.GlobalIsSet(MinerNotifyFlag.Name) {
		cfg.Notify = strings.Split(ctx.GlobalString(MinerNotifyFlag.Name), ",")
	}
	if ctx.GlobalIsSet(MinerExtraDataFlag.Name) {
		cfg.ExtraData = []byte(ctx.GlobalString(MinerExtraDataFlag.Name))
	}
	if ctx.GlobalIsSet(MinerGasTargetFlag.Name) {
		cfg.GasFloor = ctx.GlobalUint64(MinerGasTargetFlag.Name)
	}
	if ctx.GlobalIsSet(MinerGasLimitFlag.Name) {
		cfg.GasCeil = ctx.GlobalUint64(MinerGasLimitFlag.Name)
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
func SetEthConfig(ctx *cli.Context, nodeConfig *node.Config, cfg *ethconfig.Config) {
	CheckExclusive(ctx, MinerSigningKeyFlag, MinerEtherbaseFlag)
	setEtherbase(ctx, cfg)
	setGPO(ctx, &cfg.GPO)
	setTxPool(ctx, &cfg.TxPool)
	setEthash(ctx, nodeConfig.DataDir, cfg)
	setClique(ctx, &cfg.Clique, nodeConfig.DataDir)
	setAuRa(ctx, &cfg.Aura, nodeConfig.DataDir)
	setMiner(ctx, &cfg.Miner)
	setWhitelist(ctx, cfg)

	cfg.P2PEnabled = len(nodeConfig.P2P.SentryAddr) == 0

	if ctx.GlobalIsSet(NetworkIdFlag.Name) {
		cfg.NetworkID = ctx.GlobalUint64(NetworkIdFlag.Name)
	}

	cfg.EnableDebugProtocol = ctx.GlobalBool(DebugProtocolFlag.Name)

	if ctx.GlobalIsSet(DocRootFlag.Name) {
		cfg.DocRoot = ctx.GlobalString(DocRootFlag.Name)
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
	case "":
		if cfg.NetworkID == 1 {
			SetDNSDiscoveryDefaults(cfg, params.MainnetGenesisHash)
		}
	case params.MainnetChainName:
		if !ctx.GlobalIsSet(NetworkIdFlag.Name) {
			cfg.NetworkID = 1
		}
		cfg.Genesis = core.DefaultGenesisBlock()
		SetDNSDiscoveryDefaults(cfg, params.MainnetGenesisHash)
	case params.RopstenChainName:
		if !ctx.GlobalIsSet(NetworkIdFlag.Name) {
			cfg.NetworkID = 3
		}
		cfg.Genesis = core.DefaultRopstenGenesisBlock()
		SetDNSDiscoveryDefaults(cfg, params.RopstenGenesisHash)
	case params.RinkebyChainName:
		if !ctx.GlobalIsSet(NetworkIdFlag.Name) {
			cfg.NetworkID = 4
		}
		cfg.Genesis = core.DefaultRinkebyGenesisBlock()
		SetDNSDiscoveryDefaults(cfg, params.RinkebyGenesisHash)
	case params.GoerliChainName:
		if !ctx.GlobalIsSet(NetworkIdFlag.Name) {
			cfg.NetworkID = 5
		}
		cfg.Genesis = core.DefaultGoerliGenesisBlock()
		SetDNSDiscoveryDefaults(cfg, params.GoerliGenesisHash)
	case params.ErigonMineName:
		if !ctx.GlobalIsSet(NetworkIdFlag.Name) {
			cfg.NetworkID = new(big.Int).SetBytes([]byte("erigon-mine")).Uint64() // erigon-mine
		}
		cfg.Genesis = core.DefaultErigonGenesisBlock()
	case params.CalaverasChainName:
		if !ctx.GlobalIsSet(NetworkIdFlag.Name) {
			cfg.NetworkID = 123 // https://gist.github.com/holiman/c5697b041b3dc18c50a5cdd382cbdd16
		}
		cfg.Genesis = core.DefaultCalaverasGenesisBlock()
	case params.SokolChainName:
		if !ctx.GlobalIsSet(NetworkIdFlag.Name) {
			cfg.NetworkID = 77
		}
		cfg.Genesis = core.DefaultSokolGenesisBlock()
	case params.DevChainName:
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
		if !ctx.GlobalIsSet(MinerGasPriceFlag.Name) {
			cfg.Miner.GasPrice = big.NewInt(1)
		}
	default:
		Fatalf("Chain name is not recognized: %s", chain)
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

// MakeChainDatabase open a database using the flags passed to the client and will hard crash if it fails.
func MakeChainDatabase(cfg *node.Config) ethdb.RwKV {
	chainDb, err := node.OpenDatabase(cfg, ethdb.Chain)
	if err != nil {
		Fatalf("Could not open database: %v", err)
	}
	return chainDb
}

func MakeGenesis(ctx *cli.Context) *core.Genesis {
	var genesis *core.Genesis
	chain := ctx.GlobalString(ChainFlag.Name)
	switch chain {
	case params.RopstenChainName:
		genesis = core.DefaultRopstenGenesisBlock()
	case params.RinkebyChainName:
		genesis = core.DefaultRinkebyGenesisBlock()
	case params.GoerliChainName:
		genesis = core.DefaultGoerliGenesisBlock()
	case params.ErigonMineName:
		genesis = core.DefaultErigonGenesisBlock()
	case params.CalaverasChainName:
		genesis = core.DefaultCalaverasGenesisBlock()
	case params.SokolChainName:
		genesis = core.DefaultSokolGenesisBlock()
	case params.DevChainName:
		Fatalf("Developer chains are ephemeral")
	}
	return genesis
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
