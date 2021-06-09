package commands

import (
	"crypto/ecdsa"
	"fmt"
	"os"
	"path"
	"strings"

	"github.com/ledgerwatch/erigon/cmd/sentry/download"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/crypto"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/internal/debug"
	"github.com/ledgerwatch/erigon/log"
	"github.com/ledgerwatch/erigon/node"
	"github.com/ledgerwatch/erigon/p2p/nat"
	"github.com/ledgerwatch/erigon/p2p/netutil"
	node2 "github.com/ledgerwatch/erigon/turbo/node"
	"github.com/spf13/cobra"
)

var (
	sentryAddr string // Address of the sentry <host>:<port>
	chaindata  string // Path to chaindata
	datadir    string // Path to td working dir

	natSetting   string   // NAT setting
	port         int      // Listening port
	staticPeers  []string // static peers
	discoveryDNS []string
	nodiscover   bool // disable sentry's discovery mechanism
	protocol     string
	netRestrict  string // CIDR to restrict peering to
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))

	rootCmd.Flags().StringVar(&natSetting, "nat", "", `NAT port mapping mechanism (any|none|upnp|pmp|extip:<IP>)
	     "" or "none"         default - do not nat
	     "extip:77.12.33.4"   will assume the local machine is reachable on the given IP
	     "any"                uses the first auto-detected mechanism
	     "upnp"               uses the Universal Plug and Play protocol
	     "pmp"                uses NAT-PMP with an auto-detected gateway address
	     "pmp:192.168.0.1"    uses NAT-PMP with the given gateway address
`)
	rootCmd.Flags().IntVar(&port, "port", 30303, "p2p port number")
	rootCmd.Flags().StringVar(&sentryAddr, "sentry.api.addr", "localhost:9091", "comma separated sentry addresses '<host>:<port>,<host>:<port>'")
	rootCmd.Flags().StringVar(&protocol, "p2p.protocol", "eth66", "eth65|eth66")
	rootCmd.Flags().StringSliceVar(&staticPeers, "staticpeers", []string{}, "static peer list [enode]")
	rootCmd.Flags().StringSliceVar(&discoveryDNS, utils.DNSDiscoveryFlag.Name, []string{}, utils.DNSDiscoveryFlag.Usage)
	rootCmd.Flags().BoolVar(&nodiscover, utils.NoDiscoverFlag.Name, false, utils.NoDiscoverFlag.Usage)
	rootCmd.Flags().StringVar(&netRestrict, "netrestrict", "", "CIDR range to accept peers from <CIDR>")
	rootCmd.Flags().StringVar(&datadir, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	if err := rootCmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}

}

var rootCmd = &cobra.Command{
	Use:   "sentry",
	Short: "Run p2p sentry",
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		if err := debug.SetupCobra(cmd); err != nil {
			panic(err)
		}
		if chaindata == "" {
			chaindata = path.Join(datadir, "erigon", "chaindata")
		}
		//if snapshotDir == "" {
		//	snapshotDir = path.Join(datadir, "erigon", "snapshot")
		//}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		p := eth.ETH66
		switch strings.ToLower(protocol) {
		case "eth65":
			p = eth.ETH65
		}

		p2pConfig := node.DefaultConfig.P2P
		p2pConfig.NoDiscovery = nodiscover
		if netRestrict != "" {
			p2pConfig.NetRestrict = new(netutil.Netlist)
			p2pConfig.NetRestrict.Add(netRestrict)
		}
		if staticPeers != nil {
			if err := utils.SetStaticPeers(&p2pConfig, staticPeers); err != nil {
				return err
			}
		}
		serverKey := nodeKey(path.Join(datadir, "erigon", "nodekey"))
		natif, err := nat.Parse(natSetting)
		if err != nil {
			return fmt.Errorf("invalid nat option %s: %v", natSetting, err)
		}
		p2pConfig.NAT = natif
		p2pConfig.PrivateKey = serverKey

		var enodeDBPath string
		switch p {
		case eth.ETH65:
			enodeDBPath = path.Join(datadir, "nodes", "eth65")
		case eth.ETH66:
			enodeDBPath = path.Join(datadir, "nodes", "eth66")
		default:
			return fmt.Errorf("unknown protocol: %v", protocol)
		}

		nodeConfig := node2.NewNodeConfig(node2.Params{})
		p2pConfig.Name = nodeConfig.NodeName()
		p2pConfig.Logger = log.New()
		p2pConfig.NodeDatabase = enodeDBPath
		p2pConfig.ListenAddr = fmt.Sprintf(":%d", port)

		return download.Sentry(datadir, sentryAddr, discoveryDNS, &p2pConfig, uint(p))
	},
}

func Execute() {
	ctx, cancel := utils.RootContext()
	defer cancel()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func nodeKey(keyfile string) *ecdsa.PrivateKey {
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
