package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/ledgerwatch/erigon-lib/common"
	"github.com/ledgerwatch/erigon/cmd/sentry/sentry"
	"github.com/ledgerwatch/erigon/cmd/utils"
	"github.com/ledgerwatch/erigon/common/paths"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/ledgerwatch/erigon/internal/debug"
	node2 "github.com/ledgerwatch/erigon/turbo/node"
	"github.com/spf13/cobra"
)

// generate the messages

var (
	sentryAddr string // Address of the sentry <host>:<port>
	chaindata  string // Path to chaindata
	datadir    string // Path to td working dir

	natSetting   string   // NAT setting
	port         int      // Listening port
	staticPeers  []string // static peers
	trustedPeers []string // trusted peers
	discoveryDNS []string
	nodiscover   bool // disable sentry's discovery mechanism
	protocol     string
	netRestrict  string // CIDR to restrict peering to
	healthCheck  bool
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
	rootCmd.Flags().StringVar(&sentryAddr, "sentry.api.addr", "localhost:9091", "grpc addresses")
	rootCmd.Flags().StringVar(&protocol, "p2p.protocol", "eth66", "eth66")
	rootCmd.Flags().StringSliceVar(&staticPeers, "staticpeers", []string{}, "static peer list [enode]")
	rootCmd.Flags().StringSliceVar(&trustedPeers, "trustedpeers", []string{}, "trusted peer list [enode]")
	rootCmd.Flags().StringSliceVar(&discoveryDNS, utils.DNSDiscoveryFlag.Name, []string{}, utils.DNSDiscoveryFlag.Usage)
	rootCmd.Flags().BoolVar(&nodiscover, utils.NoDiscoverFlag.Name, false, utils.NoDiscoverFlag.Usage)
	rootCmd.Flags().StringVar(&netRestrict, "netrestrict", "", "CIDR range to accept peers from <CIDR>")
	rootCmd.Flags().StringVar(&datadir, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	rootCmd.Flags().BoolVar(&healthCheck, utils.HealthCheckFlag.Name, false, utils.HealthCheckFlag.Usage)
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
			chaindata = filepath.Join(datadir, "chaindata")
		}
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		p := eth.ETH66

		nodeConfig := node2.NewNodeConfig()
		p2pConfig, err := utils.NewP2PConfig(nodiscover, datadir, netRestrict, natSetting, nodeConfig.NodeName(), staticPeers, trustedPeers, uint(port), uint(p))
		if err != nil {
			return err
		}
		return sentry.Sentry(cmd.Context(), datadir, sentryAddr, discoveryDNS, p2pConfig, uint(p), healthCheck)
	},
}

func main() {
	ctx, cancel := common.RootContext()
	defer cancel()
	if err := rootCmd.ExecuteContext(ctx); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
