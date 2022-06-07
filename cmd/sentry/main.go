package main

import (
	"fmt"
	"os"

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
	datadir    string // Path to td working dir

	natSetting   string   // NAT setting
	port         int      // Listening port
	staticPeers  []string // static peers
	trustedPeers []string // trusted peers
	discoveryDNS []string
	nodiscover   bool // disable sentry's discovery mechanism
	protocol     string
	netRestrict  string // CIDR to restrict peering to
	maxPeers     int
	maxPendPeers int
	healthCheck  bool
)

func init() {
	utils.CobraFlags(rootCmd, append(debug.Flags, utils.MetricFlags...))

	rootCmd.Flags().StringVar(&sentryAddr, "sentry.api.addr", "localhost:9091", "grpc addresses")
	rootCmd.Flags().StringVar(&datadir, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	rootCmd.Flags().StringVar(&natSetting, utils.NATFlag.Name, utils.NATFlag.Value, utils.NATFlag.Usage)
	rootCmd.Flags().IntVar(&port, utils.ListenPortFlag.Name, utils.ListenPortFlag.Value, utils.ListenPortFlag.Usage)
	rootCmd.Flags().StringSliceVar(&staticPeers, utils.StaticPeersFlag.Name, []string{}, utils.StaticPeersFlag.Usage)
	rootCmd.Flags().StringSliceVar(&trustedPeers, utils.TrustedPeersFlag.Name, []string{}, utils.TrustedPeersFlag.Usage)
	rootCmd.Flags().StringSliceVar(&discoveryDNS, utils.DNSDiscoveryFlag.Name, []string{}, utils.DNSDiscoveryFlag.Usage)
	rootCmd.Flags().BoolVar(&nodiscover, utils.NoDiscoverFlag.Name, false, utils.NoDiscoverFlag.Usage)
	rootCmd.Flags().StringVar(&protocol, "p2p.protocol", "eth66", "eth66")
	rootCmd.Flags().StringVar(&netRestrict, utils.NetrestrictFlag.Name, utils.NetrestrictFlag.Value, utils.NetrestrictFlag.Usage)
	rootCmd.Flags().IntVar(&maxPeers, utils.MaxPeersFlag.Name, utils.MaxPeersFlag.Value, utils.MaxPeersFlag.Usage)
	rootCmd.Flags().IntVar(&maxPendPeers, utils.MaxPendingPeersFlag.Name, utils.MaxPendingPeersFlag.Value, utils.MaxPendingPeersFlag.Usage)
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
	},
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		p := eth.ETH66

		nodeConfig := node2.NewNodeConfig()
		p2pConfig, err := utils.NewP2PConfig(
			nodiscover,
			datadir,
			netRestrict,
			natSetting,
			maxPeers,
			maxPendPeers,
			nodeConfig.NodeName(),
			staticPeers,
			trustedPeers,
			uint(port),
			uint(p),
		)
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
