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

package main

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon/cmd/utils"
	"github.com/erigontech/erigon/db/datadir"
	"github.com/erigontech/erigon/node/paths"
	"github.com/erigontech/erigon/p2p/sentry"
	"github.com/erigontech/erigon/turbo/debug"
	"github.com/erigontech/erigon/turbo/logging"
	node2 "github.com/erigontech/erigon/turbo/node"
)

// generate the messages

var (
	sentryAddr string // Address of the sentry <host>:<port>
	datadirCli string // Path to td working dir

	natSetting   string   // NAT setting
	port         int      // Listening port
	staticPeers  []string // static peers
	trustedPeers []string // trusted peers
	discoveryDNS []string
	nodiscover   bool // disable sentry's discovery mechanism
	protocol     uint
	allowedPorts []uint
	netRestrict  string // CIDR to restrict peering to
	maxPeers     int
	maxPendPeers int
	healthCheck  bool
	metrics      bool
	witProtocol  bool // Enable/disable WIT protocol
)

func init() {
	utils.CobraFlags(rootCmd, debug.Flags, utils.MetricFlags, logging.Flags)

	rootCmd.Flags().StringVar(&sentryAddr, "sentry.api.addr", "localhost:9091", "grpc addresses")
	rootCmd.Flags().StringVar(&datadirCli, utils.DataDirFlag.Name, paths.DefaultDataDir(), utils.DataDirFlag.Usage)
	rootCmd.Flags().StringVar(&natSetting, utils.NATFlag.Name, utils.NATFlag.Value, utils.NATFlag.Usage)
	rootCmd.Flags().IntVar(&port, utils.ListenPortFlag.Name, utils.ListenPortFlag.Value, utils.ListenPortFlag.Usage)
	rootCmd.Flags().StringSliceVar(&staticPeers, utils.StaticPeersFlag.Name, []string{}, utils.StaticPeersFlag.Usage)
	rootCmd.Flags().StringSliceVar(&trustedPeers, utils.TrustedPeersFlag.Name, []string{}, utils.TrustedPeersFlag.Usage)
	rootCmd.Flags().StringSliceVar(&discoveryDNS, utils.DNSDiscoveryFlag.Name, []string{}, utils.DNSDiscoveryFlag.Usage)
	rootCmd.Flags().BoolVar(&nodiscover, utils.NoDiscoverFlag.Name, false, utils.NoDiscoverFlag.Usage)
	rootCmd.Flags().UintVar(&protocol, utils.P2pProtocolVersionFlag.Name, utils.P2pProtocolVersionFlag.Value.Value()[0], utils.P2pProtocolVersionFlag.Usage)
	rootCmd.Flags().UintSliceVar(&allowedPorts, utils.P2pProtocolAllowedPorts.Name, utils.P2pProtocolAllowedPorts.Value.Value(), utils.P2pProtocolAllowedPorts.Usage)
	rootCmd.Flags().StringVar(&netRestrict, utils.NetrestrictFlag.Name, utils.NetrestrictFlag.Value, utils.NetrestrictFlag.Usage)
	rootCmd.Flags().IntVar(&maxPeers, utils.MaxPeersFlag.Name, utils.MaxPeersFlag.Value, utils.MaxPeersFlag.Usage)
	rootCmd.Flags().IntVar(&maxPendPeers, utils.MaxPendingPeersFlag.Name, utils.MaxPendingPeersFlag.Value, utils.MaxPendingPeersFlag.Usage)
	rootCmd.Flags().BoolVar(&healthCheck, utils.HealthCheckFlag.Name, false, utils.HealthCheckFlag.Usage)
	rootCmd.Flags().BoolVar(&metrics, utils.MetricsEnabledFlag.Name, false, utils.MetricsEnabledFlag.Usage)
	rootCmd.Flags().BoolVar(&witProtocol, utils.PolygonPosWitProtocolFlag.Name, false, utils.PolygonPosWitProtocolFlag.Usage)

	if err := rootCmd.MarkFlagDirname(utils.DataDirFlag.Name); err != nil {
		panic(err)
	}

	if err := debug.SetCobraFlagsFromConfigFile(rootCmd); err != nil {
		panic(err)
	}
}

var rootCmd = &cobra.Command{
	Use:   "sentry",
	Short: "Run p2p sentry",
	PersistentPostRun: func(cmd *cobra.Command, args []string) {
		debug.Exit()
	},
	RunE: func(cmd *cobra.Command, args []string) error {
		dirs := datadir.New(datadirCli)
		nodeConfig := node2.NewNodeConfig(nil)
		p2pConfig, err := utils.NewP2PConfig(
			nodiscover,
			dirs,
			netRestrict,
			natSetting,
			maxPeers,
			maxPendPeers,
			nodeConfig.NodeName(),
			staticPeers,
			trustedPeers,
			uint(port),
			protocol,
			allowedPorts,
			metrics,
			witProtocol,
		)
		if err != nil {
			return err
		}

		logger := debug.SetupCobra(cmd, "sentry")
		return sentry.Sentry(cmd.Context(), dirs, sentryAddr, discoveryDNS, p2pConfig, protocol, healthCheck, logger)
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
