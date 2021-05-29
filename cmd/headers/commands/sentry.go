package commands

import (
	"strings"

	"github.com/ledgerwatch/erigon/cmd/headers/download"
	"github.com/ledgerwatch/erigon/eth/protocols/eth"
	"github.com/spf13/cobra"
)

var (
	natSetting  string   // NAT setting
	port        int      // Listening port
	staticPeers []string // static peers
	discovery   bool     // enable sentry's discovery mechanism
	protocol    string
	netRestrict string // CIDR to restrict peering to
)

func init() {
	sentryCmd.Flags().StringVar(&natSetting, "nat", "any", "NAT port mapping mechanism (any|none|upnp|pmp|extip:<IP>)")
	sentryCmd.Flags().IntVar(&port, "port", 30303, "p2p port number")
	sentryCmd.Flags().StringVar(&sentryAddr, "sentry.api.addr", "localhost:9091", "comma separated sentry addresses '<host>:<port>,<host>:<port>'")
	sentryCmd.Flags().StringVar(&protocol, "p2p.protocol", "eth66", "eth65|eth66")
	sentryCmd.Flags().StringArrayVar(&staticPeers, "staticpeers", []string{}, "static peer list [enode]")
	sentryCmd.Flags().BoolVar(&discovery, "discovery", true, "discovery mode")
	sentryCmd.Flags().StringVar(&netRestrict, "netrestrict", "", "CIDR range to accept peers from <CIDR>")
	rootCmd.AddCommand(sentryCmd)
}

var sentryCmd = &cobra.Command{
	Use:   "sentry",
	Short: "Run p2p sentry for the downloader",
	RunE: func(cmd *cobra.Command, args []string) error {
		p := eth.ETH66
		switch strings.ToLower(protocol) {
		case "eth65":
			p = eth.ETH65
		}
		return download.Sentry("" /* datadir */, natSetting, port, sentryAddr, staticPeers, discovery, netRestrict, uint(p))
	},
}
