package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/headers/download"
	"github.com/spf13/cobra"
)

var (
	natSetting  string   // NAT setting
	port        int      // Listening port
	staticPeers []string // static peers
	discovery   bool     // enable sentry's discovery mechanism
	netRestrict string   // CIDR to restrict peering to

)

func init() {
	sentryCmd.Flags().StringVar(&natSetting, "nat", "any", "NAT port mapping mechanism (any|none|upnp|pmp|extip:<IP>)")
	sentryCmd.Flags().IntVar(&port, "port", 30303, "p2p port number")
	sentryCmd.Flags().StringVar(&sentryAddr, "sentryAddr", "localhost:9091", "sentry address <host>:<port>")
	sentryCmd.Flags().StringVar(&coreAddr, "coreAddr", "localhost:9092", "core address <host>:<port>")
	sentryCmd.Flags().StringArrayVar(&staticPeers, "staticpeers", []string{}, "static peer list [enode]")
	sentryCmd.Flags().BoolVar(&discovery, "discovery", true, "discovery mode")
	sentryCmd.Flags().StringVar(&netRestrict, "netrestrict", "", "CIDR range to accept peers from <CIDR>")
	rootCmd.AddCommand(sentryCmd)
}

var sentryCmd = &cobra.Command{
	Use:   "sentry",
	Short: "Run p2p sentry for the downloader",
	RunE: func(cmd *cobra.Command, args []string) error {
		return download.Sentry(natSetting, port, sentryAddr, coreAddr, staticPeers, discovery, netRestrict)
	},
}
