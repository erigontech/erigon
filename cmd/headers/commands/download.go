package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/headers/download"
	"github.com/spf13/cobra"
)

var (
	combined bool   // Whether downloader also includes sentry
	timeout  int    // Timeout for delivery requests
	window   int    // Size of sliding window for downloading block bodies
	chain    string // Name of the network to connect to
)

func init() {
	downloadCmd.Flags().StringSliceVar(&sentryAddrs, "sentry.addr", []string{"localhost:9091"}, "comma separated sentry addresses '<host>:<port>,<host>:<port>'")
	downloadCmd.Flags().BoolVar(&combined, "combined", false, "run downloader and sentry in the same process")
	downloadCmd.Flags().IntVar(&timeout, "timeout", 30, "timeout for devp2p delivery requests, in seconds")
	downloadCmd.Flags().IntVar(&window, "window", 65536, "size of sliding window for downloading block bodies, block")
	downloadCmd.Flags().StringVar(&chain, "chain", "mainnet", "Name of the network (mainnet, testnets) to connect to")

	// Options below are only used in the combined mode
	downloadCmd.Flags().StringVar(&natSetting, "nat", "any", "NAT port mapping mechanism (any|none|upnp|pmp|extip:<IP>)")
	downloadCmd.Flags().IntVar(&port, "port", 30303, "p2p port number")
	downloadCmd.Flags().StringSliceVar(&staticPeers, "staticpeers", []string{}, "static peer list [enode]")
	downloadCmd.Flags().BoolVar(&discovery, "discovery", true, "discovery mode")
	downloadCmd.Flags().StringVar(&netRestrict, "netrestrict", "", "CIDR range to accept peers from <CIDR>")

	withChaindata(downloadCmd)
	withLmdbFlags(downloadCmd)
	rootCmd.AddCommand(downloadCmd)
}

var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download headers backwards",
	RunE: func(cmd *cobra.Command, args []string) error {
		db := openDatabase(chaindata)
		defer db.Close()
		if combined {
			return download.Combined(natSetting, port, staticPeers, discovery, netRestrict, db, timeout, window, chain)
		}

		return download.Download(sentryAddrs, coreAddr, db, timeout, window, chain)
	},
}
