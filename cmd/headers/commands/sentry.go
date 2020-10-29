package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/headers/download"
	"github.com/spf13/cobra"
)

var (
	natSetting string // NAT setting
	port       int    // Listening port
)

func init() {
	sentryCmd.Flags().StringVar(&natSetting, "nat", "any", "NAT port mapping mechanism (any|none|upnp|pmp|extip:<IP>)")
	sentryCmd.Flags().IntVar(&port, "port", 30303, "p2p port number")
	sentryCmd.Flags().StringVar(&sentryAddr, "sentryAddr", "localhost:9091", "sentry address <host>:<port>")
	sentryCmd.Flags().StringVar(&coreAddr, "coreAddr", "localhost:9092", "core address <host>:<port>")
	rootCmd.AddCommand(sentryCmd)
}

var sentryCmd = &cobra.Command{
	Use:   "sentry",
	Short: "Run p2p sentry for the downloader",
	RunE: func(cmd *cobra.Command, args []string) error {
		return download.Sentry(natSetting, port, sentryAddr, coreAddr)
	},
}
