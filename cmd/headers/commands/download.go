package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/headers/download"
	"github.com/spf13/cobra"
)

var (
	filesDir   string // Directory when the files should be stored
	bufferSize int    // Size of buffer in MiB
	natSetting string // NAT setting
)

func init() {
	downloadCmd.Flags().StringVar(&filesDir, "filesdir", "", "path to directory where files will be stored")
	downloadCmd.Flags().IntVar(&bufferSize, "buffersize", 512, "size o the buffer in MiB")
	downloadCmd.Flags().StringVar(&natSetting, "nat", "any", "NAT port mapping mechanism (any|none|upnp|pmp|extip:<IP>)")
	rootCmd.AddCommand(downloadCmd)
}

var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download headers backwards",
	RunE: func(cmd *cobra.Command, args []string) error {
		return download.Download(natSetting, filesDir)
	},
}
