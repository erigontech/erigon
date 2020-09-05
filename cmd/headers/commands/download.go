package commands

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/cmd/headers/download"
	"github.com/spf13/cobra"
)

var (
	filesDir   string // Directory when the files should be stored
	bufferSize int    // Size of buffer in MiB
)

func init() {
	downloadCmd.Flags().StringVar(&filesDir, "filesdir", "", "path to directory where files will be stored")
	downloadCmd.Flags().IntVar(&bufferSize, "buffersize", 512, "size o the buffer in MiB")
	rootCmd.AddCommand(downloadCmd)
}

var downloadCmd = &cobra.Command{
	Use:   "download",
	Short: "Download headers backwards",
	RunE: func(cmd *cobra.Command, args []string) error {
		fmt.Printf("Download hello world!\n")
		return download.Download()
	},
}
