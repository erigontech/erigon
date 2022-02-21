package commands

import (
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(mockRequestCmd)
}

var mockRequestCmd = &cobra.Command{
	Use:   "mock",
	Short: "Mocks a request on the devnet",
	Run: func(cmd *cobra.Command, args []string) {
		requests.MockGetRequest(reqId)
	},
}
