package commands

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"

	"github.com/ledgerwatch/turbo-geth/cmd/restapi/rest"
)

var (
	remoteDbAddress string
	listenAddress   string
)

func init() {
	rootCmd.Flags().StringVar(&remoteDbAddress, "remote-db-addr", "localhost:9999", "address of remote DB listener of a turbo-geth node")
	rootCmd.Flags().StringVar(&listenAddress, "rpcaddr", "localhost:8080", "REST server listening interface")
}

var rootCmd = &cobra.Command{
	Use:   "restapi",
	Short: "restapi exposes read-only blockchain APIs through REST (requires running turbo-geth node)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return rest.ServeREST(listenAddress, remoteDbAddress)
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
