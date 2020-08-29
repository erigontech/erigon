package commands

import (
	"fmt"
	"github.com/ledgerwatch/turbo-geth/cmd/restapi/rest"
	"github.com/spf13/cobra"
)

var (
	rpcAddr   string
	chaindata string
	addr      string
	port      uint
)

func init() {
	rootCmd.Flags().StringVar(&rpcAddr, "private.api.addr", "127.0.0.1:9090", "binary RPC network address, for example: 127.0.0.1:9090, empty string means not to start the listener. do not expose to public network. serves remote database interface")
	rootCmd.Flags().StringVar(&addr, "http.addr", "127.0.0.1", "REST server listening host")
	rootCmd.Flags().UintVar(&port, "http.port", 8080, "REST server listening port")
	rootCmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the database")
}

var rootCmd = &cobra.Command{
	Use:   "restapi",
	Short: "restapi exposes read-only blockchain APIs through REST (requires running turbo-geth node)",
	RunE: func(cmd *cobra.Command, args []string) error {
		return rest.ServeREST(cmd.Context(), fmt.Sprintf("%s:%d", addr, port), rpcAddr, chaindata)
	},
}

func RootCommand() *cobra.Command {
	return rootCmd
}
