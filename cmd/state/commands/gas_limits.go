package commands

import (
	"fmt"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/ledgerwatch/turbo-geth/ethdb"
	"github.com/spf13/cobra"
)

func init() {
	withPrivateRpc(gasLimitsCmd)
	rootCmd.AddCommand(gasLimitsCmd)
}

var gasLimitsCmd = &cobra.Command{
	Use:   "gasLimits",
	Short: "gasLimits",
	RunE: func(cmd *cobra.Command, args []string) error {
		ctx := cmd.Context()

		localDB, err := bolt.Open(file()+"_gl", 0600, &bolt.Options{})
		if err != nil {
			panic(err)
		}

		remoteDB, err := ethdb.NewRemote().Path(privateRpcAddr).Open()
		if err != nil {
			return err
		}

		fmt.Println("Processing started...")
		stateless.NewGasLimitReporter(ctx, remoteDB, localDB).GasLimits(ctx)
		return nil
	},
}
