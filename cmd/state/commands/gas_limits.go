package commands

import (
	"fmt"

	"github.com/ledgerwatch/bolt"
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/ledgerwatch/turbo-geth/ethdb/remote"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(gasLimitsCmd)
	withRemoteDb(gasLimitsCmd)
	rootCmd.AddCommand(gasLimitsCmd)
}

var gasLimitsCmd = &cobra.Command{
	Use:   "gasLimits",
	Short: "gasLimits",
	RunE: func(cmd *cobra.Command, args []string) error {
		localDb, err := bolt.Open(file()+"_gl", 0600, &bolt.Options{})
		if err != nil {
			panic(err)
		}
		ctx := getContext()

		remoteDb, err := remote.Open(ctx, remote.DefaultOpts.Addr(remoteDbAddress))
		if err != nil {
			return err
		}

		fmt.Println("Processing started...")
		stateless.NewGasLimitReporter(ctx, remoteDb, localDb).GasLimits(ctx)
		return nil
	},
}
