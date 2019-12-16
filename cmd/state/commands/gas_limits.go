package commands

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
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
		reporter, err := stateless.NewReporter(remoteDbAddress)
		if err != nil {
			return err
		}

		ctx := getContext()
		fmt.Println("Processing started...")
		reporter.GasLimits(ctx)
		return nil
	},
}
