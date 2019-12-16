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
		ctx := getContext()
		reporter, err := stateless.NewReporter(ctx, remoteDbAddress)
		if err != nil {
			return err
		}

		fmt.Println("Processing started...")
		reporter.GasLimits(ctx)
		return nil
	},
}
