package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

func init() {
	stateGrowthCmd := &cobra.Command{
		Use:   "stateGrowth",
		Short: "stateGrowth",
		RunE: func(cmd *cobra.Command, args []string) error {
			reporter, err := stateless.NewReporter(remoteDbAdddress)
			if err != nil {
				return err
			}

			reporter.StateGrowth1(chaindata)
			reporter.StateGrowth2(chaindata)
			return nil
		},
	}

	withChaindata(stateGrowthCmd)

	withRemoteDb(stateGrowthCmd)

	rootCmd.AddCommand(stateGrowthCmd)
}
