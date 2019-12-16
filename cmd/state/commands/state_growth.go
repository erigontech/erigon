package commands

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

func init() {
	stateGrowthCmd := &cobra.Command{
		Use:   "stateGrowth",
		Short: "stateGrowth",
		RunE: func(cmd *cobra.Command, args []string) error {
			ctx := getContext()
			reporter, err := stateless.NewReporter(ctx, remoteDbAddress)
			if err != nil {
				return err
			}

			fmt.Println("Processing started...")
			reporter.StateGrowth1(ctx)
			reporter.StateGrowth2(ctx)
			return nil
		},
	}

	withChaindata(stateGrowthCmd)
	withRemoteDb(stateGrowthCmd)
	rootCmd.AddCommand(stateGrowthCmd)
}
