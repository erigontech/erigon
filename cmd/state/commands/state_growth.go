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
			db, err := connectRemoteDb(ctx, remoteDbAddress)
			if err != nil {
				return err
			}

			fmt.Println("Processing started...")
			stateless.NewStateGrowth1Reporter(ctx, db).StateGrowth1(ctx)
			stateless.NewStateGrowth2Reporter(ctx, db).StateGrowth2(ctx)
			return nil
		},
	}

	withChaindata(stateGrowthCmd)
	withRemoteDb(stateGrowthCmd)
	rootCmd.AddCommand(stateGrowthCmd)
}
