package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(stateGrowthCmd)
	rootCmd.AddCommand(stateGrowthCmd)
}

var stateGrowthCmd = &cobra.Command{
	Use:   "stateGrowth",
	Short: "stateGrowth",
	RunE: func(cmd *cobra.Command, args []string) error {
		stateless.StateGrowth1(chaindata)
		stateless.StateGrowth2(chaindata)
		return nil
	},
}
