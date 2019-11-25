package commands

import "github.com/spf13/cobra"

var (
	chaindata string
	statsfile string
	block     uint64
)

func withBlock(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 1, "specifies a block number for operation")
}

func withChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chaindata, "chaindata", "chaindata", "path to the chaindata file used as input to analysis")
}

func withStatsfile(cmd *cobra.Command) {
	cmd.Flags().StringVar(&statsfile, "statsfile", "stateless.csv", "path where to write the stats file")
}
