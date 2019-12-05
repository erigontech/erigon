package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

var (
	start  int
	window int
)

func withStartAndWindow(cmd *cobra.Command) {
	cmd.Flags().IntVar(&start, "start", 0, "number of data points to skip when making a chart")
	cmd.Flags().IntVar(&window, "window", 1024, "size of the window for moving average")
}

func init() {
	withStartAndWindow(stateGrowthChart1Cmd)
	withStartAndWindow(stateGrowthChart2Cmd)

	rootCmd.AddCommand(stateGrowthChart1Cmd)
	rootCmd.AddCommand(stateGrowthChart2Cmd)
}

var stateGrowthChart1Cmd = &cobra.Command{
	Use:   "stateGrowthChart1",
	Short: "stateGrowthChart1",
	RunE: func(cmd *cobra.Command, args []string) error {
		stateless.StateGrowthChart1(start, window)
		return nil
	},
}

var stateGrowthChart2Cmd = &cobra.Command{
	Use:   "stateGrowthChart2",
	Short: "stateGrowthChart2",
	RunE: func(cmd *cobra.Command, args []string) error {
		stateless.StateGrowthChart2(start, window)
		return nil
	},
}
