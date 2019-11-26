package commands

import (
	"fmt"

	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

func init() {
	withStatsfile(statelessChartCmd)
	rootCmd.AddCommand(statelessChartCmd)
}

var statelessChartCmd = &cobra.Command{
	Use:   "statelessChart",
	Short: "Make a chart from based on the statistics of the 'stateless' action",
	RunE: func(cmd *cobra.Command, args []string) error {
		stateless.DoKVChart(statsfile, []int{21, 20, 19, 18}, fmt.Sprintf("%s-chart.png", statsfile), 2800000, 1)
		return nil
	},
}
