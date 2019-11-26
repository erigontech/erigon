package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

var (
	output     string
	filter     []int
	fromRow    uint64
	startColor int
)

func init() {
	withStatsfile(statelessChartCmd)
	if err := statelessChartCmd.MarkFlagRequired("statsfile"); err != nil {
		panic(err)
	}

	statelessChartCmd.Flags().StringVarP(&output, "output", "o", "chart.png",
		"Where to save the output file to")
	if err := statelessChartCmd.MarkFlagFilename("output", "png"); err != nil {
		panic(err)
	}
	if err := statelessChartCmd.MarkFlagRequired("output"); err != nil {
		panic(err)
	}

	statelessChartCmd.Flags().IntSliceVar(&filter, "filter", nil,
		"Show only the specified columns")

	statelessChartCmd.Flags().Uint64Var(&fromRow, "from", 0,
		"From which data row (excl header) to chart")

	statelessChartCmd.Flags().IntVar(&startColor, "start-color", 1,
		"From which color in the palette")

	rootCmd.AddCommand(statelessChartCmd)
}

var statelessChartCmd = &cobra.Command{
	Use:   "statelessChart",
	Short: "Make a chart from based on the statistics of the 'stateless' action",
	RunE: func(cmd *cobra.Command, args []string) error {
		return stateless.MakeChart(statsfile, filter, output, fromRow, startColor)
	},
}
