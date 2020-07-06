package main

import "github.com/spf13/cobra"

var (
	chaindata     string
	blocksPerStep uint64
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chaindata, "chaindata", "chaindata", "path to the db")
	must(cmd.MarkFlagFilename("chaindata", ""))
	must(cmd.MarkFlagRequired("chaindata"))
}

func withBlocksPerStep(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&blocksPerStep, "blocks_per_step", 2, "how much blocks unwind/exec on each iteration")
}
