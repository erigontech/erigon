package main

import "github.com/spf13/cobra"

var (
	chaindata string
	block     uint64
	stride    uint64
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

func withBlockNumber(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 0, "stop test at this block")
}

func withStride(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&stride, "stride", 2, "how much blocks unwind/exec on each iteration")
}
