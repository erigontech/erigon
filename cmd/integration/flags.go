package main

import "github.com/spf13/cobra"

var (
	chaindata   string
	stop        uint64
	unwind      uint64
	unwindEvery uint64
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

func withStop(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&stop, "stop", 0, "stop test at this block")
}

func withUnwind(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwind, "unwind", 2, "how much blocks unwind on each iteration")
}

func withUnwindEvery(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwindEvery, "unwind_every", 100, "each iteration test will move forward `--unwind_every` blocks, then unwind `--unwind` blocks")
}
