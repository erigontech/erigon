package commands

import "github.com/spf13/cobra"

var (
	chaindata          string
	referenceChaindata string
	block              uint64
	unwind             uint64
	unwindEvery        uint64
	reset              bool
	bucket             string
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func withChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&chaindata, "chaindata", "", "path to the db")
	must(cmd.MarkFlagDirname("chaindata"))
	must(cmd.MarkFlagRequired("chaindata"))
}

func withReferenceChaindata(cmd *cobra.Command) {
	cmd.Flags().StringVar(&referenceChaindata, "reference_chaindata", "", "path to the 2nd (reference/etalon) db")
	must(cmd.MarkFlagDirname("reference_chaindata"))
}

func withBlock(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&block, "block", 0, "block test at this block")
}

func withUnwind(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwind, "unwind", 0, "how much blocks unwind on each iteration")
}

func withUnwindEvery(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwindEvery, "unwind_every", 100, "each iteration test will move forward `--unwind_every` blocks, then unwind `--unwind` blocks")
}

func withReset(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&reset, "reset", false, "reset given stage")
}

func withBucket(cmd *cobra.Command) {
	cmd.Flags().StringVar(&bucket, "bucket", "", "reset given stage")
}
