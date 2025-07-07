package commands

import "github.com/spf13/cobra"

var (
	unwindBatchNo uint64
)

func withUnwindBatchNo(cmd *cobra.Command) {
	cmd.Flags().Uint64Var(&unwindBatchNo, "unwind-batch-no", 0, "batch number to unwind to (this batch number will be the tip after unwind)")
}

func withOnlySmtV2(cmd *cobra.Command) {
	cmd.Flags().BoolVar(&onlySmtV2, "only-smt-v2", false, "only use SMT v2")
}
