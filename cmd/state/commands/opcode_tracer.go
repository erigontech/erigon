package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/stateless"
	"github.com/spf13/cobra"
)

var (
	numBlocks   uint64
	saveOpcodes bool
	saveBBlocks bool
)

func init() {
	withBlock(opcodeTracer)
	withChaindata(opcodeTracer)
	opcodeTracer.Flags().Uint64Var(&numBlocks, "numBlocks", 1, "number of blocks to run the operation on")
	opcodeTracer.Flags().BoolVar(&saveOpcodes, "saveOpcodes", false, "set to save the opcodes")
	opcodeTracer.Flags().BoolVar(&saveBBlocks, "saveBBlocks", false, "set to save the basic blocks")

	rootCmd.AddCommand(opcodeTracer)
}

var opcodeTracer = &cobra.Command{
	Use:   "opcodeTracer",
	Short: "Re-executes historical transactions in read-only mode and traces them at the opcode level",
	RunE: func(cmd *cobra.Command, args []string) error {
		return stateless.OpcodeTracer(genesis, block, chaindata, numBlocks, saveOpcodes, saveBBlocks)
	},
}
