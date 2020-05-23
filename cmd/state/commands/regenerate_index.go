package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/state/generate"
	"github.com/spf13/cobra"
)

func init() {
	withChaindata(regenerateIndexCmd)
	withIndexBucket(regenerateIndexCmd)
	withCSBucket(regenerateIndexCmd)
	rootCmd.AddCommand(regenerateIndexCmd)
}

var regenerateIndexCmd = &cobra.Command{
	Use:   "regenerateIndex",
	Short: "Generate index for accounts/storage based on changesets",
	RunE: func(cmd *cobra.Command, args []string) error {
		return generate.RegenerateIndex(chaindata, []byte(indexBucket), []byte(changeSetBucket))
	},
}
