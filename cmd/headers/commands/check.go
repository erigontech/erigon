package commands

import (
	"github.com/ledgerwatch/turbo-geth/cmd/headers/check"
	"github.com/spf13/cobra"
)

func init() {
	checkCmd.Flags().StringVar(&filesDir, "filesdir", "", "path to directory where files will be stored")
	rootCmd.AddCommand(checkCmd)
}

var checkCmd = &cobra.Command{
	Use:   "check",
	Short: "Read the header files and print out anchor table",
	RunE: func(cmd *cobra.Command, args []string) error {
		return check.Check(filesDir)
	},
}
