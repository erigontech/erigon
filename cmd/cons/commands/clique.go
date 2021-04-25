package commands

import (
	"github.com/spf13/cobra"
)

func init() {
	cliqueCmd.Flags().StringVar(&consensusAddr, "consensus.api.addr", "localhost:9093", "address to listen to for consensus engine api <host>:<port>")
	withDatadir(cliqueCmd)
	rootCmd.AddCommand(cliqueCmd)
}

var cliqueCmd = &cobra.Command{
	Use:   "clique",
	Short: "Run clique consensus engine",
	RunE: func(cmd *cobra.Command, args []string) error {
		return clique(consensusAddr, datadir, database)
	},
}

func clique(consensusAddr string, datadir string, database string) error {
	return nil
}
