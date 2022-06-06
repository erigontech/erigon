package commands

import (
	"github.com/spf13/cobra"
)

var (
	reqId int
)

func init() {
	rootCmd.PersistentFlags().IntVar(&reqId, "req-id", 0, "Defines number of request id")
}

var rootCmd = &cobra.Command{
	Use:   "devnettest",
	Short: "Devnettest root command",
}

// Execute executes the root command.
func Execute() error {
	return rootCmd.Execute()
}
