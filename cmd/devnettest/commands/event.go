package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(LogsCmd)
}

var LogsCmd = &cobra.Command{
	Use: "logs",
	Short: "Subscribes to log event sends a notification each time a new log appears",
	Run: func(cmd *cobra.Command, args []string) {
		if err := services.Logs(); err != nil {
			fmt.Println("LogsCmd()")
			fmt.Printf("could not subscribe to log events: %v\n", err)
		}
	},
}