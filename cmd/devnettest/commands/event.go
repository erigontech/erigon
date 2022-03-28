package commands

import (
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/spf13/cobra"
)

var (
	eventAddr   []string
	eventTopics []string
)

func init() {
	LogsCmd.Flags().StringSliceVar(&eventAddr, "addr", []string{}, "an address or a list of addresses separated by commas")
	LogsCmd.Flags().StringSliceVar(&eventTopics, "topics", []string{}, "a topic or a list of topics separated by commas")
	rootCmd.AddCommand(LogsCmd)
}

var LogsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Subscribes to log event sends a notification each time a new log appears",
	Run: func(cmd *cobra.Command, args []string) {
		if err := services.Logs(eventAddr, eventTopics); err != nil {
			fmt.Printf("could not subscribe to log events: %v\n", err)
		}
	},
}
