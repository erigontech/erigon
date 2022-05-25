package commands

import (
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/spf13/cobra"
)

var (
	eventAddrs  = []string{devAddress, recvAddr}
	eventTopics []string
)

func init() {
	rootCmd.AddCommand(LogsCmd)
}

var LogsCmd = &cobra.Command{
	Use:   "logs",
	Short: "Subscribes to log event sends a notification each time a new log appears",
	Run: func(cmd *cobra.Command, args []string) {
		callLogs()
	},
}

func callLogs() {
	go func() {
		if err := services.Logs(eventAddrs, eventTopics); err != nil {
			fmt.Printf("could not subscribe to log events: %v\n", err)
		}
	}()

	callContractTx()
}
