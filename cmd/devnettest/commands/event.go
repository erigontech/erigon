package commands

import (
	"fmt"
	"sync"

	"github.com/ledgerwatch/erigon/cmd/devnettest/services"
	"github.com/spf13/cobra"
)

var (
	//eventAddrs  = []string{devAddress, recvAddr}
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

var wg sync.WaitGroup

func callLogs() {
	wg.Add(1)
	go func() {
		if err := services.Logs([]string{}, eventTopics); err != nil {
			fmt.Printf("could not subscribe to log events: %v\n", err)
		}
		defer wg.Done()
	}()
	wg.Wait()

	callContractTx()
}
