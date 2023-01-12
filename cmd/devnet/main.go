package main

import (
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cmd/devnet/commands"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/node"
	"github.com/ledgerwatch/erigon/cmd/devnet/services"
)

func main() {
	defer func() {
		// unsubscribe from all the subscriptions made
		defer services.UnsubscribeAll()

		// clear all the dev files
		devnetutils.ClearDevDB()
	}()

	// wait group variable to prevent main function from terminating until routines are finished
	var wg sync.WaitGroup

	// remove the old logs from previous runs
	devnetutils.DeleteLogs()

	// start the first erigon node in a go routine
	node.Start(&wg)

	// sleep for seconds to allow the nodes fully start up
	time.Sleep(time.Second * 10)

	// start up the subscription services for the different sub methods
	services.InitSubscriptions([]models.SubMethod{models.ETHNewHeads})

	// execute all rpc methods amongst the two nodes
	commands.ExecuteAllMethods()

	// wait for all goroutines to complete before exiting
	wg.Wait()
}
