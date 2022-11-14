package main

import (
	"github.com/ledgerwatch/erigon/cmd/devnet/commands"
	"sync"
	"time"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/node"
)

func main() {
	// wait group variable to prevent main function from terminating until routines are finished
	var wg sync.WaitGroup

	// remove the old logs from previous runs
	devnetutils.DeleteLogs()

	defer devnetutils.ClearDevDB()

	// start the first erigon node in a go routine
	node.Start(&wg)

	// sleep for seconds to allow the nodes fully start up
	time.Sleep(time.Second * 10)

	// execute all rpc methods amongst the two nodes
	commands.ExecuteAllMethods()

	// wait for all goroutines to complete before exiting
	wg.Wait()
}
