package main

import (
	"sync"

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

	// wait for all goroutines to complete before exiting
	wg.Wait()
}
