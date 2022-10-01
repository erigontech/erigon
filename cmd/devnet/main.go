package main

import (
	"github.com/ledgerwatch/erigon/cmd/devnet/node"
	"github.com/ledgerwatch/erigon/cmd/devnet/utils"
	"sync"
)

func main() {
	// wait group variable to prevent main function from terminating until routines are finished
	var wg sync.WaitGroup

	defer utils.ClearDevDB()

	// start the first erigon node in a go routine
	node.Start(&wg)

	// wait for all goroutines to complete before exiting
	wg.Wait()
}
