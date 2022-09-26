package main

import (
	"github.com/ledgerwatch/erigon/cmd/devnet/node"
	"github.com/ledgerwatch/erigon/cmd/devnet/utils"
	"sync"
)

func main() {
	// wait group variable to prevent main function from terminating until routines are finished
	var wg sync.WaitGroup

	wg.Add(1)

	defer utils.ClearDevDB()

	// Start the first erigon node in a go routine
	go node.StartNode(&wg)
	wg.Wait()
}
