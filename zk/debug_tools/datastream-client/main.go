package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"time"

	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon/zk/datastream/client"
	"github.com/erigontech/erigon/zk/datastream/types"
)

func main() {
	// Parse command line flags
	serverAddr := flag.String("server", "localhost:6900", "datastream server address")
	flag.Parse()

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Create stream client (using same configuration as stage_batches.go)
	dsClient := client.NewClient(ctx, *serverAddr, false, 5*time.Second, 0, client.DefaultEntryChannelSize)

	// Start the client
	if err := dsClient.Start(); err != nil {
		log.Error("Failed to start datastream client", "err", err)
		return
	}
	defer dsClient.Stop()

	// Get entry channel
	entryChan := dsClient.GetEntryChan()

	// Start reading entries in a goroutine
	go func() {
		// Start reading entries from the datastream
		if err := dsClient.ReadAllEntriesToChannel(); err != nil {
			log.Error("Failed to read entries from datastream", "err", err)
			return
		}
	}()

	// Start processing entries in a goroutine
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case entry := <-*entryChan:
				processEntry(entry)
			}
		}
	}()

	// Wait for interrupt signal
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	<-sigChan

}

func processEntry(entry interface{}) {
	switch e := entry.(type) {
	case *types.BatchStart:
		fmt.Printf("Batch Start: Number=%d, ForkId=%d\n", e.Number, e.ForkId)
	case *types.BatchEnd:
		fmt.Printf("Batch End: Number=%d, StateRoot=%x\n", e.Number, e.StateRoot)
	case *types.FullL2Block:
		fmt.Printf("Block: Number=%d, Hash=%x, Batch=%d\n",
			e.L2BlockNumber, e.L2Blockhash, e.BatchNumber)
	case *types.GerUpdate:
		fmt.Printf("GER Update: Batch=%d, GER=%x\n",
			e.BatchNumber, e.GlobalExitRoot)
	default:
		fmt.Printf("Unknown entry type: %T\n", entry)
	}
}
