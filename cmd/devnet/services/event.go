package services

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rpc"
)

var subscriptionChan = make(chan interface{})

// SearchBlockForTransactionHash looks for the given hash in the latest black using the eth_newHeads method
func SearchBlockForTransactionHash(hash common.Hash) (uint64, error) {
	client, err := rpc.DialWebsocket(context.Background(), fmt.Sprintf("ws://%s", models.Localhost), "")
	if err != nil {
		return 0, fmt.Errorf("failed to dial websocket: %v", err)
	}

	fmt.Printf("Searching for tx %q in new block...\n", hash)
	blockN, err := subscribeToNewHeads(client, hash)
	if err != nil {
		return 0, fmt.Errorf("failed to subscribe to ws: %v", err)
	}

	return blockN, nil
}

// subscribe connects to a websocket client and returns the subscription handler and a channel buffer
func subscribe(client *rpc.Client, method string, args ...interface{}) (*rpc.ClientSubscription, error) {
	namespace, subMethod, err := devnetutils.NamespaceAndSubMethodFromMethod(method)
	if err != nil {
		return nil, fmt.Errorf("cannot get namespace and submethod from method: %v", err)
	}

	arr := append([]interface{}{subMethod}, args...)

	sub, err := client.Subscribe(context.Background(), namespace, subscriptionChan, arr...)
	if err != nil {
		return nil, fmt.Errorf("client failed to subscribe: %v", err)
	}

	return sub, nil
}

func subscribeToNewHeads(client *rpc.Client, hash common.Hash) (uint64, error) {
	sub, err := subscribe(client, string(models.ETHNewHeads))
	if err != nil {
		return uint64(0), fmt.Errorf("error subscribing to newHeads: %v", err)
	}
	defer unsubscribe(sub)

	var (
		blockCount int
		blockN     uint64
	)

mark:
	for {
		select {
		case v := <-subscriptionChan:
			blockCount++ // increment the number of blocks seen to check against the max number of blocks to iterate over
			blockNumber := v.(map[string]interface{})["number"]
			num, foundTx, err := txHashInBlock(client, hash, blockNumber.(string)) // check if the block has the transaction to look for inside of it
			if err != nil {
				return uint64(0), fmt.Errorf("could not verify if current block contains the tx hash: %v", err)
			}
			// if the tx is found or the max number of blocks to check is reached, break the tag
			if foundTx || blockCount == models.MaxNumberOfBlockChecks {
				blockN = num
				break mark
			}
		case err := <-sub.Err():
			return uint64(0), fmt.Errorf("subscription error from client: %v", err)
		}
	}

	return blockN, nil
}

// unsubscribe closes the client subscription and empties the global subscription channel
func unsubscribe(sub *rpc.ClientSubscription) {
	sub.Unsubscribe()
	for len(subscriptionChan) > 0 {
		<-subscriptionChan
	}
}
