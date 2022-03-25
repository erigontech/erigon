package services

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/requests"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rpc"
)

const numberOfIterations = 128

// subscribe connects to a websocket and returns the subscription handler and a channel buffer
func subscribe(client *rpc.Client, method string) (*rpc.ClientSubscription, chan interface{}, error) {
	var namespace string
	namespace, method = requests.GetNamespaceFromMethod(method)
	ch := make(chan interface{})
	sub, err := client.Subscribe(context.Background(), namespace, ch, []interface{}{method}...)
	if err != nil {
		return nil, nil, fmt.Errorf("client failed to subscribe: %v", err)
	}
	return sub, ch, nil
}

// subscribeToNewHeads makes a ws subscription for eth_newHeads
func subscribeToNewHeads(client *rpc.Client, method string, hash common.Hash) (uint64, error) {
	sub, ch, err := subscribe(client, method)
	if err != nil {
		return uint64(0), fmt.Errorf("error subscribing to newHeads: %v", err)
	}
	defer sub.Unsubscribe()

	var (
		blockCount int
		blockN     uint64
	)
ForLoop:
	for {
		select {
		case v := <-ch:
			blockCount++
			blockNumber := v.(map[string]interface{})["number"]
			fmt.Printf("Searching for the transaction in block with number: %+v\n", blockNumber.(string))
			num, foundTx, err := blockHasHash(client, hash, blockNumber.(string))
			if err != nil {
				return uint64(0), fmt.Errorf("could not verify if current block contains the tx hash: %v", err)
			}
			if foundTx || blockCount == numberOfIterations {
				blockN = num
				break ForLoop
			}
		case err := <-sub.Err():
			return uint64(0), fmt.Errorf("subscription error from client: %v", err)
		}
	}

	return blockN, nil

}

// Logs dials a websocket connection and listens for log events by calling subscribeToSubscribeLogs
func Logs() error {
	fmt.Println("Logs()")
	client, clientErr := rpc.DialWebsocket(context.Background(), "ws://127.0.0.1:8545", "")
	if clientErr != nil {
		return fmt.Errorf("failed to dial websocket: %v", clientErr)
	}
	fmt.Println()
	fmt.Println("Connected to web socket successfully")

	if err := subscribeToLogs(client, "eth_logs"); err != nil {
		return fmt.Errorf("failed to subscribe to logs: %v", err)
	}
	//if err := subscribeToHeads(client, "eth_newHeads"); err != nil {
	//	return fmt.Errorf("failed to subscribe to logs: %v", err)
	//}
	return nil
}

// subscribeToNewHeads makes a ws subscription for eth_subscribeLogs
func subscribeToLogs(client *rpc.Client, method string) error {
	fmt.Println("subscribeToLogs()")
	sub, ch, err := subscribe(client, method)
	if err != nil {
		return fmt.Errorf("error subscribing to subscribeLogs: %v", err)
	}
	defer sub.Unsubscribe()

	var count int

	fmt.Println("About to start for loop!")

ForLoop:
	for {
		select {
		case v := <-ch:
			count++
			fmt.Printf("Value in channel is: %+v\n", v)
			if count == numberOfIterations {
				break ForLoop
			}
		case err := <-sub.Err():
			return fmt.Errorf("subscription error from client: %v", err)
		}
	}

	return nil
}

// subscribeToHeads makes a ws subscription for eth_subscribeLogs
func subscribeToHeads(client *rpc.Client, method string) error {
	fmt.Println("subToHeads()")
	sub, ch, err := subscribe(client, method)
	if err != nil {
		return fmt.Errorf("error subscribing to subscribeLogs: %v", err)
	}
	defer sub.Unsubscribe()

	var count int

	fmt.Println("About to start for loop!")

ForLoop:
	for {
		select {
		case v := <-ch:
			count++
			fmt.Printf("%+v\n\n", v)
			if count == numberOfIterations {
				break ForLoop
			}
		case err := <-sub.Err():
			return fmt.Errorf("subscription error from client: %v", err)
		}
	}

	return nil
}