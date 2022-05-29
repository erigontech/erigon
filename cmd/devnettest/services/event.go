package services

import (
	"context"
	"fmt"
	"github.com/ledgerwatch/erigon/cmd/devnettest/utils"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rpc"
)

const numberOfIterations = 128

// subscribe connects to a websocket and returns the subscription handler and a channel buffer
func subscribe(client *rpc.Client, method string, args ...interface{}) (*rpc.ClientSubscription, chan interface{}, error) {
	var (
		namespace string
		subMethod string
		splitErr  error
	)

	namespace, subMethod, splitErr = utils.NamespaceAndSubMethodFromMethod(method)
	if splitErr != nil {
		return nil, nil, fmt.Errorf("cannot get namespace and submethod from method: %v", splitErr)
	}

	ch := make(chan interface{})
	var arr = []interface{}{subMethod}
	arr = append(arr, args...)

	sub, err := client.Subscribe(context.Background(), namespace, ch, arr...)
	if err != nil {
		return nil, nil, fmt.Errorf("client failed to subscribe: %v", err)
	}

	return sub, ch, nil
}

// subscribeToNewHeadsAndSearch makes a ws subscription for eth_newHeads and searches each new header for the tx hash
func subscribeToNewHeadsAndSearch(client *rpc.Client, method string, hash common.Hash) (uint64, error) {
	sub, ch, err := subscribe(client, method)
	if err != nil {
		return uint64(0), fmt.Errorf("error subscribing to newHeads: %v", err)
	}
	defer sub.Unsubscribe()

	var (
		blockCount int
		blockN     uint64
	)
mark:
	for {
		select {
		case v := <-ch:
			blockCount++
			blockNumber := v.(map[string]interface{})["number"]
			num, foundTx, err := blockHasHash(client, hash, blockNumber.(string))
			if err != nil {
				return uint64(0), fmt.Errorf("could not verify if current block contains the tx hash: %v", err)
			}
			if foundTx || blockCount == numberOfIterations {
				blockN = num
				break mark
			}
		case err := <-sub.Err():
			return uint64(0), fmt.Errorf("subscription error from client: %v", err)
		}
	}

	return blockN, nil
}

// Logs dials a websocket connection and listens for log events by calling subscribeToLogs
func Logs(addresses, topics []string) error {
	client, clientErr := rpc.DialWebsocket(context.Background(), "ws://127.0.0.1:8545", "")
	if clientErr != nil {
		return fmt.Errorf("failed to dial websocket: %v", clientErr)
	}

	if err := subscribeToLogs(client, "eth_logs", addresses, topics); err != nil {
		return fmt.Errorf("failed to subscribe to logs: %v", err)
	}

	return nil
}

// subscribeToLogs makes a ws subscription for eth_subscribeLogs
func subscribeToLogs(client *rpc.Client, method string, addresses []string, topics []string) error {
	params := map[string][]string{
		"address": addresses,
		"topics":  topics,
	}

	sub, ch, err := subscribe(client, method, params)
	if err != nil {
		return fmt.Errorf("error subscribing to logs: %v", err)
	}
	defer sub.Unsubscribe()

	var count int

ForLoop:
	for {
		select {
		case v := <-ch:
			count++
			_map := v.(map[string]interface{})
			for k, val := range _map {
				fmt.Printf("%s: %+v, ", k, val)
			}
			fmt.Println()
			fmt.Println()
			if count == numberOfIterations {
				break ForLoop
			}
		case err := <-sub.Err():
			return fmt.Errorf("subscription error from client: %v", err)
		}
	}

	return nil
}
