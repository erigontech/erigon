package services

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rpc"
)

// SearchBlockForTransactionHash looks for the given hash in the latest black using the eth_newHeads method
func SearchBlockForTransactionHash(hash common.Hash) (uint64, error) {
	fmt.Printf("Searching for tx %q in new block...\n", hash)
	blockN, err := searchBlock(hash)
	if err != nil {
		return 0, fmt.Errorf("failed to subscribe to ws: %v", err)
	}

	return blockN, nil
}

// subscribe connects to a websocket client and returns the subscription handler and a channel buffer
func subscribe(client *rpc.Client, method models.SubMethod, args ...interface{}) (*models.MethodSubscription, error) {
	methodSub := models.NewMethodSubscription(method)

	namespace, subMethod, err := devnetutils.NamespaceAndSubMethodFromMethod(string(method))
	if err != nil {
		return nil, fmt.Errorf("cannot get namespace and submethod from method: %v", err)
	}

	arr := append([]interface{}{subMethod}, args...)

	sub, err := client.Subscribe(context.Background(), namespace, methodSub.SubChan, arr...)
	if err != nil {
		return nil, fmt.Errorf("client failed to subscribe: %v", err)
	}

	methodSub.ClientSub = sub

	return methodSub, nil
}

func subscribeToMethod(method models.SubMethod) (*models.MethodSubscription, error) {
	client, err := rpc.DialWebsocket(context.Background(), fmt.Sprintf("ws://%s", models.Localhost), "")
	if err != nil {
		return nil, fmt.Errorf("failed to dial websocket: %v", err)
	}

	sub, err := subscribe(client, method)
	if err != nil {
		return nil, fmt.Errorf("error subscribing to method: %v", err)
	}

	sub.Client = client

	return sub, nil
}

func searchBlock(hash common.Hash) (uint64, error) {
	methodSub := (*models.MethodSubscriptionMap)[models.ETHNewHeads]
	if methodSub == nil {
		return uint64(0), fmt.Errorf("client subscription should not be nil")
	}

	var (
		blockCount int
		blockN     uint64
	)

mark:
	for {
		select {
		case v := <-methodSub.SubChan:
			blockCount++ // increment the number of blocks seen to check against the max number of blocks to iterate over
			blockNumber := v.(map[string]interface{})["number"]
			num, foundTx, err := txHashInBlock(methodSub.Client, hash, blockNumber.(string)) // check if the block has the transaction to look for inside of it
			if err != nil {
				return uint64(0), fmt.Errorf("could not verify if current block contains the tx hash: %v", err)
			}
			// if the tx is found or the max number of blocks to check is reached, break the tag
			if foundTx || blockCount == models.MaxNumberOfBlockChecks {
				blockN = num
				break mark
			}
		case err := <-methodSub.ClientSub.Err():
			return uint64(0), fmt.Errorf("subscription error from client: %v", err)
		}
	}

	return blockN, nil
}

// UnsubscribeAll closes all the client subscriptions and empties their global subscription channel
func UnsubscribeAll() {
	for _, methodSub := range *models.MethodSubscriptionMap {
		if methodSub != nil {
			methodSub.ClientSub.Unsubscribe()
			for len(methodSub.SubChan) > 0 {
				<-methodSub.SubChan
			}
			methodSub.SubChan = nil // avoid memory leak
		}
	}
}

// SubscribeAll subscribes to the range of methods provided
func SubscribeAll(methods []models.SubMethod) error {
	m := make(map[models.SubMethod]*models.MethodSubscription)
	models.MethodSubscriptionMap = &m
	for _, method := range methods {
		sub, err := subscribeToMethod(method)
		if err != nil {
			return err
		}
		(*models.MethodSubscriptionMap)[method] = sub
	}

	return nil
}
