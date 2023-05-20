package services

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
)

func InitSubscriptions(methods []models.SubMethod, logger log.Logger) {
	fmt.Printf("CONNECTING TO WEBSOCKETS AND SUBSCRIBING TO METHODS...\n")
	if err := subscribeAll(methods, logger); err != nil {
		fmt.Printf("failed to subscribe to all methods: %v\n", err)
		return
	}

	// Initializing subscription methods
	fmt.Printf("INITIATE LISTENS ON SUBSCRIPTION CHANNELS")
	models.NewHeadsChan = make(chan interface{})

	go func() {
		methodSub := (*models.MethodSubscriptionMap)[models.ETHNewHeads]
		if methodSub == nil {
			fmt.Printf("method subscription should not be nil")
			return
		}

		block := <-methodSub.SubChan
		models.NewHeadsChan <- block
	}()
}

func SearchReservesForTransactionHash(hashes map[libcommon.Hash]bool) (*map[libcommon.Hash]string, error) {
	fmt.Printf("Searching for transactions in reserved blocks...\n")
	m, err := searchBlockForHashes(hashes)
	if err != nil {
		return nil, fmt.Errorf("failed to search reserves for hashes: %v", err)
	}

	return m, nil
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

func subscribeToMethod(method models.SubMethod, logger log.Logger) (*models.MethodSubscription, error) {
	client, err := rpc.DialWebsocket(context.Background(), fmt.Sprintf("ws://%s", models.Localhost), "", logger)
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

func searchBlockForHashes(hashmap map[libcommon.Hash]bool) (*map[libcommon.Hash]string, error) {
	if len(hashmap) == 0 {
		return nil, fmt.Errorf("no hashes to search for")
	}

	txToBlock := make(map[libcommon.Hash]string, len(hashmap))

	toFind := len(hashmap)
	methodSub := (*models.MethodSubscriptionMap)[models.ETHNewHeads]
	if methodSub == nil {
		return nil, fmt.Errorf("client subscription should not be nil")
	}

	var blockCount int
	for {
		// get a block from the new heads channel
		block := <-models.NewHeadsChan
		blockCount++ // increment the number of blocks seen to check against the max number of blocks to iterate over
		blockNum := block.(map[string]interface{})["number"].(string)
		_, numFound, foundErr := txHashInBlock(methodSub.Client, hashmap, blockNum, txToBlock)
		if foundErr != nil {
			return nil, fmt.Errorf("failed to find hash in block with number %q: %v", foundErr, blockNum)
		}
		toFind -= numFound // remove the amount of found txs from the amount we're looking for
		if toFind == 0 {   // this means we have found all the txs we're looking for
			fmt.Printf("All the transactions created have been mined\n")
			return &txToBlock, nil
		}
		if blockCount == models.MaxNumberOfBlockChecks {
			return nil, fmt.Errorf("timeout when searching for tx")
		}
	}
}

// UnsubscribeAll closes all the client subscriptions and empties their global subscription channel
func UnsubscribeAll() {
	if models.MethodSubscriptionMap == nil {
		return
	}
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

// subscribeAll subscribes to the range of methods provided
func subscribeAll(methods []models.SubMethod, logger log.Logger) error {
	m := make(map[models.SubMethod]*models.MethodSubscription)
	models.MethodSubscriptionMap = &m
	for _, method := range methods {
		sub, err := subscribeToMethod(method, logger)
		if err != nil {
			return err
		}
		(*models.MethodSubscriptionMap)[method] = sub
	}

	return nil
}
