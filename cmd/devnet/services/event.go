package services

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
)

func InitSubscriptions(methods []requests.SubMethod, logger log.Logger) {
	logger.Info("CONNECTING TO WEBSOCKETS AND SUBSCRIBING TO METHODS...")
	if err := subscribeAll(methods, logger); err != nil {
		logger.Error("failed to subscribe to all methods", "error", err)
		return
	}

	// Initializing subscription methods
	logger.Info("INITIATE LISTENS ON SUBSCRIPTION CHANNELS")
	models.NewHeadsChan = make(chan interface{})

	go func() {
		methodSub := (*models.MethodSubscriptionMap)[requests.Methods.ETHNewHeads]
		if methodSub == nil {
			logger.Error("method subscription should not be nil")
			return
		}

		block := <-methodSub.SubChan
		models.NewHeadsChan <- block
	}()
}

func SearchReservesForTransactionHash(hashes map[libcommon.Hash]bool, logger log.Logger) (*map[libcommon.Hash]string, error) {
	logger.Info("Searching for transactions in reserved blocks...")
	m, err := searchBlockForHashes(hashes, logger)
	if err != nil {
		return nil, fmt.Errorf("failed to search reserves for hashes: %v", err)
	}

	return m, nil
}

// subscribe connects to a websocket client and returns the subscription handler and a channel buffer
func subscribe(client *rpc.Client, method requests.SubMethod, args ...interface{}) (*models.MethodSubscription, error) {
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

func subscribeToMethod(method requests.SubMethod, logger log.Logger) (*models.MethodSubscription, error) {
	client, err := rpc.DialWebsocket(context.Background(), "ws://localhost:8545", "", logger)
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

func searchBlockForHashes(hashmap map[libcommon.Hash]bool, logger log.Logger) (*map[libcommon.Hash]string, error) {
	if len(hashmap) == 0 {
		return nil, fmt.Errorf("no hashes to search for")
	}

	txToBlock := make(map[libcommon.Hash]string, len(hashmap))

	toFind := len(hashmap)
	methodSub := (*models.MethodSubscriptionMap)[requests.Methods.ETHNewHeads]
	if methodSub == nil {
		return nil, fmt.Errorf("client subscription should not be nil")
	}

	var blockCount int
	for {
		// get a block from the new heads channel
		block := <-models.NewHeadsChan
		blockCount++ // increment the number of blocks seen to check against the max number of blocks to iterate over
		blockNum := block.(map[string]interface{})["number"].(string)
		_, numFound, foundErr := txHashInBlock(methodSub.Client, hashmap, blockNum, txToBlock, logger)
		if foundErr != nil {
			return nil, fmt.Errorf("failed to find hash in block with number %q: %v", foundErr, blockNum)
		}
		toFind -= numFound // remove the amount of found txs from the amount we're looking for
		if toFind == 0 {   // this means we have found all the txs we're looking for
			logger.Info("All the transactions created have been included in blocks")
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
func subscribeAll(methods []requests.SubMethod, logger log.Logger) error {
	m := make(map[requests.SubMethod]*models.MethodSubscription)
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
