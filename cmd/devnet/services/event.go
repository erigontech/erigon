package services

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/models"
	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/rpc"
)

func InitSubscriptions(methods []models.SubMethod) {
	fmt.Printf("CONNECTING TO WEBSOCKETS AND SUBSCRIBING TO METHODS...\n")
	if err := subscribeAll(methods); err != nil {
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

		for {
			select {
			case block := <-methodSub.SubChan:
				models.NewHeadsChan <- block
			}
		}
	}()
}

func SearchReservesForTransactionHash(hashes map[common.Hash]bool) (*map[common.Hash]string, error) {
	fmt.Printf("Searching for txes in reserved blocks...\n")
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

//func searchBlockForHash(hash common.Hash) (uint64, error) {
//	// create a one entry hashmap to hold the hash
//	hashmap := map[common.Hash]bool{hash: true}
//
//	methodSub := (*models.MethodSubscriptionMap)[models.ETHNewHeads]
//	if methodSub == nil {
//		return uint64(0), fmt.Errorf("client subscription should not be nil")
//	}
//
//	var (
//		wg     sync.WaitGroup
//		err    error
//		blockN uint64
//	)
//
//	// add the current process to the waitGroup
//	wg.Add(1)
//
//	go func() {
//		var blockCount int
//	mark:
//		for {
//			select {
//			case v := <-models.NewHeadsChan:
//				blockCount++ // increment the number of blocks seen to check against the max number of blocks to iterate over
//				blockNumber := v.(map[string]interface{})["number"].(string)
//				// add the block to the old heads
//				models.OldHeads = append(models.OldHeads, blockNumber)
//				num, numFound, foundErr := txHashInBlock(methodSub.Client, hashmap, blockNumber) // check if the block has the transaction to look for inside of it
//				// return if error is found and end the goroutine
//				if foundErr != nil {
//					err = foundErr
//					wg.Done()
//					CheckTxPoolContent(1, 0)
//					return
//				}
//				// if the max number of blocks to check is reached, look for tx old heads and break the tag
//				if blockCount == models.MaxNumberOfBlockChecks {
//					num, numFound, foundErr = checkOldHeads(methodSub.Client, hashmap)
//					if foundErr != nil {
//						err = foundErr
//						wg.Done()
//						CheckTxPoolContent(1, 0)
//						return
//					}
//					if numFound == 0 {
//						err = fmt.Errorf("timeout when searching for tx")
//						wg.Done()
//						break mark
//					}
//				}
//				// if the tx is found, through new heads or old heads, break the tag
//				if numFound == 1 {
//					blockN = num
//					wg.Done()
//					CheckTxPoolContent(0, 0)
//					break mark
//				}
//			case subErr := <-methodSub.ClientSub.Err():
//				fmt.Printf("subscription error from client: %v", subErr)
//				return
//			}
//		}
//	}()
//
//	return blockN, err
//}

func searchBlockForHashes(hashesmap map[common.Hash]bool) (*map[common.Hash]string, error) {
	txToBlock := make(map[common.Hash]string, len(hashesmap))

	toFind := len(hashesmap)
	methodSub := (*models.MethodSubscriptionMap)[models.ETHNewHeads]
	if methodSub == nil {
		return nil, fmt.Errorf("client subscription should not be nil")
	}

	var blockCount int
	for {
		select {
		// get a block from the new heads channel
		case block := <-models.NewHeadsChan:
			blockCount++ // increment the number of blocks seen to check against the max number of blocks to iterate over
			blockNum := block.(map[string]interface{})["number"].(string)
			_, numFound, foundErr := txHashInBlock(methodSub.Client, hashesmap, blockNum, txToBlock)
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

// subscribeAll subscribes to the range of methods provided
func subscribeAll(methods []models.SubMethod) error {
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
