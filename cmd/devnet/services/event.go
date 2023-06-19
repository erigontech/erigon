package services

import (
	"context"
	"fmt"

	libcommon "github.com/ledgerwatch/erigon-lib/common"

	"github.com/ledgerwatch/erigon/cmd/devnet/devnet"
	"github.com/ledgerwatch/erigon/cmd/devnet/devnetutils"
	"github.com/ledgerwatch/erigon/cmd/devnet/requests"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
)

var (
	// MaxNumberOfBlockChecks is the max number of blocks to look for a transaction in
	MaxNumberOfEmptyBlockChecks = 25
	Subscriptions               *map[requests.SubMethod]*Subscription
)

// Subscription houses the client subscription, name and channel for its delivery
type Subscription struct {
	Client    *rpc.Client
	ClientSub *rpc.ClientSubscription
	Name      requests.SubMethod
	SubChan   chan interface{}
}

// NewSubscription returns a new Subscription instance
func NewSubscription(name requests.SubMethod) *Subscription {
	return &Subscription{
		Name:    name,
		SubChan: make(chan interface{}),
	}
}

func InitSubscriptions(ctx context.Context, methods []requests.SubMethod) {
	logger := devnet.Logger(ctx)

	logger.Info("CONNECTING TO WEBSOCKETS AND SUBSCRIBING TO METHODS...")
	if err := subscribeAll(methods, logger); err != nil {
		logger.Error("failed to subscribe to all methods", "error", err)
		return
	}
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
func subscribe(client *rpc.Client, method requests.SubMethod, args ...interface{}) (*Subscription, error) {
	methodSub := NewSubscription(method)

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

func subscribeToMethod(method requests.SubMethod, logger log.Logger) (*Subscription, error) {
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

	methodSub := (*Subscriptions)[requests.Methods.ETHNewHeads]
	if methodSub == nil {
		return nil, fmt.Errorf("client subscription should not be nil")
	}

	headsSub := (*Subscriptions)[requests.Methods.ETHNewHeads]

	// get a block from the new heads channel
	if headsSub == nil {
		return nil, fmt.Errorf("no block heads subscription")
	}

	var blockCount int
	for {
		block := <-headsSub.SubChan
		blockNum := block.(map[string]interface{})["number"].(string)
		_, numFound, foundErr := txHashInBlock(methodSub.Client, hashmap, blockNum, txToBlock, logger)

		if foundErr != nil {
			return nil, fmt.Errorf("failed to find hash in block with number %q: %v", foundErr, blockNum)
		}

		if len(hashmap) == 0 { // this means we have found all the txs we're looking for
			logger.Info("All the transactions created have been included in blocks")
			return &txToBlock, nil
		}

		if numFound == 0 {
			blockCount++ // increment the number of blocks seen to check against the max number of blocks to iterate over
		}

		if blockCount == MaxNumberOfEmptyBlockChecks {
			for h := range hashmap {
				logger.Error("Missing Tx", "txHash", h)
			}

			return nil, fmt.Errorf("timeout when searching for tx")
		}
	}
}

// UnsubscribeAll closes all the client subscriptions and empties their global subscription channel
func UnsubscribeAll() {
	if Subscriptions == nil {
		return
	}
	for _, methodSub := range *Subscriptions {
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
	m := make(map[requests.SubMethod]*Subscription)
	Subscriptions = &m
	for _, method := range methods {
		sub, err := subscribeToMethod(method, logger)
		if err != nil {
			return err
		}
		(*Subscriptions)[method] = sub
	}

	return nil
}
