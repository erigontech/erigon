package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/turbo-geth/common/hexutil"
	"github.com/ledgerwatch/turbo-geth/core/types"
	"github.com/ledgerwatch/turbo-geth/rpc"
)

// NewPendingTransactionFilter new transaction filter
func (api *APIImpl) NewPendingTransactionFilter(_ context.Context) (hexutil.Uint64, error) {
	return 0, fmt.Errorf(NotImplemented, "eth_newPendingTransactionFilter")
}

// NewBlockFilter new transaction filter
func (api *APIImpl) NewBlockFilter(_ context.Context) (hexutil.Uint64, error) {
	return 0, fmt.Errorf(NotImplemented, "eth_newBlockFilter")
}

// NewFilter implements eth_newFilter. Creates an arbitrary filter object, based on filter options, to notify when the state changes (logs).
func (api *APIImpl) NewFilter(_ context.Context, filter interface{}) (hexutil.Uint64, error) {
	return 0, fmt.Errorf(NotImplemented, "eth_newFilter")
}

// UninstallFilter new transaction filter
func (api *APIImpl) UninstallFilter(_ context.Context, index hexutil.Uint64) (bool, error) {
	return false, fmt.Errorf(NotImplemented, "eth_uninstallFilter")
}

// GetFilterChanges implements eth_getFilterChanges. Polling method for a previously-created filter, which returns an array of logs which occurred since last poll.
func (api *APIImpl) GetFilterChanges(_ context.Context, index hexutil.Uint64) ([]interface{}, error) {
	var stub []interface{}
	return stub, fmt.Errorf(NotImplemented, "eth_getFilterChanges")
}

// NewHeads send a notification each time a new (header) block is appended to the chain.
func (api *APIImpl) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		headers := make(chan *types.Header)
		/*
			headersSub := api.events.SubscribeNewHeads(headers)
		*/

		for {
			select {
			case h := <-headers:
				notifier.Notify(rpcSub.ID, h)
			case <-rpcSub.Err():
				//headersSub.Unsubscribe()
				return
			case <-notifier.Closed():
				//headersSub.Unsubscribe()
				return
			}
		}
	}()

	return rpcSub, nil
}
