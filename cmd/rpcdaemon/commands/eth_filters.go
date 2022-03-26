package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/log/v3"
)

// NewPendingTransactionFilter new transaction filter
func (api *APIImpl) NewPendingTransactionFilter(ctx context.Context) (hexutil.Uint64, error) {
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
	if api.filters == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		defer debug.LogPanic()
		headers := make(chan *types.Header, 1)
		defer close(headers)
		id := api.filters.SubscribeNewHeads(headers)
		defer api.filters.UnsubscribeHeads(id)

		for {
			select {
			case h := <-headers:
				err := notifier.Notify(rpcSub.ID, h)
				if err != nil {
					log.Warn("error while notifying subscription", "err", err)
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// NewPendingTransactions send a notification each time a new (header) block is appended to the chain.
func (api *APIImpl) NewPendingTransactions(ctx context.Context) (*rpc.Subscription, error) {
	if api.filters == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		defer debug.LogPanic()
		txsCh := make(chan []types.Transaction, 1)
		defer close(txsCh)
		id := api.filters.SubscribePendingTxs(txsCh)
		defer api.filters.UnsubscribePendingTxs(id)

		for {
			select {
			case txs := <-txsCh:
				for _, t := range txs {
					if t != nil {
						err := notifier.Notify(rpcSub.ID, t.Hash())
						if err != nil {
							log.Warn("error while notifying subscription", "err", err)
						}
					}
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// SubscribeLogs send a notification each time a new log appears.
func (api *APIImpl) Logs(ctx context.Context, crit filters.FilterCriteria) (*rpc.Subscription, error) {
	if api.filters == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	rpcSub := notifier.CreateSubscription()

	go func() {
		defer debug.LogPanic()
		logs := make(chan *types.Log, 1)
		defer close(logs)
		id := api.filters.SubscribeLogs(logs, crit)
		defer api.filters.UnsubscribeLogs(id)

		for {
			select {
			case h := <-logs:
				err := notifier.Notify(rpcSub.ID, h)
				if err != nil {
					log.Warn("error while notifying subscription", "err", err)
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}
