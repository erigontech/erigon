package commands

import (
	"context"

	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"
)

// NewPendingTransactionFilter new transaction filter
func (api *APIImpl) NewPendingTransactionFilter(_ context.Context) (string, error) {
	if api.filters == nil {
		return "", rpc.ErrNotificationsUnsupported
	}
	txsCh, id := api.filters.SubscribePendingTxs(32)
	go func() {
		for txs := range txsCh {
			api.filters.AddPendingTxs(id, txs)
		}
	}()
	return "0x" + string(id), nil
}

// NewBlockFilter implements eth_newBlockFilter. Creates a filter in the node, to notify when a new block arrives.
func (api *APIImpl) NewBlockFilter(_ context.Context) (string, error) {
	if api.filters == nil {
		return "", rpc.ErrNotificationsUnsupported
	}
	ch, id := api.filters.SubscribeNewHeads(32)
	go func() {
		for block := range ch {
			api.filters.AddPendingBlock(id, block)
		}
	}()
	return "0x" + string(id), nil
}

// NewFilter implements eth_newFilter. Creates an arbitrary filter object, based on filter options, to notify when the state changes (logs).
func (api *APIImpl) NewFilter(_ context.Context, crit filters.FilterCriteria) (string, error) {
	if api.filters == nil {
		return "", rpc.ErrNotificationsUnsupported
	}
	logs, id := api.filters.SubscribeLogs(256, crit)
	go func() {
		for lg := range logs {
			api.filters.AddLogs(id, lg)
		}
	}()
	return hexutil.EncodeUint64(uint64(id)), nil
}

// UninstallFilter new transaction filter
func (api *APIImpl) UninstallFilter(_ context.Context, index string) (bool, error) {
	if api.filters == nil {
		return false, rpc.ErrNotificationsUnsupported
	}
	var isDeleted bool
	// remove 0x
	cutIndex := index
	if len(index) >= 2 && index[0] == '0' && (index[1] == 'x' || index[1] == 'X') {
		cutIndex = index[2:]
	}
	isDeleted = api.filters.UnsubscribeHeads(rpchelper.HeadsSubID(cutIndex)) ||
		api.filters.UnsubscribePendingTxs(rpchelper.PendingTxsSubID(cutIndex))
	id, err := hexutil.DecodeUint64(index)
	if err == nil {
		return isDeleted || api.filters.UnsubscribeLogs(rpchelper.LogsSubID(id)), nil
	}

	return isDeleted, nil
}

// GetFilterChanges implements eth_getFilterChanges. Polling method for a previously-created filter, which returns an array of logs which occurred since last poll.
func (api *APIImpl) GetFilterChanges(_ context.Context, index string) ([]interface{}, error) {
	if api.filters == nil {
		return nil, rpc.ErrNotificationsUnsupported
	}
	stub := make([]interface{}, 0)

	// remove 0x
	cutIndex := index
	if len(index) >= 2 && index[0] == '0' && (index[1] == 'x' || index[1] == 'X') {
		cutIndex = index[2:]
	}
	if blocks, ok := api.filters.ReadPendingBlocks(rpchelper.HeadsSubID(cutIndex)); ok {
		for _, v := range blocks {
			stub = append(stub, v.Hash())
		}
		return stub, nil
	}
	if txs, ok := api.filters.ReadPendingTxs(rpchelper.PendingTxsSubID(cutIndex)); ok {
		if len(txs) > 0 {
			for _, tx := range txs[0] {
				stub = append(stub, tx.Hash())
			}
			return stub, nil
		}
		return stub, nil
	}
	id, err := hexutil.DecodeUint64(index)
	if err != nil {
		return stub, nil
	}
	if logs, ok := api.filters.ReadLogs(rpchelper.LogsSubID(id)); ok {
		for _, v := range logs {
			stub = append(stub, v)
		}
		return stub, nil
	}
	return stub, nil
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
		headers, id := api.filters.SubscribeNewHeads(32)
		defer api.filters.UnsubscribeHeads(id)

		for {
			select {
			case h, ok := <-headers:
				if h != nil {
					err := notifier.Notify(rpcSub.ID, h)
					if err != nil {
						log.Warn("error while notifying subscription", "err", err)
						return
					}
				}
				if !ok {
					log.Warn("new heads channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// NewPendingTransactions send a notification each time when a transaction had added into mempool.
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
		txsCh, id := api.filters.SubscribePendingTxs(256)
		defer api.filters.UnsubscribePendingTxs(id)

		for {
			select {
			case txs, ok := <-txsCh:
				for _, t := range txs {
					if t != nil {
						err := notifier.Notify(rpcSub.ID, t.Hash())
						if err != nil {
							log.Warn("error while notifying subscription", "err", err)
							return
						}
					}
				}
				if !ok {
					log.Warn("new pending transactions channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// NewPendingTransactionsWithBody send a notification each time when a transaction had added into mempool.
func (api *APIImpl) NewPendingTransactionsWithBody(ctx context.Context) (*rpc.Subscription, error) {
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
		txsCh, id := api.filters.SubscribePendingTxs(512)
		defer api.filters.UnsubscribePendingTxs(id)

		for {
			select {
			case txs, ok := <-txsCh:
				for _, t := range txs {
					if t != nil {
						err := notifier.Notify(rpcSub.ID, t)
						if err != nil {
							log.Warn("error while notifying subscription", "err", err)
							return
						}
					}
				}
				if !ok {
					log.Warn("new pending transactions channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// Logs send a notification each time a new log appears.
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
		logs, id := api.filters.SubscribeLogs(128, crit)
		defer api.filters.UnsubscribeLogs(id)

		for {
			select {
			case h, ok := <-logs:
				if h != nil {
					err := notifier.Notify(rpcSub.ID, h)
					if err != nil {
						log.Warn("error while notifying subscription", "err", err)
						return
					}
				}
				if !ok {
					log.Warn("log channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}
