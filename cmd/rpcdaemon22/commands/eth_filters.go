package commands

import (
	"context"
	"fmt"

	"github.com/ledgerwatch/erigon/common"
	"github.com/ledgerwatch/erigon/common/debug"
	"github.com/ledgerwatch/erigon/common/hexutil"
	"github.com/ledgerwatch/erigon/core/types"
	"github.com/ledgerwatch/erigon/eth/filters"
	"github.com/ledgerwatch/erigon/rpc"
	"github.com/ledgerwatch/erigon/turbo/rpchelper"
	"github.com/ledgerwatch/log/v3"
)

// NewPendingTransactionFilter new transaction filter
func (api *APIImpl) NewPendingTransactionFilter(_ context.Context) (common.Hash, error) {
	if api.filters == nil {
		return common.Hash{}, rpc.ErrNotificationsUnsupported
	}
	txsCh := make(chan []types.Transaction, 1)
	id := api.filters.SubscribePendingTxs(txsCh)
	go func() {
		for {
			select {
			case txs, ok := <-txsCh:
				if !ok {
					return
				}
				api.filters.AddPendingTxs(id, txs)
			default:
			}
		}
	}()
	return common.HexToHash(string(id)), nil
}

// NewBlockFilter implements eth_newBlockFilter. Creates a filter in the node, to notify when a new block arrives.
func (api *APIImpl) NewBlockFilter(_ context.Context) (common.Hash, error) {
	if api.filters == nil {
		return common.Hash{}, rpc.ErrNotificationsUnsupported
	}
	ch := make(chan *types.Block, 1)
	id := api.filters.SubscribePendingBlock(ch)
	go func() {
		for {
			select {
			case block, ok := <-ch:
				if !ok {
					return
				}
				api.filters.AddPendingBlock(id, block)
			default:
			}
		}
	}()
	return common.HexToHash(string(id)), nil
}

// NewFilter implements eth_newFilter. Creates an arbitrary filter object, based on filter options, to notify when the state changes (logs).
func (api *APIImpl) NewFilter(_ context.Context, crit filters.FilterCriteria) (common.Hash, error) {
	if api.filters == nil {
		return common.Hash{}, rpc.ErrNotificationsUnsupported
	}
	logs := make(chan *types.Log, 1)
	id := api.filters.SubscribeLogs(logs, crit)
	go func() {
		for {
			select {
			case lg, ok := <-logs:
				if !ok {
					return
				}
				api.filters.AddLogs(id, lg)
			default:
			}
		}
	}()
	return common.HexToHash(hexutil.EncodeUint64(uint64(id))), nil
}

// UninstallFilter new transaction filter
func (api *APIImpl) UninstallFilter(_ context.Context, index string) (bool, error) {
	if api.filters == nil {
		return false, rpc.ErrNotificationsUnsupported
	}
	if common.IsHexAddress32(index) {
		// remove 0x
		if len(index) >= 2 && index[0] == '0' && (index[1] == 'x' || index[1] == 'X') {
			index = index[2:]
		}
		isDeleted := api.filters.UnsubscribePendingBlock(rpchelper.PendingBlockSubID(index)) ||
			api.filters.UnsubscribePendingTxs(rpchelper.PendingTxsSubID(index))
		id, err := hexutil.DecodeUint64(index)
		if err == nil {
			return isDeleted || api.filters.UnsubscribeLogs(rpchelper.LogsSubID(id)), nil
		}
	}

	return false, nil
}

// GetFilterChanges implements eth_getFilterChanges. Polling method for a previously-created filter, which returns an array of logs which occurred since last poll.
func (api *APIImpl) GetFilterChanges(_ context.Context, index string) ([]interface{}, error) {
	if api.filters == nil {
		return nil, rpc.ErrNotificationsUnsupported
	}
	stub := make([]interface{}, 0)
	if common.IsHexAddress32(index) {
		// remove 0x
		if len(index) >= 2 && index[0] == '0' && (index[1] == 'x' || index[1] == 'X') {
			index = index[2:]
		}
		if blocks, ok := api.filters.ReadPendingBlocks(rpchelper.PendingBlockSubID(index)); ok {
			for _, v := range blocks {
				stub = append(stub, v.Hash())
			}
			return stub, nil
		}
		if txs, ok := api.filters.ReadPendingTxs(rpchelper.PendingTxsSubID(index)); ok {
			for _, v := range txs {
				for _, tx := range v {
					stub = append(stub, tx.Hash())
				}
			}
			return stub, nil
		}
		id, err := hexutil.DecodeUint64(index)
		if err != nil {
			return stub, fmt.Errorf("eth_getFilterChanges, wrong index: %w", err)
		}
		if logs, ok := api.filters.ReadLogs(rpchelper.LogsSubID(id)); ok {
			for _, v := range logs {
				stub = append(stub, v)
			}
			return stub, nil
		}
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
		logs := make(chan *types.Log, 1)
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
