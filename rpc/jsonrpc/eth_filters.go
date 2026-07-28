// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package jsonrpc

import (
	"context"
	"strings"

	"github.com/erigontech/erigon/common/dbg"
	"github.com/erigontech/erigon/common/log/v3"
	"github.com/erigontech/erigon/execution/types"
	"github.com/erigontech/erigon/execution/types/ethutils"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/filters"
	"github.com/erigontech/erigon/rpc/rpchelper"
)

// NewPendingTransactionFilter new transaction filter
func (api *APIImpl) NewPendingTransactionFilter(_ context.Context) (string, error) {
	if api.filters == nil {
		return "", rpc.ErrNotificationsUnsupported
	}
	txsCh, id := api.filters.SubscribePendingTxs(32, rpchelper.ProtocolHTTP)
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
	ch, id := api.filters.SubscribeNewHeads(32, rpchelper.ProtocolHTTP)
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
	logs, id, err := api.filters.SubscribeLogs(256, crit, rpchelper.ProtocolHTTP)
	if err != nil {
		return "", err
	}
	go func() {
		for lg := range logs {
			api.filters.AddLogs(id, lg)
		}
	}()
	return "0x" + string(id), nil
}

// UninstallFilter new transaction filter
func (api *APIImpl) UninstallFilter(_ context.Context, index string) (isDeleted bool, err error) {
	if api.filters == nil {
		return false, rpc.ErrNotificationsUnsupported
	}
	// remove 0x
	cutIndex := strings.TrimPrefix(index, "0x")
	if ok := api.filters.UnsubscribeHeads(rpchelper.HeadsSubID(cutIndex)); ok {
		isDeleted = true
	}
	if ok := api.filters.UnsubscribePendingTxs(rpchelper.PendingTxsSubID(cutIndex)); ok {
		isDeleted = true
	}
	if ok := api.filters.UnsubscribeLogs(rpchelper.LogsSubID(cutIndex)); ok {
		isDeleted = true
	}
	return
}

// GetFilterChanges implements eth_getFilterChanges.
// Polling method for a previously created filter
// returns an array of logs, block headers, or pending transactions which have occurred since the last poll.
func (api *APIImpl) GetFilterChanges(_ context.Context, index string) ([]any, error) {
	if api.filters == nil {
		return nil, rpc.ErrNotificationsUnsupported
	}
	stub := make([]any, 0)
	// remove 0x
	cutIndex := strings.TrimPrefix(index, "0x")
	ft, ok := api.filters.TouchSubscription(rpchelper.SubscriptionID(cutIndex))
	if !ok {
		return nil, rpc.ErrFilterNotFound
	}
	switch ft {
	case rpchelper.FilterTypeHeads:
		if blocks, ok := api.filters.ReadPendingBlocks(rpchelper.HeadsSubID(cutIndex)); ok {
			for _, v := range blocks {
				stub = append(stub, v.Hash())
			}
		}
	case rpchelper.FilterTypePendingTxs:
		if txs, ok := api.filters.ReadPendingTxs(rpchelper.PendingTxsSubID(cutIndex)); ok {
			for _, batch := range txs {
				for _, txn := range batch {
					stub = append(stub, txn.Hash())
				}
			}
		}
	case rpchelper.FilterTypeLogs:
		if logs, ok := api.filters.ReadLogs(rpchelper.LogsSubID(cutIndex)); ok {
			for _, v := range logs {
				stub = append(stub, v)
			}
		}
	}
	return stub, nil
}

// GetFilterLogs implements eth_getFilterLogs.
// Polling method for a previously created filter
// returns an array of logs which have occurred since the last poll.
func (api *APIImpl) GetFilterLogs(_ context.Context, index string) ([]*types.Log, error) {
	if api.filters == nil {
		return nil, rpc.ErrNotificationsUnsupported
	}
	cutIndex := strings.TrimPrefix(index, "0x")
	if ft, ok := api.filters.TouchSubscription(rpchelper.SubscriptionID(cutIndex)); !ok || ft != rpchelper.FilterTypeLogs {
		return nil, rpc.ErrFilterNotFound
	}
	if logs, ok := api.filters.ReadLogs(rpchelper.LogsSubID(cutIndex)); ok {
		return logs, nil
	}
	return []*types.Log{}, nil
}

// subscribeRPC runs the shared subscription skeleton: guard checks, subscription
// creation, and a goroutine that pumps items from the filter channel into notify until
// the channel closes or the client goes away. subscribe is called inside the goroutine
// and must return the item channel plus an unsubscribe func. notify receives an emit
// func that sends a payload to the client, logging on failure.
func subscribeRPC[T any](ctx context.Context, apiFilters *rpchelper.Filters, subscribe func() (<-chan T, func(), error), notify func(emit func(payload any), item T), closedWarn string) (*rpc.Subscription, error) {
	if apiFilters == nil {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}
	notifier, supported := rpc.NotifierFromContext(ctx)
	if !supported {
		return &rpc.Subscription{}, rpc.ErrNotificationsUnsupported
	}

	ch, unsubscribe, err := subscribe()
	if err != nil {
		return nil, err
	}
	rpcSub := notifier.CreateSubscription()

	go func() {
		defer dbg.LogPanic()
		defer unsubscribe()

		emit := func(payload any) {
			if err := notifier.Notify(rpcSub.ID, payload); err != nil {
				log.Warn("[rpc] error while notifying subscription", "err", err)
			}
		}
		for {
			select {
			case item, ok := <-ch:
				if !ok {
					log.Warn(closedWarn)
					return
				}
				notify(emit, item)
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}

// NewHeads send a notification each time a new (header) block is appended to the chain.
func (api *APIImpl) NewHeads(ctx context.Context) (*rpc.Subscription, error) {
	return subscribeRPC(ctx, api.filters,
		func() (<-chan *types.Header, func(), error) {
			headers, id := api.filters.SubscribeNewHeads(32, rpchelper.ProtocolWS)
			return headers, func() { api.filters.UnsubscribeHeads(id) }, nil
		},
		func(emit func(payload any), h *types.Header) {
			if h != nil {
				emit(h)
			}
		},
		"[rpc] new heads channel was closed")
}

// NewPendingTransactions send a notification each time when a transaction had added into mempool.
func (api *APIImpl) NewPendingTransactions(ctx context.Context, fullTx *bool) (*rpc.Subscription, error) {
	return api.subscribePendingTransactions(ctx, 256, fullTx != nil && *fullTx)
}

// NewPendingTransactionsWithBody send a notification each time when a transaction had added into mempool.
func (api *APIImpl) NewPendingTransactionsWithBody(ctx context.Context) (*rpc.Subscription, error) {
	return api.subscribePendingTransactions(ctx, 512, true)
}

func (api *APIImpl) subscribePendingTransactions(ctx context.Context, chanSize int, fullTx bool) (*rpc.Subscription, error) {
	return subscribeRPC(ctx, api.filters,
		func() (<-chan []types.Transaction, func(), error) {
			txsCh, id := api.filters.SubscribePendingTxs(chanSize, rpchelper.ProtocolWS)
			return txsCh, func() { api.filters.UnsubscribePendingTxs(id) }, nil
		},
		func(emit func(payload any), txs []types.Transaction) {
			for _, t := range txs {
				if t != nil {
					if fullTx {
						emit(newRPCPendingTransaction(t, nil, nil))
					} else {
						emit(t.Hash())
					}
				}
			}
		},
		"[rpc] new pending transactions channel was closed")
}

// Logs send a notification each time a new log appears.
func (api *APIImpl) Logs(ctx context.Context, crit filters.FilterCriteria) (*rpc.Subscription, error) {
	return subscribeRPC(ctx, api.filters,
		func() (<-chan *types.Log, func(), error) {
			logs, id, err := api.filters.SubscribeLogs(api.SubscribeLogsChannelSize, crit, rpchelper.ProtocolWS)
			if err != nil {
				return nil, nil, err
			}
			return logs, func() { api.filters.UnsubscribeLogs(id) }, nil
		},
		func(emit func(payload any), h *types.Log) {
			if h != nil {
				emit(h)
			}
		},
		"[rpc] log channel was closed")
}

// TransactionReceipts send a notification each time a new receipt appears.
func (api *APIImpl) TransactionReceipts(ctx context.Context, crit filters.ReceiptsFilterCriteria) (*rpc.Subscription, error) {
	return subscribeRPC(ctx, api.filters,
		func() (<-chan *remoteproto.SubscribeReceiptsReply, func(), error) {
			receipts, id, err := api.filters.SubscribeReceipts(api.SubscribeLogsChannelSize, crit)
			if err != nil {
				return nil, nil, err
			}
			return receipts, func() { api.filters.UnsubscribeReceipts(id) }, nil
		},
		func(emit func(payload any), protoReceipt *remoteproto.SubscribeReceiptsReply) {
			if protoReceipt != nil {
				receipt := ethutils.MarshalSubscribeReceipt(protoReceipt)
				emit([]map[string]any{receipt})
			}
		},
		"[rpc] receipts channel was closed")
}
