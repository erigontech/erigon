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
	"errors"
	"strings"

	"github.com/erigontech/erigon-lib/common/debug"
	"github.com/erigontech/erigon-lib/log/v3"
	"github.com/erigontech/erigon-lib/types"
	"github.com/erigontech/erigon/eth/filters"
	"github.com/erigontech/erigon/rpc"
	"github.com/erigontech/erigon/rpc/rpchelper"
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
	if blocks, ok := api.filters.ReadPendingBlocks(rpchelper.HeadsSubID(cutIndex)); ok {
		for _, v := range blocks {
			stub = append(stub, v.Hash())
		}
		return stub, nil
	}
	if txs, ok := api.filters.ReadPendingTxs(rpchelper.PendingTxsSubID(cutIndex)); ok {
		if len(txs) > 0 {
			for _, txn := range txs[0] {
				stub = append(stub, txn.Hash())
			}
			return stub, nil
		}
		return stub, nil
	}
	if logs, ok := api.filters.ReadLogs(rpchelper.LogsSubID(cutIndex)); ok {
		for _, v := range logs {
			stub = append(stub, v)
		}
		return stub, nil
	}
	return nil, errors.New("filter not found")
}

// GetFilterLogs implements eth_getFilterLogs.
// Polling method for a previously created filter
// returns an array of logs which have occurred since the last poll.
func (api *APIImpl) GetFilterLogs(_ context.Context, index string) ([]*types.Log, error) {
	if api.filters == nil {
		return nil, rpc.ErrNotificationsUnsupported
	}
	cutIndex := strings.TrimPrefix(index, "0x")
	if logs, ok := api.filters.ReadLogs(rpchelper.LogsSubID(cutIndex)); ok {
		return logs, nil
	}
	return nil, errors.New("filter not found")
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
						log.Warn("[rpc] error while notifying subscription", "err", err)
					}
				}
				if !ok {
					log.Warn("[rpc] new heads channel was closed")
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
func (api *APIImpl) NewPendingTransactions(ctx context.Context, fullTx *bool) (*rpc.Subscription, error) {
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
						var err error
						if fullTx != nil && *fullTx {
							err = notifier.Notify(rpcSub.ID, t)
						} else {
							err = notifier.Notify(rpcSub.ID, t.Hash())
						}

						if err != nil {
							log.Warn("[rpc] error while notifying subscription", "err", err)
						}
					}
				}
				if !ok {
					log.Warn("[rpc] new pending transactions channel was closed")
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
							log.Warn("[rpc] error while notifying subscription", "err", err)
						}
					}
				}
				if !ok {
					log.Warn("[rpc] new pending transactions channel was closed")
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
		logs, id := api.filters.SubscribeLogs(api.SubscribeLogsChannelSize, crit)
		defer api.filters.UnsubscribeLogs(id)

		for {
			select {
			case h, ok := <-logs:
				if h != nil {
					err := notifier.Notify(rpcSub.ID, h)
					if err != nil {
						log.Warn("[rpc] error while notifying subscription", "err", err)
					}
				}
				if !ok {
					log.Warn("[rpc] log channel was closed")
					return
				}
			case <-rpcSub.Err():
				return
			}
		}
	}()

	return rpcSub, nil
}
