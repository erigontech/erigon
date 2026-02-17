// Copyright 2025 The Erigon Authors
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

package rpchelper

import (
	sync "github.com/linkdata/deadlock"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/common/concurrent"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
)

type ReceiptsFilterAggregator struct {
	aggReceiptsFilter  ReceiptsFilter                                      // Aggregation of all current receipt filters
	receiptsFilters    *concurrent.SyncMap[ReceiptsSubID, *ReceiptsFilter] // Filter for each subscriber
	receiptsFilterLock sync.RWMutex
}

// ReceiptsFilter filters receipts by transaction hashes
type ReceiptsFilter struct {
	allTxHashes       int                                   // Counter: subscribe to all receipts if > 0
	transactionHashes *concurrent.SyncMap[common.Hash, int] // Transaction hashes to filter, with ref count
	sender            Sub[*remoteproto.SubscribeReceiptsReply]
}

// Send sends a receipt to the subscriber
func (f *ReceiptsFilter) Send(receipt *remoteproto.SubscribeReceiptsReply) {
	f.sender.Send(receipt)
}

// Close closes the sender
func (f *ReceiptsFilter) Close() {
	f.sender.Close()
}

// NewReceiptsFilterAggregator creates a new ReceiptsFilterAggregator
func NewReceiptsFilterAggregator() *ReceiptsFilterAggregator {
	return &ReceiptsFilterAggregator{
		aggReceiptsFilter: ReceiptsFilter{
			transactionHashes: concurrent.NewSyncMap[common.Hash, int](),
		},
		receiptsFilters: concurrent.NewSyncMap[ReceiptsSubID, *ReceiptsFilter](),
	}
}

// insertReceiptsFilter creates a fully-configured filter, inserts it into the map,
// and adds its counts to the aggregate, all under the write lock.
func (a *ReceiptsFilterAggregator) insertReceiptsFilter(sender Sub[*remoteproto.SubscribeReceiptsReply], txHashes []common.Hash, maxTxHashes int) ReceiptsSubID {
	filter := &ReceiptsFilter{
		transactionHashes: concurrent.NewSyncMap[common.Hash, int](),
		sender:            sender,
	}
	if len(txHashes) == 0 {
		filter.allTxHashes = 1
	} else {
		for i, txHash := range txHashes {
			if maxTxHashes > 0 && i >= maxTxHashes {
				break
			}
			filter.transactionHashes.Put(txHash, 1)
		}
	}

	a.receiptsFilterLock.Lock()
	defer a.receiptsFilterLock.Unlock()
	filterId := ReceiptsSubID(generateSubscriptionID())
	a.receiptsFilters.Put(filterId, filter)
	a.addReceiptsFilters(filter)
	return filterId
}

// removeReceiptsFilter removes a receipt filter
func (a *ReceiptsFilterAggregator) removeReceiptsFilter(filterId ReceiptsSubID) bool {
	a.receiptsFilterLock.Lock()
	defer a.receiptsFilterLock.Unlock()

	filter, ok := a.receiptsFilters.Get(filterId)
	if !ok {
		return false
	}

	filter.Close()
	a.subtractReceiptsFilters(filter)

	_, ok = a.receiptsFilters.Delete(filterId)
	return ok
}

// addReceiptsFilters adds filter counts to the aggregate
func (a *ReceiptsFilterAggregator) addReceiptsFilters(f *ReceiptsFilter) {
	a.aggReceiptsFilter.allTxHashes += f.allTxHashes

	f.transactionHashes.Range(func(txHash common.Hash, count int) error {
		a.aggReceiptsFilter.transactionHashes.DoAndStore(txHash, func(value int, exists bool) int {
			return value + count
		})
		return nil
	})
}

// subtractReceiptsFilters subtracts filter counts from the aggregate
func (a *ReceiptsFilterAggregator) subtractReceiptsFilters(f *ReceiptsFilter) {
	a.aggReceiptsFilter.allTxHashes -= f.allTxHashes

	f.transactionHashes.Range(func(txHash common.Hash, count int) error {
		a.aggReceiptsFilter.transactionHashes.Do(txHash, func(value int, exists bool) (int, bool) {
			if exists {
				newValue := value - count
				if newValue <= 0 {
					return 0, false
				}
				return newValue, true
			}
			return 0, false
		})
		return nil
	})
}

// createFilterRequest creates a ReceiptsFilterRequest from current state
func (a *ReceiptsFilterAggregator) createFilterRequest() *remoteproto.ReceiptsFilterRequest {
	a.receiptsFilterLock.RLock()
	defer a.receiptsFilterLock.RUnlock()

	req := &remoteproto.ReceiptsFilterRequest{
		AllTransactions: a.aggReceiptsFilter.allTxHashes >= 1,
	}

	// Always add specific transaction hashes (even if also subscribing to all)
	// Backend will use OR logic: send if (AllTransactions OR hash matches)
	a.aggReceiptsFilter.transactionHashes.Range(func(txHash common.Hash, count int) error {
		if count > 0 {
			req.TransactionHashes = append(req.TransactionHashes, gointerfaces.ConvertHashToH256(txHash))
		}
		return nil
	})

	return req
}

// distributeReceipt processes a receipt and distributes it to matching filters
func (a *ReceiptsFilterAggregator) distributeReceipt(receipt *remoteproto.SubscribeReceiptsReply) error {
	a.receiptsFilterLock.RLock()
	defer a.receiptsFilterLock.RUnlock()

	txHash := gointerfaces.ConvertH256ToHash(receipt.TransactionHash)

	a.receiptsFilters.Range(func(k ReceiptsSubID, filter *ReceiptsFilter) error {
		// Check if this filter matches the receipt
		if filter.allTxHashes == 0 {
			// Filter has specific transaction hashes
			if _, ok := filter.transactionHashes.Get(txHash); !ok {
				return nil // This filter doesn't want this receipt
			}
		}
		// allTxHashes > 0 means subscribe to all receipts

		// Send to subscriber
		filter.sender.Send(receipt)
		return nil
	})

	return nil
}
