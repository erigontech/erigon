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

package privateapi

import (
	"fmt"
	"io"
	"sync"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/node/gointerfaces"
	"github.com/erigontech/erigon/node/gointerfaces/remoteproto"
	"github.com/erigontech/erigon/node/shards"
)

type ReceiptsFilterAggregator struct {
	aggReceiptsFilter  ReceiptsFilter
	receiptsFilters    map[uint64]*ReceiptsFilter
	receiptsFilterLock sync.Mutex
	nextFilterId       uint64
	events             *shards.Events
}

type ReceiptsFilter struct {
	allTxHashes int
	txHashes    map[common.Hash]int
	sender      remoteproto.ETHBACKEND_SubscribeReceiptsServer
}

func NewReceiptsFilterAggregator(events *shards.Events) *ReceiptsFilterAggregator {
	return &ReceiptsFilterAggregator{
		aggReceiptsFilter: ReceiptsFilter{
			txHashes: make(map[common.Hash]int),
		},
		receiptsFilters: make(map[uint64]*ReceiptsFilter),
		nextFilterId:    0,
		events:          events,
	}
}

func (a *ReceiptsFilterAggregator) insertReceiptsFilter(sender remoteproto.ETHBACKEND_SubscribeReceiptsServer) (uint64, *ReceiptsFilter) {
	a.receiptsFilterLock.Lock()
	defer a.receiptsFilterLock.Unlock()
	filterId := a.nextFilterId
	a.nextFilterId++
	filter := &ReceiptsFilter{
		txHashes: make(map[common.Hash]int),
		sender:   sender,
	}
	a.receiptsFilters[filterId] = filter
	return filterId, filter
}

func (a *ReceiptsFilterAggregator) checkEmpty() {
	isEmpty := a.aggReceiptsFilter.allTxHashes == 0 && len(a.aggReceiptsFilter.txHashes) == 0
	a.events.EmptyReceiptSubscription(isEmpty)
}

func (a *ReceiptsFilterAggregator) removeReceiptsFilter(filterId uint64, filter *ReceiptsFilter) {
	a.receiptsFilterLock.Lock()
	defer a.receiptsFilterLock.Unlock()
	a.subtractReceiptsFilters(filter)
	delete(a.receiptsFilters, filterId)
	a.checkEmpty()
}

func (a *ReceiptsFilterAggregator) updateReceiptsFilter(filter *ReceiptsFilter, filterReq *remoteproto.ReceiptsFilterRequest) {
	a.receiptsFilterLock.Lock()
	defer a.receiptsFilterLock.Unlock()
	a.subtractReceiptsFilters(filter)
	filter.txHashes = make(map[common.Hash]int)

	// Empty TransactionHashes slice (not nil) means subscribe to all
	txHashes := filterReq.GetTransactionHashes()
	if txHashes != nil && len(txHashes) == 0 {
		filter.allTxHashes = 1
	} else if filterReq.GetAllTransactions() {
		filter.allTxHashes = 1
	} else {
		filter.allTxHashes = 0
	}

	// Process specific transaction hashes
	for _, txHash := range txHashes {
		filter.txHashes[gointerfaces.ConvertH256ToHash(txHash)] = 1
	}

	a.addReceiptsFilters(filter)
	a.checkEmpty()
}

func (a *ReceiptsFilterAggregator) addReceiptsFilters(filter *ReceiptsFilter) {
	a.aggReceiptsFilter.allTxHashes += filter.allTxHashes
	for txHash, count := range filter.txHashes {
		a.aggReceiptsFilter.txHashes[txHash] += count
	}
}

func (a *ReceiptsFilterAggregator) subtractReceiptsFilters(filter *ReceiptsFilter) {
	a.aggReceiptsFilter.allTxHashes -= filter.allTxHashes
	for txHash, count := range filter.txHashes {
		a.aggReceiptsFilter.txHashes[txHash] -= count
		if a.aggReceiptsFilter.txHashes[txHash] == 0 {
			delete(a.aggReceiptsFilter.txHashes, txHash)
		}
	}
}

func (a *ReceiptsFilterAggregator) subscribeReceipts(server remoteproto.ETHBACKEND_SubscribeReceiptsServer) error {
	filterId, filter := a.insertReceiptsFilter(server)
	defer a.removeReceiptsFilter(filterId, filter)
	var filterReq *remoteproto.ReceiptsFilterRequest
	var recvErr error
	for filterReq, recvErr = server.Recv(); recvErr == nil; filterReq, recvErr = server.Recv() {
		a.updateReceiptsFilter(filter, filterReq)
	}
	if recvErr != io.EOF {
		return fmt.Errorf("receiving receipts filter request: %w", recvErr)
	}
	return nil
}

func (a *ReceiptsFilterAggregator) distributeReceipts(receipts []*remoteproto.SubscribeReceiptsReply) error {
	a.receiptsFilterLock.Lock()
	defer a.receiptsFilterLock.Unlock()
	filtersToDelete := make(map[uint64]*ReceiptsFilter)
	for _, receipt := range receipts {
		if a.aggReceiptsFilter.allTxHashes == 0 {
			txHash := gointerfaces.ConvertH256ToHash(receipt.TransactionHash)
			if _, ok := a.aggReceiptsFilter.txHashes[txHash]; !ok {
				continue
			}
		}
		for filterId, filter := range a.receiptsFilters {
			if filter.allTxHashes == 0 {
				txHash := gointerfaces.ConvertH256ToHash(receipt.TransactionHash)
				if _, ok := filter.txHashes[txHash]; !ok {
					continue
				}
			}
			if err := filter.sender.Send(receipt); err != nil {
				filtersToDelete[filterId] = filter
			}
		}
	}
	for filterId, filter := range filtersToDelete {
		a.subtractReceiptsFilters(filter)
		delete(a.receiptsFilters, filterId)
	}
	return nil
}
