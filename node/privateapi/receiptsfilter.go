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
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/types"
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

// distributeReceipts receives native receipt notifications, filters them, and converts
// to protobuf only when sending over gRPC.
func (a *ReceiptsFilterAggregator) distributeReceipts(receipts []*notifications.ReceiptNotification) error {
	a.receiptsFilterLock.Lock()
	defer a.receiptsFilterLock.Unlock()
	filtersToDelete := make(map[uint64]*ReceiptsFilter)
	for _, rn := range receipts {
		txHash := rn.Receipt.TxHash
		if a.aggReceiptsFilter.allTxHashes == 0 {
			if _, ok := a.aggReceiptsFilter.txHashes[txHash]; !ok {
				continue
			}
		}
		// Lazy convert: only build protobuf when we actually need to send
		var proto *remoteproto.SubscribeReceiptsReply
		for filterId, filter := range a.receiptsFilters {
			if filter.allTxHashes == 0 {
				if _, ok := filter.txHashes[txHash]; !ok {
					continue
				}
			}
			if proto == nil {
				proto = receiptNotificationToProto(rn)
			}
			if err := filter.sender.Send(proto); err != nil {
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

// receiptNotificationToProto converts a native ReceiptNotification to protobuf for gRPC.
func receiptNotificationToProto(rn *notifications.ReceiptNotification) *remoteproto.SubscribeReceiptsReply {
	receipt := rn.Receipt
	blockNum := receipt.BlockNumber.Uint64()

	// Convert logs
	protoLogs := make([]*remoteproto.SubscribeLogsReply, 0, len(receipt.Logs))
	for _, l := range receipt.Logs {
		protoLogs = append(protoLogs, logNotificationToProto(&notifications.LogNotification{
			Log:     l,
			Removed: rn.Removed,
		}))
	}

	protoReceipt := &remoteproto.SubscribeReceiptsReply{
		BlockHash:         gointerfaces.ConvertHashToH256(receipt.BlockHash),
		BlockNumber:       blockNum,
		TransactionHash:   gointerfaces.ConvertHashToH256(receipt.TxHash),
		TransactionIndex:  uint64(receipt.TransactionIndex),
		Type:              uint32(receipt.Type),
		Status:            receipt.Status,
		CumulativeGasUsed: receipt.CumulativeGasUsed,
		GasUsed:           receipt.GasUsed,
		LogsBloom:         receipt.Bloom[:],
		Logs:              protoLogs,
		BlobGasUsed:       receipt.BlobGasUsed,
	}

	// Add contract address if present
	if receipt.ContractAddress != (common.Address{}) {
		protoReceipt.ContractAddress = gointerfaces.ConvertAddressToH160(receipt.ContractAddress)
	}

	// Add transaction data (from/to)
	if rn.Tx != nil {
		signer := types.MakeSigner(nil, blockNum, 0)
		if sender, err := rn.Tx.Sender(*signer); err == nil {
			protoReceipt.From = gointerfaces.ConvertAddressToH160(sender.Value())
		}
		if to := rn.Tx.GetTo(); to != nil {
			protoReceipt.To = gointerfaces.ConvertAddressToH160(*to)
		}
		protoReceipt.TxType = uint32(rn.Tx.Type())
	}

	// Add header data
	if rn.Header != nil {
		if rn.Header.BaseFee != nil {
			protoReceipt.BaseFee = gointerfaces.ConvertUint256IntToH256(rn.Header.BaseFee)
		}
		protoReceipt.BlockTime = rn.Header.Time
		if rn.Header.ExcessBlobGas != nil {
			protoReceipt.ExcessBlobGas = *rn.Header.ExcessBlobGas
		}
	}

	return protoReceipt
}
