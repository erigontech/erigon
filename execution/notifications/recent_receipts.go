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

package notifications

import (
	"sync"

	"github.com/erigontech/erigon/execution/types"
)

// RecentReceipts caches recent block receipts for notification dispatch.
//
// Requirements:
// - Erigon3 doesn't store logs in db (yet)
// - need support unwind of receipts
// - need send notification after `rwtx.Commit` (or user will recv notification, but can't request new data by RPC)
type RecentReceipts struct {
	receipts map[uint64]types.Receipts
	txs      map[uint64][]types.Transaction
	headers  map[uint64]*types.Header
	limit    uint64
	mu       sync.Mutex
}

func NewRecentReceipts(limit uint64) *RecentReceipts {
	return &RecentReceipts{
		receipts: make(map[uint64]types.Receipts, limit),
		txs:      make(map[uint64][]types.Transaction, limit),
		headers:  make(map[uint64]*types.Header, limit),
		limit:    limit,
	}
}

// Clear removes all stored receipts, transactions, and headers.
func (r *RecentReceipts) Clear() {
	r.mu.Lock()
	defer r.mu.Unlock()
	clear(r.receipts)
	clear(r.txs)
	clear(r.headers)
}

// NotifyLogs sends log notifications in native types (no protobuf conversion).
// [from,to)
func (r *RecentReceipts) NotifyLogs(n ChainEventNotifier, from, to uint64, isUnwind bool) {
	if !n.HasLogSubscriptions() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for bn, receipts := range r.receipts {
		if bn+r.limit < from { // evict old
			delete(r.receipts, bn)
			delete(r.txs, bn)
			delete(r.headers, bn)
			continue
		}
		if bn < from || bn >= to {
			continue
		}

		reply := make([]*LogNotification, 0, len(receipts))
		for _, receipt := range receipts {
			if receipt == nil {
				continue
			}
			for _, l := range receipt.Logs {
				reply = append(reply, &LogNotification{
					Log:     l,
					Removed: isUnwind,
				})
			}
		}

		n.OnLogs(reply)
	}
}

func (r *RecentReceipts) Add(receipts types.Receipts, txs []types.Transaction, header *types.Header) {
	if len(receipts) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var blockNum uint64
	var ok bool
	// find non-nil receipt
	for _, receipt := range receipts {
		if receipt != nil {
			ok = true
			blockNum = receipt.BlockNumber.Uint64()
			break
		}
	}
	if !ok {
		return
	}
	r.receipts[blockNum] = receipts
	r.txs[blockNum] = txs
	r.headers[blockNum] = header

	// enforce `limit`: drop all items older than `limit` blocks
	if len(r.receipts) <= int(r.limit) {
		return
	}
	for bn := range r.receipts {
		if bn+r.limit < blockNum {
			delete(r.receipts, bn)
			delete(r.txs, bn)
			delete(r.headers, bn)
		}
	}
}

// NotifyReceipts sends receipt notifications in native types (no protobuf conversion).
// [from,to)
func (r *RecentReceipts) NotifyReceipts(n ChainEventNotifier, from, to uint64, isUnwind bool) {
	if !n.HasReceiptSubscriptions() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	for blockNum, receipts := range r.receipts {
		if blockNum+r.limit < from { // evict old
			delete(r.receipts, blockNum)
			delete(r.txs, blockNum)
			delete(r.headers, blockNum)
			continue
		}
		if blockNum < from || blockNum >= to {
			continue
		}

		txs := r.txs[blockNum]
		header := r.headers[blockNum]
		if len(receipts) == 0 || len(txs) == 0 || header == nil {
			continue
		}

		var reply []*ReceiptNotification
		for _, receipt := range receipts {
			if receipt == nil {
				continue
			}
			txIndex := receipt.TransactionIndex
			if int(txIndex) >= len(txs) {
				continue
			}
			reply = append(reply, &ReceiptNotification{
				Receipt: receipt,
				Tx:      txs[txIndex],
				Header:  header,
				Removed: isUnwind,
			})
		}

		if len(reply) > 0 {
			n.OnReceipts(reply)
		}
	}
}

func (r *RecentReceipts) CopyAndReset(target *RecentReceipts) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for blockNum, receipts := range r.receipts {
		txs := r.txs[blockNum]
		header := r.headers[blockNum]
		target.Add(receipts, txs, header)
		delete(r.receipts, blockNum)
		delete(r.txs, blockNum)
		delete(r.headers, blockNum)
	}
}
