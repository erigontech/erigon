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
	"maps"
	"slices"
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
	receipts    map[uint64]types.Receipts
	txs         map[uint64][]types.Transaction
	headers     map[uint64]*types.Header
	removedLogs []*LogNotification
	limit       uint64
	mu          sync.Mutex
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
	r.removedLogs = nil
}

// Snapshot returns a function that restores the cache to its state at the time of the call.
func (r *RecentReceipts) Snapshot() (restore func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	receipts, txs, headers, removedLogs := maps.Clone(r.receipts), maps.Clone(r.txs), maps.Clone(r.headers), slices.Clone(r.removedLogs)
	return func() {
		r.mu.Lock()
		defer r.mu.Unlock()
		r.receipts, r.txs, r.headers, r.removedLogs = receipts, txs, headers, removedLogs
	}
}

// NotifyLogs sends moved-aside logs as removed, then added logs in ascending block order for [from,to).
func (r *RecentReceipts) NotifyLogs(n ChainEventNotifier, from, to uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	defer func() { r.removedLogs = nil }()
	if !n.HasLogSubscriptions() {
		return
	}
	reply := r.removedLogs
	for _, bn := range slices.Sorted(maps.Keys(r.receipts)) {
		if bn+r.limit < from { // evict old
			delete(r.receipts, bn)
			delete(r.txs, bn)
			delete(r.headers, bn)
			continue
		}
		if bn < from || bn >= to {
			continue
		}
		reply = append(reply, logNotifications(r.receipts[bn], r.headers[bn], false)...)
	}
	if len(reply) > 0 {
		n.OnLogs(reply)
	}
}

func logNotifications(receipts types.Receipts, header *types.Header, removed bool) []*LogNotification {
	var blockTimestamp uint64
	if header != nil {
		blockTimestamp = header.Time
	}
	reply := make([]*LogNotification, 0, len(receipts))
	for _, receipt := range receipts {
		if receipt == nil {
			continue
		}
		for _, l := range receipt.Logs {
			reply = append(reply, &LogNotification{
				Log:            l,
				BlockTimestamp: blockTimestamp,
				Removed:        removed,
			})
		}
	}
	return reply
}

// Unwind moves entries above unwindPoint out of the active cache for the next removed-log notification.
func (r *RecentReceipts) Unwind(unwindPoint uint64) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for _, blockNum := range slices.Sorted(maps.Keys(r.receipts)) {
		if blockNum <= unwindPoint {
			continue
		}
		r.removedLogs = append(r.removedLogs, logNotifications(r.receipts[blockNum], r.headers[blockNum], true)...)
		delete(r.receipts, blockNum)
		delete(r.txs, blockNum)
		delete(r.headers, blockNum)
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

// NotifyReceipts sends added receipts in one batch, in ascending block order for [from,to).
func (r *RecentReceipts) NotifyReceipts(n ChainEventNotifier, from, to uint64) {
	if !n.HasReceiptSubscriptions() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	var reply []*ReceiptNotification
	for _, blockNum := range slices.Sorted(maps.Keys(r.receipts)) {
		if blockNum+r.limit < from { // evict old
			delete(r.receipts, blockNum)
			delete(r.txs, blockNum)
			delete(r.headers, blockNum)
			continue
		}
		if blockNum < from || blockNum >= to {
			continue
		}

		receipts := r.receipts[blockNum]
		txs := r.txs[blockNum]
		header := r.headers[blockNum]
		if len(receipts) == 0 || len(txs) == 0 || header == nil {
			continue
		}

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
			})
		}
	}
	if len(reply) > 0 {
		n.OnReceipts(reply)
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
