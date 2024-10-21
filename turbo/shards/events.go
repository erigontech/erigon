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

package shards

import (
	"sync"

	"github.com/erigontech/erigon-lib/common"
	"github.com/erigontech/erigon-lib/gointerfaces"
	remote "github.com/erigontech/erigon-lib/gointerfaces/remoteproto"
	types2 "github.com/erigontech/erigon-lib/gointerfaces/typesproto"
	"github.com/erigontech/erigon/core/types"
)

type RpcEventType uint64

type NewSnapshotSubscription func() error
type HeaderSubscription func(headerRLP []byte) error
type PendingLogsSubscription func(types.Logs) error
type PendingBlockSubscription func(*types.Block) error
type PendingTxsSubscription func([]types.Transaction) error
type LogsSubscription func([]*remote.SubscribeLogsReply) error

// Events manages event subscriptions and dissimination. Thread-safe
type Events struct {
	id                        int
	headerSubscriptions       map[int]chan [][]byte
	newSnapshotSubscription   map[int]chan struct{}
	pendingLogsSubscriptions  map[int]PendingLogsSubscription
	pendingBlockSubscriptions map[int]PendingBlockSubscription
	pendingTxsSubscriptions   map[int]PendingTxsSubscription
	logsSubscriptions         map[int]chan []*remote.SubscribeLogsReply
	hasLogSubscriptions       bool
	lock                      sync.RWMutex
}

func NewEvents() *Events {
	return &Events{
		headerSubscriptions:       map[int]chan [][]byte{},
		pendingLogsSubscriptions:  map[int]PendingLogsSubscription{},
		pendingBlockSubscriptions: map[int]PendingBlockSubscription{},
		pendingTxsSubscriptions:   map[int]PendingTxsSubscription{},
		logsSubscriptions:         map[int]chan []*remote.SubscribeLogsReply{},
		newSnapshotSubscription:   map[int]chan struct{}{},
	}
}

func (e *Events) AddHeaderSubscription() (chan [][]byte, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan [][]byte, 8)
	e.id++
	id := e.id
	e.headerSubscriptions[id] = ch
	return ch, func() {
		delete(e.headerSubscriptions, id)
		close(ch)
	}
}

func (e *Events) AddNewSnapshotSubscription() (chan struct{}, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan struct{}, 8)
	e.id++
	id := e.id
	e.newSnapshotSubscription[id] = ch
	return ch, func() {
		delete(e.newSnapshotSubscription, id)
		close(ch)
	}
}

func (e *Events) AddLogsSubscription() (chan []*remote.SubscribeLogsReply, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan []*remote.SubscribeLogsReply, 8)
	e.id++
	id := e.id
	e.logsSubscriptions[id] = ch
	return ch, func() {
		delete(e.logsSubscriptions, id)
		close(ch)
	}
}

func (e *Events) EmptyLogSubsctiption(empty bool) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.hasLogSubscriptions = !empty
}

func (e *Events) HasLogSubsriptions() bool {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.hasLogSubscriptions
}

func (e *Events) AddPendingLogsSubscription(s PendingLogsSubscription) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.pendingLogsSubscriptions[len(e.pendingLogsSubscriptions)] = s
}

func (e *Events) AddPendingBlockSubscription(s PendingBlockSubscription) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.pendingBlockSubscriptions[len(e.pendingBlockSubscriptions)] = s
}

func (e *Events) OnNewSnapshot() {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, ch := range e.newSnapshotSubscription {
		common.PrioritizedSend(ch, struct{}{})
	}
}

func (e *Events) OnNewHeader(newHeadersRlp [][]byte) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, ch := range e.headerSubscriptions {
		common.PrioritizedSend(ch, newHeadersRlp)
	}
}

func (e *Events) OnNewPendingLogs(logs types.Logs) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for i, sub := range e.pendingLogsSubscriptions {
		if err := sub(logs); err != nil {
			delete(e.pendingLogsSubscriptions, i)
		}
	}
}

func (e *Events) OnLogs(logs []*remote.SubscribeLogsReply) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, ch := range e.logsSubscriptions {
		common.PrioritizedSend(ch, logs)
	}
}

type Notifications struct {
	Events               *Events
	Accumulator          *Accumulator // StateAccumulator
	StateChangesConsumer StateChangeConsumer
	RecentLogs           *RecentLogs
}

func NewNotifications(StateChangesConsumer StateChangeConsumer) *Notifications {
	return &Notifications{
		Events:               NewEvents(),
		Accumulator:          NewAccumulator(),
		RecentLogs:           NewRecentLogs(512),
		StateChangesConsumer: StateChangesConsumer,
	}
}

// Requirements:
// - Erigon3 doesn't store logs in db (yet)
// - need support unwind of receipts
// - need send notification after `rwtx.Commit` (or user will recv notification, but can't request new data by RPC)
type RecentLogs struct {
	receipts map[uint64]types.Receipts
	limit    uint64
	mu       sync.Mutex
}

func NewRecentLogs(limit uint64) *RecentLogs {
	return &RecentLogs{receipts: make(map[uint64]types.Receipts, limit), limit: limit}
}

// [from,to)
func (r *RecentLogs) Notify(n *Events, from, to uint64, isUnwind bool) {
	if !n.HasLogSubsriptions() {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for bn, receipts := range r.receipts {
		if bn+r.limit < from { //evict old
			delete(r.receipts, bn)
			continue
		}
		if bn < from || bn >= to {
			continue
		}

		var blockNum uint64
		reply := make([]*remote.SubscribeLogsReply, 0, len(receipts))
		for _, receipt := range receipts {
			blockNum = receipt.BlockNumber.Uint64()
			//txIndex++
			//// bor transactions are at the end of the bodies transactions (added manually but not actually part of the block)
			//if txIndex == uint64(len(block.Transactions())) {
			//	txHash = bortypes.ComputeBorTxHash(blockNum, block.Hash())
			//} else {
			//	txHash = block.Transactions()[txIndex].Hash()
			//}

			for _, l := range receipt.Logs {
				res := &remote.SubscribeLogsReply{
					Address:          gointerfaces.ConvertAddressToH160(receipt.ContractAddress),
					BlockHash:        gointerfaces.ConvertHashToH256(receipt.BlockHash),
					BlockNumber:      blockNum,
					Data:             l.Data,
					LogIndex:         uint64(l.Index),
					Topics:           make([]*types2.H256, 0, len(l.Topics)),
					TransactionHash:  gointerfaces.ConvertHashToH256(receipt.TxHash),
					TransactionIndex: uint64(l.TxIndex),
					Removed:          isUnwind,
				}
				for _, topic := range l.Topics {
					res.Topics = append(res.Topics, gointerfaces.ConvertHashToH256(topic))
				}
				reply = append(reply, res)
			}
		}

		n.OnLogs(reply)
	}
}

func (r *RecentLogs) Add(receipts types.Receipts) {
	if len(receipts) == 0 {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	// find non-nil receipt
	for _, receipt := range receipts {
		if receipt != nil {
			r.receipts[receipts[0].BlockNumber.Uint64()] = receipts
			return
		}
	}
}
