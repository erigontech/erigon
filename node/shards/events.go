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
	"sync/atomic"

	"github.com/erigontech/erigon/common"
	"github.com/erigontech/erigon/db/state/execctx"
	"github.com/erigontech/erigon/execution/notifications"
	"github.com/erigontech/erigon/execution/types"
)

// RecentReceipts re-exports the type from execution/notifications.
type RecentReceipts = notifications.RecentReceipts

func NewRecentReceipts(limit uint64) *RecentReceipts {
	return notifications.NewRecentReceipts(limit)
}

type NewSnapshotSubscription func() error
type HeaderSubscription func(headerRLP []byte) error
type PendingLogsSubscription func(types.Logs) error
type PendingBlockSubscription func(*types.Block) error
type PendingTxsSubscription func([]types.Transaction) error

// Events manages event subscriptions and dissemination. Thread-safe.
type Events struct {
	id                          int
	headerSubscriptions         map[int]chan [][]byte
	overlaySubscriptions        map[int]chan *execctx.SharedDomains
	newSnapshotSubscription     map[int]chan struct{}
	retirementStartSubscription map[int]chan bool
	retirementDoneSubscription  map[int]chan struct{}
	pendingLogsSubscriptions    map[int]PendingLogsSubscription
	pendingBlockSubscriptions   map[int]PendingBlockSubscription
	pendingTxsSubscriptions     map[int]PendingTxsSubscription
	logsSubscriptions           map[int]chan []*notifications.LogNotification
	hasLogSubscriptions         bool
	receiptsSubscriptions       map[int]chan []*notifications.ReceiptNotification
	hasReceiptSubscriptions     bool
	lock                        sync.RWMutex

	// latestSD holds the most recently published SharedDomains from FCU.
	// Accessible lock-free for the builder and RPC layer.
	latestSD atomic.Pointer[execctx.SharedDomains]
}

func NewEvents() *Events {
	return &Events{
		headerSubscriptions:         map[int]chan [][]byte{},
		overlaySubscriptions:        map[int]chan *execctx.SharedDomains{},
		receiptsSubscriptions:       map[int]chan []*notifications.ReceiptNotification{},
		pendingLogsSubscriptions:    map[int]PendingLogsSubscription{},
		pendingBlockSubscriptions:   map[int]PendingBlockSubscription{},
		pendingTxsSubscriptions:     map[int]PendingTxsSubscription{},
		logsSubscriptions:           map[int]chan []*notifications.LogNotification{},
		newSnapshotSubscription:     map[int]chan struct{}{},
		retirementStartSubscription: map[int]chan bool{},
		retirementDoneSubscription:  map[int]chan struct{}{},
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
		e.lock.Lock()
		defer e.lock.Unlock()
		delete(e.headerSubscriptions, id)
		close(ch)
	}
}

func (e *Events) AddReceiptsSubscription() (chan []*notifications.ReceiptNotification, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan []*notifications.ReceiptNotification, 8)
	e.id++
	id := e.id
	e.receiptsSubscriptions[id] = ch
	return ch, func() {
		e.lock.Lock()
		defer e.lock.Unlock()
		delete(e.receiptsSubscriptions, id)
		close(ch)
	}
}

func (e *Events) EmptyReceiptSubscription(empty bool) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.hasReceiptSubscriptions = !empty
}

func (e *Events) HasReceiptSubscriptions() bool {
	e.lock.RLock()
	defer e.lock.RUnlock()
	return e.hasReceiptSubscriptions
}

func (e *Events) AddNewSnapshotSubscription() (chan struct{}, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan struct{}, 8)
	e.id++
	id := e.id
	e.newSnapshotSubscription[id] = ch
	return ch, func() {
		e.lock.Lock()
		defer e.lock.Unlock()
		delete(e.newSnapshotSubscription, id)
		close(ch)
	}
}

func (e *Events) AddRetirementStartSubscription() (chan bool, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan bool, 8)
	e.id++
	id := e.id
	e.retirementStartSubscription[id] = ch
	return ch, func() {
		e.lock.Lock()
		defer e.lock.Unlock()
		delete(e.retirementStartSubscription, id)
		close(ch)
	}
}

func (e *Events) AddRetirementDoneSubscription() (chan struct{}, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan struct{}, 8)
	e.id++
	id := e.id
	e.retirementDoneSubscription[id] = ch
	return ch, func() {
		e.lock.Lock()
		defer e.lock.Unlock()
		delete(e.retirementDoneSubscription, id)
		close(ch)
	}
}

func (e *Events) AddLogsSubscription() (chan []*notifications.LogNotification, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan []*notifications.LogNotification, 8)
	e.id++
	id := e.id
	e.logsSubscriptions[id] = ch
	return ch, func() {
		e.lock.Lock()
		defer e.lock.Unlock()
		delete(e.logsSubscriptions, id)
		close(ch)
	}
}

func (e *Events) EmptyLogSubscription(empty bool) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.hasLogSubscriptions = !empty
}

func (e *Events) HasLogSubscriptions() bool {
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

// AddOverlaySubscription subscribes to SharedDomains publications. The SD
// holds both the block overlay (table data) and domain state (accounts,
// storage, code). In-process consumers (RPC, builder) use this to read
// uncommitted data during background FCU commits.
func (e *Events) AddOverlaySubscription() (chan *execctx.SharedDomains, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan *execctx.SharedDomains, 2)
	e.id++
	id := e.id
	e.overlaySubscriptions[id] = ch
	return ch, func() {
		e.lock.Lock()
		defer e.lock.Unlock()
		delete(e.overlaySubscriptions, id)
		close(ch)
	}
}

// PublishOverlay sends the SharedDomains to all in-process subscribers.
// The SD is shared read-only; the background commit goroutine owns its lifecycle.
func (e *Events) PublishOverlay(sd *execctx.SharedDomains) {
	e.latestSD.Store(sd)
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, ch := range e.overlaySubscriptions {
		common.PrioritizedSend(ch, sd)
	}
}

// LatestSD returns the most recently published SharedDomains, or nil.
func (e *Events) LatestSD() *execctx.SharedDomains {
	return e.latestSD.Load()
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

func (e *Events) OnLogs(logs []*notifications.LogNotification) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, ch := range e.logsSubscriptions {
		common.PrioritizedSend(ch, logs)
	}
}

func (e *Events) OnReceipts(receipts []*notifications.ReceiptNotification) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, ch := range e.receiptsSubscriptions {
		common.PrioritizedSend(ch, receipts)
	}
}

func (e *Events) OnRetirementStart(started bool) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, ch := range e.retirementStartSubscription {
		common.PrioritizedSend(ch, started)
	}
}

func (e *Events) OnRetirementDone() {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, ch := range e.retirementDoneSubscription {
		common.PrioritizedSend(ch, struct{}{})
	}
}

type Notifications struct {
	Events               *Events
	Accumulator          *Accumulator // StateAccumulator
	StateChangesConsumer StateChangeConsumer
	RecentReceipts       *RecentReceipts
	LastNewBlockSeen     atomic.Uint64 // This is used by eth_syncing as an heuristic to determine if the node is syncing or not.
}

func (n *Notifications) NewLastBlockSeen(blockNum uint64) {
	n.LastNewBlockSeen.Store(blockNum)
}

func NewNotifications(StateChangesConsumer StateChangeConsumer) *Notifications {
	return &Notifications{
		Events:               NewEvents(),
		Accumulator:          NewAccumulator(),
		RecentReceipts:       NewRecentReceipts(512),
		StateChangesConsumer: StateChangesConsumer,
	}
}
