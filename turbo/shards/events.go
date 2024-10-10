package shards

import (
	"sync"

	"github.com/gateway-fm/cdk-erigon-lib/common"
	"github.com/gateway-fm/cdk-erigon-lib/gointerfaces/remote"
	"github.com/ledgerwatch/erigon/core/types"
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
	Accumulator          *Accumulator
	StateChangesConsumer StateChangeConsumer
}
