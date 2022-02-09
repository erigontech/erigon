package privateapi

import (
	"sync"

	"github.com/ledgerwatch/erigon/core/types"
)

type RpcEventType uint64

type HeaderSubscription func(headerRLP []byte) error
type PendingLogsSubscription func(types.Logs) error
type PendingBlockSubscription func(*types.Block) error
type PendingTxsSubscription func([]types.Transaction) error

// Events manages event subscriptions and dissimination. Thread-safe
type Events struct {
	id                        int
	headerSubscriptions       map[int]chan []byte
	pendingLogsSubscriptions  map[int]PendingLogsSubscription
	pendingBlockSubscriptions map[int]PendingBlockSubscription
	pendingTxsSubscriptions   map[int]PendingTxsSubscription
	lock                      sync.RWMutex
}

func NewEvents() *Events {
	return &Events{
		headerSubscriptions:       map[int]chan []byte{},
		pendingLogsSubscriptions:  map[int]PendingLogsSubscription{},
		pendingBlockSubscriptions: map[int]PendingBlockSubscription{},
		pendingTxsSubscriptions:   map[int]PendingTxsSubscription{},
	}
}

func (e *Events) AddHeaderSubscription() (chan []byte, func()) {
	e.lock.Lock()
	defer e.lock.Unlock()
	ch := make(chan []byte, 8)
	e.id++
	id := e.id
	e.headerSubscriptions[id] = ch
	return ch, func() {
		delete(e.headerSubscriptions, id)
		close(ch)
	}
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

func (e *Events) OnNewHeader(newHeaderRlp []byte) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for _, ch := range e.headerSubscriptions {
		select {
		case ch <- newHeaderRlp:
		default: //if channel is full (slow consumer), drop old messages
			for i := 0; i < cap(ch)/2; i++ {
				select {
				case <-ch:
				default:
				}
			}
			ch <- newHeaderRlp
		}
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
