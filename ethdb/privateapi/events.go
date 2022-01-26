package privateapi

import (
	"sync"

	"github.com/ledgerwatch/erigon/core/types"
)

type RpcEventType uint64

type HeaderSubscription func(*types.Header) error
type PendingLogsSubscription func(types.Logs) error
type PendingBlockSubscription func(*types.Block) error
type PendingTxsSubscription func([]types.Transaction) error
type LogsSubscription func(*types.Log) error

// Events manages event subscriptions and dissimination. Thread-safe
type Events struct {
	headerSubscriptions       map[int]HeaderSubscription
	pendingLogsSubscriptions  map[int]PendingLogsSubscription
	pendingBlockSubscriptions map[int]PendingBlockSubscription
	pendingTxsSubscriptions   map[int]PendingTxsSubscription
	lock                      sync.RWMutex
}

func NewEvents() *Events {
	return &Events{
		headerSubscriptions:       map[int]HeaderSubscription{},
		pendingLogsSubscriptions:  map[int]PendingLogsSubscription{},
		pendingBlockSubscriptions: map[int]PendingBlockSubscription{},
		pendingTxsSubscriptions:   map[int]PendingTxsSubscription{},
	}
}

func (e *Events) AddHeaderSubscription(s HeaderSubscription) {
	e.lock.Lock()
	defer e.lock.Unlock()
	e.headerSubscriptions[len(e.headerSubscriptions)] = s
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

func (e *Events) AddLogsSubscription(s LogsSubscription) {
	e.lock.Lock()
	defer e.lock.Unlock()
}

func (e *Events) OnNewHeader(newHeader *types.Header) {
	e.lock.Lock()
	defer e.lock.Unlock()
	for i, sub := range e.headerSubscriptions {
		if err := sub(newHeader); err != nil {
			delete(e.headerSubscriptions, i)
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
