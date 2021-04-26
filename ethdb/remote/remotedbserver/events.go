package remotedbserver

import (
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type RpcEventType uint64

type HeaderSubscription func(*types.Header) error
type PendingLogsSubscription func(types.Logs) error
type PendingBlockSubscription func(*types.Block) error
type PendingTxsSubscription func([]types.Transaction) error

type Events struct {
	headerSubscriptions       map[int]HeaderSubscription
	pendingLogsSubscriptions  map[int]PendingLogsSubscription
	pendingBlockSubscriptions map[int]PendingBlockSubscription
	pendingTxsSubscriptions   map[int]PendingTxsSubscription
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
	e.headerSubscriptions[len(e.headerSubscriptions)] = s
}

func (e *Events) AddPendingLogsSubscription(s PendingLogsSubscription) {
	e.pendingLogsSubscriptions[len(e.pendingLogsSubscriptions)] = s
}

func (e *Events) AddPendingBlockSubscription(s PendingBlockSubscription) {
	e.pendingBlockSubscriptions[len(e.pendingBlockSubscriptions)] = s
}

func (e *Events) AddPendingTxsSubscription(s PendingTxsSubscription) {
	e.pendingTxsSubscriptions[len(e.pendingTxsSubscriptions)] = s
}

func (e *Events) OnNewHeader(newHeader *types.Header) {
	for i, sub := range e.headerSubscriptions {
		if err := sub(newHeader); err != nil {
			delete(e.headerSubscriptions, i)
		}
	}
}

func (e *Events) OnNewPendingLogs(logs types.Logs) {
	for i, sub := range e.pendingLogsSubscriptions {
		if err := sub(logs); err != nil {
			delete(e.pendingLogsSubscriptions, i)
		}
	}
}

func (e *Events) OnNewPendingBlock(block *types.Block) {
	for i, sub := range e.pendingBlockSubscriptions {
		if err := sub(block); err != nil {
			delete(e.pendingBlockSubscriptions, i)
		}
	}
}

func (e *Events) OnNewPendingTxs(txs []types.Transaction) {
	for i, sub := range e.pendingTxsSubscriptions {
		if err := sub(txs); err != nil {
			delete(e.pendingTxsSubscriptions, i)
		}
	}
}
