package remotedbserver

import (
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type RpcEventType uint64

type HeaderSubscription func(*types.Header) error
type PendingLogsSubscription func(types.Logs) error
type PendingBlockSubscription func(*types.Block) error

type Events struct {
	headerSubscriptions       []HeaderSubscription
	pendingLogsSubscriptions  []PendingLogsSubscription
	pendingBlockSubscriptions []PendingBlockSubscription
}

func NewEvents() *Events {
	return &Events{}
}

func (e *Events) AddHeaderSubscription(s HeaderSubscription) {
	e.headerSubscriptions = append(e.headerSubscriptions, s)
}

func (e *Events) AddPendingLogsSubscription(s PendingLogsSubscription) {
	e.pendingLogsSubscriptions = append(e.pendingLogsSubscriptions, s)
}

func (e *Events) AddPendingBlockSubscription(s PendingBlockSubscription) {
	e.pendingBlockSubscriptions = append(e.pendingBlockSubscriptions, s)
}

func (e *Events) OnNewHeader(newHeader *types.Header) {
	for i, sub := range e.headerSubscriptions {
		if err := sub(newHeader); err != nil {
			// remove subscription
			if i == len(e.headerSubscriptions)-1 {
				e.headerSubscriptions = e.headerSubscriptions[:i]
			} else if i < len(e.headerSubscriptions)-1 {
				e.headerSubscriptions = append(e.headerSubscriptions[:i], e.headerSubscriptions[i+1:]...)
			}
		}
	}
}

func (e *Events) OnNewPendingLogs(logs types.Logs) {
	for i, sub := range e.pendingLogsSubscriptions {
		if err := sub(logs); err != nil {
			// remove subscription
			if i == len(e.pendingLogsSubscriptions)-1 {
				e.pendingLogsSubscriptions = e.pendingLogsSubscriptions[:i]
			} else if i < len(e.pendingLogsSubscriptions)-1 {
				e.pendingLogsSubscriptions = append(e.pendingLogsSubscriptions[:i], e.pendingLogsSubscriptions[i+1:]...)
			}
		}
	}
}

func (e *Events) OnNewPendingBlock(block *types.Block) {
	for i, sub := range e.pendingBlockSubscriptions {
		if err := sub(block); err != nil {
			// remove subscription
			if i == len(e.pendingBlockSubscriptions)-1 {
				e.pendingBlockSubscriptions = e.pendingBlockSubscriptions[:i]
			} else if i < len(e.pendingBlockSubscriptions)-1 {
				e.pendingBlockSubscriptions = append(e.pendingBlockSubscriptions[:i], e.pendingBlockSubscriptions[i+1:]...)
			}
		}
	}
}
