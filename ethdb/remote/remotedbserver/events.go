package remotedbserver

import (
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type RpcEventType uint64

type HeaderSubscription func(*types.Header) error
type PendingLogsSubscription func(types.Logs) error

type Events struct {
	headerSubscriptions      []HeaderSubscription
	pendingLogsSubscriptions []PendingLogsSubscription
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
