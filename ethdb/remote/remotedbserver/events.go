package remotedbserver

import (
	"github.com/ledgerwatch/turbo-geth/core/types"
)

type RpcEventType uint64

const (
	EventTypeHeader = RpcEventType(iota)
)

type HeaderSubscription func(*types.Header) error

type Events struct {
	headerSubscriptions []HeaderSubscription
}

func NewEvents() *Events {
	return &Events{}
}

func (e *Events) AddHeaderSubscription(s HeaderSubscription) {
	e.headerSubscriptions = append(e.headerSubscriptions, s)
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
