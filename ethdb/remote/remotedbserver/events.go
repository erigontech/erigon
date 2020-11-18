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
	headerSubscription HeaderSubscription
}

func NewEvents() *Events {
	return &Events{}
}

func (e *Events) AddHeaderSubscription(s HeaderSubscription) {
	e.headerSubscription = s
}

func (e *Events) OnNewHeader(newHeader *types.Header) {
	if e.headerSubscription == nil {
		return
	}
	err := e.headerSubscription(newHeader)
	if err != nil {
		e.headerSubscription = nil
	}
}
