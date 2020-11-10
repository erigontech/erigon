package remotedbserver

import (
	"github.com/ledgerwatch/turbo-geth/core/types"
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
	e.headerSubscription(newHeader)
}
