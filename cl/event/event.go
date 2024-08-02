package event

import (
	ethevent "github.com/erigontech/erigon/event"
)

type EventNotifier struct {
	stateFeed     Feed
	operationFeed Feed
}

func NewEventNotifier() *EventNotifier {
	return &EventNotifier{
		stateFeed:     &ethevent.Feed{},
		operationFeed: &ethevent.Feed{},
	}
}

func (e *EventNotifier) State() Feed {
	return e.stateFeed
}

func (e *EventNotifier) Operation() Feed {
	return e.operationFeed
}
