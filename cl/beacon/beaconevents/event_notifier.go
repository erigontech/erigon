package beaconevents

import (
	"sync"
	"sync/atomic"
)

type EventEmitter struct {
	stateFeed                    *stateFeed     // block state feed
	operationFeed                *operationFeed // block operation feed
	headEventMu                  sync.Mutex
	payloadAttributesSubscribers atomic.Int64
}

func (e *EventEmitter) TrackPayloadAttributesSubscriber() func() {
	e.payloadAttributesSubscribers.Add(1)
	return func() { e.payloadAttributesSubscribers.Add(-1) }
}

func (e *EventEmitter) HasPayloadAttributesSubscribers() bool {
	return e.payloadAttributesSubscribers.Load() > 0
}

func (e *EventEmitter) WithHeadEventLock(fn func()) {
	e.headEventMu.Lock()
	defer e.headEventMu.Unlock()
	fn()
}

func NewEventEmitter() *EventEmitter {
	return &EventEmitter{
		operationFeed: newOpFeed(),
		stateFeed:     newStateFeed(),
	}
}

func (e *EventEmitter) State() *stateFeed {
	return e.stateFeed
}

func (e *EventEmitter) Operation() *operationFeed {
	return e.operationFeed
}
