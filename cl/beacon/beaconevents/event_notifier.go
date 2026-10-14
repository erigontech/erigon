package beaconevents

import "sync"

type EventEmitter struct {
	stateFeed     *stateFeed     // block state feed
	operationFeed *operationFeed // block operation feed
	headEventMu   sync.Mutex
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
