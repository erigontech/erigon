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
	blockIndicesMu               sync.Mutex
	blockIndicesChanged          chan struct{}
}

// BlockIndicesChanged returns a channel that closes when an import commits block
// indices. Capture it before reading so a commit between checking and waiting
// cannot be missed.
func (e *EventEmitter) BlockIndicesChanged() <-chan struct{} {
	if e == nil {
		return nil
	}
	e.blockIndicesMu.Lock()
	defer e.blockIndicesMu.Unlock()
	if e.blockIndicesChanged == nil {
		e.blockIndicesChanged = make(chan struct{})
	}
	return e.blockIndicesChanged
}

// NotifyBlockIndicesChanged must be called after the index transaction commits.
func (e *EventEmitter) NotifyBlockIndicesChanged() {
	if e == nil {
		return
	}
	e.blockIndicesMu.Lock()
	defer e.blockIndicesMu.Unlock()
	if e.blockIndicesChanged != nil {
		close(e.blockIndicesChanged)
		e.blockIndicesChanged = nil
	}
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
