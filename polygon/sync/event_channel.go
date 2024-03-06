package sync

import (
	"container/list"
	"context"
	"sync"
	"time"
)

// EventChannel is a buffered channel that drops oldest events when full.
type EventChannel[TEvent any] struct {
	events    chan TEvent
	pollDelay time.Duration

	queue      *list.List
	queueCap   uint
	queueMutex sync.Mutex
}

func NewEventChannel[TEvent any](capacity uint, pollDelay time.Duration) *EventChannel[TEvent] {
	if capacity == 0 {
		panic("NewEventChannel: capacity must be > 0")
	}
	return &EventChannel[TEvent]{
		events:    make(chan TEvent),
		pollDelay: pollDelay,

		queue:    list.New(),
		queueCap: capacity,
	}
}

// Events returns a channel for reading events.
func (te *EventChannel[TEvent]) Events() <-chan TEvent {
	return te.events
}

// PushEvent queues an event. If the queue is full, it drops the oldest event to make space.
func (te *EventChannel[TEvent]) PushEvent(e TEvent) {
	te.queueMutex.Lock()
	defer te.queueMutex.Unlock()

	if uint(te.queue.Len()) == te.queueCap {
		te.queue.Remove(te.queue.Front())
	}

	te.queue.PushBack(e)
}

// takeEvent dequeues an event. If the queue was empty, it returns false.
func (te *EventChannel[TEvent]) takeEvent() (TEvent, bool) {
	te.queueMutex.Lock()
	defer te.queueMutex.Unlock()

	if elem := te.queue.Front(); elem != nil {
		e := te.queue.Remove(elem).(TEvent)
		return e, true
	} else {
		var emptyEvent TEvent
		return emptyEvent, false
	}
}

// Run pumps events from the queue to the events channel.
func (te *EventChannel[TEvent]) Run(ctx context.Context) error {
	for {
		e, ok := te.takeEvent()
		if !ok {
			pollDelayTimer := time.NewTimer(te.pollDelay)
			select {
			case <-pollDelayTimer.C:
				continue
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		select {
		case te.events <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
