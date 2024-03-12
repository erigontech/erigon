package sync

import (
	"container/list"
	"context"
	"sync"
)

// EventChannel is a buffered channel that drops oldest events when full.
type EventChannel[TEvent any] struct {
	events chan TEvent

	queue      *list.List
	queueCap   uint
	queueMutex sync.Mutex
	queueCond  *sync.Cond
}

func NewEventChannel[TEvent any](capacity uint) *EventChannel[TEvent] {
	if capacity == 0 {
		panic("NewEventChannel: capacity must be > 0")
	}

	ec := &EventChannel[TEvent]{
		events: make(chan TEvent),

		queue:    list.New(),
		queueCap: capacity,
	}

	ec.queueCond = sync.NewCond(&ec.queueMutex)

	return ec
}

// Events returns a channel for reading events.
func (ec *EventChannel[TEvent]) Events() <-chan TEvent {
	return ec.events
}

// PushEvent queues an event. If the queue is full, it drops the oldest event to make space.
func (ec *EventChannel[TEvent]) PushEvent(e TEvent) {
	ec.queueMutex.Lock()
	defer ec.queueMutex.Unlock()

	if uint(ec.queue.Len()) == ec.queueCap {
		ec.queue.Remove(ec.queue.Front())
	}

	ec.queue.PushBack(e)
	ec.queueCond.Signal()
}

// takeEvent dequeues an event. If the queue was empty, it returns false.
func (ec *EventChannel[TEvent]) takeEvent() (TEvent, bool) {
	if elem := ec.queue.Front(); elem != nil {
		e := ec.queue.Remove(elem).(TEvent)
		return e, true
	} else {
		var emptyEvent TEvent
		return emptyEvent, false
	}
}

// takeEvent dequeues an event. If the queue was empty, it blocks.
func (ec *EventChannel[TEvent]) waitForEvent(ctx context.Context) (TEvent, error) {
	waitCtx, waitCancel := context.WithCancel(ctx)
	defer waitCancel()

	var e TEvent

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		ec.queueMutex.Lock()
		defer ec.queueMutex.Unlock()

		var ok bool
		for e, ok = ec.takeEvent(); !ok && (waitCtx.Err() == nil); e, ok = ec.takeEvent() {
			ec.queueCond.Wait()
		}

		waitCancel()
	}()

	// wait for the waiting goroutine or the parent context to finish, whichever happens first
	<-waitCtx.Done()

	// if the parent context is done, force the waiting goroutine to exit
	ec.queueCond.Signal()
	wg.Wait()

	return e, ctx.Err()
}

// Run pumps events from the queue to the events channel.
func (ec *EventChannel[TEvent]) Run(ctx context.Context) error {
	for {
		e, err := ec.waitForEvent(ctx)
		if err != nil {
			return err
		}

		select {
		case ec.events <- e:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}
