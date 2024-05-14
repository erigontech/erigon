package polygoncommon

import (
	"context"
	"sync"
	"sync/atomic"
)

// EventNotifier notifies waiters about an event.
// It supports a single "producer" and multiple waiters.
// A producer can set the event state to "signaled" or "non-signaled".
// Waiters can wait for the "signaled" event state.
type EventNotifier struct {
	mutex    sync.Mutex
	cond     *sync.Cond
	hasEvent atomic.Bool
}

func NewEventNotifier() *EventNotifier {
	instance := &EventNotifier{}
	instance.cond = sync.NewCond(&instance.mutex)
	return instance
}

// Reset to the "non-signaled" state.
func (en *EventNotifier) Reset() {
	en.hasEvent.Store(false)
}

// SetAndBroadcast sets the "signaled" state and notifies all waiters.
func (en *EventNotifier) SetAndBroadcast() {
	en.hasEvent.Store(true)
	en.cond.Broadcast()
}

// Wait for the "signaled" state.
// If the event is already "signaled" it returns immediately.
func (en *EventNotifier) Wait(ctx context.Context) {
	waitCtx, waitCancel := context.WithCancel(ctx)
	defer waitCancel()

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()

		en.mutex.Lock()
		defer en.mutex.Unlock()

		for !en.hasEvent.Load() && (waitCtx.Err() == nil) {
			en.cond.Wait()
		}
		waitCancel()
	}()

	// wait for the waiting goroutine or the parent context to finish, whichever happens first
	<-waitCtx.Done()

	// if the parent context is done, force the waiting goroutine to exit
	// this might lead to spurious wake ups for other waiters,
	// but it is ok due to the waiting loop conditions
	en.cond.Broadcast()

	wg.Wait()
}
