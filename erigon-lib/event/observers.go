// Copyright 2024 The Erigon Authors
// This file is part of Erigon.
//
// Erigon is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// Erigon is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public License
// along with Erigon. If not, see <http://www.gnu.org/licenses/>.

package event

import (
	"sync"
)

type Observer[TEvent any] func(event TEvent)
type UnregisterFunc func()

type Observers[TEvent any] struct {
	observers          map[uint64]Observer[TEvent]
	observerIdSequence uint64
	observersMu        sync.Mutex
}

func NewObservers[TEvent any]() *Observers[TEvent] {
	return &Observers[TEvent]{
		observers: map[uint64]Observer[TEvent]{},
	}
}

func (o *Observers[TEvent]) nextObserverId() uint64 {
	o.observerIdSequence++
	return o.observerIdSequence
}

// Register an observer. Call the returned function to unregister it.
func (o *Observers[TEvent]) Register(observer Observer[TEvent]) UnregisterFunc {
	o.observersMu.Lock()
	defer o.observersMu.Unlock()

	observerId := o.nextObserverId()
	o.observers[observerId] = observer
	return o.unregisterFunc(observerId)
}

func (o *Observers[TEvent]) unregisterFunc(observerId uint64) UnregisterFunc {
	return func() {
		o.observersMu.Lock()
		defer o.observersMu.Unlock()

		delete(o.observers, observerId)
	}
}

// Close unregisters all observers.
func (o *Observers[TEvent]) Close() {
	o.observersMu.Lock()
	defer o.observersMu.Unlock()

	o.observers = map[uint64]Observer[TEvent]{}
}

// Notify all observers in parallel without waiting for them to process the event.
func (o *Observers[TEvent]) Notify(event TEvent) {
	o.observersMu.Lock()
	defer o.observersMu.Unlock()

	for _, observer := range o.observers {
		go observer(event)
	}
}

// NotifySync all observers in parallel and wait until all of them process the event.
func (o *Observers[TEvent]) NotifySync(event TEvent) {
	o.observersMu.Lock()
	defer o.observersMu.Unlock()

	var wg sync.WaitGroup
	for _, observer := range o.observers {
		wg.Add(1)
		go func(observer Observer[TEvent]) {
			defer wg.Done()
			observer(event)
		}(observer)
	}

	wg.Wait()
}
