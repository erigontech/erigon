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
	"sync/atomic"
	"sync"
)

type Observer[TEvent any] func(event TEvent)
type UnregisterFunc func()

type observersState[TEvent any] struct {
    observers map[uint64]Observer[TEvent]
}

type Observers[TEvent any] struct {
    state              atomic.Pointer[observersState[TEvent]]
    observerIdSequence atomic.Uint64
}

func NewObservers[TEvent any]() *Observers[TEvent] {
    o := &Observers[TEvent]{}
    o.state.Store(&observersState[TEvent]{observers: map[uint64]Observer[TEvent]{}{}})
    return o
}

func (o *Observers[TEvent]) nextObserverId() uint64 {
    return o.observerIdSequence.Add(1)
}

// Register an observer. Call the returned function to unregister it.
func (o *Observers[TEvent]) Register(observer Observer[TEvent]) UnregisterFunc {
    observerId := o.nextObserverId()
    for {
        cur := o.state.Load()
        var curMap map[uint64]Observer[TEvent]
        if cur != nil {
            curMap = cur.observers
        }
        newMap := make(map[uint64]Observer[TEvent], len(curMap)+1)
        for k, v := range curMap {
            newMap[k] = v
        }
        newMap[observerId] = observer
        newState := &observersState[TEvent]{observers: newMap}
        if o.state.CompareAndSwap(cur, newState) {
            break
        }
    }
    return o.unregisterFunc(observerId)
}

func (o *Observers[TEvent]) unregisterFunc(observerId uint64) UnregisterFunc {
    return func() {
        for {
            cur := o.state.Load()
            if cur == nil {
                return
            }
            if _, ok := cur.observers[observerId]; !ok {
                return
            }
            newMap := make(map[uint64]Observer[TEvent], len(cur.observers))
            for k, v := range cur.observers {
                if k != observerId {
                    newMap[k] = v
                }
            }
            newState := &observersState[TEvent]{observers: newMap}
            if o.state.CompareAndSwap(cur, newState) {
                return
            }
        }
    }
}

// Close unregisters all observers.
func (o *Observers[TEvent]) Close() {
    o.state.Store(&observersState[TEvent]{observers: map[uint64]Observer[TEvent][]{}})
}

// Notify all observers in parallel without waiting for them to process the event.
func (o *Observers[TEvent]) Notify(event TEvent) {
    cur := o.state.Load()
    if cur == nil {
        return
    }
    for _, observer := range cur.observers {
        go observer(event)
    }
}

// NotifySync all observers in parallel and wait until all of them process the event.
func (o *Observers[TEvent]) NotifySync(event TEvent) {
    cur := o.state.Load()
    if cur == nil {
        return
    }
    var wg sync.WaitGroup
    for _, observer := range cur.observers {
        wg.Add(1)
        go func(observer Observer[TEvent]) {
            defer wg.Done()
            observer(event)
        }(observer)
    }
    wg.Wait()
}
