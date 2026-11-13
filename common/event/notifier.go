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
	"context"
	"sync"
)

// Notifier notifies waiters about an event.
// It supports a single "producer" and multiple waiters.
// A producer can set the event state to "signaled" or "non-signaled".
// Waiters can wait for the "signaled" event state.
//
// Every access to hasEvent and every Broadcast happens under mutex. A waiter
// evaluates the loop condition and registers on the cond as one atomic step,
// so a broadcast can never land in between and be lost.
type Notifier struct {
	mutex    sync.Mutex
	cond     *sync.Cond
	hasEvent bool
}

func NewNotifier() *Notifier {
	instance := &Notifier{}
	instance.cond = sync.NewCond(&instance.mutex)
	return instance
}

// Reset to the "non-signaled" state.
func (en *Notifier) Reset() {
	en.mutex.Lock()
	defer en.mutex.Unlock()

	en.hasEvent = false
}

// SetAndBroadcast sets the "signaled" state and notifies all waiters.
func (en *Notifier) SetAndBroadcast() {
	en.mutex.Lock()
	defer en.mutex.Unlock()

	en.hasEvent = true
	en.cond.Broadcast()
}

// Wait for the "signaled" state.
// If the event is already "signaled" it returns immediately.
func (en *Notifier) Wait(ctx context.Context) error {
	waitCtx, waitCancel := context.WithCancel(ctx)
	defer waitCancel()

	var wg sync.WaitGroup
	wg.Go(func() {
		en.mutex.Lock()
		defer en.mutex.Unlock()

		for !en.hasEvent && (waitCtx.Err() == nil) {
			en.cond.Wait()
		}
		waitCancel()
	})

	// wait for the waiting goroutine or the parent context to finish, whichever happens first
	<-waitCtx.Done()

	// if the parent context is done, force the waiting goroutine to exit
	// this might lead to spurious wake-ups for other waiters,
	// but it is ok due to the waiting loop conditions
	en.mutex.Lock()
	en.cond.Broadcast()
	en.mutex.Unlock()

	wg.Wait()
	return ctx.Err()
}
