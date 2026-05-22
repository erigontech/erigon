// Copyright 2021 The Erigon Authors
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

package common

import (
	"context"
	"errors"
	"sync"
)

var ErrStopped = errors.New("stopped")
var ErrUnwind = errors.New("unwound")

// FastContextErr is faster than ctx.Err() because usually it doesn't lock an internal mutex.
// It locks it only if the context is done and at the first call.
// See implementation of cancelCtx in context/context.go.
func FastContextErr(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		return nil
	}
}

func Stopped(ch <-chan struct{}) error {
	if ch == nil {
		return nil
	}
	select {
	case <-ch:
		return ErrStopped
	default:
	}
	return nil
}

func SafeClose(ch chan struct{}) {
	if ch == nil {
		return
	}
	select {
	case <-ch:
		// Channel was already closed
	default:
		close(ch)
	}
}

// Ready is a one-shot signal: zero-value is usable, calling Set closes the
// internal channel so that all waiters on On() unblock. Safe for concurrent use.
type Ready struct {
	mu     sync.Mutex
	on     chan struct{}
	state  bool
	inited bool
}

// On returns a channel that is closed once Set has been called.
func (r *Ready) On() <-chan struct{} {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lazyInit()
	return r.on
}

func (r *Ready) lazyInit() {
	if r.inited {
		return
	}
	r.on = make(chan struct{})
	r.inited = true
}

// Set signals readiness. Only the first call has an effect; subsequent calls are no-ops.
func (r *Ready) Set() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lazyInit()
	if r.state {
		return
	}
	r.state = true
	close(r.on)
}

// PrioritizedSend message to channel, but if channel is full (slow consumer) - drop half of old messages (not new)
func PrioritizedSend[t any](ch chan t, msg t) {
	select {
	case ch <- msg:
	default: //if channel is full (slow consumer), drop old messages (not new)
		for i := 0; i < cap(ch)/2; i++ {
			select {
			case <-ch:
			default:
			}
		}
		ch <- msg
	}
}
