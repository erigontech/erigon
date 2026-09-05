// Copyright 2026 The Erigon Authors
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

package concurrent

import (
	"context"
	"fmt"
	"runtime/debug"
	"sync"
	"time"

	"github.com/erigontech/erigon/common/log/v3"
)

// CachedValue keeps a value for a TTL and lets one producer at a time refresh it. Whether
// a value was ever produced is tracked apart from when it was last asked for, so a caller
// can tell a default from an observation and a producer that keeps failing costs one
// attempt per TTL. The zero value is usable and never fresh.
type CachedValue[T any] struct {
	mu        sync.Mutex
	value     T
	observed  bool
	attempted time.Time
	ttl       time.Duration
	running   *run[T]
}

// run is one pass of a producer. Its result is readable once done is closed.
type run[T any] struct {
	done  chan struct{}
	value T
	err   error
}

func (c *CachedValue[T]) SetTTL(ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.ttl = ttl
}

// Load reports the value, whether a producer ever delivered one, and whether the last
// attempt is recent enough to stand in for another one.
func (c *CachedValue[T]) Load() (value T, observed, fresh bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.value, c.observed, time.Since(c.attempted) < c.ttl
}

// Produce runs produce on the caller's goroutine, or waits for the pass already running
// and reports ran=false. Running on the caller's own goroutine is what a producer reading
// through the caller's transaction needs: the pass cannot outlive the caller that owns it.
// A caller that only waits is released by ctx, since the pass is not its to abandon.
func (c *CachedValue[T]) Produce(ctx context.Context, produce func() (value T, store bool, err error)) (value T, ran bool, err error) {
	r, mine := c.claim()
	if mine {
		value, err = c.run(r, produce)
		return value, true, err
	}
	select {
	case <-r.done:
		return r.value, false, r.err
	case <-ctx.Done():
		var zero T
		return zero, false, ctx.Err()
	}
}

// Go runs produce on a goroutine of its own, or joins the pass already running, and
// returns the channel closed when that pass ends. It is for a caller that must not pay
// for the refresh it triggers; the value is read back with Load.
func (c *CachedValue[T]) Go(produce func() (value T, store bool, err error)) <-chan struct{} {
	r, mine := c.claim()
	if mine {
		go func() {
			// The pass owns this goroutine, where an escaping panic would take the
			// process down instead of the caller that caused it.
			defer func() {
				if p := recover(); p != nil {
					log.Error("[cached-value] producer panicked", "err", p, "stack", string(debug.Stack()))
				}
			}()
			_, _ = c.run(r, produce)
		}()
	}
	return r.done
}

func (c *CachedValue[T]) claim() (*run[T], bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.running != nil {
		return c.running, false
	}
	c.running = &run[T]{done: make(chan struct{})}
	return c.running, true
}

// run publishes what produce answered and releases the callers waiting on it. A producer
// that panics is published as a failed pass and then re-panics: swallowing it here would
// hand the caller a zero value it cannot tell from an answer.
func (c *CachedValue[T]) run(r *run[T], produce func() (T, bool, error)) (value T, err error) {
	store := false
	defer func() {
		if p := recover(); p != nil {
			c.publish(r, value, false, fmt.Errorf("cached value producer panicked: %v", p))
			panic(p)
		}
		c.publish(r, value, store, err)
	}()

	value, store, err = produce()
	return value, err
}

// publish records the attempt, stores what is worth keeping, and ends the pass. Closing
// done before clearing running keeps a caller from starting a pass of its own while this
// one is still published as in flight.
func (c *CachedValue[T]) publish(r *run[T], value T, store bool, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.attempted = time.Now()
	if store && err == nil {
		c.value, c.observed = value, true
	}
	r.value, r.err = value, err
	close(r.done)
	c.running = nil
}
