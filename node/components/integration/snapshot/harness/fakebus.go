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

package harness

import (
	"reflect"
	"sync"

	"github.com/erigontech/erigon/node/app/event"
)

// CapturedBus wraps an EventBus and records every published event in order.
// Useful for assertions on the timeline of events fired during a scenario.
//
// CapturedBus delegates subscribe/unsubscribe to the underlying bus but
// intercepts Publish to maintain the capture log before delegating.
type CapturedBus struct {
	inner event.EventBus

	mu     sync.Mutex
	events []Captured
}

// Captured pairs a publish timestamp-free ordinal with the published arguments.
// Publish arguments are stored as-is; do not mutate fields after the test has
// consumed the capture.
type Captured struct {
	Ordinal int
	Args    []interface{}
}

// NewCapturedBus wraps inner and begins capturing its publications.
func NewCapturedBus(inner event.EventBus) *CapturedBus {
	return &CapturedBus{inner: inner}
}

// Publish records the event and delegates to the underlying bus. The return
// value is the count from the inner bus (number of handlers invoked).
func (c *CapturedBus) Publish(args ...interface{}) int {
	c.mu.Lock()
	c.events = append(c.events, Captured{Ordinal: len(c.events), Args: args})
	c.mu.Unlock()
	return c.inner.Publish(args...)
}

func (c *CapturedBus) Subscribe(fn interface{}) error      { return c.inner.Subscribe(fn) }
func (c *CapturedBus) SubscribeAsync(fn interface{}) error { return c.inner.SubscribeAsync(fn) }
func (c *CapturedBus) SubscribeOnce(fn interface{}) error  { return c.inner.SubscribeOnce(fn) }
func (c *CapturedBus) SubscribeOnceAsync(fn interface{}) error {
	return c.inner.SubscribeOnceAsync(fn)
}
func (c *CapturedBus) Unsubscribe(fn interface{}) error { return c.inner.Unsubscribe(fn) }

func (c *CapturedBus) HasCallback(types ...reflect.Type) bool { return c.inner.HasCallback(types...) }
func (c *CapturedBus) WaitAsync()                             { c.inner.WaitAsync() }

// Events returns a snapshot copy of every event published so far. The returned
// slice is independent of future captures.
func (c *CapturedBus) Events() []Captured {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]Captured, len(c.events))
	copy(out, c.events)
	return out
}

// EventsOfType returns every captured event whose first argument has the
// given reflect.Type. Useful for counting how many of a specific event fired
// regardless of intervening publications.
func (c *CapturedBus) EventsOfType(t reflect.Type) []Captured {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []Captured
	for _, e := range c.events {
		if len(e.Args) == 0 {
			continue
		}
		if reflect.TypeOf(e.Args[0]) == t {
			out = append(out, e)
		}
	}
	return out
}

// CountOfType returns the number of published events whose first argument has
// the given type. Convenience for invariant assertions.
func (c *CapturedBus) CountOfType(t reflect.Type) int {
	return len(c.EventsOfType(t))
}
