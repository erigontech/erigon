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

package rpchelper

import (
	"sync"
	"sync/atomic"
	"time"
)

// a simple interface for subscriptions for rpc helper
type Sub[T any] interface {
	Send(T)
	Close()
	// Tracking methods for timeout-based eviction and metrics
	Touch()                                 // Reset last access time (for HTTP polling filters)
	SetProtocol(protocol string)            // Set protocol label for metrics
	EnableTimeout()                         // Enable timeout tracking (HTTP only)
	ShouldEvict(timeout time.Duration) bool // Check if subscription should be evicted
	Protocol() string                       // Get protocol label for metrics
}

type chan_sub[T any] struct {
	lock   sync.Mutex // protects ch and closed fields
	ch     chan T
	closed bool

	// protocol is set once at subscription setup and never changes (effectively immutable)
	protocol string
	// lastAccess tracks last poll time for timeout eviction (atomic for concurrent Touch/ShouldEvict)
	// lastAccess == 0 means no timeout tracking (WebSocket subscriptions)
	lastAccess atomic.Int64
}

// newChanSub - buffered channel
func newChanSub[T any](size int) *chan_sub[T] {
	if size < 8 { // set min size to 8
		size = 8
	}
	return &chan_sub[T]{
		ch: make(chan T, size),
	}
}
func (s *chan_sub[T]) Send(x T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return
	}
	select {
	case s.ch <- x:
	default: // the sub is overloaded, dispose message
	}
}
func (s *chan_sub[T]) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

// SetProtocol sets the protocol label for metrics tracking.
// Call this once at subscription setup. The value is immutable after being set.
func (s *chan_sub[T]) SetProtocol(protocol string) {
	s.protocol = protocol
}

// EnableTimeout enables timeout tracking for this subscription (HTTP polling filters).
// For WebSocket subscriptions, don't call this - they will never be evicted.
func (s *chan_sub[T]) EnableTimeout() {
	s.lastAccess.Store(time.Now().UnixNano())
}

// Touch resets the last access time, preventing timeout eviction.
// Only effective if EnableTimeout was called.
func (s *chan_sub[T]) Touch() {
	if s.lastAccess.Load() != 0 {
		s.lastAccess.Store(time.Now().UnixNano())
	}
}

// ShouldEvict returns true if the subscription has not been accessed within the timeout.
// Returns false if tracking is not enabled (lastAccess == 0).
func (s *chan_sub[T]) ShouldEvict(timeout time.Duration) bool {
	last := s.lastAccess.Load()
	if last == 0 {
		return false // No tracking, never evict (WebSocket)
	}
	return time.Since(time.Unix(0, last)) > timeout
}

// Protocol returns the protocol label for metrics.
func (s *chan_sub[T]) Protocol() string {
	return s.protocol
}
