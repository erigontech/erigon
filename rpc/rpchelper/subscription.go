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
	CloseIfIdle(timeout time.Duration) bool // Close and report true when idle longer than timeout
	Protocol() string                       // Get protocol label for metrics
}

type chan_sub[T any] struct {
	lock     sync.Mutex // protects all fields of this struct
	ch       chan T
	closed   bool
	protocol string
	// lastAccess is the last poll time for timeout eviction; zero means no
	// timeout tracking (push subscriptions die with their connection instead).
	lastAccess time.Time
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

// Close is idempotent and safe to call concurrently with Send and CloseIfIdle.
func (s *chan_sub[T]) Close() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.closeLocked()
}

func (s *chan_sub[T]) closeLocked() {
	if s.closed {
		return
	}
	s.closed = true
	close(s.ch)
}

// SetProtocol sets the protocol label for metrics tracking.
func (s *chan_sub[T]) SetProtocol(protocol string) {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.protocol = protocol
}

// EnableTimeout enables timeout tracking for this subscription (HTTP polling filters).
// For WebSocket subscriptions, don't call this - they will never be evicted.
func (s *chan_sub[T]) EnableTimeout() {
	s.lock.Lock()
	defer s.lock.Unlock()
	s.lastAccess = time.Now()
}

// Touch resets the last access time, preventing timeout eviction.
// Only effective if EnableTimeout was called.
func (s *chan_sub[T]) Touch() {
	s.lock.Lock()
	defer s.lock.Unlock()
	if !s.lastAccess.IsZero() && !s.closed {
		s.lastAccess = time.Now()
	}
}

// CloseIfIdle closes the subscription and reports true when timeout tracking is
// enabled and the last access is older than timeout. Deciding and closing under
// the same lock Touch uses guarantees a subscription touched within the timeout
// is never evicted.
func (s *chan_sub[T]) CloseIfIdle(timeout time.Duration) bool {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed || s.lastAccess.IsZero() || time.Since(s.lastAccess) <= timeout {
		return false
	}
	s.closeLocked()
	return true
}

// Protocol returns the protocol label for metrics. Returns "" if SetProtocol was never called.
func (s *chan_sub[T]) Protocol() string {
	s.lock.Lock()
	defer s.lock.Unlock()
	return s.protocol
}
