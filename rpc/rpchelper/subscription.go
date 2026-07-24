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

// SubProtocol says how a subscription is consumed and thereby its lifecycle: HTTP
// polling filters are subject to timeout eviction, WS subscriptions end with their
// connection, and the empty value marks internal subscriptions that are neither
// evicted nor counted in metrics.
type SubProtocol string

const (
	ProtocolHTTP SubProtocol = "http"
	ProtocolWS   SubProtocol = "ws"
)

// a simple interface for subscriptions for rpc helper
type Sub[T any] interface {
	Send(T)
	Close()
	SubTracker
}

// SubTracker is the element-type-independent surface of a subscription,
// used for timeout-based eviction and metrics.
type SubTracker interface {
	Touch()                                 // reset the eviction deadline (HTTP polling filters)
	CloseIfIdle(timeout time.Duration) bool // close and report true when idle longer than timeout
	Protocol() SubProtocol
}

type chan_sub[T any] struct {
	lock     sync.Mutex // protects all mutable fields of this struct
	ch       chan T
	closed   bool
	protocol SubProtocol // immutable after construction
	// lastAccess is the last poll time for timeout eviction; zero means no
	// timeout tracking (push subscriptions die with their connection instead).
	lastAccess time.Time
}

// newChanSub - buffered channel
func newChanSub[T any](size int, protocol SubProtocol) *chan_sub[T] {
	if size < 8 { // set min size to 8
		size = 8
	}
	s := &chan_sub[T]{
		ch:       make(chan T, size),
		protocol: protocol,
	}
	if protocol == ProtocolHTTP {
		s.lastAccess = time.Now()
	}
	return s
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

// SendLatest delivers with latest-value semantics: when the subscriber is
// overloaded the oldest buffered message is dropped to make room, so the most
// recent value always survives. For subscriptions where only the current
// state matters, not the history.
func (s *chan_sub[T]) SendLatest(x T) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if s.closed {
		return
	}
	for {
		select {
		case s.ch <- x:
			return
		default:
		}
		if cap(s.ch) == 0 {
			// nothing to evict: deliver only if a reader was ready
			return
		}
		select {
		case <-s.ch:
		default:
		}
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

// Touch resets the last access time, preventing timeout eviction.
// A no-op unless the subscription is tracked for eviction.
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

func (s *chan_sub[T]) Protocol() SubProtocol {
	return s.protocol
}
