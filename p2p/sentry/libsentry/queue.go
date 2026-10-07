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

package libsentry

import (
	"context"
	"fmt"
	"io"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/erigontech/erigon/diagnostics/metrics"
)

const (
	// MessagesQueueSize bounds bookkeeping overhead for small messages.
	MessagesQueueSize = 1024
	// MessagesQueueByteLimit bounds serialized data waiting in each queue.
	// In-flight messages, transport buffers, and decoded objects are outside this budget.
	MessagesQueueByteLimit = 64 * 1024 * 1024
)

var (
	sentryQueueDroppedByBytes = metrics.GetOrCreateCounter(`p2p_sentry_queue_dropped_messages_total{limit="bytes"}`)
	sentryQueueDroppedByCount = metrics.GetOrCreateCounter(`p2p_sentry_queue_dropped_messages_total{limit="count"}`)
)

type queuedMessage[T protoreflect.ProtoMessage] struct {
	message T
	size    int
}

// messageQueue keeps eviction and receiving under the same lock, so each
// item releases its byte budget exactly once. All access to items, bytes,
// and err holds mu.
type messageQueue[T protoreflect.ProtoMessage] struct {
	mu    sync.Mutex
	items []queuedMessage[T]
	ready chan struct{}
	bytes int
	err   error // nil while open, the terminal error while draining, then io.EOF.
}

func (q *messageQueue[T]) push(message T) error {
	size := proto.Size(message)
	if size > MessagesQueueByteLimit {
		return fmt.Errorf("sentry message exceeds queue byte limit: %d", size)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err != nil {
		return io.EOF
	}
	for q.bytes+size > MessagesQueueByteLimit {
		q.pop()
		sentryQueueDroppedByBytes.Inc()
	}
	q.items = append(q.items, queuedMessage[T]{message: message, size: size})
	q.bytes += size
	// Evict in batches so slow consumers see recent traffic and leave room
	// for bursts. Use the fixed limit because popping entries changes the
	// slice capacity.
	if len(q.items) > MessagesQueueSize/2 {
		for range MessagesQueueSize / 4 {
			q.pop()
			sentryQueueDroppedByCount.Inc()
		}
	}
	q.notify()
	return nil
}

// pop requires mu to be held and items to be non-empty. Clear the removed
// slot so its payload can be collected while remaining entries share the array.
func (q *messageQueue[T]) pop() queuedMessage[T] {
	item := q.items[0]
	q.items[0] = queuedMessage[T]{}
	if len(q.items) == 1 {
		// Reuse the last slot to avoid an allocation on the next push.
		q.items = q.items[:0]
	} else {
		q.items = q.items[1:]
	}
	q.bytes -= item.size
	return item
}

// notify coalesces wake-ups: ready is a hint to recheck items under mu.
// A receiver must signal again if items remain. The caller must hold mu
// to avoid signaling after close.
func (q *messageQueue[T]) notify() {
	if q.err == nil && len(q.items) > 0 {
		select {
		case q.ready <- struct{}{}:
		default:
		}
	}
}

func (q *messageQueue[T]) recv(ctx context.Context) (T, error) {
	var zero T
	for {
		select {
		case <-ctx.Done():
			return zero, ctx.Err()
		case <-q.ready:
		}
		q.mu.Lock()
		if len(q.items) > 0 {
			item := q.pop()
			q.notify()
			q.mu.Unlock()
			return item.message, nil
		}
		err := q.err
		if err != nil {
			q.err = io.EOF
		}
		q.mu.Unlock()
		if err != nil {
			return zero, err
		}
	}
}

// close stores the first terminal error outside items so eviction cannot drop it.
// Closing ready wakes all receivers; recv drains items before reporting the error.
func (q *messageQueue[T]) close(err error) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.err == nil {
		if err == nil {
			err = io.EOF
		}
		q.err = err
		close(q.ready)
	}
}
