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
)

const (
	MessagesQueueSize      = 1024
	MessagesQueueByteLimit = 64 * 1024 * 1024
)

type streamReply[T protoreflect.ProtoMessage] struct {
	message T
	err     error
	size    int
}

type messageQueue[T protoreflect.ProtoMessage] struct {
	mu     sync.Mutex
	items  chan streamReply[T]
	ready  chan struct{}
	bytes  int
	closed bool
}

func (q *messageQueue[T]) push(message T, err error) error {
	size := proto.Size(message)
	if size > MessagesQueueByteLimit {
		return fmt.Errorf("sentry message exceeds queue byte limit: %d", size)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return io.EOF
	}
	for q.bytes+size > MessagesQueueByteLimit {
		q.pop()
	}
	q.items <- streamReply[T]{message: message, err: err, size: size}
	q.bytes += size
	if len(q.items) > cap(q.items)/2 {
		for range cap(q.items) / 4 {
			q.pop()
		}
	}
	q.notify()
	return nil
}

func (q *messageQueue[T]) pop() streamReply[T] {
	item := <-q.items
	q.bytes -= item.size
	return item
}

func (q *messageQueue[T]) notify() {
	if !q.closed && len(q.items) > 0 {
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
			return item.message, item.err
		}
		closed := q.closed
		q.mu.Unlock()
		if closed {
			return zero, io.EOF
		}
	}
}

func (q *messageQueue[T]) close() {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.ready)
	}
}
